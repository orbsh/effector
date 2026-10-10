//! BGI + exec carriers (ADR-0035): out-of-process booths in two shapes
//! that do NOT share a protocol — the fcgi/cgi split, named precisely.
//!
//! **bgi (Booth Gateway Interface — resident, framed)**: the child lives
//! as long as the residency and speaks the line protocol — request
//! frames in, result frames out, ctx round trips inline (host frames the
//! parent answers). The author's code runs a loop: its own, or the
//! nushell fifo adapter (the "fcgi-adapts-cgi" move — see `BgiKind`).
//! Frame line:
//!   parent → child: {"id":N,"kind":"call|iterate_start|iterate_next|iterate_dispose","event":"...","op":"start|next|dispose","args":...,"stream_id":"..."}
//!   child → parent: {"host":{"type":"invoke|iterate.start|iterate.next|iterate.dispose|store|schema",…}} — TYPED frames (ADR-0037 §2): the discriminator fails at decode, never as a silent unknown-op string lookup. Arms are the ctx method paths (ADR-0043): a wrong type is a decode error, never a table miss.
//!   → parent answers {"host_reply":{"ok":...}}
//!   child → parent: {"result":...} (terminates one request; success only — failures ride the outer Result, ADR-0012)
//!
//! Frame ENCODING is declared per booth (ADR-0037 §2, user ruling
//! 2026-09-30 — dual-protocol, replacing the single-encoding plan):
//! JSON-lines (the default, stdlib-reachable — nu rides it) or CBOR
//! (one self-delimited document per frame; Rust guests via ciborium).
//! The carrier speaks the declared codec on both directions and the
//! host frame vocabulary is typed under BOTH (orthogonal axes). The
//! child learns the encoding through the `BGI_ENCODING` env var
//! ("json" | "cbor"; absent = json) — an env, not an appended argv:
//! the fifo shape passes exactly [req rep] and a runtime-generated
//! argument would break the author's `def main` arity.
//! One outstanding host call at a time (synchronous-by-contract, like
//! every bridge). The source string IS the argv (whitespace-split — the
//! spawn spec; remote content-addressed binaries are a recorded
//! residual). bwrap policy wraps the spawn (mount policy before exec,
//! the child's capability surface = the channels + jail).
//!
//! The dispatch contract (user ruling 2026-09-29): the event frame
//! enters the child's SINGLE entry point (bgi: the loop reads frames and
//! calls the author's dispatch — `main` for nushell, a match in the loop
//! body for Rust fixtures; embedded carriers hand the name to a native
//! registry: python/steel collect `on` declarations, wasm address
//! exports). No language needs eval; languages without a runtime name
//! lookup dispatch by hand (literal match arms).
//!
//! **exec (bare one-shot)**: no protocol. Spawn per call — one JSON
//! document written to stdin (closed = the script's cue), stdout read
//! whole as the result; the script implements nothing (the php-fpm
//! lineage refined from the user's fcgi-vs-cgi analysis: stateless by
//! DEFINITION — iterate is a named error, no ctx channel exists, nothing
//! to sweep).
//!
//! ADR-0035's "B is A-without-a-loop" phrasing is superseded by this
//! file's shape: B is NOT a degraded protocol — it has no protocol.

use super::session::{ResidentSession, StreamOp};
use super::HostBridge;
use anyhow::{anyhow, Result};
use effector_protocol::ChannelEncoding;
use serde::Deserialize;
use serde_json::Value;
use std::ffi::CString;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};

// ----------------------------------------------------------- frames --

/// The typed host frame the child sends (ADR-0037 §2 — one stream, typed
/// frames; the free-function-name lookup is retired). Deserializing the
/// discriminator at the frame-structure layer turns a typo'd shape into a
/// decode error (an error value back into the child), never a silent
/// unknown-op miss in the bridge table. The store payload stays DATA —
/// the effector never parses the okm instruction (schema-blind rule; §3's
/// retirement is the ENTRY's retirement, not the instruction document's).
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TypedHostFrame {
    /// `ctx.invoke` (bridge key `invoke`): the target selection object rides as args.
    Invoke { args: Value },
    /// The ADR-0034 consumer legs — one frame type, the verb inside
    /// (start carries the target selection, next/dispose the stream id).
    #[serde(rename = "iterate.start")]
    IterateStart { args: Value },
    #[serde(rename = "iterate.next")]
    IterateNext { args: Value },
    #[serde(rename = "iterate.dispose")]
    IterateDispose { args: Value },
    /// `ctx.store` (bridge key `store`): one okm Collection instruction as data.
    #[serde(rename = "store")]
    Store { op: Value },
    /// `ctx.schema` (bridge key `schema`): the declared schema, no arguments.
    #[serde(rename = "schema")]
    Schema {},
    /// `ctx.emit` (bridge key `emit`): fire-and-forget by contract
    /// (ADR-0043 §5) — the parent resolves routes and sends, the guest
    /// does NOT wait for a reply; no host_reply frame goes back, and a
    /// publish failure lands on the parent's stderr, never as an error
    /// value into the child. Reliability rides the event log / pull
    /// planes, not this frame.
    #[serde(rename = "emit")]
    Emit { args: Value },
}

impl TypedHostFrame {
    /// Map onto the bridge's named-function table (ADR-0043: the names
    /// are the dotted ctx method paths — the in-process carriers inject
    /// these same keys as one `ctx` object; the typed frame is the
    /// process seam's shape, not a new vocabulary). Returns
    /// (fn name, arg, expects_reply).
    fn dispatch(self) -> (&'static str, Value, bool) {
        match self {
            TypedHostFrame::Invoke { args } => ("invoke", args, true),
            TypedHostFrame::IterateStart { args } => ("iterate.start", args, true),
            TypedHostFrame::IterateNext { args } => ("iterate.next", args, true),
            TypedHostFrame::IterateDispose { args } => ("iterate.dispose", args, true),
            TypedHostFrame::Store { op } => ("store", op, true),
            TypedHostFrame::Schema {} => ("schema", Value::Null, true),
            TypedHostFrame::Emit { args } => ("emit", args, false),
        }
    }
}

// --------------------------------------------------------------- bgi --

/// One resident bgi session: the framed child, in one of two shapes.
pub struct BgiSession {
    /// Host functions for the child's ctx round trips — the child sends
    /// them inside a TYPED host frame (ADR-0037 §2), mapped to these
    /// dotted keys at dispatch ("invoke", "store", …, ADR-0043).
    host: HostBridge,
    kind: BgiKind,
    next_id: u64,
    /// Declared frame codec (ADR-0037 §2, dual-protocol): the channel's
    /// encoding for the session's whole life, both directions.
    encoding: ChannelEncoding,
}

/// The request channel differs per language's blocking-read capability:
///
/// **Pipes** — a child that block-reads stdin (Rust fixtures, bash
/// `read`, any line reader): the classic two-pipe shape.
///
/// **Fifo** — the nushell adapter (ADR-0035 §8, two fifos). nu cannot
/// block-read non-TTY stdin and its `open` delivers at writer-EOF, so
/// stdin-direct is impossible. The parent creates `req` + `rep` fifos
/// and spawns `nu --no-config-file <author.nu> <req> <rep>`: the
/// author's OWN `def main` is the loop (no generated shim, no `source`
/// of the author module — nu auto-invokes `main` with the script
/// arguments; dynamic `source` paths are rejected at parse). Requests
/// ride `req` (the parent writes one frame and closes the writer — the
/// batch EOF the child's `open --raw $req | lines` wakes on), ctx
/// replies ride `rep` — a SECOND fifo, because two readers on one fifo
/// race (the author's outer loop and its inline reply read would fight
/// over frames; measured deadlock). Result frames ride stdout.
enum BgiKind {
    Pipes {
        child: Option<Child>,
        stdin: Option<ChildStdin>,
        reader: Option<BufReader<std::process::ChildStdout>>,
    },
    Fifo {
        child: Option<Child>,
        reader: Option<BufReader<std::process::ChildStdout>>,
        req: PathBuf,
        rep: PathBuf,
        /// The session dir holding both fifos; removed on teardown
        /// (the PTY session-dir precedent).
        dir: PathBuf,
    },
}

/// Session dirs carry the process pid + a sequence so concurrent effectors
/// never collide (same rule as the PTY carrier).
static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl BgiSession {
    /// Spawn and keep (no sandbox, JSON-lines).
    pub fn spawn(argv: &[String], host: Option<&HostBridge>) -> Result<Self> {
        Self::spawn_wrapped(argv, host, &crate::sandbox::SandboxPolicy::None, ChannelEncoding::Json)
    }

    /// Spawn under the sandbox policy (bwrap wraps argv, §header) with
    /// the declared frame codec (ADR-0037 §2). The CBOR shape is the
    /// pipes channel only: the fifo adapter exists FOR nushell (nu has
    /// no CBOR codec in its stdlib — the entrance criterion routes it to
    /// JSON), so `nu` specs are asserted Json (a CBOR-declared nu booth
    /// is a declaration error, not a silent downgrade).
    pub fn spawn_wrapped(
        argv: &[String],
        host: Option<&HostBridge>,
        policy: &crate::sandbox::SandboxPolicy,
        encoding: ChannelEncoding,
    ) -> Result<Self> {
        // The nushell adapter shape: `["nu", "<author.nu>"]` — the spawn
        // spec's argv head selects the channel, the protocol stays ONE.
        if argv.first().map(|s| s.as_str()) == Some("nu") {
            let author = argv
                .get(1)
                .filter(|_| argv.len() == 2)
                .ok_or_else(|| anyhow!("bgi/nu adapter: spawn spec must be exactly [nu <author.nu>]"))?;
            anyhow::ensure!(
                encoding == ChannelEncoding::Json,
                "bgi/nu adapter: nushell speaks JSON-lines only (no CBOR codec in its stdlib) — declare encoding json"
            );
            return Self::spawn_fifo(Path::new(author), host, policy);
        }
        let (child, stdin, reader) = spawn_child(argv, policy, encoding)?;
        Ok(Self {
            host: host.cloned().unwrap_or_default(),
            kind: BgiKind::Pipes {
                child: Some(child),
                stdin: Some(stdin),
                reader: Some(reader),
            },
            next_id: 0,
            encoding,
        })
    }

    /// The fifo-backed spawn (nushell): session dir + two fifos, child =
    /// `nu --no-config-file <author> <req> <rep>` with stdout piped (the
    /// result frames); stderr inherits — diagnostics, never frames.
    fn spawn_fifo(
        author: &Path,
        host: Option<&HostBridge>,
        policy: &crate::sandbox::SandboxPolicy,
    ) -> Result<Self> {
        if !author.exists() {
            anyhow::bail!("bgi/nu adapter: author script {} not found", author.display());
        }
        // Sandbox dirs follow the PTY carrier's precedent: under a
        // Bubblewrap policy /tmp is tmpfs-mounted (files written outside
        // vanish), so the session dir lives under the policy's cwd
        // (bind-mounted, visible to the child).
        let id = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = match policy {
            crate::sandbox::SandboxPolicy::None => {
                std::env::temp_dir().join(format!("effector-bgi-nu-{}-{id}", std::process::id()))
            }
            crate::sandbox::SandboxPolicy::Bubblewrap { cwd, .. } => {
                Path::new(cwd).join(format!("effector-bgi-nu-{}-{id}", std::process::id()))
            }
        };
        std::fs::create_dir_all(&dir)?;
        let req = dir.join("req");
        let rep = dir.join("rep");
        for fifo in [&req, &rep] {
            // Unlink a same-named node first: mkfifo fails on leftovers.
            let _ = std::fs::remove_file(fifo);
            let c = CString::new(fifo.to_string_lossy().as_bytes())
                .map_err(|_| anyhow!("bgi/nu adapter: fifo path has interior NUL"))?;
            if unsafe { libc::mkfifo(c.as_ptr(), 0o600) } != 0 {
                anyhow::bail!(
                    "bgi/nu adapter: mkfifo {}: {}",
                    fifo.display(),
                    std::io::Error::last_os_error()
                );
            }
        }
        let (child, stdin, reader) = spawn_child(
            &[
                "nu".to_string(),
                "--no-config-file".to_string(),
                author.display().to_string(),
                req.display().to_string(),
                rep.display().to_string(),
            ],
            policy,
            ChannelEncoding::Json,
        )?;
        // The fifos replaced stdin as the channels; drop the pipe writer
        // (nu never reads it — leaving it open only delays child exit).
        drop(stdin);
        Ok(Self {
            host: host.cloned().unwrap_or_default(),
            kind: BgiKind::Fifo {
                child: Some(child),
                reader: Some(reader),
                req,
                rep,
                dir,
            },
            next_id: 0,
            // The fifo shape is the nu adapter — JSON by construction
            // (asserted at the spawn entry).
            encoding: ChannelEncoding::Json,
        })
    }

    /// Liveness for the registry sweep (Sessions::sweep_dead): a child
    /// that exited is a broken cache entry — evict so the next call
    /// cold-starts.
    ///
    /// NOT `Child::try_wait`: without a reaping parent it leaves the
    /// exited child as a zombie whose pid never resolves, so a crashed
    /// child would read alive forever. WNOHANG waitpid reaps directly —
    /// 0 still-alive, negative ESRCH (already reaped), other = exited.
    pub fn is_alive(&mut self) -> bool {
        let child = match &mut self.kind {
            BgiKind::Pipes { child, .. } | BgiKind::Fifo { child, .. } => child.as_mut(),
        };
        match child {
            None => false,
            Some(child) => {
                let pid = libc::pid_t::try_from(child.id()).unwrap_or(-1);
                let res = unsafe { libc::waitpid(pid, std::ptr::null_mut(), libc::WNOHANG) };
                res == 0
            }
        }
    }

    /// The session dir for the fifo shape (test surface: teardown must
    /// leave no fifo litter). Pipes sessions have none.
    pub fn session_dir(&self) -> Option<&Path> {
        match &self.kind {
            BgiKind::Pipes { .. } => None,
            BgiKind::Fifo { dir, .. } => Some(dir),
        }
    }

    /// The child pid (test surface: crash-the-child-behind-the-registry
    /// scenarios need to signal it externally).
    pub fn child_pid(&self) -> Option<u32> {
        match &self.kind {
            BgiKind::Pipes { child, .. } | BgiKind::Fifo { child, .. } => {
                child.as_ref().map(|c| c.id())
            }
        }
    }

    /// One request frame in, one result out, servicing typed host frames
    /// in between (the child blocks on its ctx call; we answer and keep
    /// reading until the result arrives). The frame codec is the session's
    /// declared encoding (ADR-0037 §2, dual-protocol): JSON frames are
    /// text lines, CBOR frames are self-delimited documents on the byte
    /// stream — the same serde_json Value shape under both codecs (only
    /// the codec changes; a CBOR frame carries no line terminator).
    fn exchange(&mut self, mut frame: Value) -> Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        frame["id"] = Value::from(id);
        self.send(&frame)?;
        loop {
            let msg: Value = match self.encoding {
                ChannelEncoding::Json => {
                    let mut line = String::new();
                    let reader = match &mut self.kind {
                        BgiKind::Pipes { reader, .. } | BgiKind::Fifo { reader, .. } => reader
                            .as_mut()
                            .ok_or_else(|| anyhow!("bgi carrier: no stdout"))?,
                    };
                    let n = reader
                        .read_line(&mut line)
                        .map_err(|e| anyhow!("bgi carrier: read: {e}"))?;
                    if n == 0 {
                        self.teardown();
                        return Err(anyhow!("bgi carrier: child closed stdout (exited or crashed)"));
                    }
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    serde_json::from_str(trimmed)
                        .map_err(|e| anyhow!("bgi carrier: malformed frame {trimmed}: {e}"))?
                }
                ChannelEncoding::Cbor => {
                    // Self-delimited CBOR reading: decode_one_value from a
                    // byte reader that blocks per byte. ciborium reports
                    // an incomplete value as EOF on the reader, so the
                    // natural read blocks until the document completes.
                    let rd: &mut dyn Read = match &mut self.kind {
                        BgiKind::Pipes { reader, .. } | BgiKind::Fifo { reader, .. } => reader
                            .as_mut()
                            .ok_or_else(|| anyhow!("bgi carrier: no stdout"))?,
                    };
                    ciborium::de::from_reader(rd)
                        .map_err(|e| anyhow!("bgi carrier: malformed cbor frame: {e}"))?
                }
            };
            if let Some(host) = msg.get("host") {
                // ONE ctx round trip: the typed frame discriminates at the
                // frame-structure layer (ADR-0037 §2) — an unknown type /
                // verb / shape is a decode failure, an error value back
                // into the child (no silent unknown-op table miss). The
                // store payload is never parsed here (schema-blind).
                // Exception: `emit` is fire-and-forget by contract
                // (ADR-0043 §5) — it rides the same dispatch but answers
                // NOTHING; a publish failure lands on stderr, never as an
                // error value into a guest that stopped listening.
                let typed = serde_json::from_value::<TypedHostFrame>(host.clone());
                let (name, arg, expects_reply) = match typed {
                    Ok(typed) => typed.dispatch(),
                    Err(e) => {
                        let reply =
                            serde_json::json!({"host_reply": {"error": format!("bgi carrier: bad host frame: {e}")}});
                        self.send_reply(&reply)?;
                        continue;
                    }
                };
                let outcome = match self.host.functions.get(name) {
                    Some(f) => (f)(arg),
                    None => Err(anyhow!("bgi carrier: host fn '{name}' not bridged")),
                };
                if !expects_reply {
                    if let Err(e) = outcome {
                        eprintln!("[bgi] ctx.{name} (fire-and-forget) failed: {e}");
                    }
                    continue;
                }
                let reply = match outcome {
                    Ok(v) => serde_json::json!({"host_reply": {"ok": v}}),
                    Err(e) => serde_json::json!({"host_reply": {"error": e.to_string()}}),
                };
                self.send_reply(&reply)?;
                continue;
            }
            if let Some(result) = msg.get("result") {
                return Ok(result.clone());
            }
            return Err(anyhow!("bgi carrier: unexpected frame {msg}"));
        }
    }

    /// One request frame onto the request channel. JSON: text line, pipes
    /// append + flush (the child's line reader wakes per newline), fifo
    /// write + CLOSE (the batch EOF is what wakes the child's
    /// `open --raw $req`). CBOR (pipes only — the fifo shape is the nu
    /// adapter, JSON by construction): encode the self-delimited document
    /// and flush; no newline, no length prefix.
    fn send(&mut self, frame: &Value) -> Result<()> {
        match &mut self.kind {
            BgiKind::Pipes { stdin, .. } => {
                let stdin = stdin
                    .as_mut()
                    .ok_or_else(|| anyhow!("bgi carrier: stdin closed"))?;
                match self.encoding {
                    ChannelEncoding::Json => {
                        writeln!(stdin, "{frame}")
                            .map_err(|e| anyhow!("bgi carrier: write request: {e}"))?;
                    }
                    ChannelEncoding::Cbor => {
                        ciborium::ser::into_writer(frame, &mut *stdin)
                            .map_err(|e| anyhow!("bgi carrier: write cbor request: {e}"))?;
                    }
                }
                stdin.flush()?;
                Ok(())
            }
            BgiKind::Fifo { req, child, .. } => {
                fifo_write(req, &frame.to_string()).map_err(|e| {
                    let alive = child
                        .as_mut()
                        .map(|c| c.try_wait().ok().flatten().is_none())
                        .unwrap_or(false);
                    anyhow!(
                        "bgi/nu adapter: write request {} (child alive: {alive}): {e}",
                        req.display()
                    )
                })
            }
        }
    }

    /// One ctx reply onto the reply channel (same codec and shape rules
    /// as `send`; the pipes reply in-band on stdin, the fifos use the
    /// second channel — JSON only there by construction).
    fn send_reply(&mut self, frame: &Value) -> Result<()> {
        match &mut self.kind {
            BgiKind::Pipes { stdin, .. } => {
                let stdin = stdin
                    .as_mut()
                    .ok_or_else(|| anyhow!("bgi carrier: stdin closed"))?;
                match self.encoding {
                    ChannelEncoding::Json => {
                        writeln!(stdin, "{frame}")
                            .map_err(|e| anyhow!("bgi carrier: write host_reply: {e}"))?;
                    }
                    ChannelEncoding::Cbor => {
                        ciborium::ser::into_writer(frame, &mut *stdin)
                            .map_err(|e| anyhow!("bgi carrier: write cbor host_reply: {e}"))?;
                    }
                }
                stdin.flush()?;
                Ok(())
            }
            BgiKind::Fifo { rep, .. } => {
                fifo_write(rep, &frame.to_string()).map_err(|e| {
                    anyhow!("bgi/nu adapter: write host_reply {}: {e}", rep.display())
                })
            }
        }
    }

    /// End the child: Pipes drop stdin (the loop's EOF), Fifo unlink
    /// (the blocked `open` errors and the loop exits); reap with a kill
    /// guard either way, then remove the session dir.
    fn teardown(&mut self) {
        match &mut self.kind {
            BgiKind::Pipes { child, stdin, reader } => {
                *stdin = None;
                if let Some(mut child) = child.take() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                *reader = None;
            }
            BgiKind::Fifo { child, reader, dir, .. } => {
                // Killing first is the reliable exit: the child blocks in
                // `open --raw` and an unlink race (open before unlink)
                // can leave it waiting on a dead inode.
                if let Some(mut child) = child.take() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                *reader = None;
                let _ = std::fs::remove_dir_all(dir);
            }
        }
    }
}

/// Write one line to a fifo with a fresh writer fd, close (EOF) after —
/// the per-batch writer-close discipline the child's read wakes on.
fn fifo_write(path: &Path, line: &str) -> std::io::Result<()> {
    let c = CString::new(path.to_string_lossy().as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "interior NUL"))?;
    let fd = unsafe { libc::open(c.as_ptr(), libc::O_WRONLY) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let bytes = format!("{line}\n");
    let mut written = 0usize;
    let res = loop {
        let n = unsafe {
            libc::write(
                fd,
                bytes.as_ptr().add(written) as *const libc::c_void,
                bytes.len() - written,
            )
        };
        if n < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            break Err(e);
        }
        written += n as usize;
        if written >= bytes.len() {
            break Ok(());
        }
    };
    unsafe { libc::close(fd) };
    res
}

impl ResidentSession for BgiSession {
    /// The source was consumed by spawn (the argv); the child's module
    /// state is its own.
    fn load(&mut self, _source: &str) -> Result<()> {
        Ok(())
    }

    fn call(&mut self, handler: &str, args: &Value) -> Result<Value> {
        self.exchange(serde_json::json!({
            "kind": "call", "event": handler, "args": args,
        }))
    }

    /// ADR-0034 over the bgi seam, unified envelope (ADR-0036): the
    /// stream verbs are request frames — the child keeps whatever guard
    /// state or generator its language offers — and the child's reply is
    /// envelope-validated here: a plain Start reply (no `done`) wraps to
    /// `{done: true, value: <reply>}` at the carrier; mid-stream rounds
    /// must write the envelope. The seam op set is FROZEN (ADR-0037 —
    /// this transitional JSON document retires with CBOR).
    fn iterate(&mut self, op: StreamOp) -> Result<Value> {
        let (kind, event, op_tag, args, stream_id) = match &op {
            StreamOp::Start { stream_id, handler, args } => {
                ("iterate_start", handler, "start", args, stream_id)
            }
            StreamOp::Next { stream_id, handler, args } => {
                ("iterate_next", handler, "next", args, stream_id)
            }
            StreamOp::Dispose { stream_id, handler, args } => {
                ("iterate_dispose", handler, "dispose", args, stream_id)
            }
        };
        let fresh = matches!(op, StreamOp::Start { .. });
        let reply = self.exchange(serde_json::json!({
            "kind": kind, "event": event, "op": op_tag,
            "args": args, "stream_id": stream_id,
        }))?;
        match &op {
            // Dispose has no envelope reply (the routing entry already
            // left; the answer is success).
            StreamOp::Dispose { .. } => Ok(reply),
            // Start AND Next are envelope rounds: Start's plain reply
            // wraps to terminal, Next's missing `done` is an error.
            _ => super::session::validate_envelope(reply, fresh, event),
        }
    }

    fn as_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl Drop for BgiSession {
    fn drop(&mut self) {
        self.teardown();
    }
}

// -------------------------------------------------------------- exec --

/// The bare one-shot carrier: NO ResidentSession, NO protocol — the cgi
/// shape. Spawn, feed the whole request as one JSON document on stdin,
/// close, read stdout to EOF as the result. Nothing survives the call
/// (php-fpm semantics, on purpose: the SKILL downgrade; nushell takes
/// this shape bare — no loop needed — or bgi via `BgiKind::Fifo`).
pub struct ExecOneShot {
    argv: Vec<String>,
    policy: crate::sandbox::SandboxPolicy,
    encoding: ChannelEncoding,
}

impl ExecOneShot {
    pub fn new(
        argv: &[String],
        policy: &crate::sandbox::SandboxPolicy,
        encoding: ChannelEncoding,
    ) -> Self {
        Self { argv: argv.to_vec(), policy: policy.clone(), encoding }
    }
    /// One invocation = one process = one document in, one document out
    /// (the codec is the declaration; the request SHAPE is protocol-free
    /// — `{handler, args}` with no host seam either way).
    pub fn run(&self, handler: &str, args: &Value) -> Result<Value> {
        let (mut child, mut stdin, mut stdout) =
            spawn_child(&self.argv, &self.policy, self.encoding)?;
        let request = serde_json::json!({ "handler": handler, "args": args });
        // Write the whole request, then CLOSE — EOF is the script's cue
        // to run (and nushell's `open /dev/stdin` shape needs exactly
        // this: it delivers at writer-EOF).
        match self.encoding {
            ChannelEncoding::Json => {
                stdin
                    .write_all(request.to_string().as_bytes())
                    .map_err(|e| anyhow!("exec carrier: write request: {e}"))?;
            }
            ChannelEncoding::Cbor => ciborium::ser::into_writer(&request, &mut stdin)
                .map_err(|e| anyhow!("exec carrier: write cbor request: {e}"))?,
        }
        stdin.flush()?;
        // Close the writer NOW — the child sees EOF and runs (the
        // original block-scope discipline, explicit drop keeps it).
        drop(stdin);
        let mut out = Vec::new();
        stdout
            .read_to_end(&mut out)
            .map_err(|e| anyhow!("exec carrier: read stdout: {e}"))?;
        let status = child
            .wait()
            .map_err(|e| anyhow!("exec carrier: wait: {e}"))?;
        match self.encoding {
            ChannelEncoding::Json => {
                let out = String::from_utf8_lossy(&out);
                let trimmed = out.trim();
                if trimmed.is_empty() {
                    anyhow::bail!(
                        "exec carrier: empty stdout (exit {:?}) — a one-shot script must print its result JSON",
                        status.code()
                    );
                }
                serde_json::from_str(trimmed)
                    .map_err(|e| anyhow!("exec carrier: stdout is not one JSON document: {e}"))
            }
            ChannelEncoding::Cbor => {
                if out.is_empty() {
                    anyhow::bail!(
                        "exec carrier: empty stdout (exit {:?}) — a one-shot script must write its result document",
                        status.code()
                    );
                }
                ciborium::de::from_reader(out.as_slice())
                    .map_err(|e| anyhow!("exec carrier: stdout is not one CBOR document: {e}"))
            }
        }
    }
}

/// The session-seam wrapper so the dispatch machinery (aura run_job, the
/// remote carrier path, the introspection throwaway) keeps ONE shape:
/// this parks no process between calls — every `call` spawns, feeds,
/// reaps. `iterate` is an error value on principle: a one-shot booth has
/// no residency to hold a stream (same ruling as Rust closure bodies,
/// ADR-0034 — the cgi lineage is stateless by definition, not by
/// omission). Sweep ignores these slots (not a BgiSession: nothing can
/// die that was never promised to live).
pub struct OneShotSession {
    inner: ExecOneShot,
}

impl OneShotSession {
    pub fn new(
        argv: &[String],
        policy: &crate::sandbox::SandboxPolicy,
        encoding: ChannelEncoding,
    ) -> Self {
        Self { inner: ExecOneShot::new(argv, policy, encoding) }
    }
}

impl ResidentSession for OneShotSession {
    /// Nothing to load — the spawn spec is the whole program.
    fn load(&mut self, _source: &str) -> Result<()> {
        Ok(())
    }

    fn call(&mut self, handler: &str, args: &Value) -> Result<Value> {
        self.inner.run(handler, args)
    }

    /// Unified seam (ADR-0036): a Start runs the one-shot and the
    /// carrier wraps its bare stdout into `{done: true, value}` — the
    /// invoke shape (the script stays protocol-free, ADR-0035 §4: the
    /// exec form has no envelope to write). Next/Dispose stay named
    /// errors: a one-shot booth has no residency to hold a stream
    /// (same ruling as Rust closure bodies, ADR-0034 — the cgi lineage
    /// is stateless by definition, not by omission).
    fn iterate(&mut self, op: StreamOp) -> Result<Value> {
        match op {
            StreamOp::Start { handler, args, .. } => {
                let value = self.inner.run(&handler, &args)?;
                Ok(serde_json::json!({ "done": true, "value": value }))
            }
            StreamOp::Next { .. } | StreamOp::Dispose { .. } => Err(anyhow!(
                "exec carrier: one-shot booths carry no stream state (ADR-0035 — \
                 the cgi shape is stateless by definition; use bgi for residency) \
                 — no stream is started"
            )),
        }
    }

    fn as_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

// -------------------------------------------------------------- spawn --

/// Fork/exec argv[0] (or the bwrap wrapper) with piped stdio; stderr is
/// inherited (the child's diagnostics reach the effector's log, never the
/// result channel). The declared frame codec crosses as the
/// `BGI_ENCODING` env ("json" | "cbor") — the child-side protocol library
/// branches on it (an env, not an appended argv: the fifo shape passes
/// exactly [req rep] and a generated argument would break `def main`).
fn spawn_child(
    argv: &[String],
    policy: &crate::sandbox::SandboxPolicy,
    encoding: ChannelEncoding,
) -> Result<(
    Child,
    ChildStdin,
    BufReader<std::process::ChildStdout>,
)> {
    let (head, rest) = argv
        .split_first()
        .ok_or_else(|| anyhow!("exec/bgi carrier: empty spawn spec"))?;
    let mut cmd = match policy {
        crate::sandbox::SandboxPolicy::None => {
            let mut c = Command::new(head);
            c.args(rest);
            c
        }
        crate::sandbox::SandboxPolicy::Bubblewrap { allow_write, deny_read, cwd, .. } => {
            let quoted = argv
                .iter()
                .map(|a| format!("'{}'", a.replace('\'', "'\\''")))
                .collect::<Vec<_>>()
                .join(" ");
            let config = sandbox_runtime::config::SandboxRuntimeConfig {
                filesystem: sandbox_runtime::config::FilesystemConfig {
                    allow_write: allow_write.clone(),
                    deny_read: deny_read.clone(),
                    ..Default::default()
                },
                ..Default::default()
            };
            let (wrapped, _) = sandbox_runtime::sandbox::linux::generate_bwrap_command(
                &quoted,
                &config,
                std::path::Path::new(cwd),
                None,
                None,
                0,
                0,
                Some("/bin/bash"),
            )
            .map_err(|e| anyhow!("exec/bgi carrier: bwrap command: {e}"))?;
            let mut c = Command::new("bash");
            c.arg("-c").arg(wrapped);
            c
        }
    };
    // bwrap inherits the parent environment (--clearenv is not part of
    // the generated command), so the codec crosses the jail edge either way.
    cmd.env(
        "BGI_ENCODING",
        match encoding {
            ChannelEncoding::Json => "json",
            ChannelEncoding::Cbor => "cbor",
        },
    );
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| anyhow!("exec/bgi carrier: spawn {head}: {e}"))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("exec/bgi carrier: no stdin"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("exec/bgi carrier: no stdout"))?;
    Ok((child, stdin, BufReader::new(stdout)))
}
