//! Induction + exec carriers (ADR-0035): out-of-process booths in two shapes
//! that do NOT share a protocol — the fcgi/cgi split, named precisely.
//!
//! **induction (resident, framed)**: the child lives as long as the residency
//! and speaks the line protocol — request
//! frames in, result frames out, ctx round trips inline (host frames the
//! parent answers). The author's code runs a loop: its own, or the
//! nushell fifo adapter (the "fcgi-adapts-cgi" move — see `CarrierShape`).
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
//! child learns the encoding through the `INDUCTION_ENCODING` env var
//! ("json" | "cbor"; absent = json) — an env, not an appended argv:
//! the fifo shape passes exactly [req rep] and a runtime-generated
//! argument would break the author's `def main` arity.
//! Several calls may be in flight on one child: every request carries an id,
//! results come back by that id, and a ctx frame names the request it serves
//! (`host.call`) so the answer comes from the right call's bridge. The reader
//! is a thread for exactly that reason. A booth that cannot be re-entered says
//! so in its declaration and the realm never hands it two calls at once. The source string IS the argv (whitespace-split — the
//! spawn spec; remote content-addressed binaries are a recorded
//! residual). bwrap policy wraps the spawn (mount policy before exec,
//! the child's capability surface = the channels + jail).
//!
//! The dispatch contract (user ruling 2026-09-29): the event frame
//! enters the child's SINGLE entry point (induction: the loop reads frames and
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

use super::executor::{Executor, StreamOp};
use super::HostBridge;
use anyhow::{anyhow, Result};
use induction_protocol::{Arm, CallFrame, Encoding, FrameKind, IterateKind, Reply, ResultFrame};
use serde_json::Value;
use std::collections::HashMap;
use std::ffi::CString;
use std::io::{BufRead, BufReader, Read, Write};
use std::sync::{mpsc, Arc, Mutex};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};

// ----------------------------------------------------------- frames --

// The frame vocabulary is NOT defined here (ADR-0001 §Consequences): a
// request is `induction_protocol::CallFrame`, the child's ctx calls are
// `HostFrame`/`Arm`, this side's answers are `Reply`, and the child's answer
// is `ResultFrame`. One vocabulary home for every host of the protocol —
// which is the whole point of the extraction (three spellings produced drift
// no single side could see). The carrier's own job is the CHANNEL: spawn,
// codec, the correlation counter, and routing an arm onto the bridge's
// named-function table (the arm names ARE the bridge keys — the dotted ctx
// method paths, ADR-0043).

// ---------------------------------------------------------- induction --

/// One resident induction executor: the framed child, in one of two shapes.
pub struct InductionExecutor {
    /// The resident child and everything shared with the reader thread.
    shared: Arc<Shared>,
    /// Owner-side handles: the process (kill/reap), the fifo executor dir, the
    /// reader thread. Nothing a call needs — a call only touches `shared`.
    child: Option<Child>,
    dir: Option<PathBuf>,
    reader: Option<std::thread::JoinHandle<()>>,
}

/// What a call shares with the reader thread: the write side of the channel, the
/// correlation counter, the frames waiting for answers, and the bridges that
/// answer ctx frames.
///
/// The reader is a THREAD, not the call's own loop, because one process may serve
/// several calls at once (a concurrent guest: induction pairs replies by request
/// id and runs each handler on its own thread). A reader owned by the call would
/// make the process serial no matter what the guest could do.
struct Shared {
    /// Write side: stdin for the pipe shape, the two fifos for the nu adapter.
    /// Held for one frame's write, never across a call.
    out: Mutex<Out>,
    encoding: Encoding,
    next_id: std::sync::atomic::AtomicU64,
    /// Requests waiting for their result, by request id.
    pending: Mutex<HashMap<u64, mpsc::Sender<Result<Value>>>>,
    /// The enclosing call's bridge, by request id — a concurrent guest's ctx
    /// frame carries `host.call` (the request it serves), so the answer comes
    /// from the right call's host functions.
    bridges: Mutex<HashMap<u64, HostBridge>>,
    /// The bridge armed by `set_host`: the SERIAL answer, and the fallback for a
    /// guest that does not label its ctx frames (pre-`host.call` wire form).
    armed: Mutex<HostBridge>,
    /// Why the channel died. Set once; every waiter present and future reads it
    /// instead of parking forever on a child that will never answer.
    dead: Mutex<Option<String>>,
}

/// The write side of the channel. The request channel and the reply channel are
/// the same fd on the pipe shape (frames are self-describing) and two fifos on
/// the nu adapter — two readers on one fifo race, measured (ADR-0035 §8).
enum Out {
    Pipes(ChildStdin),
    Fifo { req: PathBuf, rep: PathBuf },
    /// Torn down: the writer is gone, so every further frame is that error
    /// rather than a guess at a half-closed channel.
    Closed,
}

impl Shared {
    fn dead_reason(&self) -> Option<String> {
        self.dead
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// The channel is unusable: one reason for every waiter, and no new call
    /// starts against a corpse (the caller's own error, not a hang).
    fn fail(&self, why: String) {
        let mut dead = self
            .dead
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if dead.is_some() {
            return;
        }
        *dead = Some(why.clone());
        drop(dead);
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (_, tx) in pending.drain() {
            let _ = tx.send(Err(anyhow!("{why}")));
        }
    }

    /// One frame onto the channel (JSON line or self-delimited CBOR document;
    /// the fifo shape is JSON by construction — the nu adapter asserts it).
    fn write_frame(&self, frame: &Value, host_reply: bool) -> Result<()> {
        if let Some(why) = self.dead_reason() {
            return Err(anyhow!("{why}"));
        }
        let mut out = self
            .out
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut *out {
            Out::Pipes(stdin) => {
                match self.encoding {
                    Encoding::Json => writeln!(stdin, "{frame}").map_err(|e| {
                        anyhow!("induction carrier: write frame: {e}")
                    })?,
                    Encoding::Cbor => ciborium::ser::into_writer(frame, &mut *stdin)
                        .map_err(|e| anyhow!("induction carrier: write cbor frame: {e}"))?,
                }
                stdin.flush()?;
                Ok(())
            }
            Out::Closed => Err(anyhow!("induction carrier: executor channel closed")),
            Out::Fifo { req, rep } => {
                let path = if host_reply { rep } else { req };
                let writing = if host_reply { "host_reply" } else { "request" };
                fifo_write(path, &frame.to_string()).map_err(|e| {
                    anyhow!(
                        "induction/nu adapter: write {writing} {}: {e}",
                        path.display()
                    )
                })
            }
        }
    }

    /// One request frame out, one result in — the correlation is the frame's
    /// id, so several of these run at once on one child.
    fn exchange(
        &self,
        kind: FrameKind,
        event: &str,
        args: Value,
        stream_id: Option<String>,
        host: Option<&HostBridge>,
    ) -> Result<Value> {
        // The carrier owns the frame's identity (its own counter); the typed
        // SHAPE comes from the protocol crate — including the `op` tag the
        // iterate kinds write beside `kind`, which is that type's business.
        // A frame's kind and its stream id are one decision, not two: an
        // iterate kind without an id would silently ride out as a `call`, and
        // a `call` with an id as a stream frame. Both are this carrier's own
        // bug, so they return rather than reach the child.
        let frame_id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let frame = match (IterateKind::try_from(kind), stream_id) {
            (Ok(kind), Some(stream)) => CallFrame::iterate(frame_id, kind, event, args, stream),
            (Err(_), None) => CallFrame::call(frame_id, event, args),
            (Ok(_), None) | (Err(_), Some(_)) => {
                return Err(anyhow!(
                    "induction carrier: frame kind `{}` and its stream id disagree",
                    kind.as_str()
                ));
            }
        };
        let (tx, rx) = mpsc::channel();
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(frame_id, tx);
        if let Some(host) = host {
            self.bridges
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(frame_id, host.clone());
        }
        let written = self
            .write_frame(&frame.to_value(), false)
            .map(|()| rx.recv());
        // The entries leave whether the frame went out, the child answered, or
        // the send failed: nothing accumulates on a channel that is done.
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&frame_id);
        self.bridges
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&frame_id);
        match written {
            Err(e) => Err(e),
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(anyhow!(
                "induction carrier: the child never answered request {frame_id} (channel closed)"
            )),
        }
    }

    /// The bridge that answers a ctx frame: the one registered for the request
    /// the frame names, else the armed one (a serial guest, or the pre-`call`
    /// wire form — unambiguous only while one call is in flight, which is
    /// exactly when such a guest sends frames).
    fn bridge_for(&self, call: Option<u64>) -> HostBridge {
        if let Some(call) = call {
            let bridges = self
                .bridges
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(bridge) = bridges.get(&call) {
                return bridge.clone();
            }
        }
        self.armed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

/// The reader: one frame at a time off the child's stdout, routed by id. Results
/// wake their request; ctx frames are answered here (the child is blocked on
/// them, so any other order would deadlock a concurrent guest); anything else
/// ends the channel for everybody.
fn read_loop(shared: Arc<Shared>, mut reader: BufReader<std::process::ChildStdout>) {
    loop {
        let msg: Value = match shared.encoding {
            Encoding::Json => {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) => {
                        shared.fail(
                            "induction carrier: child closed stdout (exited or crashed)".to_string(),
                        );
                        return;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        shared.fail(format!("induction carrier: read: {e}"));
                        return;
                    }
                }
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                match serde_json::from_str(trimmed) {
                    Ok(v) => v,
                    Err(e) => {
                        shared.fail(format!(
                            "induction carrier: malformed frame {trimmed}: {e}"
                        ));
                        return;
                    }
                }
            }
            Encoding::Cbor => {
                // Self-delimited CBOR reading: decode_one_value from a byte
                // reader that blocks per byte. ciborium reports an incomplete
                // value as EOF on the reader, so the read blocks until the
                // document completes.
                match ciborium::de::from_reader(&mut reader) {
                    Ok(v) => v,
                    Err(e) => {
                        shared.fail(format!("induction carrier: malformed cbor frame: {e}"));
                        return;
                    }
                }
            }
        };
        if let Some(result) = ResultFrame::from_value(&msg) {
            let Some(id) = result.id.as_u64() else {
                shared.fail(format!("induction carrier: result without a request id: {msg}"));
                return;
            };
            let waiter = shared
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&id);
            match waiter {
                Some(tx) => {
                    let _ = tx.send(Ok(result.result));
                }
                // A result nobody waits for is a guest answering twice or
                // answering a request that already failed: noise would hide a
                // real protocol break, so it ends the channel.
                None => {
                    shared.fail(format!(
                        "induction carrier: result for unknown request {id}"
                    ));
                    return;
                }
            }
            continue;
        }
        if msg.get("host").is_some() {
            // ONE dispatch through the shared arm core (induction-host
            // `route_frame` — the same dispatcher mudra's host runs): an unknown
            // arm or a malformed shape is a decode failure — an error value back
            // into the child, never a silent unknown-op table miss. The store
            // payload is never parsed here (schema-blind). The bridge's sync fns
            // run to completion inside `route_frame`'s future; block_on polls it
            // from this thread (no io inside — the future cannot actually block
            // on the runtime, it parks only until the sync fn returns).
            let frame = match induction_host::parse_guest_value(&msg) {
                Some(induction_protocol::GuestFrame::Host(f)) => f,
                Some(induction_protocol::GuestFrame::Result(_)) | None => {
                    let reply = Reply::error(format!(
                        "induction carrier: bad host frame: {}",
                        msg["host"]
                    ));
                    // An undecodable arm is answered, not fatal: the guest gets
                    // the error and keeps its executor (the serial rule, kept).
                    if let Err(e) = shared.write_frame(&reply.to_value(), true) {
                        shared.fail(format!("{e}"));
                        return;
                    }
                    continue;
                }
            };
            let bridge = shared.bridge_for(frame.call);
            // The ctx call's own id, echoed on the answer: several handlers of one
            // child may be waiting at once, so "answer the next reply" is not an
            // answer (ADR-0002 — the echo is what lets a concurrent guest pair).
            let host_id = frame.id;
            match block_serve(induction_host::route_frame(
                &BridgeOps { bridge: &bridge },
                induction_protocol::GuestFrame::Host(frame),
            )) {
                induction_host::Routed::Reply(reply) => {
                    let answer = match host_id {
                        Some(id) => reply.to_value_with_id(id),
                        None => reply.to_value(),
                    };
                    if let Err(e) = shared.write_frame(&answer, true) {
                        shared.fail(format!("{e}"));
                        return;
                    }
                }
                // `emit` is fire-and-forget by CONTRACT (ADR-0043 §5): the frame
                // carries no reply — route_frame accepted it, and its failure (if
                // the bridge fn errored) is already logged by the dispatcher,
                // never written as an error value into a guest that stopped
                // listening.
                induction_host::Routed::Accepted | induction_host::Routed::Result(_) => {}
                induction_host::Routed::DecodeFailed => {
                    shared.fail(format!("induction carrier: unexpected frame {msg}"));
                    return;
                }
            }
            continue;
        }
        shared.fail(format!("induction carrier: unexpected frame {msg}"));
        return;
    }
}

/// Executor dirs carry the process pid + a sequence so concurrent effectors
/// never collide (same rule as the PTY carrier).
static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl InductionExecutor {
    /// The concurrent surface: this carrier pairs replies by request id, so one
    /// instance can serve several calls at once (`Concurrency::Unbounded`).
    pub fn concurrent_handle(&self) -> Arc<dyn super::executor::SharedExecutor> {
        self.shared.clone()
    }

    /// Spawn and keep (no sandbox, JSON-lines).
    pub fn spawn(argv: &[String], host: Option<&HostBridge>) -> Result<Self> {
        Self::spawn_wrapped(
            argv,
            host,
            &crate::sandbox::SandboxPolicy::None,
            Encoding::Json,
        )
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
        encoding: Encoding,
    ) -> Result<Self> {
        // The nushell adapter shape: `["nu", "<author.nu>"]` — the spawn
        // spec's argv head selects the channel, the protocol stays ONE.
        if argv.first().map(|s| s.as_str()) == Some("nu") {
            let author = argv.get(1).filter(|_| argv.len() == 2).ok_or_else(|| {
                anyhow!("induction/nu adapter: spawn spec must be exactly [nu <author.nu>]")
            })?;
            anyhow::ensure!(
                encoding == Encoding::Json,
                "induction/nu adapter: nushell speaks JSON-lines only (no CBOR codec in its stdlib) — declare encoding json"
            );
            return Self::spawn_fifo(Path::new(author), host, policy);
        }
        let (child, stdin, reader) = spawn_child(argv, policy, encoding)?;
        Ok(Self::from_parts(
            child,
            Out::Pipes(stdin),
            reader,
            None,
            host,
            encoding,
        ))
    }

    /// Assemble the executor: one `Shared` for the calls and the reader, one
    /// owner for the process.
    fn from_parts(
        child: Child,
        out: Out,
        reader: BufReader<std::process::ChildStdout>,
        dir: Option<PathBuf>,
        host: Option<&HostBridge>,
        encoding: Encoding,
    ) -> Self {
        let shared = Arc::new(Shared {
            out: Mutex::new(out),
            encoding,
            next_id: std::sync::atomic::AtomicU64::new(0),
            pending: Mutex::new(HashMap::new()),
            bridges: Mutex::new(HashMap::new()),
            armed: Mutex::new(host.cloned().unwrap_or_default()),
            dead: Mutex::new(None),
        });
        let reader = {
            let shared = shared.clone();
            std::thread::Builder::new()
                .name("induction-reader".to_string())
                .spawn(move || read_loop(shared, reader))
                .ok()
        };
        Self {
            shared,
            child: Some(child),
            dir,
            reader,
        }
    }

    /// The fifo-backed spawn (nushell): executor dir + two fifos, child =
    /// `nu --no-config-file <author> <req> <rep>` with stdout piped (the
    /// result frames); stderr inherits — diagnostics, never frames.
    fn spawn_fifo(
        author: &Path,
        host: Option<&HostBridge>,
        policy: &crate::sandbox::SandboxPolicy,
    ) -> Result<Self> {
        if !author.exists() {
            anyhow::bail!(
                "induction/nu adapter: author script {} not found",
                author.display()
            );
        }
        // Sandbox dirs follow the PTY carrier's precedent: under a
        // Bubblewrap policy /tmp is tmpfs-mounted (files written outside
        // vanish), so the executor dir lives under the policy's cwd
        // (bind-mounted, visible to the child).
        let id = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = match policy {
            crate::sandbox::SandboxPolicy::None => std::env::temp_dir()
                .join(format!("effector-induction-nu-{}-{id}", std::process::id())),
            crate::sandbox::SandboxPolicy::Bubblewrap { cwd, .. } => {
                Path::new(cwd).join(format!("effector-induction-nu-{}-{id}", std::process::id()))
            }
        };
        std::fs::create_dir_all(&dir)?;
        let req = dir.join("req");
        let rep = dir.join("rep");
        for fifo in [&req, &rep] {
            // Unlink a same-named node first: mkfifo fails on leftovers.
            let _ = std::fs::remove_file(fifo);
            let c = CString::new(fifo.to_string_lossy().as_bytes())
                .map_err(|_| anyhow!("induction/nu adapter: fifo path has interior NUL"))?;
            if unsafe { libc::mkfifo(c.as_ptr(), 0o600) } != 0 {
                anyhow::bail!(
                    "induction/nu adapter: mkfifo {}: {}",
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
            Encoding::Json,
        )?;
        // The fifos replaced stdin as the channels; drop the pipe writer
        // (nu never reads it — leaving it open only delays child exit).
        drop(stdin);
        Ok(Self::from_parts(
            child,
            Out::Fifo { req, rep },
            reader,
            Some(dir),
            host,
            // The fifo shape is the nu adapter — JSON by construction
            // (asserted at the spawn entry).
            Encoding::Json,
        ))
    }

    /// Liveness for the registry sweep (Executors::sweep_dead): a child
    /// that exited is a broken cache entry — evict so the next call
    /// cold-starts.
    ///
    /// NOT `Child::try_wait`: without a reaping parent it leaves the
    /// exited child as a zombie whose pid never resolves, so a crashed
    /// child would read alive forever. WNOHANG waitpid reaps directly —
    /// 0 still-alive, negative ESRCH (already reaped), other = exited.
    pub fn is_alive(&mut self) -> bool {
        if self.shared.dead_reason().is_some() {
            return false;
        }
        match self.child.as_mut() {
            None => false,
            Some(child) => {
                let pid = libc::pid_t::try_from(child.id()).unwrap_or(-1);
                let res = unsafe { libc::waitpid(pid, std::ptr::null_mut(), libc::WNOHANG) };
                res == 0
            }
        }
    }

    /// The executor dir for the fifo shape (test surface: teardown must
    /// leave no fifo litter). Pipes executors have none.
    pub fn executor_dir(&self) -> Option<&Path> {
        self.dir.as_deref()
    }

    /// The child pid (test surface: crash-the-child-behind-the-registry
    /// scenarios need to signal it externally).
    pub fn child_pid(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }

    /// End the child: drop the writer (the loop's EOF), kill and reap, then
    /// remove the executor dir. The reader wakes on the closed stdout and exits
    /// on its own — joining it here would block on a pipe the kernel is still
    /// draining.
    fn teardown(&mut self) {
        {
            let mut out = self
                .shared
                .out
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // Dropping the writer closes the pipe (pipes) — the loop's EOF.
            *out = Out::Closed;
        }
        if let Some(mut child) = self.child.take() {
            // Killing first is the reliable exit: the fifo child blocks in
            // `open --raw` and an unlink race (open before unlink) can leave it
            // waiting on a dead inode.
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(dir) = self.dir.as_ref() {
            let _ = std::fs::remove_dir_all(dir);
        }
        self.reader.take();
        self.shared
            .fail("induction carrier: executor torn down".to_string());
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

impl Executor for InductionExecutor {
    /// The source was consumed by spawn (the argv); the child's module
    /// state is its own.
    fn load(&mut self, _source: &str) -> Result<()> {
        Ok(())
    }

    fn call(&mut self, handler: &str, args: &Value) -> Result<Value> {
        self.shared
            .exchange(FrameKind::Call, handler, args.clone(), None, None)
    }

    /// ADR-0034 over the induction seam, unified envelope (ADR-0036): the
    /// stream verbs are request frames — the child keeps whatever guard
    /// state or generator its language offers — and the child's reply is
    /// envelope-validated here: a plain Start reply (no `done`) wraps to
    /// `{done: true, value: <reply>}` at the carrier; mid-stream rounds
    /// must write the envelope. The seam op set is FROZEN (ADR-0037 —
    /// this transitional JSON document retires with CBOR).
    fn iterate(&mut self, op: StreamOp) -> Result<Value> {
        let (kind, event, args, stream_id, fresh) = decompose(&op);
        let reply = self.shared.exchange(kind, &event, args, stream_id, None)?;
        finish(reply, fresh, &event)
    }

    /// The child's ctx frames ride the bridge of the call that is running
    /// now (the dispatch seam re-arms it on every call — a resident executor
    /// spawned by an earlier call must not answer with that call's id).
    fn set_host(&mut self, host: Option<&HostBridge>) {
        if let Some(host) = host {
            *self
                .shared
                .armed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = host.clone();
        }
    }

    fn as_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    /// This child pairs every reply with its request id and runs each handler
    /// on its own thread, so the instance can serve calls concurrently — the
    /// declaration's `Unbounded` case.
    fn shared(&self) -> Option<Arc<dyn super::executor::SharedExecutor>> {
        Some(self.shared.clone())
    }
}

/// One stream verb sliced into the frame it becomes, and whether it is a fresh
/// round (an envelope the carrier must validate) or a dispose (no envelope).
fn decompose(op: &StreamOp) -> (FrameKind, String, Value, Option<String>, Option<bool>) {
    match op {
        StreamOp::Start {
            stream_id,
            handler,
            args,
        } => (
            FrameKind::IterateStart,
            handler.clone(),
            args.clone(),
            Some(stream_id.clone()),
            Some(true),
        ),
        StreamOp::Next {
            stream_id,
            handler,
            args,
        } => (
            FrameKind::IterateNext,
            handler.clone(),
            args.clone(),
            Some(stream_id.clone()),
            Some(false),
        ),
        StreamOp::Dispose {
            stream_id,
            handler,
            args,
        } => (
            FrameKind::IterateDispose,
            handler.clone(),
            args.clone(),
            Some(stream_id.clone()),
            None,
        ),
    }
}

/// The envelope rule for one round: dispose is bare, Start and Next are rounds
/// (Start's plain reply wraps to terminal, Next's missing `done` is an error).
fn finish(reply: Value, fresh: Option<bool>, event: &str) -> Result<Value> {
    match fresh {
        None => Ok(reply),
        Some(fresh) => super::executor::validate_envelope(reply, fresh, event),
    }
}

impl super::executor::SharedExecutor for Shared {
    fn iterate(&self, op: StreamOp, host: Option<&HostBridge>) -> Result<Value> {
        let (kind, event, args, stream_id, fresh) = decompose(&op);
        let reply = self.exchange(kind, &event, args, stream_id, host)?;
        finish(reply, fresh, &event)
    }

    fn call(&self, handler: &str, args: &Value, host: Option<&HostBridge>) -> Result<Value> {
        self.exchange(FrameKind::Call, handler, args.clone(), None, host)
    }
}

impl Drop for InductionExecutor {
    fn drop(&mut self) {
        self.teardown();
    }
}

// -------------------------------------------------------------- exec --

/// The bare one-shot carrier: NO Executor, NO protocol — the cgi
/// shape. Spawn, feed the whole request as one JSON document on stdin,
/// close, read stdout to EOF as the result. Nothing survives the call
/// (php-fpm semantics, on purpose: the SKILL downgrade; nushell takes
/// this shape bare — no loop needed — or the induction fifo via `CarrierShape::Fifo`).
pub struct ExecOneShot {
    argv: Vec<String>,
    policy: crate::sandbox::SandboxPolicy,
    encoding: Encoding,
}

impl ExecOneShot {
    pub fn new(
        argv: &[String],
        policy: &crate::sandbox::SandboxPolicy,
        encoding: Encoding,
    ) -> Self {
        Self {
            argv: argv.to_vec(),
            policy: policy.clone(),
            encoding,
        }
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
            Encoding::Json => {
                stdin
                    .write_all(request.to_string().as_bytes())
                    .map_err(|e| anyhow!("exec carrier: write request: {e}"))?;
            }
            Encoding::Cbor => ciborium::ser::into_writer(&request, &mut stdin)
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
            Encoding::Json => {
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
            Encoding::Cbor => {
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

/// The executor-seam wrapper so the dispatch machinery (aura run_job, the
/// remote carrier path, the introspection throwaway) keeps ONE shape:
/// this parks no process between calls — every `call` spawns, feeds,
/// reaps. `iterate` is an error value on principle: a one-shot booth has
/// no residency to hold a stream (same ruling as Rust closure bodies,
/// ADR-0034 — the cgi lineage is stateless by definition, not by
/// omission). Sweep ignores these slots (not a InductionExecutor: nothing can
/// die that was never promised to live).
pub struct OneShotExecutor {
    inner: ExecOneShot,
}

impl OneShotExecutor {
    pub fn new(
        argv: &[String],
        policy: &crate::sandbox::SandboxPolicy,
        encoding: Encoding,
    ) -> Self {
        Self {
            inner: ExecOneShot::new(argv, policy, encoding),
        }
    }
}

impl Executor for OneShotExecutor {
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
                 the cgi shape is stateless by definition; use induction for residency) \
                 — no stream is started"
            )),
        }
    }

    fn as_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

// ---------------------------------------------------- bridge adapter --

/// Poll one routing future to completion from this synchronous
/// (spawn_blocking) thread. The routing future holds no io and the bridge
/// fns are synchronous — `block_on` here can never actually block on a
/// runtime resource; it parks only while the future's sync body runs.
/// (`futures_util::executor::block_on` would do, but its `executor` module
/// is not a stable API surface; this is the same five lines.)
fn block_serve<R>(fut: impl std::future::Future<Output = R>) -> R {
    let mut fut = Box::pin(fut);
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    loop {
        match fut.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(v) => return v,
            std::task::Poll::Pending => std::thread::yield_now(),
        }
    }
}

/// The named-function bridge as a `HostOps`: one dispatcher shape for every
/// host of the protocol. The arm name IS the table key, so this is a lookup,
/// nothing more — semantics live in the functions (aura's ctx handles) and
/// the arm names (the protocol's).
struct BridgeOps<'a> {
    bridge: &'a super::HostBridge,
}

impl induction_host::HostOps for BridgeOps<'_> {
    fn invoke<'a>(
        &'a self,
        call: &'a induction_protocol::InvokeCall,
    ) -> induction_host::OpsFuture<'a> {
        // The flat `verb` shape has no bridge fn (verbs are a mudra host's
        // surface); the canonical nested booth shape maps onto the
        // `invoke` table entry the ctx handles were registered under.
        let args = match call {
            induction_protocol::InvokeCall::Booth {
                target_type,
                target_key,
                handler,
                args,
            } => {
                serde_json::json!({
                    "type": target_type, "key": target_key, "handler": handler, "args": args,
                })
            }
            induction_protocol::InvokeCall::Verb { .. } => {
                return Box::pin(async {
                    Err("induction carrier: host fn 'invoke' cannot serve the flat `verb` shape (no verbs on this host)".to_string())
                });
            }
        };
        let f = self.bridge.functions.get("invoke").cloned();
        Box::pin(async move {
            let f =
                f.ok_or_else(|| "induction carrier: host fn 'invoke' not bridged".to_string())?;
            (f)(args).map_err(|e| e.to_string())
        })
    }

    fn emit(&self, event: &str, data: &Value) -> Result<(), String> {
        let f = self
            .bridge
            .functions
            .get("emit")
            .ok_or_else(|| "host fn 'emit' not bridged".to_string())?;
        (f)(serde_json::json!({ "event": event, "data": data }))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    fn store<'a>(&'a self, op: &'a Value) -> induction_host::OpsFuture<'a> {
        // `store` carries its instruction beside `type`, not under `args`
        // (the schema-blind rule) — route_frame hands it over verbatim.
        let f = self.bridge.functions.get("store").cloned();
        Box::pin(async move {
            let f =
                f.ok_or_else(|| "induction carrier: host fn 'store' not bridged".to_string())?;
            (f)(op.clone()).map_err(|e| e.to_string())
        })
    }

    fn iterate<'a>(&'a self, arm: Arm, args: &'a Value) -> induction_host::OpsFuture<'a> {
        let name = arm.as_str();
        let f = self.bridge.functions.get(name).cloned();
        Box::pin(async move {
            let f = f.ok_or_else(|| format!("induction carrier: host fn '{name}' not bridged"))?;
            (f)(args.clone()).map_err(|e| e.to_string())
        })
    }

    fn timer_register<'a>(&'a self, args: &'a Value) -> induction_host::OpsFuture<'a> {
        table_fn(&self.bridge.functions, "timer.register", args)
    }

    fn timer_cancel<'a>(&'a self, args: &'a Value) -> induction_host::OpsFuture<'a> {
        table_fn(&self.bridge.functions, "timer.cancel", args)
    }

    fn queue_depth<'a>(&'a self, args: &'a Value) -> induction_host::OpsFuture<'a> {
        table_fn(&self.bridge.functions, "queue.depth", args)
    }

    fn queue_skip_to_head<'a>(&'a self, args: &'a Value) -> induction_host::OpsFuture<'a> {
        table_fn(&self.bridge.functions, "queue.skip_to_head", args)
    }

    fn schema<'a>(&'a self) -> induction_host::OpsFuture<'a> {
        let f = self.bridge.functions.get("schema").cloned();
        Box::pin(async move {
            let f =
                f.ok_or_else(|| "induction carrier: host fn 'schema' not bridged".to_string())?;
            (f)(Value::Null).map_err(|e| e.to_string())
        })
    }
}

/// One table lookup + call, the shape every non-invoke arm shares.
fn table_fn<'a>(
    functions: &'a std::collections::BTreeMap<String, super::HostFn>,
    name: &'static str,
    args: &'a Value,
) -> induction_host::OpsFuture<'a> {
    let f = functions.get(name).cloned();
    Box::pin(async move {
        let f = f.ok_or_else(|| format!("induction carrier: host fn '{name}' not bridged"))?;
        (f)(args.clone()).map_err(|e| e.to_string())
    })
}

// -------------------------------------------------------------- spawn --

/// Fork/exec argv[0] (or the bwrap wrapper) with piped stdio; stderr is
/// inherited (the child's diagnostics reach the effector's log, never the
/// result channel). The declared frame codec crosses as the
/// `INDUCTION_ENCODING` env ("json" | "cbor") — the child-side protocol
/// library branches on it (an env, not an appended argv: the fifo shape passes
/// exactly [req rep] and a generated argument would break `def main`). The
/// name is the protocol's own constant, not a literal kept here.
fn spawn_child(
    argv: &[String],
    policy: &crate::sandbox::SandboxPolicy,
    encoding: Encoding,
) -> Result<(Child, ChildStdin, BufReader<std::process::ChildStdout>)> {
    let (head, rest) = argv
        .split_first()
        .ok_or_else(|| anyhow!("exec/induction carrier: empty spawn spec"))?;
    let mut cmd = match policy {
        crate::sandbox::SandboxPolicy::None => {
            let mut c = Command::new(head);
            c.args(rest);
            c
        }
        crate::sandbox::SandboxPolicy::Bubblewrap {
            allow_write,
            deny_read,
            cwd,
            ..
        } => {
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
            .map_err(|e| anyhow!("exec/induction carrier: bwrap command: {e}"))?;
            let mut c = Command::new("bash");
            c.arg("-c").arg(wrapped);
            c
        }
    };
    // bwrap inherits the parent environment (--clearenv is not part of
    // the generated command), so the codec crosses the jail edge either way.
    cmd.env(
        induction_protocol::ENCODING_ENV,
        match encoding {
            Encoding::Json => "json",
            Encoding::Cbor => "cbor",
        },
    );
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| anyhow!("exec/induction carrier: spawn {head}: {e}"))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("exec/induction carrier: no stdin"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("exec/induction carrier: no stdout"))?;
    Ok((child, stdin, BufReader::new(stdout)))
}
