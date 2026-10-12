//! Resident VM sessions: one long-lived execution context per language,
//! loaded once, called per event until evicted. Replaces one-shot execution
//! (fresh VM per call) — cross-call state survives inside the session.

use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::HostBridge;

/// One resident session for an booth instance. Loaded once (source in),
/// then each call addresses a handler by name with JSON args and gets a
/// JSON result. Session state (defined vars, loaded code) persists across
/// calls; eviction = drop.
pub trait ResidentSession: Send {
    /// Load the booth module (defines handlers / declarations).
    fn load(&mut self, source: &str) -> Result<()>;
    /// Invoke one handler by name with parsed JSON args.
    fn call(&mut self, handler: &str, args: &Value) -> Result<Value>;
    /// Drive one stream op (ADR-0034, unified envelope ADR-0036 — THE
    /// job seam: every handler response is an envelope, and `call` is
    /// the raw carrier-internal primitive underneath it, never the
    /// dispatch seam). Envelope-mode carriers (steel,
    /// the process carriers — languages without a host-drivable generator
    /// protocol; the handler writes the envelope explicitly
    /// module and returns the envelope at the ABI edge) delegate to
    /// `envelope_pull`; carriers with a native generator the host can
    /// park and step (python) override and never let the handler see
    /// the wire shape. A plain (non-streaming) Start answers
    /// `{done: true, value: <ret>}` — the carrier wraps, the handler
    /// never writes the envelope sugar for an invoke (0036 §2).
    fn iterate(&mut self, op: StreamOp) -> Result<Value>;
    /// Downcast support (carrier-specific introspection, e.g.
    /// export-list derivation).
    fn as_any(&mut self) -> &mut dyn std::any::Any;
}

/// One stream verb from the control plane to a resident session
/// (ADR-0034). Every op carries the handler name + args: envelope-mode
/// sessions are stateless per call (the handler keeps its own guard
/// state in module globals), and generator-mode sessions use
/// `stream_id` only as the map key — the registry of live streams lives
/// in the control plane either way.
#[derive(Debug, Clone)]
pub enum StreamOp {
    Start { stream_id: String, handler: String, args: Value },
    Next { stream_id: String, handler: String, args: Value },
    Dispose { stream_id: String, handler: String, args: Value },
}

impl StreamOp {
    pub fn stream_id(&self) -> &str {
        match self {
            StreamOp::Start { stream_id, .. }
            | StreamOp::Next { stream_id, .. }
            | StreamOp::Dispose { stream_id, .. } => stream_id,
        }
    }
    pub fn handler(&self) -> &str {
        match self {
            StreamOp::Start { handler, .. }
            | StreamOp::Next { handler, .. }
            | StreamOp::Dispose { handler, .. } => handler,
        }
    }
    pub fn args(&self) -> &Value {
        match self {
            StreamOp::Start { args, .. }
            | StreamOp::Next { args, .. }
            | StreamOp::Dispose { args, .. } => args,
        }
    }
    /// The op tag injected into the handler args (envelope mode).
    pub fn tag(&self) -> &'static str {
        match self {
            StreamOp::Start { .. } => "start",
            StreamOp::Next { .. } => "next",
            StreamOp::Dispose { .. } => "dispose",
        }
    }
}

/// The unified envelope (ADR-0036 §1): one shape for EVERY handler
/// response — a terminal one with an optional `value`, or a
/// non-terminal one with an `item`. `done` is always present and
/// always boolean; the cross-field rules are enforced, not convention:
/// `item` only with `done: false`, `value` only with `done: true`, and
/// a non-terminal round without an `item` is a protocol error (the
/// generator-mode null-start reply is gone — the pull that starts a
/// stream IS the first pull and answers its first envelope).
///
/// `fresh` says whether this is the Start round. The one freedom the
/// ADR grants at Start (§2, "plain returns wrap to terminal envelopes
/// AT THE CARRIER"): a reply with no `done` field is a plain return —
/// wrapped into `{done: true, value: <reply>}`. Mid-stream (Next) a
/// missing `done` is exactly what the positional heuristic used to
/// hide: an error, never a silent terminal.
pub(crate) fn validate_envelope(reply: Value, fresh: bool, handler: &str) -> Result<Value> {
    let done = reply.get("done").and_then(|v| v.as_bool());
    match done {
        Some(true) => {
            if reply.get("item").is_some() {
                anyhow::bail!("envelope with `done: true` must not carry an `item` (ADR-0036)");
            }
            Ok(reply)
        }
        Some(false) => {
            if reply.get("item").is_none() {
                anyhow::bail!(
                    "envelope handler '{handler}': `done: false` must carry an `item` (ADR-0036)"
                );
            }
            if reply.get("value").is_some() {
                anyhow::bail!(
                    "envelope handler '{handler}': `value` is legal only with `done: true` (ADR-0036)"
                );
            }
            Ok(reply)
        }
        None => {
            if fresh {
                // Plain return → terminal envelope (ADR-0036 §2). The
                // injected `iterate` tag is FRAMEWORK vocabulary riding
                // the args — a handler that echoes its args back would
                // hand our own injection right into the result, so it
                // is stripped from the wrapped value (`iterate` is
                // reserved: a handler returning that key mid-envelope
                // rides the object below, never this branch).
                let value = match reply {
                    Value::Object(mut map) => {
                        map.remove("iterate");
                        Value::Object(map)
                    }
                    other => other,
                };
                Ok(serde_json::json!({ "done": true, "value": value }))
            } else if reply.get("error").is_some() {
                // The out-of-process children's failure convention
                // (`{"error": "..."}` with no envelope — the bgi
                // fixtures' shape): a mid-stream failure surfaces as the
                // outer error value (ADR-0012), never a fake envelope.
                let msg = reply.get("error").and_then(|v| v.as_str()).unwrap_or("stream error");
                anyhow::bail!("iterate: producer error: {msg}")
            } else {
                anyhow::bail!(
                    "iterate: envelope handler '{handler}' must return an object with a boolean \
                     `done` field (ADR-0036)"
                )
            }
        }
    }
}

/// The envelope-mode iteration shape (ADR-0034 §1, languages without a
/// host-drivable generator protocol — steel, wasm): the handler
/// is a repeatedly callable function that returns the envelope
/// explicitly. The framework injects `{stream_id, op}` into the args,
/// forwards the call, and validates the reply against the unified
/// envelope. `done: true` is WRITTEN, not derived — the validation
/// narrows the guard-value footgun, it does not eliminate it. A plain
/// (envelope-free) Start reply wraps to a terminal envelope here, at
/// the carrier (ADR-0036 §2).
pub(crate) fn envelope_pull(session: &mut dyn ResidentSession, op: &StreamOp) -> Result<Value> {
    let mut args = op.args().clone();
    let inject = serde_json::json!({
        "stream_id": op.stream_id(),
        "op": op.tag(),
    });
    let injected = match &mut args {
        Value::Object(map) => {
            map.insert("iterate".into(), inject);
            true
        }
        Value::Null => {
            args = Value::Object([("iterate".to_string(), inject)].into_iter().collect());
            true
        }
        // Non-object args cannot carry the injection — the handler is
        // addressed bare. A Start answers the plain/invoke shape; a
        // stream verb cannot be driven without the tag (no envelope
        // vocabulary reaches the handler) — named as an error value.
        _ => false,
    };
    if !injected && !matches!(op, StreamOp::Start { .. }) {
        anyhow::bail!(
            "iterate: envelope-mode stream verbs need object args (the `{{stream_id, op}}` \
             tag rides them) — handler '{}' got {}",
            op.handler(),
            op.args()
        );
    }
    let envelope = session.call(op.handler(), &args)?;
    if matches!(op, StreamOp::Dispose { .. }) {
        return Ok(Value::Null);
    }
    validate_envelope(envelope, matches!(op, StreamOp::Start { .. }), op.handler())
}
/// Per-instance slot: the session plus its own lock. The lock is held only
/// for the duration of one call on THAT instance — other instances proceed
/// concurrently, and host functions that call back into other instances
/// (ctx.invoke) never contend on a global lock.
type Slot = Mutex<Box<dyn ResidentSession>>;

/// Session registry: per booth-instance key, one live session.
#[derive(Default, Clone)]
pub struct Sessions {
    map: Arc<Mutex<HashMap<String, Arc<Slot>>>>,
}

impl Sessions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get or create the resident session for `key` and run `f` on it.
    ///
    /// Locking: the registry lock is taken only for map get/insert; the
    /// per-slot lock serializes calls to the SAME instance (session state
    /// is not thread-safe and calls to one instance must order anyway).
    /// `load` runs at cold start inside the slot lock; later calls reuse
    /// the loaded session.
    pub fn with_session(
        &self,
        key: &str,
        language: &str,
        source: &str,
        host: Option<&HostBridge>,
        sandbox: &crate::sandbox::SandboxPolicy,
        f: impl FnOnce(&mut dyn ResidentSession) -> Result<Value>,
    ) -> Result<Value> {
        self.with_session_encoded(
            key,
            language,
            source,
            host,
            sandbox,
            effector_protocol::ChannelEncoding::Json,
            f,
        )
    }

    /// The declared-encoding form (ADR-0037 §2 — dual-protocol): the
    /// frame codec for the PROCESS carriers (bgi/exec). Embedded carriers
    /// carry no channel — the encoding is inert there; `Json` is the
    /// stdlib-reachable default every older call site rides.
    // The session-spawn parameter set (+codec) is the shape `with_session`
    // already carries; grouping them into a struct would churn every
    // existing call site for no semantic gain.
    #[allow(clippy::too_many_arguments)]
    pub fn with_session_encoded(
        &self,
        key: &str,
        language: &str,
        source: &str,
        host: Option<&HostBridge>,
        sandbox: &crate::sandbox::SandboxPolicy,
        encoding: effector_protocol::ChannelEncoding,
        f: impl FnOnce(&mut dyn ResidentSession) -> Result<Value>,
    ) -> Result<Value> {
        let slot = {
            let mut map = self.map.lock().unwrap();
            match map.get(key) {
                Some(slot) => slot.clone(),
                None => {
                    // Cold start (or after eviction): spawn + load while
                    // holding the registry lock — spawn errors surface as
                    // errors, never panics, and the failed entry is not
                    // cached.
                    let mut session = spawn_session(language, source, host, sandbox, encoding)?;
                    session.load(source)?;
                    let slot = Arc::new(Mutex::new(session));
                    map.insert(key.to_string(), slot.clone());
                    slot
                }
            }
        };
        let mut session = slot.lock().unwrap();
        f(session.as_mut())
    }

    /// Evict (drop) the session — instance idle-expiry or hot replacement.
    /// Waits for an in-flight call on the slot to finish before dropping.
    pub fn evict(&self, key: &str) {
        self.map.lock().unwrap().remove(key);
    }

    /// Evict by predicate (the exec carrier's liveness rule): a slot whose
    /// child DIED is a broken cache entry — remove it so the next call
    /// cold-starts a fresh spawn. Dropping the exec session closes stdin
    /// and reaps; a live child stays parked (idle_ttl eviction owns it —
    /// the child is the residency). Returns the removed keys (diagnostics).
    pub fn sweep_dead(&self) -> Vec<String> {
        let mut map = self.map.lock().unwrap();
        let dead: Vec<String> = map
            .iter()
            .filter_map(|(k, slot)| {
                // try_lock: a slot mid-call is alive by definition (a
                // request is running) — never block the sweep behind one.
                slot.try_lock()
                    .ok()
                    .and_then(|mut s| {
                        let dead = s
                            .as_any()
                            .downcast_mut::<crate::carrier::exec::BgiSession>()
                            .map(|e| !e.is_alive())
                            .unwrap_or(false);
                        dead.then(|| k.clone())
                    })
            })
            .collect();
        for k in &dead {
            map.remove(k);
        }
        dead
    }
}

/// The exec spawn spec: whitespace-separated argv, no quoting layer
/// (paths with spaces are the author's config problem, declared as a
/// plain split — the spawn spec is a list of words by contract).
fn shellish_split(source: &str) -> Vec<String> {
    source.split_whitespace().map(|s| s.to_string()).collect()
}

fn spawn_session(
    language: &str,
    source: &str,
    host: Option<&HostBridge>,
    sandbox: &crate::sandbox::SandboxPolicy,
    encoding: effector_protocol::ChannelEncoding,
) -> Result<Box<dyn ResidentSession>> {
    // `source` is consumed by the wasmtime arm (compile at spawn); the
    // other arms ignore it (the spawn spec is argv) or one or both.
    // `encoding` is consumed by the process carriers only (bgi/exec —
    // ADR-0037 §2); the embedded carriers have no channel.
    let _ = (language, source, host, sandbox, encoding);
    Ok(match language {
        #[cfg(feature = "steel")]
        "steel" => Box::new(super::steel::SteelSession::new(host)?),
        #[cfg(feature = "python")]
        "python" => Box::new(super::python::PythonSession::new(host)?),
        // BGI (ADR-0035, Booth Gateway Interface — the framed resident
        // shape): the source string IS the argv (whitespace-separated,
        // quoted-free — the spawn spec); the child lives as long as the
        // session and runs the BGI loop over the line protocol. A `nu`
        // head selects the two-fifo adapter shape inside the carrier
        // (the nushell PTY carrier retired — one execution shape per
        // language, the nu form is `bgi` with spec `nu <author.nu>`).
        // Under a sandbox policy the whole spawn is bwrap-wrapped (the
        // mount policy runs before exec).
        "bgi" => {
            let argv: Vec<String> = shellish_split(source);
            Box::new(super::exec::BgiSession::spawn_wrapped(&argv, host, sandbox, encoding)?)
        }
        // exec (ADR-0035 — the bare cgi shape, NO protocol): one process
        // per call — the request JSON is written, stdin closed (EOF is
        // the script's cue; nushell's `open /dev/stdin` needs exactly
        // this), stdout read whole as the result. Nothing survives the
        // call: iterate is an error value, no ctx channel exists, no
        // residency to sweep. The SKILL shape (user ruling 2026-09-28).
        "exec" => {
            let argv: Vec<String> = shellish_split(source);
            Box::new(super::exec::OneShotSession::new(&argv, sandbox, encoding))
        }
        // Wasm compiles AT SPAWN (from_source) — its `load` is a no-op;
        // other carriers load lazily inside `load`. Same session shape.
        other => anyhow::bail!("language not resident-carried by this effector build: {other}"),
    })
}

pub type DynSession = Box<dyn ResidentSession>;
