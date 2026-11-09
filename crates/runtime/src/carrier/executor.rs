//! Resident VM executors: one long-lived execution context per language,
//! loaded once, called per event until evicted. Replaces one-shot execution
//! (fresh VM per call) — cross-call state survives inside the executor.

use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::HostBridge;

/// One resident executor for an booth instance. Loaded once (source in),
/// then each call addresses a handler by name with JSON args and gets a
/// JSON result. Executor state (defined vars, loaded code) persists across
/// calls; eviction = drop.
pub trait Executor: Send {
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
    /// Arm this call's stop handle (ADR-0047 stage two), if the carrier can be told
    /// about one at all. The default arms nothing, and `CallControl::interrupt` then
    /// answers `landed: false` — an embedded VM mid-call has no place to receive a
    /// stop, and saying so is the only correct answer.
    fn arm_interrupt(&mut self, _control: &crate::carrier::CallControl) {}

    /// Re-arm the host bridge for the call that is about to run. The bridge
    /// is per-CALL data — it carries the enclosing remote call's id, which
    /// the control plane resolves the ctx scope with — while a resident
    /// executor outlives the call that spawned it. A executor whose bridge is
    /// only the cold start's answers ctx frames with a call id nobody is
    /// waiting on any more ("the enclosing remote call is not in flight"),
    /// so the dispatch seam re-arms it on EVERY call, not only at spawn.
    /// Carriers with no ctx channel keep a bridge that binds once at load
    /// (their host fns come from realm handles, not from the call) and ride
    /// this default.
    fn set_host(&mut self, _host: Option<&HostBridge>) {}
    /// Downcast support (carrier-specific introspection, e.g.
    /// export-list derivation).
    fn as_any(&mut self) -> &mut dyn std::any::Any;
    /// The CONCURRENT surface, where the carrier has one: a executor that pairs
    /// every reply with the request that asked for it can serve several calls at
    /// once, so its instance needs no queue and no second process — the
    /// declaration's `Unbounded` (`BoothType::concurrency`). `None` (the default)
    /// means this executor must be entered one call at a time.
    fn shared(&self) -> Option<Arc<dyn SharedExecutor>> {
        None
    }
}

/// A resident executor that multiplexes: `iterate` may be called from several
/// threads with no lock of the registry's, because the carrier carries each
/// call's identity on the wire and routes the answers back.
pub trait SharedExecutor: Send + Sync {
    /// One stream op, with the call's own bridge (the ctx frames it provokes
    /// belong to THIS call, not to whoever called last) and the call's own stop
    /// handle. Both are per-call facts, which is why they ride the CALL and not a
    /// field on the carrier: an unbounded executor serves many calls at once, and a
    /// handle kept beside it would belong to whoever armed last.
    fn iterate(
        &self,
        op: StreamOp,
        host: Option<&HostBridge>,
        control: Option<&crate::carrier::CallControl>,
    ) -> Result<Value>;
    /// One plain handler call on the same terms — a lifecycle hook rides the
    /// resident executor too, and it must not open a second process beside the
    /// one the calls are using.
    fn call(
        &self,
        handler: &str,
        args: &Value,
        host: Option<&HostBridge>,
        control: Option<&crate::carrier::CallControl>,
    ) -> Result<Value>;
}

/// One stream verb from the control plane to a resident executor
/// (ADR-0034). Every op carries the handler name + args: envelope-mode
/// executors are stateless per call (the handler keeps its own guard
/// state in module globals), and generator-mode executors use
/// `stream_id` only as the map key — the registry of live streams lives
/// in the control plane either way.
#[derive(Debug, Clone)]
pub enum StreamOp {
    Start {
        stream_id: String,
        handler: String,
        args: Value,
    },
    Next {
        stream_id: String,
        handler: String,
        args: Value,
    },
    Dispose {
        stream_id: String,
        handler: String,
        args: Value,
    },
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
                // (`{"error": "..."}` with no envelope — the induction
                // fixtures' shape): a mid-stream failure surfaces as the
                // outer error value (ADR-0012), never a fake envelope.
                let msg = reply
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("stream error");
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
pub(crate) fn envelope_pull(executor: &mut dyn Executor, op: &StreamOp) -> Result<Value> {
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
    let envelope = executor.call(op.handler(), &args)?;
    if matches!(op, StreamOp::Dispose { .. }) {
        return Ok(Value::Null);
    }
    validate_envelope(envelope, matches!(op, StreamOp::Start { .. }), op.handler())
}
/// Per-instance slot: the executor plus its own lock. The lock is held only
/// for the duration of one call on THAT instance — other instances proceed
/// concurrently, and host functions that call back into other instances
/// (ctx.invoke) never contend on a global lock.
type Slot = Mutex<Box<dyn Executor>>;

/// A booth instance's residency: one slot, or a POOL of them when the booth
/// declared that it can serve overlapping calls.
///
/// The capacity is the booth's own declaration (`BoothType::concurrency`), and
/// it is the php-fpm shape: a call takes a free slot, a call that finds every
/// slot busy takes a NEW one while the pool is under capacity, and a call past
/// capacity WAITS on a slot. Capacity 1 is the old rule — one resident executor,
/// calls to the instance in arrival order — and it stays the default, because a
/// booth whose code cannot be re-entered must not be handed two calls at once.
struct Pool {
    capacity: usize,
    slots: Mutex<Vec<Arc<Slot>>>,
    /// The concurrent handle, when the booth declared `Unbounded`: one executor,
    /// entered from many calls at once (the carrier does the pairing).
    shared: Mutex<Option<Arc<dyn SharedExecutor>>>,
    /// The executor OWNER the handle came from: the handle routes frames, but the
    /// process and its reader thread are owned here — dropping this kills the
    /// child, so the pool must hold it for as long as the handle is in use.
    owner: Mutex<Option<Box<dyn Executor>>>,
}

impl Pool {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            slots: Mutex::new(Vec::new()),
            shared: Mutex::new(None),
            owner: Mutex::new(None),
        }
    }

    /// Run `f` in a slot: a free one, else a fresh one under capacity, else the
    /// first one (WAITING — the queue is the declaration's own size).
    ///
    /// The slot is chosen under the pool lock and LOCKED after it is dropped, so
    /// the pool lock is never held across a call: two calls choosing different
    /// slots do not contend on it.
    fn run<R>(
        &self,
        spawn: impl FnOnce() -> Result<Box<dyn Executor>>,
        f: impl FnOnce(&mut dyn Executor) -> Result<R>,
    ) -> Result<R> {
        let mut spawn = Some(spawn);
        let chosen: Arc<Slot> = {
            let mut slots = self
                .slots
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut free = None;
            for arc in slots.iter() {
                // Probe only: the lock is taken for real below, and a probe that
                // lost the race costs a short wait, never a correctness problem.
                if arc.try_lock().is_ok() {
                    free = Some(arc.clone());
                    break;
                }
            }
            match free {
                Some(arc) => arc,
                None if slots.len() < self.capacity => {
                    let spawn = spawn
                        .take()
                        .ok_or_else(|| anyhow::anyhow!("executor: spawn used twice"))?;
                    let arc = Arc::new(Mutex::new(spawn()?));
                    slots.push(arc.clone());
                    arc
                }
                // Every slot is busy and the pool is full: wait for the first.
                None => slots[0].clone(),
            }
        };
        let mut executor = chosen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f(executor.as_mut())
    }
}

/// Executor registry: one entry per booth-instance key, holding its pool.
#[derive(Default, Clone)]
pub struct Executors {
    map: Arc<Mutex<HashMap<String, Arc<Pool>>>>,
}

impl Executors {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get or create the resident executor for `key` and run `f` on it.
    ///
    /// Locking: the registry lock is taken only for map get/insert; a slot's lock
    /// is held for the length of one call. ONE slot is the serial form — calls to
    /// the instance then order, which is what a body that cannot be re-entered
    /// needs (aura ADR-0046, `BoothType::concurrency` = 1). A booth that declares
    /// more gets a POOL and overlapping calls; there is no global lock to fight.
    /// `load` runs at cold start inside the slot lock; later calls reuse the
    /// loaded executor.
    pub fn with_executor(
        &self,
        key: &str,
        language: &str,
        source: &str,
        host: Option<&HostBridge>,
        sandbox: &crate::sandbox::SandboxPolicy,
        f: impl FnOnce(&mut dyn Executor) -> Result<Value>,
    ) -> Result<Value> {
        self.with_executor_encoded(
            key,
            language,
            source,
            host,
            sandbox,
            induction_protocol::Encoding::Json,
            f,
        )
    }

    /// The declared-encoding form (ADR-0037 §2 — dual-protocol): the
    /// frame codec for the PROCESS carriers (induction/exec). Embedded carriers
    /// carry no channel — the encoding is inert there; `Json` is the
    /// stdlib-reachable default every older call site rides.
    // The executor-spawn parameter set (+codec) is the shape `with_executor`
    // already carries; grouping them into a struct would churn every
    // existing call site for no semantic gain.
    #[allow(clippy::too_many_arguments)]
    pub fn with_executor_encoded(
        &self,
        key: &str,
        language: &str,
        source: &str,
        host: Option<&HostBridge>,
        sandbox: &crate::sandbox::SandboxPolicy,
        encoding: induction_protocol::Encoding,
        f: impl FnOnce(&mut dyn Executor) -> Result<Value>,
    ) -> Result<Value> {
        // The serial form: one slot, calls in arrival order. Every caller that
        // has not declared otherwise keeps exactly this behaviour.
        self.with_slot_encoded(key, language, source, host, sandbox, encoding, 1, f)
    }

    /// The UNBOUNDED form (`Concurrency::Unbounded`): one resident executor, no
    /// queue and no cap — every call runs at once, which only a carrier that
    /// pairs replies by request id can do. The booth declares it; the realm then
    /// never makes a call wait for another call's completion.
    ///
    /// A carrier without the concurrent surface is a declaration error, not a
    /// silent downgrade: a booth that promised overlap and cannot overlap would
    /// otherwise queue under a name that says it does not.
    #[allow(clippy::too_many_arguments)]
    pub fn with_shared(
        &self,
        key: &str,
        language: &str,
        source: &str,
        sandbox: &crate::sandbox::SandboxPolicy,
        encoding: induction_protocol::Encoding,
        host: Option<&HostBridge>,
        f: impl FnOnce(&dyn SharedExecutor) -> Result<Value>,
    ) -> Result<Value> {
        let pool = {
            let mut map = self
                .map
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            map.entry(key.to_string())
                .or_insert_with(|| Arc::new(Pool::new(1)))
                .clone()
        };
        let shared = {
            let mut guard = pool
                .shared
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match guard.as_ref() {
                Some(shared) => shared.clone(),
                None => {
                    // Cold start under the handle lock: a second call never sees a
                    // half-loaded executor, and the spawn happens once.
                    let mut executor = spawn_executor(language, source, host, sandbox, encoding)?;
                    executor.load(source)?;
                    executor.set_host(host);
                    let shared = executor.shared().ok_or_else(|| {
                        anyhow::anyhow!(
                            "booth `{key}` declares unbounded concurrency, but its carrier `{language}` has no concurrent surface — declare a queue (`concurrency: 1`) or a pool instead"
                        )
                    })?;
                    // The OWNER stays: the handle routes frames, the owner holds
                    // the process. Dropping it here would kill the child that was
                    // just spawned.
                    *pool
                        .owner
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(executor);
                    *guard = Some(shared.clone());
                    shared
                }
            }
        };
        f(shared.as_ref())
    }

    /// The declared-capacity form: `capacity` slots may run at once for this
    /// instance. The booth states the number (`BoothType::concurrency`) because
    /// only the booth knows whether its body is re-entrant — a Rust binary that
    /// handles each call on its own stack wants several; an interpreter holding
    /// one VM behind a GIL wants one per PROCESS, i.e. still a queue here and a
    /// process pool below.
    ///
    /// Locking: the registry lock is taken only for map get/insert; the chosen
    /// slot's lock is held for the call. Cold start (`load`) runs inside that
    /// slot lock, so a second call never sees a half-loaded executor.
    #[allow(clippy::too_many_arguments)]
    pub fn with_slot_encoded(
        &self,
        key: &str,
        language: &str,
        source: &str,
        host: Option<&HostBridge>,
        sandbox: &crate::sandbox::SandboxPolicy,
        encoding: induction_protocol::Encoding,
        capacity: usize,
        f: impl FnOnce(&mut dyn Executor) -> Result<Value>,
    ) -> Result<Value> {
        let pool = {
            let mut map = self
                .map
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // ONE instance, ONE process: a pool that already runs through its
            // shared executor must not open a slot executor beside it. Reaching
            // here means a caller took the queue path for a booth that declared
            // `unbounded` — a bug in the dispatcher, said plainly.
            if let Some(pool) = map.get(key) {
                if pool
                    .shared
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_some()
                {
                    anyhow::bail!(
                        "instance `{key}` runs through its shared executor (declared unbounded); a queued call would give it a second process — dispatch it on the shared surface"
                    );
                }
            }
            match map.get(key) {
                Some(pool) if pool.capacity >= capacity.max(1) => pool.clone(),
                // A declaration that widened the pool replaces the old one: the
                // executors in it stay alive for their in-flight calls, and the
                // next call lands in a pool that can carry the new number.
                Some(_) => {
                    let pool = Arc::new(Pool::new(capacity));
                    map.insert(key.to_string(), pool.clone());
                    pool
                }
                None => {
                    let pool = Arc::new(Pool::new(capacity));
                    map.insert(key.to_string(), pool.clone());
                    pool
                }
            }
        };
        pool.run(
            || {
                // Cold start (or after eviction): spawn + load BEFORE the slot is
                // published, so a spawn error surfaces as an error and a failed
                // entry is never cached.
                let mut executor = spawn_executor(language, source, host, sandbox, encoding)?;
                executor.load(source)?;
                Ok(executor)
            },
            |executor| {
                // The ctx channel's outbound path is per-call (the bridge carries
                // the enclosing call's id): re-arm it here so a executor spawned by
                // an earlier call does not answer this call's ctx frames with a
                // stale id (residency is the executor, the bridge is the call).
                executor.set_host(host);
                f(executor)
            },
        )
    }

    /// Evict (drop) the executor — instance idle-expiry or hot replacement.
    /// Waits for an in-flight call on the slot to finish before dropping.
    pub fn evict(&self, key: &str) {
        self.map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(key);
    }

    /// Evict by predicate (the exec carrier's liveness rule): a slot whose
    /// child DIED is a broken cache entry — remove it so the next call
    /// cold-starts a fresh spawn. Dropping the exec executor closes stdin
    /// and reaps; a live child stays parked (idle_ttl eviction owns it —
    /// the child is the residency). Returns the removed keys (diagnostics).
    pub fn sweep_dead(&self) -> Vec<String> {
        let mut map = self
            .map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dead: Vec<String> = map
            .iter()
            .filter_map(|(k, pool)| {
                // ONE executor, held as the concurrent owner: its liveness is the
                // child's own. Checked before the slots because an unbounded pool
                // keeps no slots — reading "no slots" as "nothing alive" would bury
                // a healthy child on the next sweep.
                {
                    let mut owner = pool
                        .owner
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if let Some(owner) = owner.as_mut() {
                        let alive = owner
                            .as_any()
                            .downcast_mut::<crate::carrier::exec::InductionExecutor>()
                            .map(crate::carrier::exec::InductionExecutor::is_alive)
                            .unwrap_or(true);
                        return (!alive).then(|| k.clone());
                    }
                }
                let slots = pool
                    .slots
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                // try_lock: a slot mid-call is alive by definition (a
                // request is running) — never block the sweep behind one.
                let any_alive = slots.iter().any(|slot| {
                    slot.try_lock()
                        .map(|mut s| {
                            s.as_any()
                                .downcast_mut::<crate::carrier::exec::InductionExecutor>()
                                .map(crate::carrier::exec::InductionExecutor::is_alive)
                                .unwrap_or(true)
                        })
                        .unwrap_or(true)
                });
                (!any_alive).then(|| k.clone())
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

fn spawn_executor(
    language: &str,
    source: &str,
    host: Option<&HostBridge>,
    sandbox: &crate::sandbox::SandboxPolicy,
    encoding: induction_protocol::Encoding,
) -> Result<Box<dyn Executor>> {
    // `source` is consumed by the wasmtime arm (compile at spawn); the
    // other arms ignore it (the spawn spec is argv) or one or both.
    // `encoding` is consumed by the process carriers only (induction/exec —
    // ADR-0037 §2); the embedded carriers have no channel.
    let _ = (language, source, host, sandbox, encoding);
    Ok(match language {
        #[cfg(feature = "steel")]
        "steel" => Box::new(super::steel::SteelExecutor::new(host)?),
        #[cfg(feature = "python")]
        "python" => Box::new(super::python::PythonExecutor::new(host)?),
        // Induction (ADR-0035; the pipe protocol, ADR-0001): the source string
        // IS the argv (whitespace-separated, quoted-free — the spawn spec);
        // the child lives as long as the executor and runs the framed loop
        // over the line protocol. A `nu` head selects the two-fifo adapter
        // shape inside the carrier (the nushell PTY carrier retired — one
        // execution shape per language, the nu form is `induction` with
        // spec `nu <author.nu>`). Under a sandbox policy the whole spawn is
        // bwrap-wrapped (the mount policy runs before exec).
        "induction" => {
            let argv: Vec<String> = shellish_split(source);
            Box::new(super::exec::InductionExecutor::spawn_wrapped(
                &argv, host, sandbox, encoding,
            )?)
        }
        // exec (ADR-0035 — the bare cgi shape, NO protocol): one process
        // per call — the request JSON is written, stdin closed (EOF is
        // the script's cue; nushell's `open /dev/stdin` needs exactly
        // this), stdout read whole as the result. Nothing survives the
        // call: iterate is an error value, no ctx channel exists, no
        // residency to sweep. The SKILL shape (user ruling 2026-09-28).
        "exec" => {
            let argv: Vec<String> = shellish_split(source);
            Box::new(super::exec::OneShotExecutor::new(&argv, sandbox, encoding))
        }
        // Wasm compiles AT SPAWN (from_source) — its `load` is a no-op;
        // other carriers load lazily inside `load`. Same executor shape.
        other => anyhow::bail!("language not resident-carried by this effector build: {other}"),
    })
}

pub type DynExecutor = Box<dyn Executor>;
