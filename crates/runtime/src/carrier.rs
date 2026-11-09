//! Language carriers: the three settled engines, feature-gated.
//!
//! Every carrier exposes the same contract: source in, JSON value out. The
//! runtime does not interpret what the script means — it received an
//! operation from the control plane and runs its executable part.

use serde_json::Value;

/// What a carrier needs to execute one operation.
/// Host-provided functions callable from inside an operation. Each takes
/// one JSON argument and returns a JSON result (or an error). The caller
/// (effector or embedded host) supplies state access, invoke, emit — the
/// carrier only marshals values across the VM boundary.
pub type HostFn = std::sync::Arc<dyn Fn(Value) -> anyhow::Result<Value> + Send + Sync>;

/// Stage two of a stop, at a node (aura ADR-0047, effector's half): one CALL's
/// cancel handle. Per call and never per carrier — an executor outlives the call,
/// and a flag that outlived it would stop the next one.
///
/// Two things live here, and the difference is the honest part: the FLAG is what
/// the carrier's wait loop reads (it stops the caller from waiting), and the HOOK
/// is what the carrier registers when it also has a way to tell the work itself
/// (writing induction's interrupt frame into a guest, killing a child). A carrier
/// that registers no hook can be released but not interrupted, and `interrupt()`
/// says so instead of pretending.
#[derive(Clone, Default)]
pub struct CallControl {
    inner: std::sync::Arc<ControlInner>,
}

#[derive(Default)]
struct ControlInner {
    cancelled: std::sync::atomic::AtomicBool,
    hook: std::sync::Mutex<Option<std::sync::Arc<dyn Fn() -> bool + Send + Sync>>>,
}

impl std::fmt::Debug for CallControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CallControl")
            .field(&self.is_cancelled())
            .field(&self.has_hook())
            .finish()
    }
}

impl CallControl {
    /// Raised? What a blocking wait loop reads between timeouts.
    pub fn is_cancelled(&self) -> bool {
        self.inner
            .cancelled
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// True once the carrier armed it — the capability question, separate from
    /// whether anybody has asked yet.
    pub fn has_hook(&self) -> bool {
        self.inner
            .hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }

    /// The carrier's way to reach the work itself. Called once per call, from the
    /// carrier, right after it knows what to address (`frame_id`, a child pid).
    pub fn arm(&self, hook: std::sync::Arc<dyn Fn() -> bool + Send + Sync>) {
        let mut slot = self
            .inner
            .hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *slot = Some(hook);
    }

    /// Raise the flag and tell the work, if this carrier can. The return value is
    /// "the node acted", not "the work is dead": a guest that never looks at its
    /// token still finishes, and its result is discarded.
    pub fn interrupt(&self) -> bool {
        self.inner
            .cancelled
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let hook = self
            .inner
            .hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        match hook {
            Some(hook) => hook(),
            None => false,
        }
    }
}

/// The host-injected byte-level engine face (ADR-0037 4.16a). FOUR
/// closures in okm's `VirtualStorage` shapes — bytes in, bytes out.
/// No okm type crosses the slot on purpose: the contract is byte-shaped,
/// so each side's own error type stays on its side of it. (The trait is
/// okm-entry's, so both sides build against ONE okm rev — the SHAPE is
/// shared even though no value type is.) Semantics mirror
/// okm's engine rules: ordered keys, `None` end = unbounded,
/// scan returns FULL keys (the caller's coordinate space).
///
/// Every closure answers `Result<_, String>`: since ADR-0038/0039 a
/// storage failure is a value, and it must cross this seam as a plain
/// string rather than as `okm_core::Error` — the same rev-independence
/// reason. A failure is never folded into an absent row or an empty
/// scan; the binding maps the string into its own error type.
pub type PutFn = std::sync::Arc<dyn Fn(Vec<u8>, Vec<u8>) -> Result<(), String> + Send + Sync>;
pub type GetFn = std::sync::Arc<dyn Fn(&[u8]) -> Result<Option<Vec<u8>>, String> + Send + Sync>;
pub type DelFn = std::sync::Arc<dyn Fn(&[u8]) -> Result<(), String> + Send + Sync>;
pub type ScanRangeFn =
    std::sync::Arc<dyn Fn(&[u8], Option<&[u8]>) -> Result<Vec<Vec<u8>>, String> + Send + Sync>;

pub struct StorageEngineFns {
    pub put: PutFn,
    pub get: GetFn,
    pub del: DelFn,
    pub scan_range: ScanRangeFn,
}

/// One declared collection of the injection slot: the RAW entry shape
/// `Collection::with_store` consumes —
/// `{ "schema": <CollectionSchema serde>, "indexes": [...], "reduces": [...] }`
/// (ns NOT in the entry — the DSL's rule; the slot binds it).
pub struct StorageCollection {
    pub name: String,
    pub entry: serde_json::Value,
}

/// The python carrier's storage injection slot (ADR-0037 §1): the
/// host's live engine behind the byte face + the type's declared
/// collections. The carrier's load step builds one `Collection` per
/// entry over this engine and registers it into the module namespace
/// under the collection name — the script's binding face, zero
/// translation. Other carriers ignore the slot (steel envelope ops
/// ride `store` (ctx.store); wasmtime rides its own raw `store.frame`
/// HostFn).
pub struct StorageSlot {
    pub ns: u16,
    pub collections: Vec<StorageCollection>,
    pub engine: std::sync::Arc<StorageEngineFns>,
}

/// Named host functions for one execution. `None` = pure operation
/// (no host contact), the current default.
#[derive(Clone, Default)]
pub struct HostBridge {
    pub functions: std::collections::BTreeMap<String, HostFn>,
    /// Storage injection (python carrier only; `None` = no collections).
    pub storage: Option<std::sync::Arc<StorageSlot>>,
}

pub struct ExecRequest<'a> {
    /// Operation source code (from `ToolCall.code` inline bytes, or fetched
    /// by the caller from a `link` payload before dispatch).
    pub source: &'a str,
    /// Entry point the operation declares (function name); `None` = run the
    /// script top-level and use its result.
    pub entry: Option<&'a str>,
    /// JSON arguments generated by the LLM.
    pub args: &'a Value,
    /// Host functions exposed to the operation (state/invoke/...). `None`
    /// for pure operations.
    pub host: Option<&'a HostBridge>,
}

pub type ExecResult = anyhow::Result<Value>;

pub mod exec;
#[cfg(feature = "python")]
pub mod python;
pub mod executor;
#[cfg(feature = "steel")]
pub mod steel;

/// Dispatch by declared language. Unknown language = error value, never a
/// panic: the control plane declared it, the Effector only validates.
pub fn execute(language: &str, _req: ExecRequest) -> anyhow::Result<Value> {
    match language {
        #[cfg(feature = "steel")]
        "steel" => anyhow::bail!("steel is resident-only: use carrier::executor"),
        #[cfg(feature = "python")]
        "python" => anyhow::bail!("python is resident-only: use carrier::executor"),
        other => anyhow::bail!("language not carried by this effector build: {other}"),
    }
}

/// Upload-time introspection, dispatched per language. Every carrier
/// assembles/merges `interface_schema` behind this one call — the host
/// never knows how each language collects its declarations.
pub fn introspect(language: &str, source: &str) -> anyhow::Result<Value> {
    introspect_encoded(language, source, induction_protocol::Encoding::Json)
}

/// The declared-encoding form (ADR-0037 §2): the process carriers spawn
/// their throwaway executor in the DECLARED codec — a CBOR-declared induction
/// booth's `interface_schema` frame round trip must ride CBOR too, or
/// the child would not parse the request.
pub fn introspect_encoded(
    language: &str,
    source: &str,
    encoding: induction_protocol::Encoding,
) -> anyhow::Result<Value> {
    match language {
        #[cfg(feature = "steel")]
        "steel" => steel::introspect(source),
        #[cfg(feature = "python")]
        "python" => python::introspect(source),
        // Languages without collectors: the script hand-writes the whole
        // `interface_schema` — run it in a THROWAWAY resident executor
        // (load, call the schema function, drop). Same contract, no
        // one-shot execution path.
        _ => {
            let executors = executor::Executors::new();
            executors.with_executor_encoded(
                "__introspect",
                language,
                source,
                None,
                &crate::sandbox::SandboxPolicy::None,
                encoding,
                |s: &mut dyn crate::carrier::executor::Executor| {
                    s.call("interface_schema", &Value::Null)
                },
            )
        }
    }
}
