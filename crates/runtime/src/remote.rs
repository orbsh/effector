//! The remote wrapper (Phase 3): outbound WS to the control plane, register
//! presence + capability, then serve pushed-down calls against the resident
//! carrier executors. No inbound ports — the effector always dials out; the
//! connection carries both directions. No degraded fallback exists by
//! design: long-polling was explicitly rejected (WS-only is the contract —
//! the control determines compromise, not the industry's conservative
//! defaults).

use crate::carrier::executor::Executors;
use crate::carrier::{CallControl, HostBridge};
use anyhow::{Context, Result};
use effector_config::EffectorConfig;
use effector_protocol::{CodeRef, Frame, HostCall, HostFrame, HostOp, ToolCall, ToolResult};
use futures_util::{SinkExt, StreamExt};
use std::io::Read as _;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tokio_tungstenite::tungstenite::Message;

/// Serve one connection until it drops. Reconnect loop lives in `run`.
async fn serve_connection(
    ws_url: &str,
    key: &effector_protocol::SigningKey,
    config: &EffectorConfig,
    executors: &Executors,
    code_cache: &Arc<CodeCache>,
    policy: &crate::sandbox::SandboxPolicy,
) -> Result<()> {
    let pending_host: Arc<PendingHost> =
        Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    // Stage two of a stop (ADR-0047): one control per in-flight call, addressed by the
    // control plane's call id. It dies with the call's entry — a handle that outlived
    // the call would stop the next one.
    let calls: Arc<std::sync::Mutex<std::collections::HashMap<String, CallControl>>> =
        Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    let (ws, _) = tokio_tungstenite::connect_async(ws_url)
        .await
        .with_context(|| format!("dial control plane {ws_url}"))?;
    let (mut sink, mut stream) = ws.split();

    // The control plane speaks first: a challenge this node signs with its key. The signature
    // is the whole claim — the host looks the PUBLIC key up in its registry, so nothing
    // secret ever crosses this connection (effector ADR-0002).
    let nonce = match stream.next().await {
        Some(Ok(Message::Text(text))) => match serde_json::from_str::<Frame>(&text)? {
            Frame::Challenge { nonce } => nonce,
            other => anyhow::bail!("expected Challenge, got {other:?}"),
        },
        Some(Err(e)) => return Err(e.into()),
        _ => anyhow::bail!("control plane closed before the challenge"),
    };
    let node_alias = config.capabilities.node_alias.clone();
    let register = Frame::Register {
        public_key: effector_protocol::public_key_b64(key),
        signature: effector_protocol::sign_register(key, &nonce, &node_alias),
        node_alias,
        carriers: config.capabilities.carriers.clone(),
    };
    sink.send(Message::Text(serde_json::to_string(&register)?))
        .await?;

    // Await acceptance before serving calls.
    match stream.next().await {
        Some(Ok(Message::Text(text))) => match serde_json::from_str::<Frame>(&text)? {
            Frame::Registered => {}
            other => anyhow::bail!("expected Registered, got {other:?}"),
        },
        Some(Err(e)) => return Err(e.into()),
        _ => anyhow::bail!("control plane closed before registration"),
    }

    // Bidirectional loop: a writer task drains an mpsc into the sink; the
    // reader dispatches incoming frames. Host calls (ctx bridge) flow
    // effector->control-plane while a ToolCall is executing — the two task
    // structure makes the connection full-duplex at the frame level.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();
    let writer = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            if sink
                .send(Message::Text(serde_json::to_string(&frame)?))
                .await
                .is_err()
            {
                break;
            }
        }
        Ok::<_, anyhow::Error>(())
    });

    while let Some(msg) = stream.next().await {
        let Message::Text(text) = msg? else { continue };
        let frame: Frame = serde_json::from_str(&text)?;
        match frame {
            Frame::Call(call) => {
                let control = CallControl::default();
                calls
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(call.call_id.clone(), control.clone());
                let tx = tx.clone();
                let pending_host = pending_host.clone();
                let config = config.clone();
                let executors = executors.clone();
                let code_cache = code_cache.clone();
                let policy = policy.clone();
                // One executor task per call; host calls it makes run
                // concurrently over the same writer channel.
                tokio::spawn(async move {
                    let result = execute_call(
                        &config,
                        &executors,
                        &call,
                        &tx,
                        &pending_host,
                        &code_cache,
                        &control,
                        &policy,
                    )
                    .await;
                    let _ = tx.send(Frame::Result(ToolResult {
                        call_id: call.call_id.clone(),
                        outcome: result,
                    }));
                });
            }
            // The control plane stopped waiting. Two outcomes, and the difference is
            // the honest part: `landed: true` means this carrier reached the work
            // (a guest's token, a child's kill); `false` means the flag went up, this
            // wait will return, and the work itself will finish into the void.
            Frame::Interrupt { call_id } => {
                let control = calls
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&call_id);
                let landed = match control {
                    Some(control) => control.interrupt(),
                    None => false,
                };
                let _ = tx.send(Frame::Interrupted { call_id, landed });
            }
            Frame::Host(HostFrame::Result(hr)) => {
                let mut pending = pending_host
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(tx) = pending.remove(&hr.host_call_id) {
                    let _ = tx.send(hr.outcome);
                }
                // Unknown id: the caller timed out — drop the late result.
            }
            other => anyhow::bail!("unexpected frame from control plane: {other:?}"),
        }
    }
    writer.abort();
    Ok(())
}

/// Pending host calls: host_call_id -> reply path.
type PendingHost = std::sync::Mutex<
    std::collections::HashMap<
        String,
        tokio::sync::oneshot::Sender<Result<serde_json::Value, String>>,
    >,
>;

/// Execute one ToolCall against the resident executors. Phase 4 adds the
/// `link` payload form; today only inline bytes are consumed (a `link`
/// payload is an explicit error, not a silent fetch — the effector initiates
/// no fetch of its own).
#[allow(clippy::too_many_arguments)]
async fn execute_call(
    config: &EffectorConfig,
    executors: &Executors,
    call: &ToolCall,
    tx: &tokio::sync::mpsc::UnboundedSender<Frame>,
    pending_host: &Arc<PendingHost>,
    code_cache: &Arc<CodeCache>,
    control: &CallControl,
    policy: &crate::sandbox::SandboxPolicy,
) -> Result<serde_json::Value, String> {
    execute_call_inner(
        config,
        executors,
        call,
        tx,
        pending_host,
        code_cache,
        control,
        policy,
    )
    .await
    .map_err(|e| e.to_string())
}

#[allow(clippy::too_many_arguments)]
async fn execute_call_inner(
    config: &EffectorConfig,
    executors: &Executors,
    call: &ToolCall,
    tx: &tokio::sync::mpsc::UnboundedSender<Frame>,
    pending_host: &Arc<PendingHost>,
    code_cache: &Arc<CodeCache>,
    control: &CallControl,
    policy: &crate::sandbox::SandboxPolicy,
) -> Result<serde_json::Value> {
    // Content-addressed code (ADR-0027): the frame carries a reference,
    // bytes ride the data path. Resolve = per-hash cache hit, else fetch +
    // verify (mismatch = error, never silent). The cache is a discardable
    // hot layer — same legitimacy tier as the resident executor; nothing
    // in it would need to be recovered.
    let source = code_cache.resolve(&call.code)?;

    // Args carry no partition here — the control plane's booth model owns
    // partitioning; the effector only keys residency by the caller's executor
    // identity (below).
    // Host bridge: ctx ops ride Frame::Host over the same connection, each
    // with a unique host_call_id; the closure awaits the correlated reply.
    let bridge = build_host_bridge(call.call_id.clone(), tx, pending_host);
    // Residency: the caller's executor identity keys the resident runtime —
    // calls sharing a `executor` share VM/module state, different ones never
    // do. The node alias in the key is only for readability (the registry is
    // process-local). `entry` names the point the delivered code exposes.
    let key = format!(
        "effector/{}/{}",
        config.capabilities.node_alias, call.executor
    );
    let executors = executors.clone();
    let language = call.language.clone();
    let entry = call.entry.clone();
    let args = call.args.clone();
    let kind = call.kind;
    let stream_id = call.stream.clone().unwrap_or_default();
    // The deployment's policy: built ONCE at boot (`run`), because the egress
    // face it may name is a live listener — a per-call policy would have to
    // start proxies per call. `sandbox: true` is the remote default (unattended
    // external code) and wraps every executor process; `false` is the
    // trusted-domain form and passes None. The field IS the switch.
    let sandbox = policy.clone();
    // Copy the declared codec out — `call` is a reference and cannot
    // enter the move-closure (the field is Copy).
    let encoding = call.encoding;
    // The control is owned by the task: the hook a carrier arms outlives this
    // function's borrow (it lives as long as the call does).
    let control = control.clone();
    tokio::task::spawn_blocking(move || {
        executors.with_executor_encoded(
            &key,
            &language,
            &source,
            Some(&bridge),
            &sandbox,
            // ADR-0037 §2: the process carriers speak the DECLARED codec;
            // the control plane sends it with every call, embedded
            // carriers ignore it (no channel).
            encoding,
            // Unified seam (ADR-0036): every dispatch job is a stream
            // op — invoke is the stream whose first round is terminal
            // (plain handlers answer bare, the carrier wraps to
            // `{done:true,value}`). The generator/envelope verbs drive
            // the executor's iterate surface exactly as ADR-0034; the
            // stream_id is minted by the control plane — the effector
            // derives nothing from it.
            move |s: &mut dyn crate::carrier::executor::Executor| {
                // Arm before the op: a carrier that can be told registers its hook
                // here, and one that cannot leaves the control unhooked — which the
                // `Interrupted` frame then reports rather than hides.
                s.arm_interrupt(&control);
                match kind {
                effector_protocol::CallKind::IterateStart => {
                    s.iterate(crate::carrier::executor::StreamOp::Start {
                        stream_id,
                        handler: entry,
                        args,
                    })
                }
                effector_protocol::CallKind::IterateNext => {
                    s.iterate(crate::carrier::executor::StreamOp::Next {
                        stream_id,
                        handler: entry,
                        args,
                    })
                }
                effector_protocol::CallKind::IterateDispose => {
                    s.iterate(crate::carrier::executor::StreamOp::Dispose {
                        stream_id,
                        handler: entry,
                        args,
                    })
                }
                }
            },
        )
    })
    .await
    .unwrap_or_else(|e| Err(anyhow::anyhow!("executor task join: {e}")))
}

/// Build a HostBridge whose functions send Frame::Host(Call) over the
/// connection and await the correlated Frame::Host(Result). One JSON arg
/// in, JSON value out — the same marshal contract as in-process bridges.
fn build_host_bridge(
    call_id: String,
    tx: &tokio::sync::mpsc::UnboundedSender<Frame>,
    pending_host: &Arc<PendingHost>,
) -> HostBridge {
    let tx = tx.clone();
    let pending = pending_host.clone();
    let mut bridge = HostBridge::default();
    {
        let name = "invoke";
        let tx = tx.clone();
        let pending = pending.clone();
        let call_id = call_id.clone();
        let f: crate::carrier::HostFn = Arc::new(move |arg: serde_json::Value| {
            let (op_tx, op_rx) = tokio::sync::oneshot::channel();
            let host_call_id = format!(
                "host-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_nanos()
            );
            let op = match name {
                "invoke" => {
                    let m = match &arg {
                        serde_json::Value::String(s) => {
                            serde_json::from_str::<serde_json::Value>(s)
                                .unwrap_or(serde_json::Value::Null)
                        }
                        other => other.clone(),
                    };
                    let gs = |k: &str| m.get(k).cloned().unwrap_or(serde_json::Value::Null);
                    let gs_s = |k: &str| gs(k).as_str().unwrap_or_default().to_string();
                    HostOp::Invoke {
                        target_type: gs_s("type"),
                        target_key: gs_s("key"),
                        handler: gs_s("handler"),
                        args: gs("args"),
                    }
                }
                _ => return Err(anyhow::anyhow!("host op `{name}` has no builder")),
            };
            pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(host_call_id.clone(), op_tx);
            tx.send(Frame::Host(HostFrame::Call(HostCall {
                host_call_id: host_call_id.clone(),
                call_id: call_id.clone(),
                op,
            })))
            .map_err(|_| anyhow::anyhow!("connection closed"))?;
            // Block on the reply — host fns are synchronous from the
            // script's perspective; spawn_blocking isolation makes the
            // await of the oneshot's blocking recv safe.
            match op_rx.blocking_recv() {
                Ok(Ok(v)) => Ok(v),
                Ok(Err(e)) => Err(anyhow::anyhow!("{e}")),
                Err(_) => Err(anyhow::anyhow!("host call dropped")),
            }
        });
        bridge.functions.insert(name.to_string(), f);
    }
    // ADR-0034 consumer legs over the wire: a remote script booth
    // starting, pulling, and disposing a stream rides the same
    // Frame::Host round trip as invoke — one send/receive path,
    // three ops. The control plane mints the stream_id at start and
    // echoes it back; the cursor loop lives in the script wrapper.
    // `store` (ADR-0043: the ctx.store method; formerly ctx_store_emit)
    // joins the loop (Phase 4.14 gate 1): the script's
    // okm Collection instruction is pure DATA here — the effector decodes
    // a JSON string argument at most, never the instruction's schema
    // (the schema lives with the type registration on the control plane).
    {
        let ops = [
            ("iterate.start", HostOpArm::Start),
            ("iterate.next", HostOpArm::Next),
            ("iterate.dispose", HostOpArm::Dispose),
            ("store", HostOpArm::StoreEmit),
            ("emit", HostOpArm::Emit),
        ];
        for (name, arm) in ops {
            let tx = tx.clone();
            let pending = pending.clone();
            let call_id = call_id.clone();
            let f: crate::carrier::HostFn = Arc::new(move |arg: serde_json::Value| {
                let (op_tx, op_rx) = tokio::sync::oneshot::channel();
                let host_call_id = format!(
                    "host-{}-{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)?
                        .as_nanos(),
                    std::process::id()
                );
                let m = match &arg {
                    serde_json::Value::String(s) => serde_json::from_str::<serde_json::Value>(s)
                        .unwrap_or(serde_json::Value::Null),
                    other => other.clone(),
                };
                let gs = |k: &str| m.get(k).cloned().unwrap_or(serde_json::Value::Null);
                let gs_s = |k: &str| gs(k).as_str().unwrap_or_default().to_string();
                let op = match arm {
                    HostOpArm::Start => HostOp::Iterate {
                        target_type: gs_s("type"),
                        target_key: gs_s("key"),
                        handler: gs_s("handler"),
                        args: gs("args"),
                    },
                    HostOpArm::Next => HostOp::IterateNext {
                        stream_id: gs_s("stream_id"),
                    },
                    HostOpArm::Dispose => HostOp::IterateDispose {
                        stream_id: gs_s("stream_id"),
                    },
                    HostOpArm::StoreEmit => HostOp::StoreEmit {
                        // The decoded argument IS the instruction —
                        // carried as data, schema-blind (see above).
                        instruction: m.clone(),
                    },
                    HostOpArm::Emit => HostOp::Emit {
                        // ctx.emit over the wire: the script passed
                        // {event, data}; the control plane resolves routes.
                        event: gs_s("event"),
                        data: gs("data"),
                    },
                };
                pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(host_call_id.clone(), op_tx);
                tx.send(Frame::Host(HostFrame::Call(HostCall {
                    host_call_id: host_call_id.clone(),
                    call_id: call_id.clone(),
                    op,
                })))
                .map_err(|_| anyhow::anyhow!("connection closed"))?;
                match op_rx.blocking_recv() {
                    Ok(Ok(v)) => Ok(v),
                    Ok(Err(e)) => Err(anyhow::anyhow!("{e}")),
                    Err(_) => Err(anyhow::anyhow!("host call dropped")),
                }
            });
            bridge.functions.insert(name.to_string(), f);
        }
    }
    bridge
}

/// Which iterate/store op a bridge arm builds from the script's JSON arg.
#[derive(Clone, Copy)]
enum HostOpArm {
    Start,
    Next,
    Dispose,
    StoreEmit,
    Emit,
}

/// Per-hash code cache (ADR-0027): resolved bytes keyed by the asserted
/// hash. Content addressing makes eviction semantics trivial — a stale
/// entry is unreachable waste (new code is a new hash, a new key), so the
/// cache grows with distinct code, never with call count. Survives
/// reconnection (lives at `run` scope); a process start begins empty.
pub struct CodeCache {
    entries: StdMutex<std::collections::HashMap<String, Arc<str>>>,
}

impl CodeCache {
    pub fn new() -> Self {
        Self {
            entries: StdMutex::new(std::collections::HashMap::new()),
        }
    }

    /// Resolve a reference to source text: cache hit, or fetch + verify +
    /// insert. A hash mismatch is tampering or a stale resolution — an
    /// error, never a silent accept.
    pub fn resolve(&self, code: &CodeRef) -> anyhow::Result<String> {
        if let Some(hit) = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&code.sha256)
        {
            return Ok(hit.to_string());
        }
        let bytes = fetch_verified(&code.url, &code.sha256)?;
        let source = String::from_utf8(bytes)
            .map_err(|_| anyhow::anyhow!("fetched code is not valid UTF-8"))?;
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(code.sha256.clone(), Arc::from(source.as_str()));
        Ok(source)
    }
}

impl Default for CodeCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Fetch by reference: GET the URL, verify sha256 against the expected
/// hash (hex, asserted by the frame — never parsed from the URL).
fn fetch_verified(url: &str, expected_sha256: &str) -> anyhow::Result<Vec<u8>> {
    let resp = ureq::get(url)
        .timeout(std::time::Duration::from_secs(30))
        .call()
        .map_err(|e| anyhow::anyhow!("link fetch {url}: {e}"))?;
    let mut bytes = Vec::new();
    resp.into_reader()
        .read_to_end(&mut bytes)
        .map_err(|e| anyhow::anyhow!("link read {url}: {e}"))?;
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(&bytes);
    let got = hex::encode(hasher.finalize());
    if got != expected_sha256.to_lowercase() {
        anyhow::bail!("link hash mismatch for {url}: expected {expected_sha256}, got {got}");
    }
    Ok(bytes)
}

/// One handshake, for `effector register`: dial, answer the challenge, print the verdict,
/// close. The connect/sign/verify path is shared with `serve_connection` — a CLI that had its
/// own copy of the handshake would be testing a different protocol than the one that runs.
pub async fn register_once(config: &EffectorConfig) -> Result<()> {
    config
        .validate()
        .map_err(|e| anyhow::anyhow!("effector config: {e}"))?;
    let key = effector_protocol::read_signing_key(std::path::Path::new(&config.private_key_path))?;
    let (mut ws, _) = tokio_tungstenite::connect_async(&config.control_plane_url)
        .await
        .with_context(|| format!("dial control plane {}", config.control_plane_url))?;
    let nonce = match ws.next().await {
        Some(Ok(Message::Text(text))) => match serde_json::from_str::<Frame>(&text)? {
            Frame::Challenge { nonce } => nonce,
            other => anyhow::bail!("expected Challenge, got {other:?}"),
        },
        Some(Err(e)) => return Err(e.into()),
        _ => anyhow::bail!("control plane closed before the challenge"),
    };
    let node_alias = config.capabilities.node_alias.clone();
    let register = Frame::Register {
        public_key: effector_protocol::public_key_b64(&key),
        signature: effector_protocol::sign_register(&key, &nonce, &node_alias),
        node_alias,
        carriers: config.capabilities.carriers.clone(),
    };
    ws.send(Message::Text(serde_json::to_string(&register)?))
        .await?;
    match ws.next().await {
        Some(Ok(Message::Text(text))) => match serde_json::from_str::<Frame>(&text)? {
            Frame::Registered => Ok(()),
            Frame::Refused { reason } => anyhow::bail!("refused: {reason}"),
            other => anyhow::bail!("expected Registered or Refused, got {other:?}"),
        },
        Some(Err(e)) => Err(e.into()),
        _ => anyhow::bail!("control plane closed without answering"),
    }
}

/// Top-level loop: dial, register, serve; reconnect with backoff on drop.
pub async fn run(config: EffectorConfig) -> Result<()> {
    // Fail closed before dialing: a declaration this node cannot enforce is
    // refused here rather than registered as a capability (see
    // `EffectorConfig::validate`). The credential check below is the same
    // shape — a startup fact, not a per-call error value.
    config
        .validate()
        .map_err(|e| anyhow::anyhow!("effector config: {e}"))?;
    // The key is a startup fact, like the capability declaration: a node without one cannot
    // make a claim, and that is a refusal to run, not a per-call error value.
    let key = effector_protocol::read_signing_key(std::path::Path::new(&config.private_key_path))?;
    let executors = Executors::new();
    let code_cache = Arc::new(CodeCache::new());
    // The egress face, when this node declared an allowlist. It lives for the
    // whole run — across reconnects — because the sockets the jail connects to
    // are the node's, not the connection's.
    let egress = match (&config.capabilities.network, config.sandbox) {
        (effector_config::NetworkPolicy::Allow(domains), true) => {
            Some(crate::egress::Egress::start(domains).await?)
        }
        _ => None,
    };
    let policy = if config.sandbox {
        sandbox_policy_for(&config, egress.as_ref().map(|e| e.face().clone()))
    } else {
        crate::sandbox::SandboxPolicy::None
    };
    let mut backoff = std::time::Duration::from_secs(1);
    loop {
        match serve_connection(
            &config.control_plane_url,
            &key,
            &config,
            &executors,
            &code_cache,
            &policy,
        )
        .await
        {
            Ok(()) => anyhow::bail!("control plane closed the connection cleanly"),
            Err(e) => eprintln!("connection lost: {e:#}; reconnecting in {:?}", backoff),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(std::time::Duration::from_secs(60));
    }
}
/// The jail is built from the NODE'S OWN declaration (`CapabilitySurface`):
/// the machine is the party that knows what it can expose, so the machine
/// states it — nothing here is derived from a control-plane frame, and no
/// field is left hardcoded while a declaration for it exists.
fn sandbox_policy_for(
    config: &EffectorConfig,
    proxy: Option<crate::sandbox::ProxyFace>,
) -> crate::sandbox::SandboxPolicy {
    use crate::sandbox::SandboxPolicy;
    use effector_config::NetworkPolicy;
    SandboxPolicy::Bubblewrap {
        // Write scope: everything else is read-only.
        allow_write: config.capabilities.fs_scope.clone(),
        // The read-side declaration (credentials, shell configs, …).
        deny_read: config.capabilities.deny_read.clone(),
        // Declared cwd, or the pre-declaration default (the system temp dir).
        cwd: config
            .capabilities
            .cwd
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().display().to_string()),
        // The declared allowlist, carried as declared. What makes it
        // ENFORCEABLE is `proxy`: the jail only reaches the world through the
        // node's proxies, and with no face the namespace has no network at all.
        allowed_domains: match &config.capabilities.network {
            NetworkPolicy::Allow(domains) => domains.clone(),
            NetworkPolicy::None | NetworkPolicy::Open => Vec::new(),
        },
        proxy,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use effector_config::{CapabilitySurface, NetworkPolicy};

    fn config(capabilities: CapabilitySurface) -> EffectorConfig {
        EffectorConfig {
            control_plane_url: "ws://127.0.0.1:8787".into(),
            sandbox: true,
            private_key_path: "/dev/null/key".into(),
            capabilities,
        }
    }

    /// The four declared halves plus the egress face (None when the node has no
    /// allowlist — the tests that build a policy without a live proxy all take
    /// this shape).
    fn wrapped(
        caps: CapabilitySurface,
    ) -> (
        Vec<String>,
        Vec<String>,
        String,
        Vec<String>,
        Option<crate::sandbox::ProxyFace>,
    ) {
        match sandbox_policy_for(&config(caps), None) {
            crate::sandbox::SandboxPolicy::Bubblewrap {
                allow_write,
                deny_read,
                cwd,
                allowed_domains,
                proxy,
            } => (allow_write, deny_read, cwd, allowed_domains, proxy),
            other => panic!("a wrapped deployment builds a bwrap policy, got {other:?}"),
        }
    }

    #[test]
    fn the_jail_is_the_nodes_own_declaration() {
        let caps = CapabilitySurface {
            node_alias: "n1".into(),
            fs_scope: vec!["/srv/work".into()],
            deny_read: vec!["/home/user/.ssh".into()],
            cwd: Some("/srv/work".into()),
            ..Default::default()
        };
        let (allow_write, deny_read, cwd, allowed_domains, proxy) = wrapped(caps);
        assert_eq!(allow_write, vec!["/srv/work".to_string()]);
        assert_eq!(deny_read, vec!["/home/user/.ssh".to_string()]);
        assert_eq!(cwd, "/srv/work");
        assert!(
            allowed_domains.is_empty(),
            "no allowlist → nothing to carry"
        );
        assert!(
            proxy.is_none(),
            "and no face → the namespace has no network at all"
        );
    }

    #[test]
    fn an_undeclared_cwd_keeps_the_old_default() {
        let caps = CapabilitySurface {
            node_alias: "n1".into(),
            fs_scope: vec!["/srv/work".into()],
            ..Default::default()
        };
        let (_, deny_read, cwd, _, _) = wrapped(caps);
        assert!(deny_read.is_empty());
        assert_eq!(cwd, std::env::temp_dir().display().to_string());
    }

    #[test]
    fn a_declared_allowlist_reaches_the_policy_field() {
        let caps = CapabilitySurface {
            node_alias: "n1".into(),
            network: NetworkPolicy::Allow(vec!["github.com:443".into()]),
            ..Default::default()
        };
        let (_, _, _, allowed_domains, proxy) = wrapped(caps);
        assert_eq!(allowed_domains, vec!["github.com:443".to_string()]);
        assert!(
            proxy.is_none(),
            "the declaration is carried; the FACE is the live `Egress`'s, and this \
             builder without one describes a jail with no network"
        );
    }

    #[test]
    fn a_live_face_is_what_makes_the_allowlist_reachable() {
        // The jail only reaches the world through `proxy`: the two sockets and the
        // two loopback ports ride the policy, and the bwrap builder consumes them.
        let caps = CapabilitySurface {
            node_alias: "n1".into(),
            network: NetworkPolicy::Allow(vec!["github.com:443".into()]),
            ..Default::default()
        };
        let face = crate::sandbox::ProxyFace {
            http_socket: "/home/u/.cache/effector/egress-1/http.sock".into(),
            socks_socket: "/home/u/.cache/effector/egress-1/socks.sock".into(),
            http_port: crate::egress::JAIL_HTTP_PORT,
            socks_port: crate::egress::JAIL_SOCKS_PORT,
        };
        match sandbox_policy_for(&config(caps), Some(face.clone())) {
            crate::sandbox::SandboxPolicy::Bubblewrap { proxy, .. } => {
                assert_eq!(proxy, Some(face), "the face rides the policy verbatim");
            }
            other => panic!("a wrapped deployment builds a bwrap policy, got {other:?}"),
        }
    }
}
