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
/// Why a served connection ended. The difference matters to `run`: a lease that ran
/// out is this node's own decision to leave, a remote close is not (ADR-0003).
enum Closing {
    LeaseExpired,
    Remote,
}

async fn serve_connection(
    base_url: &str,
    token: &str,
    config: &EffectorConfig,
    executors: &Executors,
    code_cache: &Arc<CodeCache>,
    adopted: &Arc<Adopted>,
) -> Result<Closing> {
    let pending_host: Arc<PendingHost> =
        Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    // Stage two of a stop (ADR-0047): one control per in-flight call, addressed by the
    // control plane's call id. It dies with the call's entry — a handle that outlived
    // the call would stop the next one.
    let calls: Arc<std::sync::Mutex<std::collections::HashMap<String, CallControl>>> =
        Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    // Material answers, routed by (call_id, resource name).
    let pending_material: Arc<PendingMaterial> =
        Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    // What the plane said each declared resource's mode is, for the publish step after the call.
    let url = effector_protocol::dial_url(base_url, token);
    let (ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .with_context(|| format!("dial control plane {base_url}"))?;
    let (mut sink, mut stream) = ws.split();

    // The node speaks first, and it is a DECLARATION, not a proof: the token in the dial URL
    // was the proof, checked by the control plane before this socket was served (ADR-0003).
    // Anything here describes what the node is (alias, carriers) — never what it knows.
    let register = Frame::Register {
        node_alias: config.capabilities.node_alias.clone(),
        carriers: config.capabilities.carriers.clone(),
        // What contains this node (ADR-0003 §10). A call that NEEDS a container is refused by a
        // host node in words — see `execute_call_inner`.
        class: node_class(config),
    };
    sink.send(Message::Text(serde_json::to_string(&register)?))
        .await?;

    // Await acceptance before serving calls — but not forever. A plane that accepts the socket
    // and then says nothing would leave a container node alive with nothing to do, which is the
    // container the lease exists to end; the bound is the lease's first grace, so a host node
    // (no lease) waits exactly as long as it always did.
    let acceptance = async {
        match stream.next().await {
            Some(Ok(Message::Text(text))) => match serde_json::from_str::<Frame>(&text)? {
                Frame::Registered => Ok(()),
                other => anyhow::bail!("expected Registered, got {other:?}"),
            },
            Some(Err(e)) => Err(anyhow::Error::from(e)),
            _ => anyhow::bail!("control plane closed before registration"),
        }
    };
    match config.lease.first_grace() {
        Some(grace) => tokio::time::timeout(grace, acceptance)
            .await
            .map_err(|_| anyhow::anyhow!("no registered frame within {grace:?}"))??,
        None => acceptance.await?,
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

    // Calls served on THIS connection: the lease's grace grows with them
    // (`LeaseConfig::grace`). A node that is a container leaves when the work
    // stops, which is why the timer is the process's own and not a pool's.
    let mut served: u32 = 0;
    loop {
        let next = match config.lease.grace(served) {
            // Only idle time counts: a call in flight is work, and its task is
            // holding the timer at bay.
            Some(grace)
                if calls
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_empty() =>
            {
                match tokio::time::timeout(grace, stream.next()).await {
                    Ok(next) => next,
                    Err(_) => {
                        eprintln!("lease: nothing arrived within {grace:?}; leaving");
                        writer.abort();
                        return Ok(Closing::LeaseExpired);
                    }
                }
            }
            _ => stream.next().await,
        };
        let Some(msg) = next else {
            writer.abort();
            return Ok(Closing::Remote);
        };
        let Message::Text(text) = msg? else { continue };
        let frame: Frame = serde_json::from_str(&text)?;
        match frame {
            Frame::Call(call) => {
                served = served.saturating_add(1);
                let control = CallControl::default();
                calls
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(call.call_id.clone(), control.clone());
                let tx = tx.clone();
                let pending_host = pending_host.clone();
                let pending_material = pending_material.clone();
                let config = config.clone();
                let executors = executors.clone();
                let code_cache = code_cache.clone();
                let calls = calls.clone();
                let adopted = adopted.clone();
                // One executor task per call; host calls it makes run
                // concurrently over the same writer channel.
                tokio::spawn(async move {
                    let result = execute_call(
                        &config,
                        &executors,
                        &call,
                        &tx,
                        &pending_host,
                        &pending_material,
                        &code_cache,
                        &control,
                        &adopted,
                    )
                    .await;
                    let _ = tx.send(Frame::Result(ToolResult {
                        call_id: call.call_id.clone(),
                        outcome: result,
                    }));
                    // The call is over, so its control entry goes: the map's length
                    // is what the lease reads to decide whether work is in flight,
                    // and a handle outliving its call would stop the next one.
                    calls
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .remove(&call.call_id);
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
            // The plane's answer to a `MaterialRequest`: hand it to the call that is waiting.
            Frame::Material {
                call_id,
                kind,
                base,
                credential,
                mode,
            } => {
                let key = material_key(&call_id, &kind);
                let waiter = pending_material
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&key);
                if let Some(waiter) = waiter {
                    let _ = waiter.send(Ok(Minted {
                        base,
                        credential,
                        mode,
                    }));
                }
            }
            // The plane could not answer: the wait ends with a reason, not a deadline.
            Frame::MaterialFailed {
                call_id,
                kind,
                reason,
            } => {
                let key = material_key(&call_id, &kind);
                let waiter = pending_material
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&key);
                if let Some(waiter) = waiter {
                    let _ = waiter.send(Err(anyhow::anyhow!("{reason}")));
                }
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
}

/// Pending host calls: host_call_id -> reply path.
type PendingHost = std::sync::Mutex<
    std::collections::HashMap<
        String,
        tokio::sync::oneshot::Sender<Result<serde_json::Value, String>>,
    >,
>;

/// Material requests in flight: `(call_id, name)` → where the answer goes. Exactly one answer
/// per request (the control plane mints per transfer), so an entry dies with its request.
/// What the plane minted for one transfer: where to reach the store, how, and on what terms.
struct Minted {
    /// `<base>/<tenant>/<kind>`: the slice this execution may name things inside.
    base: String,
    credential: Option<String>,
    mode: String,
}

type PendingMaterial = std::sync::Mutex<
    std::collections::HashMap<
        String,
        tokio::sync::oneshot::Sender<anyhow::Result<Minted>>,
    >,
>;

/// What this node has materialised, by `kind/seq`. The directories are a cache with a known
/// owner — this node — so they go when it leaves (an executor's data is not something a later
/// process should inherit). A shared resource is fetched once; this is a set, not a count, because
/// the lifetime is the process's, not a call's.
type Adopted = std::sync::Mutex<std::collections::BTreeSet<String>>;

/// One ask per (call, kind): which NAME the script wants is never part of the key, because the plane
/// only ever hears the kind.
fn material_key(call_id: &str, kind: &str) -> String {
    format!("{call_id}\u{1f}{kind}")
}

/// Ask the control plane for one transfer's material and wait for it. The node never invents a
/// credential: it names the resource, and the plane answers with where to reach the store and how
/// (ADR-0003 §8). The answer is used for one `ensure`/`sync` and dropped.
fn ask_material(
    tx: &tokio::sync::mpsc::UnboundedSender<Frame>,
    pending: &Arc<PendingMaterial>,
    call_id: &str,
    kind: &str,
    write: bool,
) -> anyhow::Result<tokio::sync::oneshot::Receiver<anyhow::Result<Minted>>> {
    let key = material_key(call_id, kind);
    let (sender, receiver) = tokio::sync::oneshot::channel();
    pending
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(key.clone(), sender);
    let sent = tx.send(Frame::MaterialRequest {
        call_id: call_id.to_string(),
        kind: kind.to_string(),
        write,
    });
    if sent.is_err() {
        pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&key);
        anyhow::bail!("resource `{kind}`: the connection is gone");
    }
    Ok(receiver)
}

/// The same ask, from a host fn — a blocking context (the script is waiting on a value), so the
/// reply is awaited by parking the thread rather than by an `async` frame.
fn ask_material_blocking(
    tx: &tokio::sync::mpsc::UnboundedSender<Frame>,
    pending: &Arc<PendingMaterial>,
    call_id: &str,
    kind: &str,
    write: bool,
) -> anyhow::Result<Minted> {
    match ask_material(tx, pending, call_id, kind, write)?.blocking_recv() {
        Ok(material) => material,
        Err(_) => anyhow::bail!("resource `{kind}`: the control plane never answered"),
    }
}

/// Where resources live on this node. `/work/resources` is the ADR's path; the override exists so
/// a node without a writable `/work` (a test, a host node) can put them somewhere honest.
fn resources_root() -> std::path::PathBuf {
    std::env::var("EFFECTOR_RESOURCES_ROOT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("/work/resources"))
}

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
    pending_material: &Arc<PendingMaterial>,
    code_cache: &Arc<CodeCache>,
    control: &CallControl,
    adopted: &Arc<Adopted>,
) -> Result<serde_json::Value, String> {
    execute_call_inner(
        config,
        executors,
        call,
        tx,
        pending_host,
        pending_material,
        code_cache,
        control,
        adopted,
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
    pending_material: &Arc<PendingMaterial>,
    code_cache: &Arc<CodeCache>,
    control: &CallControl,
    adopted: &Arc<Adopted>,
) -> Result<serde_json::Value> {
    // A call may REQUIRE a class (ADR-0003 §1): code nobody reviewed asks for a container, and a
    // node that is not one refuses BY NAME. Running it anyway would be the silent acceptance the
    // ADR forbids, and it is the one check that has to be here rather than at the plane — the node
    // is the only party that knows what contains it.
    if let Some(why) = class_refusal(config, call) {
        anyhow::bail!("{why}");
    }
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
    let bridge = build_host_bridge(
        call.call_id.clone(),
        tx,
        pending_host,
        pending_material,
        adopted,
        control,
    );
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
    pending_material: &Arc<PendingMaterial>,
    adopted: &Arc<Adopted>,
    control: &CallControl,
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
    // Durable data, asked for at RUN time (`ctx.resource`). This is the only way a declaration
    // reaches a node: a booth's need is not knowable before it runs, so nothing rides the call.
    // The transfer happens HERE — the script gets a path, never a credential, and the store's
    // answers stay on this side of the host call.
    {
        let tx = tx.clone();
        let pending = pending_material.clone();
        let call_id = call_id.clone();
        let adopted = adopted.clone();
        let control = control.clone();
        let f: crate::carrier::HostFn = Arc::new(move |arg: serde_json::Value| {
            let m = match &arg {
                serde_json::Value::String(s) => {
                    serde_json::from_str::<serde_json::Value>(s).unwrap_or(serde_json::Value::Null)
                }
                other => other.clone(),
            };
            let text = |k: &str| m.get(k).and_then(serde_json::Value::as_str).map(str::to_string);
            let (Some(op), Some(kind), Some(remote)) = (text("op"), text("kind"), text("remote"))
            else {
                anyhow::bail!(
                    "a resource op names `op` (`get` or `publish`), `kind` and `remote`"
                );
            };
            let write = match op.as_str() {
                "get" => false,
                "publish" => true,
                other => anyhow::bail!(
                    "`{other}` is not a resource op; a resource is `get` or `publish`"
                ),
            };
            let mount = text("mount").map(std::path::PathBuf::from);
            // A call that was cancelled before it asked must not ask: the plane has already stopped
            // accounting for this call, and a transfer nobody is waiting for is pure cost.
            if control.is_cancelled() {
                anyhow::bail!("`{kind}:{remote}` was cancelled before the transfer started");
            }
            let reach = ask_material_blocking(&tx, &pending, &call_id, &kind, write)?;
            let spec = crate::resource::Spec::from_plane(&kind, &reach.mode, text("ref").as_deref())?;
            let material = crate::resource::Material {
                base: reach.base,
                credential: reach.credential,
            };
            let root = resources_root();
            let token: &dyn crate::resource::Interrupt = &control;
            let path = crate::resource::ensure(
                &spec,
                &material,
                &remote,
                mount.as_deref(),
                &root,
                Some(token),
            )?;
            adopted
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(format!("{kind}/{remote}"));
            if !write {
                return Ok(serde_json::json!(path.to_string_lossy()));
            }
            let reference = crate::resource::sync(
                &spec,
                &material,
                &remote,
                mount.as_deref(),
                &root,
                Some(token),
            )?;
            Ok(serde_json::json!(reference))
        });
        bridge.functions.insert("resource".to_string(), f);
    }
    bridge
}

/// Removes the resource directories this node materialised, when the node leaves. What a task
/// fetched is a cache with a known owner; a later process starts from nothing rather than from
/// whatever a previous one happened to leave lying around.
struct Materialised {
    root: std::path::PathBuf,
    adopted: Arc<Adopted>,
}

impl Drop for Materialised {
    fn drop(&mut self) {
        let identities: Vec<String> = self
            .adopted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .cloned()
            .collect();
        for identity in identities {
            let path = self.root.join(&identity);
            if let Err(e) = std::fs::remove_dir_all(&path) {
                // Leaving it is reported, never hidden: a node that cannot clean up is a node
                // whose disk will fill, and silence would make that a mystery later.
                eprintln!("resource `{identity}`: left behind at {}: {e}", path.display());
            }
        }
    }
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

/// One handshake, for `effector register`: dial with the token, declare the shape, print the
/// verdict, close. The dial/declare path is shared with `serve_connection` — a CLI that had its
/// own copy of the handshake would be testing a different protocol than the one that runs.
pub async fn register_once(config: &EffectorConfig) -> Result<()> {
    config
        .validate()
        .map_err(|e| anyhow::anyhow!("effector config: {e}"))?;
    let url = effector_protocol::dial_url(&config.control_plane_url, &config.token);
    let (mut ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .with_context(|| format!("dial control plane {}", config.control_plane_url))?;
    let register = Frame::Register {
        node_alias: config.capabilities.node_alias.clone(),
        carriers: config.capabilities.carriers.clone(),
        // What contains this node (ADR-0003 §10). A call that NEEDS a container is refused by a
        // host node in words — see `execute_call_inner`.
        class: node_class(config),
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

/// Why this node cannot run this call, if it cannot (ADR-0003 §1). The node is the only party that
/// knows what contains it, so this check cannot live at the plane: a plane can route, but only the
/// process knows whether it is inside a container.
fn class_refusal(config: &EffectorConfig, call: &ToolCall) -> Option<String> {
    match (call.requires, config.class) {
        (effector_protocol::NodeClass::Container, effector_config::Class::Host) => Some(
            format!(
                "call {} requires a container, and this node is `{}` — it cannot become one on \
                 demand. Bare metal is the exception a caller asks for; this call did not, and \
                 the plane routes container calls to a node started with `docker run`",
                call.call_id,
                config.class.as_str()
            ),
        ),
        _ => None,
    }
}

/// The node's declared class, as the wire spells it. One conversion, at the dialect boundary.
fn node_class(config: &EffectorConfig) -> effector_protocol::NodeClass {
    match config.class {
        effector_config::Class::Host => effector_protocol::NodeClass::Host,
        effector_config::Class::Container => effector_protocol::NodeClass::Container,
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
    let executors = Executors::new();
    let code_cache = Arc::new(CodeCache::new());
    // What this node has fetched, and the guard that removes it when the node leaves. A `Drop`
    // rather than a call at each exit: `run` has several honest ends (lease, disconnect, error),
    // and one of them forgetting is the leak nobody notices until a disk fills.
    let adopted: Arc<Adopted> = Arc::new(std::sync::Mutex::new(Default::default()));
    let _materialised = Materialised {
        root: resources_root(),
        adopted: adopted.clone(),
    };
    let mut backoff = std::time::Duration::from_secs(1);
    loop {
        match serve_connection(
            &config.control_plane_url,
            &config.token,
            &config,
            &executors,
            &code_cache,
            &adopted,
        )
        .await
        {
            // The node's own decision: the lease said leave (ADR-0003). This is a
            // clean end — in the container form the process exit IS the container
            // ending, and nothing is saved on the way out.
            Ok(Closing::LeaseExpired) => {
                eprintln!("lease: this node leaves; the container ends with it");
                return Ok(());
            }
            // A node that declared `exit_on_disconnect` has no work and no way to
            // be given any, so it leaves instead of reconnecting (a container node).
            // Leaving is not the same as succeeding: a clean close is an Ok, and a dead
            // link or an unanswered handshake keeps its ERROR — the launcher's exit code
            // is the only thing an operator will look at, and it must not read "fine"
            // for a node that never registered.
            Ok(Closing::Remote) if config.lease.exit_on_disconnect => {
                eprintln!("lease: the connection ended; this node does not reconnect");
                return Ok(());
            }
            Err(e) if config.lease.exit_on_disconnect => {
                eprintln!("lease: leaving instead of reconnecting: {e:#}");
                return Err(e);
            }
            Ok(Closing::Remote) => anyhow::bail!("control plane closed the connection cleanly"),
            Err(e) => eprintln!("connection lost: {e:#}; reconnecting in {:?}", backoff),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(std::time::Duration::from_secs(60));
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use effector_config::CapabilitySurface;

    fn config(capabilities: CapabilitySurface) -> EffectorConfig {
        EffectorConfig {
            class: effector_config::Class::Host,
            control_plane_url: "ws://127.0.0.1:8787".into(),
            token: "test-token".into(),
            capabilities,
            lease: Default::default(),
        }
    }

    fn call_requiring(class: effector_protocol::NodeClass) -> ToolCall {
        ToolCall {
            call_id: "c-class".into(),
            requires: class,
            kind: effector_protocol::CallKind::IterateStart,
            stream: None,
            executor: "e/1".into(),
            entry: "run".into(),
            language: "induction".into(),
            encoding: induction_protocol::Encoding::Json,
            args: serde_json::json!({}),
            code: effector_protocol::CodeRef {
                url: String::new(),
                sha256: String::new(),
            },
        }
    }

    #[test]
    fn a_call_that_needs_a_container_is_refused_by_a_host_node_in_words() {
        let host = config(CapabilitySurface::default());
        let needs = call_requiring(effector_protocol::NodeClass::Container);
        let why = class_refusal(&host, &needs).expect("a host node cannot pretend");
        assert!(why.contains("requires a container"), "{why}");
        assert!(why.contains("host"), "the refusal names what this node is: {why}");

        // THE DEFAULT IS THE CONTAINER: a call that says nothing about the class still requires
        // one, so silence is never a way onto bare metal.
        let silent: ToolCall = serde_json::from_value(serde_json::json!({
            "call_id": "c-silent",
            "executor": "e/1",
            "entry": "run",
            "language": "induction",
            "encoding": "json",
            "args": {},
            "code": { "url": "", "sha256": "" },
        }))
        .expect("a call with no class field decodes");
        assert_eq!(silent.requires, effector_protocol::NodeClass::Container);
        assert!(class_refusal(&host, &silent).is_some(), "silence still refuses");

        // Bare metal is the exception a caller WRITES DOWN.
        assert!(class_refusal(&host, &call_requiring(effector_protocol::NodeClass::Host)).is_none());

        // A container node can run both — it is strictly more contained, so a `host` requirement
        // is satisfied by a container, not the other way round.
        let mut inside = config(CapabilitySurface::default());
        inside.class = effector_config::Class::Container;
        assert!(class_refusal(&inside, &needs).is_none());
        assert!(class_refusal(&inside, &call_requiring(effector_protocol::NodeClass::Host)).is_none());
    }

}
