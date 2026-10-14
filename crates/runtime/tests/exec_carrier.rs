//! Exec/Induction carrier tests (ADR-0035): the framed resident shape
//! (induction — the `induction_loop` fixture: the line protocol, call, ctx
//! round trip, the iterate verbs, residency/sweep rules) and the bare
//! one-shot shape (exec — the `one_shot` fixture: no protocol, one JSON in,
//! one JSON out, nothing survives the call).
//!
//! Fixtures are built by the workspace (`cargo build -p effector-runtime
//! --examples`; the nu author script lives beside them in this crate's
//! `examples/`); a missing binary fails the test loudly — a stale build is a
//! recipe error, never a skip.

use effector_runtime::carrier::session::StreamOp;
use effector_runtime::carrier::HostBridge;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

fn induction_bin() -> String {
    bin("induction_loop")
}

fn one_shot_bin() -> String {
    bin("one_shot")
}

fn bin(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../../target/debug/examples/{name}"));
    assert!(
        path.exists(),
        "{name} missing — build it: cargo build -p effector-runtime --examples"
    );
    path.to_str().unwrap().to_string()
}

fn sessions() -> effector_runtime::carrier::session::Sessions {
    effector_runtime::carrier::session::Sessions::new()
}

/// Plain call through the resident session: request frame out, result
/// frame back — the invoke shape across the process boundary.
#[test]
fn induction_call_round_trip() {
    let s = sessions();
    let argv = induction_bin();
    let out = s
        .with_session(
            "box/k1",
            "induction",
            &argv,
            None,
            &effector_runtime::sandbox::SandboxPolicy::None,
            |sess| sess.call("echo", &json!({"x": 1})),
        )
        .unwrap();
    assert_eq!(out, json!({"echoed": {"x": 1}}));

    // The SECOND call reuses the same child (residency): if each call
    // re-spawned, the stream guard state below would never work — and
    // here, the same session key must not have been evicted.
    let out2 = s
        .with_session(
            "box/k1",
            "induction",
            &argv,
            None,
            &effector_runtime::sandbox::SandboxPolicy::None,
            |sess| sess.call("echo", &json!({"y": 2})),
        )
        .unwrap();
    assert_eq!(out2, json!({"echoed": {"y": 2}}));
}

/// The ctx seam crosses the boundary: the child sends a host frame
/// (invoke / ctx.invoke) mid-call, the parent's bridge answers on stdin, the
/// child forwards the reply into its result. This is ADR-0035's §3
/// `host` / `host_reply` pair over real pipes.
#[test]
fn induction_host_call_crosses_the_seam() {
    let seen: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let bridge = {
        let seen = seen.clone();
        let mut b = HostBridge::default();
        b.functions.insert(
            "invoke".into(),
            Arc::new(move |arg: Value| {
                seen.lock().unwrap().push(arg.clone());
                Ok(json!({"from": "host", "echo": arg}))
            }) as effector_runtime::carrier::HostFn,
        );
        b
    };
    let s = sessions();
    let argv = induction_bin();
    let out = s
        .with_session(
            "box/k2",
            "induction",
            &argv,
            Some(&bridge),
            &effector_runtime::sandbox::SandboxPolicy::None,
            |sess| {
                sess.call(
                    "ctx_round_trip",
                    &json!({"type": "t", "key": "k", "handler": "h", "args": {"q": 1}}),
                )
            },
        )
        .unwrap();
    assert_eq!(seen.lock().unwrap().len(), 1, "the host fn ran once");
    assert_eq!(
        out["from"], "host",
        "the parent's answer reached the child's reply"
    );
}

/// ADR-0034 over the induction seam: start carries the first envelope,
/// next advances the guard the CHILD keeps in its own memory (the
/// residency is the stream state — the envelope producer has no
/// generator, and it does not need one), done is written.
#[test]
fn induction_iterate_stream() {
    let s = sessions();
    let argv = induction_bin();
    // One with_session per pull — each call re-enters the SAME child
    // (the registry keys residency; the guard state lives in the
    // child's memory across the calls, exactly like a session VM).
    let pull = |op: StreamOp| -> Value {
        s.with_session(
            "box/k3",
            "induction",
            &argv,
            None,
            &effector_runtime::sandbox::SandboxPolicy::None,
            move |sess| sess.iterate(op.clone()),
        )
        .unwrap()
    };
    let mut envs = vec![pull(StreamOp::Start {
        stream_id: "s1".into(),
        handler: "count".into(),
        args: json!({"total": 3}),
    })];
    for _ in 0..4 {
        let e = pull(StreamOp::Next {
            stream_id: "s1".into(),
            handler: "count".into(),
            args: json!({"total": 3}),
        });
        let done = e.get("done").and_then(|d| d.as_bool()) == Some(true);
        envs.push(e);
        if done {
            break;
        }
    }
    assert_eq!(
        envs[0],
        json!({"item": "i0", "done": false}),
        "start carries the first item"
    );
    assert_eq!(envs[1], json!({"item": "i1", "done": false}));
    assert_eq!(envs[2], json!({"item": "i2", "done": false}));
    assert_eq!(
        envs.last().unwrap(),
        &json!({"done": true}),
        "exhaustion writes done"
    );

    // Pull-after-done: the child's guard is gone — a failed pull (an
    // error value through the stream), never a resurrection.
    let late = s
        .with_session(
            "box/k3",
            "induction",
            &argv,
            None,
            &effector_runtime::sandbox::SandboxPolicy::None,
            |sess| {
                sess.iterate(StreamOp::Next {
                    stream_id: "s1".into(),
                    handler: "count".into(),
                    args: json!({}),
                })
            },
        )
        .unwrap_err()
        .to_string();
    assert!(
        late.contains("unknown stream"),
        "unknown stream is an error value (ADR-0012), got {late}"
    );
}

/// exec (the bare cgi shape): one process per call, NO protocol — the
/// request is one JSON document on stdin (closed = the child's cue),
/// the result is stdout whole. Two calls = two processes; nothing
/// survives between them (the `count` handler counts what THIS call's
/// args say — a guard counter could not exist).
#[test]
fn exec_oneshot_runs_per_call() {
    let s = sessions();
    let argv = one_shot_bin();
    let call = |v: Value| {
        s.with_session(
            "box/b1",
            "exec",
            &argv,
            None,
            &effector_runtime::sandbox::SandboxPolicy::None,
            move |sess| sess.call("echo", &v),
        )
        .unwrap()
    };
    assert_eq!(call(json!({"n": 1})), json!({"echoed": {"n": 1}}));
    assert_eq!(call(json!({"n": 2})), json!({"echoed": {"n": 2}}));

    // No residency to sweep: a one-shot slot never parks a child.
    assert!(s.sweep_dead().is_empty(), "one-shot slots are never 'dead'");
}

/// The statelessness is asserted, not assumed. Unified seam (ADR-0036):
/// a Start on a one-shot IS the invoke shape — it runs the script and
/// wraps stdout into `{done:true,value}` (bare script, carrier-wrapped).
/// The STREAM is what a one-shot cannot do: the follow-up Next names the
/// design as an error value (the cgi lineage has no residency to hold a
/// stream — the fix is induction, not a retry).
#[test]
fn exec_oneshot_iterate_is_a_named_error() {
    let s = sessions();
    let argv = one_shot_bin();
    let r = s.with_session(
        "box/b2",
        "exec",
        &argv,
        None,
        &effector_runtime::sandbox::SandboxPolicy::None,
        |sess| {
            let start = sess.iterate(StreamOp::Start {
                stream_id: "s1".into(),
                handler: "count".into(),
                args: json!({"total": 2}),
            })?;
            // Start answers the terminal invoke envelope...
            assert_eq!(
                start.get("done").and_then(|d| d.as_bool()),
                Some(true),
                "a one-shot Start is terminal (invoke shape): {start}"
            );
            // ...and the stream verb after it names the design.
            sess.iterate(StreamOp::Next {
                stream_id: "s1".into(),
                handler: "count".into(),
                args: json!({}),
            })
        },
    );
    let err = r.unwrap_err().to_string();
    assert!(
        err.contains("stateless by definition") && err.contains("induction"),
        "one-shot iterate names the design: {err}"
    );
}

/// Induction residency rule: eviction closes stdin (the loop's EOF) and
/// reaps; a child that died on its own is swept by sweep_dead so the
/// next call cold-starts a fresh spawn.
#[test]
fn eviction_ends_the_residency_and_sweep_clears_the_dead() {
    let s = sessions();
    let argv = induction_bin();
    // Spawn + one call.
    s.with_session(
        "box/k4",
        "induction",
        &argv,
        None,
        &effector_runtime::sandbox::SandboxPolicy::None,
        |sess| sess.call("echo", &json!(1)),
    )
    .unwrap();

    // Kill the child behind the registry's back (simulates a crash):
    // sweep_dead finds it and drops the slot (which reaps + releases).
    let pid = s
        .with_session(
            "box/k4",
            "induction",
            &argv,
            None,
            &effector_runtime::sandbox::SandboxPolicy::None,
            |sess| {
                sess.as_any()
                    .downcast_mut::<effector_runtime::carrier::exec::InductionSession>()
                    .and_then(|e| e.child_pid().map(|p| json!({ "pid": p })))
                    .ok_or_else(|| anyhow::anyhow!("not a InductionSession"))
            },
        )
        .unwrap()["pid"]
        .as_u64()
        .unwrap() as i32;
    // SIGKILL from the outside; delivery is asynchronous — poll until
    // the kernel finishes (WNOHANG reads "alive" inside the delivery
    // window, so a single shot races the signal).
    unsafe { libc::kill(pid, libc::SIGKILL) };
    let mut swept = Vec::new();
    for _ in 0..200 {
        swept = s.sweep_dead();
        if swept.contains(&"box/k4".to_string()) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(
        swept.contains(&"box/k4".to_string()),
        "the dead slot is swept: {swept:?}"
    );
    let out = s
        .with_session(
            "box/k4",
            "induction",
            &argv,
            None,
            &effector_runtime::sandbox::SandboxPolicy::None,
            |sess| sess.call("echo", &json!("after respawn")),
        )
        .unwrap();
    assert_eq!(out, json!({"echoed": "after respawn"}));
}

// ------------------------------------------------- nu induction (two-fifo) --

/// The nushell author script (ADR-0035 §8): `def main [req rep]` IS the
/// loop; the session shape is chosen by the spawn spec's head (`nu`) and
/// the protocol is IDENTICAL to the pipes shape — the fifos replace
/// stdin only because nu cannot block-read a pipe.
fn nu_induction_spec() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/induction_nu.nu");
    assert!(
        path.exists(),
        "nu induction fixture missing: {}",
        path.display()
    );
    format!("nu {}", path.display())
}

/// Round trip over the fifo channels + residency: the `count` handler
/// rides `$env` across calls on the SAME child (the for-loop scope rule
/// the fixture documents — `each` would eat the writes).
#[test]
fn nu_induction_call_round_trip_and_residency() {
    let s = sessions();
    let spec = nu_induction_spec();
    let call = |event: &str, args: Value| {
        s.with_session(
            "box/nu1",
            "induction",
            &spec,
            None,
            &effector_runtime::sandbox::SandboxPolicy::None,
            move |sess| sess.call(event, &args),
        )
        .unwrap()
    };
    assert_eq!(call("echo", json!({"a": 1})), json!({"echoed": {"a": 1}}));
    assert_eq!(
        call("count", json!({})),
        json!({"count": 1}),
        "$env rides the resident child"
    );
    assert_eq!(call("count", json!({})), json!({"count": 2}));
    // The schema frame carries the storage block (the plan the control
    // plane resolves at upload — ADR-0037's typed shape over the nu seam).
    let schema = call("interface_schema", json!({}));
    assert!(
        schema["storage"]["collections"]["counters"]["schema"]["key_len"].is_number(),
        "the nu fixture declares the counters collection: {schema}"
    );
}

/// The ctx seam over the SECOND fifo: the child emits a host frame on
/// stdout, blocks reading `rep`, the parent answers there (never on
/// `req` — two readers on one fifo race; measured deadlock).
#[test]
fn nu_induction_host_call_crosses_the_seam() {
    let seen: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let bridge = {
        let seen = seen.clone();
        let mut b = HostBridge::default();
        b.functions.insert(
            "invoke".into(),
            Arc::new(move |arg: Value| {
                seen.lock().unwrap().push(arg.clone());
                Ok(json!({"from": "host", "echo": arg}))
            }) as effector_runtime::carrier::HostFn,
        );
        b
    };
    let s = sessions();
    let spec = nu_induction_spec();
    let out = s
        .with_session(
            "box/nu2",
            "induction",
            &spec,
            Some(&bridge),
            &effector_runtime::sandbox::SandboxPolicy::None,
            // the CANONICAL invoke payload (ADR-0001): the child forwards
            // it verbatim into the host frame's args, where the shared
            // dispatcher decodes the nested booth shape — an opaque
            // payload was decode-failure bait the old unvalidated carrier
            // let through
            |sess| {
                sess.call(
                    "ctx_round_trip",
                    &json!({"type": "t", "key": "k", "handler": "h", "args": {"q": 1}}),
                )
            },
        )
        .unwrap();
    assert_eq!(seen.lock().unwrap().len(), 1, "the host fn ran once");
    assert_eq!(
        out["invoked"]["from"], "host",
        "the parent's rep-fifo answer reached the child: {out}"
    );
}

/// The envelope stream over the nu adapter, guard state in `$env` —
/// start carries the first item, exhaustion WRITES done (the same
/// ADR-0034 shape the Rust induction fixture locks; nu lands it with no
/// generator language, only `$env`).
#[test]
fn nu_induction_iterate_stream() {
    let s = sessions();
    let spec = nu_induction_spec();
    let pull = |op: StreamOp| -> Value {
        s.with_session(
            "box/nu3",
            "induction",
            &spec,
            None,
            &effector_runtime::sandbox::SandboxPolicy::None,
            move |sess| sess.iterate(op.clone()),
        )
        .unwrap()
    };
    let mut envs = vec![pull(StreamOp::Start {
        stream_id: "s1".into(),
        handler: "stream".into(),
        args: json!({"total": 3}),
    })];
    for _ in 0..4 {
        let e = pull(StreamOp::Next {
            stream_id: "s1".into(),
            handler: "stream".into(),
            args: json!({}),
        });
        let done = e.get("done").and_then(|d| d.as_bool()) == Some(true);
        envs.push(e);
        if done {
            break;
        }
    }
    assert_eq!(envs[0], json!({"item": "i0", "done": false}));
    assert_eq!(envs[1], json!({"item": "i1", "done": false}));
    assert_eq!(envs[2], json!({"item": "i2", "done": false}));
    assert_eq!(
        envs.last().unwrap(),
        &json!({"done": true}),
        "exhaustion writes done"
    );
}

/// Teardown discipline of the fifo shape: eviction kills the child and
/// removes the session dir (the blocked `open` cannot be EOF-released
/// like a pipe — unlink alone races with the open).
#[test]
fn nu_induction_eviction_reaps_the_child_and_clears_the_dir() {
    let s = sessions();
    let spec = nu_induction_spec();
    s.with_session(
        "box/nu4",
        "induction",
        &spec,
        None,
        &effector_runtime::sandbox::SandboxPolicy::None,
        |sess| sess.call("echo", &json!("x")),
    )
    .unwrap();
    let got = s
        .with_session(
            "box/nu4",
            "induction",
            &spec,
            None,
            &effector_runtime::sandbox::SandboxPolicy::None,
            |sess| {
                let b = sess
                    .as_any()
                    .downcast_mut::<effector_runtime::carrier::exec::InductionSession>()
                    .ok_or_else(|| anyhow::anyhow!("not a InductionSession"))?;
                let pid = b.child_pid().ok_or_else(|| anyhow::anyhow!("no child"))?;
                let dir = b
                    .session_dir()
                    .ok_or_else(|| anyhow::anyhow!("no session dir"))?
                    .to_path_buf();
                Ok(json!({"pid": pid, "dir": dir.display().to_string()}))
            },
        )
        .unwrap();
    let pid = got["pid"].as_u64().unwrap() as i32;
    let dir = PathBuf::from(got["dir"].as_str().unwrap());
    assert!(dir.exists(), "the session dir exists while the child lives");

    s.evict("box/nu4");
    // Reaped: the pid is gone (ESRCH) — poll for the kernel's delivery.
    let mut dead = false;
    for _ in 0..200 {
        dead = unsafe { libc::kill(pid, 0) } != 0;
        if dead {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(
        dead,
        "the evicted nu child is reaped (pid {pid} still alive)"
    );
    // The session dir is removed on teardown (no fifo litter) — THIS
    // session's dir, never a glob (parallel tests own their dirs).
    for _ in 0..50 {
        if !dir.exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(
        !dir.exists(),
        "the session dir is cleaned on teardown: {}",
        dir.display()
    );
}

// ------------------------------------------------- ADR-0037 §2 locks --

/// The declared CBOR codec crosses the induction pipes channel: the parent
/// writes self-delimited CBOR documents, the child decodes them and
/// answers the same way — the echo round trip works with NO line
/// protocol in between (the frame shape is unchanged; only the codec
/// differs).
#[test]
fn induction_cbor_round_trip() {
    use induction_protocol::Encoding;
    let s = sessions();
    let argv = induction_bin();
    let out = s
        .with_session_encoded(
            "box/cbor1",
            "induction",
            &argv,
            None,
            &effector_runtime::sandbox::SandboxPolicy::None,
            Encoding::Cbor,
            |sess| sess.call("echo", &json!({"x": 1})),
        )
        .unwrap();
    assert_eq!(out, json!({"echoed": {"x": 1}}), "CBOR frames round trip");

    // Residency holds across calls on the same codec (one child).
    let out2 = s
        .with_session_encoded(
            "box/cbor1",
            "induction",
            &argv,
            None,
            &effector_runtime::sandbox::SandboxPolicy::None,
            Encoding::Cbor,
            |sess| {
                sess.iterate(StreamOp::Start {
                    stream_id: "cs1".into(),
                    handler: "count".into(),
                    args: json!({"total": 2}),
                })
            },
        )
        .unwrap();
    assert_eq!(out2["item"], "i0", "the second call rides the same child");
}

/// The CBOR ctx seam: the child sends a TYPED host frame (CBOR), the
/// parent decodes it into the typed enum, runs the bridge fn, answers
/// with a CBOR host_reply — the whole duplex on the binary codec.
#[test]
fn induction_cbor_host_call_crosses_the_seam() {
    use induction_protocol::Encoding;
    let seen: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let bridge = {
        let seen = seen.clone();
        let mut b = HostBridge::default();
        b.functions.insert(
            "invoke".into(),
            Arc::new(move |arg: Value| {
                seen.lock().unwrap().push(arg.clone());
                Ok(json!({"from": "host", "echo": arg}))
            }) as effector_runtime::carrier::HostFn,
        );
        b
    };
    let s = sessions();
    let argv = induction_bin();
    let out = s
        .with_session_encoded(
            "box/cbor2",
            "induction",
            &argv,
            Some(&bridge),
            &effector_runtime::sandbox::SandboxPolicy::None,
            Encoding::Cbor,
            // canonical booth payload (see the nu seam test's note): the
            // shared dispatcher decodes the nested shape on the CBOR leg too
            |sess| {
                sess.call(
                    "ctx_round_trip",
                    &json!({"type": "t", "key": "k", "handler": "h", "args": {"q": 1}}),
                )
            },
        )
        .unwrap();
    assert_eq!(seen.lock().unwrap().len(), 1, "the host fn ran once");
    assert_eq!(
        out["from"], "host",
        "the CBOR answer reached the child: {out}"
    );
}

/// The typing kills the document-layer mistake (ADR-0037 §2): a child
/// sending the RETIRED untyped shape (`{"op": "invoke"}` with no
/// discriminator) fails at the parent's typed-frame DECODE — the bridge
/// fn never runs, and the child sees an error reply (no `ok`). The free
/// op-name lookup that silently missed the table is gone.
#[test]
fn induction_untyped_host_frame_fails_at_decode() {
    let seen: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let bridge = {
        let seen = seen.clone();
        let mut b = HostBridge::default();
        b.functions.insert(
            "invoke".into(),
            Arc::new(move |arg: Value| {
                seen.lock().unwrap().push(arg.clone());
                Ok(json!({"from": "host"}))
            }) as effector_runtime::carrier::HostFn,
        );
        b
    };
    let s = sessions();
    let argv = induction_bin();
    let out = s
        .with_session(
            "box/untyped",
            "induction",
            &argv,
            Some(&bridge),
            &effector_runtime::sandbox::SandboxPolicy::None,
            |sess| sess.call("bad_host_frame", &json!({"q": 1})),
        )
        .unwrap();
    assert!(
        seen.lock().unwrap().is_empty(),
        "the bridge fn must NOT run on an untyped frame"
    );
    // The parent's error reply carries no `ok` — the fixture's host
    // helper forwards the error string itself (surfaced, not swallowed
    // to Null): the error value crossed the seam.
    assert!(
        out.as_str().is_some_and(|s| s.contains("bad host frame"))
            || out.get("note").is_some()
            || out.get("error").is_some(),
        "the decode failure landed as an error value, got: {out}"
    );
}

/// exec (the bare cgi shape) rides the SAME declared codec (ADR-0037
/// §2): one CBOR document in (stdin closed), one CBOR document out —
/// protocol-free shape, binary codec.
#[test]
fn exec_cbor_round_trip() {
    use induction_protocol::Encoding;
    let s = sessions();
    let argv = one_shot_bin();
    let out = s
        .with_session_encoded(
            "box/exec-cbor",
            "exec",
            &argv,
            None,
            &effector_runtime::sandbox::SandboxPolicy::None,
            Encoding::Cbor,
            |sess| sess.call("echo", &json!({"n": 1})),
        )
        .unwrap();
    assert_eq!(out, json!({"echoed": {"n": 1}}), "one-shot CBOR round trip");
}

/// A CBOR declaration against the nushell fifo shape is a DECLARATION
/// ERROR, not a silent downgrade (nu has no CBOR codec — the entrance
/// criterion routes it to JSON; the spec must say so).
#[test]
fn nu_cbor_declaration_is_an_error() {
    use induction_protocol::Encoding;
    let s = sessions();
    let spec = nu_induction_spec();
    let err = s
        .with_session_encoded(
            "box/nu-cbor-bad",
            "induction",
            &spec,
            None,
            &effector_runtime::sandbox::SandboxPolicy::None,
            Encoding::Cbor,
            |sess| sess.call("echo", &json!({})),
        )
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("JSON-lines only"),
        "named the codec conflict: {err}"
    );
}

// -------------------------------------------------------- deploy runners --

/// The node's runner artifacts (gravity ADR-0001 §4) live in `deploy/runners/`
/// — a spawn spec like any other fixture, so a missing one fails loudly.
fn runner_spec(file: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy/runners")
        .join(file);
    assert!(path.exists(), "runner artifact missing: {}", path.display());
    format!("python3 {}", path.display())
}

/// A bridge whose `invoke` arm records every frame it is handed (the
/// observation point for both tests below).
fn recording_bridge(seen: &Arc<Mutex<Vec<Value>>>, tag: &'static str) -> HostBridge {
    let seen = seen.clone();
    let mut b = HostBridge::default();
    b.functions.insert(
        "invoke".into(),
        Arc::new(move |arg: Value| {
            seen.lock().unwrap().push(arg.clone());
            Ok(json!({ "answered_by": tag, "echo": arg }))
        }) as effector_runtime::carrier::HostFn,
    );
    b
}

/// The bridge is per-CALL data, the session is not (the two clocks ADR-0001
/// separates): a second call through one resident session must reach the
/// SECOND call's bridge. A session that kept its cold start's bridge answers
/// with a call id nobody is waiting on any more — over the wire that is "the
/// enclosing remote call is not in flight", an error VALUE and never a
/// timeout, which is how the two failure shapes are told apart.
#[test]
fn induction_resident_session_rearms_the_bridge_per_call() {
    let first: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let second: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let s = sessions();
    let argv = induction_bin();
    let args = json!({ "type": "t", "key": "k", "handler": "h", "args": { "q": 1 } });
    let b1 = recording_bridge(&first, "first");
    let out1 = s
        .with_session(
            "box/bridge-clock",
            "induction",
            &argv,
            Some(&b1),
            &effector_runtime::sandbox::SandboxPolicy::None,
            |sess| sess.call("ctx_round_trip", &args),
        )
        .unwrap();
    assert_eq!(out1["answered_by"], "first");
    let b2 = recording_bridge(&second, "second");
    let out2 = s
        .with_session(
            "box/bridge-clock",
            "induction",
            &argv,
            Some(&b2),
            &effector_runtime::sandbox::SandboxPolicy::None,
            |sess| sess.call("ctx_round_trip", &args),
        )
        .unwrap();
    assert_eq!(
        out2["answered_by"], "second",
        "the second call's ctx frame reached the second call's bridge"
    );
    assert_eq!(
        first.lock().unwrap().len(),
        1,
        "the cold start's bridge is not answering later calls"
    );
    assert_eq!(second.lock().unwrap().len(), 1);
}

/// The python resident runner (a node artifact, not a fixture) against a
/// bridge: the delivered script runs with `sys.stdout` REDIRECTED into the
/// result capture buffer, so the runner's protocol writer must ride fds
/// pinned at import. The bridge is the observation point — a guest whose
/// frame landed in the capture buffer leaves it unfired and parks the call to
/// its deadline, which is the failure this test exists to prevent.
#[test]
fn the_resident_runner_keeps_its_ctx_frames_on_the_wire() {
    let seen: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let bridge = recording_bridge(&seen, "host");
    let s = sessions();
    let spec = runner_spec("runner_py_guest.py");
    let out = s
        .with_session_encoded(
            "box/runner-guest",
            "induction",
            &spec,
            Some(&bridge),
            &effector_runtime::sandbox::SandboxPolicy::None,
            induction_protocol::Encoding::Json,
            |sess| {
                sess.iterate(StreamOp::Start {
                    stream_id: "s1".into(),
                    handler: "execute".into(),
                    args: json!({
                        "code": "import json\n\
                                 r = ctx_invoke('sibling', '', 'execute', {'q': 1})\n\
                                 print(json.dumps({'bridged': r}))"
                    }),
                })
            },
        )
        .unwrap();
    let frames = seen.lock().unwrap().clone();
    assert_eq!(
        frames.len(),
        1,
        "the runner's ctx frame reached the bridge: {frames:?}"
    );
    assert_eq!(frames[0]["type"], "sibling");
    assert_eq!(frames[0]["handler"], "execute");
    assert_eq!(frames[0]["args"], json!({ "q": 1 }));
    assert_eq!(
        out,
        json!({ "done": true, "value": { "bridged": { "answered_by": "host", "echo": frames[0] } } }),
        "the delivered script's own result came back through the envelope"
    );
}
