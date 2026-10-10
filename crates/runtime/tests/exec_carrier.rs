//! Exec/BGI carrier tests (ADR-0035): the framed resident shape (bgi —
//! the `bgi_loop` fixture: the line protocol, call, ctx round trip, the
//! iterate verbs, residency/sweep rules) and the bare one-shot shape
//! (exec — the `one_shot` fixture: no protocol, one JSON in, one JSON
//! out, nothing survives the call).
//!
//! Fixtures are built by the workspace (`cargo build -p actor-guest
//! --examples`); a missing binary fails the test loudly — a stale build
//! is a recipe error, never a skip.

use effector_runtime::carrier::session::StreamOp;
use effector_runtime::carrier::HostBridge;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

fn bgi_bin() -> String {
    bin("bgi_loop")
}

fn one_shot_bin() -> String {
    bin("one_shot")
}

fn bin(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../../target/debug/examples/{name}"));
    assert!(
        path.exists(),
        "{name} missing — build it: cargo build -p actor-guest --examples"
    );
    path.to_str().unwrap().to_string()
}

fn sessions() -> effector_runtime::carrier::session::Sessions {
    effector_runtime::carrier::session::Sessions::new()
}

/// Plain call through the resident session: request frame out, result
/// frame back — the invoke shape across the process boundary.
#[test]
fn bgi_call_round_trip() {
    let s = sessions();
    let argv = bgi_bin();
    let out = s
        .with_session(
            "box/k1",
            "bgi",
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
            "bgi",
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
fn bgi_host_call_crosses_the_seam() {
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
    let argv = bgi_bin();
    let out = s
        .with_session(
            "box/k2",
            "bgi",
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

/// ADR-0034 over the bgi seam: start carries the first envelope,
/// next advances the guard the CHILD keeps in its own memory (the
/// residency is the stream state — the envelope producer has no
/// generator, and it does not need one), done is written.
#[test]
fn bgi_iterate_stream() {
    let s = sessions();
    let argv = bgi_bin();
    // One with_session per pull — each call re-enters the SAME child
    // (the registry keys residency; the guard state lives in the
    // child's memory across the calls, exactly like a session VM).
    let pull = |op: StreamOp| -> Value {
        s.with_session(
            "box/k3",
            "bgi",
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
    assert_eq!(envs[0], json!({"item": "i0", "done": false}), "start carries the first item");
    assert_eq!(envs[1], json!({"item": "i1", "done": false}));
    assert_eq!(envs[2], json!({"item": "i2", "done": false}));
    assert_eq!(envs.last().unwrap(), &json!({"done": true}), "exhaustion writes done");

    // Pull-after-done: the child's guard is gone — a failed pull (an
    // error value through the stream), never a resurrection.
    let late = s
        .with_session(
            "box/k3",
            "bgi",
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
/// stream — the fix is bgi, not a retry).
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
        err.contains("stateless by definition") && err.contains("bgi"),
        "one-shot iterate names the design: {err}"
    );
}

/// BGI residency rule: eviction closes stdin (the loop's EOF) and
/// reaps; a child that died on its own is swept by sweep_dead so the
/// next call cold-starts a fresh spawn.
#[test]
fn eviction_ends_the_residency_and_sweep_clears_the_dead() {
    let s = sessions();
    let argv = bgi_bin();
    // Spawn + one call.
    s.with_session("box/k4", "bgi", &argv, None, &effector_runtime::sandbox::SandboxPolicy::None, |sess| {
        sess.call("echo", &json!(1))
    })
    .unwrap();

    // Kill the child behind the registry's back (simulates a crash):
    // sweep_dead finds it and drops the slot (which reaps + releases).
    let pid = s
        .with_session(
            "box/k4",
            "bgi",
            &argv,
            None,
            &effector_runtime::sandbox::SandboxPolicy::None,
            |sess| {
                sess.as_any()
                    .downcast_mut::<effector_runtime::carrier::exec::BgiSession>()
                    .and_then(|e| e.child_pid().map(|p| json!({ "pid": p })))
                    .ok_or_else(|| anyhow::anyhow!("not a bgi session"))
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
    assert!(swept.contains(&"box/k4".to_string()), "the dead slot is swept: {swept:?}");
    let out = s
        .with_session("box/k4", "bgi", &argv, None, &effector_runtime::sandbox::SandboxPolicy::None, |sess| {
            sess.call("echo", &json!("after respawn"))
        })
        .unwrap();
    assert_eq!(out, json!({"echoed": "after respawn"}));
}

// ----------------------------------------------------- nu bgi (two-fifo) --

/// The nushell author script (ADR-0035 §8): `def main [req rep]` IS the
/// loop; the session shape is chosen by the spawn spec's head (`nu`) and
/// the protocol is IDENTICAL to the pipes shape — the fifos replace
/// stdin only because nu cannot block-read a pipe.
fn nu_bgi_spec() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../actor-guest/examples/bgi_nu.nu");
    assert!(path.exists(), "nu bgi fixture missing: {}", path.display());
    format!("nu {}", path.display())
}

/// Round trip over the fifo channels + residency: the `count` handler
/// rides `$env` across calls on the SAME child (the for-loop scope rule
/// the fixture documents — `each` would eat the writes).
#[test]
fn nu_bgi_call_round_trip_and_residency() {
    let s = sessions();
    let spec = nu_bgi_spec();
    let call = |event: &str, args: Value| {
        s.with_session(
            "box/nu1",
            "bgi",
            &spec,
            None,
            &effector_runtime::sandbox::SandboxPolicy::None,
            move |sess| sess.call(event, &args),
        )
        .unwrap()
    };
    assert_eq!(call("echo", json!({"a": 1})), json!({"echoed": {"a": 1}}));
    assert_eq!(call("count", json!({})), json!({"count": 1}), "$env rides the resident child");
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
fn nu_bgi_host_call_crosses_the_seam() {
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
    let spec = nu_bgi_spec();
    let out = s
        .with_session(
            "box/nu2",
            "bgi",
            &spec,
            Some(&bridge),
            &effector_runtime::sandbox::SandboxPolicy::None,
            |sess| sess.call("ctx_round_trip", &json!({"q": 1})),
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
/// ADR-0034 shape the Rust bgi fixture locks; nu lands it with no
/// generator language, only `$env`).
#[test]
fn nu_bgi_iterate_stream() {
    let s = sessions();
    let spec = nu_bgi_spec();
    let pull = |op: StreamOp| -> Value {
        s.with_session(
            "box/nu3",
            "bgi",
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
    assert_eq!(envs.last().unwrap(), &json!({"done": true}), "exhaustion writes done");
}

/// Teardown discipline of the fifo shape: eviction kills the child and
/// removes the session dir (the blocked `open` cannot be EOF-released
/// like a pipe — unlink alone races with the open).
#[test]
fn nu_bgi_eviction_reaps_the_child_and_clears_the_dir() {
    let s = sessions();
    let spec = nu_bgi_spec();
    s.with_session("box/nu4", "bgi", &spec, None, &effector_runtime::sandbox::SandboxPolicy::None, |sess| {
        sess.call("echo", &json!("x"))
    })
    .unwrap();
    let got = s
        .with_session("box/nu4", "bgi", &spec, None, &effector_runtime::sandbox::SandboxPolicy::None, |sess| {
            let b = sess
                .as_any()
                .downcast_mut::<effector_runtime::carrier::exec::BgiSession>()
                .ok_or_else(|| anyhow::anyhow!("not a bgi session"))?;
            let pid = b.child_pid().ok_or_else(|| anyhow::anyhow!("no child"))?;
            let dir = b
                .session_dir()
                .ok_or_else(|| anyhow::anyhow!("no session dir"))?
                .to_path_buf();
            Ok(json!({"pid": pid, "dir": dir.display().to_string()}))
        })
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
    assert!(dead, "the evicted nu child is reaped (pid {pid} still alive)");
    // The session dir is removed on teardown (no fifo litter) — THIS
    // session's dir, never a glob (parallel tests own their dirs).
    for _ in 0..50 {
        if !dir.exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(!dir.exists(), "the session dir is cleaned on teardown: {}", dir.display());
}

// ------------------------------------------------- ADR-0037 §2 locks --

/// The declared CBOR codec crosses the bgi pipes channel: the parent
/// writes self-delimited CBOR documents, the child decodes them and
/// answers the same way — the echo round trip works with NO line
/// protocol in between (the frame shape is unchanged; only the codec
/// differs).
#[test]
fn bgi_cbor_round_trip() {
    use effector_protocol::ChannelEncoding;
    let s = sessions();
    let argv = bgi_bin();
    let out = s
        .with_session_encoded(
            "box/cbor1",
            "bgi",
            &argv,
            None,
            &effector_runtime::sandbox::SandboxPolicy::None,
            ChannelEncoding::Cbor,
            |sess| sess.call("echo", &json!({"x": 1})),
        )
        .unwrap();
    assert_eq!(out, json!({"echoed": {"x": 1}}), "CBOR frames round trip");

    // Residency holds across calls on the same codec (one child).
    let out2 = s
        .with_session_encoded(
            "box/cbor1",
            "bgi",
            &argv,
            None,
            &effector_runtime::sandbox::SandboxPolicy::None,
            ChannelEncoding::Cbor,
            |sess| sess.iterate(StreamOp::Start {
                stream_id: "cs1".into(),
                handler: "count".into(),
                args: json!({"total": 2}),
            }),
        )
        .unwrap();
    assert_eq!(out2["item"], "i0", "the second call rides the same child");
}

/// The CBOR ctx seam: the child sends a TYPED host frame (CBOR), the
/// parent decodes it into the typed enum, runs the bridge fn, answers
/// with a CBOR host_reply — the whole duplex on the binary codec.
#[test]
fn bgi_cbor_host_call_crosses_the_seam() {
    use effector_protocol::ChannelEncoding;
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
    let argv = bgi_bin();
    let out = s
        .with_session_encoded(
            "box/cbor2",
            "bgi",
            &argv,
            Some(&bridge),
            &effector_runtime::sandbox::SandboxPolicy::None,
            ChannelEncoding::Cbor,
            |sess| sess.call("ctx_round_trip", &json!({"q": 1})),
        )
        .unwrap();
    assert_eq!(seen.lock().unwrap().len(), 1, "the host fn ran once");
    assert_eq!(out["from"], "host", "the CBOR answer reached the child: {out}");
}

/// The typing kills the document-layer mistake (ADR-0037 §2): a child
/// sending the RETIRED untyped shape (`{"op": "invoke"}` with no
/// discriminator) fails at the parent's typed-frame DECODE — the bridge
/// fn never runs, and the child sees an error reply (no `ok`). The free
/// op-name lookup that silently missed the table is gone.
#[test]
fn bgi_untyped_host_frame_fails_at_decode() {
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
    let argv = bgi_bin();
    let out = s
        .with_session(
            "box/untyped",
            "bgi",
            &argv,
            Some(&bridge),
            &effector_runtime::sandbox::SandboxPolicy::None,
            |sess| sess.call("bad_host_frame", &json!({"q": 1})),
        )
        .unwrap();
    assert!(seen.lock().unwrap().is_empty(), "the bridge fn must NOT run on an untyped frame");
    // The parent's error reply carries no `ok` — the child's helper
    // lands the recorded note (the error value crossed the seam).
    assert!(
        out.get("note").is_some() || out.get("error").is_some(),
        "the decode failure surfaced as an error value, got: {out}"
    );
}

/// exec (the bare cgi shape) rides the SAME declared codec (ADR-0037
/// §2): one CBOR document in (stdin closed), one CBOR document out —
/// protocol-free shape, binary codec.
#[test]
fn exec_cbor_round_trip() {
    use effector_protocol::ChannelEncoding;
    let s = sessions();
    let argv = one_shot_bin();
    let out = s
        .with_session_encoded(
            "box/exec-cbor",
            "exec",
            &argv,
            None,
            &effector_runtime::sandbox::SandboxPolicy::None,
            ChannelEncoding::Cbor,
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
    use effector_protocol::ChannelEncoding;
    let s = sessions();
    let spec = nu_bgi_spec();
    let err = s
        .with_session_encoded(
            "box/nu-cbor-bad",
            "bgi",
            &spec,
            None,
            &effector_runtime::sandbox::SandboxPolicy::None,
            ChannelEncoding::Cbor,
            |sess| sess.call("echo", &json!({})),
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains("JSON-lines only"), "named the codec conflict: {err}");
}
