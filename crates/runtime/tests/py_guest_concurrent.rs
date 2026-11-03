//! The python resident runner as a CONCURRENT induction guest (induction
//! ADR-0002, aura ADR-0046 `unbounded`): one process, several requests at once,
//! each ctx frame attributed to the request it serves.
//!
//! The runner is a deployment artifact (`deploy/runners/runner_py_guest.py`), not
//! a cargo target: these tests spawn `python3 <that file>` the way a node mount
//! does. A missing `python3` fails the test loudly — a stale environment is a
//! recipe error, never a skip.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use effector_runtime::carrier::session::{Sessions, StreamOp};
use effector_runtime::carrier::HostBridge;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn guest() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy/runners/runner_py_guest.py");
    assert!(path.exists(), "the python guest is gone: {}", path.display());
    format!("python3 {}", path.display())
}

fn run(code: &str, args: Value, stream: &str) -> StreamOp {
    StreamOp::Start {
        stream_id: stream.to_string(),
        handler: "execute".to_string(),
        args: json!({ "code": code, "args": args }),
    }
}

/// Two requests land in one process while the first is still working: the runner
/// dispatches each to its own thread, so 0.8 s + 0.8 s finishes in about 0.8 s.
/// Queued (the serial runner this replaced) it takes 1.6 s.
#[test]
fn two_python_requests_run_at_once_in_one_process() {
    let sessions = Sessions::new();
    let argv = guest();
    let code = "import time\ntime.sleep(0.8)\nprint(json.dumps({'n': ARGS['n']}))";
    let started = Instant::now();
    let mut handles = Vec::new();
    for n in 0..2 {
        let sessions = sessions.clone();
        let argv = argv.clone();
        handles.push(std::thread::spawn(move || {
            sessions
                .with_shared(
                    "box/py-concurrent",
                    "induction",
                    &argv,
                    &effector_runtime::sandbox::SandboxPolicy::None,
                    induction_protocol::Encoding::Json,
                    None,
                    |shared| shared.iterate(run(code, json!({ "n": n }), &format!("s{n}")), None),
                )
                .unwrap()
        }));
    }
    let mut answers: Vec<Value> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let elapsed = started.elapsed();
    answers.sort_by_key(|v| v["value"]["n"].as_u64());
    assert_eq!(answers[0]["value"], json!({ "n": 0 }));
    assert_eq!(answers[1]["value"], json!({ "n": 1 }));
    assert!(
        elapsed < Duration::from_millis(1400),
        "the two requests overlapped (took {elapsed:?}); a serial runner takes 1.6 s"
    );
}

/// Attribution, not just parallelism: each request carries its OWN bridge, and a
/// ctx call the guest makes must be answered from the bridge of the request it
/// serves. The delivered code sleeps first so both requests are inside the ctx
/// call at once — the case where "answer the last call's bridge" is wrong.
#[test]
fn a_ctx_call_is_answered_by_its_own_requests_bridge() {
    let sessions = Sessions::new();
    let argv = guest();
    let code = "import time\ntime.sleep(0.5)\n\
                r = ctx_invoke('sibling', 'k', 'echo', {'x': 1})\n\
                print(json.dumps({'marker': r['who']}))";
    let mut handles = Vec::new();
    for tag in ["A", "B"] {
        let sessions = sessions.clone();
        let argv = argv.clone();
        let bridge = HostBridge {
            functions: BTreeMap::from([(
                "invoke".to_string(),
                Arc::new(move |_args: Value| Ok(json!({ "who": tag }))) as _,
            )]),
            storage: None,
        };
        handles.push(std::thread::spawn(move || {
            sessions
                .with_shared(
                    "box/py-attribution",
                    "induction",
                    &argv,
                    &effector_runtime::sandbox::SandboxPolicy::None,
                    induction_protocol::Encoding::Json,
                    Some(&bridge),
                    |shared| shared.iterate(run(code, json!({}), &format!("s{tag}")), Some(&bridge)),
                )
                .unwrap()
        }));
    }
    let answers: Vec<Value> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let markers: Vec<&str> = answers
        .iter()
        .map(|a| a["value"]["marker"].as_str().unwrap_or("<none>"))
        .collect();
    assert!(
        markers.contains(&"A") && markers.contains(&"B"),
        "each ctx call came back from its own request's bridge: {markers:?}"
    );
}