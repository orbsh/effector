#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]
use effector_runtime::carrier::executor::Executors;
use effector_runtime::carrier::HostBridge;

// The python cases below need the python carrier: gated per-test (not
// file-level [[test]]) so the steel coverage still runs in default builds —
// steel is a default feature, python is not, and a default-feature
// `cargo test --workspace` must not fail on a carrier that was never
// compiled in (with_executor returns an error value → unwrap).
#[cfg(feature = "python")]
#[test]
fn python_event_name_addressing() {
    // @on handlers are bound under the EVENT name — delivery addresses
    // handler="add_to_cart" resolves directly, no execute fallback. The
    // executor is resident: load once, call per event.
    let src = r##"
@on("add_to_cart", key="user_id")
def add(args):
    return {"added": args["item"]}

@on("remove_from_cart")
def remove(args):
    return {"removed": True}
"##;
    let executors = Executors::new();
    let out = executors
        .with_executor(
            "inst-1",
            "python",
            src,
            None::<&HostBridge>,
            &effector_runtime::sandbox::SandboxPolicy::None,
            |s: &mut dyn effector_runtime::carrier::executor::Executor| {
                s.call(
                    "add_to_cart",
                    &serde_json::json!({"item": "book", "user_id": "u1"}),
                )
            },
        )
        .unwrap();
    println!("PY: {out}");
    assert_eq!(out["added"], "book");
}

#[test]
fn steel_event_name_addressing() {
    // (on ...) binds the handler under the event name in the VM; resident.
    let src = r##"
(on "add_to_cart" (hash "key" "user_id") (lambda (args) (hash "added" (hash-ref args "item"))))
"##;
    let executors = Executors::new();
    let out = executors
        .with_executor(
            "inst-1",
            "steel",
            src,
            None::<&HostBridge>,
            &effector_runtime::sandbox::SandboxPolicy::None,
            |s: &mut dyn effector_runtime::carrier::executor::Executor| {
                s.call("add_to_cart", &serde_json::json!({"item": "book"}))
            },
        )
        .unwrap();
    println!("STEEL: {out}");
    assert_eq!(out["added"], "book");
}

// mixed-language uniformity test: the python half is load-bearing, so the
// whole test needs both carriers present
#[cfg(feature = "python")]
#[test]
fn executors_keep_state_across_calls_and_evict_drops_it() {
    // Resident semantics, language-uniform: same executor key = same VM.
    // State written by one call is visible to the next; evict() destroys it.
    let py_src = r##"
counter = 0

@on("bump")
def bump(args):
    global counter
    counter = counter + args["by"]
    return {"count": counter}
"##;
    let steel_src = r##"
(define count 0)
(on "bump" (hash) (lambda (args)
  (set! count (+ count (hash-ref args "by")))
  (hash "count" count)))
"##;
    let executors = Executors::new();
    let host: Option<&HostBridge> = None;

    let out = executors
        .with_executor(
            "a1",
            "python",
            py_src,
            host,
            &effector_runtime::sandbox::SandboxPolicy::None,
            |s: &mut dyn effector_runtime::carrier::executor::Executor| {
                s.call("bump", &serde_json::json!({"by": 1}))
            },
        )
        .unwrap();
    assert_eq!(out["count"], 1);
    let out = executors
        .with_executor(
            "a1",
            "python",
            py_src,
            host,
            &effector_runtime::sandbox::SandboxPolicy::None,
            |s: &mut dyn effector_runtime::carrier::executor::Executor| {
                s.call("bump", &serde_json::json!({"by": 1}))
            },
        )
        .unwrap();
    assert_eq!(out["count"], 2, "python: same instance accumulates");

    let out = executors
        .with_executor(
            "s1",
            "steel",
            steel_src,
            host,
            &effector_runtime::sandbox::SandboxPolicy::None,
            |s: &mut dyn effector_runtime::carrier::executor::Executor| {
                s.call("bump", &serde_json::json!({"by": 5}))
            },
        )
        .unwrap();
    assert_eq!(out["count"], 5);
    let out = executors
        .with_executor(
            "s1",
            "steel",
            steel_src,
            host,
            &effector_runtime::sandbox::SandboxPolicy::None,
            |s: &mut dyn effector_runtime::carrier::executor::Executor| {
                s.call("bump", &serde_json::json!({"by": 5}))
            },
        )
        .unwrap();
    assert_eq!(out["count"], 10, "steel: same instance accumulates");

    // A DIFFERENT instance key = a fresh executor (no cross-talk).
    let out = executors
        .with_executor(
            "a2",
            "python",
            py_src,
            host,
            &effector_runtime::sandbox::SandboxPolicy::None,
            |s: &mut dyn effector_runtime::carrier::executor::Executor| {
                s.call("bump", &serde_json::json!({"by": 100}))
            },
        )
        .unwrap();
    assert_eq!(out["count"], 100, "fresh instance starts at zero");

    // Evict wipes the instance; the next call rebuilds from zero.
    executors.evict("a1");
    let out = executors
        .with_executor(
            "a1",
            "python",
            py_src,
            host,
            &effector_runtime::sandbox::SandboxPolicy::None,
            |s: &mut dyn effector_runtime::carrier::executor::Executor| {
                s.call("bump", &serde_json::json!({"by": 1}))
            },
        )
        .unwrap();
    assert_eq!(out["count"], 1, "evicted instance restarted fresh");
}
