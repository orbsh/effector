//! Wasm carrier contract: resident session, CBOR pointer ABI, export-list
//! schema derivation, host imports. Fixtures are hand-written WAT — the
//! full ABI is exercised without a rustc wasm target.

use effector_runtime::carrier::session::Sessions;
use effector_runtime::carrier::{HostBridge, HostFn};
use serde_json::Value;
use std::sync::Arc;

/// Pack helper shared by the fixtures (ptr, len) -> i64, and a bump
/// allocator over 64 KiB starting at 1024.
macro_rules! counter_module {
    ($host_import:expr) => { concat!(r#"
(module
"#, $host_import, r#"
  (memory (export "memory") 1)
  (global $g (mut i32) (i32.const 0))
  (global $heap (mut i32) (i32.const 1024))
  (func (export "aura_alloc") (param $len i32) (result i32)
    (local $p i32)
    (local.set $p (global.get $heap))
    (global.set $heap (i32.add (global.get $heap) (local.get $len)))
    (local.get $p))
  (func $pack (param $p i32) (param $l i32) (result i64)
    (i64.or (i64.shl (i64.extend_i32_u (local.get $p)) (i64.const 32))
            (i64.extend_i32_u (local.get $l))))
  ;; counter handler: ignores arg bytes (ABI exercised anyway), bumps the
  ;; global, replies with a 1-byte CBOR fixint of the counter value.
  (func (export "bump") (param i32 i32) (result i64)
    (global.set $g (i32.add (global.get $g) (i32.const 1)))
    (i32.store8 (i32.const 0) (global.get $g))
    (call $pack (i32.const 0) (i32.const 1)))
  (func (export "peek") (param i32 i32) (result i64)
    (i32.store8 (i32.const 0) (global.get $g))
    (call $pack (i32.const 0) (i32.const 1))))"#) };
}

/// Pure module: no host import — instantiates with no bridge. The counter
/// reply is a 1-byte CBOR fixint of the counter value (fixints 0x00..0x17
/// encode themselves; the fixture only counts 1 and 2).
const PURE_WAT: &str = counter_module!("");

/// Module that imports `aura_host.double` (host doubles a CBOR fixint).
const HOST_WAT: &str = counter_module!(
    r#"(import "aura_host" "double" (func $double (param i32 i32) (result i64)))
  (func (export "hosted") (param i32 i32) (result i64)
    ;; 1-byte CBOR arg (fixint 5) parked at 512
    (i32.store8 (i32.const 512) (i32.const 5))
    (call $double (i32.const 512) (i32.const 1)))"#
);

/// The 1-byte CBOR fixint reply decodes to that exact number.
fn fixint(b: u8) -> Value {
    Value::from(b)
}

fn run(language: &str, src: &str, handler: &str, args: &Value) -> anyhow::Result<Value> {
    let sessions = Sessions::new();
    sessions.with_session("t1", language, src, None::<&HostBridge>, &effector_runtime::sandbox::SandboxPolicy::None, |s: &mut dyn effector_runtime::carrier::session::ResidentSession| {
        s.call(handler, args)
    })
}

#[cfg(feature = "wasmtime")]
#[test]
fn wasm_entry_and_abi() {
    // The handler ignores arg bytes but the ABI (CBOR args written into
    // guest memory through the guest allocator, packed pointer reply) is
    // exercised end to end.
    let out = run("wasmtime", PURE_WAT, "bump", &serde_json::json!({"n": 1})).unwrap();
    assert_eq!(out, fixint(1));
}

#[cfg(feature = "wasmtime")]
#[test]
fn wasm_state_persists_across_calls() {
    let sessions = Sessions::new();
    let call = |s: &mut dyn effector_runtime::carrier::session::ResidentSession| {
        s.call("bump", &serde_json::json!(null))
    };
    sessions.with_session("i", "wasmtime", PURE_WAT, None::<&HostBridge>, &effector_runtime::sandbox::SandboxPolicy::None, call).unwrap();
    let out = sessions.with_session("i", "wasmtime", PURE_WAT, None::<&HostBridge>, &effector_runtime::sandbox::SandboxPolicy::None, call).unwrap();
    assert_eq!(out, fixint(2)); // the global survived the first call

    // A different instance key gets a FRESH module (fresh global).
    let out2 = sessions.with_session("j", "wasmtime", PURE_WAT, None::<&HostBridge>, &effector_runtime::sandbox::SandboxPolicy::None, call).unwrap();
    assert_eq!(out2, fixint(1));
}

#[cfg(feature = "wasmtime")]
#[test]
fn wasm_missing_handler_is_error_value() {
    let err = run("wasmtime", PURE_WAT, "nope", &serde_json::json!(null)).unwrap_err();
    assert!(err.to_string().contains("nope"));
}

#[cfg(feature = "wasmtime")]
#[test]
fn wasm_undeclared_host_import_is_instantiation_error() {
    // HOST_WAT imports aura_host.double; a session with no bridge refuses
    // it at instantiation (capability refusal — the contract, not a bug).
    let err = run("wasmtime", HOST_WAT, "hosted", &serde_json::json!(null)).unwrap_err();
    assert!(err.to_string().contains("aura_host"));
}

#[cfg(feature = "wasmtime")]
#[test]
fn wasm_host_import_round_trip() {
    // Host fn: CBOR fixint n -> CBOR fixint 2n, written back through the
    // guest allocator; the guest returns the packed reply pointer.
    let mut bridge = HostBridge::default();
    bridge.functions.insert(
        "double".into(),
        Arc::new(|v: Value| {
            let n = v.as_i64().unwrap_or(0);
            Ok(Value::from(n * 2))
        }) as HostFn,
    );
    let sessions = Sessions::new();
    let out = sessions
        .with_session("t", "wasmtime", HOST_WAT, Some(&bridge), &effector_runtime::sandbox::SandboxPolicy::None, |s: &mut dyn effector_runtime::carrier::session::ResidentSession| {
            s.call("hosted", &serde_json::json!(null))
        })
        .unwrap();
    assert_eq!(out, Value::from(10)); // the host doubled the guest's 5
}

#[cfg(feature = "wasmtime")]
#[test]
fn wasm_introspect_export_list_derivation() {
    // No interface_schema export: receives derives purely from the export
    // list — every function export except aura_alloc/memory is a handler.
    let schema = effector_runtime::carrier::introspect("wasmtime", PURE_WAT).unwrap();
    assert_eq!(
        schema,
        serde_json::json!({
            "receives": { "bump": {}, "peek": {} },
            "wildcard_receives": []
        })
    );
}

// The retired one-shot path refuses wasmtime explicitly, same as the other
// resident languages.
#[cfg(feature = "wasmtime")]
#[test]
fn wasm_one_shot_execute_is_gone() {
    let err = effector_runtime::carrier::execute(
        "wasmtime",
        effector_runtime::carrier::ExecRequest {
            source: "",
            entry: None,
            args: &serde_json::json!(null),
            host: None,
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("resident-only"));
}
