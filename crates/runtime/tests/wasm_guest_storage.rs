//! Wasm guest storage path (ADR-0026 §4 full-power path): a REAL
//! rustc-compiled module (`actor-guest`'s counter_actor example) runs
//! the in-module Collection API over the `aura_host.store.frame` bridge. The
//! host arm decodes the okm-wire `OpFrame`, executes it against a real
//! in-process engine (the same shape the realm's executor uses), and
//! answers with `OpResponse` — the dynamic and static storage modes
//! meet at the same byte contract.
//!
//! The fixture artifact is built by `tests/wasm_guest_build.rs` (a
//! build-time wrapper invoking cargo with the wasm32 target) and read
//! from `target/wasm32-unknown-unknown/debug/examples/`. Skipped (with
//! a loud pass-through) when the artifact is absent: building it needs
//! the wasm32-unknown-unknown rust target installed.

use effector_runtime::carrier::{HostBridge, HostFn};
use effector_runtime::carrier::session::Sessions;
use serde_json::Value;
use std::sync::Arc;

#[cfg(feature = "wasmtime")]
mod storage {
    use super::*;
    use base64::Engine as _;
    use okm_core::engine::storage::VirtualStorage;
    use okm_core::engine::test_engine::TestStore;
    use okm_wire::{OpFrame, OpResponse, OP_DELETE, OP_GET, OP_PUT, OP_SCAN};
    use std::cell::RefCell;

    // The host-side engine: a REAL okm engine (TestStore matrix's
    // slatedb-mem), addressed by raw engine calls — exactly the bytes
    // the guest's Collection emitted. The realm's production executor
    // adds the declared-schema collection layer; this fixture answers
    // at the engine layer (the byte contract under test is the same).
    thread_local! {
        static ENGINE: RefCell<TestStore> = RefCell::new(TestStore::default());
    }

    fn exec_frame(frame: &[u8]) -> OpResponse {
        let parsed = OpFrame::decode(frame).expect("guest op frame");
        let mut out = OpResponse::default();
        ENGINE.with(|e| {
            let store = e.borrow_mut();
            for (tag, key, value) in &parsed.0 {
                match *tag {
                    OP_PUT => store.put(key.clone(), value.clone()),
                    OP_DELETE => store.del(key),
                    OP_GET => out.value = store.get(key),
                    OP_SCAN => out.suffixes = store.scan_range(key, None),
                    other => panic!("fixture does not exercise op tag {other}"),
                }
            }
        });
        out
    }

    fn artifact() -> Option<String> {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../target/wasm32-unknown-unknown/debug/examples/counter_actor.wasm"
        );
        std::fs::read(path)
            .ok()
            .map(|bytes| base64::engine::general_purpose::STANDARD.encode(&bytes))
    }

    fn bridge() -> HostBridge {
        let mut bridge = HostBridge::default();
        bridge.functions.insert(
            "store.frame".into(),
            Arc::new(move |arg: Value| {
                // The carrier marshals the guest's (ptr,len) bytes into a
                // JSON value?? No — the arg here IS the CBOR-decoded value
                // of the guest's request bytes. The guest sends an OpFrame
                // (raw okm-wire bytes), not a JSON value. The generic
                // HostFn seam is JSON; the storage bridge needs BYTES.
                // Handled by WasmSession's raw-byte host arm: a host fn
                // named `store.frame` gets the request bytes as a JSON array of
                // numbers (bytes survive the JSON detour losslessly).
                let bytes: Vec<u8> = arg
                    .as_array()
                    .expect("emit arg is a byte array")
                    .iter()
                    .map(|v| v.as_u64().expect("byte") as u8)
                    .collect();
                let resp = exec_frame(&bytes);
                let out = resp.encode();
                Ok(Value::Array(out.into_iter().map(Value::from).collect()))
            }) as HostFn,
        );
        bridge
    }

    #[test]
    fn wasm_guest_introspect_schema_block() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../target/wasm32-unknown-unknown/debug/examples/counter_actor.wasm"
        );
        let bytes = std::fs::read(path).expect("artifact");
        let source = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let schema = effector_runtime::carrier::introspect("wasmtime", &source).unwrap();
        println!("SCHEMA = {schema:#}");
        assert!(
            schema.get("storage").is_some(),
            "storage block missing from introspected schema"
        );
    }

    #[test]
    fn wasm_guest_collection_over_emit_bridge() {
        let Some(source) = artifact() else {
            panic!("counter_actor.wasm missing — build it: cargo build -p actor-guest --example counter_actor --target wasm32-unknown-unknown");
        };
        let sessions = Sessions::new();
        let call = |handler: &'static str, s: &mut dyn effector_runtime::carrier::session::ResidentSession| {
            s.call(handler, &serde_json::json!(null))
        };
        // bump twice, read back: two RMW round trips through the bridge.
        let out = sessions.with_session("k", "wasmtime", &source, Some(&bridge()), &effector_runtime::sandbox::SandboxPolicy::None, |s| call("bump", s)).unwrap();
        assert_eq!(out, Value::from(1));
        let out = sessions.with_session("k", "wasmtime", &source, Some(&bridge()), &effector_runtime::sandbox::SandboxPolicy::None, |s| call("bump", s)).unwrap();
        assert_eq!(out, Value::from(2));
        let out = sessions.with_session("k", "wasmtime", &source, Some(&bridge()), &effector_runtime::sandbox::SandboxPolicy::None, |s| call("peek", s)).unwrap();
        assert_eq!(out, Value::from(2));
    }
}
