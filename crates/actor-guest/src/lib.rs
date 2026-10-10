//! actor-guest — the in-module storage kit for wasm (Rust source) actors
//! (ADR-0026 §4 full-power path; PLAN 4.9 wasm schema item).
//!
//! A compiled booth module links this crate plus okm-core (static mode:
//! `#[derive(KeyEncode/DocumentEncode)]` run at wasm build time, the
//! schema IS code in the artifact) and implements its storage plane as
//! one [`EmitStore`] — a `VirtualStorage` impl whose every primitive is
//! one `aura_host.store.frame` host round trip carrying an okm-wire `OpFrame`.
//! The module then runs the real `Collection` API in-module; the engine
//! executes the engine calls through the SAME executor path the dynamic
//! mode uses (same bytes, compiled schema).
//!
//! Wire contract (the effector carrier's existing ABI, unchanged):
//! - import `aura_host.store.frame(ptr: i32, len: i32) -> i64` — the guest
//!   hands over one `OpFrame` (okm-wire encoding, request ops ride the
//!   standard tags); the host answers with one packed `(ptr << 32 |
//!   len)` pointing at the `OpResponse` bytes it wrote through the
//!   guest's `aura_alloc`.
//! - the guest allocates its request bytes via `aura_alloc` and reads
//!   the reply from the returned pointer, same convention as handlers.
//!
//! NO dynamic instruction path exists for wasm: the op vocabulary here
//! is okm-wire's engine-call layer (put/get/delete/scan), which the
//! host translates onto the actor type's declared collections. Schema
//! declaration is the derive — nothing in this crate emits JSON at
//! runtime except the upload-time `interface_schema` export, which
//! serializes the compiled `CollectionSchema`s (serde form, the exact
//! object `StorePlan::from_schema` parses).

pub mod schema;
pub use schema::CollectionDecl;

use okm_core::engine::storage::VirtualStorage;
use okm_core::{OpFrame, OpResponse};
use okm_wire::{OP_DELETE, OP_GET, OP_PUT, OP_SCAN};

// The host import. Signature per the carrier ABI: `(ptr, len) -> i64`
// packed return. Linked under `aura_host.store.frame` — the wasm import
// module MUST be `aura_host` (the carrier resolves host imports there);
// the field name is the flat import ABI (ADR-0043 §6: the WASM ABI stays
// flat — the source-level `ctx.store.frame` maps onto this name).
#[link(wasm_import_module = "aura_host")]
extern "C" {
    #[link_name = "store.frame"]
    fn host_emit(ptr: i32, len: i32) -> i64;
}

// The guest allocator export the carrier requires (bump allocator) —
// an EXPORT, declared as an undefined extern so the linker leaves it
// as an unresolved import... no: exports come from definitions. The
// fixture module defines its own `aura_alloc`; this crate only needs
// the call, declared as an extern the final module MUST provide (it
// lands in the `env` import module by default — the fixture defines
// and exports it, and cdylib linking re-exports undefined symbols is
// NOT automatic). Simplest correct shape: the fixture defines the
// allocator; this crate declares the extern without a wasm import
// module and the fixture's `#[no_mangle] pub extern "C" fn aura_alloc`
// definition satisfies it at link time.
extern "C" {
    #[link_name = "aura_alloc"]
    fn guest_alloc(len: i32) -> i32;
}

/// One engine call = one `OpFrame` round trip over the host bridge.
/// This is the ONLY line of guest→host storage transport; every
/// `VirtualStorage` method funnels here.
fn round_trip(frame: &OpFrame) -> OpResponse {
    let bytes = frame.encode();
    // Allocate in guest memory and hand (ptr, len) to the host.
    let ptr = unsafe { guest_alloc(bytes.len() as i32) };
    // Copy into linear memory: wasm32 with std — write through the
    // raw memory via a volatile slice built from the pointer.
    unsafe {
        let dst = std::slice::from_raw_parts_mut(ptr as *mut u8, bytes.len());
        dst.copy_from_slice(&bytes);
    }
    let packed = unsafe { host_emit(ptr, bytes.len() as i32) };
    let ret_ptr = (packed >> 32) as u32 as usize;
    let ret_len = (packed & 0xFFFF_FFFF) as u32 as usize;
    let reply = unsafe { std::slice::from_raw_parts(ret_ptr as *const u8, ret_len) };
    OpResponse::decode(reply).unwrap_or_default()
}

/// The storage plane a wasm actor ships: one `VirtualStorage` over the
/// emit bridge. Construct one per collection assembly point (the same
/// role `MqStore`/`TestStore` play for engine-bearing hosts — here the
/// "engine" lives behind the host boundary).
pub struct EmitStore;

impl VirtualStorage for EmitStore {
    fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        round_trip(&OpFrame::one(OP_PUT, key, value));
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        round_trip(&OpFrame::one(OP_GET, key.to_vec(), Vec::new())).value
    }

    fn del(&self, key: &[u8]) {
        round_trip(&OpFrame::one(OP_DELETE, key.to_vec(), Vec::new()));
    }

    /// Same OP_SCAN frame semantics the RemoteStore arm documents: the
    /// value segment carries `[0x01][end]` for a finite end, `[0x00]`
    /// for unbounded — prefix scanning is the special case.
    fn scan_range(&self, begin: &[u8], end: Option<&[u8]>) -> Vec<Vec<u8>> {
        let mut value = vec![0u8];
        if let Some(end) = end {
            value[0] = 1;
            value.extend_from_slice(end);
        }
        round_trip(&OpFrame::one(OP_SCAN, begin.to_vec(), value)).suffixes
    }
}
