//! ADR-0034 consumer-side fixture: a real rustc-compiled wasm booth that
//! PULLS a sibling booth's stream to done through the aura_host iterate
//! imports (ctx.iterate.start / ctx.iterate.next / ctx.iterate.dispose —
//! the flat wasm ABI of the ADR-0043 ctx methods). The wasm
//! side of "python 生产摊位 → wasm/python 消费" acceptance; the envelope
//! crosses as CBOR (carrier marshal), the guest never speaks JSON.
//!
//! Exports:
//! - `pull_all`: start + pull until done, return `{got: [...]}` — full
//!   drain proves termination (StopIteration on the producer encodes
//!   done:true, the loop exits structurally).
//! - `pull_break`: start, take the first item, dispose explicitly —
//!   proves the dispose verb crosses the import seam (a consumer with no
//!   destructor hook calling the mandatory dual).

use serde_json::Value;

#[link(wasm_import_module = "aura_host")]
extern "C" {
    #[link_name = "iterate.start"]
    fn ctx_iter_start(ptr: i32, len: i32) -> i64;
    #[link_name = "iterate.next"]
    fn ctx_iter_next(ptr: i32, len: i32) -> i64;
    #[link_name = "iterate.dispose"]
    fn ctx_iter_dispose(ptr: i32, len: i32) -> i64;
}

// The allocator export the carrier requires (same shape as counter_actor).
static mut HEAP: usize = 4096;

#[no_mangle]
pub extern "C" fn aura_alloc(len: i32) -> i32 {
    unsafe {
        let p = HEAP;
        HEAP += len as usize;
        p as i32
    }
}

fn pack(ptr: u32, len: u32) -> i64 {
    ((ptr as i64) << 32) | (len as i64)
}

fn unpack(packed: i64) -> (usize, usize) {
    (((packed >> 32) as u32) as usize, (packed & 0xFFFF_FFFF) as u32 as usize)
}

/// Send a CBOR-encoded request through one import, decode the CBOR reply.
fn host_call(f: unsafe extern "C" fn(i32, i32) -> i64, req: &Value) -> Value {
    let mut bytes = Vec::new();
    ciborium::into_writer(req, &mut bytes).expect("cbor encode");
    unsafe {
        let ptr = aura_alloc(bytes.len() as i32);
        std::slice::from_raw_parts_mut(ptr as *mut u8, bytes.len()).copy_from_slice(&bytes);
        let packed = f(ptr, bytes.len() as i32);
        let (rp, rl) = unpack(packed);
        let reply = std::slice::from_raw_parts(rp as *const u8, rl);
        ciborium::from_reader(reply).unwrap_or(Value::Null)
    }
}

fn start(target_type: &str, key: &str, handler: &str, args: Value) -> Value {
    host_call(
        ctx_iter_start,
        &serde_json::json!({
            "type": target_type, "key": key, "handler": handler, "args": args,
        }),
    )
}

fn next(stream_id: &str) -> Value {
    host_call(ctx_iter_next, &serde_json::json!({ "stream_id": stream_id }))
}

fn dispose(stream_id: &str) -> Value {
    host_call(ctx_iter_dispose, &serde_json::json!({ "stream_id": stream_id }))
}

/// Read the incoming `{type, key, handler, args}` selection object from
/// the handler's CBOR args (the consumer's caller names the producer).
fn parse_args(raw: &[u8]) -> Value {
    ciborium::from_reader(raw).unwrap_or(Value::Null)
}

fn pull_field(v: &Value, k: &str) -> Value {
    v.get(k).cloned().unwrap_or(Value::Null)
}

fn reply_value(v: &Value) -> i64 {
    let mut bytes = Vec::new();
    ciborium::into_writer(v, &mut bytes).expect("cbor reply");
    unsafe {
        let ptr = aura_alloc(bytes.len() as i32);
        std::slice::from_raw_parts_mut(ptr as *mut u8, bytes.len()).copy_from_slice(&bytes);
        pack(ptr as u32, bytes.len() as u32)
    }
}

/// Full drain: for-loop to done. `{got: [...]}` mirrors the python
/// consumer's shape; termination came from the envelope, not a sentinel.
#[no_mangle]
pub extern "C" fn pull_all(ptr: i32, len: i32) -> i64 {
    let raw = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) }.to_vec();
    let req = parse_args(&raw);
    let first = start(
        req["type"].as_str().unwrap_or_default(),
        req["key"].as_str().unwrap_or_default(),
        req["handler"].as_str().unwrap_or_default(),
        pull_field(&req, "args"),
    );
    let stream_id = first["stream_id"].as_str().unwrap_or_default().to_string();
    let mut got = Vec::new();
    let mut env = first;
    while env.get("done").and_then(|d| d.as_bool()) != Some(true) {
        got.push(pull_field(&env, "item"));
        env = next(&stream_id);
    }
    reply_value(&serde_json::json!({ "got": got }))
}

/// Break mid-stream with the explicit dispose (ADR-0034 §3: the mandatory
/// dual for carriers without a destructor hook). Takes one item, disposes,
/// returns what was in hand — the realm's registry must drain after.
#[no_mangle]
pub extern "C" fn pull_break(ptr: i32, len: i32) -> i64 {
    let raw = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) }.to_vec();
    let req = parse_args(&raw);
    let first = start(
        req["type"].as_str().unwrap_or_default(),
        req["key"].as_str().unwrap_or_default(),
        req["handler"].as_str().unwrap_or_default(),
        pull_field(&req, "args"),
    );
    let stream_id = first["stream_id"].as_str().unwrap_or_default().to_string();
    let mut got = Vec::new();
    if first.get("done").and_then(|d| d.as_bool()) != Some(true) {
        got.push(pull_field(&first, "item"));
        // One more pull, then abandon mid-stream — dispose is the point.
        let second = next(&stream_id);
        if second.get("done").and_then(|d| d.as_bool()) != Some(true) {
            got.push(pull_field(&second, "item"));
        }
    }
    dispose(&stream_id);
    reply_value(&serde_json::json!({ "got": got }))
}

/// Upload-time schema (4.5b): the receives half names both exports.
#[no_mangle]
pub extern "C" fn interface_schema(_ptr: i32, _len: i32) -> i64 {
    reply_value(&serde_json::json!({
        "receives": { "pull_all": {}, "pull_break": {} },
        "wildcard_receives": [],
    }))
}
