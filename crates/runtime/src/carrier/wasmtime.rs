//! Wasm carrier (Wasmtime). Third-party untrusted code: hardware isolation.
//!
//! The module exports one function per handler, named after the event it
//! serves (the multi-entry export convention — event name = export name),
//! plus `memory` and the guest allocator `aura_alloc(len: i32) -> i32`.
//! Values cross the linear memory as CBOR bytes: the host serializes the
//! args, writes them into guest-allocated memory, calls the handler with
//! `(ptr: i32, len: i32) -> i64`, and unpacks the return
//! `(ptr: u32) << 32 | len: u32`. No JSON debt — CBOR is the wire; the
//! host-side document is JSON only at the ResidentSession boundary, the
//! same seam every carrier sits behind.
//!
//! `interface_schema` follows the same convention: if the module exports a
//! function by that name it is called like a handler (its result is the
//! JSON schema, CBOR-encoded on the wire); otherwise the receives half is
//! derived from the export list (every export that is not infrastructure
//! is an event handler).
//!
//! Host imports (the ctx bridge) are registered under the `aura_host`
//! module namespace, one import per host function, uniform signature
//! `(ptr: i32, len: i32) -> i64` with the same packed return. The guest
//! marshals its argument to CBOR into linear memory and calls the import;
//! the host reads the argument, runs the HostFn, writes the reply back
//! through the guest's `aura_alloc`, and returns the packed pointer.
//!
//! Source encoding: wasm modules are binary, but the session seam (and the
//! persisted BoothDef) carries a String — so a module arrives either as
//! WAT text (tests, hand-written fixtures; starts with `(module`) or as
//! base64 of the raw `.wasm` bytes. Everything downstream is bytes.

use super::session::ResidentSession;
use super::{HostBridge, HostFn};
use anyhow::{anyhow, Result};
use serde_json::Value;
use std::collections::HashMap;

use wasmtime::{Caller, Engine, Instance, Linker, Memory, Module, Store, TypedFunc};

/// Guest-allocator export name and host-import namespace — the ABI
/// contract between this carrier and compiled booth modules.
pub const ALLOC: &str = "aura_alloc";
pub const MEMORY: &str = "memory";
pub const HOST_NS: &str = "aura_host";
pub const SCHEMA: &str = "interface_schema";

/// Exports that are infrastructure, never event handlers (schema
/// derivation skips them).
const RESERVED: [&str; 3] = [ALLOC, SCHEMA, MEMORY];

/// One resident wasm session: module compiled once, instance and store
/// live as long as the session (module globals / linear memory persist
/// across calls). Drop = everything gone.
pub struct WasmSession {
    instance: Instance,
    store: Store<State>,
}

struct State {
    /// Host functions behind the `aura_host.<name>` imports. Cloned into
    /// the import closures at link time; kept here only for clarity.
    _host: HashMap<String, HostFn>,
}



impl WasmSession {
    /// Construct from the source string: WAT text or base64 `.wasm` bytes.
    pub fn from_source(source: &str, host: Option<&HostBridge>) -> Result<Self> {
        let engine = Engine::default();
        let module = compile(&engine, source)?;

        let mut linker: Linker<State> = Linker::new(&engine);
        if let Some(bridge) = host {
            for (name, f) in &bridge.functions {
                let f = f.clone();
                let full = format!("{HOST_NS}.{name}");
                // Synchronous host import (func_wrap): the HostFn is a
                // blocking closure by contract — every caller site runs
                // under spawn_blocking (the ctx bridge blocks on the async
                // ctx inside it). No async host import machinery needed.
                // `store.frame` (ADR-0043 rename of the bare `emit`) is
                // the RAW-BYTE host arm (ADR-0026 §4 wasm
                // storage): the guest hands over okm-wire OpFrame bytes —
                // NOT a CBOR value — and expects raw OpResponse bytes
                // back. The JSON HostFn seam carries the bytes as JSON
                // number arrays (lossless; the storage bridge is not a
                // hot path at the JSON-detour cost). Every other host fn
                // keeps the CBOR value marshal. NOTE (ADR-0043 §6): the
                // WASM import ABI stays FLAT by design — import field
                // names are the dotted bridge keys verbatim
                // (`aura_host.invoke`, `aura_host.store.frame`,
                // `aura_host.iterate.start`, …); the ctx object is the
                // SOURCE-level shape and the guest SDK maps source
                // `ctx.store` onto the flat import.
                let is_emit = name == "store.frame";
                linker
                    .func_wrap(
                        HOST_NS,
                        name,
                        move |mut caller: Caller<'_, State>, ptr: i32, len: i32| -> Result<i64> {
                            let arg_bytes = read_host_memory(&mut caller, ptr, len)?;
                            let arg: Value = if is_emit {
                                Value::Array(arg_bytes.iter().map(|b| Value::from(*b)).collect())
                            } else {
                                ciborium::from_reader(&arg_bytes[..])?
                            };
                            let out = f(arg)?;

                            // Write the reply back through the guest's
                            // allocator (the guest reads it after resume).
                            let (mem, alloc) = guest_facilities(&mut caller)?;
                            let reply: Vec<u8> = if is_emit {
                                out.as_array()
                                    .ok_or_else(|| anyhow!("emit host fn must return a byte array"))?
                                    .iter()
                                    .map(|v| v.as_u64().map(|x| x as u8).ok_or_else(|| anyhow!("emit reply byte")))
                                    .collect::<Result<Vec<u8>>>()?
                            } else {
                                let mut r = Vec::new();
                                ciborium::into_writer(&out, &mut r)?;
                                r
                            };
                            let rptr = write_guest(&mem, &alloc, &mut caller, &reply)?;
                            Ok(pack(rptr as u32, reply.len() as u32))
                        },
                    )
                    .map_err(|e| anyhow!("host import {full}: {e}"))?;
            }
        }

        let mut store = Store::new(&engine, State { _host: HashMap::new() });
        let instance = linker
            .instantiate(&mut store, &module)
            .map_err(|e| anyhow!("wasm instantiate: {e}"))?;
        Ok(Self { instance, store })
    }

    /// Guest handler memory helpers for this session.
    fn memory(&mut self) -> Result<Memory> {
        self.instance
            .get_memory(&mut self.store, MEMORY)
            .ok_or_else(|| anyhow!("wasm module exports no {MEMORY}"))
    }

    fn alloc(&mut self) -> Result<TypedFunc<(i32,), (i32,)>> {
        self.instance
            .get_typed_func::<(i32,), (i32,)>(&mut self.store, ALLOC)
            .map_err(|e| anyhow!("wasm module exports no {ALLOC}: {e}"))
    }

    /// Event handlers = every export except the infrastructure trio.
    /// Wildcard handlers end in `.*` (the event pattern IS the export
    /// name); `interface_schema` is handled separately by `introspect`.
    pub fn handlers(&mut self) -> Vec<String> {
        self.instance
            .exports(&mut self.store)
            .filter_map(|e| {
                let name = e.name().to_string();
                let is_func = matches!(e.into_extern(), wasmtime::Extern::Func(_));
                (is_func && !RESERVED.contains(&name.as_str())).then_some(name)
            })
            .collect()
    }

    /// Introspect: explicit `interface_schema` export wins (called like a
    /// handler, JSON result CBOR-encoded on the wire); otherwise derive
    /// the receives half from the export list. Field-wise merge order
    /// matches steel/python: derived (export list) provides the base,
    /// explicit fills the rest.
    pub fn introspect(&mut self) -> Result<Value> {
        let handlers = self.handlers();
        let mut receives = serde_json::Map::new();
        let mut wildcards: Vec<Value> = Vec::new();
        for h in &handlers {
            if h.ends_with(".*") {
                wildcards.push(Value::String(h.clone()));
                continue;
            }
            receives.insert(h.clone(), Value::Object(serde_json::Map::new()));
        }
        let derived = Value::Object([
            ("receives".to_string(), Value::Object(receives)),
            ("wildcard_receives".to_string(), Value::Array(wildcards)),
        ]
        .into_iter()
        .collect());

        if self.instance.get_func(&mut self.store, SCHEMA).is_some() {
            let explicit = self.call(SCHEMA, &Value::Null)?;
            return Ok(merge_schema(derived, explicit));
        }
        Ok(derived)
    }
}

impl ResidentSession for WasmSession {
    fn load(&mut self, _source: &str) -> Result<()> {
        // Compilation happened at construction (from_source) — the
        // resident-session contract's load phase has nothing left to do.
        Ok(())
    }

    fn call(&mut self, handler: &str, args: &Value) -> Result<Value> {
        let func = self
            .instance
            .get_typed_func::<(i32, i32), (i64,)>(&mut self.store, handler)
            .map_err(|_| anyhow!("wasm handler '{handler}' not exported or not (i32, i32) -> i64"))?;

        let mut cbor = Vec::new();
        ciborium::into_writer(args, &mut cbor)?;

        let mem = self.memory()?;
        let alloc = self.alloc()?;
        let ptr = write_guest(&mem, &alloc, &mut self.store, &cbor)?;
        let packed = func.call(&mut self.store, (ptr, cbor.len() as i32))?.0;

        let (ret_ptr, ret_len) = unpack(packed);
        let mut out = vec![0u8; ret_len as usize];
        mem.read(&self.store, ret_ptr as usize, &mut out)?;
        Ok(ciborium::from_reader(&out[..])?)
    }

    /// ADR-0034 envelope mode over the existing ABI: the export is an
    /// ordinary `(ptr,len)->i64` handler called through `call` with args
    /// carrying `{iterate: {stream_id, op}}`; a Rust guest parks its own
    /// `Iterator` in module state and projects exhaustion into the
    /// explicit `done: true` envelope at the ABI edge. No new import or
    /// memory shape — the envelope is CBOR-encodable schema.
    fn iterate(&mut self, op: super::session::StreamOp) -> Result<Value> {
        super::session::envelope_pull(self, &op)
    }

    fn as_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

// ----------------------------------------------------------------- schema --

/// Field-wise merge, identical to the steel/python carriers: the derived
/// half wins on its keys; the explicit half contributes the rest.
pub fn merge_schema(derived: Value, explicit: Value) -> Value {
    match (derived, explicit) {
        (Value::Object(mut d), Value::Object(e)) => {
            for (k, v) in e {
                d.entry(k).or_insert(v);
            }
            Value::Object(d)
        }
        (d, _) => d,
    }
}

// --------------------------------------------------------------------- ABI --

fn pack(ptr: u32, len: u32) -> i64 {
    ((ptr as i64) << 32) | len as i64
}

fn unpack(packed: i64) -> (u32, u32) {
    ((packed >> 32) as u32, (packed & 0xFFFF_FFFF) as u32)
}

/// Compile the source string: WAT text or base64-encoded wasm binary.
fn compile(engine: &Engine, source: &str) -> Result<Module> {
    if source.trim_start().starts_with("(module") {
        return Module::new(engine, source).map_err(|e| anyhow!("wasm compile (wat): {e}"));
    }
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(source.trim())
        .map_err(|e| anyhow!("wasm source is neither WAT nor base64: {e}"))?;
    Module::new(engine, &bytes).map_err(|e| anyhow!("wasm compile: {e}"))
}

/// A guest-exported `i32 -> i32` function handle.
type GuestFunc = TypedFunc<(i32,), (i32,)>;

/// Resolve (memory, aura_alloc) from inside a host import's caller.
fn guest_facilities(caller: &mut Caller<'_, State>) -> Result<(Memory, GuestFunc)> {
    let mem = caller
        .get_export(MEMORY)
        .and_then(|e| e.into_memory())
        .ok_or_else(|| anyhow!("wasm module exports no {MEMORY}"))?;
    let alloc = caller
        .get_export(ALLOC)
        .and_then(|e| e.into_func())
        .ok_or_else(|| anyhow!("wasm module exports no {ALLOC}"))?
        .typed::<(i32,), (i32,)>(&*caller)?;
    Ok((mem, alloc))
}

/// Read `len` bytes at `ptr` from the calling guest's linear memory.
fn read_host_memory(caller: &mut Caller<'_, State>, ptr: i32, len: i32) -> Result<Vec<u8>> {
    let mem = caller
        .get_export(MEMORY)
        .and_then(|e| e.into_memory())
        .ok_or_else(|| anyhow!("wasm module exports no {MEMORY}"))?;
    let mut buf = vec![0u8; len as usize];
    mem.read(&caller, ptr as usize, &mut buf)?;
    Ok(buf)
}

/// Allocate `bytes.len()` in the guest and copy them in. Returns ptr.
fn write_guest(
    mem: &Memory,
    alloc: &TypedFunc<(i32,), (i32,)>,
    mut store: impl wasmtime::AsContextMut,
    bytes: &[u8],
) -> Result<i32> {
    let (ptr,) = alloc.call(&mut store, (bytes.len() as i32,))?;
    mem.write(&mut store, ptr as usize, bytes)?;
    Ok(ptr)
}
