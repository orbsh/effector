//! Python carrier (PyO3, in-process CPython, zero IPC).

use super::session::StreamOp;
use super::{ExecResult, HostBridge, HostFn, StorageEngineFns};
use std::collections::HashMap;
use std::ffi::CString;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyCFunction, PyDict, PyFloat, PyInt, PyList, PyModule, PyString};
use serde_json::Value;

/// Adapter: the bridge's four byte-level closures (host-owned engine,
/// ADR-0037 4.16a) behind okm-python's `Engine` trait. The trait's
/// signature is byte-for-byte the closure shapes — this arm exists to
/// satisfy the trait, no translation happens. The engine handle never
/// crosses a seam back: `Collection::with_store` runs HERE, under the
/// load step, and the pyclass instance goes straight into the module
/// (a pyclass carries `*mut PyObject` registration data — not Send —
/// so an intermediate `Vec<(String, Collection)>` return would fight
/// the Send bound for nothing).
struct ClosureEngine(std::sync::Arc<StorageEngineFns>);

impl okm::pyo3_impl::Engine for ClosureEngine {
    fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        (self.0.put)(key, value)
    }
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        (self.0.get)(key)
    }
    fn del(&self, key: &[u8]) {
        (self.0.del)(key)
    }
    fn scan_range(&self, begin: &[u8], end: Option<&[u8]>) -> Vec<Vec<u8>> {
        (self.0.scan_range)(begin, end)
    }
}

/// Compile `source` and execute it at import time with the `@on` collector
/// already bound — decorators append `(event, key)` pairs to the returned
/// registry while the module body runs. The collector is language-shape
/// (a declaration registry); deriving schema semantics from it is the
/// caller's (aura's) job.
fn load_module<'py>(py: Python<'py>, source: &str) -> PyResult<(Bound<'py, PyModule>, Bound<'py, PyList>)> {
    let module = PyModule::new(py, "operation")?;
    let registry = PyList::empty(py);
    let reg: Py<PyList> = registry.clone().unbind();
    let module_handle: Py<PyModule> = module.clone().unbind();
    let on = PyCFunction::new_closure(py, None, None, move |args, kw| {
        let py = unsafe { Python::assume_attached() };
        let reg = reg.bind(py);
        // Two call shapes: `on("event", key=...)` (declaration — returns an
        // identity decorator) and `decorator(fn)` (application — returns
        // the function unchanged).
        let first = args.get_item(0)?;
        if first.extract::<String>().is_ok() {
            let mut key = String::new();
            if let Some(kw) = kw {
                if let Some(k) = kw.get_item("key")? {
                    key = k.extract()?;
                }
            }
            let event = first.extract::<String>()?;
            reg.append((event.clone(), key))?;
            // The returned decorator binds the function under the EVENT
            // NAME — event delivery addresses handlers by event name, so
            // the module must expose them there. The module handle is
            // captured (no sys.modules lookup: our module is synthetic).
            let module_handle = module_handle.clone_ref(py);
            let binder = PyCFunction::new_closure(py, None, None, move |a: &Bound<'_, pyo3::types::PyTuple>, _kw: Option<&Bound<'_, pyo3::types::PyDict>>| {
                let py = unsafe { Python::assume_attached() };
                let f = a.get_item(0)?;
                module_handle.bind(py).setattr(event.clone(), &f)?;
                Ok::<_, pyo3::PyErr>(f.unbind())
            })?;
            Ok::<_, pyo3::PyErr>(binder.into_any().unbind())
        } else {
            Ok::<_, pyo3::PyErr>(first.unbind())
        }
    })?;
    module.add("on", on)?;

    // ---- Schema-declaration decorators (ADR-0026 §4) ----
    // The DSL lives in okm (okm-python's OKM_SCHEMA_PY — one source, every
    // host injects the same module): exec it into the booth's module
    // namespace so `@KeyEncode` / `@DocumentEncode` / `@ok_*` resolve.
    let globals = module.dict();
    globals.set_item("__name__", "operation")?;
    py.run(
        CString::new(okm::OKM_SCHEMA_PY)?.as_c_str(),
        Some(&globals),
        Some(&globals),
    )
    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("okm schema DSL: {e}")))?;

    // ADR-0034 consumer wrapper: `ctx_iterate` turns the three ctx.iterate
    // host methods (ctx.iterate.start/next/dispose, bound right after the
    // module is
    // loaded when a bridge exists) into ONE native generator. The
    // consumer writes `for tok in ctx_iterate(...)`: the first envelope
    // arrives with the Start round trip (stream_id merged by the realm),
    // `done: true` ends the loop (StopIteration — structural, not a
    // sentinel), and abandoning mid-loop raises GeneratorExit into this
    // frame, whose `finally` sends dispose. Unified envelope (ADR-0036):
    // the Start reply may be TERMINAL (invoke shape — the stream never
    // opened, no stream_id present), and the terminal round may carry a
    // `value`; it lands on `ctx_iterate.last_value` for the consumer
    // that wants it (native `for` iteration discards it — the ADR §3
    // rule; `ctx.invoke` is the shape that unwraps values).
    // Args marshal as JSON strings
    // — the PyCFunction seam extracts String (same contract the other
    // ctx host fns use).
    let globals = module.dict();
    py.run(
        CString::new(
            r#"
import json as _aura_json

def ctx_iterate(booth_type, booth_key, handler, args=None):
    env = ctx.iterate.start(_aura_json.dumps({
        "type": booth_type, "key": booth_key,
        "handler": handler, "args": args if args is not None else {},
    }))
    ctx_iterate.last_value = env.get("value")
    if env.get("done"):
        return
    stream_id = env["stream_id"]
    try:
        while True:
            yield env["item"]
            env = ctx.iterate.next(_aura_json.dumps({"stream_id": stream_id}))
            ctx_iterate.last_value = env.get("value")
            if env.get("done"):
                break
    finally:
        # Mid-stream abandonment (break / GeneratorExit / error before
        # done): dispose is the mandatory dual. Idempotent realm-side —
        # a stream already ended or failed answers success.
        if not env.get("done"):
            ctx.iterate.dispose(_aura_json.dumps({"stream_id": stream_id}))
"#,
        )?
        .as_c_str(),
        Some(&globals),
        Some(&globals),
    )
    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("iterate prelude: {e}")))?;

    // Execute the module body with the module dict as globals so the
    // decorators resolve `on` (bound above, before the body runs).
    let globals = module.dict();
    globals.set_item("__name__", "operation")?;
    py.run(CString::new(source)?.as_c_str(), Some(&globals), None)?;
    // Assemble the merged `interface_schema` AFTER the body ran. Captures:
    // the registry (decorator declarations) + the script's explicit partial
    // schema, if it declared one. Field-wise merge: derived
    // receives/wildcard_receives + whatever the explicit half adds
    // (lifecycle, ...). One `interface_schema` name on the module either
    // way — aura's call path is uniform across languages.
    //
    // The storage block is assembled HERE (load time, right after the
    // body — the `@DocumentEncode` classes are intact) and captured as
    // DATA, NOT re-evaluated at schema-call time: the python injection
    // face (ADR-0037 4.16a) registers the host-built `Collection`
    // pyclasses into this SAME module namespace, replacing the class
    // names (that IS the author's binding face — `Counters.put`). A late
    // `assemble_module(globals())` would then see pyclass instances, no
    // classes, and assemble an empty block — introspection would silently
    // lose the storage half. Load-time capture kills that ordering trap.
    let module_dict_at_load = module.dict();
    let storage_at_load = {
        let storage_py = py.eval(
            CString::new("assemble_module(globals())")?.as_c_str(),
            Some(&module_dict_at_load),
            Some(&module_dict_at_load),
        )?;
        json_from_py(py, &storage_py)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
    };
    let reg_handle: Py<PyList> = registry.clone().unbind();
    let explicit_fn: Option<Py<PyAny>> = module
        .getattr("interface_schema")
        .ok()
        .map(|f| f.unbind());
    let merge = PyCFunction::new_closure(py, None, None, move |_args, _kw| {
        let py = unsafe { Python::assume_attached() };
        let reg = reg_handle.bind(py);
        // Derived half from the @on registry: [(event, key), ...].
        let mut receives = serde_json::Map::new();
        let mut wildcards: Vec<Value> = Vec::new();
        for item in reg.iter() {
            let pair: (String, String) = match item.extract() {
                Ok(p) => p,
                Err(e) => return Err(pyo3::exceptions::PyRuntimeError::new_err(format!("@on registry entry: {e}"))),
            };
            if pair.0.ends_with(".*") {
                wildcards.push(Value::String(pair.0));
                continue;
            }
            let mut entry = serde_json::Map::new();
            if !pair.1.is_empty() {
                entry.insert("key".into(), Value::String(pair.1));
            }
            receives.insert(pair.0, Value::Object(entry));
        }
        let mut merged = Value::Object([
            ("receives".to_string(), Value::Object(receives)),
            ("wildcard_receives".to_string(), Value::Array(wildcards)),
        ].into_iter().collect());
        // Decorator-derived storage half: the load-time capture (see
        // above — assembled before injection could shadow the classes).
        // Empty block omitted so it cannot shadow an explicit-only
        // storage declaration (merge_schema's or_insert keeps the first
        // key seen).
        let storage = storage_at_load.clone();
        if let Value::Object(colls) = &storage {
            if let Some(colls) = colls.get("collections").and_then(|c| c.as_object()) {
                if !colls.is_empty() {
                    if let Value::Object(merged_map) = &mut merged {
                        merged_map.insert("storage".into(), storage);
                    }
                }
            }
        }
        // Explicit half: call the script's captured declaration, if any.
        // Decorator storage WINS over an explicit storage block (the
        // class definitions are the authoritative DDL — the explicit half
        // contributes lifecycle and the like).
        if let Some(f) = &explicit_fn {
            let r = f.bind(py).call1((py.None(),))
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("interface_schema: {e}")))?;
            let explicit_v = json_from_py(py, &r)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            merged = merge_schema(merged, explicit_v);
        }
        json_to_py(py, &merged).map(|v| v.unbind())
    })?;
    module.add("interface_schema", merge)?;
    Ok((module, registry))
}

/// Resident python session: module loaded once, handlers called by name
/// across calls. The module's global dict IS the session state — variables
/// set by one handler are visible to the next. Drop = state gone. Host
/// functions are bound at load and stay bound.
pub struct PythonSession {
    module: Option<Py<PyModule>>,
    host: Option<HostBridge>,
    /// Live iterate streams (ADR-0034 generator mode): the framework
    /// drives the native generator; the handler never sees the wire
    /// protocol. stream_id -> the generator object (GIL held whenever
    /// this is touched — the session already lives under one slot lock).
    /// `unsafe impl Send` below covers the whole session; streams die
    /// with it (eviction = failed stream, per the ADR).
    streams: HashMap<String, Py<PyAny>>,
}

// Py<PyModule> is not Send; sessions live on one runtime thread each, so
// the Send impl is exact (same reasoning as steel's thread-local design).
unsafe impl Send for PythonSession {}

impl PythonSession {
    pub fn new(host: Option<&HostBridge>) -> anyhow::Result<Self> {
        Ok(Self { module: None, host: host.cloned(), streams: HashMap::new() })
    }
}

impl super::session::ResidentSession for PythonSession {
    fn load(&mut self, source: &str) -> anyhow::Result<()> {
        Python::attach(|py| -> anyhow::Result<()> {
            let (module, _registry) = load_module(py, source)
                .map_err(|e| anyhow::anyhow!("python load: {e}"))?;
            // Host functions bind once at load and stay for the session's
            // whole life — same marshal contract as the old one-shot path.
            // ADR-0043: the host bridge is ONE module-level object `ctx`,
            // its attribute tree mirroring the dotted bridge keys
            // ("store" -> ctx.store, "iterate.start" -> ctx.iterate.start,
            // …). Each leaf is a PyCFunction with the same JSON-string
            // marshal the flat injection used; the bare `ctx_*` globals
            // retire (breaking rename, no aliases).
            if let Some(bridge) = &self.host {
                let types_mod = py.import("types")?;
                let new_ns = || -> pyo3::PyResult<Bound<'_, pyo3::PyAny>> {
                    Ok(types_mod.getattr("SimpleNamespace")?.call0()?.into_any())
                };
                let ctx_obj = new_ns()?;
                // Insert in key order (BTreeMap: parents before children —
                // "iterate" never collides with "iterate.start" because
                // the group node is created by the first child's split).
                for (key, f) in &bridge.functions {
                    let f: HostFn = f.clone();
                    let call = PyCFunction::new_closure(py, None, None, move |args, _kw| {
                        let py = unsafe { Python::assume_attached() };
                        let raw: String = args.get_item(0)?.extract()?;
                        let decoded: Value = serde_json::from_str(&raw)
                            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("host arg not JSON: {e}")))?;
                        // Release the GIL around the host call: ctx fns
                        // block_on an async round trip that may run ANOTHER
                        // python booth's session (iterate, invoke) — that
                        // session needs the GIL. Holding it across the
                        // block is a guaranteed deadlock; allow_threads is
                        // the pyo3 contract for exactly this (ADR-0034 e2e
                        // caught it; the same shape applies to ctx.invoke
                        // between two python booths).
                        let out = {
                            let f = f.clone();
                            py.detach(move || (f)(decoded))
                        }
                            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                        Ok::<_, pyo3::PyErr>(json_to_py(py, &out)?.unbind())
                    })?;
                    // Split the dotted path: descend/create a
                    // SimpleNamespace per group, bind the leaf callable.
                    let mut node = ctx_obj.clone().into_any();
                    let parts: Vec<&str> = key.split('.').collect();
                    let (groups, leaf) = parts.split_at(parts.len() - 1);
                    for g in groups {
                        node = match node.getattr(*g) {
                            Ok(existing) => existing,
                            Err(_) => {
                                let group = new_ns()?;
                                node.setattr(*g, &group)?;
                                group
                            }
                        };
                    }
                    node.setattr(leaf[0], call)?;
                }
                module.add("ctx", ctx_obj)?;
            }
            // Storage injection (ADR-0037 4.16a): one Collection per
            // declared entry over the HOST's engine, registered into the
            // module namespace under the collection name — the script
            // writes plain `Counters.put(...)` against the realm store,
            // zero translation. Built HERE (engine handle never crosses
            // a seam back): the pyclass goes straight into `module.add`.
            if let Some(slot) = self.host.as_ref().and_then(|b| b.storage.as_ref()) {
                let engine: std::sync::Arc<dyn okm::pyo3_impl::Engine> =
                    std::sync::Arc::new(ClosureEngine(slot.engine.clone()));
                for coll in &slot.collections {
                    let table = okm::pyo3_impl::Collection::with_store(
                        engine.clone(),
                        &coll.entry,
                        slot.ns,
                    )
                    .map_err(|e| anyhow::anyhow!(
                        "inject collection {}: {e}", coll.name
                    ))?;
                    module.add(coll.name.as_str(), table)?;
                }
            }
            self.module = Some(module.unbind());
            Ok(())
        })
    }

    fn call(&mut self, handler: &str, args: &Value) -> anyhow::Result<Value> {
        Python::attach(|py| -> anyhow::Result<Value> {
            let module = self.module.as_ref()
                .ok_or_else(|| anyhow::anyhow!("python session: call before load"))?
                .bind(py);
            // Handlers are addressed by their EVENT name (the @on
            // decorator binds functions under it). No fallback: a name
            // that does not resolve is an error, never a magic-entry
            // redirect.
            let func = module
                .getattr(handler)
                .map_err(|e| anyhow::anyhow!("python handler {handler}: {e}"))?;
            let args_py = json_to_py(py, args)?;
            let result = func
                .call1((args_py,))
                .map_err(|e| anyhow::anyhow!("python call {handler}: {e}"))?;
            json_from_py(py, &result)
        })
    }

    /// ADR-0034 generator mode: the handler is a native `yield`
    /// generator and NEVER sees the wire shape. Start calls the handler
    /// (creating the generator object; the body runs on first advance)
    /// and parks it under the stream id. Next advances it one step:
    /// a value → `{item, done: false}`, StopIteration → `{done: true}`
    /// and the stream is dropped (exhaustion IS the end, symmetrically
    /// encoded). Dispose sends GeneratorExit via `close()` and drops the
    /// entry — idempotent. Mid-stream exceptions surface as errors
    /// (failure values, ADR-0012).
    fn iterate(&mut self, op: StreamOp) -> anyhow::Result<Value> {
        Python::attach(|py| -> anyhow::Result<Value> {
            let module = self
                .module
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("python session: iterate before load"))?
                .bind(py);
            match op {
                StreamOp::Start { stream_id, handler, args } => {
                    if self.streams.contains_key(&stream_id) {
                        anyhow::bail!("python iterate: stream id '{stream_id}' already live");
                    }
                    let func = module
                        .getattr(&*handler)
                        .map_err(|e| anyhow::anyhow!("python handler {handler}: {e}"))?;
                    let args_py = json_to_py(py, &args)?;
                    let gen = func
                        .call1((args_py,))
                        .map_err(|e| anyhow::anyhow!("python iterate start {handler}: {e}"))?;
                    // A non-generator handler return is legal python
                    // semantics (the body ran to completion, no yield):
                    // unified envelope (ADR-0036 §2) — the plain return
                    // IS the terminal `value`, invoke's shape.
                    if gen.getattr("__next__").is_err() {
                        return Ok(serde_json::json!({
                            "done": true,
                            "value": json_from_py(py, &gen)?,
                        }));
                    }
                    self.streams.insert(stream_id.clone(), gen.unbind());
                    // Start delivers the FIRST envelope (the realm
                    // merges the stream_id into it): the pull that
                    // started the stream is also the first pull —
                    // nothing extra crosses for a consumer that breaks
                    // before the first item.
                    let gen = self
                        .streams
                        .get(&stream_id)
                        .ok_or_else(|| anyhow::anyhow!("python iterate: stream vanished"))?
                        .bind(py);
                    match gen.call_method0("__next__") {
                        Ok(item) => Ok(serde_json::json!({
                            "item": json_from_py(py, &item)?,
                            "done": false,
                        })),
                        Err(e) if e.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) => {
                            self.streams.remove(&stream_id);
                            // StopIteration.value: a generator's `return x`
                            // is the stream's terminal value (ADR-0036 §2 —
                            // the host language already unifies this; the
                            // wire follows).
                            match stop_iteration_value(py, &e) {
                                Some(v) => Ok(serde_json::json!({
                                    "done": true,
                                    "value": json_from_py(py, &v)?,
                                })),
                                None => Ok(serde_json::json!({ "done": true })),
                            }
                        }
                        Err(e) => {
                            self.streams.remove(&stream_id);
                            Err(anyhow::anyhow!("python iterate {stream_id}: {e}"))
                        }
                    }
                }
                StreamOp::Next { stream_id, .. } => {
                    let gen = self
                        .streams
                        .get(&stream_id)
                        .ok_or_else(|| anyhow::anyhow!(
                            "python iterate: stream '{stream_id}' not live (evicted or already done)"
                        ))?
                        .bind(py);
                    match gen.call_method0("__next__") {
                        Ok(item) => Ok(serde_json::json!({
                            "item": json_from_py(py, &item)?,
                            "done": false,
                        })),
                        Err(e) if e.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) => {
                            self.streams.remove(&stream_id);
                            match stop_iteration_value(py, &e) {
                                Some(v) => Ok(serde_json::json!({
                                    "done": true,
                                    "value": json_from_py(py, &v)?,
                                })),
                                None => Ok(serde_json::json!({ "done": true })),
                            }
                        }
                        Err(e) => {
                            // Mid-stream failure: the stream is dead —
                            // drop it and hand the error back (ADR-0012).
                            self.streams.remove(&stream_id);
                            Err(anyhow::anyhow!("python iterate {stream_id}: {e}"))
                        }
                    }
                }
                StreamOp::Dispose { stream_id, .. } => {
                    if let Some(gen) = self.streams.remove(&stream_id) {
                        let _ = gen.bind(py).call_method0("close");
                    }
                    Ok(Value::Null)
                }
            }
        })
    }

    fn as_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Upload-time introspection: load the module once (load discarded after),
/// call the module's `interface_schema` — one function, one call path for
/// aura. The module assembles it at import: the implicit (decorator-derived)
/// half merges with the script's explicit partial declaration (which may
/// add lifecycle etc.). Same call contract as steel/wasm/bgi.
pub fn introspect(source: &str) -> ExecResult {
    Python::attach(|py| -> ExecResult {
        let (module, _registry) = load_module(py, source)
            .map_err(|e| anyhow::anyhow!("python load: {e}"))?;
        let schema_fn = module
            .getattr("interface_schema")
            .map_err(|e| anyhow::anyhow!("python: no interface_schema (carrier assembles it) — {e}"))?;
        let result = schema_fn
            .call1((py.None(),))
            .map_err(|e| anyhow::anyhow!("python interface_schema: {e}"))?;
        json_from_py(py, &result)
    })
}

/// Field-wise schema merge: the explicit (script-written) declaration
/// fills keys the derived (decorator) half does not set; derived
/// receives/wildcard_receives win on their own keys — the decorators are
/// the authoritative source for receives, the explicit half contributes
/// everything else (lifecycle, ...).
fn merge_schema(derived: Value, explicit: Value) -> Value {
    let out = match (derived, explicit) {
        (Value::Object(mut d), Value::Object(e)) => {
            for (k, v) in e {
                d.entry(k).or_insert(v);
            }
            Value::Object(d)
        }
        (d, Value::Object(e)) if e.is_empty() => d,
        (d, e) => if e.is_null() { d } else { e },
    };
    // Deep-merge the receives maps: explicit receives entries that the
    // decorators did not declare still contribute (the script may know a
    // receive the decorators cannot express).
    out
}

/// The payload of a StopIteration: `return x` inside a generator sets
/// the exception's `value` attribute (ADR-0036 §2 — the framework reads
/// `.value`, not just the exception). A bare `return` / exhaustion with
/// no value attribute (or None) projects to the empty terminal.
fn stop_iteration_value<'py>(py: Python<'py>, e: &pyo3::PyErr) -> Option<Bound<'py, PyAny>> {
    let inst = e.value(py);
    inst.getattr("value").ok().filter(|v| !v.is_none())
}

fn json_to_py<'py>(py: Python<'py>, v: &Value) -> PyResult<Bound<'py, PyAny>> {
    Ok(match v {
        Value::Null => py.None().into_bound(py),
        Value::Bool(b) => PyBool::new(py, *b).to_owned().into_any(),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                PyInt::new(py, i).into_any()
            } else {
                PyFloat::new(py, n.as_f64().unwrap_or(0.0)).into_any()
            }
        }
        Value::String(s) => PyString::new(py, s).into_any(),
        Value::Array(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(json_to_py(py, item)?)?;
            }
            list.into_any()
        }
        Value::Object(map) => {
            let dict = PyDict::new(py);
            for (k, val) in map {
                dict.set_item(k, json_to_py(py, val)?)?;
            }
            dict.into_any()
        }
    })
}

// the `py` token rides ONLY into recursive calls (json_from_py walks
// nested containers); it is load-bearing for the walk, not dead —
#[allow(clippy::only_used_in_recursion)]
fn json_from_py(py: Python<'_>, v: &Bound<'_, PyAny>) -> ExecResult {
    if v.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(b) = v.extract::<bool>() {
        return Ok(Value::Bool(b));
    }
    if let Ok(i) = v.extract::<i64>() {
        return Ok(Value::Number(i.into()));
    }
    if let Ok(f) = v.extract::<f64>() {
        return Ok(serde_json::Number::from_f64(f).map(Value::Number).unwrap_or(Value::Null));
    }
    if let Ok(s) = v.extract::<String>() {
        return Ok(Value::String(s));
    }
    if let Ok(list) = v.cast::<PyList>() {
        let mut out = Vec::with_capacity(list.len());
        for item in list.iter() {
            out.push(json_from_py(py, &item)?);
        }
        return Ok(Value::Array(out));
    }
    if let Ok(dict) = v.cast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (k, val) in dict.iter() {
            let key: String = k.extract()?;
            map.insert(key, json_from_py(py, &val)?);
        }
        return Ok(Value::Object(map));
    }
    anyhow::bail!("unsupported python return type: {v:?}")
}
