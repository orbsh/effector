//! bgi-guest — the Rust user face for bgi booths (ADR-0043 §5, PLAN 4.21
//! step 1): a typed `ctx` facade over the frame protocol. Zero aura/effector
//! dependencies — serde_json + ciborium only, the protocol IS the ABI.
//!
//! A compiled Rust booth links this crate and implements its handlers as
//! plain `fn(&Ctx, Value) -> Result<Value>`; the crate owns everything else:
//!
//! - the main loop (JSON-lines or CBOR, per `BGI_ENCODING` — the parent's
//!   declared codec crosses as an env, not an argv),
//! - the typed host frames (`{"host": {"type": …}}` → `host_reply`),
//! - the dispatch by event name, the iterate guard table, and the
//!   introspection answer,
//! - registration on the `Guest` builder: `.on(event, handler)` +
//!   `.hook("__sleep"/"__wake", h)`; the schema reply (receives + hooks +
//!   optional storage) assembles from the SAME registry — one declaration
//!   surface, no hand-written schema.
//!
//! `ctx` methods mirror the aura surface 1:1 (ADR-0043 §5): invoke, emit,
//! store, schema, timer.register/cancel, queue.depth/skip_to_head (the
//! iterate consumer cursor is a Phase-4.21b follow-up — the producer guard
//! table is here). Phase gate: `on`/`hook` are BUILDER methods — after
//! `Guest::run` begins, registration is structurally closed (the Rust
//! mirror of the activation gate: unrepresentable, not an error value).

use serde_json::Value;
use std::collections::HashMap;
use std::io::{BufRead, Write};

// ------------------------------------------------------------ ctx --

/// The typed ctx facade: arms mirror the aura surface's CALLABLE members
/// (ADR-0043 §5). Every method = one typed host frame + one blocking
/// host_reply read (the synchronous-by-contract rule); `emit` is the
/// fire-and-forget exception — the parent answers NOTHING, a publish
/// failure lands on the parent's stderr, never as an error value into a
/// guest that stopped listening.
pub struct Ctx<'a> {
    io: &'a mut dyn Codec,
}

impl Ctx<'_> {
    fn round_trip(&mut self, arm: &str, inner: Value) -> anyhow::Result<Value> {
        self.io.write(&serde_json::json!({
            "host": { "type": arm, "args": inner }
        }))?;
        read_host_reply(self.io)
    }

    /// `ctx.invoke(target, handler, args)` → the target's return value.
    pub fn invoke(
        &mut self,
        ty: &str,
        key: &str,
        handler: &str,
        args: Value,
    ) -> anyhow::Result<Value> {
        self.round_trip(
            "invoke",
            serde_json::json!({
                "type": ty, "key": key, "handler": handler, "args": args,
            }),
        )
    }

    /// `ctx.emit(event, data)` — fire-and-forget by contract (no reply
    /// frame is sent; the answer is Null on the business plane).
    pub fn emit(&mut self, event: &str, data: Value) -> anyhow::Result<()> {
        self.io.write(&serde_json::json!({
            "host": { "type": "emit", "args": { "event": event, "data": data } }
        }))?;
        Ok(())
    }

    /// `ctx.store(op)` — one okm Collection instruction as data (the
    /// parent resolves it against the type's declared collections; the
    /// guest never parses the instruction's semantics).
    pub fn store(&mut self, op: Value) -> anyhow::Result<Value> {
        // The store arm carries the op as DATA, not under `args` — the
        // schema-blind rule (the carrier's TypedHostFrame::Store).
        self.io.write(&serde_json::json!({ "host": { "type": "store", "op": op } }))?;
        read_host_reply(self.io)
    }

    /// `ctx.schema` — the persisted interface_schema copy (reflection).
    pub fn schema(&mut self) -> anyhow::Result<Value> {
        self.io.write(&serde_json::json!({ "host": { "type": "schema" } }))?;
        read_host_reply(self.io)
    }

    /// `ctx.timer.register(at_ms, tag)` → the timer id.
    pub fn timer_register(&mut self, at_ms: u64, tag: &str) -> anyhow::Result<u64> {
        let v = self.round_trip("timer.register", serde_json::json!({ "at_ms": at_ms, "tag": tag }))?;
        Ok(v.get("timer_id").and_then(|x| x.as_u64()).unwrap_or(0))
    }

    /// `ctx.timer.cancel(id)` — idempotent (unknown = already fired).
    pub fn timer_cancel(&mut self, timer_id: u64) -> anyhow::Result<()> {
        self.round_trip("timer.cancel", serde_json::json!({ "timer_id": timer_id }))?;
        Ok(())
    }

    /// `ctx.queue.depth(event)` → the backlog point-read.
    pub fn queue_depth(&mut self, event: &str) -> anyhow::Result<u64> {
        let v = self.round_trip("queue.depth", Value::String(event.into()))?;
        Ok(v.as_u64().unwrap_or(0))
    }

    /// `ctx.queue.skip_to_head(event)` — the relief valve (directional
    /// discard: skipped backlog never revives).
    pub fn queue_skip_to_head(&mut self, event: &str) -> anyhow::Result<()> {
        self.round_trip("queue.skip_to_head", Value::String(event.into()))?;
        Ok(())
    }
}

// ------------------------------------------------------------ codec --

/// The frame codec: HOW a `Value` crosses the pipe. JSON = text lines;
/// CBOR = self-delimited documents (ciborium reads exactly the declared
/// bytes per value — no over-read — so sequential decodes on the
/// persistent blocking reader land document-by-document; EOF ends the
/// session). The shape under both codecs is the same `Value`.
trait Codec {
    fn read(&mut self) -> Option<Value>;
    fn write(&mut self, v: &Value) -> anyhow::Result<()>;
}

struct JsonCodec<R: BufRead, W: Write> {
    reader: R,
    out: W,
}

impl<R: BufRead, W: Write> Codec for JsonCodec<R, W> {
    fn read(&mut self) -> Option<Value> {
        let mut line = String::new();
        match self.reader.read_line(&mut line) {
            Ok(0) | Err(_) => None,
            Ok(_) => serde_json::from_str(line.trim()).ok(),
        }
    }
    fn write(&mut self, v: &Value) -> anyhow::Result<()> {
        writeln!(self.out, "{v}")?;
        self.out.flush()?;
        Ok(())
    }
}

struct CborCodec<R: std::io::Read, W: Write> {
    reader: R,
    out: W,
}

impl<R: std::io::Read, W: Write> Codec for CborCodec<R, W> {
    fn read(&mut self) -> Option<Value> {
        ciborium::de::from_reader(&mut self.reader).ok()
    }
    fn write(&mut self, v: &Value) -> anyhow::Result<()> {
        ciborium::ser::into_writer(v, &mut self.out)?;
        self.out.flush()?;
        Ok(())
    }
}

/// Read one `host_reply` and unpack it: `ok` carries the value; an
/// `error` arm becomes the Err (a swallowed Null made the parent's
/// failure invisible — the fixture lesson, now the library's rule).
fn read_host_reply(io: &mut dyn Codec) -> anyhow::Result<Value> {
    let v = io.read().ok_or_else(|| anyhow::anyhow!("host closed the channel mid-call (no host_reply)"))?;
    let hr = v
        .get("host_reply")
        .ok_or_else(|| anyhow::anyhow!("expected a host_reply frame, got {v}"))?;
    match hr.get("ok") {
        Some(ok) => Ok(ok.clone()),
        None => {
            let err = hr
                .get("error")
                .cloned()
                .unwrap_or(Value::String("host_reply carried neither ok nor error".into()));
            Err(anyhow::anyhow!("ctx call failed: {}", err))
        }
    }
}

// ------------------------------------------------------ the guest --

type Handler = Box<dyn Fn(&mut Ctx, Value) -> anyhow::Result<Value> + Send>;

/// One iterate producer's guard (the bgi_loop fixture's shape): the
/// handler drives its own stream state; the guest tracks the pull count
/// only where the protocol needs it (exhaustion is terminal — the
/// producer WRITES `done`).
struct StreamGuard {
    pulled: u64,
    total: u64,
}

/// The registration registry + the run loop owner. Assembled at
/// construction; frozen when `run` begins (the Rust phase gate).
pub struct Guest {
    handlers: HashMap<String, Handler>,
    hooks: Vec<String>,
    wildcards: Vec<String>,
    storage: Option<Value>,
}

impl Default for Guest {
    fn default() -> Self {
        Self::new()
    }
}

impl Guest {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
            hooks: Vec::new(),
            wildcards: Vec::new(),
            storage: None,
        }
    }

    /// Register an event handler (the `@on` / `ctx.on` equivalent). A
    /// `__`-prefixed event here is a DECLARATION ERROR — lifecycle hooks
    /// register through [`Guest::hook`], mirroring the schema rule that
    /// reserved names never enter receives (ADR-0043 §3).
    pub fn on(
        mut self,
        event: impl Into<String>,
        h: impl Fn(&mut Ctx, Value) -> anyhow::Result<Value> + Send + 'static,
    ) -> Self {
        let event = event.into();
        assert!(
            !event.starts_with("__"),
            "`{event}` is a reserved hook name — register lifecycle hooks with .hook(), never .on()"
        );
        self.handlers.insert(event, Box::new(h));
        self
    }

    /// Register a wildcard (prefix pattern) subscription — singleton
    /// consumers; the pattern rides wildcard_receives.
    pub fn on_wildcard(mut self, pattern: impl Into<String>) -> Self {
        let pattern = pattern.into();
        assert!(
            pattern.ends_with('*'),
            "wildcard patterns are prefix form (`prefix.*`): {pattern}"
        );
        self.wildcards.push(pattern);
        self
    }

    /// Register a reserved lifecycle hook (ADR-0043 §3): `__sleep` /
    /// `__wake` only. Lands in the schema `hooks` array, never receives.
    /// The handler runs when the realm's Declared dispatch calls the
    /// session by the reserved name.
    pub fn hook(
        mut self,
        name: impl Into<String>,
        h: impl Fn(&mut Ctx, Value) -> anyhow::Result<Value> + Send + 'static,
    ) -> Self {
        let name = name.into();
        assert!(
            matches!(name.as_str(), "__sleep" | "__wake"),
            "hook name `{name}` is not reserved (allowed: [\"__sleep\", \"__wake\"])"
        );
        if !self.hooks.contains(&name) {
            self.hooks.push(name.clone());
        }
        self.handlers.insert(name, Box::new(h));
        self
    }

    /// Declare the storage block (the ctx.store plan source): the schema
    /// object the parent's StorePlan::from_schema parses — collections
    /// keyed by name, each carrying its okm `schema` (key_fields /
    /// hot_fields / slots, the StorePlan JSON contract).
    pub fn storage(mut self, schema: Value) -> Self {
        self.storage = Some(schema);
        self
    }

    /// Run the guest: own the stdio channels, loop until EOF (the parent
    /// closed stdin = the session ends — eviction or teardown). One
    /// request frame in, one result frame out; ctx round trips answer
    /// inline on the same channel.
    pub fn run(self) -> anyhow::Result<()> {
        match std::env::var("BGI_ENCODING").as_deref() {
            Ok("cbor") => {
                let cin = std::io::stdin();
                let cout = std::io::stdout();
                self.run_loop(CborCodec { reader: cin.lock(), out: cout.lock() })
            }
            _ => {
                let cin = std::io::stdin();
                let cout = std::io::stdout();
                self.run_loop(JsonCodec { reader: cin.lock(), out: cout.lock() })
            }
        }
    }

    fn run_loop<C: Codec>(self, mut io: C) -> anyhow::Result<()> {
        let mut streams: HashMap<String, StreamGuard> = HashMap::new();
        while let Some(req) = io.read() {
            let id = req.get("id").cloned().unwrap_or(Value::Null);
            let kind = req.get("kind").and_then(|v| v.as_str()).unwrap_or("call").to_string();
            let event = req.get("event").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let args = req.get("args").cloned().unwrap_or(Value::Null);
            let stream_id = req.get("stream_id").and_then(|v| v.as_str()).unwrap_or("").to_string();

            let result = if event == "interface_schema" {
                // The introspection reply IS the assembled registry — one
                // declaration surface (receives + wildcard_receives +
                // hooks + optional storage).
                self.schema_reply()
            } else {
                let mut ctx = Ctx { io: &mut io };
                match (kind.as_str(), event.as_str()) {
                    // Unified seam (ADR-0036): dispatch jobs arrive as
                    // iterate_start — a plain handler's bare reply wraps
                    // to `{done:true,value}` at the carrier (§2). The
                    // bare `call` kind stays the carrier-internal
                    // primitive (introspection, effector-side tests).
                    ("call", e) | ("iterate_start", e) => self.run_handler(&mut ctx, e, args, &stream_id, &mut streams),
                    ("iterate_next", _) => self.pull(&mut ctx, &stream_id, &mut streams),
                    ("iterate_dispose", _) => {
                        streams.remove(&stream_id);
                        Ok(Value::Null)
                    }
                    _ => Err(anyhow::anyhow!("unknown frame kind {kind}")),
                }
            };
            let result = match result {
                Ok(v) => v,
                Err(e) => Value::String(format!("{e:#}")),
            };
            io.write(&serde_json::json!({ "id": id, "result": result }))?;
        }
        Ok(())
    }

    fn run_handler(
        &self,
        ctx: &mut Ctx,
        event: &str,
        args: Value,
        stream_id: &str,
        streams: &mut HashMap<String, StreamGuard>,
    ) -> anyhow::Result<Value> {
        let Some(h) = self.handlers.get(event) else {
            anyhow::bail!("no such handler: {event}");
        };
        // Iterate producers: the `count`-style guard pattern stays
        // handler-owned; a producer arms its guard by returning a
        // non-terminal envelope on the first round. The guest only
        // tracks that a stream opened (the disposal bookkeeping the
        // carrier validates).
        let out = h(ctx, args)?;
        let opened = out.get("done").and_then(|d| d.as_bool()) == Some(false);
        if opened {
            let total = out.get("total").and_then(|t| t.as_u64()).unwrap_or(u64::MAX);
            streams.insert(stream_id.to_string(), StreamGuard { pulled: 1, total });
        }
        Ok(out)
    }

    fn pull(
        &self,
        ctx: &mut Ctx,
        stream_id: &str,
        streams: &mut HashMap<String, StreamGuard>,
    ) -> anyhow::Result<Value> {
        let Some(g) = streams.get_mut(stream_id) else {
            anyhow::bail!("unknown stream {stream_id}");
        };
        g.pulled += 1;
        if g.pulled > g.total {
            streams.remove(stream_id);
            return Ok(serde_json::json!({ "done": true }));
        }
        // The generic producer shape: ask the handler to produce round N
        // by calling it with the envelope args (the envelope-mode rule —
        // the guard state is the guest's, the args are the stream's).
        // A registered `__next__` handler takes {stream_id, round}.
        let h = self
            .handlers
            .get("__next__")
            .ok_or_else(|| anyhow::anyhow!("stream {stream_id}: no __next__ producer bound"))?;
        h(ctx, serde_json::json!({ "stream_id": stream_id, "round": g.pulled - 1 }))
    }

    fn schema_reply(&self) -> anyhow::Result<Value> {
        let mut receives = serde_json::Map::new();
        for name in self.handlers.keys() {
            if name.starts_with("__") {
                continue;
            }
            receives.insert(name.clone(), Value::Object(serde_json::Map::new()));
        }
        let mut schema = serde_json::Map::new();
        schema.insert("receives".into(), Value::Object(receives));
        schema.insert(
            "wildcard_receives".into(),
            Value::Array(self.wildcards.iter().cloned().map(Value::String).collect()),
        );
        if !self.hooks.is_empty() {
            schema.insert(
                "hooks".into(),
                Value::Array(self.hooks.iter().cloned().map(Value::String).collect()),
            );
        }
        if let Some(storage) = &self.storage {
            schema.insert("storage".into(), storage.clone());
        }
        Ok(Value::Object(schema))
    }
}
