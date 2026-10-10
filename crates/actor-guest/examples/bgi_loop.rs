//! BGI carrier fixture (ADR-0035): a native bin speaking the framed
//! protocol — the contract a compiled Rust booth implements against
//! (or links a shim to; the loop lives on the child side in bgi).
//! One request in, one result out.
//! `ctx_round_trip` answers a plain call with a child→parent ctx
//! round trip (typed host frame → host_reply → result), proving the ctx
//! seam crosses the process boundary. Iterate handlers return envelopes
//! per ADR-0034 with the guard counter in this process's memory (the
//! residency IS the booth state — idle_ttl eviction kills it, matching
//! generator-mode semantics without a generator language).
//!
//! Frame ENCODING is declared by the parent through `BGI_ENCODING`
//! ("json" | "cbor"; absent = json) — the ADR-0037 §2 dual-protocol
//! ruling. Both codecs carry the same Value-shaped frames; JSON frames
//! are text lines, CBOR frames are self-delimited documents. The host
//! frame is TYPED under both (`{"host":{"type":…}}`) — the free op-name
//! string is retired; a bad discriminator fails at the parent's decode.
//!
//! No aura dependencies: serde_json + ciborium. The protocol is the ABI.

use serde_json::Value;
use std::collections::HashMap;
use std::io::{BufRead, Write};

struct Guard {
    pulled: u64,
    total: u64,
}

fn main() {
    match std::env::var("BGI_ENCODING")
        .as_deref()
        .unwrap_or("json")
    {
        "cbor" => run_cbor(),
        _ => run_json(),
    }
}

/// One request frame's fields unpacked (identical shape under both codecs).
struct Job {
    id: Value,
    kind: String,
    event: String,
    args: Value,
    stream_id: String,
}

fn job_from(req: &Value) -> Job {
    Job {
        id: req.get("id").cloned().unwrap_or(Value::Null),
        kind: req.get("kind").and_then(|v| v.as_str()).unwrap_or("call").to_string(),
        event: req.get("event").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        args: req.get("args").cloned().unwrap_or(Value::Null),
        stream_id: req.get("stream_id").and_then(|v| v.as_str()).unwrap_or("").to_string(),
    }
}

/// The JSON-lines shape: read lines until EOF, one request per line;
/// a host_reply arrives as its own line mid-call (the child blocks on
/// its ctx call — the synchronous-by-contract rule).
fn run_json() {
    let stdin = std::io::stdin();
    let mut lines = stdin.lock().lines();
    let mut streams: HashMap<String, Guard> = HashMap::new();
    while let Some(Ok(line)) = lines.next() {
        let req: Value = match serde_json::from_str(line.trim()) {
            Ok(v) => v,
            Err(_) => {
                emit_json(&serde_json::json!({"result": {"error": "malformed frame"}}));
                continue;
            }
        };
        let job = job_from(&req);
        // The ctx round trips answer inline on THIS channel — a shared
        // helper per handler, emitting the typed frame and reading the
        // host_reply line it waits for.
        let mut host = |frame: Value| -> Value {
            emit_json(&frame);
            loop {
                match lines.next() {
                    Some(Ok(inner)) => {
                        if let Ok(v) = serde_json::from_str::<Value>(inner.trim()) {
                            if let Some(hr) = v.get("host_reply") {
                                break hr.get("ok").cloned().unwrap_or(Value::Null);
                            }
                        }
                    }
                    _ => break Value::String("no host_reply".into()),
                }
            }
        };
        let result = dispatch(&job, &mut streams, &mut host);
        emit_json(&serde_json::json!({"id": job.id, "result": result}));
    }
}

/// The CBOR shape: decode one self-delimited document per read from the
/// blocking byte stream; host replies ride the same stream (decoded in
/// order — the parent answers on stdin between request rounds).
fn run_cbor() {
    let stdin = std::io::stdin();
    let mut reader = stdin.lock();
    let mut streams: HashMap<String, Guard> = HashMap::new();
    // ciborium reads EXACTLY the declared bytes per value (no
    // over-read), so sequential from_reader calls on the persistent
    // blocking reader land document-by-document; a decode error (the
    // parent closed stdin = EOF) ends the session.
    while let Ok(req) = ciborium::de::from_reader(&mut reader) {
        let job = job_from(&req);
        let mut host = |frame: Value| -> Value {
            let mut out = std::io::stdout();
            ciborium::ser::into_writer(&frame, &mut out).expect("cbor encode");
            out.flush().unwrap();
            match ciborium::de::from_reader(&mut reader) {
                Ok(v) => {
                    let v: Value = v;
                    v.get("host_reply")
                        .and_then(|hr| hr.get("ok"))
                        .cloned()
                        .unwrap_or(Value::Null)
                }
                Err(_) => Value::String("no host_reply".into()),
            }
        };
        let result = dispatch(&job, &mut streams, &mut host);
        let mut out = std::io::stdout();
        ciborium::ser::into_writer(
            &serde_json::json!({"id": job.id, "result": result}),
            &mut out,
        )
        .expect("cbor encode");
        out.flush().unwrap();
    }
}

/// The shared dispatch (identical under both codecs): one request in,
/// the reply frame's result value out — the caller emits it in the
/// session's codec (the codec is I/O, the dispatch is logic).
fn dispatch(
    job: &Job,
    streams: &mut HashMap<String, Guard>,
    host: &mut impl FnMut(Value) -> Value,
) -> Value {
    let Job { kind, event, args, stream_id, .. } = job;
    // Unified seam (ADR-0036): dispatch jobs arrive as
    // `iterate_start` — invoke is the stream whose first round is
    // terminal. A plain handler replies with a bare value (no `done`
    // field); the carrier wraps it to `{done:true,value}` (0036 §2).
    // The bare `call` kind rides the same arms: it stays the
    // carrier-internal primitive (introspection, effector-side tests)
    // while the aura dispatch seam has folded onto the stream verb.
    match (kind.as_str(), event.as_str()) {
        // Introspection answers the same schema either way.
        ("call", "interface_schema") | ("iterate_start", "interface_schema") => serde_json::json!({
            "receives": { "echo": {}, "slow": {}, "ctx_round_trip": {}, "store_round_trip": {}, "bad_host_frame": {}, "count": {} },
            "wildcard_receives": [],
            "storage": { "collections": { "counters": { "schema": {
                "key_len": 8,
                "key_fields": [{"name": "id", "ty": "U64", "width": 8, "offset": 0, "tag": 0}],
                "layout_version": 1, "hot_width": 8, "payload_header_len": 3,
                "hot_fields": [{"name": "count", "ty": "U64", "width": 8, "offset": 0, "tag": 0}],
                "cold_fields": [],
                "slots": {"primary": 0, "dynamic": 1, "dict_id": 2, "dict_name": 3,
                          "declared_index_base": 4096, "declared_reduce_base": 8192,
                          "junction_base": 12288}
            }}}}
        }),
        // Plain handlers: reply a bare value; the carrier wraps to a
        // terminal envelope. echo echoes the args.
        ("call" | "iterate_start", "echo") => serde_json::json!({"echoed": args}),
        // A genuinely slow handler (the deadline tests need real
        // latency a hot timeout can beat): half a second, synchronous.
        ("call" | "iterate_start", "slow") => {
            std::thread::sleep(std::time::Duration::from_millis(500));
            args.clone()
        }
        // Plain handler: ONE typed host frame (ADR-0037 §2 — the
        // discriminator is `type`, no free op-name string), then forward
        // the answer.
        ("call" | "iterate_start", "ctx_round_trip") => {
            host(serde_json::json!({"host": {"type": "invoke", "args": args}}))
        }
        // The retired shape on purpose (ADR-0037 §2 lock): an UNTYPED
        // frame (free `op` name, no discriminator) must fail at the
        // parent's typed decode — the bridge's fn never runs, and the
        // child sees the decode failure as an error value.
        ("call" | "iterate_start", "bad_host_frame") => {
            let mut out = host(serde_json::json!({"host": {"op": "invoke", "args": args}}));
            if out.is_null() {
                out = serde_json::json!({"note": "host_reply.ok was null (decode error lands as error field, not ok)"});
            }
            out
        }
        // Typed store frames over the seam (Phase 4.14 gate 1 entry,
        // ADR-0037 §2 re-shaped): the child forwards its `args` as two
        // okm instructions — put, then read-back — and answers with what
        // the host's store read returns. Pure transport: the child never
        // parses the instruction, exactly the rule the wire enforces.
        ("iterate_start", "store_round_trip") => {
            let put = args.get("put").cloned().unwrap_or(Value::Null);
            let get = args.get("get").cloned().unwrap_or(Value::Null);
            host(serde_json::json!({"host": {"type": "store", "op": put}}));
            let read_back = host(serde_json::json!({"host": {"type": "store", "op": get}}));
            serde_json::json!({"read_back": read_back})
        }
        // Streaming producer (`count`): iterate_start registers the
        // guard and the first round carries the first item (ADR-0034
        // — Start is also the first pull); a total of 0 is terminal.
        ("iterate_start", "count") => {
            let total = args.get("total").and_then(|v| v.as_u64()).unwrap_or(0);
            if total == 0 {
                serde_json::json!({"done": true})
            } else {
                streams.insert(stream_id.clone(), Guard { pulled: 1, total });
                serde_json::json!({"item": "i0", "done": false})
            }
        }
        // iterate_next: advance the guard; exhaustion WRITES done.
        ("iterate_next", _) => match streams.get_mut(stream_id) {
            None => serde_json::json!({"error": format!("unknown stream {stream_id}")}),
            Some(g) => {
                g.pulled += 1;
                if g.pulled > g.total {
                    streams.remove(stream_id);
                    serde_json::json!({"done": true})
                } else {
                    // pulled counts rounds INCLUDING start's i0 —
                    // item index is the 0-based round.
                    let n = g.pulled - 1;
                    serde_json::json!({"item": format!("i{n}"), "done": false})
                }
            }
        },
        ("iterate_dispose", _) => {
            streams.remove(stream_id);
            Value::Null
        }
        _ => serde_json::json!({"error": "no such handler"}),
    }
}

fn emit_json(v: &Value) {
    let mut out = std::io::stdout();
    writeln!(out, "{v}").unwrap();
    out.flush().unwrap();
}
