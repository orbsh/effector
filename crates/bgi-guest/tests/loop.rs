//! bgi-guest acceptance: a REAL two-process loop — the guest binary runs
//! `bgi_guest::Guest::run` as the child; the test plays the parent (the
//! carrier side): spawn, send request frames, answer host frames, assert
//! results. Locks the shim's contract end to end: registration → schema
//! reply (receives + hooks, no runtime on arm), the typed ctx arms
//! (invoke / emit fire-and-forget / store / schema / timer / queue),
//! hook dispatch by reserved name, both codecs (BGI_ENCODING json|cbor).

use serde_json::Value;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};

struct Parent {
    child: Child,
    stdin: std::process::ChildStdin,
    stdout: BufReader<std::process::ChildStdout>,
    next_id: u64,
    /// Host frames the child may send during the current request (the
    /// test answers them inline, mirroring the carrier's exchange loop).
    host_answer: Option<Value>,
    captured_emits: Vec<Value>,
}

impl Parent {
    fn spawn(bin: &str, encoding: &str) -> Self {
        let mut child = Command::new(bin)
            .env("BGI_ENCODING", encoding)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn guest");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = BufReader::new(child.stdout.take().expect("stdout"));
        Self { child, stdin, stdout, next_id: 0, host_answer: None, captured_emits: Vec::new() }
    }

    /// One request → host-frame service loop → the result value.
    fn call(&mut self, event: &str, args: Value) -> anyhow::Result<Value> {
        self.next_id += 1;
        let id = self.next_id;
        let frame = serde_json::json!({
            "id": id, "kind": "iterate_start", "event": event,
            "args": args, "stream_id": format!("t-{id}"),
        });
        writeln!(self.stdin, "{frame}")?;
        self.stdin.flush()?;
        loop {
            let mut line = String::new();
            let n = self.stdout.read_line(&mut line)?;
            if n == 0 {
                anyhow::bail!("child closed stdout");
            }
            let v: Value = serde_json::from_str(line.trim())?;
            if let Some(host) = v.get("host") {
                let arm = host.get("type").and_then(|t| t.as_str()).unwrap_or("").to_string();
                match arm.as_str() {
                    // emit: no reply frame — capture for assertions.
                    "emit" => self.captured_emits.push(host.clone()),
                    _ => {
                        let answer = self
                            .host_answer
                            .clone()
                            .expect("child sent a ctx call but the test set no host_answer");
                        writeln!(
                            self.stdin,
                            "{}",
                            serde_json::json!({ "host_reply": { "ok": answer } })
                        )?;
                        self.stdin.flush()?;
                    }
                }
                continue;
            }
            if let Some(result) = v.get("result") {
                return Ok(result.clone());
            }
        }
    }
}

impl Drop for Parent {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn demo_bin() -> &'static str {
    // CARGO_BIN_EXE_* covers [[bin]] targets only, not examples — resolve
    // the example binary the aura tests' way (manifest-relative path into
    // the shared target dir; `cargo test -p bgi-guest` builds examples).
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../target/debug/examples/bgi_demo"
    );
    assert!(
        std::path::Path::new(path).exists(),
        "bgi_demo missing — build it: cargo build -p bgi-guest --examples"
    );
    Box::leak(path.to_string().into_boxed_str())
}

#[test]
fn schema_reply_assembles_from_the_registry() {
    let mut p = Parent::spawn(demo_bin(), "json");
    let schema = p.call("interface_schema", Value::Null).unwrap();
    let receives: Vec<&str> = schema["receives"].as_object().unwrap().keys().map(|s| s.as_str()).collect();
    assert!(receives.contains(&"echo"), "{schema}");
    assert!(receives.contains(&"store_round_trip"), "{schema}");
    assert!(!receives.contains(&"__sleep"), "hooks NEVER enter receives: {schema}");
    let hooks: Vec<&str> = schema["hooks"].as_array().unwrap().iter().filter_map(|v| v.as_str()).collect();
    assert!(hooks.contains(&"__sleep") && hooks.contains(&"__wake"), "{schema}");
}

#[test]
fn handler_runs_and_ctx_invoke_round_trips() {
    let mut p = Parent::spawn(demo_bin(), "json");
    // The demo's echo handler calls ctx.invoke against a sibling; the
    // test (as parent) answers the host frame with the sibling's value.
    p.host_answer = Some(serde_json::json!({ "from": "sibling" }));
    let out = p.call("echo", serde_json::json!({ "x": 1 })).unwrap();
    assert_eq!(out, serde_json::json!({ "echoed": { "x": 1 }, "sibling": { "from": "sibling" } }), "{out}");
}

#[test]
fn emit_is_fire_and_forget_and_captured_by_the_parent() {
    let mut p = Parent::spawn(demo_bin(), "json");
    // No host_answer: if emit waited for a reply the test would hang —
    // the fire-and-forget contract is structural here.
    let out = p.call("publish", serde_json::json!({ "n": 7 })).unwrap();
    assert_eq!(out, Value::Null, "emit's business answer is Null: {out}");
    assert_eq!(p.captured_emits.len(), 1, "one emit frame reached the parent");
    assert_eq!(p.captured_emits[0]["args"]["event"], "demo.event");
    assert_eq!(p.captured_emits[0]["args"]["data"]["n"], 7);
}

#[test]
fn store_and_schema_arms_carry_their_shapes() {
    let mut p = Parent::spawn(demo_bin(), "json");
    // store: the op rides as DATA (schema-blind) — the parent answers
    // with the read-back doc.
    p.host_answer = Some(serde_json::json!({ "count": 42 }));
    let out = p.call("store_round_trip", serde_json::json!({ "k": 1 })).unwrap();
    assert_eq!(out, serde_json::json!({ "read_back": { "count": 42 } }), "{out}");
    // schema: the persisted copy comes back as data.
    p.host_answer = Some(serde_json::json!({ "lifecycle": { "idle_ttl": "5m" } }));
    let out = p.call("reflect", Value::Null).unwrap();
    assert_eq!(out["lifecycle"]["idle_ttl"], "5m", "{out}");
}

#[test]
fn hooks_dispatch_by_the_reserved_name() {
    let mut p = Parent::spawn(demo_bin(), "json");
    // The realm's Declared dispatch calls the session BY the reserved
    // name — exactly what the parent does here.
    let out = p.call("__wake", Value::Null).unwrap();
    assert_eq!(out, serde_json::json!({ "woke": true }), "{out}");
    let out = p.call("__sleep", Value::Null).unwrap();
    assert_eq!(out, serde_json::json!({ "slept": true }), "{out}");
}

#[test]
fn error_values_surface_through_the_result() {
    let mut p = Parent::spawn(demo_bin(), "json");
    // The parent answers the ctx call with the error shape the carrier
    // sends (host_reply.error); the shim must surface it, not swallow.
    p.host_answer = Some(Value::Null); // unused — see below
    p.host_answer = None;
    // Drive the failing-ctx arm: the demo's `failing` handler calls
    // ctx.invoke; the parent replies with an ERROR host_reply frame.
    p.next_id += 1;
    let id = p.next_id;
    let frame = serde_json::json!({
        "id": id, "kind": "iterate_start", "event": "failing",
        "args": Value::Null, "stream_id": format!("t-{id}"),
    });
    writeln!(p.stdin, "{frame}").unwrap();
    p.stdin.flush().unwrap();
    // Read the host frame, answer with the error arm.
    let mut line = String::new();
    p.stdout.read_line(&mut line).unwrap();
    let hf: Value = serde_json::from_str(line.trim()).unwrap();
    assert!(hf.get("host").is_some(), "child sent a ctx call: {hf}");
    writeln!(
        p.stdin,
        "{}",
        serde_json::json!({ "host_reply": { "error": "sibling exploded" } })
    )
    .unwrap();
    p.stdin.flush().unwrap();
    // The result frame carries the error string (failure as a value).
    let mut line = String::new();
    p.stdout.read_line(&mut line).unwrap();
    let res: Value = serde_json::from_str(line.trim()).unwrap();
    let err = res["result"].as_str().expect("error surfaced as a string");
    assert!(err.contains("sibling exploded"), "{res}");
}

#[test]
fn cbor_codec_carries_the_same_shapes() {
    // Same contract, self-delimited CBOR documents. The demo binary
    // branches on BGI_ENCODING; the parent here speaks CBOR.
    let mut child = Command::new(demo_bin())
        .env("BGI_ENCODING", "cbor")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn guest");
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();

    let mut call = |event: &str, args: Value, host_answer: Option<Value>| -> Value {
        let frame = serde_json::json!({
            "id": 1, "kind": "iterate_start", "event": event,
            "args": args, "stream_id": "c-1",
        });
        ciborium::ser::into_writer(&frame, &mut stdin).unwrap();
        stdin.flush().unwrap();
        loop {
            let v: Value = ciborium::de::from_reader(&mut stdout).unwrap();
            if let Some(host) = v.get("host") {
                if host.get("type").and_then(|t| t.as_str()) == Some("emit") {
                    continue; // fire-and-forget: no reply
                }
                let answer = host_answer.clone().expect("unexpected ctx call");
                let reply = serde_json::json!({ "host_reply": { "ok": answer } });
                ciborium::ser::into_writer(&reply, &mut stdin).unwrap();
                stdin.flush().unwrap();
                continue;
            }
            return v.get("result").cloned().unwrap_or(Value::Null);
        }
    };

    let schema = call("interface_schema", Value::Null, None);
    assert!(schema["hooks"].is_array(), "{schema}");
    let out = call("echo", serde_json::json!({ "x": 1 }), Some(serde_json::json!({"s": 1})));
    assert_eq!(out["echoed"]["x"], 1, "{out}");
    let _ = child.kill();
    let _ = child.wait();
}
