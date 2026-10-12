//! ADR-0035 `exec` fixture — the bare cgi shape: NO protocol. Read the
//! whole stdin as one JSON request (`{"handler":..., "args":...}`),
//! dispatch, print one JSON result, exit. Nothing survives the call —
//! that is the definition, not a limitation (the php-fpm lineage; the
//! SKILL shape; the bare cgi form any language runs without a loop).
//! The codec follows `BGI_ENCODING` (ADR-0037 §2 dual-protocol): json
//! (default) or cbor — one document in, one document out either way;
//! the request SHAPE is protocol-free under both.

use serde_json::Value;

fn dispatch(req: &Value) -> Value {
    let handler = req.get("handler").and_then(|v| v.as_str()).unwrap_or("");
    let args = req.get("args").cloned().unwrap_or(Value::Null);
    match handler {
        // Echo the args — the invoke shape, no loop involved.
        "echo" => serde_json::json!({ "echoed": args }),
        // A guard counter would be meaningless across calls (the process
        // died); within ONE call it just counts — included to make the
        // statelessness observable, not assumed.
        "count" => {
            let total = args.get("total").and_then(|v| v.as_u64()).unwrap_or(0);
            serde_json::json!({ "counted": total })
        }
        other => serde_json::json!({ "error": format!("one-shot: no handler '{other}'") }),
    }
}

fn main() {
    let mut input = Vec::new();
    if std::io::Read::read_to_end(&mut std::io::stdin(), &mut input).is_err() {
        return;
    }
    let result = match std::env::var("BGI_ENCODING").as_deref() {
        Ok("cbor") => match ciborium::de::from_reader::<Value, _>(input.as_slice()) {
            Ok(req) => dispatch(&req),
            Err(_) => serde_json::json!({ "error": "one-shot: stdin is not one CBOR document" }),
        },
        _ => {
            let text = String::from_utf8_lossy(&input);
            match serde_json::from_str::<Value>(text.trim()) {
                Ok(req) => dispatch(&req),
                Err(_) => {
                    println!("{{\"error\": \"one-shot: stdin is not one JSON document\"}}");
                    return;
                }
            }
        }
    };
    match std::env::var("BGI_ENCODING").as_deref() {
        Ok("cbor") => {
            let mut out = std::io::stdout();
            ciborium::ser::into_writer(&result, &mut out).expect("cbor encode");
        }
        _ => println!("{result}"),
    }
}
