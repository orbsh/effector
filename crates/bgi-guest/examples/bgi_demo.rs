//! bgi_demo — the acceptance binary for the bgi-guest shim: a real guest
//! built ON the facade (what a user's booth looks like), exercising every
//! ctx arm the tests drive. The handlers' bodies ARE the user contract:
//! plain `fn(&mut Ctx, Value)`, registration on the builder, `Guest::run`.

use bgi_guest::Guest;
use serde_json::Value;

fn main() -> anyhow::Result<()> {
    Guest::new()
        .on("echo", |ctx, args| {
            // ctx.invoke against a sibling: the shim sends the typed
            // frame and blocks on the host_reply — the handler sees a
            // plain method call, never the wire.
            let sibling = ctx.invoke("other", "k1", "execute", args.clone())?;
            Ok(serde_json::json!({ "echoed": args, "sibling": sibling }))
        })
        .on("publish", |ctx, args| {
            // ctx.emit: fire-and-forget — the shim sends the frame and
            // answers Null without waiting (the parent must not have to
            // reply, or this call would deadlock).
            ctx.emit("demo.event", args)?;
            Ok(Value::Null)
        })
        .on("store_round_trip", |ctx, args| {
            // The okm instruction rides as DATA (schema-blind); the
            // put's result is discarded, the get's answer returns.
            let put = serde_json::json!({
                "collection": "counters", "op": "put_document",
                "key": { "id": 1 }, "doc": { "count": 42 },
            });
            ctx.store(put)?;
            let get = serde_json::json!({
                "collection": "counters", "op": "get_document",
                "key": { "id": args.get("k").cloned().unwrap_or(Value::Null) },
            });
            let read_back = ctx.store(get)?;
            Ok(serde_json::json!({ "read_back": read_back }))
        })
        .on("reflect", |ctx, _args| {
            let schema = ctx.schema()?;
            Ok(schema)
        })
        .on("failing", |ctx, args| {
            // A ctx call the host answers with the error arm: the shim
            // surfaces it as an Err — failure as a value, never a Null.
            let v = ctx.invoke("other", "k1", "execute", args)?;
            Ok(v)
        })
        .hook("__wake", |_ctx, _args| Ok(serde_json::json!({ "woke": true })))
        .hook("__sleep", |_ctx, _args| Ok(serde_json::json!({ "slept": true })))
        .run()
}
