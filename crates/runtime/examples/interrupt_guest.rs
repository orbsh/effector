//! Interrupt fixture (ADR-0035 + aura ADR-0047 stage two): a resident guest whose
//! one handler does NOT answer on its own — it waits until the parent's interrupt
//! frame turns its token, then answers. That shape is the whole point: a handler
//! blocked inside one long operation, stoppable only from the loop that is still
//! reading.
//!
//! Built from `induction-guest`, i.e. the shim a deployed guest links, so the test
//! exercises the shipped path rather than a hand-written frame loop.

use induction_guest::{Ctx, Guest};
use serde_json::{Value, json};
use std::time::{Duration, Instant};

fn main() -> anyhow::Result<()> {
    Guest::new()
        .named("interrupt")
        .on("wait", |ctx: &mut Ctx, _args: Value| {
            let started = Instant::now();
            // No ceiling worth the name: the test's assertion is that this returns
            // because it was told, not because it gave up.
            while started.elapsed() < Duration::from_secs(30) {
                if ctx.cancel().wait(Duration::from_millis(20)) {
                    return Ok(json!({ "cancelled": true }));
                }
            }
            Ok(json!({ "cancelled": false }))
        })
        .run()
}