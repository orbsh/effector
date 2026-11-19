# 0005 — A job is an envelope, not a second plan

> **Languages:** [English](0005-a-job-is-an-envelope.md) (primary) · [中文](0005-a-job-is-an-envelope.zh-CN.md)

**Status:** Proposed (2026-11-19). Step one of the delivery task: the types and the vocabulary. No behaviour changes under this record.
**Date:** 2026-11-19

## Context

A booth can answer the turn it was given and nothing else. It cannot start work: there is no way for it to say "run these steps", so everything a booth needs executed has to be either compiled into the host (`Body::Rust`) or pre-placed as a fixed node in the dispatch layer. That is where gravity's skill execution lives today, and it is the complexity this record is about.

The knowledge of *how* to run something is also in the wrong layer. `Body::Script { language, source, encoding }` is a spawn spec, it lives in aura's dispatch types, and it is the reason a deployment question ("where does gravity run?") has no answer that does not involve editing dispatch.

Meanwhile the platform's vocabulary already has three words that must not be confused:

| word | what it is | where it lives |
|---|---|---|
| `Plan` | a definition: a graph of steps, kinds, edges | `schematic` (a plan is a graph) |
| `Run` | the log of one plan's execution, append-only | `schematic` (a run is an append log) |
| turn | one request-and-answer of a booth | aura / gravity |

A fourth word is needed for the thing this record is about, and it is not "plan": the plan already means the definition, and a booth submitting the *same* plan twice is a different act from defining it once.

## Decision

**RULING — the unit a booth hands to the platform is a JOB. A job is an envelope: it names what to run (by reference, not by redefinition), what it may touch, and whose state the result becomes.**

```rust
struct Job {
    id: JobId,              // the idempotency key: a replayed submission runs once
    booth: InstanceKey,     // whose state the results become
    scope: Scope,           // Turn | Shared
    source: JobSource,      // what to run
    resources: Vec<ResourceReq>,
    placement: Placement,   // Any | Labels([...])
}

enum JobSource {
    Ops(Vec<Op>),                       // a few calls, no graph
    Plan { plan_hash: String, run: RunId },  // schematic's definition + the run to advance
}

struct Op {
    call: ToolCall,         // executor / entry / language / code: the existing call
    needs: Need,            // ctx services this op is given
    on_error: OnError,      // Fail | Skip | Retry(n) | Never
}

struct ResourceReq {
    name: String,           // how ops refer to it
    kind: ResourceKind,     // Git { url, rev } | S3 { bucket, prefix } | Store { ns }
    mode: Mode,             // Read | Write
}
```

1. **A job references a plan; it never restates one.** `JobSource::Plan` carries `plan_hash` and the run to advance, so schematic stays the single definition of what a plan is, `Step` keeps its meaning (a node in a graph), and `Op` is what a job has instead: one call with its own ctx and failure policy. A job that carries `Ops` is simply a job whose graph is a line.

2. **The job does not decide where or for how long.** Node choice is the realm's (by declared capability, labels and locality); lifetime is the node's `LeaseConfig` (`idle_secs`, `max_idle_secs`, `exit_on_disconnect`); the codec is per leg. A job states `scope` (Turn or Shared) because scope is a fact about the work, and `Turn` is what makes one-shot execution expressible without special-casing it.

2a. **There is no user in the envelope.** `Scope` has no `User` variant and no field names a user. aura has no notion of one, and a job that carried one would force it to acquire one. Who the work is FOR is booth business: it travels inside an op's `args`, where the booth's own code reads it, and the platform never learns it. The same reason makes user identity the wrong basis for placement — see 2b.

2b. **Placement is a label, and a label is the control plane's word.** `Placement::Labels(["u101:laptop"])` asks for a node carrying that label; the node itself cannot claim it. Effector's `Register` frame says what a node IS (alias, carriers, class) and not who it is — identity arrived with the dial, in the token the control plane validated. So labels are minted by prism from the grant and attached to the connection; a node that could name its own labels could name someone else's.

3. **Resources are declared, not created ad hoc.** A job says "the repository at this rev, read-only"; the realm grants, the node materialises. Anything an op touches that is not in `resources` is a defect in the job, not a capability of the executor.

4. **`OnError::Never` exists for a reason.** Retry policy belongs to the job, not to the executor's intuition, and a step with a side effect (a push, a write) must be able to say "do not run this twice". A default of retry-on-failure would be a footgun aimed at exactly the steps that matter most.

5. **A job's frames say which job and which op they belong to.** A booth is already emitting frames for its own turn; frames coming back from a job are a second stream through the same channel, and without ids the booth cannot tell what it is looking at.

## Consequences

- **The realm holds job rows.** A job is data before it is work: it can be inspected, retried, and replayed, which the spawn-spec-in-dispatch arrangement could never be. The row is `(id, attempt, state)` plus the job document — the same shape as the run table k10r already keeps.
- **A booth gains exactly one new verb.** `ctx.plan(job)` (name of the verb still open) submits and returns a stream handle; the results arrive over the iterate surface induction-guest already has (`IterateStart`/`Next`/`Dispose`), so no new streaming concept is introduced.
- **`Body::Script`'s spawn spec moves out of dispatch.** This record does not perform that move; it states that a job is where `language`, `entry` and `code` belong, so the move has a destination.
- **Schematic is a carrier, not a dependency of this type.** `JobSource::Plan` names a plan by hash and a run by id; whether the node runs it via the planned `plan` carrier arm is the node's business.
- **Compiled-in `Body::Rust` becomes a choice with a cost.** A body that ships as a job can change without recompiling the platform; a body compiled in cannot. This record does not forbid the compiled form — it makes it the exception rather than the only way to run something.

## Nesting is allowed, and it is the point

A job may submit another job: that is how gravity runs a skill whose steps are themselves booth
calls without embedding a dispatch layer of its own. Three limits keep it a tree rather than a
storm:

- **Depth is capped** (the number is the realm's policy, not the envelope's). A nested job that
  would exceed it is refused as an error value, not silently dropped.
- **A child's scope may not exceed its parent's.** `Turn` may not give birth to `Shared`: scope
  narrows downward, so a discarded parent cannot leave a durable child behind.
- **Every frame names its path.** `(job, op)` is already required for a job's results; a nested
  job's frames carry the chain, so a booth can tell "my step 3 said this" from "a grandchild said
  this".

## The verb

The act is a DISPATCH: a booth dispatches a job. The noun is a job, because a "task" in this
installation already means the user's work item (a vertex with a sheet, k10r ADR-0021) and a second
meaning for that word would be exactly the collision this vocabulary exists to avoid. A dispatch
carries a job; the job is not a task.

Still open: the exact spelling on the ctx surface (`ctx.dispatch(job)`), and whether a booth may
dispatch from outside a turn at all.

## Related records

ADR-0002 (a booth is a body), ADR-0003 (the environment model — lease and container form), ADR-0004 (the carrier is an interface), gravity ADR-0011 (composition is a graph — schematic's `Plan`/`Run`), gravity ADR-0013 (encoding is a transport axis).