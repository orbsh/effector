# PLAN

The decisions live in `docs/adr/`, the design in the wiki (`stateless-agent-architecture.md`), the operator-facing facts in `README.md` and `USAGE.md`. This file is the index: what is sequenced, what is done, what is left, and where the reason is written.

**Iron rule — effector never depends on Aura crates.** The one coupling is the frame protocol (`effector-protocol`); any service that speaks the frames can drive a node.

## Milestone A — Runtime

- [x] 0 — Workspace skeleton: `crates/{runtime,protocol,config}`, the task contract — 2026-09 — isolation model superseded by [ADR-0003](adr/0003-environment-model.md)
- [x] 1 — Carriers: `steel`, `python`, `induction`, `exec` — 2026-09, roster corrected 2026-11-09 — `crates/runtime/Cargo.toml`

## Milestone B — Effector

- [x] 3 — Outbound registration: a node dials the plane, holds the connection, tasks ride down it — 2026-09-16 — [ADR-0002](adr/0002-signed-register.md); its signed handshake was replaced by a dial token, [ADR-0003](adr/0003-environment-model.md) §3
- [x] 4 — Operation dispatch: a node receives a `CodeRef`, never a skill — 2026-11-09 — ADR-0027 (aura)
- [x] 4.5 — Host-side storage — WITHDRAWN 2026-09-20 — ADR-0010 (okm)
- [ ] 5 — Scope constraints: the `Host` class's coarse grant — [ADR-0003](adr/0003-environment-model.md) §1 — not blocking
- [ ] 6 — Data-path discipline: the plane carries instructions and result summaries, artifacts stay on the node — [ADR-0003](adr/0003-environment-model.md) §8, gravity ADR-0012 — not blocking
- [ ] 7 — Environment backends — [ADR-0003](adr/0003-environment-model.md)
  - [x] 1 — the type is `Environment` (`Host`, `Jailed`) — 2026-11-16
  - [x] 2 — the idle lease — 2026-11-16
  - [x] 3 — the dial token replaces the signed register — 2026-11-16
  - [x] 4 — the launcher (`effector-launch`, `docker run --rm --init`) — 2026-11-16
  - [x] 5 — the resource contract, all three kinds (`git`, `object`, `archive`) — 2026-11-17, `archive` 2026-11-18
  - [ ] the container backend — until it exists the `disposable` domain has no backend — blocking for a turn's freshly written code
  - [ ] a Kubernetes `Job` backend — later, same contract

## Open items

- [x] A stop reaches a node, and the node says whether it could act — 2026-11-09 — `Frame::Interrupt` / `Frame::Interrupted`, gate `exec_carrier::an_interrupt_releases_the_wait_and_reaches_the_guest`
- [x] `command_exec` retired — 2026-11-09 — [ADR-0001](adr/0001-sandbox-declaration.md) Update
- [x] The jail's fail-closed half — 2026-11-17 — the jail retired with the class boundary, `edf4bf2`, [ADR-0003](adr/0003-environment-model.md) §1

## Deferred

- Aura-embedded form (Wasmtime-heavy isolation) — until the remote node is stable; it changes nothing in the task contract
- Skill packaging and install lifecycle — skills are emergent in the graph, not installed artifacts; revisit only if spec fetch becomes a measurable cold-start cost