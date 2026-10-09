# Effector

Skill runtime / remote effector of the stateless agent architecture. Design: [stateless-agent-architecture.md](../../.hermes/wiki/stateless-agent-architecture.md) (wiki), Gravity (turn executor), Krystallizer (skill graph).

**Upstream-agnostic by iron rule**: effector never depends on Aura crates — all
coupling with Aura is the frame protocol (serde types in `effector-protocol`).
Any service that speaks the frames can drive it; the Aura Booth deployment
form lives entirely on the Aura side (connection-plane adapter).

Control-plane integration (connect, handshake, every frame, and the obligations
each one carries): [docs/USAGE.md](docs/USAGE.md) · [中文](docs/USAGE.zh-CN.md).

## Model

Execution only — provides a runtime environment, never lives in Krystallizer. Container-isolated; skill code is untrusted.

Two deployment forms, both supported:

- **Aura embedded**: execution base component (heavy-isolation end of the Wasmtime sandbox lineage).
- **Remote effector**: GitHub Actions runner mode — outbound registration + task pull, no inbound ports. Deploy it on your laptop or a target server and it operates that machine.

Skill resolution happens on the Gravity side (Krystallizer → Gravity, live on every tool call, zero cache — emergent skills must have zero staleness window). The Effector never sees a skill: it receives an operation and its arguments, executes, and returns a tool result.

Runtimes: steel / python (PyO3) / wasmtime (in-process, Aura polyglot
ruling) + `bgi` (resident framed line protocol — compiled guests or the
two-fifo nushell adapter) / `exec` (bare one-shot CGI, one JSON in / one
JSON out per call), feature-gated. The nushell PTY carrier retired
(ADR-0035 §6): nu's shapes are `exec` and `bgi`.

Run tests with the carrier combo:
`cargo test -p effector-runtime --features steel,python,wasmtime`.
