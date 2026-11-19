# 0006 — Transport and connection lifetime: local sockets first, UDP only when measured

> **Languages:** [English](0006-transport-and-connection-lifetime.md) (primary) · [中文](0006-transport-and-connection-lifetime.zh-CN.md)

**Status:** Accepted (2026-11-19). It settles what carries frames and who ends a connection; it does not change any carrier that already exists.
**Date:** 2026-11-19

## Context

ADR-0004 makes the carrier an interface. This record says what the implementations are, in which order they matter, and — the part that is easy to get wrong — what ends a connection.

Two facts were measured on the development machine rather than assumed (256 B round trip, 20 000 iterations, Python-bound so the absolute values are inflated while the ratios are what matter):

| carrier | round trip |
|---|---|
| pipe (today's `exec`) | 0.89 µs |
| unix socket | 0.81 µs |
| TCP loopback | 4.00 µs |

A pipe and a unix socket are the same thing in cost. Loopback TCP is five times worse and still two orders of magnitude below anything a person notices. So the interesting question is not "how do we go faster than a pipe" — it is "what do we do when the two ends are not on one machine", where the cost is the network's round trip and not the syscall.

## Decision

**RULING — the carrier choice follows locality, not preference: on one host a unix socket; across hosts, TCP until a measurement says otherwise; datagrams only when a number demands them.**

1. **Same host: unix socket (`uds`).** It matches the pipe's cost, it survives the parent that spawned it (which is what a resident booth needs), it needs no handshake and no TLS argument, and it carries the peer's identity in a way the kernel vouches for (a socket file's permissions). The pipe stays, because a per-turn child genuinely wants to die with its parent and EOF is the cleanest stop signal there is.

2. **Across hosts: TCP, not UDP.** UI frames are snapshots at human rates; the milliseconds TCP costs are invisible, while the failure modes datagrams add (reordering, loss, retransmit policy of our own) are visible as bugs. Slow start is not the problem it sounds like: it costs a few round trips once per connection, and a connection is deliberately long-lived.

3. **Datagrams only on a measured demand, and never to satisfy a principle.** If streaming tokens or high-frequency input ever shows head-of-line blocking across users, the answer is KCP (reliable UDP with tunable retransmission, no TLS) or QUIC (with a self-signed internal CA — QUIC mandates TLS 1.3, but "mandatory TLS" is not "mandatory public certificates"). WebTransport's TLS requirement is a BROWSER constraint: a process that talks to another process is not a browser, and internal plaintext datagrams are legitimate.

4. **One connection per user; the protocol carries no drop policy.** Isolation is what makes a slow consumer harmless — it stalls its own connection. Priorities, coalescing, and "which operations are droppable" are not transport business, and this record rules them out rather than leaving them to be added later.

5. **A single frame is capped (16–32 KB).** The one geometric rule that stays: an unbounded frame blocks its own connection for as long as it takes to transmit, and that is a property of the frame, not of a scheduling policy.

6. **Lifetime is the carrier's, and it is already stated in configuration.** `LeaseConfig { idle_secs, max_idle_secs, exit_on_disconnect }`: a container node leaves when nothing is happening and ends with its container (no pool manager, no reaper); a host node never expires and reconnects instead. A pipe carrier ends on EOF. Nothing else decides this.

7. **TLS is not required between processes.** It is required where a browser is one of the ends (WebSocket, WebTransport), because that is the only channel a browser offers. Between an effector and a realm, the connection is authorized by a token the control plane minted (ADR-0003) and the socket is local or on a trusted network; adding TLS there buys confidentiality against a threat the deployment already excludes, at the cost of an internal PKI nobody asked for.

## Consequences

- **A resident booth is a socket deployment.** "One booth, all partitions, one effector instance" is expressible exactly because the carrier is a socket with a lease, not a child with an inherited pipe.
- **The host-vs-container distinction stays a property of the node, not of the call.** A node says what it is (`Register { class }`); the caller says what it needs (`requires`), and a mismatch is refused by name rather than downgraded.
- **Reconnect is a first-class state for host nodes.** `exit_on_disconnect: false` means the node comes back and re-registers; placement therefore addresses labels rather than a socket, or a returning node would be unreachable.
- **No pool manager will be written.** The container is the lifetime; a reaper would be a second, disagreeing source of truth about when a node should die.
- **A measurement opens the door to datagrams, not an argument.** When KCP or QUIC lands, this record is amended with the numbers that justified it.

## Related records

ADR-0003 (the environment model — token, container form, `LeaseConfig`), ADR-0004 (the carrier is an interface), ADR-0005 (a job is an envelope — the unit that travels), gravity ADR-0013 (encoding is settled per leg, at the handshake).