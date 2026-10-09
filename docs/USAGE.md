# Effector — control-plane integration guide

> **Languages:** [English](USAGE.md) (primary) · [中文](USAGE.zh-CN.md)

Everything a control plane must implement to drive a Effector: how the connection is
established, which frames are exchanged, and which obligations come with each of
them. The authoritative sources are the frame types in `crates/protocol` and the
wrapper behaviour in `crates/runtime/src/remote.rs`; a working control-plane end is
Aura's effector gateway (`aura/crates/engine/src/effectors.rs`), and the acceptance tests
under `crates/runtime/tests/` are fake control planes driving the frames one by one.

## 1. Roles

The Effector is a **client**: it dials out, registers, then serves what arrives. It
opens no port and initiates nothing of its own — the only URL it ever fetches is a
`link` payload's declared URL.

The Effector is also **skill-blind**: it never sees a skill, an operation name, or an
LLM prompt. What arrives is `session` (residency identity), `entry` (the entry
point inside the code), `language` (which runtime), `args`, and `code` (the
executable). Resolving a skill into that set is control-plane work — and none of
the fields carries capability semantics: the Effector derives nothing from `session`
or `entry` beyond an identity and an entry-point name.

The Effector owns runtimes, resident sessions, and the sandbox. The control plane owns identity, scheduling, deadlines, ctx scope, and
policy (whether an operation is AI-generated live or pre-audited).

## 2. Connection and startup

Order of operations at effector startup:

1. Read the config file — `argv[1]`, default `effector.json`.
2. Read the credential from the environment variable named by `credential_env`.
   A missing variable is a **startup failure**: the process never dials.
3. Dial `control_plane_url` and register. Then reconnect forever on drops (§6).

```json
{
  "control_plane_url": "ws://127.0.0.1:8787",
  "sandbox": true,
  "credential_env": "PROBE_CREDENTIAL",
  "capabilities": {
    "node_alias": "home-pc",
    "carriers": ["steel", "python", "bgi"],
    "fs_scope": ["/home/user/work"],
    "command_exec": false,
    "network": "none"
  }
}
```

- `control_plane_url` — WebSocket only. There is no degraded fallback by design
  (long-polling was rejected; a WS-only egress is the contract).
- `sandbox` — defaults to `true`; wraps session processes in bubblewrap. Remote
  deployments must keep it on (unattended external code).
- `credential_env` — the registration credential is the **user** credential; it
  never lives in the config file.
- `capabilities.node_alias` — **the alias must be set.** The default is the empty
  string, and a effector registered as `""` cannot be addressed by name (the failure
  shows up on the control-plane side as "effector not connected").
- `capabilities.network` — `"none"`, `{ "allow": ["host:port"] }`, or `"open"`.

Connection shape: one WS message carries exactly one frame, as JSON text tagged by
`"type"`. The effector's reader and writer are separate tasks, so the connection is
**full-duplex** — frames flow in both directions while a call executes (§4.2
depends on this). Non-text WS messages are ignored.

## 3. Handshake

The first frame the effector sends is `register`; the control plane must answer
`registered`. The effector waits for that answer before serving anything.

```json
{ "type": "register", "node_alias": "home-pc", "credential": "tok-abc",
  "carriers": ["steel", "python", "bgi"] }
```

```json
{ "type": "registered" }
```

- `node_alias` keys the connection on the control-plane side. A reconnection is a
  re-registration: the control plane must replace the previous connection for that
  alias (the old one is already dead).
- `credential` is validated here and mapped to the user's namespace. The capability
  list is what the control plane writes into that user's namespace registry.

## 4. Frames

| Direction | `type` | Purpose |
|---|---|---|
| effector → CP | `register` | presence + capability handshake (§3) |
| CP → effector | `registered` | handshake accepted |
| CP → effector | `call` | one operation |
| effector → CP | `result` | that operation's answer |
| effector → CP | `host` (`kind: "call"`) | a `ctx_*` host call the running script made |
| CP → effector | `host` (`kind: "result"`) | the answer to that host call |

### 4.1 `call` / `result`

`session` is the caller's opaque **residency identity**: the Effector keys its
resident runtime by it (`effector/<node_alias>/<session>`), so calls sharing a
`session` share runtime state (VM / module globals) and calls with different
values never do. It must be stable across calls — an upstream booth's `type/key`
is the natural value. The per-call `call_id` cannot serve here: it is unique per
call, so residency keyed by it would start a cold runtime every time.

`entry` names the entry point inside the delivered code. It is **not** a lookup
key: the Effector keeps no registry and resolves nothing against its value. A name
the code does not bind is an error value, never a magic redirect (one
execution shape per language, the dispatch-table ruling: languages with a
runtime name lookup ride it — python/steel dict/getattr, wasm export table;
languages without one hand-write a `main` + match on literal names).

`kind`/`stream` carry the unified envelope's verbs (ADR-0036): every dispatch
call is a stream op — `kind: "iterate_start"` (the default) with a minted
`stream`, and an invoke is the stream whose first reply is terminal. The
result's `Ok` payload is therefore an envelope (`{done: true, value: <ret>}`
for a plain handler, whose bare return the carrier wrapped).

`language` must be a carrier this node actually carries; an unknown or unbuilt
language is an error value in `result`, not a dropped call.

```json
{
  "type": "call",
  "call_id": "rp-7",
  "kind": "iterate_start",
  "stream": "stream-12",
  "session": "notes/7",
  "entry": "read_file",
  "language": "python",
  "args": { "path": "/tmp/x" },
  "code": { "type": "inline", "bytes": [100, 101, 102, 32, 101, 120, 101, 99, 117, 116, 101, 40, 97, 114, 103, 115, 41, 58, 10, 32, 32, 32, 32, 114, 101, 116, 117, 114, 110, 32, 97, 114, 103, 115] }
}
```

```json
{
  "type": "call",
  "call_id": "rp-8",
  "kind": "iterate_start",
  "stream": "stream-13",
  "session": "heavy/1",
  "entry": "main",
  "language": "wasmtime",
  "args": {},
  "code": { "type": "link", "url": "https://cdn.example/op.wasm",
            "version": "sha256:abc123", "expected_sha256": "abc123" }
}
```

`code` has exactly two forms:

- `inline` — `bytes` in the frame (KB-scale scripts).
- `link` — the Effector GETs `url` (30s timeout), verifies sha256 against
  `expected_sha256` **before** execution, and treats a mismatch as an error value
  (never a silent accept). The URL is a CDN cache key, not a Effector-side cache: the
  Effector holds nothing between calls.

```json
{ "type": "result", "call_id": "rp-7", "outcome": { "Ok": { "done": true, "value": { "ok": true } } } }
{ "type": "result", "call_id": "rp-7", "outcome": { "Err": "unknown language 'koto'" } }
```

Every `call` gets exactly one `result`, success or failure. The success
payload is the unified envelope (ADR-0036 §1): a plain handler answers
`{done: true, value}` — the carrier wrapped its bare return; a stream that
started non-terminal answers `{done: false, item, stream_id}` on the Start
round. Results are message-sized by discipline: large artifacts never travel
the control plane — the operation handles them on the Effector side.

### 4.2 `host` — the ctx bridge

A running script can call back into the control plane. **This is how a delivered
script persists anything durable**: the control plane's store is the durable truth
and the effector keeps nothing between calls (its resident session is a discardable
hot cache). State externalization is the architecture's founding shape, not an
escape hatch — and it is the reason the effector holds no storage and no data-plane
credentials of its own.

The mapping from script function to wire op:

| script fn | `op.op` | payload |
|---|---|---|
| `ctx_invoke` | `invoke` | `target_type`, `target_key`, `handler`, `args` |
| `ctx_iter_start` | `iterate` | `target_type`, `target_key`, `handler`, `args` |
| `ctx_iter_next` | `iterate_next` | `stream_id` |
| `ctx_iter_dispose` | `iterate_dispose` | `stream_id` |
| `ctx_store_emit` | `store_emit` | `instruction` — one okm Collection instruction, carried as data (`{"collection": …, "op": …, …}` as the script wrote it; the control plane executes it against the type's own storage plan) |

```json
{
  "type": "host", "kind": "call", "host_call_id": "h-1", "call_id": "rp-7",
  "op": { "op": "store_emit", "instruction": {
      "collection": "counters", "doc": {"visits": 1}, "key": {"id": 1},
      "op": "put_document" } }
}
```

```json
{
  "type": "host", "kind": "result", "host_call_id": "h-1",
  "outcome": { "Ok": { "ok": true } }
}
```

- `call_id` is the **enclosing** `call`'s id — that is how the control plane
  resolves the ctx scope (which booth instance's fields are being read/written).
  The Effector never needs to know its own instance key.
- `host_call_id` is minted by the Effector, unique per host call. The response must
  echo it; a response with an unknown id is silently dropped.
- **The script's host call blocks until the reply arrives.** There is no deadline
  on this side: an unanswered `host` call wedges that script call until the
  connection drops. Answer every `host` call.
- Host calls arrive **while** the enclosing `call` is still running — a
  half-duplex "read a call, answer it, read the next" server cannot carry this.
- `ctx_store_emit` is persistence over the wire: the effector never parses the
  instruction (the schema lives with the type registration on the control
  plane); a type with no declared storage answers an error string.

## 5. Obligations checklist

1. Answer `register` with `registered` before sending anything else.
2. Send only frames in the table above, with every field present. The effector's
   reader **tears the connection down** on an unrecognized frame or one that does
   not parse (missing or mistyped field), then reconnects — treat both as protocol
   errors, not as forward compatibility.
3. Mint a unique `call_id` per request and correlate replies yourself.
4. Answer every `host` call - the script is blocked on it.
5. Own call deadlines. The effector has none for calls; results are only delivered on
   the live connection, so an in-flight call dies with it and the control plane's
   timeout is the only thing that ends it.
6. Treat a reconnection as a re-registration: replace the alias's connection, and
   expect to re-drive any state the effector does not hold (it holds nothing between
   calls).

## 6. Reconnect and lifecycle

- On any drop the effector reconnects with exponential backoff: 1s, 2s, 4s … capped at
  60s. **The backoff never resets** — not even after an hour of healthy running, so
  a long-lived connection that later drops reconnects at the current (up to 60s)
  delay.
- A **clean** close by the control plane does not reconnect: the effector treats it as
  fatal (`control plane closed the connection cleanly`) and exits the process. Use
  it only when you want that effector gone from that machine.
- Resident session state is process memory; it survives reconnects but not an
  eviction or a process exit. Durable truth lives in the control plane's store.

## 7. Transcript

A complete session, one frame per line, as it appears on the wire (pretty-printed
above; single-line here for the flow):

```
effector → CP   {"type":"register","node_alias":"home-pc","credential":"tok-abc","carriers":["steel","python"]}
CP → effector   {"type":"registered"}
CP → effector   {"type":"call","call_id":"rp-7","kind":"iterate_start","stream":"stream-9","session":"counter/k1","entry":"counter","language":"steel","args":{"n":4},"code":{"type":"inline","bytes":[40,100,101,102,105,110,101,32,40,99,111,117,110,116,101,114,32,97,114,103,115,41,32,40,104,97,115,104,32,34,110,34,32,52,41,41]}}
effector → CP   {"type":"host","kind":"call","host_call_id":"h-1","call_id":"rp-7","op":{"op":"store_emit","instruction":{"collection":"counters","doc":{"visits":1},"key":{"id":1},"op":"put_document"}}}
CP → effector   {"type":"host","kind":"result","host_call_id":"h-1","outcome":{"Ok":{"ok":true}}}
effector → CP   {"type":"result","call_id":"rp-7","outcome":{"Ok":{"done":true,"value":{"n":4}}}}
```

(`bytes` is a byte array: the effector's code payload here is the steel source
`(define (counter args) (hash "n" 4))`. The assembly is illustrative — a script
that emits one storage instruction and then returns is just one of the shapes a
call can take.)

## 8. Failure semantics

| Input | Effector behaviour |
|---|---|
| unknown `language` / carrier not built | `result` with an `Err` outcome |
| `link` hash mismatch, fetch timeout (30s), non-UTF-8 code | `result` with an `Err` outcome |
| unrecognized frame type, or a frame that does not parse | connection torn down, then reconnect |
| non-text WS message | ignored |
| `credential_env` variable missing at startup | process exits before dialing |
| `host` result with an unknown `host_call_id` | dropped (the caller already gave up) |

## 9. Evolution

New frame types and fields append to this document as sections; a change to an
existing frame's meaning belongs in `docs/PLAN.md` with the phase that lands it, and
in the protocol crate's doc comments, which are the machine-checked half of this
contract.