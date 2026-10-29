# 0002 — The handshake is a signature, not a shared secret

> **Languages:** [English](0002-signed-register.md) (primary) · [中文](0002-signed-register.zh-CN.md)

**Status:** Accepted (2026-10-29). Landed: `Frame::Challenge` / signed `Frame::Register` /
`Frame::Refused`, `effector keygen`, `effector register`, `private_key_path`.
**Date:** 2026-10-29

## Context

Registration was a shared credential: the node read a token from an environment variable and
put it in its `Register` frame, and the control plane compared it to what it held. One value,
both ends, no proof of anything.

That shape fails in three ways, and the third is the one that matters here:

1. **A credential is copyable, so it cannot name a machine.** Anyone holding the token is
   equally the node. Trust was "someone who once knew the string", never "this machine".
2. **An env-var secret leaks by default.** `/proc/<pid>/environ`, crash dumps, a stray
   `env` in a log, a child process inheriting it — the token is readable by everything the
   user runs, and the node is *supposed* to run other people's code.
3. **The control plane's trust table had nothing to key on.** Prism's node registry
   (`prism ADR-0015 §3`) maps an alias to a PUBLIC key and decides `registered | pending |
   conflict`. A shared token cannot be looked up in that table — the registry was built for
   the handshake this record replaces.

The transport was already the right one (one outbound WS, one frame per message). What was
missing was proof, not plumbing.

## Decision

The control plane speaks first, with a nonce. The node signs it with an ed25519 private key
that stays on the node as a file (`private_key_path`, written 0600 by `effector keygen`).
`Register` carries the PUBLIC key and the signature; the host verifies the signature and then
decides trust by looking that public key up in its registry.

Details that are part of the decision, not implementation slack:

- **The host speaks first in every trust posture.** `open` (no registry) and `required`
  (registry consulted) are the SAME wire: a node never has to know how it is being trusted,
  and there is no second frame shape to keep in sync.
- **A signature is verified in both postures.** `open` means "no registry", never "no proof".
  A host that skipped verification would accept a `Register` that names someone else's key.
- **The signed message is one function in this crate** (`register_message`): the protocol
  states the exact bytes, version-tagged, so the signing side and the verifying side cannot
  drift into a formatting difference nobody can debug. Keys and signatures ride base64; the
  nonce is 16 random bytes, base64.
- **The nonce is per connection**, so a captured `Register` cannot be replayed onto another
  one. Verification is `verify_strict` — a malleable signature or a weak key is refused,
  because the registry's public key has to name exactly one signer.
- **A refusal is stated before the socket closes** (`Frame::Refused { reason }`). The
  operator who needs the reason is on the NODE machine; the reason would otherwise exist only
  in the host's log.
- **Registration is a network fact; serving is runtime state.** `effector register <config>`
  is one handshake, one verdict, exit. A restart re-earns trust per connection — there is no
  "registered once" mode to keep in sync with a registry an operator may have changed.
- **The key is a startup fact.** A node without a readable key refuses to run, like a node
  whose capability declaration has no enforcement point (ADR-0001). No generated fallback: a
  key invented at startup is a key the operator cannot have enrolled.
- **`keygen` never overwrites.** `create_new` — a key file that already exists is an error,
  because silently replacing it would orphan the registry row the old key owns.

## Consequences

- The credential field is gone from both ends (`EffectorConfig::credential_env` →
  `private_key_path`; `Frame::Register.credential` → `public_key` + `signature`). Nothing
  reads a secret from the environment any more.
- Prism's `required` posture now has something to decide on: the verdict gates the
  connection, `pending`/`conflict` are refused with a stated reason, and the approval queue is
  the registry table itself (`POST /admin/nodes/{alias}/approve`).
- Aura's local-mode effector gateway (`serve_effectors`) sends the challenge and verifies the
  signature too, with no registry behind it — local mode has no operator to approve anything,
  and inventing one would be a second trust plane.
- Trust still follows ownership: a node is trusted because a human approved its public key for
  that alias. The handshake proves the key; it never grants anything by itself.
- Owed: key rotation is "revoke the old key, approve the new one" — there is no in-band
  rotation frame, and an enrolled key is never silently replaced (that is a registry rule, not
  a transport one).