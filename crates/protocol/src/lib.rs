//! Task contract: tool_call in, tool_result out.
//!
//! The contract is intentionally transport-agnostic: the same frames ride the
//! outbound WS connection, the degraded chunked-delivery channel, or any
//! future carrier. Nothing here knows about serialization beyond serde.
//!
//! The Effector is skill-blind: it executes the operation handed to it and
//! nothing else. Skills live in the Krystallizer graph and are resolved by
//! Gravity / the LLM; what reaches the Effector is only an operation and its
//! arguments.

use serde::{Deserialize, Serialize};

/// The signing type, re-exported so both ends name one type instead of importing the crypto
/// crate (and so a version bump is one line here rather than two manifests that can disagree).
pub use ed25519_dalek::SigningKey;

/// The verb a ToolCall asks the session to perform (ADR-0034, unified
/// envelope ADR-0036 — the Invoke kind retired: every dispatch job is a
/// stream op, invoke is the stream whose first round is terminal,
/// `IterateStart` with a plain handler reply is exactly an invoke).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallKind {
    /// Begin a stream: run the handler as a producer under `stream` id.
    #[default]
    IterateStart,
    /// Pull one envelope `{item, done:false}` / `{done:true, value?}`
    /// from the stream.
    IterateNext,
    /// Abandon the stream (consumer break, ADR-0034 §3). Idempotent.
    IterateDispose,
}

// The frame codec declaration is NOT defined here: the pipe protocol owns its
// vocabulary, so the type is `induction_protocol::Encoding` (ADR-0037 §2's
// dual-protocol ruling lives with it, and it carries the serde spellings a
// control frame stores). Treating the codec as a per-host enum is what
// produced the drift class its consolidation ended.
//
// Frames are TYPED (`{"host": {"type": …}}`, ADR-0037 §2) under BOTH
// encodings — the encoding changes the codec, the typing changes the
// vocabulary; orthogonal axes.

/// One operation arriving at the Effector.
///
/// The Effector ships no built-in operations and keeps no registry. `session`
/// is the caller's opaque residency identity and `entry` names the point the
/// delivered code exposes; `code` always carries its executable part.
/// `language` names the carrier to run it in — the node must carry it
/// (registration advertises carried languages) or the call is an error value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    /// Correlates the eventual ToolResult back to the caller (Gravity's
    /// pending_calls deadline scan reuses this id).
    pub call_id: String,
    /// What this call asks the session to do (ADR-0034, unified
    /// envelope ADR-0036). Every dispatch job is a stream op:
    /// `IterateStart` runs the handler as a producer (a plain handler is
    /// legal there — its bare reply wraps to `{done:true,value}` at the
    /// carrier, which IS an invoke), the other kinds drive the stream.
    /// The envelope is schema, not protocol — the frame vocabulary only
    /// names the verb.
    #[serde(default)]
    pub kind: CallKind,
    /// Stream identity the `Iterate*` kinds correlate by (minted by the
    /// control plane at start; every dispatch frame carries it — ADR-0036
    /// retired the Invoke kind, no job travels without a stream id).
    /// Same correlating role as `call_id` for results — a frame
    /// identity, no new machinery.
    #[serde(default)]
    pub stream: Option<String>,
    /// Opaque residency identity, minted by the caller: the Effector keys its
    /// resident session by this value, so calls sharing a `session` share
    /// runtime state (VM / module globals) and calls with different values
    /// never do. The Effector derives nothing from it beyond identity — an
    /// upstream booth's `type/key` is the natural value. It cannot be the
    /// per-call id: residency needs an identity that is stable across calls,
    /// or every call would start a cold runtime.
    pub session: String,
    /// Entry point inside the delivered code — the function/event name the
    /// code binds, whichever convention it uses. This is NOT a lookup key:
    /// the Effector resolves nothing against it (no registration, no
    /// validation). A name the code does not bind falls back to the
    /// conventional `execute` entry when the code defines one, and is an
    /// error value otherwise.
    pub entry: String,
    /// Carrier to execute in ("steel", "python", "induction", "exec"). The
    /// framed resident process carrier is `induction` (the pipe protocol;
    /// wasmtime retired with PLAN 4.21) — a nushell booth declares
    /// `induction` with a `nu <author.nu>` spawn spec.
    pub language: String,
    /// Frame codec for the process carriers (induction/exec, ADR-0037 §2 —
    /// dual-protocol by declaration; the type is the pipe protocol's own).
    /// `#[serde(default)]` = Json: an older control plane's frame decodes as
    /// today's behavior, the embedded carriers ignore this field entirely (no
    /// channel).
    #[serde(default)]
    pub encoding: induction_protocol::Encoding,
    /// Arguments generated by the LLM — instruction semantics (paths,
    /// options, fragments). AI-generated args are naturally bounded; the
    /// control plane's loose message cap is the only guard, per PLAN
    /// (data-path discipline).
    pub args: serde_json::Value,
    /// The operation's code. Always present: there is no native-operation
    /// path — the Effector provides runtimes, not logic. One delivery form
    /// (ADR-0027): bytes left the control frame — code is content-
    /// addressed, every remote call rides a reference.
    pub code: CodeRef,
}

/// The operation's code by content address (ADR-0027 — replaces the old
/// Inline/Link enum; one option means no option to choose).
///
/// `url` is assembled by the control plane from a deployment-declared
/// prefix plus the hash (the serving endpoint exposes content ONLY under
/// its own `/{hash}` path, so the URL is self-verifying by construction).
/// `sha256` is asserted BY THIS FRAME, never parsed from the URL: a CDN
/// may rewrite the path, and verification derived from a transport detail
/// stands on the wrong side. The effector fetches on demand, verifies
/// against this hash (mismatch = error, never a silent accept), and
/// caches by hash — a discardable hot layer, same legitimacy tier as the
/// resident session itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeRef {
    pub url: String,
    /// Lowercase hex.
    pub sha256: String,
}

/// The Effector's answer, message-sized by discipline.
///
/// Large artifacts never enter the control plane: the operation handles them
/// on the Effector side (local fs, user-configured transfer tooling, transport
/// skills). `Result` stays a summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResult {
    pub call_id: String,
    pub outcome: Result<serde_json::Value, String>,
}

/// The exact bytes a `Register` signature covers.
///
/// ONE function, both ends, because the alternative is a formatting habit on whichever side
/// signed first and a mismatch nobody can debug: the message IS part of the protocol. The
/// version tag is there so a future shape can be told apart from this one by a verifier that
/// supports both, instead of guessing from the length.
pub fn register_message(nonce: &str, node_alias: &str) -> Vec<u8> {
    let mut m = Vec::with_capacity(24 + nonce.len() + node_alias.len());
    m.extend_from_slice(b"effector-register-v1\n");
    m.extend_from_slice(nonce.as_bytes());
    m.push(b'\n');
    m.extend_from_slice(node_alias.as_bytes());
    m
}

/// A fresh challenge nonce: 16 random bytes, base64. The length is not a security parameter
/// (any collision-free value works — it only has to be unguessable for one connection); 16
/// bytes keeps the frame small enough to read in a log.
pub fn nonce() -> String {
    use base64::Engine as _;
    use rand_core::RngCore as _;
    let mut bytes = [0u8; 16];
    rand_core::OsRng.fill_bytes(&mut bytes);
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// base64 the public half of a signing key, in the form `Register.public_key` carries.
pub fn public_key_b64(key: &ed25519_dalek::SigningKey) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes())
}

/// Sign one challenge. The only way a node produces a `Register`.
pub fn sign_register(
    key: &ed25519_dalek::SigningKey,
    nonce: &str,
    node_alias: &str,
) -> String {
    use base64::Engine as _;
    use ed25519_dalek::Signer as _;
    let sig = key.sign(&register_message(nonce, node_alias));
    base64::engine::general_purpose::STANDARD.encode(sig.to_bytes())
}

/// Verify a `Register` against the challenge it answers. `Err` carries the reason for the log
/// (a refusal nobody can explain is indistinguishable from a bug).
///
/// Strict verification: a malleable or weak-keyed signature is refused outright, because
/// accepting one would mean the registry's public key no longer names exactly one signer.
pub fn verify_register(
    public_key: &str,
    nonce: &str,
    node_alias: &str,
    signature: &str,
) -> Result<(), String> {
    use base64::Engine as _;
    let engine = base64::engine::general_purpose::STANDARD;
    let pk: [u8; 32] = engine
        .decode(public_key)
        .map_err(|e| format!("public_key is not base64: {e}"))?
        .try_into()
        .map_err(|v: Vec<u8>| format!("public_key is {} bytes, not 32", v.len()))?;
    let key = ed25519_dalek::VerifyingKey::from_bytes(&pk)
        .map_err(|e| format!("public_key is not a curve point: {e}"))?;
    let sig: [u8; 64] = engine
        .decode(signature)
        .map_err(|e| format!("signature is not base64: {e}"))?
        .try_into()
        .map_err(|v: Vec<u8>| format!("signature is {} bytes, not 64", v.len()))?;
    let sig = ed25519_dalek::Signature::from_bytes(&sig);
    key.verify_strict(&register_message(nonce, node_alias), &sig)
        .map_err(|e| format!("signature does not verify for this nonce and alias: {e}"))
}

/// A fresh keypair, written where the config's `private_key_path` points. Returns the public
/// key as `Register` would state it.
///
/// The file is created 0600 (`create_new`, so an existing key is never silently replaced) and
/// holds the 32-byte seed, base64. Nothing else — a key file is a key file.
pub fn write_keypair(path: &std::path::Path) -> std::io::Result<String> {
    use base64::Engine as _;
    use std::io::Write as _;
    use rand_core::RngCore as _;
    let mut seed = [0u8; 32];
    rand_core::OsRng.fill_bytes(&mut seed);
    let body = base64::engine::general_purpose::STANDARD.encode(seed);
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.mode(0o600);
    }
    let mut file = opts.open(path)?;
    writeln!(file, "{body}")?;
    Ok(public_key_b64(&ed25519_dalek::SigningKey::from_bytes(&seed)))
}

/// Load the key a node signs with. A missing or malformed file is the caller's error to state:
/// the node cannot register, which is not a condition to paper over with a generated key.
pub fn read_signing_key(path: &std::path::Path) -> anyhow::Result<ed25519_dalek::SigningKey> {
    use base64::Engine as _;
    let raw = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("private key {}: {e}", path.display()))?;
    let seed: [u8; 32] = base64::engine::general_purpose::STANDARD
        .decode(raw.trim())
        .map_err(|e| anyhow::anyhow!("private key {} is not base64: {e}", path.display()))?
        .try_into()
        .map_err(|v: Vec<u8>| {
            anyhow::anyhow!("private key {} is {} bytes, not 32", path.display(), v.len())
        })?;
    Ok(ed25519_dalek::SigningKey::from_bytes(&seed))
}

/// Transport envelope over the outbound connection (Phase 3). The control
/// plane sends exactly one frame per WS message; the effector replies likewise.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Frame {
    /// Control plane to effector, ALWAYS the first frame on a mount (either trust posture —
    /// one wire shape, so a node never has to know how it is being trusted). The nonce is
    /// fresh per connection: a captured `Register` cannot be replayed onto another one.
    Challenge { nonce: String },
    /// Effector to control plane, once per connection: the answer to the `Challenge`. The
    /// signature proves possession of the key named by the alias; the control plane decides
    /// trust by looking the PUBLIC key up in its registry (effector ADR-0002, prism ADR-0015 §3).
    Register {
        node_alias: String,
        /// base64 (STANDARD) of the 32-byte ed25519 public key.
        public_key: String,
        /// base64 (STANDARD) of the 64-byte signature over `register_message(nonce, node_alias)`.
        signature: String,
        carriers: Vec<String>,
    },
    /// Control plane to effector: a `Register` was accepted.
    Registered,
    /// Control plane to effector: the claim was refused, and this is why. Sent before the
    /// socket closes, because the alternative is an operator on a node machine staring at a
    /// dropped connection while the reason sits in the OTHER machine's log.
    Refused { reason: String },
    /// Control plane to effector: one operation.
    Call(ToolCall),
    /// Effector to control plane: the operation's answer.
    Result(ToolResult),
    /// Bidirectional: a host-function round trip (ctx bridge over the
    /// wire). Effector -> control plane = Call, control plane -> effector =
    /// Result; the `host_call_id` correlates. The control plane resolves
    /// the call against the BOOTH INSTANCE that the enclosing ToolCall was
    /// routed to (the gateway tracks call_id -> instance; ctx state is
    /// scoped to that instance's own fields).
    Host(HostFrame),
}

/// One host-function round trip (ctx over the wire).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HostFrame {
    Call(HostCall),
    Result(HostResult),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostCall {
    pub host_call_id: String,
    /// The enclosing ToolCall's id — the control plane resolves the ctx
    /// scope (booth instance) from it.
    pub call_id: String,
    pub op: HostOp,
}

/// The ctx surface scripts can reach, carried op-by-op. `invoke` and the
/// iterate verbs ride the control plane's unified call model;
/// `store_emit` carries one okm Collection instruction as data — the
/// wire moves facts for the receiver to execute, it never parses them
/// (the schema lives with the type registration).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum HostOp {
    Invoke {
        target_type: String,
        target_key: String,
        handler: String,
        args: serde_json::Value,
    },
    /// ADR-0034 consumer leg over the wire: a remote script booth starts,
    /// pulls, and disposes a stream produced by another booth (resident
    /// or remote — the envelope is the same on both sides). `stream_id`
    /// is minted by the control plane at start and echoes back here.
    Iterate {
        target_type: String,
        target_key: String,
        handler: String,
        args: serde_json::Value,
    },
    IterateNext {
        stream_id: String,
    },
    IterateDispose {
        stream_id: String,
    },
    /// One okm Collection instruction (ADR-0026 §3's op document —
    /// `{collection, op, …}` as the script wrote it), carried as DATA.
    /// The effector never parses it: the schema lives with the type
    /// registration on the control plane, the wire's job is to move the
    /// instruction and deliver the answer. (Phase 4.14 gate 1.)
    ///
    /// The payload field is `instruction`, not `op` — the enum's
    /// internally-tagged discriminator is already `op`, and a same-named
    /// field would collide on the wire.
    StoreEmit {
        instruction: serde_json::Value,
    },
    /// `ctx.emit` over the wire (ADR-0043 §2's publish, remote shape):
    /// event name + data, resolved by the control plane's realm. The
    /// fire-and-forget semantics are the METHOD's contract — this hop
    /// still request/response at the frame layer (the reply pairing is
    /// protocol, not business; the answer is Null on success).
    Emit {
        event: String,
        data: serde_json::Value,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostResult {
    pub host_call_id: String,
    pub outcome: Result<serde_json::Value, String>,
}
