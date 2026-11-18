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

/// The verb a ToolCall asks the executor to perform (ADR-0034, unified
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
/// The Effector ships no built-in operations and keeps no registry. `executor`
/// is the caller's opaque name for one execution unit and `entry` names the point
/// the delivered code exposes; `code` always carries its executable part.
/// `language` names the carrier to run it in — the node must carry it
/// (registration advertises carried languages) or the call is an error value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    /// Correlates the eventual ToolResult back to the caller (Gravity's
    /// pending_calls deadline scan reuses this id).
    pub call_id: String,
    /// What this call needs from the node that runs it (ADR-0003 §1). **The default is the
    /// CONTAINER**: code nobody reviewed runs separated, and running it on bare metal is the
    /// exception a caller has to ask for — `host` never arrives by omission. A `host` node refuses
    /// a call it cannot contain, BY NAME, rather than run it with the machine's authority.
    #[serde(default = "container_class")]
    pub requires: NodeClass,
    /// What this call asks the executor to do (ADR-0034, unified
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
    /// Opaque name of the execution unit, minted by the caller: the Effector
    /// keys its executor by this value, so calls sharing an `executor` share
    /// the loaded logic and its runtime state (module globals, a child's
    /// memory) and calls with different values never do. The Effector derives
    /// nothing from it beyond identity — an upstream booth's `type/key` is the
    /// natural value. It cannot be the per-call id: the unit must be stable
    /// across calls, or every call would start a cold one.
    pub executor: String,
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
/// executor itself.
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

/// The dial URL for one connection: the mount URL with the node's token attached.
///
/// The token rides the DIAL rather than a frame, because a credential inside a frame is a
/// credential in every log that carries frames. Tokens are minted URL-safe (base64, no
/// padding), so this appends without escaping — the one rule both ends must share.
pub fn dial_url(base: &str, token: &str) -> String {
    // Split the authority from the path first: appending `?token=` to `ws://host:port` would
    // put the query where the path belongs, and a client that parses the URI strictly refuses
    // the result. A base with no path gets `/`.
    let (prefix, rest) = match base.find("://") {
        Some(i) => (&base[..i + 3], &base[i + 3..]),
        None => ("", base),
    };
    let (authority, path_query) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    let (path, query) = match path_query.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (path_query, None),
    };
    let path = if path.is_empty() { "/" } else { path };
    match query {
        Some(q) => format!("{prefix}{authority}{path}?{q}&token={token}"),
        None => format!("{prefix}{authority}{path}?token={token}"),
    }
}

/// What a node IS, on the wire: the boundary that contains the code it runs.
///
/// `Host` — the node is a process on somebody's machine, and the code it runs has that machine's
/// authority. `Container` — the node IS the container's root process (ADR-0003 §10): its filesystem
/// is the container's, and anything it spawns is inside it. The class is a property of the DEPLOYMENT,
/// not of a call, which is why it is declared once at the dial and a call can only REQUIRE one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeClass {
    /// The node is a process on somebody's machine. The DEFAULT for a NODE (that is what an
    /// undeclared node is), and the EXCEPTION for a call.
    #[default]
    Host,
    Container,
}

/// The default a CALL gets: separated, unless it says otherwise. A free function because serde's
/// `default` takes a path, and this is the policy point — one line to read when the question is
/// "what happens when a caller says nothing".
pub fn container_class() -> NodeClass {
    NodeClass::Container
}

impl NodeClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeClass::Host => "host",
            NodeClass::Container => "container",
        }
    }
}

/// Transport envelope over the outbound connection (Phase 3). The control
/// plane sends exactly one frame per WS message; the effector replies likewise.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Frame {
    /// Effector to control plane, once per connection, and the FIRST frame it sends: what
    /// this node IS, not who it is. Identity came with the connection — the dial carried a
    /// token the control plane validated before this socket was served (ADR-0003), so the
    /// frame describes shape: alias, carriers. There is no challenge and no signature: a
    /// proof of identity that has to travel inside the frame is a credential inside the
    /// frame, and the authorization we have is the connection itself.
    Register {
        node_alias: String,
        carriers: Vec<String>,
        /// What contains this node (ADR-0003 §10). Defaults to `host`, which is the safe reading:
        /// a node that does not say it is a container is treated as one that is not.
        #[serde(default)]
        class: NodeClass,
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
    /// Effector to control plane: this call is still making progress.
    ///
    /// Silence is not death. A call whose work is a TRANSFER is quiet for exactly as long as the
    /// bytes take, and the plane's only other evidence of life is the answer — so accounting that
    /// treats quiet as stuck kills a healthy clone the moment it is big enough to matter. The node
    /// therefore states the fact: throttled by the sender, addressed by `call_id`, carrying nothing
    /// (a rate is a measurement, and a measurement that nobody asked for is a second vocabulary).
    Progress {
        call_id: String,
    },
    /// Control plane to effector: stop waiting for this call (aura ADR-0047 stage
    /// two — the realm lifted an in-flight operation). Addressed by `call_id` because
    /// that is the only handle the control plane holds; the node answers with
    /// `Interrupted`, which says whether it could act, not whether the work died.
    Interrupt { call_id: String },
    /// Effector to control plane: what the node did with an `Interrupt`. `landed:
    /// false` is the honest answer from a carrier with no way in (an embedded VM
    /// mid-call) — the operation continues and its result is discarded, which the
    /// control plane must not read as "it stopped".
    Interrupted { call_id: String, landed: bool },
    /// Effector to control plane: the script asked for durable data of one kind (`ctx.resource`)
    /// and the node needs what it takes to reach it. Addressed by (`call_id`, `kind`) — WHICH name
    /// the script wants is the node's business, because it is the node that joins that name onto the
    /// root it is given here.
    MaterialRequest {
        call_id: String,
        kind: String,
        /// Which half of the contract this is: `false` fetches, `true` publishes. The plane answers
        /// differently for each — a grant that is `ro` has no write half at all.
        write: bool,
    },
    /// Control plane to effector: the material for that request. It is minted per transfer and
    /// lives in the node's memory for it — never in a log, never on disk, never in the guest's
    /// reach (ADR-0003 §8).
    Material {
        call_id: String,
        kind: String,
        /// The root this execution may reach, already separated by tenant
        /// (`<base>/<tenant>/<kind>`). The node appends the script's own name to it.
        base: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        credential: Option<String>,
        // No `transport` field: `kind` IS the identity, and the node is the one that knows how a
        // git resource moves. A field that always equalled the kind was two words for one fact,
        // and the day a second transport exists it will be the row's new KIND, not a parallel
        // vocabulary here.
        /// `ro` or `rw`. The plane's word wins over the task's wish — it is the row's decision.
        /// No `ref` field: which branch or version to check out is the SCRIPT's to name, and it
        /// rides the ask; a grant describes a place, not a revision of what is in it.
        mode: String,
    },
    /// Control plane to effector: no material could be minted, and why. A REFUSAL IS A VALUE —
    /// the node turns it into a failed call rather than waiting out a deadline for an answer that
    /// is never coming.
    MaterialFailed {
        call_id: String,
        kind: String,
        reason: String,
    },
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

/// The shapes a resource declaration carries, stated once, here — because these fields are the
/// WIRE contract (`Material.url`, `Material.credential`), and both ends must agree: the plane
/// refuses a declaration at the operator's door with the same rule the node applies when it moves
/// the data.
pub mod resource {
    use anyhow::{bail, Result};

    /// The mechanisms a node can actually move. A plane may serve ONLY these: a grant for a
    /// mechanism nobody implements is a promise the operator would discover at the first task, not
    /// at the door they declared it. `archive` was the case that taught this: it is a word in the
    /// protocol so a node can refuse it BY NAME, and being nameable is not being servable.
    pub const KINDS: [&str; 3] = ["git", "object", "archive"];

    /// Whether `kind` names a mechanism some node implements.
    pub fn is_kind(name: &str) -> bool {
        KINDS.contains(&name)
    }

    /// What an `archive` credential carries: a repository PASSWORD, and — when the repository sits
    /// in a store rather than on a path — the signing triple that reaches it.
    ///
    /// Two shapes, and no guessing between them: `password` for a repository the node opens by
    /// address, `access:secret:region:password` for one it signs into. A password is not optional
    /// (an archive is encrypted at rest, that is what it is for), which is why it is the one field
    /// every shape ends with.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ArchiveCredential<'a> {
        pub s3: Option<(&'a str, &'a str, &'a str)>,
        pub password: &'a str,
    }

    pub fn archive_credential(credential: &str) -> Result<ArchiveCredential<'_>> {
        let parts: Vec<&str> = credential.split(':').collect();
        match parts.as_slice() {
            [password] if !password.is_empty() => Ok(ArchiveCredential {
                s3: None,
                password,
            }),
            [access, secret, region, password]
                if !access.is_empty()
                    && !secret.is_empty()
                    && !region.is_empty()
                    && !password.is_empty() =>
            {
                Ok(ArchiveCredential {
                    s3: Some((access, secret, region)),
                    password,
                })
            }
            _ => bail!(
                "an `archive` credential must be `password` or `access:secret:region:password`"
            ),
        }
    }

    /// The file name an object resource lands under, taken from its URL: `s3://bucket/builds/out.tar`
    /// and `https://host/builds/out.tar` both give `out.tar`. A URL that ends in `/` names a PREFIX,
    /// which is a different kind of resource; inventing a name would put a task's data somewhere it
    /// was never told to look.
    pub fn object_name(url: &str) -> Result<String> {
        if url.ends_with('/') {
            bail!("resource URL `{url}` names no object (it names a prefix)");
        }
        let last = url
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow::anyhow!("resource URL `{url}` names no object"))?;
        Ok(last.to_string())
    }

    /// `access_key:secret_key[:region]` — ONE string, because that is what a secret row holds.
    /// Split once, here, so the door and the node cannot disagree about the shape.
    pub fn credential_parts(credential: &str) -> Result<(&str, &str, &str)> {
        let parts: Vec<&str> = credential.splitn(3, ':').collect();
        match parts.as_slice() {
            [access_key, secret_key] if !access_key.is_empty() && !secret_key.is_empty() => {
                Ok((access_key, secret_key, "us-east-1"))
            }
            [access_key, secret_key, region]
                if !access_key.is_empty() && !secret_key.is_empty() && !region.is_empty() =>
            {
                Ok((access_key, secret_key, region))
            }
            _ => bail!("a credential must be `access_key:secret_key[:region]`, got {credential:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::resource;

    /// The archive credential has two shapes and no third: a repository password, or the signing
    /// triple plus one. Everything else is refused HERE rather than becoming a puzzling rustic
    /// failure at the first transfer.
    #[test]
    fn an_archive_credential_is_a_password_or_a_store_and_a_password() {
        let plain = resource::archive_credential("correct horse").expect("a password");
        assert_eq!(plain.password, "correct horse");
        assert!(plain.s3.is_none());

        let stored = resource::archive_credential("ak:sk:us-east-1:hunter2").expect("a store");
        assert_eq!(stored.s3, Some(("ak", "sk", "us-east-1")));
        assert_eq!(stored.password, "hunter2");

        for bad in ["", "ak:sk", "ak:sk:region", "ak::region:pw", "ak:sk::pw"] {
            assert!(
                resource::archive_credential(bad).is_err(),
                "`{bad}` must be refused"
            );
        }
    }

    /// The kinds a plane may serve are the kinds a node can move: one list, read by both ends.
    #[test]
    fn the_servable_kinds_are_the_protocols_list() {
        assert!(resource::is_kind("git"));
        assert!(resource::is_kind("object"));
        assert!(resource::is_kind("archive"));
        assert!(!resource::is_kind("ftp"));
    }
}
