//! A job: the unit a booth hands to the platform (ADR-0005).
//!
//! A job is an ENVELOPE, not a second plan. `schematic` owns what a plan is — a graph of steps —
//! and this module never restates that: `JobSource::Plan` names a definition by hash and the run to
//! advance, while `JobSource::Ops` is the degenerate case where the graph is a line. What a job adds
//! is everything the graph cannot say: whose state the results become, how long the work is meant to
//! live, what it is allowed to touch, and how it fails.
//!
//! What a job deliberately does NOT say:
//!
//! - WHICH node runs it. Placement states the place the work must be in — a set of labels, or one
//!   machine id (aura ADR-0053/0054) — and the realm resolves that against the nodes it holds. The
//!   node never chooses itself, and a job that says nothing gets whatever can carry it.
//! - HOW LONG the node lives. That is the node's own `LeaseConfig`; `scope` states a fact about the
//!   work, not a timeout.
//! - WHICH codec it travels in. That is settled per leg at the handshake.
//!
//! The pieces it is built from already existed: `ToolCall` is one operation as the platform has
//! always dispatched it, and a job is a set of those with the envelope around them.

use serde::{Deserialize, Serialize};

use crate::ToolCall;

/// One job, as submitted. `id` is the idempotency key: a replayed submission must not run twice,
/// which is what makes an at-least-once delivery channel safe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// The idempotency key, minted by the booth (or by whatever retries on its behalf).
    pub id: String,
    /// Whose state the results become — the instance key of the submitting booth
    /// (`<booth_type>/<key>`, the same spelling an instance already carries).
    pub booth: String,
    /// How long the work is meant to live. Absent means `Turn`: the smallest scope, and the one
    /// that needs no user identity to be expressible.
    #[serde(default)]
    pub scope: Scope,
    /// What to run.
    pub source: JobSource,
    /// What it may touch. An op reaching for anything not declared here is a defect in the job,
    /// not a capability of the executor.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<ResourceReq>,
    /// WHERE the node must be, if anywhere in particular. The labels are the control plane's
    /// vocabulary, not the node's: a node cannot claim `u101:laptop` for itself — the label comes
    /// from the grant the control plane validated when the node dialled (ADR-0003). A job states
    /// requirements; placement picks the node.
    #[serde(default)]
    pub placement: Placement,
    /// Things that travel WITH the job but are not the job's meaning: opaque to this crate, named and
    /// typed so the layer that filled them can read them back. A job that runs a type defined at
    /// submission time (a skill taken out of a store, a graph's nodes) cannot name it in `ops` alone —
    /// the definition has to arrive, and it has to arrive in the envelope or a remote submission has
    /// nowhere to put it. This crate never decodes one: `name` and `media` are the filler's
    /// vocabulary, and a realm refuses an attachment it does not recognise rather than ignoring it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<Attachment>,
}

/// One thing carried beside the work, at the filler's naming (see `Job::attachments`).
///
/// Three rules keep this from becoming an untyped side channel across a boundary that only frames
/// are supposed to cross: the DISPATCHER fills them (a booth cannot hand a realm definitions that
/// were not part of its own submission), the total is capped by the receiver, and an unrecognised
/// `name`/`media` is a refusal, never a silent skip — a definition that failed to arrive must not
/// leave a job looking healthy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attachment {
    /// The filler's own name for it (`boothset`, say). Meaningless here, deliberate there.
    pub name: String,
    /// Media type of `bytes` (`application/cbor`, `application/json`). Bytes, not text: a CBOR
    /// envelope carries them as a byte string, so an attachment never pays base64.
    pub media: String,
    /// The payload, uninterpreted.
    #[serde(with = "crate::bytes_serde")]
    pub bytes: Vec<u8>,
}

/// Where a job may run. Absent means "anywhere that can carry it", which is the only thing a job
/// that has no opinion should have to say.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Placement {
    /// Any node whose carriers cover the ops. The realm picks by locality and load.
    #[default]
    Any,
    /// A node carrying every one of these labels (`u101:laptop`, `site:home`, …). Labels are
    /// minted by the control plane from the node's grant; an empty list means `Any`.
    Labels { labels: Vec<String> },
    /// ONE machine, by the id it reports (effector ADR-0006) — what addressing uses once the
    /// caller has resolved a name to a box: a dispatcher retrying the step that ran on a
    /// particular piece of hardware, or one that keeps its own table of which machine holds
    /// which file. A label is a word a person says; a machine id is the thing the word meant.
    ///
    /// Every process of that machine is reachable (two nodes on one box are one address). An
    /// empty id addresses nothing — it is refused, never widened to `Any`.
    Machine { machine: String },
}

/// The lifetime a job claims. This is not a timeout (the node's lease owns that) — it says which
/// state the work is allowed to become part of.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    /// Ends with the turn that submitted it. The degenerate, safest case.
    #[default]
    Turn,
    /// Shared: survives every session, and the results are the installation's.
    ///
    /// There is deliberately no `User` variant: aura has no notion of a user, and an envelope that
    /// carried one would force it to. Who the work is FOR is booth business — it travels in the
    /// op's `args`, where the booth's own code reads it, and the platform never learns it.
    Shared,
}

/// What a job runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum JobSource {
    /// A few calls, in order. The graph is a line, and no plan document exists to reference.
    Ops { ops: Vec<Op> },
    /// A definition that lives elsewhere: the hash of the plan as submitted, and the run to
    /// advance. `schematic` keeps its own authority over what those mean.
    Plan { plan_hash: String, run: String },
}

/// One call plus the two things a raw `ToolCall` cannot say about itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Op {
    /// The operation, in the shape the platform already dispatches.
    pub call: ToolCall,
    /// The ctx services this op is given. A capability it did not ask for is not granted; asking
    /// for one the node cannot offer is refused before the op runs.
    #[serde(default)]
    pub needs: Need,
    /// What to do when the op fails. The default runs nothing again.
    #[serde(default)]
    pub on_error: OnError,
}

/// The ctx services an op asks for, named rather than stringly-typed so a typo cannot silently
/// become "not requested".
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Need {
    /// The booth's store namespace (read/write as granted).
    #[serde(default)]
    pub store: bool,
    /// The right to emit frames back to the booth.
    #[serde(default)]
    pub emit: bool,
    /// Handles for the resources this job declared.
    #[serde(default)]
    pub resource: bool,
    /// Timers (a step that wakes later rather than blocks the node).
    #[serde(default)]
    pub timer: bool,
}

/// Failure policy, stated by the job rather than guessed by the executor.
///
/// `Never` exists for a reason: a step with a side effect (a push, a write, a message that leaves)
/// must be able to say "do not run this twice", and a default of retry-on-failure would aim that
/// footgun at exactly the steps where it hurts most.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum OnError {
    /// One attempt. Failure is recorded as the op's answer and the job's own policy (if any)
    /// decides what happens next.
    #[default]
    Fail,
    /// Record the failure and carry on with the next op.
    Skip,
    /// Try again, at most `times` more times, then fail.
    Retry {
        /// Additional attempts, not total attempts: `times: 2` means at most three runs.
        times: u32,
    },
    /// One attempt, and the failure is not retryable by anything: the frame that carried this op
    /// may be redelivered, and this op must not run because of that.
    Never,
}

/// What a job may touch, declared by name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceReq {
    /// How an op refers to it.
    pub name: String,
    pub kind: ResourceKind,
    #[serde(default)]
    pub mode: Mode,
}

/// The kinds of resource a job can ask for. Deliberately closed: a new kind is a decision about
/// what the platform will create, not a string an executor may interpret.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResourceKind {
    /// A repository at a revision. The rev is pinned: a job that runs twice must see the same tree.
    Git { url: String, rev: String },
    /// A bucket prefix.
    S3 { bucket: String, prefix: String },
    /// A store namespace, for the op's own data rather than the booth's.
    Store { ns: String },
}

/// Read or write. Absent means read, because the cheapest thing to grant is the one that cannot
/// change anything.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    #[default]
    Read,
    Write,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CodeRef;

    fn call(executor: &str) -> ToolCall {
        ToolCall {
            call_id: "call-1".to_string(),
            requires: crate::NodeClass::Container,
            kind: crate::CallKind::IterateStart,
            stream: Some("stream-1".to_string()),
            executor: executor.to_string(),
            entry: "execute".to_string(),
            language: "python".to_string(),
            encoding: induction_protocol::Encoding::Json,
            args: serde_json::json!({ "a": 1 }),
            code: CodeRef {
                url: "https://cdn.example/abc".to_string(),
                sha256: "ab".repeat(32),
            },
        }
    }

    #[test]
    fn an_ops_job_round_trips_and_defaults_to_the_turn_scope() {
        let job = Job {
            id: "job-1".to_string(),
            booth: "gravity/model-1".to_string(),
            scope: Scope::default(),
            source: JobSource::Ops {
                ops: vec![Op {
                    call: call("gravity/model-1"),
                    needs: Need {
                        store: true,
                        ..Need::default()
                    },
                    on_error: OnError::default(),
                }],
            },
            placement: Placement::Any,
            attachments: Vec::new(),
            resources: vec![ResourceReq {
                name: "repo".to_string(),
                kind: ResourceKind::Git {
                    url: "https://example/repo".to_string(),
                    rev: "main".to_string(),
                },
                mode: Mode::default(),
            }],
        };

        let text = serde_json::to_string(&job).expect("serializes");
        let back: Job = serde_json::from_str(&text).expect("deserializes");
        assert_eq!(
            serde_json::to_value(&back).expect("value"),
            serde_json::to_value(&job).expect("value"),
            "a job survives its own wire form"
        );

        // Absent fields are the safe ones, not the loud ones: a job written before `scope` existed
        // runs for one turn, and one written before `on_error` existed runs nothing twice.
        let lean = serde_json::json!({
            "id": "job-2",
            "booth": "gravity/model-1",
            "source": { "kind": "ops", "ops": [] }
        });
        let parsed: Job = serde_json::from_value(lean).expect("defaults apply");
        assert_eq!(parsed.scope, Scope::Turn);
        assert!(parsed.resources.is_empty());
    }

    #[test]
    fn a_plan_job_names_the_definition_and_never_restates_it() {
        let job = Job {
            id: "job-3".to_string(),
            booth: "gravity/model-1".to_string(),
            scope: Scope::Shared,
            source: JobSource::Plan {
                plan_hash: "deadbeef".to_string(),
                run: "run-7".to_string(),
            },
            resources: Vec::new(),
            placement: Placement::Labels {
                labels: vec!["u101:laptop".to_string()],
            },
            attachments: Vec::new(),
        };
        let text = serde_json::to_string(&job).expect("serializes");
        assert!(
            text.contains("\"plan_hash\":\"deadbeef\"") && text.contains("\"run\":\"run-7\""),
            "the reference is the whole of what a plan job says: {text}"
        );
        let back: Job = serde_json::from_str(&text).expect("deserializes");
        assert_eq!(
            serde_json::to_value(&back).expect("value"),
            serde_json::to_value(&job).expect("value")
        );
    }

    #[test]
    fn retry_states_its_own_count_and_never_is_not_a_count() {
        let retry: OnError = serde_json::from_str(r#"{"kind":"retry","times":2}"#).expect("parses");
        assert_eq!(retry, OnError::Retry { times: 2 });
        let never: OnError = serde_json::from_str(r#"{"kind":"never"}"#).expect("parses");
        assert_eq!(never, OnError::Never);
        assert_eq!(
            OnError::default(),
            OnError::Fail,
            "silence means one attempt"
        );
    }

    // --- attachments: a carrying slot, not a type (ADR-0005 §attachments) -------------------

    fn with_attachment() -> Job {
        Job {
            id: "job-1".to_string(),
            booth: "gravity/turn-7".to_string(),
            scope: Scope::Turn,
            source: JobSource::Ops {
                ops: vec![Op {
                    call: call("runner/execute"),
                    needs: Need::default(),
                    on_error: OnError::default(),
                }],
            },
            resources: Vec::new(),
            placement: Placement::Any,
            attachments: vec![Attachment {
                name: "boothset".to_string(),
                media: "application/cbor".to_string(),
                // Not valid UTF-8 on purpose: a definition is bytes, and a codec that assumed
                // text would corrupt it rather than fail.
                bytes: vec![0xa1, 0x00, 0xff, 0x02],
            }],
        }
    }

    #[test]
    fn an_attachment_survives_both_codecs_unchanged() {
        let job = with_attachment();
        let text = serde_json::to_string(&job).expect("json");
        let back: Job = serde_json::from_str(&text).expect("json back");
        assert_eq!(
            back.attachments, job.attachments,
            "the payload is bytes in and bytes out, under JSON too"
        );

        let mut encoded = Vec::new();
        ciborium::ser::into_writer(&job, &mut encoded).expect("cbor");
        let back: Job = ciborium::de::from_reader(encoded.as_slice()).expect("cbor back");
        assert_eq!(back.attachments, job.attachments, "CBOR carries it too");
    }

    #[test]
    fn a_byte_payload_is_a_byte_string_in_cbor_and_an_array_in_json() {
        // The two codecs' realities: CBOR has a byte string, JSON does not. `bytes_serde` writes
        // `serialize_bytes` and accepts either form on the way back, which is why a leg that changed
        // codec mid-flight still lands.
        let payload = vec![0xde, 0xad, 0xbe, 0xef];

        let mut encoded = Vec::new();
        ciborium::ser::into_writer(&Payload(payload.clone()), &mut encoded).expect("cbor");
        let value: ciborium::value::Value =
            ciborium::de::from_reader(encoded.as_slice()).expect("cbor value");
        // A newtype is transparent on the wire, so the encoded value IS the payload: a byte
        // string, one header plus four bytes.
        assert_eq!(
            value,
            ciborium::value::Value::Bytes(payload.clone()),
            "four bytes must cost four bytes, not four integers"
        );

        let text = serde_json::to_string(&Payload(payload.clone())).expect("json");
        assert!(
            text.contains("222,173,190,239"),
            "JSON's only spelling is a number array: {text}"
        );
        let back: Payload = serde_json::from_str(&text).expect("json back");
        assert_eq!(back.0, payload);
    }

    #[test]
    fn a_job_without_attachments_says_nothing_and_still_decodes() {
        let job = Job {
            id: "job-2".to_string(),
            booth: "gravity/turn-8".to_string(),
            scope: Scope::Turn,
            source: JobSource::Ops {
                ops: vec![Op {
                    call: call("runner/execute"),
                    needs: Need::default(),
                    on_error: OnError::default(),
                }],
            },
            resources: Vec::new(),
            placement: Placement::Any,
            attachments: Vec::new(),
        };
        let text = serde_json::to_string(&job).expect("json");
        assert!(
            !text.contains("attachments"),
            "an empty attachment list is not written: {text}"
        );
        // The other direction matters more: a frame minted before this field existed must still
        // decode, which is what `#[serde(default)]` buys and why the field is additive.
        let older = text.replace("\"placement\":\"any\"", "\"placement\":\"any\"");
        let back: Job = serde_json::from_str(&older).expect("an older frame decodes");
        assert!(back.attachments.is_empty());
    }

    /// A newtype so `bytes_serde` can be exercised on its own.
    #[derive(Debug, Serialize, Deserialize)]
    struct Payload(#[serde(with = "crate::bytes_serde")] Vec<u8>);
}
