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
//! - WHICH node runs it. Placement is the realm's, decided from declared capability and locality.
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
        assert_eq!(OnError::default(), OnError::Fail, "silence means one attempt");
    }
}