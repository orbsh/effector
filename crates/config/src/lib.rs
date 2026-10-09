//! Effector registration and capability configuration.
//!
//! Registration credential = user credential (outbound connection carries
//! "whose machine am I"). The capability list is written into the user's
//! namespace registry by the control plane; this file is the Effector-side
//! declaration of the same surface.

use serde::{Deserialize, Serialize};

/// Per-effector capability surface.
///
/// The Effector ships NO built-in operations: every operation (`read_file`,
/// `list_processes`, ...) is a user-written or AI-generated script/booth
/// delivered through a carrier. What this surface declares is (a) which
/// carriers the node carries — the registration's capability list is
/// exactly this — and (b) the scope constraints enforced on delivered
/// scripts at execution time. Whether an operation is AI-generated-live or
/// pre-audited is a control-plane (Gravity/Krystallizer) policy; the Effector
/// executes what it is handed.
///
/// This is the ONLY containment layer (no per-operation container
/// boundary beneath it), so the scope check must be enforced at execution,
/// not just honored at dispatch-declaration. No credential fields by design.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CapabilitySurface {
    /// Carriers this node carries (e.g. ["steel", "python"]). Registration
    /// advertises these; a task naming a missing carrier is an error value.
    pub carriers: Vec<String>,
    /// Filesystem paths delivered scripts may read/write. Prefix-scoped;
    /// empty = no fs.
    pub fs_scope: Vec<String>,
    /// Whether delivered scripts may execute host commands.
    pub command_exec: bool,
    /// Network egress policy for script execution.
    pub network: NetworkPolicy,
    /// Host alias identifying this node's connection: `register` carries it,
    /// and the control plane keys the live connection by it. Residency and
    /// entry addressing do NOT ride on it — those are the call's own
    /// `session` (opaque residency identity) and `entry` (entry point in the
    /// delivered code) fields.
    pub node_alias: String,
}

impl Default for CapabilitySurface {
    fn default() -> Self {
        Self {
            carriers: Vec::new(),
            fs_scope: Vec::new(),
            command_exec: false,
            network: NetworkPolicy::None,
            node_alias: String::new(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkPolicy {
    /// No outbound network from skills.
    #[default]
    None,
    /// Explicit allowlist of host:port targets.
    Allow(Vec<String>),
    /// Unrestricted (single-user nodes where the user accepted the trade).
    Open,
}

/// Effector startup configuration (Phase 3): where to register and what to claim.
fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EffectorConfig {
    /// Control plane endpoint for the outbound registration connection
    /// WS only — no degraded fallback (long-polling explicitly rejected).
    pub control_plane_url: String,
    /// Wrap session processes in bubblewrap. Remote deployments MUST set
    /// true (unattended external code); requires bwrap on PATH. In-process
    /// embedded deployments (aura) pass None instead.
    #[serde(default = "default_true")]
    pub sandbox: bool,
    /// User credential used as the registration credential. Provided by the
    /// environment (single source of truth outside config files).
    pub credential_env: String,
    pub capabilities: CapabilitySurface,
}
