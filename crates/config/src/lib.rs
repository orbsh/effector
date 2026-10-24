//! Effector registration and capability configuration.
//!
//! Registration credential = user credential (outbound connection carries
//! "whose machine am I"). The capability list is written into the user's
//! namespace registry by the control plane; this file is the Effector-side
//! declaration of the same surface.


// Unit tests may use the unwrap/panic vocabulary the production targets are
// denied (a panicking test is a failing test). `cfg(test)` is on only for this
// crate's own test build; `tests/` crates set the same line at their own root.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::unwrap_in_result, clippy::panic_in_result_fn))]
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
    /// Filesystem paths delivered scripts may WRITE. Enforced as the jail's
    /// write scope: everything else is read-only, and reads are not confined
    /// by this field (deny_read is the read-side declaration).
    pub fs_scope: Vec<String>,
    /// Paths delivered scripts must NOT read, whatever else would allow them
    /// (credentials, shell configs, other users' trees). Enforced as the
    /// jail's deny_read.
    pub deny_read: Vec<String>,
    /// Working directory inside the jail. `None` = the system temp dir (the
    /// pre-declaration default, kept so an unconfigured node behaves as it
    /// did before this field existed).
    pub cwd: Option<String>,
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
            deny_read: Vec::new(),
            cwd: None,
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

impl EffectorConfig {
    /// Refuse a configuration that claims containment this node cannot
    /// enforce. A declared field IS a claim about what delivered code may do,
    /// so every field must name its enforcement point: a claim nothing
    /// enforces is a lie the config file tells, and this node fails closed
    /// rather than registering it as a capability.
    ///
    /// Enforced today: `fs_scope` (the jail's write scope), `deny_read`, `cwd`,
    /// and the wrap itself (`sandbox`). Not wired: a command-execution GRANT,
    /// and any `network` other than `none` (egress allowlists ride the sandbox
    /// runtime's proxy, which nothing here configures; the jail denies egress
    /// outright, so a declaration of access would contradict it).
    pub fn validate(&self) -> Result<(), String> {
        if self.capabilities.command_exec {
            return Err(
                "capabilities.command_exec = true: nothing in this node enforces a \
                 command-execution grant (no policy field carries it, and the jail admits \
                 exec inside its namespace regardless) — declare false, or wire the \
                 enforcement before claiming it"
                    .into(),
            );
        }
        match &self.capabilities.network {
            NetworkPolicy::None => Ok(()),
            other => Err(format!(
                "capabilities.network = {}: this node wires no enforcement for it — egress \
                 allowlists ride the sandbox runtime's proxy (not configured here) and the \
                 jail denies egress outright, so the declaration would contradict the jail \
                 — declare \"none\", or wire the allowlist path first",
                serde_json::to_string(other).unwrap_or_else(|_| "<unprintable>".into())
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(capabilities: CapabilitySurface) -> EffectorConfig {
        EffectorConfig {
            control_plane_url: "ws://127.0.0.1:8787".into(),
            sandbox: true,
            credential_env: "EFFECTOR_CREDENTIAL".into(),
            capabilities,
        }
    }

    fn declaring() -> CapabilitySurface {
        CapabilitySurface {
            node_alias: "n1".into(),
            carriers: vec!["exec".into()],
            ..Default::default()
        }
    }

    #[test]
    fn a_declaration_without_an_enforcement_point_is_refused() {
        let mut caps = declaring();
        caps.command_exec = true;
        let err = config(caps).validate().unwrap_err();
        assert!(err.contains("command_exec"), "{err}");

        let mut caps = declaring();
        caps.network = NetworkPolicy::Allow(vec!["github.com:443".into()]);
        let err = config(caps).validate().unwrap_err();
        assert!(err.contains("network") && err.contains("proxy"), "{err}");

        let mut caps = declaring();
        caps.network = NetworkPolicy::Open;
        assert!(
            config(caps).validate().is_err(),
            "an unrestricted-egress claim has no enforcement either"
        );
    }

    #[test]
    fn the_enforceable_declarations_pass() {
        let mut caps = declaring();
        caps.fs_scope = vec!["/srv/work".into()];
        caps.deny_read = vec!["/home/user/.ssh".into()];
        caps.cwd = Some("/srv/work".into());
        assert!(config(caps).validate().is_ok());
    }
}
