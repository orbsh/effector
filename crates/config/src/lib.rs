//! Effector registration and capability configuration.
//!
//! Registration credential = user credential (outbound connection carries
//! "whose machine am I"). The capability list is written into the user's
//! namespace registry by the control plane; this file is the Effector-side
//! declaration of the same surface.

// Unit tests may use the unwrap/panic vocabulary the production targets are
// denied (a panicking test is a failing test). `cfg(test)` is on only for this
// crate's own test build; `tests/` crates set the same line at their own root.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unwrap_in_result,
        clippy::panic_in_result_fn
    )
)]
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
    /// Network egress policy for script execution.
    pub network: NetworkPolicy,
    /// Host alias identifying this node's connection: `register` carries it,
    /// and the control plane keys the live connection by it. Residency and
    /// entry addressing do NOT ride on it — those are the call's own
    /// `executor` (the opaque name of one execution unit) and `entry` (entry point in the
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

fn default_max_idle() -> u64 {
    300
}

/// How long a node lives when nothing is happening (ADR-0003's lease).
///
/// A node that IS a container (ADR-0003): its lifetime is the container's, so the
/// process leaves when the work stops and the container ends with it — no pool
/// manager, no reaper, no `docker rm`. A node on a host leaves this at the default,
/// where it never expires and reconnects instead.
///
/// The grace grows with use, because a node used more is likelier to be used again:
/// the first idle waits `idle_secs`, each reuse doubles it, and `max_idle_secs` caps
/// the wait — past a few minutes the wait costs more than a cold start, and a task
/// idle that long is not latency-sensitive anyway.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseConfig {
    /// Seconds of idleness before the node leaves; `0` = never (a host node).
    #[serde(default)]
    pub idle_secs: u64,
    /// The cap on the doubling.
    #[serde(default = "default_max_idle")]
    pub max_idle_secs: u64,
    /// Leave when the connection ends instead of reconnecting. A container node
    /// sets this: a node whose control-plane link is gone has no work and no way
    /// to be given any.
    #[serde(default)]
    pub exit_on_disconnect: bool,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            idle_secs: 0,
            max_idle_secs: default_max_idle(),
            exit_on_disconnect: false,
        }
    }
}

impl LeaseConfig {
    /// The grace after `reuses` calls have run on this node: base, doubled per use,
    /// capped. `None` when the lease never expires.
    pub fn grace(&self, reuses: u32) -> Option<std::time::Duration> {
        if self.idle_secs == 0 {
            return None;
        }
        let doublings = reuses.saturating_sub(1).min(20);
        let secs = self
            .idle_secs
            .saturating_mul(1u64 << doublings)
            .min(self.max_idle_secs.max(self.idle_secs));
        Some(std::time::Duration::from_secs(secs))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EffectorConfig {
    /// Control plane endpoint for the outbound registration connection
    /// WS only — no degraded fallback (long-polling explicitly rejected).
    pub control_plane_url: String,
    /// Wrap executor processes in bubblewrap. Remote deployments MUST set
    /// true (unattended external code); requires bwrap on PATH. In-process
    /// embedded deployments (aura) pass None instead.
    #[serde(default = "default_true")]
    pub sandbox: bool,
    /// The token the control plane minted for this node's alias (ADR-0003). It rides the DIAL
    /// — a query parameter on the mount URL — and is checked once, when the connection is
    /// accepted; everything after that rides the connection, which is the authorization. A
    /// node never holds a long-lived identity private key any more: a token is revocable by
    /// re-minting, and a node that is a container is given its token at launch.
    pub token: String,
    /// When this node leaves (ADR-0003). Defaults to never: a host node.
    #[serde(default)]
    pub lease: LeaseConfig,
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
    /// the wrap itself (`sandbox`), and `network` as an allowlist — the node runs
    /// the proxies and hands the jail the sockets to reach them, so a declared
    /// list is a list the script is confined to.
    ///
    /// There is no command-execution grant to declare, and there never can be
    /// one: a node's whole job is to run a delivered script inside a jail that
    /// admits `exec` by construction, so "may execute" is not a capability
    /// distinct from existing. The field that used to claim it was retired
    /// (ADR-0001's Update) rather than wired — the honest close for a claim
    /// with no enforcement point is to stop making it.
    ///
    /// Refused: `open` (the jail has no way to be open — its network namespace is
    /// created unshared, so "unrestricted" is a promise this shape cannot keep),
    /// and an EMPTY allowlist (the filter reads an empty list as "allow
    /// everything", which is the opposite of what a declaration asks for).
    pub fn validate(&self) -> Result<(), String> {
        // A node without a token cannot be let in, and finding that out at the first dial
        // (after spawning, after the container started) is a worse time than at load.
        if self.token.trim().is_empty() {
            return Err(
                "token: empty — a node's authority is the token it dialed with; mint one on \
                 the control plane (`/admin/nodes`) and pass it in the config or as `--token`"
                    .into(),
            );
        }
        match &self.capabilities.network {
            NetworkPolicy::None => Ok(()),
            NetworkPolicy::Allow(domains) if domains.is_empty() => Err(
                "capabilities.network = allow []: an empty allowlist reads as \"allow \
                 everything\" to the egress filter — declare the hostnames, or declare \
                 \"none\""
                    .into(),
            ),
            NetworkPolicy::Allow(_) if !self.sandbox => Err(
                "capabilities.network = allow [...]: the allowlist is reached through the \
                 jail's bridges, and `sandbox: false` builds no jail — declare \
                 \"none\" for a trusted-domain node"
                    .into(),
            ),
            NetworkPolicy::Allow(_) => Ok(()),
            NetworkPolicy::Open => Err(
                "capabilities.network = open: this node's network namespace is unshared, so \
                 an unrestricted namespace is not a shape it can offer — declare an allowlist"
                    .into(),
            ),
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
            token: "test-token".into(),
            capabilities,
            lease: LeaseConfig::default(),
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
    fn the_lease_doubles_per_use_and_caps() {
        let lease = LeaseConfig {
            idle_secs: 15,
            max_idle_secs: 300,
            exit_on_disconnect: true,
        };
        let secs = |n: u32| lease.grace(n).map(|d| d.as_secs()).unwrap_or(0);
        assert_eq!(secs(0), 15, "a node that has served nothing waits the base");
        assert_eq!(secs(1), 15);
        assert_eq!(secs(2), 30);
        assert_eq!(secs(3), 60);
        assert_eq!(secs(4), 120);
        assert_eq!(secs(5), 240);
        assert_eq!(secs(6), 300, "the cap");
        assert_eq!(secs(9), 300, "and it stays capped");
    }

    #[test]
    fn a_host_node_never_expires() {
        let lease = LeaseConfig::default();
        assert!(lease.grace(1).is_none(), "no idle exit unless declared");
        assert!(!lease.exit_on_disconnect, "a host node reconnects");
    }

    #[test]
    fn a_declaration_without_an_enforcement_point_is_refused() {
        // An allowlist IS enforceable now (the node runs the proxies and the jail
        // reaches them through its bridges), so the accepted shape is the ordinary
        // one; what is refused is the shapes nothing can enforce.
        let mut caps = declaring();
        caps.network = NetworkPolicy::Allow(vec!["github.com:443".into()]);
        config(caps).validate().expect("a declared allowlist is a jail");

        let mut caps = declaring();
        caps.network = NetworkPolicy::Open;
        let err = config(caps).validate().unwrap_err();
        assert!(err.contains("unshared"), "{err}");

        let mut caps = declaring();
        caps.network = NetworkPolicy::Allow(Vec::new());
        let err = config(caps).validate().unwrap_err();
        assert!(
            err.contains("empty allowlist") || err.contains("allow everything"),
            "an empty list would read as allow-all: {err}"
        );

        let mut caps = declaring();
        caps.network = NetworkPolicy::Allow(vec!["github.com".into()]);
        let mut trusted = config(caps);
        trusted.sandbox = false;
        let err = trusted.validate().unwrap_err();
        assert!(
            err.contains("builds no jail"),
            "an allowlist needs the jail's bridges: {err}"
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
