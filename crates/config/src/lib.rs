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
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct CapabilitySurface {
    /// Carriers this node carries (e.g. ["steel", "python"]). Registration
    /// advertises these; a task naming a missing carrier is an error value.
    pub carriers: Vec<String>,
    /// Host alias identifying this node's connection: `register` carries it,
    /// and the control plane keys the live connection by it. Residency and
    /// entry addressing do NOT ride on it — those are the call's own
    /// `executor` (the opaque name of one execution unit) and `entry` (entry point in the
    /// delivered code) fields.
    pub node_alias: String,
    /// The machine's identity, stated by the deployment: SMBIOS UUID, product serial, asset tag.
    ///
    /// `None` means "read the box": the runtime asks the platform for its own serial and reports
    /// that. Stated explicitly it wins, because a container's `/sys` is the host's or nobody's,
    /// and an operator running twelve nodes is the one who knows which box each of them is on.
    pub machine: Option<String>,
}

/// Effector startup configuration (Phase 3): where to register and what to claim.
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
    /// The first grace, before any call has run. It bounds more than idleness: a plane that
    /// accepts the socket and never says `registered` would otherwise leave a container node
    /// alive with nothing to do, which is the container the lease exists to end.
    pub fn first_grace(&self) -> Option<std::time::Duration> {
        self.grace(0)
    }

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

/// What contains this node. Mirrors `effector_protocol::NodeClass` at the config surface (the
/// protocol crate is the wire; this one is the file), and the two are converted once, at the dial.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Class {
    #[default]
    Host,
    Container,
}

impl Class {
    pub fn as_str(&self) -> &'static str {
        match self {
            Class::Host => "host",
            Class::Container => "container",
        }
    }

    /// Whether THIS machine is inside a container right now — the markers the runtimes leave.
    ///
    /// Three paths, because the runtimes disagree on where they leave it: Docker writes
    /// `/.dockerenv`; Podman (and CRI-O) write `/run/.containerenv`. The root-level
    /// `/.containerenv` this used to check is a path nobody writes — measured against Podman
    /// 6.1.4, where a container node was refused its own class until the `/run/` form was added,
    /// which is the failure mode a marker check exists to avoid in the first place.
    fn marker_present() -> bool {
        marker_in(&[
            std::path::Path::new("/.dockerenv"),
            std::path::Path::new("/.containerenv"),
            std::path::Path::new("/run/.containerenv"),
        ])
    }
}

/// The marker rule itself, with the paths as arguments: a claim about being contained is checked
/// against facts on disk, and which facts is a list — not one hard-coded path that a runtime can
/// move out from under us (see `Class::marker_present`).
fn marker_in(paths: &[&std::path::Path]) -> bool {
    paths.iter().any(|p| p.exists())
}

/// Unknown fields are REFUSED, not ignored: after the jail's fields were deleted (2026-11-17), a
/// config that still carries `fs_scope` or `sandbox` would otherwise be read as "this node has a
/// boundary", when the honest answer is that the file no longer offers one. Silence about a
/// misspelling is the same lie as a field with no enforcement point.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectorConfig {
    /// Control plane endpoint for the outbound registration connection
    /// WS only — no degraded fallback (long-polling explicitly rejected).
    pub control_plane_url: String,
    /// What contains this node (ADR-0003 §10). `container` says the node IS a container's root
    /// process — the launcher started `docker run --rm --init -i` — and its filesystem is the
    /// boundary. `host` says it is a process on somebody's machine with that machine's authority.
    /// Declared, not guessed, and CHECKED at load: a node claiming `container` without the marker
    /// the runtime leaves is refused, because a claim nothing enforces is a lie the config tells.
    #[serde(default)]
    pub class: Class,
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
    /// Refuse a configuration whose claims this node cannot make true.
    ///
    /// A declared field IS a claim about what delivered code may do, so every claim names its
    /// enforcement point. After ADR-0003 §1 there is exactly one: the class. A node that is not a
    /// container runs uncontained, and it runs only what its callers declared they accept
    /// (`ToolCall.requires`); a node that claims `container` is checked against the markers the
    /// runtime leaves. The jail's declaration fields (`fs_scope`, `deny_read`, `cwd`, `sandbox`,
    /// `network`) were retired with it — a claim whose enforcement point no longer exists is the
    /// lie this function exists to refuse.
    ///
    pub fn validate(&self) -> Result<(), String> {
        // A node claiming a container must BE in one: the launcher starts it with `docker run` and
        // the runtime leaves a marker, so the claim is checkable against a fact rather than trusted
        // as a word. There is no jail beneath the class any more (ADR-0003 §1): the filesystem of
        // whatever contains this process is the only boundary there is, which is exactly why the
        // claim has to be true.
        if self.class == Class::Container && !Class::marker_present() {
            return Err(
                "class = container: this process is not inside a container (no /.dockerenv or \
                 /.containerenv) — the launcher starts the node with `docker run`, and a host \
                 process cannot claim the container's boundary"
                    .into(),
            );
        }
        // A node without a token cannot be let in, and finding out at the first dial
        // (after spawning, after the container started) is a worse time than at load.
        if self.token.trim().is_empty() {
            return Err(
                "token: empty — a node's authority is the token it dialed with; mint one on \
                 the control plane (`/admin/nodes`) and pass it in the config or as `--token`"
                    .into(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(capabilities: CapabilitySurface) -> EffectorConfig {
        EffectorConfig {
            control_plane_url: "ws://127.0.0.1:8787".into(),
            class: Class::Host,
            token: "test-token".into(),
            capabilities,
            lease: LeaseConfig::default(),
        }
    }

    fn declaring() -> CapabilitySurface {
        CapabilitySurface {
            node_alias: "n1".into(),
            carriers: vec!["exec".into()],
            machine: None,
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
    fn the_container_marker_rule_reads_every_path_a_runtime_writes() {
        // A container claim is checked against facts, and the facts are per-runtime: Docker leaves
        // `/.dockerenv`, Podman and CRI-O leave `/run/.containerenv`. A rule that knew only the
        // first refuses a Podman container its own class — which is how this one was found.
        let dir = std::env::temp_dir().join(format!("effector-marker-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let absent = dir.join("not-a-marker");
        let podman = dir.join("containerenv");
        std::fs::write(&podman, b"").expect("marker file");

        assert!(
            !marker_in(&[absent.as_path()]),
            "no marker is not a container"
        );
        assert!(marker_in(&[absent.as_path(), podman.as_path()]), "a marker is");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_host_node_never_expires() {
        let lease = LeaseConfig::default();
        assert!(lease.grace(1).is_none(), "no idle exit unless declared");
        assert!(!lease.exit_on_disconnect, "a host node reconnects");
    }

    #[test]
    fn a_container_class_is_refused_where_it_would_be_a_claim_and_not_a_fact() {
        // The claim and the boundary are checked together: a jail inside a container bounds nothing,
        // and a host process claiming a container's boundary is the more dangerous of the two
        // because it makes an uncontained machine look safe.
        // The marker decides — and this test runs outside a container, so the honest answer here is
        // the refusal, naming what is missing.
        let mut c = config(declaring());
        c.class = Class::Container;
        match c.validate() {
            Err(err) => assert!(err.contains("not inside a container"), "{err}"),
            Ok(()) => { /* this test ran inside a container: the claim is checkable, so it held */ }
        }
    }

    #[test]
    fn a_config_that_still_names_the_jails_fields_is_refused_rather_than_ignored() {
        // The retirement left the fields in operators' files. Ignoring them would tell an operator
        // their node has a filesystem scope when the file no longer offers one.
        let text = r#"{
            "control_plane_url": "ws://127.0.0.1:8787",
            "token": "t",
            "sandbox": true,
            "capabilities": {"node_alias": "n", "carriers": ["steel"]}
        }"#;
        let err = serde_json::from_str::<EffectorConfig>(text)
            .unwrap_err()
            .to_string();
        assert!(err.contains("sandbox"), "{err}");
    }

    #[test]
    fn only_the_class_and_the_token_are_claims_left_to_check() {
        // The jail's declaration fields retired with it (ADR-0003 §1), so the only claims a config
        // can still make are the two that have an enforcement point: what contains the node, and the
        // token it dialed with.
        assert!(config(declaring()).validate().is_ok());

        let mut c = config(declaring());
        c.token = "  ".into();
        let err = c.validate().unwrap_err();
        assert!(err.contains("token: empty"), "{err}");
    }
}
