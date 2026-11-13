//! Sandbox policy: process-boundary containment for code-executing
//! executors. Belongs to the DEPLOYMENT FORM, not the execution mechanism —
//! effector (unattended machine, external code) wraps executor processes in a
//! bwrap sandbox; aura in-process (user's own trust domain) passes None.

use serde::{Deserialize, Serialize};

/// Sandbox policy for a executor's process.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SandboxPolicy {
    /// No process sandbox: in-process carriers (steel/python) or a
    /// deployment where the code source is fully trusted (aura embedded —
    /// the user's own Krystallizer graph, user present).
    #[default]
    None,
    /// Wrap the executor process in bubblewrap: fs mounts from the allow
    /// lists, network unshared (allowlist domains ride the proxy when
    /// configured). Linux only; macOS uses sandbox-exec.
    Bubblewrap {
        /// Writable paths (bind mounts); everything else is read-only.
        allow_write: Vec<String>,
        /// Read-denied paths (mandatory deny: credentials, shell configs).
        deny_read: Vec<String>,
        /// Working directory inside the sandbox.
        cwd: String,
        /// Allowlisted domains (None/empty list = network unshared).
        allowed_domains: Vec<String>,
        /// The egress face the jail can reach when the allowlist is non-empty:
        /// the node's own proxies plus the bridges into the namespace. `None`
        /// means the namespace has no network at all.
        proxy: Option<ProxyFace>,
    },
}

/// Where the jail reaches the node's egress proxies.
///
/// The jailed process cannot see the host's loopback (it is a different network
/// namespace), so the two ends meet on a UNIX socket: the node listens on it and
/// the jail's `socat` bridge connects out to it, exposing an ordinary
/// `127.0.0.1:<port>` proxy inside. The socket paths live OUTSIDE `/tmp` and
/// `/run` because the jail tmpfs-es both — a path there is invisible inside.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyFace {
    /// UNIX socket the node's bridge listens on, for the HTTP proxy.
    pub http_socket: String,
    /// The same, for the SOCKS5 proxy (no HTTP proxy honors a non-HTTP client).
    pub socks_socket: String,
    /// The loopback ports the jailed bridges listen on (fixed numbers, not
    /// discovered: the jail's `socat` binds them before the script runs).
    pub http_port: u16,
    pub socks_port: u16,
}

impl SandboxPolicy {
    pub fn is_wrapped(&self) -> bool {
        matches!(self, SandboxPolicy::Bubblewrap { .. })
    }
}
