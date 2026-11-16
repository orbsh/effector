//! The environment a call runs in (ADR-0003). The axis is a TRUST DOMAIN — whose
//! code this is — and each domain has a backend that decides how it runs.
//!
//! Two domains exist as values here today, because two of them already have a
//! backend that produces them:
//!
//! - [`Environment::Host`] — trusted delivered code on a machine whose owner gave
//!   it access: a child process on this host, with no boundary of its own (the
//!   deployment's `sandbox: false`).
//! - [`Environment::Jailed`] — a child process wrapped by the bubblewrap policy
//!   ADR-0001 built: fs mounts from the allow lists, an unshared network, an
//!   optional egress proxy. ADR-0003 retires this shape in favour of a container;
//!   until that backend exists, this is what "a boundary on this machine" is.
//!
//! The two domains ADR-0003 names and this enum does NOT yet carry are `InProcess`
//! (the engine's own code, called in this address space — the embedded carriers)
//! and `Container` (a task-scoped container). They are absent on purpose: a value
//! nothing produces is the same lie as a declared field with no enforcement point
//! (ADR-0001), so each arrives with the backend that produces it. Today an
//! embedded carrier simply takes no environment at all, which says the same thing
//! with less ambiguity.

use serde::{Deserialize, Serialize};

/// How a call's code runs, by trust domain.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Environment {
    /// A child process on this host with no boundary: the deployment trusts the
    /// code's source (a host node whose owner gave it the machine's authority).
    #[default]
    Host,
    /// A child process wrapped by the declared jail: fs mounts from the allow
    /// lists, network unshared (allowlisted domains ride the proxy when one is
    /// configured). Linux only; macOS would need sandbox-exec.
    Jailed {
        /// Writable paths (bind mounts); everything else is read-only.
        allow_write: Vec<String>,
        /// Read-denied paths (mandatory deny: credentials, shell configs).
        deny_read: Vec<String>,
        /// Working directory inside the jail.
        cwd: String,
        /// Allowlisted domains (empty list = network unshared).
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

impl Environment {
    /// Whether this environment wraps its process. `false` for `Host` — a child
    /// process with the node's own authority — and for any future environment
    /// whose backend does not build a jail.
    pub fn is_wrapped(&self) -> bool {
        matches!(self, Environment::Jailed { .. })
    }
}