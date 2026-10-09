//! Sandbox policy: process-boundary containment for code-executing
//! sessions. Belongs to the DEPLOYMENT FORM, not the execution mechanism —
//! effector (unattended machine, external code) wraps session processes in a
//! bwrap sandbox; aura in-process (user's own trust domain) passes None.

use serde::{Deserialize, Serialize};

/// Sandbox policy for a session's process.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SandboxPolicy {
    /// No process sandbox: in-process carriers (steel/python) or a
    /// deployment where the code source is fully trusted (aura embedded —
    /// the user's own Krystallizer graph, user present).
    #[default]
    None,
    /// Wrap the session process in bubblewrap: fs mounts from the allow
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
    },
}

impl SandboxPolicy {
    pub fn is_wrapped(&self) -> bool {
        matches!(self, SandboxPolicy::Bubblewrap { .. })
    }
}
