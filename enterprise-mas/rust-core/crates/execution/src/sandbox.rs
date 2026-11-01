//! Sandbox policies for hermetic execution.
//!
//! A [`SandboxPolicy`] describes *what isolation a run requires*: which
//! environment variables may flow in, which hosts may be contacted, and the
//! resource envelope. Enforcement of the hard isolation (seccomp, cgroups,
//! network namespaces) belongs to the deployment; this crate checks policy
//! conformance at invocation time so unsanctioned configurations can never
//! reach a runtime by accident.

use mas_common::error::AppError;
use mas_common::result::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Network egress behavior inside the sandbox.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EgressPolicy {
    /// No network at all (default — most restrictive).
    #[default]
    DenyAll,
    /// Only the explicitly allow-listed hosts (exact match, no wildcards).
    AllowListOnly,
    /// Full egress (requires elevated tool safety classification downstream).
    Unrestricted,
}

/// Hermetic run policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxPolicy {
    /// Isolate this run even when the tool doesn't demand it.
    pub required: bool,
    /// Environment variable names allowed into the sandbox.
    #[serde(default)]
    pub env_allowlist: BTreeSet<String>,
    #[serde(default)]
    pub egress: EgressPolicy,
    /// Hosts reachable when `egress == AllowListOnly` (no schemes, no ports).
    #[serde(default)]
    pub allowed_hosts: BTreeSet<String>,
    /// Working memory ceiling (MiB) applied by the runtime.
    pub memory_limit_mib: u32,
    /// Maximum filesystem writes (MiB); 0 ⇒ read-only scratch.
    pub scratch_disk_mib: u32,
    /// Max processes/threads the workload may spawn.
    pub max_processes: u32,
}

impl Default for SandboxPolicy {
    fn default() -> Self {
        Self {
            required: false,
            env_allowlist: BTreeSet::new(),
            egress: EgressPolicy::DenyAll,
            allowed_hosts: BTreeSet::new(),
            memory_limit_mib: 512,
            scratch_disk_mib: 64,
            max_processes: 32,
        }
    }
}

impl SandboxPolicy {
    /// Fully locked-down policy: required, no egress, read-only scratch.
    #[must_use]
    pub fn hermetic() -> Self {
        Self {
            required: true,
            scratch_disk_mib: 0,
            ..Self::default()
        }
    }

    /// Validates internal consistency.
    pub fn validate(&self) -> Result<()> {
        if self.egress == EgressPolicy::AllowListOnly && self.allowed_hosts.is_empty() {
            return Err(AppError::invalid_field(
                "allowed_hosts",
                "required",
                "egress allow-list mode requires at least one allowed host",
            ));
        }
        if self.egress != EgressPolicy::AllowListOnly && !self.allowed_hosts.is_empty() {
            return Err(AppError::invalid_field(
                "allowed_hosts",
                "invalid_policy",
                "allowed hosts only make sense in allow-list egress mode",
            ));
        }
        if self.memory_limit_mib == 0 || self.memory_limit_mib > 64 * 1024 {
            return Err(AppError::invalid_field(
                "memory_limit_mib",
                "out_of_range",
                "memory limit must be in 1..=65536 MiB",
            ));
        }
        if self.max_processes == 0 || self.max_processes > 4096 {
            return Err(AppError::invalid_field(
                "max_processes",
                "out_of_range",
                "process limit must be in 1..=4096",
            ));
        }
        for var in &self.env_allowlist {
            if var.is_empty()
                || var.len() > 128
                || !var
                    .chars()
                    .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_')
            {
                return Err(AppError::invalid_field(
                    "env_allowlist",
                    "invalid_format",
                    format!("environment names must be SCREAMING_SNAKE_CASE, got '{var}'"),
                ));
            }
            // Secrets are sourced through SecretReference resolution, never
            // through inherited environment — this is a hard platform rule.
            if var.contains("SECRET")
                || var.contains("TOKEN")
                || var.contains("PASSWORD")
                || var.contains("KEY")
            {
                return Err(AppError::invalid_field(
                    "env_allowlist",
                    "secret_passthrough",
                    format!(
                        "'{var}' looks like a secret; secrets must resolve via \
                         SecretReference, not environment passthrough"
                    ),
                ));
            }
        }
        for host in &self.allowed_hosts {
            let valid = !host.is_empty()
                && host.len() <= 253
                && host
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '.' || ch == '-');
            if !valid {
                return Err(AppError::invalid_field(
                    "allowed_hosts",
                    "invalid_format",
                    format!("'{host}' is not a valid hostname"),
                ));
            }
        }
        Ok(())
    }

    /// Whether the sandbox must be active for a tool that doesn't itself
    /// request sandboxing.
    #[must_use]
    pub const fn is_required(&self) -> bool {
        self.required
    }

    /// Whether a URL with `host` may be contacted under this policy.
    #[must_use]
    pub fn permits_host(&self, host: &str) -> bool {
        match self.egress {
            EgressPolicy::DenyAll => false,
            EgressPolicy::Unrestricted => true,
            EgressPolicy::AllowListOnly => self.allowed_hosts.contains(host),
        }
    }

    /// Merges the strictest dimensions of two policies (defense in depth when
    /// both tool- and execution-level policies apply).
    #[must_use]
    pub fn intersection(&self, other: &Self) -> Self {
        Self {
            required: self.required || other.required,
            env_allowlist: self
                .env_allowlist
                .intersection(&other.env_allowlist)
                .cloned()
                .collect(),
            egress: match (self.egress, other.egress) {
                (EgressPolicy::DenyAll, _) | (_, EgressPolicy::DenyAll) => EgressPolicy::DenyAll,
                (EgressPolicy::AllowListOnly, _) | (_, EgressPolicy::AllowListOnly) => {
                    EgressPolicy::AllowListOnly
                },
                _ => EgressPolicy::Unrestricted,
            },
            allowed_hosts: if self.egress == EgressPolicy::AllowListOnly
                && other.egress == EgressPolicy::AllowListOnly
            {
                // Strictest = hosts allowed by BOTH policies.
                self.allowed_hosts
                    .intersection(&other.allowed_hosts)
                    .cloned()
                    .collect()
            } else if self.egress == EgressPolicy::AllowListOnly {
                self.allowed_hosts.clone()
            } else {
                other.allowed_hosts.clone()
            },
            memory_limit_mib: self.memory_limit_mib.min(other.memory_limit_mib),
            scratch_disk_mib: self.scratch_disk_mib.min(other.scratch_disk_mib),
            max_processes: self.max_processes.min(other.max_processes),
        }
    }

    /// Ensures a tool execution request conforms; unsanctioned runs are
    /// rejected before reaching any runtime.
    pub fn enforce(&self, requests_network: bool, network_host: Option<&str>) -> Result<()> {
        self.validate()?;
        if !requests_network {
            return Ok(());
        }
        match (self.egress, network_host) {
            (EgressPolicy::DenyAll, _) => Err(AppError::forbidden(
                "sandbox policy denies all network egress",
            )),
            (EgressPolicy::AllowListOnly, Some(host)) if self.allowed_hosts.contains(host) => {
                Ok(())
            },
            (EgressPolicy::AllowListOnly, Some(host)) => Err(AppError::forbidden(format!(
                "host '{host}' is not in the sandbox allow-list"
            ))),
            (EgressPolicy::AllowListOnly, None) => Err(AppError::forbidden(
                "network request without a host cannot be allow-listed",
            )),
            (EgressPolicy::Unrestricted, _) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_locked_down_but_valid() {
        let policy = SandboxPolicy::default();
        policy.validate().expect("default policy is valid");
        assert!(!policy.permits_host("example.com"));
        let hermetic = SandboxPolicy::hermetic();
        hermetic.validate().expect("hermetic policy is valid");
        assert!(hermetic.is_required());
        assert_eq!(hermetic.scratch_disk_mib, 0);
    }

    #[test]
    fn allowlist_mode_requires_hosts_and_blocks_secrets() {
        let mut policy = SandboxPolicy {
            egress: EgressPolicy::AllowListOnly,
            ..SandboxPolicy::default()
        };
        assert!(policy.validate().is_err());
        policy.allowed_hosts.insert("api.internal".to_owned());
        policy.validate().expect("with a host it validates");
        assert!(policy.permits_host("api.internal"));
        assert!(!policy.permits_host("evil.example"));

        policy.env_allowlist.insert("AWS_SECRET_KEY".to_owned());
        let error = policy.validate().unwrap_err();
        assert_eq!(error.error_code(), "VALIDATION_FAILED");
    }

    #[test]
    fn intersection_keeps_the_strictest_side() {
        let mut a = SandboxPolicy {
            egress: EgressPolicy::AllowListOnly,
            allowed_hosts: BTreeSet::from(["one.internal".to_owned(), "two.internal".to_owned()]),
            memory_limit_mib: 1024,
            ..SandboxPolicy::default()
        };
        let b = SandboxPolicy {
            egress: EgressPolicy::AllowListOnly,
            allowed_hosts: BTreeSet::from(["two.internal".to_owned()]),
            memory_limit_mib: 256,
            env_allowlist: BTreeSet::from(["HOME".to_owned()]),
            ..SandboxPolicy::default()
        };
        a.env_allowlist = BTreeSet::from(["HOME".to_owned(), "PATH".to_owned()]);

        let merged = a.intersection(&b);
        merged.validate().expect("merged policy validates");
        assert_eq!(merged.memory_limit_mib, 256);
        assert_eq!(merged.allowed_hosts.len(), 1);
        assert!(merged.permits_host("two.internal"));
        assert_eq!(merged.env_allowlist.len(), 1);
    }

    #[test]
    fn enforce_denies_unlisted_network_but_allows_local() {
        let mut policy = SandboxPolicy::default();
        policy
            .enforce(false, None)
            .expect("no network = no problem");
        assert!(policy.enforce(true, Some("example.com")).is_err());

        policy.egress = EgressPolicy::Unrestricted;
        policy
            .enforce(true, Some("example.com"))
            .expect("unrestricted egress");
    }
}
