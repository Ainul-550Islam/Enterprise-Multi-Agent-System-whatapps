//! Immutable agent versions.
//!
//! A published `AgentVersion` is **write-once**: configuration snapshot,
//! checksum and audit metadata are fixed at publish time. The only legal
//! post-publish changes are deployment-status transitions, which is what the
//! methods below model — there are intentionally no setters for anything else.

use mas_common::enums::DeploymentStatus;
use mas_common::error::AppError;
use mas_common::ids::{AgentId, AgentVersionId, UserId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

/// One immutable published configuration of an agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentVersion {
    id: AgentVersionId,
    agent_id: AgentId,
    /// Monotonic per-agent version number (1, 2, 3, …).
    version_number: u64,
    /// Canonical lowercase-hex SHA-256 of the versioned configuration.
    configuration_checksum: String,
    /// Versioned orchestration snapshot (agent config + capabilities).
    configuration_snapshot: serde_json::Value,
    published_by: UserId,
    published_at: Timestamp,
    deployment_status: DeploymentStatus,
    /// Set when this version was created/activated as a rollback target.
    rollback_of: Option<AgentVersionId>,
}

impl AgentVersion {
    /// Publishes a new immutable version.
    pub fn publish(
        agent_id: AgentId,
        version_number: u64,
        configuration_checksum: impl Into<String>,
        configuration_snapshot: serde_json::Value,
        published_by: UserId,
    ) -> Result<Self> {
        let configuration_checksum = configuration_checksum.into();
        Self::validate_checksum(&configuration_checksum)?;
        if version_number == 0 {
            return Err(AppError::invalid_field(
                "version_number",
                "out_of_range",
                "version numbers start at 1",
            ));
        }
        if !configuration_snapshot.is_object() {
            return Err(AppError::invalid_field(
                "configuration_snapshot",
                "invalid_format",
                "snapshot must be a JSON object",
            ));
        }
        Ok(Self {
            id: AgentVersionId::new(),
            agent_id,
            version_number,
            configuration_checksum,
            configuration_snapshot,
            published_by,
            published_at: Timestamp::now(),
            deployment_status: DeploymentStatus::NotDeployed,
            rollback_of: None,
        })
    }

    /// Marks this version as the product of a rollback to `source`.
    #[must_use]
    pub fn as_rollback_of(mut self, source: AgentVersionId) -> Self {
        self.rollback_of = Some(source);
        self
    }

    /// Storage rehydration: rebuilds a previously-persisted row WITHOUT
    /// re-running publish-time semantics (a fresh id/timestamp would break
    /// round-trip fidelity). Structural invariants (checksum shape, version
    /// ≥ 1, object snapshot) are still enforced — a row that fails them is
    /// corrupt persistence, not a legitimate historical state.
    #[allow(clippy::too_many_arguments)]
    pub fn rehydrate(
        id: AgentVersionId,
        agent_id: AgentId,
        version_number: u64,
        configuration_checksum: impl Into<String>,
        configuration_snapshot: serde_json::Value,
        published_by: UserId,
        published_at: Timestamp,
        deployment_status: DeploymentStatus,
        rollback_of: Option<AgentVersionId>,
    ) -> Result<Self> {
        let configuration_checksum = configuration_checksum.into();
        Self::validate_checksum(&configuration_checksum)?;
        if version_number == 0 {
            return Err(AppError::invalid_field(
                "version_number",
                "out_of_range",
                "version numbers start at 1",
            ));
        }
        if !configuration_snapshot.is_object() {
            return Err(AppError::invalid_field(
                "configuration_snapshot",
                "not_object",
                "snapshot must be a JSON object",
            ));
        }
        Ok(Self {
            id,
            agent_id,
            version_number,
            configuration_checksum,
            configuration_snapshot,
            published_by,
            published_at,
            deployment_status,
            rollback_of,
        })
    }

    fn validate_checksum(checksum: &str) -> Result<()> {
        validation::validate_length("configuration_checksum", checksum, 64, 64)?;
        if !checksum
            .chars()
            .all(|ch| ch.is_ascii_hexdigit() || ch.is_ascii_digit())
        {
            return Err(AppError::invalid_field(
                "configuration_checksum",
                "invalid_format",
                "checksum must be lowercase hex-encoded SHA-256",
            ));
        }
        if checksum.chars().any(|ch| ch.is_ascii_uppercase()) {
            return Err(AppError::invalid_field(
                "configuration_checksum",
                "invalid_format",
                "checksum must be lowercase hex",
            ));
        }
        Ok(())
    }

    /// Verifies a recomputed checksum against the published one.
    #[must_use]
    pub fn checksum_matches(&self, recomputed: &str) -> bool {
        // Constant-time-ish comparison is unnecessary for integrity (not a
        // secret), but exactness is mandatory.
        self.configuration_checksum == recomputed
    }

    // -- deployment status transitions (the ONLY legal mutation) -----------

    /// NotDeployed/Failed → Deploying.
    pub fn begin_deployment(&mut self) -> Result<()> {
        match self.deployment_status {
            DeploymentStatus::NotDeployed | DeploymentStatus::Failed => {
                self.deployment_status = DeploymentStatus::Deploying;
                Ok(())
            },
            other => Err(AppError::conflict(format!(
                "cannot deploy a version in status '{other}'"
            ))),
        }
    }

    /// Deploying → Deployed.
    pub fn mark_deployed(&mut self) -> Result<()> {
        match self.deployment_status {
            DeploymentStatus::Deploying => {
                self.deployment_status = DeploymentStatus::Deployed;
                Ok(())
            },
            other => Err(AppError::conflict(format!(
                "cannot mark '{other}' version as deployed"
            ))),
        }
    }

    /// Deploying → Failed.
    pub fn mark_deployment_failed(&mut self) -> Result<()> {
        match self.deployment_status {
            DeploymentStatus::Deploying => {
                self.deployment_status = DeploymentStatus::Failed;
                Ok(())
            },
            other => Err(AppError::conflict(format!(
                "cannot mark '{other}' version as failed"
            ))),
        }
    }

    /// Deployed → RolledBack (another version took over).
    pub fn mark_rolled_back(&mut self) -> Result<()> {
        match self.deployment_status {
            DeploymentStatus::Deployed => {
                self.deployment_status = DeploymentStatus::RolledBack;
                Ok(())
            },
            other => Err(AppError::conflict(format!(
                "only deployed versions can be rolled back, not '{other}'"
            ))),
        }
    }

    // -- read-only accessors ------------------------------------------------

    #[must_use]
    pub const fn id(&self) -> AgentVersionId {
        self.id
    }
    #[must_use]
    pub const fn agent_id(&self) -> AgentId {
        self.agent_id
    }
    #[must_use]
    pub const fn version_number(&self) -> u64 {
        self.version_number
    }
    #[must_use]
    pub fn configuration_checksum(&self) -> &str {
        &self.configuration_checksum
    }
    #[must_use]
    pub const fn configuration_snapshot(&self) -> &serde_json::Value {
        &self.configuration_snapshot
    }
    #[must_use]
    pub const fn published_by(&self) -> UserId {
        self.published_by
    }
    #[must_use]
    pub const fn published_at(&self) -> Timestamp {
        self.published_at
    }
    #[must_use]
    pub const fn deployment_status(&self) -> DeploymentStatus {
        self.deployment_status
    }
    #[must_use]
    pub const fn rollback_of(&self) -> Option<AgentVersionId> {
        self.rollback_of
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_snapshot() -> serde_json::Value {
        serde_json::json!({"name": "scout", "model": "m1"})
    }

    const CHECKSUM: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn rehydrate_round_trips_a_published_version() {
        let agent_id = AgentId::new();
        let origin = AgentVersionId::new();
        let published =
            AgentVersion::publish(agent_id, 3, CHECKSUM, sample_snapshot(), UserId::new())
                .expect("publish")
                .as_rollback_of(origin);
        let copy = AgentVersion::rehydrate(
            published.id(),
            published.agent_id(),
            published.version_number(),
            published.configuration_checksum(),
            published.configuration_snapshot().clone(),
            published.published_by(),
            published.published_at(),
            published.deployment_status(),
            published.rollback_of(),
        )
        .expect("rehydrate");
        assert_eq!(copy.id(), published.id());
        assert_eq!(copy.published_at(), published.published_at());
        assert_eq!(copy.version_number(), 3);
        assert_eq!(copy.rollback_of(), Some(origin));
        assert!(matches!(
            copy.deployment_status(),
            DeploymentStatus::NotDeployed
        ));
    }

    #[test]
    fn rehydrate_rejects_corrupt_rows() {
        assert!(AgentVersion::rehydrate(
            AgentVersionId::new(),
            AgentId::new(),
            0,
            CHECKSUM,
            sample_snapshot(),
            UserId::new(),
            Timestamp::epoch(),
            DeploymentStatus::NotDeployed,
            None,
        )
        .is_err());
        assert!(AgentVersion::rehydrate(
            AgentVersionId::new(),
            AgentId::new(),
            1,
            "not-a-checksum",
            sample_snapshot(),
            UserId::new(),
            Timestamp::epoch(),
            DeploymentStatus::NotDeployed,
            None,
        )
        .is_err());
        assert!(AgentVersion::rehydrate(
            AgentVersionId::new(),
            AgentId::new(),
            1,
            CHECKSUM,
            serde_json::json!("not an object"),
            UserId::new(),
            Timestamp::epoch(),
            DeploymentStatus::NotDeployed,
            None,
        )
        .is_err());
    }
}
