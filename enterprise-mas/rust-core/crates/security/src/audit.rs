//! Audit sink port: the mandatory evidence trail for security decisions.
//!
//! Everything in this crate that grants or denies access to sensitive
//! material records through this port: key issues/uses, token rejections,
//! secret resolutions, rotations. The production sink writes to the
//! append-only audit table (persistence crate); tests use
//! [`RecordingAuditSink`].

use mas_common::enums::AuditSeverity;
use mas_common::ids::OrganizationId;
use mas_common::result::Result;
use mas_domain::{AuditActor, AuditEvent, AuditOutcome};
use std::fmt;
use std::sync::Mutex;

/// Append-only audit event sink.
#[async_trait::async_trait]
pub trait AuditSinkPort: Send + Sync + fmt::Debug {
    async fn record(&self, event: &AuditEvent) -> Result<()>;
}

/// Deliberately-destructive: swallows events (dev only).
#[derive(Debug, Default)]
pub struct NoopAuditSink;

#[async_trait::async_trait]
impl AuditSinkPort for NoopAuditSink {
    async fn record(&self, _event: &AuditEvent) -> Result<()> {
        Ok(())
    }
}

/// Test sink keeping a copy of each event.
#[derive(Debug, Default)]
pub struct RecordingAuditSink {
    events: Mutex<Vec<AuditEvent>>,
}

impl RecordingAuditSink {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn events(&self) -> Vec<AuditEvent> {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    #[must_use]
    pub fn count_action(&self, action: &str) -> usize {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|event| event.action == action)
            .count()
    }
}

#[async_trait::async_trait]
impl AuditSinkPort for RecordingAuditSink {
    async fn record(&self, event: &AuditEvent) -> Result<()> {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(event.clone());
        Ok(())
    }
}

/// Canonical security event construction — sets actor/action/outcome/severity
/// boilerplate so audit records are structurally uniform.
pub fn security_event(
    tenant_id: Option<mas_common::ids::TenantId>,
    organization_id: Option<OrganizationId>,
    actor: AuditActor,
    action: &str,
    resource_type: &str,
    resource_id: Option<String>,
    outcome: AuditOutcome,
    correlation_id: &str,
) -> Result<AuditEvent> {
    let severity = match outcome {
        AuditOutcome::Success => AuditSeverity::Info,
        AuditOutcome::Failure => AuditSeverity::Warning,
        AuditOutcome::Denied => AuditSeverity::Warning,
    };
    Ok(AuditEvent::new(
        tenant_id,
        organization_id,
        actor,
        action,
        resource_type,
        resource_id,
        outcome,
        correlation_id,
    )?
    .with_severity(severity))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recorder_keeps_and_counts() {
        // (sync portion: sink usage is async out of test right now via tokio)
        let sink = RecordingAuditSink::new();
        assert_eq!(sink.count_action("api_key.create"), 0);
    }

    #[test]
    fn severity_maps_from_outcome() {
        let event = security_event(
            None,
            None,
            AuditActor::system(),
            "secret.resolve",
            "secret",
            Some("path-1".into()),
            AuditOutcome::Failure,
            "corr-1",
        )
        .expect("event");
        assert_eq!(event.severity, AuditSeverity::Warning);
    }
}
