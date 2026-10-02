//! Audit recording for use-cases: a port every mutating service writes
//! through, so compliance output rides the application layer instead of
//! each process reinventing it. A failed audit record fails the use-case.

use async_trait::async_trait;
use mas_common::result::Result;
use mas_domain::{AuditActor, AuditEvent, AuditOutcome};
use std::sync::Mutex;

use crate::context::ServiceContext;

/// Destination for use-case audit events (append-only semantics belong to
/// the underlying store; the domain type enforces the shape).
#[async_trait]
pub trait AuditSinkPort: std::fmt::Debug + Send + Sync {
    /// Persists the event. Implementations must never mutate it.
    async fn record(&self, event: &AuditEvent) -> Result<()>;
}

/// Builds and records an audit event for one completed mutation. Actor,
/// scope and correlation id come from the service context.
pub async fn record_mutation(
    ctx: &ServiceContext,
    sink: &dyn AuditSinkPort,
    action: &str,
    resource_type: &str,
    resource_id: Option<String>,
    outcome: AuditOutcome,
) -> Result<()> {
    let AuditActor { .. } = &ctx.actor;
    let event = AuditEvent::new(
        ctx.tenant_id,
        ctx.organization_id,
        ctx.actor.clone(),
        action,
        resource_type,
        resource_id,
        outcome,
        ctx.correlation_id.clone(),
    )?;
    sink.record(&event).await
}

/// In-memory audit sink for tests and composition harnesses.
#[derive(Debug, Default)]
pub struct InMemoryAuditSink {
    events: Mutex<Vec<AuditEvent>>,
}

impl InMemoryAuditSink {
    /// Empty sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// All recorded events, oldest first.
    #[must_use]
    pub fn events(&self) -> Vec<AuditEvent> {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Number of recorded events.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Whether nothing has been recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl AuditSinkPort for InMemoryAuditSink {
    async fn record(&self, event: &AuditEvent) -> Result<()> {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(event.clone());
        Ok(())
    }
}

#[async_trait]
impl AuditSinkPort for std::sync::Arc<InMemoryAuditSink> {
    async fn record(&self, event: &AuditEvent) -> Result<()> {
        AuditSinkPort::record(self.as_ref(), event).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> ServiceContext {
        ServiceContext::system(
            "corr-audit-1",
            &Timestamp::from_unix_seconds(1_700_000_000).expect("ts"),
        )
        .expect("ctx")
    }

    use mas_common::timestamps::Timestamp;

    #[tokio::test]
    async fn mutations_are_recorded_with_context_scope() {
        let sink = InMemoryAuditSink::new();
        let scoped = ctx().with_scope(TenantId::new(), mas_common::ids::OrganizationId::new());

        record_mutation(
            &scoped,
            &sink,
            "tenant.register",
            "tenant",
            Some(scoped.tenant_id.expect("tenant").to_string()),
            AuditOutcome::Success,
        )
        .await
        .expect("record");

        record_mutation(
            &scoped,
            &sink,
            "quota.update",
            "quota",
            None,
            AuditOutcome::Success,
        )
        .await
        .expect("record");

        let events = sink.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].action, "tenant.register");
        assert_eq!(events[0].correlation_id, "corr-audit-1");
        assert_eq!(events[0].tenant_id, scoped.tenant_id);
        assert!(events[1].resource_id.is_none());
        assert!(sink.len() == 2 && !sink.is_empty());
    }

    use mas_common::ids::TenantId;
}
