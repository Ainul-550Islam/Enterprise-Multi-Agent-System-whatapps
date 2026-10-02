//! Bridges append-only `domain::AuditEvent`s onto the log/metrics planes.
//!
//! The audit *record* itself is authoritative and lives in the append-only
//! audit store; this module only emits derived telemetry (one redacted JSON
//! log line + counters), so operators and SIEM-style shippers ride the same
//! observability wiring without ever touching raw audit payloads.

use mas_common::enums::AuditSeverity;
use mas_common::result::Result;
use mas_domain::{AuditActor, AuditComplianceClass, AuditEvent, AuditOutcome};
use serde_json::Value;

use crate::logging::{JsonLogger, LogEvent, LogLevel, LogSinkPort};
use crate::metrics::{LabelSet, MetricsRegistry};

/// Family counting audit events by outcome × compliance class.
pub const AUDIT_EVENTS_METRIC: &str = "mas_audit_events_total";
/// Family counting `Denied` outcomes by resource type (bounded domain).
pub const AUDIT_DENIED_METRIC: &str = "mas_audit_denied_total";

/// Maps an audit outcome + severity onto one log level. `Denied` is an
/// operator signal (Warn), `Failure` an Error; High/Critical severities
/// always escalate to Error.
#[must_use]
pub fn audit_log_level(outcome: &AuditOutcome, severity: &AuditSeverity) -> LogLevel {
    let severity_floor = match severity {
        AuditSeverity::Info | AuditSeverity::Notice => LogLevel::Info,
        AuditSeverity::Warning => LogLevel::Warn,
        AuditSeverity::High | AuditSeverity::Critical => LogLevel::Error,
    };
    let outcome_floor = match outcome {
        AuditOutcome::Success => LogLevel::Info,
        AuditOutcome::Denied => LogLevel::Warn,
        AuditOutcome::Failure => LogLevel::Error,
    };
    if outcome_floor.rank() >= severity_floor.rank() {
        outcome_floor
    } else {
        severity_floor
    }
}

/// Emits both signals (log line + counters) for one audit event.
#[derive(Debug)]
pub struct TelemetryAuditor<L: LogSinkPort> {
    logger: JsonLogger<L>,
}

impl<L: LogSinkPort> TelemetryAuditor<L> {
    /// Builds over a configured JSON logger.
    #[must_use]
    pub const fn new(logger: JsonLogger<L>) -> Self {
        Self { logger }
    }

    /// The underlying logger.
    #[must_use]
    pub const fn logger(&self) -> &JsonLogger<L> {
        &self.logger
    }

    /// Emits one redacted JSON log line describing `event`. Metadata is
    /// merged under the `audit.meta.` prefix and redacted by the logger
    /// pipeline like any other field.
    pub async fn emit(&self, event: &AuditEvent) -> Result<bool> {
        let level = audit_log_level(&event.outcome, &event.severity);
        let at = event.occurred_at;
        let message = format!(
            "audit {} {} {}",
            event.action,
            event.resource_type,
            event.outcome.as_str()
        );
        let mut log_event = LogEvent::new(level, "mas.audit", &message, &at)?
            .field("audit.id", Value::from(event.id.to_string()))?
            .field("audit.action", Value::from(event.action.clone()))?
            .field(
                "audit.resource.type",
                Value::from(event.resource_type.clone()),
            )?
            .field(
                "audit.outcome",
                Value::from(event.outcome.as_str().to_owned()),
            )?
            .field(
                "audit.severity",
                Value::from(event.severity.as_str().to_owned()),
            )?
            .field(
                "audit.class",
                Value::from(class_label(&event.compliance_class)),
            )?
            .field(
                "audit.actor.kind",
                Value::from(actor_kind_label(&event.actor)),
            )?
            .field("audit.actor.id", Value::from(actor_id(&event.actor)))?
            .field("correlation_id", Value::from(event.correlation_id.clone()))?;
        if let Some(resource_id) = &event.resource_id {
            log_event = log_event.field("audit.resource.id", Value::from(resource_id.clone()))?;
        }
        if let Some(tenant) = event.tenant_id {
            log_event = log_event.field("tenant_id", Value::from(tenant.to_string()))?;
        }
        if let Some(org) = event.organization_id {
            log_event = log_event.field("organization_id", Value::from(org.to_string()))?;
        }
        for (key, value) in &event.metadata {
            let mut prefixed = String::with_capacity(key.len() + 11);
            prefixed.push_str("audit.meta.");
            prefixed.push_str(&sanitize_metadata_key(key));
            // Metadata keys are free-form; invalid ones are normalized to
            // keep the event emittable (values are redacted downstream).
            if let Ok(updated) = log_event.clone().field(&prefixed, value.clone()) {
                log_event = updated;
            }
        }
        self.logger.log(&log_event).await
    }

    /// Records the audit counters for `event` on `registry`.
    pub fn record_metrics(&self, registry: &MetricsRegistry, event: &AuditEvent) -> Result<()> {
        register_audit_metrics(registry, event)
    }

    /// Convenience: log line + counters in one call.
    pub async fn emit_and_record(
        &self,
        registry: &MetricsRegistry,
        event: &AuditEvent,
    ) -> Result<bool> {
        self.record_metrics(registry, event)?;
        self.emit(event).await
    }
}

/// Increments the standard audit counters for `event`.
pub fn register_audit_metrics(registry: &MetricsRegistry, event: &AuditEvent) -> Result<()> {
    let class = class_label(&event.compliance_class);
    let labels = LabelSet::build([
        ("outcome", event.outcome.as_str()),
        ("class", class.as_str()),
    ])?;
    registry.inc_counter(AUDIT_EVENTS_METRIC, &labels, 1)?;
    if matches!(event.outcome, AuditOutcome::Denied) {
        let denied_labels =
            LabelSet::build([("resource_type", event.resource_type.clone().leak())])?;
        registry.inc_counter(AUDIT_DENIED_METRIC, &denied_labels, 1)?;
    }
    Ok(())
}

/// Stable metric/log label for the compliance class.
#[must_use]
pub fn class_label(class: &AuditComplianceClass) -> String {
    class.as_str().to_owned()
}

fn actor_kind_label(actor: &AuditActor) -> String {
    actor.kind.as_str().to_owned()
}

fn actor_id(actor: &AuditActor) -> String {
    actor.id.clone()
}

/// Audit metadata keys are free-form: normalize them to the log field key
/// alphabet (lowercase alnum + `.`/`_`/`-`, `_` elsewhere).
fn sanitize_metadata_key(raw: &str) -> String {
    raw.chars()
        .map(|c| {
            let lowered = c.to_ascii_lowercase();
            if lowered.is_ascii_alphanumeric() || matches!(lowered, '.' | '_' | '-') {
                lowered
            } else {
                '_'
            }
        })
        .take(64)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging::InMemoryLogSink;
    use crate::metrics::MetricsRegistry;
    use mas_common::ids::TenantId;
    use mas_domain::AuditActorKind;
    use serde_json::Map;

    fn sample_event(outcome: AuditOutcome) -> AuditEvent {
        let mut metadata = Map::new();
        metadata.insert(
            "reason".to_owned(),
            Value::from("policy p-12 denies agent.publish"),
        );
        let mut event = AuditEvent::new(
            Some(TenantId::new()),
            None,
            AuditActor::new(AuditActorKind::Service, app_actor_id()).expect("actor"),
            "agent.publish",
            "agent",
            Some("agent-42".to_owned()),
            outcome,
            "corr-20260930-0001",
        )
        .expect("audit event");
        event.metadata = metadata;
        event
    }

    fn app_actor_id() -> String {
        "svc-agent-runtime@mas.internal".to_owned()
    }

    #[tokio::test]
    async fn outcomes_and_severities_drive_levels() {
        assert_eq!(
            audit_log_level(&AuditOutcome::Success, &AuditSeverity::Info),
            LogLevel::Info
        );
        assert_eq!(
            audit_log_level(&AuditOutcome::Denied, &AuditSeverity::Notice),
            LogLevel::Warn
        );
        assert_eq!(
            audit_log_level(&AuditOutcome::Failure, &AuditSeverity::Info),
            LogLevel::Error
        );
        assert_eq!(
            audit_log_level(&AuditOutcome::Success, &AuditSeverity::Critical),
            LogLevel::Error
        );
        assert_eq!(
            audit_log_level(&AuditOutcome::Denied, &AuditSeverity::High),
            LogLevel::Error
        );
    }

    #[tokio::test]
    async fn emission_enriches_redacts_and_respects_metadata() {
        let auditor =
            TelemetryAuditor::new(JsonLogger::new(InMemoryLogSink::new(), LogLevel::Info));
        let event = sample_event(AuditOutcome::Denied);
        assert!(
            auditor.emit(&event).await.expect("emit"),
            "Denied maps to Warn ≥ Info floor"
        );

        let lines = auditor.logger().sink().lines();
        assert_eq!(lines.len(), 1);
        let parsed: Value = serde_json::from_str(&lines[0]).expect("json");
        assert_eq!(parsed["level"], Value::from("warn"));
        assert_eq!(parsed["audit.action"], Value::from("agent.publish"));
        assert_eq!(parsed["audit.outcome"], Value::from("denied"));
        assert_eq!(parsed["audit.actor.kind"], Value::from("service"));
        assert_eq!(parsed["audit.resource.id"], Value::from("agent-42"));
        assert_eq!(parsed["correlation_id"], Value::from("corr-20260930-0001"));
        assert_eq!(
            parsed["audit.meta.reason"],
            Value::from("policy p-12 denies agent.publish"),
            "metadata merged under the audit.meta prefix"
        );
        assert!(parsed["tenant_id"].is_string());
    }

    #[tokio::test]
    async fn metrics_count_outcomes_classes_and_denials() {
        let registry = MetricsRegistry::new();
        let auditor =
            TelemetryAuditor::new(JsonLogger::new(InMemoryLogSink::new(), LogLevel::Info));
        auditor
            .record_metrics(&registry, &sample_event(AuditOutcome::Success))
            .expect("record");
        auditor
            .record_metrics(&registry, &sample_event(AuditOutcome::Denied))
            .expect("record");
        auditor
            .record_metrics(&registry, &sample_event(AuditOutcome::Denied))
            .expect("record");

        let success =
            LabelSet::build([("outcome", "success"), ("class", "general")]).expect("labels");
        // Domain rule: `Denied` outcomes are classified Security by
        // `AuditEvent::new` — assert against that classification.
        let denied =
            LabelSet::build([("outcome", "denied"), ("class", "security")]).expect("labels");
        assert_eq!(
            registry.counter_value(crate::audit_telemetry::AUDIT_EVENTS_METRIC, &success),
            1
        );
        assert_eq!(registry.counter_value(AUDIT_EVENTS_METRIC, &denied), 2);
        let denied_resource = LabelSet::build([("resource_type", "agent")]).expect("labels");
        assert_eq!(
            registry.counter_value(AUDIT_DENIED_METRIC, &denied_resource),
            2
        );

        let rendered = registry.render_prometheus();
        assert!(
            rendered.contains("mas_audit_events_total{class=\"security\",outcome=\"denied\"} 2")
        );
    }

    #[tokio::test]
    async fn freeform_metadata_keys_are_normalized_not_fatal() {
        let auditor =
            TelemetryAuditor::new(JsonLogger::new(InMemoryLogSink::new(), LogLevel::Info));
        let mut event = sample_event(AuditOutcome::Success);
        let mut metadata = Map::new();
        metadata.insert("Weird Key/With Spaces".to_owned(), Value::from(7));
        event.metadata = metadata;
        auditor.emit(&event).await.expect("emit");
        let parsed: Value =
            serde_json::from_str(&auditor.logger().sink().lines()[0]).expect("json");
        assert_eq!(parsed["audit.meta.weird_key_with_spaces"], Value::from(7));
    }
}
