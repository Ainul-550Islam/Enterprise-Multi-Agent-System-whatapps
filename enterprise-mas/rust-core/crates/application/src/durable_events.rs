//! PURPOSE:
//! The **one** canonical event-ownership path for the durable execution
//! pipeline.
//!
//! Before this module, events were emitted from three different places (the
//! task dispatcher, the orchestration engine, and the worker settle path),
//! which produced duplicate `execution.*` events for a single lifecycle and
//! made it impossible to say which stream was authoritative. From here on,
//! every durable-pipeline event — and only durable-pipeline events — is built
//! by a function in this file, inside the same transaction as the state change
//! that caused it, and handed to the outbox.
//!
//! Layer / ownership
//! -----------------
//! `mas-application`. This module constructs [`OutboxRecord`]s; it does not
//! publish them. Publication is the relay's job, after commit.
//!
//! Production responsibility
//! -------------------------
//! * Every record carries: event id, event type, payload version, aggregate
//!   type/id, execution id, task id (when applicable), tenant, correlation id
//!   and creation timestamp.
//! * Payloads are **redaction-safe by construction**: they carry identifiers,
//!   statuses, versions and classified failure codes — never request bodies,
//!   model outputs, credentials or tokens.
//! * Event type strings and versions are constants, so a rename is a
//!   compile-time edit and consumers get a stable contract.
//!
//! What this module must NOT do
//! ----------------------------
//! * It must NOT publish, deliver or retry anything.
//! * It must NOT put raw input/output into a payload.
//! * It must NOT be bypassed: no other module in the pipeline may build an
//!   `execution.*` or `task.*` outbox record.

use mas_common::enums::{ExecutionStatus, TaskStatus};
use mas_common::ids::{ExecutionId, OrganizationId, ProjectId, TaskAttemptId, TaskId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_events::envelope::EventEnvelope;
use mas_events::outbox::OutboxRecord;
use serde_json::{json, Value};

use crate::durable::ExecutionUsage;

/// Payload version for every event built here. Bump only with a coordinated
/// consumer change; the value is part of the wire contract.
pub const EVENT_VERSION: u32 = 1;

/// `execution.submitted` — the execution and its first task are durable.
pub const EXECUTION_SUBMITTED: &str = "execution.submitted";
/// `execution.replayed` — an idempotent re-submission returned the original.
pub const EXECUTION_REPLAYED: &str = "execution.replayed";
/// `execution.started` — a worker claimed the execution's task and the run is
/// now owned.
pub const EXECUTION_STARTED: &str = "execution.started";
/// `execution.cancellation_requested` — cooperative stop flag set.
pub const EXECUTION_CANCELLATION_REQUESTED: &str = "execution.cancellation_requested";
/// `execution.cancelled` — terminal.
pub const EXECUTION_CANCELLED: &str = "execution.cancelled";
/// `execution.completed` — terminal success.
pub const EXECUTION_COMPLETED: &str = "execution.completed";
/// `execution.failed` — terminal failure (classified).
pub const EXECUTION_FAILED: &str = "execution.failed";

/// `task.created` — a durable task row exists for the execution.
pub const TASK_CREATED: &str = "task.created";
/// `task.claimed` — a worker took ownership and started an attempt.
pub const TASK_CLAIMED: &str = "task.claimed";
/// `task.succeeded` — terminal.
pub const TASK_SUCCEEDED: &str = "task.succeeded";
/// `task.failed` — non-terminal failure (retry may follow).
pub const TASK_FAILED: &str = "task.failed";
/// `task.dead_lettered` — terminal, out of attempts.
pub const TASK_DEAD_LETTERED: &str = "task.dead_lettered";
/// `task.cancelled` — terminal.
pub const TASK_CANCELLED: &str = "task.cancelled";

/// `usage.recorded` — authoritative meter for a completed attempt.
pub const USAGE_RECORDED: &str = "usage.recorded";

/// Everything an event needs that is not the event-specific payload.
#[derive(Debug, Clone)]
pub struct EventContext {
    pub tenant_id: TenantId,
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
    pub execution_id: ExecutionId,
    pub correlation_id: String,
    pub actor: String,
    pub occurred_at: Timestamp,
}

impl EventContext {
    /// Builds an envelope for `aggregate_type`/`aggregate_id`, filling in the
    /// shared identity every durable event carries.
    pub fn envelope(
        &self,
        event_type: &str,
        aggregate_type: &str,
        aggregate_id: &str,
        payload: Value,
    ) -> Result<EventEnvelope> {
        EventEnvelope::builder(
            event_type,
            EVENT_VERSION,
            aggregate_type,
            aggregate_id,
            self.tenant_id,
        )?
        .with_organization(self.organization_id)
        .with_correlation(self.correlation_id.clone())?
        .with_actor(self.actor.clone())
        .occurred_at(self.occurred_at)
        .with_payload(payload)
        .build()
    }

    /// Wraps an envelope into the outbox row that commits with the state change.
    pub fn outbox(&self, envelope: EventEnvelope) -> OutboxRecord {
        OutboxRecord::new(envelope)
    }

    /// Builds and wraps in one step.
    pub fn record(
        &self,
        event_type: &str,
        aggregate_type: &str,
        aggregate_id: &str,
        payload: Value,
    ) -> Result<OutboxRecord> {
        let envelope = self.envelope(event_type, aggregate_type, aggregate_id, payload)?;
        Ok(self.outbox(envelope))
    }
}

// ---------------------------------------------------------------------------
// Execution events
// ---------------------------------------------------------------------------

/// `execution.submitted`.
pub fn execution_submitted(
    ctx: &EventContext,
    task_id: TaskId,
    status: ExecutionStatus,
    version: u64,
    replayed: bool,
) -> Result<OutboxRecord> {
    ctx.record(
        if replayed {
            EXECUTION_REPLAYED
        } else {
            EXECUTION_SUBMITTED
        },
        "execution",
        &ctx.execution_id.to_string(),
        json!({
            "execution_id": ctx.execution_id,
            "task_id": task_id,
            "project_id": ctx.project_id,
            "status": status.as_str(),
            "version": version,
            "replayed": replayed,
        }),
    )
}

/// `execution.cancellation_requested` (a flag, not yet a status change).
pub fn execution_cancellation_requested(
    ctx: &EventContext,
    status: ExecutionStatus,
    version: u64,
    already_requested: bool,
) -> Result<OutboxRecord> {
    ctx.record(
        EXECUTION_CANCELLATION_REQUESTED,
        "execution",
        &ctx.execution_id.to_string(),
        json!({
            "execution_id": ctx.execution_id,
            "status": status.as_str(),
            "version": version,
            "already_requested": already_requested,
        }),
    )
}

/// `execution.started`.
pub fn execution_started(ctx: &EventContext, version: u64) -> Result<OutboxRecord> {
    ctx.record(
        EXECUTION_STARTED,
        "execution",
        &ctx.execution_id.to_string(),
        json!({
            "execution_id": ctx.execution_id,
            "status": ExecutionStatus::Running.as_str(),
            "version": version,
        }),
    )
}

/// `execution.cancelled`.
pub fn execution_cancelled(ctx: &EventContext, version: u64) -> Result<OutboxRecord> {
    ctx.record(
        EXECUTION_CANCELLED,
        "execution",
        &ctx.execution_id.to_string(),
        json!({
            "execution_id": ctx.execution_id,
            "status": ExecutionStatus::Cancelled.as_str(),
            "version": version,
        }),
    )
}

/// `execution.completed`. The output body is deliberately **not** included.
pub fn execution_completed(ctx: &EventContext, version: u64) -> Result<OutboxRecord> {
    ctx.record(
        EXECUTION_COMPLETED,
        "execution",
        &ctx.execution_id.to_string(),
        json!({
            "execution_id": ctx.execution_id,
            "status": ExecutionStatus::Completed.as_str(),
            "version": version,
        }),
    )
}

/// `execution.failed`. Carries the classified code and class, never prose detail.
pub fn execution_failed(
    ctx: &EventContext,
    code: &str,
    retryable: bool,
    version: u64,
) -> Result<OutboxRecord> {
    ctx.record(
        EXECUTION_FAILED,
        "execution",
        &ctx.execution_id.to_string(),
        json!({
            "execution_id": ctx.execution_id,
            "status": ExecutionStatus::Failed.as_str(),
            "version": version,
            "error_code": code,
            "error_class": if retryable { "transient" } else { "terminal" },
        }),
    )
}

// ---------------------------------------------------------------------------
// Task events
// ---------------------------------------------------------------------------

/// `task.created`.
pub fn task_created(
    ctx: &EventContext,
    task_id: TaskId,
    status: TaskStatus,
    version: u64,
) -> Result<OutboxRecord> {
    ctx.record(
        TASK_CREATED,
        "task",
        &task_id.to_string(),
        json!({
            "task_id": task_id,
            "execution_id": ctx.execution_id,
            "status": status.as_str(),
            "version": version,
        }),
    )
}

/// `task.claimed` — a worker now owns this task for `attempt_number`.
pub fn task_claimed(
    ctx: &EventContext,
    task_id: TaskId,
    attempt_id: TaskAttemptId,
    attempt_number: u32,
    worker: &str,
    version: u64,
) -> Result<OutboxRecord> {
    ctx.record(
        TASK_CLAIMED,
        "task",
        &task_id.to_string(),
        json!({
            "task_id": task_id,
            "execution_id": ctx.execution_id,
            "attempt_id": attempt_id,
            "attempt_number": attempt_number,
            "worker_id": worker,
            "version": version,
        }),
    )
}

/// `task.succeeded`.
pub fn task_succeeded(
    ctx: &EventContext,
    task_id: TaskId,
    attempt_id: Option<TaskAttemptId>,
    version: u64,
) -> Result<OutboxRecord> {
    ctx.record(
        TASK_SUCCEEDED,
        "task",
        &task_id.to_string(),
        json!({
            "task_id": task_id,
            "execution_id": ctx.execution_id,
            "attempt_id": attempt_id,
            "status": TaskStatus::Completed.as_str(),
            "version": version,
        }),
    )
}

/// `task.failed` (may still be retried).
pub fn task_failed(
    ctx: &EventContext,
    task_id: TaskId,
    attempt_id: Option<TaskAttemptId>,
    code: &str,
    retryable: bool,
    version: u64,
    requeued: bool,
) -> Result<OutboxRecord> {
    ctx.record(
        TASK_FAILED,
        "task",
        &task_id.to_string(),
        json!({
            "task_id": task_id,
            "execution_id": ctx.execution_id,
            "attempt_id": attempt_id,
            "status": TaskStatus::Failed.as_str(),
            "version": version,
            "error_code": code,
            "error_class": if retryable { "transient" } else { "terminal" },
            "requeued": requeued,
        }),
    )
}

/// `task.dead_lettered`.
pub fn task_dead_lettered(
    ctx: &EventContext,
    task_id: TaskId,
    version: u64,
) -> Result<OutboxRecord> {
    ctx.record(
        TASK_DEAD_LETTERED,
        "task",
        &task_id.to_string(),
        json!({
            "task_id": task_id,
            "execution_id": ctx.execution_id,
            "status": TaskStatus::DeadLettered.as_str(),
            "version": version,
        }),
    )
}

/// `task.cancelled`.
pub fn task_cancelled(ctx: &EventContext, task_id: TaskId, version: u64) -> Result<OutboxRecord> {
    ctx.record(
        TASK_CANCELLED,
        "task",
        &task_id.to_string(),
        json!({
            "task_id": task_id,
            "execution_id": ctx.execution_id,
            "status": TaskStatus::Cancelled.as_str(),
            "version": version,
        }),
    )
}

// ---------------------------------------------------------------------------
// Usage event
// ---------------------------------------------------------------------------

/// `usage.recorded` — one authoritative meter per completed attempt.
pub fn usage_recorded(
    ctx: &EventContext,
    task_id: TaskId,
    attempt_id: TaskAttemptId,
    usage: &ExecutionUsage,
) -> Result<OutboxRecord> {
    ctx.record(
        USAGE_RECORDED,
        "usage",
        &ctx.execution_id.to_string(),
        json!({
            "execution_id": ctx.execution_id,
            "task_id": task_id,
            "attempt_id": attempt_id,
            "tenant_id": ctx.tenant_id,
            "tokens_input": usage.tokens_input,
            "tokens_output": usage.tokens_output,
            "tool_calls": usage.tool_calls,
            "steps": usage.steps,
            "provider": usage.provider,
            "model": usage.model,
            "cost_micros": usage.cost_micros,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::{AgentId, WorkflowId};

    fn ctx() -> EventContext {
        EventContext {
            tenant_id: TenantId::new(),
            organization_id: OrganizationId::new(),
            project_id: ProjectId::new(),
            execution_id: ExecutionId::new(),
            correlation_id: "corr-1".to_owned(),
            actor: "worker:w-1".to_owned(),
            occurred_at: Timestamp::from_unix_seconds(1_700_000_000).expect("ts"),
        }
    }

    fn usage() -> ExecutionUsage {
        ExecutionUsage {
            provider: Some("openai".to_owned()),
            model: Some("gpt-4o-mini".to_owned()),
            tokens_input: 10,
            tokens_output: 20,
            tool_calls: 1,
            steps: 2,
            cost_micros: Some(1_500),
        }
    }

    #[test]
    fn every_event_carries_the_full_identity_set() {
        let context = ctx();
        let record =
            execution_submitted(&context, TaskId::new(), ExecutionStatus::Queued, 2, false)
                .expect("record");

        assert_eq!(record.envelope.event_version, EVENT_VERSION);
        assert_eq!(record.envelope.event_type, EXECUTION_SUBMITTED);
        assert_eq!(record.envelope.aggregate_type, "execution");
        assert_eq!(
            record.envelope.aggregate_id,
            context.execution_id.to_string()
        );
        assert_eq!(record.envelope.tenant_id, context.tenant_id);
        assert_eq!(
            record.envelope.organization_id,
            Some(context.organization_id)
        );
        assert_eq!(record.envelope.correlation_id, "corr-1");
        assert_eq!(record.envelope.actor.as_deref(), Some("worker:w-1"));
        assert_eq!(record.envelope.occurred_at, context.occurred_at);
        assert_eq!(record.event_id, record.envelope.id);
        assert_eq!(record.status, mas_common::enums::EventStatus::Pending);
        assert!(!record.envelope.payload.to_string().is_empty());
    }

    #[test]
    fn replay_switches_the_event_type_not_the_payload_shape() {
        let context = ctx();
        let fresh = execution_submitted(&context, TaskId::new(), ExecutionStatus::Queued, 2, false)
            .expect("fresh");
        let replay = execution_submitted(&context, TaskId::new(), ExecutionStatus::Queued, 2, true)
            .expect("replay");
        assert_eq!(fresh.envelope.event_type, EXECUTION_SUBMITTED);
        assert_eq!(replay.envelope.event_type, EXECUTION_REPLAYED);
        assert_eq!(replay.envelope.payload["replayed"], serde_json::json!(true));
    }

    #[test]
    fn failure_events_carry_a_code_and_a_class_never_prose() {
        let context = ctx();
        let record = execution_failed(&context, "rate_limited", true, 5).expect("record");
        assert_eq!(record.envelope.payload["error_code"], "rate_limited");
        assert_eq!(record.envelope.payload["error_class"], "transient");

        let terminal = execution_failed(&context, "unauthorized", false, 5).expect("record");
        assert_eq!(terminal.envelope.payload["error_class"], "terminal");
    }

    #[test]
    fn no_payload_contains_raw_output() {
        let context = ctx();
        let completed = execution_completed(&context, 4).expect("record");
        let rendered = completed.envelope.payload.to_string();
        assert!(
            !rendered.contains("output"),
            "the output body is never emitted"
        );
    }

    #[test]
    fn usage_events_carry_the_metering_contract() {
        let context = ctx();
        let record = usage_recorded(&context, TaskId::new(), TaskAttemptId::new(), &usage())
            .expect("record");
        assert_eq!(record.envelope.payload["tokens_input"], 10);
        assert_eq!(record.envelope.payload["tokens_output"], 20);
        assert_eq!(record.envelope.payload["provider"], "openai");
        assert_eq!(record.envelope.payload["model"], "gpt-4o-mini");
        assert_eq!(record.envelope.payload["cost_micros"], 1_500);
    }

    #[test]
    fn task_events_link_back_to_the_execution() {
        let context = ctx();
        let task_id = TaskId::new();
        let claimed =
            task_claimed(&context, task_id, TaskAttemptId::new(), 2, "w-9", 7).expect("record");
        assert_eq!(claimed.envelope.aggregate_type, "task");
        assert_eq!(claimed.envelope.aggregate_id, task_id.to_string());
        assert_eq!(
            claimed.envelope.payload["execution_id"],
            serde_json::json!(context.execution_id)
        );
        assert_eq!(claimed.envelope.payload["attempt_number"], 2);
        assert_eq!(claimed.envelope.payload["worker_id"], "w-9");
    }

    #[test]
    fn event_type_constants_are_unique_and_versioned() {
        let mut all = vec![
            EXECUTION_SUBMITTED,
            EXECUTION_REPLAYED,
            EXECUTION_STARTED,
            EXECUTION_CANCELLATION_REQUESTED,
            EXECUTION_CANCELLED,
            EXECUTION_COMPLETED,
            EXECUTION_FAILED,
            TASK_CREATED,
            TASK_CLAIMED,
            TASK_SUCCEEDED,
            TASK_FAILED,
            TASK_DEAD_LETTERED,
            TASK_CANCELLED,
            USAGE_RECORDED,
        ];
        let total = all.len();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), total, "event type constants must be unique");
        // A `const` block: the version is a compile-time fact, and asserting
        // it at runtime would be dead code the compiler has to keep.
        const { assert!(EVENT_VERSION >= 1) };
        let _ = (AgentId::new(), WorkflowId::new());
    }
}
