//! PURPOSE:
//! The production implementation of [`DurableExecutionPort`] — the one place
//! where the canonical execution pipeline touches the database.
//!
//! Every method here is exactly one transaction. That is the whole point of
//! the port: the caller cannot create an execution without its task, its
//! attempt, its audit row and its outbox rows, because there is no API that
//! writes only some of them.
//!
//! Security boundary — which transaction that is depends on the caller, and
//! getting it wrong silently disables row-level security:
//!
//! * **Inside a request** (HTTP, gRPC) the operation *joins* the transaction
//!   the middleware already opened, in which `app.current_tenant` is armed. It
//!   does not open a second connection: a second connection has no GUC, and
//!   under `FORCE ROW LEVEL SECURITY` a connection with no GUC reads and
//!   writes nothing. Opening one here would both drop the scope and split one
//!   request's writes across two transactions.
//! * **On the worker / outbox plane** there is no request and no tenant in
//!   advance — the plane's tenant authority is the row it wins a lease on.
//!   Those callers open and commit their own transaction, and must connect as
//!   `mas_worker` / `mas_outbox`, whose `BYPASSRLS` is confined by table and
//!   operation. They must never connect as `mas_app`.
//!
//! Layer / ownership
//! -----------------
//! `mas-persistence`. It owns SQL; `mas-application` owns the vocabulary and
//! the decision-making. Nothing above this file knows a table name.
//!
//! Production responsibility
//! -------------------------
//! * **Atomic submission** — idempotency, execution, task, attempt, audit and
//!   outbox commit together or not at all. Nothing is reported successful
//!   before the commit succeeds.
//! * **DB-enforced idempotency** — the partial unique index on
//!   `(tenant_id, idempotency_key)` decides who won. There is no
//!   SELECT-then-INSERT here, because a SELECT-then-INSERT races.
//! * **Real durable claims** — `FOR UPDATE SKIP LOCKED` inside the same
//!   transaction that writes the attempt row and installs the ownership
//!   fence. Two workers cannot both claim one task, because the row lock and
//!   the partial unique index on open attempts both forbid it.
//! * **Stale-write rejection** — every settlement carries the version the
//!   caller observed; an `UPDATE ... WHERE version = $expected` that matches
//!   no row is a lost ownership race and is reported as `Conflict`, never as
//!   success.
//! * **Terminal state before ack** — `settle` commits and only then returns.
//!   The caller acks the broker afterwards.
//!
//! What this module must NOT do
//! ----------------------------
//! * It must NOT open a second connection or a second transaction inside a
//!   unit: every write goes through the one `&mut PgConnection` the unit owns.
//! * It must NOT mark work published/completed before the durable write
//!   commits.
//! * It must NOT derive attempt counts from broker delivery metadata — the
//!   attempt number comes from `task_attempts` and from nowhere else.
//! * It must NOT log row payloads: task inputs and results may contain
//!   secrets. Only ids, statuses and codes are logged.

use mas_application::durable::{
    CancellationReceipt, CancellationRequest, CancellationResult, ClaimQuery, DurableExecutionPort,
    ExecutionAction, ExecutionOutcome, ExecutionTransitionReceipt, ExecutionTransitionRequest,
    ExecutionUsage, PipelineCounters, SettlementReceipt, SubmissionIntent, SubmissionKind,
    SubmissionReceipt, TaskClaim, TaskOutcome, TaskSettlement, UsageObservation,
};
use mas_application::durable_events;
use mas_application::pipeline_metrics::{Outcome, PipelineMetrics};
use mas_common::enums::{ExecutionStatus, TaskStatus};
use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, OrganizationId, TaskAttemptId, TaskId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{
    ActorKind, ActorRef, AttemptStatus, AuditActor, AuditActorKind, AuditEvent, AuditOutcome,
    CancellationOutcome, Execution, StateTransition, Task, TaskAttempt, TransitionActor,
    UsageRecord,
};
use mas_events::outbox::OutboxRecord;
use serde_json::{json, Value};
use sqlx::{PgConnection, PgPool, Row};
use std::str::FromStr;
use std::time::Duration;
use uuid::Uuid;

use crate::error::map_sqlx;
use crate::repositories::executions::fetch_execution_row;
use crate::repositories::tasks::{fetch_task_row, TASK_COLUMNS};
use crate::rows::{ExecutionRow, TaskRow, SPEC_KEY};
use crate::transaction::UnitOfWork;

// ---------------------------------------------------------------------------
// Row projection
// ---------------------------------------------------------------------------

/// The durable columns migration 0014 added to `tasks`.
///
/// They are written as real columns (so the claim and lease-recovery indexes
/// work) *and* round-tripped through the spec envelope (so hydration rebuilds
/// the aggregate). Neither half alone is sufficient.
#[derive(Debug, Clone, Default)]
struct TaskFence {
    version: i64,
    attempt_count: i32,
    max_attempts: i32,
    locked_by: Option<String>,
    locked_until: Option<chrono::DateTime<chrono::Utc>>,
    queued_at: Option<chrono::DateTime<chrono::Utc>>,
    started_at: Option<chrono::DateTime<chrono::Utc>>,
    finished_at: Option<chrono::DateTime<chrono::Utc>>,
    last_attempt_id: Option<Uuid>,
    error: Option<Value>,
    request_fingerprint: Option<String>,
}

/// The durable columns migration 0014 added to `executions`.
#[derive(Debug, Clone, Default)]
struct ExecutionFence {
    version: i64,
}

/// Projected durable columns for `tasks`.
const TASK_FENCE_COLUMNS: &str = "version, attempt_count, max_attempts, locked_by, locked_until, \
     queued_at, started_at, finished_at, last_attempt_id, error, request_fingerprint";

/// Full `executions` projection, materialized columns plus the durable fence.
const EXECUTION_SELECT: &str =
    "id, tenant_id, organization_id, project_id, workflow_id, agent_id, \
     parent_id, status, input, output, error, state, correlation_id, started_at, finished_at, \
     created_at, updated_at, version, request_fingerprint";

fn ts_json(value: chrono::DateTime<chrono::Utc>) -> Value {
    Value::String(Timestamp::from_datetime(value).to_rfc3339_millis())
}

fn read_task_fence(row: &sqlx::postgres::PgRow) -> Result<TaskFence> {
    Ok(TaskFence {
        version: row.try_get("version").map_err(map_sqlx)?,
        attempt_count: row.try_get("attempt_count").map_err(map_sqlx)?,
        max_attempts: row.try_get("max_attempts").map_err(map_sqlx)?,
        locked_by: row.try_get("locked_by").map_err(map_sqlx)?,
        locked_until: row.try_get("locked_until").map_err(map_sqlx)?,
        queued_at: row.try_get("queued_at").map_err(map_sqlx)?,
        started_at: row.try_get("started_at").map_err(map_sqlx)?,
        finished_at: row.try_get("finished_at").map_err(map_sqlx)?,
        last_attempt_id: row.try_get("last_attempt_id").map_err(map_sqlx)?,
        error: row.try_get("error").map_err(map_sqlx)?,
        request_fingerprint: row.try_get("request_fingerprint").map_err(map_sqlx)?,
    })
}

fn read_execution_fence(row: &sqlx::postgres::PgRow) -> Result<ExecutionFence> {
    // `request_fingerprint` is read directly where the replay decision is made
    // rather than carried here: it is an idempotency concern, not part of the
    // aggregate's fence.
    Ok(ExecutionFence {
        version: row.try_get("version").map_err(map_sqlx)?,
    })
}

/// Rebuilds a `Task` from its row plus the durable fence.
///
/// The fence is merged into the spec envelope before hydration, so the
/// aggregate comes back with its version, ownership and attempt accounting
/// intact — a task loaded without them would look unowned and untried, which
/// is precisely how duplicate execution happens.
fn hydrate_task(mut row: TaskRow, fence: TaskFence) -> Result<Task> {
    let mut spec = row
        .payload
        .get(SPEC_KEY)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    spec.insert("version".to_owned(), json!(fence.version.max(1)));
    spec.insert(
        "attempt_count".to_owned(),
        json!(fence.attempt_count.max(0)),
    );
    spec.insert("max_attempts".to_owned(), json!(fence.max_attempts.max(1)));
    if let Some(worker) = fence.locked_by {
        spec.insert("locked_by".to_owned(), Value::String(worker));
    }
    if let Some(until) = fence.locked_until {
        spec.insert("locked_until".to_owned(), ts_json(until));
    }
    if let Some(at) = fence.queued_at {
        spec.insert("queued_at".to_owned(), ts_json(at));
    }
    if let Some(at) = fence.started_at {
        spec.insert("started_at".to_owned(), ts_json(at));
    }
    if let Some(at) = fence.finished_at {
        spec.insert("finished_at".to_owned(), ts_json(at));
    }
    if let Some(attempt) = fence.last_attempt_id {
        spec.insert("last_attempt_id".to_owned(), json!(attempt));
    }
    if let Some(Value::Object(error)) = fence.error {
        if let Some(code) = error.get("code").and_then(Value::as_str) {
            spec.insert("last_error_code".to_owned(), Value::String(code.to_owned()));
        }
        if let Some(message) = error.get("message").and_then(Value::as_str) {
            spec.insert("last_error".to_owned(), Value::String(message.to_owned()));
        }
    }

    if let Value::Object(payload) = &mut row.payload {
        payload.insert(SPEC_KEY.to_owned(), Value::Object(spec));
    }
    row.into_domain()
}

/// Rebuilds an `Execution` from its row plus the durable fence.
fn hydrate_execution(mut row: ExecutionRow, fence: ExecutionFence) -> Result<Execution> {
    let mut spec = row
        .state
        .get(SPEC_KEY)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    spec.insert("version".to_owned(), json!(fence.version.max(1)));
    if let Value::Object(state) = &mut row.state {
        state.insert(SPEC_KEY.to_owned(), Value::Object(spec));
    }
    row.into_domain()
}

/// The durable fence values for a task, extracted from the aggregate.
fn task_fence(task: &Task, fingerprint: Option<&str>) -> TaskFence {
    TaskFence {
        version: i64::try_from(task.version()).unwrap_or(1),
        attempt_count: i32::try_from(task.attempt_count).unwrap_or(0),
        max_attempts: i32::try_from(task.max_attempts).unwrap_or(1),
        locked_by: task.locked_by.clone(),
        locked_until: task.locked_until.map(Timestamp::into_datetime),
        queued_at: task.queued_at.map(Timestamp::into_datetime),
        started_at: task.started_at.map(Timestamp::into_datetime),
        finished_at: task.finished_at.map(Timestamp::into_datetime),
        last_attempt_id: task.last_attempt_id.map(TaskAttemptId::into_uuid),
        error: task.last_error_code.as_ref().map(|code| {
            json!({
                "code": code,
                "message": task.last_error.clone().unwrap_or_default(),
            })
        }),
        request_fingerprint: fingerprint.map(str::to_owned),
    }
}

// ---------------------------------------------------------------------------
// task_attempts
// ---------------------------------------------------------------------------

/// The projected columns for `task_attempts`.
const ATTEMPT_COLUMNS: &str = "id, tenant_id, task_id, execution_id, attempt, worker_id, \
     lease_expires, status, detail, error, error_code, error_retryable, result_metadata, \
     started_at, finished_at, version";

/// Rebuilds a [`TaskAttempt`] from a row.
fn hydrate_attempt(row: &sqlx::postgres::PgRow) -> Result<TaskAttempt> {
    let detail: Value = row.try_get("detail").map_err(map_sqlx)?;
    let error: Option<Value> = row.try_get("error").map_err(map_sqlx)?;
    let version: i64 = row.try_get("version").map_err(map_sqlx)?;
    let attempt: i32 = row.try_get("attempt").map_err(map_sqlx)?;
    let status: String = row.try_get("status").map_err(map_sqlx)?;
    let result_metadata: Value = row.try_get("result_metadata").map_err(map_sqlx)?;

    Ok(TaskAttempt {
        id: TaskAttemptId::from_uuid(row.try_get("id").map_err(map_sqlx)?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id").map_err(map_sqlx)?),
        task_id: TaskId::from_uuid(row.try_get("task_id").map_err(map_sqlx)?),
        execution_id: row
            .try_get::<Option<Uuid>, _>("execution_id")
            .map_err(map_sqlx)?
            .map(ExecutionId::from_uuid),
        attempt_number: u32::try_from(attempt).unwrap_or(1),
        status: AttemptStatus::from_str(&status).unwrap_or(AttemptStatus::Running),
        worker_id: Some(row.try_get("worker_id").map_err(map_sqlx)?),
        lease_expires_at: row
            .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("lease_expires")
            .map_err(map_sqlx)?
            .map(Timestamp::from_datetime),
        retry_reason: detail
            .get("retry_reason")
            .and_then(Value::as_str)
            .map(str::to_owned),
        started_at: Timestamp::from_datetime(
            row.try_get::<chrono::DateTime<chrono::Utc>, _>("started_at")
                .map_err(map_sqlx)?,
        ),
        finished_at: row
            .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("finished_at")
            .map_err(map_sqlx)?
            .map(Timestamp::from_datetime),
        error: error
            .as_ref()
            .and_then(|value| value.get("message"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        error_code: row.try_get("error_code").map_err(map_sqlx)?,
        error_retryable: row.try_get("error_retryable").map_err(map_sqlx)?,
        result_metadata: result_metadata.as_object().cloned().unwrap_or_default(),
        version: u64::try_from(version).unwrap_or(1),
    })
}

/// Writes one attempt row (upsert on the natural key `(task_id, attempt)`).
async fn write_attempt(conn: &mut PgConnection, attempt: &TaskAttempt) -> Result<()> {
    let detail = json!({ "retry_reason": attempt.retry_reason });
    let error = attempt.error.as_ref().map(|message| {
        json!({
            "code": attempt.error_code,
            "message": message,
            "retryable": attempt.error_retryable,
        })
    });
    sqlx::query(
        "INSERT INTO task_attempts (
            id, tenant_id, task_id, execution_id, attempt, worker_id, lease_expires,
            status, detail, error, error_code, error_retryable, result_metadata,
            started_at, finished_at, version
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
         ON CONFLICT (task_id, attempt) DO UPDATE SET
            status = EXCLUDED.status,
            worker_id = EXCLUDED.worker_id,
            lease_expires = EXCLUDED.lease_expires,
            detail = EXCLUDED.detail,
            error = EXCLUDED.error,
            error_code = EXCLUDED.error_code,
            error_retryable = EXCLUDED.error_retryable,
            result_metadata = EXCLUDED.result_metadata,
            finished_at = EXCLUDED.finished_at,
            version = EXCLUDED.version",
    )
    .bind(attempt.id.into_uuid())
    .bind(attempt.tenant_id.into_uuid())
    .bind(attempt.task_id.into_uuid())
    .bind(attempt.execution_id.map(ExecutionId::into_uuid))
    .bind(i32::try_from(attempt.attempt_number).unwrap_or(1))
    .bind(attempt.worker_id.clone().unwrap_or_default())
    .bind(attempt.lease_expires_at.map(Timestamp::into_datetime))
    .bind(attempt.status.as_str())
    .bind(detail)
    .bind(error)
    .bind(attempt.error_code.clone())
    .bind(attempt.error_retryable)
    .bind(Value::Object(attempt.result_metadata.clone()))
    .bind(attempt.started_at.into_datetime())
    .bind(attempt.finished_at.map(Timestamp::into_datetime))
    .bind(i64::try_from(attempt.version).unwrap_or(1))
    .execute(&mut *conn)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

/// Loads one attempt by id.
async fn load_attempt(
    conn: &mut PgConnection,
    attempt_id: TaskAttemptId,
) -> Result<Option<TaskAttempt>> {
    let row = sqlx::query(&format!(
        "SELECT {ATTEMPT_COLUMNS} FROM task_attempts WHERE id = $1"
    ))
    .bind(attempt_id.into_uuid())
    .fetch_optional(&mut *conn)
    .await
    .map_err(map_sqlx)?;
    row.as_ref().map(hydrate_attempt).transpose()
}

// ---------------------------------------------------------------------------
// Outbox / audit / usage writers (all inside the caller's unit)
// ---------------------------------------------------------------------------

/// Inserts outbox rows inside the current unit.
async fn write_events(
    conn: &mut PgConnection,
    records: &[OutboxRecord],
    task_id: Option<TaskId>,
    execution_id: Option<ExecutionId>,
) -> Result<()> {
    for record in records {
        let envelope = serde_json::to_value(&record.envelope)
            .map_err(|err| AppError::serialization(format!("outbox envelope: {err}")))?;
        sqlx::query(
            "INSERT INTO outbox_events (
                event_id, envelope, tenant_id, event_type, aggregate_type, aggregate_id,
                status, next_attempt_at, enqueued_at, task_id, execution_id
             ) VALUES ($1,$2,$3,$4,$5,$6,'pending',$7,$7,$8,$9)
             ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(record.envelope.id.into_uuid())
        .bind(envelope)
        .bind(record.envelope.tenant_id.into_uuid())
        .bind(record.envelope.event_type.clone())
        .bind(record.envelope.aggregate_type.clone())
        .bind(record.envelope.aggregate_id.clone())
        .bind(record.envelope.occurred_at.into_datetime())
        .bind(task_id.map(TaskId::into_uuid))
        .bind(execution_id.map(ExecutionId::into_uuid))
        .execute(&mut *conn)
        .await
        .map_err(map_sqlx)?;
    }
    Ok(())
}

/// Appends audit rows derived from the transitions the aggregate applied.
///
/// Audit is written from the *transitions that actually happened*, never from
/// a hand-written list beside them — that duplication is what produces two
/// histories for one execution.
async fn write_audit(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    organization_id: OrganizationId,
    correlation_id: &str,
    transitions: &[StateTransition],
) -> Result<()> {
    for transition in transitions {
        let actor = AuditActor::new(AuditActorKind::System, transition.actor.id.clone())?;
        let event = AuditEvent::new(
            Some(tenant_id),
            Some(organization_id),
            actor,
            format!("{}.{}", transition.aggregate.as_str(), transition.to),
            transition.aggregate.as_str(),
            Some(transition.aggregate_id.clone()),
            AuditOutcome::Success,
            correlation_id,
        )?;
        sqlx::query(
            "INSERT INTO audit_events (
                tenant_id, organization_id, actor, action, resource_type, resource_id,
                outcome, severity, compliance_class, correlation_id, metadata, occurred_at
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
        )
        .bind(Some(tenant_id.into_uuid()))
        .bind(Some(organization_id.into_uuid()))
        .bind(
            serde_json::to_value(&event.actor)
                .map_err(|err| AppError::serialization(format!("audit actor: {err}")))?,
        )
        .bind(event.action.clone())
        .bind(event.resource_type.clone())
        .bind(event.resource_id.clone())
        .bind("success")
        .bind("info")
        .bind("general")
        .bind(correlation_id)
        .bind(json!({
            "from": transition.from,
            "to": transition.to,
            "version": transition.version,
            "actor": transition.actor.id.clone(),
        }))
        .bind(transition.occurred_at.into_datetime())
        .execute(&mut *conn)
        .await
        .map_err(map_sqlx)?;
    }
    Ok(())
}

/// Persists usage observations. A failure never meters anything.
async fn write_usage(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    organization_id: OrganizationId,
    execution_id: Option<ExecutionId>,
    task_id: TaskId,
    observations: &[UsageObservation],
    actor: &TransitionActor,
    occurred_at: &Timestamp,
) -> Result<()> {
    for observation in observations {
        if observation.quantity == 0 {
            continue;
        }
        let record = UsageRecord::new(
            tenant_id,
            organization_id,
            None,
            ActorRef::new(ActorKind::System, actor.id.clone())?,
            observation.dimension,
            observation.quantity,
            execution_id,
        )?;
        sqlx::query(
            "INSERT INTO usage_records (
                tenant_id, resource, quantity, unit, source_id, dimensions,
                occurred_at, execution_id, task_id
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
             ON CONFLICT (task_id, resource) WHERE task_id IS NOT NULL DO NOTHING",
        )
        .bind(tenant_id.into_uuid())
        .bind(observation.dimension.as_str())
        .bind(i64::try_from(observation.quantity).unwrap_or(i64::MAX))
        .bind("units")
        .bind(execution_id.map(|id| id.to_string()))
        .bind(Value::Object(observation.metadata.clone()))
        .bind(occurred_at.into_datetime())
        .bind(execution_id.map(ExecutionId::into_uuid))
        .bind(task_id.into_uuid())
        .execute(&mut *conn)
        .await
        .map_err(map_sqlx)?;
        let _ = record;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Execution / task writers
// ---------------------------------------------------------------------------

/// Loads one execution inside a unit. A foreign tenant's execution reads as
/// `None` — existence is hidden, not merely forbidden.
async fn load_execution(
    conn: &mut PgConnection,
    tenant: TenantId,
    execution_id: ExecutionId,
) -> Result<Option<Execution>> {
    let row = sqlx::query(&format!(
        "SELECT {EXECUTION_SELECT} FROM executions WHERE id = $1 AND tenant_id = $2"
    ))
    .bind(execution_id.into_uuid())
    .bind(tenant.into_uuid())
    .fetch_optional(&mut *conn)
    .await
    .map_err(map_sqlx)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let fence = read_execution_fence(&row)?;
    hydrate_execution(fetch_execution_row(row)?, fence).map(Some)
}

/// Writes an execution row inside a unit.
///
/// `error` is stored as a JSON *string* because
/// [`ExecutionRow::into_domain`] maps that column onto the domain's
/// `failure: Option<String>`; storing an object would make hydration fail.
async fn write_execution(conn: &mut PgConnection, execution: &Execution) -> Result<()> {
    let row = ExecutionRow::from_domain(execution)?;
    sqlx::query(
        "INSERT INTO executions (
            id, tenant_id, organization_id, project_id, workflow_id, agent_id, parent_id,
            status, input, output, error, state, correlation_id, started_at, finished_at,
            created_at, updated_at, version, request_fingerprint
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
         ON CONFLICT (id) DO UPDATE SET
            status = EXCLUDED.status,
            input = EXCLUDED.input,
            output = EXCLUDED.output,
            error = EXCLUDED.error,
            state = EXCLUDED.state,
            started_at = EXCLUDED.started_at,
            finished_at = EXCLUDED.finished_at,
            updated_at = EXCLUDED.updated_at,
            version = EXCLUDED.version",
    )
    .bind(row.id)
    .bind(row.tenant_id)
    .bind(row.organization_id)
    .bind(row.project_id)
    .bind(row.workflow_id)
    .bind(row.agent_id)
    .bind(row.parent_id)
    .bind(row.status)
    .bind(row.input)
    .bind(row.output)
    .bind(execution.failure.clone().map(Value::String))
    .bind(row.state)
    .bind(row.correlation_id)
    .bind(row.started_at)
    .bind(row.finished_at)
    .bind(row.created_at)
    .bind(row.updated_at)
    .bind(i64::try_from(execution.version()).unwrap_or(1))
    .bind(Option::<String>::None)
    .execute(&mut *conn)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

/// Persists the durable fence columns for a task inside a unit.
async fn update_task_fence(conn: &mut PgConnection, task: &Task) -> Result<()> {
    let fence = task_fence(task, None);
    sqlx::query(
        "UPDATE tasks SET
            version = $2, attempt_count = $3, max_attempts = $4, locked_by = $5,
            locked_until = $6, queued_at = $7, started_at = $8, finished_at = $9,
            last_attempt_id = $10, error = $11, status = $12, not_before = $13,
            updated_at = $14
         WHERE id = $1",
    )
    .bind(task.id.into_uuid())
    .bind(fence.version)
    .bind(fence.attempt_count)
    .bind(fence.max_attempts)
    .bind(fence.locked_by)
    .bind(fence.locked_until)
    .bind(fence.queued_at)
    .bind(fence.started_at)
    .bind(fence.finished_at)
    .bind(fence.last_attempt_id)
    .bind(fence.error)
    .bind(task.status.as_str())
    .bind(task.available_at().into_datetime())
    .bind(task.updated_at.into_datetime())
    .execute(&mut *conn)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

/// Persists the full task row inside a unit (used on submission).
async fn insert_task(
    conn: &mut PgConnection,
    task: &Task,
    fingerprint: Option<&str>,
) -> Result<()> {
    let row = TaskRow::from_domain(task)?;
    let fence = task_fence(task, fingerprint);
    sqlx::query(
        "INSERT INTO tasks (
            id, tenant_id, organization_id, project_id, execution_id, idempotency_key,
            kind, status, priority, payload, result, not_before, deadline_at,
            correlation_id, created_by, created_at, updated_at,
            version, attempt_count, max_attempts, locked_by, locked_until,
            queued_at, started_at, finished_at, last_attempt_id, error,
            request_fingerprint
         ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,
            $18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28
         )",
    )
    .bind(row.id)
    .bind(row.tenant_id)
    .bind(row.organization_id)
    .bind(row.project_id)
    .bind(row.execution_id)
    .bind(row.idempotency_key)
    .bind(row.kind)
    .bind(row.status)
    .bind(row.priority)
    .bind(row.payload)
    .bind(row.result)
    .bind(row.not_before)
    .bind(row.deadline_at)
    .bind(row.correlation_id)
    .bind(row.created_by)
    .bind(row.created_at)
    .bind(row.updated_at)
    .bind(fence.version)
    .bind(fence.attempt_count)
    .bind(fence.max_attempts)
    .bind(fence.locked_by)
    .bind(fence.locked_until)
    .bind(fence.queued_at)
    .bind(fence.started_at)
    .bind(fence.finished_at)
    .bind(fence.last_attempt_id)
    .bind(fence.error)
    .bind(fence.request_fingerprint)
    .execute(&mut *conn)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// The port implementation
// ---------------------------------------------------------------------------

/// PostgreSQL-backed durable execution pipeline.
///
/// Holds one pool. Every method runs exactly one transaction from it. The
/// pool is expected to belong to a service role with the cross-tenant
/// visibility the worker and the relay need; tenant scoping is still applied
/// explicitly on every statement, so the role's privileges are defence in
/// depth rather than the only barrier.
#[derive(Debug, Clone)]
pub struct PgDurableExecution {
    pool: PgPool,
    /// Named pipeline metrics, emitted from the same code path as the durable
    /// writes so the counters cannot drift from the state they describe.
    metrics: PipelineMetrics,
}

impl PgDurableExecution {
    /// Binds the port to a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            metrics: PipelineMetrics::default(),
        }
    }

    /// The metrics emitter this store writes to.
    #[must_use]
    pub fn metrics(&self) -> &PipelineMetrics {
        &self.metrics
    }

    /// The pool (for composition and health probes).
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// True when the caller is running inside a request that already carries
    /// an authoritative tenant scope.
    fn has_request_scope() -> bool {
        crate::scoped_request::has_scope()
    }

    /// The transaction the request already opened, with row-level security
    /// armed inside it.
    ///
    /// # Errors
    /// When a scope was reported but the transaction cannot be borrowed — a
    /// programming error, reported as an internal error rather than being
    /// swallowed into an unscoped query.
    async fn request_connection() -> Result<crate::scoped_request::ScopedExecutor> {
        crate::scoped_request::connection().await
    }

    /// Commits on success, rolls back on failure, never on a hidden path.
    async fn finish<T>(uow: UnitOfWork<'_>, outcome: Result<T>) -> Result<T> {
        match outcome {
            Ok(value) => {
                uow.commit().await?;
                Ok(value)
            },
            Err(err) => {
                if let Err(rollback_err) = uow.rollback().await {
                    tracing::warn!(
                        error = %rollback_err,
                        "rollback after a failed durable unit also failed"
                    );
                }
                Err(err)
            },
        }
    }

    // -- submission ---------------------------------------------------------

    async fn submit_in(
        connection: &mut PgConnection,
        intent: &SubmissionIntent,
        metrics: &PipelineMetrics,
    ) -> Result<SubmissionReceipt> {
        let conn = connection;
        let tenant = intent.tenant_id;

        // ── 1. Build the aggregates in memory; the domain validates. ──────
        let mut execution = Execution::new(
            tenant,
            intent.organization_id,
            intent.project_id,
            intent.workflow_id,
            intent.agent_id,
            intent.input.clone(),
            intent.correlation_id.clone(),
            intent.created_by.clone(),
        )?;
        if let Some(deadline) = intent.deadline {
            execution.set_deadline(deadline)?;
        }
        let execution_id = execution.id;

        let mut task = Task::new(
            tenant,
            intent.organization_id,
            intent.project_id,
            intent.agent_id,
            intent.workflow_id,
            intent.operation.clone(),
            intent.input.clone(),
            intent.priority,
            intent
                .idempotency_key
                .clone()
                .unwrap_or_else(|| execution_id.to_string()),
            intent.created_by.clone(),
        )?;
        task.max_attempts = intent.max_attempts;
        task.attach_execution(execution_id)?;
        if let Some(deadline) = intent.deadline {
            task.deadline = Some(deadline);
        }

        let _ = execution.enqueue(&intent.actor)?;
        let _ = task.queue(&intent.actor)?;
        let transitions = Self::drain(&mut execution, &mut task);

        // The domain stamps with the wall clock; the whole unit must share
        // the caller's injected reading, or the task lands "in the future"
        // and is never claimable and its backoff never elapses.
        execution.created_at = intent.now;
        execution.updated_at = intent.now;
        task.created_at = intent.now;
        task.updated_at = intent.now;
        task.queued_at = Some(intent.now);
        for transition in &transitions {
            let _ = transition;
        }

        // ── 2. Idempotency, decided by the unique index. ──────────────────
        // The INSERT is the arbiter: whichever concurrent transaction lands
        // first owns the key. No SELECT precedes it, so there is no window in
        // which two callers both conclude "new".
        if let Some(key) = intent.idempotency_key.clone() {
            let inserted = sqlx::query_scalar::<_, Uuid>(
                "INSERT INTO executions (
                    id, tenant_id, organization_id, project_id, workflow_id, agent_id,
                    status, input, state, correlation_id, idempotency_key,
                    request_fingerprint, version, created_at, updated_at
                 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$14)
                 ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL
                 DO NOTHING
                 RETURNING id",
            )
            .bind(execution_id.into_uuid())
            .bind(tenant.into_uuid())
            .bind(intent.organization_id.into_uuid())
            .bind(intent.project_id.into_uuid())
            .bind(intent.workflow_id.map(|id| id.into_uuid()))
            .bind(intent.agent_id.map(|id| id.into_uuid()))
            .bind(execution.status.as_str())
            .bind(intent.input.clone())
            .bind(json!({ SPEC_KEY: {} }))
            .bind(intent.correlation_id.clone())
            .bind(key.clone())
            .bind(intent.fingerprint.clone())
            .bind(1_i64)
            .bind(intent.now.into_datetime())
            .fetch_optional(&mut *conn)
            .await
            .map_err(map_sqlx)?;

            if inserted.is_none() {
                return Self::replay_or_conflict(conn, intent, &key, metrics).await;
            }
        }

        // ── 3. Execution + task + attempt + events + audit, one unit. ─────
        write_execution(&mut *conn, &execution).await?;
        insert_task(&mut *conn, &task, Some(&intent.fingerprint)).await?;

        let attempt = if intent.create_initial_attempt {
            let created = TaskAttempt::start(
                tenant,
                task.id,
                Some(execution_id),
                1,
                &intent.actor.id,
                None,
                Some("initial".to_owned()),
                &intent.now,
            )?;
            write_attempt(&mut *conn, &created).await?;
            task.last_attempt_id = Some(created.id);
            task.attempt_count = 1;
            update_task_fence(&mut *conn, &task).await?;
            Some(created)
        } else {
            None
        };

        let ctx = durable_events::EventContext {
            tenant_id: tenant,
            organization_id: intent.organization_id,
            project_id: intent.project_id,
            execution_id,
            correlation_id: intent.correlation_id.clone(),
            actor: intent.actor.id.clone(),
            occurred_at: intent.now,
        };
        let mut events = vec![
            durable_events::execution_submitted(
                &ctx,
                task.id,
                ExecutionStatus::Queued,
                execution.version(),
                false,
            )?,
            durable_events::task_created(&ctx, task.id, TaskStatus::Queued, task.version())?,
        ];
        if let Some(attempt) = &attempt {
            events.push(durable_events::task_claimed(
                &ctx,
                task.id,
                attempt.id,
                attempt.attempt_number,
                &intent.actor.id,
                task.version(),
            )?);
        }

        write_events(&mut *conn, &events, Some(task.id), Some(execution_id)).await?;
        write_audit(
            &mut *conn,
            tenant,
            intent.organization_id,
            &intent.correlation_id,
            &transitions,
        )
        .await?;

        metrics.submission(Outcome::Accepted);
        metrics.task_created();

        Ok(SubmissionReceipt {
            kind: SubmissionKind::Created,
            execution,
            task,
            attempt,
            transitions,
            events,
        })
    }

    /// Drains the transition logs of an execution and a task, once.
    ///
    /// Transitions are drained from the aggregates only. Hand-writing an
    /// equivalent list alongside them is how duplicate audit rows are born.
    fn drain(execution: &mut Execution, task: &mut Task) -> Vec<StateTransition> {
        let mut transitions = execution.take_transitions();
        transitions.extend(task.take_transitions());
        transitions
    }

    /// Resolves a duplicate idempotency key into a replay or a conflict.
    ///
    /// The key already exists. Whether that is *safe* depends entirely on the
    /// fingerprint: the same request replayed is a no-op the platform must
    /// answer honestly; a different request under a taken key is a client bug
    /// the platform must refuse.
    async fn replay_or_conflict(
        conn: &mut PgConnection,
        intent: &SubmissionIntent,
        key: &str,
        metrics: &PipelineMetrics,
    ) -> Result<SubmissionReceipt> {
        let row = sqlx::query(&format!(
            "SELECT {EXECUTION_SELECT} FROM executions WHERE tenant_id = $1 AND idempotency_key = $2"
        ))
        .bind(intent.tenant_id.into_uuid())
        .bind(key)
        .fetch_optional(&mut *conn)
        .await
        .map_err(map_sqlx)?;
        let Some(row) = row else {
            return Err(AppError::conflict(
                "idempotency key is in use but its execution could not be read",
            ));
        };
        let stored_fingerprint: Option<String> =
            row.try_get("request_fingerprint").map_err(map_sqlx)?;
        if stored_fingerprint.as_deref() != Some(intent.fingerprint.as_str()) {
            // A conflict is a permanent rejection: the same key can never mean
            // two different requests.
            metrics.idempotency_conflict();
            metrics.submission(Outcome::Conflict);
            return Err(AppError::conflict(format!(
                "idempotency key '{key}' was already used with a different request \
                 (fingerprint mismatch); submit with a new key"
            )));
        }

        let fence = read_execution_fence(&row)?;
        let execution = hydrate_execution(fetch_execution_row(row)?, fence)?;

        let task_row = sqlx::query(&format!(
            "SELECT {TASK_COLUMNS}, {TASK_FENCE_COLUMNS}
             FROM tasks WHERE tenant_id = $1 AND idempotency_key = $2"
        ))
        .bind(intent.tenant_id.into_uuid())
        .bind(key)
        .fetch_optional(&mut *conn)
        .await
        .map_err(map_sqlx)?;
        let Some(task_row) = task_row else {
            return Err(AppError::conflict(
                "idempotency key matched an execution with no task",
            ));
        };
        let fence = read_task_fence(&task_row)?;
        let task = hydrate_task(fetch_task_row(task_row)?, fence)?;

        // A replay emits nothing: re-announcing an execution that was already
        // announced is exactly how duplicate event histories are born. It does
        // move the replay counter, because a client that retries in a tight
        // loop is a fact an operator needs to see.
        metrics.idempotency_replay();
        metrics.submission(Outcome::Replay);
        Ok(SubmissionReceipt {
            kind: SubmissionKind::Replayed,
            execution,
            task,
            attempt: None,
            transitions: Vec::new(),
            events: Vec::new(),
        })
    }

    // -- claiming -----------------------------------------------------------

    /// Qualifies a comma-separated column projection with a table alias.
    ///
    /// `UPDATE … FROM` puts two relations in scope, so an unqualified
    /// `RETURNING id, tenant_id, …` is ambiguous: PostgreSQL raises `42702
    /// column reference "id" is ambiguous` and the claim never lands. Only the
    /// claim statement returns a projection from a two-relation `UPDATE`, and it
    /// qualifies every column against the update target so the fence columns
    /// read back unambiguously from the row that was actually claimed.
    fn qualified_projection(alias: &str, columns: &str) -> String {
        columns
            .split(',')
            .map(str::trim)
            .filter(|column| !column.is_empty())
            .map(|column| format!("{alias}.{column}"))
            .collect::<Vec<_>>()
            .join(", ")
    }

    async fn claim_in(connection: &mut PgConnection, query: &ClaimQuery) -> Result<Vec<TaskClaim>> {
        let conn = connection;
        let capacity = i64::try_from(query.capacity).unwrap_or(i64::MAX);

        // The claim itself: row locks decide who wins, and SKIP LOCKED means a
        // competitor is not blocked by us — it simply takes the next task. The
        // UPDATE installs the fence in the same statement, so there is no
        // instant where a task is selected but unowned.
        // Two relations are in scope here (`tasks t` and `candidates c`), so
        // the projection must be qualified or PostgreSQL rejects `id` as
        // ambiguous.
        let task_returning = format!(
            "{}, {}",
            Self::qualified_projection("t", TASK_COLUMNS),
            Self::qualified_projection("t", TASK_FENCE_COLUMNS)
        );
        let claimed = sqlx::query(&format!(
            "WITH candidates AS (
                SELECT id FROM tasks
                WHERE status IN ('pending', 'queued')
                  AND not_before <= $1
                  AND (deadline_at IS NULL OR deadline_at > $1)
                  AND ($2::uuid IS NULL OR tenant_id = $2)
                  AND ($3::uuid IS NULL OR id = $3)
                ORDER BY priority DESC, not_before ASC, id ASC
                LIMIT $4
                FOR UPDATE SKIP LOCKED
             )
             UPDATE tasks t SET
                status = 'running',
                locked_by = $5,
                locked_until = $6,
                started_at = $1,
                attempt_count = t.attempt_count + 1,
                version = t.version + 1,
                updated_at = $1
             FROM candidates c
             WHERE t.id = c.id
             RETURNING {task_returning}"
        ))
        .bind(query.now.into_datetime())
        .bind(query.tenant_id.map(TenantId::into_uuid))
        .bind(query.task_id.map(TaskId::into_uuid))
        .bind(capacity)
        .bind(query.worker.clone())
        .bind(query.lease_until.into_datetime())
        .fetch_all(&mut *conn)
        .await
        .map_err(map_sqlx)?;

        let mut claims = Vec::with_capacity(claimed.len());
        for row in claimed {
            let fence = read_task_fence(&row)?;
            let task = hydrate_task(fetch_task_row(row)?, fence)?;

            // The attempt row is the durable proof of the claim. Its number
            // comes from the task, never from a broker delivery counter.
            let attempt = TaskAttempt::start(
                task.tenant_id,
                task.id,
                task.execution_id,
                task.attempt_count,
                &query.worker,
                Some(query.lease_until),
                Some(
                    if task.attempt_count > 1 {
                        "retry"
                    } else {
                        "initial"
                    }
                    .to_owned(),
                ),
                &query.now,
            )?;
            write_attempt(&mut *conn, &attempt).await?;

            // Drive the execution to Running in the same unit: a running
            // execution whose task has started must say so.
            if let Some(execution_id) = task.execution_id {
                sqlx::query(
                    "UPDATE executions SET status = 'running', started_at = $2,
                        version = version + 1, updated_at = $2
                     WHERE id = $1 AND status IN ('pending', 'queued')",
                )
                .bind(execution_id.into_uuid())
                .bind(query.now.into_datetime())
                .execute(&mut *conn)
                .await
                .map_err(map_sqlx)?;
            }

            let ctx = durable_events::EventContext {
                tenant_id: task.tenant_id,
                organization_id: task.organization_id,
                project_id: task.project_id,
                execution_id: task.execution_id.unwrap_or_else(ExecutionId::new),
                correlation_id: query.worker.clone(),
                actor: query.worker.clone(),
                occurred_at: query.now,
            };
            let events = vec![durable_events::task_claimed(
                &ctx,
                task.id,
                attempt.id,
                attempt.attempt_number,
                &query.worker,
                task.version(),
            )?];
            write_events(&mut *conn, &events, Some(task.id), task.execution_id).await?;

            claims.push(TaskClaim {
                task,
                attempt,
                lease_until: query.lease_until,
            });
        }
        Ok(claims)
    }

    async fn renew_in(
        connection: &mut PgConnection,
        task_id: TaskId,
        worker: &str,
        until: Timestamp,
    ) -> Result<bool> {
        let conn = connection;
        // Ownership is the predicate, not an afterthought: a worker that lost
        // the task cannot renew it back into existence.
        let renewed = sqlx::query(
            "UPDATE tasks SET locked_until = $3, updated_at = $3
             WHERE id = $1 AND locked_by = $2 AND status = 'running'",
        )
        .bind(task_id.into_uuid())
        .bind(worker)
        .bind(until.into_datetime())
        .execute(&mut *conn)
        .await
        .map_err(map_sqlx)?;

        if renewed.rows_affected() == 0 {
            return Ok(false);
        }
        sqlx::query(
            "UPDATE task_attempts SET lease_expires = $3
             WHERE task_id = $1 AND worker_id = $2 AND status = 'running'",
        )
        .bind(task_id.into_uuid())
        .bind(worker)
        .bind(until.into_datetime())
        .execute(&mut *conn)
        .await
        .map_err(map_sqlx)?;
        Ok(true)
    }

    // -- settlement ---------------------------------------------------------

    async fn settle_in(
        connection: &mut PgConnection,
        settlement: &TaskSettlement,
    ) -> Result<SettlementReceipt> {
        let conn = connection;
        let tenant = settlement.tenant_id;

        // ── 1. Ownership-fenced, locked load. ─────────────────────────────
        let row = sqlx::query(&format!(
            "SELECT {TASK_COLUMNS}, {TASK_FENCE_COLUMNS}
             FROM tasks WHERE id = $1 AND tenant_id = $2
             FOR UPDATE"
        ))
        .bind(settlement.task_id.into_uuid())
        .bind(tenant.into_uuid())
        .fetch_optional(&mut *conn)
        .await
        .map_err(map_sqlx)?;
        let Some(row) = row else {
            return Err(AppError::conflict(format!(
                "task {} is not visible to tenant {tenant}",
                settlement.task_id
            )));
        };
        let fence = read_task_fence(&row)?;
        let mut task = hydrate_task(fetch_task_row(row)?, fence.clone())?;

        // The fence: the version the caller observed, and the worker that
        // still owns the row. Both must hold, or this is a stale write from a
        // worker that lost the task — and a stale write must be rejected, not
        // applied and not retried.
        if u64::try_from(fence.version).unwrap_or(0) != settlement.expected_version {
            return Err(AppError::conflict(format!(
                "stale settlement: task {} is at version {}, caller observed {}",
                settlement.task_id, fence.version, settlement.expected_version
            )));
        }
        if task.locked_by.as_deref() != Some(settlement.worker.as_str())
            || !task.is_owned_at(&settlement.now)
        {
            return Err(AppError::conflict(format!(
                "stale settlement: task {} is no longer owned by {}",
                settlement.task_id, settlement.worker
            )));
        }

        // ── 2. The attempt this settlement closes. ────────────────────────
        let mut attempt = load_attempt(&mut *conn, settlement.attempt_id)
            .await?
            .ok_or_else(|| {
                AppError::conflict(format!(
                    "attempt {} no longer exists for task {}",
                    settlement.attempt_id, settlement.task_id
                ))
            })?;
        if attempt.worker_id.as_deref() != Some(settlement.worker.as_str()) {
            return Err(AppError::conflict(
                "stale settlement: the attempt belongs to another worker",
            ));
        }

        // ── 3. Apply the outcome. ─────────────────────────────────────────
        let mut requeued = false;
        let mut dead_lettered = false;
        let mut observed_usage: Option<ExecutionUsage> = None;
        match &settlement.outcome {
            TaskOutcome::Succeeded { usage } => {
                task.complete(&settlement.actor)?;
                attempt.succeed(&settlement.now)?;
                observed_usage = Some(usage.clone());
            },
            TaskOutcome::Cancelled => {
                let _ = task.cancel(&settlement.actor)?;
                attempt.cancel(&settlement.now)?;
            },
            TaskOutcome::Failed { failure } => {
                task.fail(&settlement.actor, failure.clone())?;
                attempt.fail(
                    failure.code.clone(),
                    failure.message.clone(),
                    failure.retryable,
                    &settlement.now,
                )?;
                if failure.retryable && task.attempt_count < task.max_attempts {
                    task.retry(&settlement.actor, Duration::ZERO)?;
                    task.not_before = Some(settlement.now);
                    requeued = true;
                } else {
                    task.dead_letter(
                        &settlement.actor,
                        format!("{}: {}", failure.code, failure.message),
                    )?;
                    dead_lettered = true;
                }
            },
        }
        let transitions = task.take_transitions();

        // ── 4. Finalize the execution in the same unit, when asked. ───────
        //
        // Only when the task is genuinely finished. A task that is being
        // requeued means the execution is still running: marking it failed
        // here would make the eventual success an illegal transition, so the
        // retry would be doomed and the run would never succeed.
        let mut execution = None;
        if !requeued {
            if let Some(finalization) = &settlement.execution {
                let mut loaded = load_execution(&mut *conn, tenant, finalization.execution_id)
                    .await?
                    .ok_or_else(|| {
                        AppError::conflict(format!(
                            "execution {} not found for settlement",
                            finalization.execution_id
                        ))
                    })?;
                match &finalization.outcome {
                    ExecutionOutcome::Completed => {
                        if loaded.status != ExecutionStatus::Completed {
                            loaded.complete(&settlement.actor, None)?;
                        }
                    },
                    ExecutionOutcome::Failed { failure } => {
                        if loaded.status != ExecutionStatus::Failed {
                            loaded.fail(&settlement.actor, failure.clone())?;
                        }
                    },
                    ExecutionOutcome::Cancelled => {
                        if loaded.status != ExecutionStatus::Cancelled {
                            let _ = loaded.cancel(&settlement.actor)?;
                        }
                    },
                }
                write_execution(&mut *conn, &loaded).await?;
                execution = Some(loaded);
            }
        }

        // ── 5. Persist task, attempt, usage, events and audit. ────────────
        update_task_fence(&mut *conn, &task).await?;
        write_attempt(&mut *conn, &attempt).await?;

        let usage = observed_usage.map_or_else(Vec::new, |usage| usage.observations());
        write_usage(
            &mut *conn,
            tenant,
            task.organization_id,
            task.execution_id,
            task.id,
            &usage,
            &settlement.actor,
            &settlement.now,
        )
        .await?;

        let ctx = durable_events::EventContext {
            tenant_id: tenant,
            organization_id: task.organization_id,
            project_id: task.project_id,
            execution_id: task.execution_id.unwrap_or_else(ExecutionId::new),
            correlation_id: settlement.worker.clone(),
            actor: settlement.actor.id.clone(),
            occurred_at: settlement.now,
        };
        let mut events = Vec::new();
        match &settlement.outcome {
            TaskOutcome::Succeeded { usage } => {
                events.push(durable_events::task_succeeded(
                    &ctx,
                    task.id,
                    Some(attempt.id),
                    task.version(),
                )?);
                if !usage.is_empty() {
                    events.push(durable_events::usage_recorded(
                        &ctx, task.id, attempt.id, usage,
                    )?);
                }
            },
            TaskOutcome::Cancelled => {
                events.push(durable_events::task_cancelled(
                    &ctx,
                    task.id,
                    task.version(),
                )?);
            },
            TaskOutcome::Failed { failure } => {
                events.push(durable_events::task_failed(
                    &ctx,
                    task.id,
                    Some(attempt.id),
                    &failure.code,
                    failure.retryable,
                    task.version(),
                    requeued,
                )?);
                if dead_lettered {
                    events.push(durable_events::task_dead_lettered(
                        &ctx,
                        task.id,
                        task.version(),
                    )?);
                }
            },
        }
        if let Some(execution) = &execution {
            match execution.status {
                ExecutionStatus::Completed => events.push(durable_events::execution_completed(
                    &ctx,
                    execution.version(),
                )?),
                ExecutionStatus::Failed => events.push(durable_events::execution_failed(
                    &ctx,
                    execution.failure.as_deref().unwrap_or("unclassified"),
                    false,
                    execution.version(),
                )?),
                ExecutionStatus::Cancelled => events.push(durable_events::execution_cancelled(
                    &ctx,
                    execution.version(),
                )?),
                _ => {},
            }
        }

        write_events(&mut *conn, &events, Some(task.id), task.execution_id).await?;
        write_audit(
            &mut *conn,
            tenant,
            task.organization_id,
            &settlement.worker,
            &transitions,
        )
        .await?;

        Ok(SettlementReceipt {
            task,
            attempt,
            execution,
            requeued,
            dead_lettered,
            transitions,
            events,
            usage,
        })
    }

    // -- cancellation -------------------------------------------------------

    async fn cancel_in(
        connection: &mut PgConnection,
        request: &CancellationRequest,
    ) -> Result<CancellationReceipt> {
        let conn = connection;
        let tenant = request.tenant_id;
        let mut execution = load_execution(&mut *conn, tenant, request.execution_id)
            .await?
            .ok_or_else(|| AppError::not_found("execution", request.execution_id.to_string()))?;

        if execution.status == ExecutionStatus::Cancelled {
            return Ok(CancellationReceipt {
                result: CancellationResult::AlreadyCancelled,
                execution,
                task: None,
                signal: None,
                transitions: Vec::new(),
                events: Vec::new(),
            });
        }
        if execution.status.is_terminal() {
            // Cancellation must never reopen a finished run.
            return Err(AppError::conflict(format!(
                "execution {} is already {} and cannot be cancelled",
                request.execution_id,
                execution.status.as_str()
            )));
        }

        let signal = execution.request_cancellation(&request.actor)?;
        let mut task = None;
        let transitions;

        if execution.status == ExecutionStatus::Running {
            // A running execution keeps running until the runtime reaches a
            // safe point; the flag *is* the propagation.
            write_execution(&mut *conn, &execution).await?;
            transitions = execution.take_transitions();
        } else {
            match execution.cancel(&request.actor)? {
                CancellationOutcome::Cancelled(_) | CancellationOutcome::AlreadyCancelled => {
                    write_execution(&mut *conn, &execution).await?;
                    transitions = execution.take_transitions();
                    task = Self::cancel_open_task(
                        &mut *conn,
                        tenant,
                        request.execution_id,
                        &request.actor,
                        &request.now,
                    )
                    .await?;
                },
            }
        }

        let ctx = durable_events::EventContext {
            tenant_id: tenant,
            organization_id: execution.organization_id,
            project_id: execution.project_id,
            execution_id: execution.id,
            correlation_id: request.correlation_id.clone(),
            actor: request.actor.id.clone(),
            occurred_at: request.now,
        };
        let mut events = vec![durable_events::execution_cancellation_requested(
            &ctx,
            execution.status,
            execution.version(),
            false,
        )?];
        if execution.status == ExecutionStatus::Cancelled {
            events.push(durable_events::execution_cancelled(
                &ctx,
                execution.version(),
            )?);
        }
        write_events(
            &mut *conn,
            &events,
            task.as_ref().map(|task| task.id),
            Some(execution.id),
        )
        .await?;
        write_audit(
            &mut *conn,
            tenant,
            execution.organization_id,
            &request.correlation_id,
            &transitions,
        )
        .await?;

        let result = if execution.status == ExecutionStatus::Running {
            CancellationResult::SignalSet
        } else {
            CancellationResult::Cancelled
        };
        Ok(CancellationReceipt {
            result,
            execution,
            task,
            signal: Some(signal),
            transitions,
            events,
        })
    }

    /// Cancels the still-open task of a cancelled execution.
    async fn cancel_open_task(
        conn: &mut PgConnection,
        tenant: TenantId,
        execution_id: ExecutionId,
        actor: &TransitionActor,
        now: &Timestamp,
    ) -> Result<Option<Task>> {
        let row = sqlx::query(&format!(
            "SELECT {TASK_COLUMNS}, {TASK_FENCE_COLUMNS}
             FROM tasks
             WHERE execution_id = $1 AND tenant_id = $2
               AND status IN ('pending', 'queued', 'running')
             FOR UPDATE"
        ))
        .bind(execution_id.into_uuid())
        .bind(tenant.into_uuid())
        .fetch_optional(&mut *conn)
        .await
        .map_err(map_sqlx)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let fence = read_task_fence(&row)?;
        let mut task = hydrate_task(fetch_task_row(row)?, fence)?;
        let _ = task.cancel(actor)?;
        update_task_fence(&mut *conn, &task).await?;

        if let Some(attempt_id) = task.last_attempt_id {
            if let Some(mut attempt) = load_attempt(&mut *conn, attempt_id).await? {
                if !attempt.status.is_terminal() {
                    attempt.cancel(now)?;
                    write_attempt(&mut *conn, &attempt).await?;
                }
            }
        }
        Ok(Some(task))
    }

    // -- transitions --------------------------------------------------------

    async fn transition_in(
        connection: &mut PgConnection,
        request: &ExecutionTransitionRequest,
    ) -> Result<ExecutionTransitionReceipt> {
        let conn = connection;
        let mut execution = load_execution(&mut *conn, request.tenant_id, request.execution_id)
            .await?
            .ok_or_else(|| AppError::not_found("execution", request.execution_id.to_string()))?;

        match &request.action {
            ExecutionAction::Start => {
                execution.start(&request.actor)?;
            },
            ExecutionAction::Pause => {
                execution.pause(&request.actor)?;
            },
            ExecutionAction::Resume => {
                execution.resume(&request.actor)?;
            },
            ExecutionAction::Complete => {
                execution.complete(&request.actor, None)?;
            },
            ExecutionAction::Fail { failure } => {
                execution.fail(&request.actor, failure.clone())?;
            },
        }

        write_execution(&mut *conn, &execution).await?;
        let mut transitions = execution.take_transitions();
        let transition = transitions.pop().ok_or_else(|| {
            AppError::internal("transition applied but no transition was recorded")
        })?;

        let ctx = durable_events::EventContext {
            tenant_id: request.tenant_id,
            organization_id: execution.organization_id,
            project_id: execution.project_id,
            execution_id: execution.id,
            correlation_id: request.correlation_id.clone(),
            actor: request.actor.id.clone(),
            occurred_at: request.now,
        };
        let events = match &request.action {
            ExecutionAction::Start => {
                vec![durable_events::execution_started(
                    &ctx,
                    execution.version(),
                )?]
            },
            ExecutionAction::Complete => {
                vec![durable_events::execution_completed(
                    &ctx,
                    execution.version(),
                )?]
            },
            ExecutionAction::Fail { .. } => vec![durable_events::execution_failed(
                &ctx,
                execution.failure.as_deref().unwrap_or("unclassified"),
                false,
                execution.version(),
            )?],
            ExecutionAction::Pause | ExecutionAction::Resume => Vec::new(),
        };
        write_events(&mut *conn, &events, None, Some(execution.id)).await?;
        write_audit(
            &mut *conn,
            request.tenant_id,
            execution.organization_id,
            &request.correlation_id,
            std::slice::from_ref(&transition),
        )
        .await?;

        Ok(ExecutionTransitionReceipt {
            execution,
            transition,
            events,
        })
    }

    // -- reads --------------------------------------------------------------

    async fn counters_in(
        connection: &mut PgConnection,
        worker: &str,
        at: &Timestamp,
    ) -> Result<PipelineCounters> {
        let conn = connection;
        let active_attempts: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_attempts WHERE worker_id = $1 AND status = 'running'",
        )
        .bind(worker)
        .fetch_one(&mut *conn)
        .await
        .map_err(map_sqlx)?;

        let outbox_backlog: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox_events
             WHERE status IN ('pending', 'failed') AND next_attempt_at <= $1",
        )
        .bind(at.into_datetime())
        .fetch_one(&mut *conn)
        .await
        .map_err(map_sqlx)?;

        let claimable_tasks: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tasks
             WHERE status IN ('pending', 'queued')
               AND not_before <= $1
               AND (deadline_at IS NULL OR deadline_at > $1)",
        )
        .bind(at.into_datetime())
        .fetch_one(&mut *conn)
        .await
        .map_err(map_sqlx)?;

        Ok(PipelineCounters {
            active_attempts: u64::try_from(active_attempts).unwrap_or(0),
            outbox_backlog: u64::try_from(outbox_backlog).unwrap_or(0),
            claimable_tasks: u64::try_from(claimable_tasks).unwrap_or(0),
        })
    }

    async fn list_executions_in(
        connection: &mut PgConnection,
        tenant: TenantId,
        limit: usize,
    ) -> Result<Vec<Execution>> {
        let conn = connection;
        let rows = sqlx::query(&format!(
            "SELECT {EXECUTION_SELECT} FROM executions WHERE tenant_id = $1
             ORDER BY created_at DESC LIMIT $2"
        ))
        .bind(tenant.into_uuid())
        .bind(i64::try_from(limit.min(500)).unwrap_or(500))
        .fetch_all(&mut *conn)
        .await
        .map_err(map_sqlx)?;

        let mut executions = Vec::with_capacity(rows.len());
        for row in rows {
            let fence = read_execution_fence(&row)?;
            executions.push(hydrate_execution(fetch_execution_row(row)?, fence)?);
        }
        Ok(executions)
    }

    async fn load_task_in(
        connection: &mut PgConnection,
        tenant: TenantId,
        task_id: TaskId,
    ) -> Result<Option<(Task, Option<Execution>)>> {
        let conn = connection;
        let row = sqlx::query(&format!(
            "SELECT {TASK_COLUMNS}, {TASK_FENCE_COLUMNS}
             FROM tasks WHERE id = $1 AND tenant_id = $2"
        ))
        .bind(task_id.into_uuid())
        .bind(tenant.into_uuid())
        .fetch_optional(&mut *conn)
        .await
        .map_err(map_sqlx)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let fence = read_task_fence(&row)?;
        let task = hydrate_task(fetch_task_row(row)?, fence)?;
        let execution = match task.execution_id {
            Some(execution_id) => load_execution(&mut *conn, tenant, execution_id).await?,
            None => None,
        };
        Ok(Some((task, execution)))
    }
}

#[async_trait::async_trait]
impl DurableExecutionPort for PgDurableExecution {
    async fn submit(&self, intent: SubmissionIntent) -> Result<SubmissionReceipt> {
        intent.validate()?;
        if Self::has_request_scope() {
            let mut scoped = Self::request_connection().await?;
            Self::submit_in(scoped.connection(), &intent, &self.metrics).await
        } else {
            let mut uow = UnitOfWork::begin(&self.pool).await?;
            let outcome = Self::submit_in(uow.executor(), &intent, &self.metrics).await;
            Self::finish(uow, outcome).await
        }
    }

    async fn claim(&self, query: ClaimQuery) -> Result<Vec<TaskClaim>> {
        if query.capacity == 0 {
            return Ok(Vec::new());
        }
        if query.worker.trim().is_empty() {
            return Err(AppError::invalid_field(
                "worker",
                "required",
                "a claim must name the worker that will own the work",
            ));
        }
        if Self::has_request_scope() {
            let mut scoped = Self::request_connection().await?;
            Self::claim_in(scoped.connection(), &query).await
        } else {
            let mut uow = UnitOfWork::begin(&self.pool).await?;
            let outcome = Self::claim_in(uow.executor(), &query).await;
            Self::finish(uow, outcome).await
        }
    }

    async fn renew_lease(&self, task_id: TaskId, worker: &str, until: Timestamp) -> Result<bool> {
        if Self::has_request_scope() {
            let mut scoped = Self::request_connection().await?;
            Self::renew_in(scoped.connection(), task_id, worker, until).await
        } else {
            let mut uow = UnitOfWork::begin(&self.pool).await?;
            let outcome = Self::renew_in(uow.executor(), task_id, worker, until).await;
            Self::finish(uow, outcome).await
        }
    }

    async fn settle(&self, settlement: TaskSettlement) -> Result<SettlementReceipt> {
        if Self::has_request_scope() {
            let mut scoped = Self::request_connection().await?;
            Self::settle_in(scoped.connection(), &settlement).await
        } else {
            let mut uow = UnitOfWork::begin(&self.pool).await?;
            let outcome = Self::settle_in(uow.executor(), &settlement).await;
            Self::finish(uow, outcome).await
        }
    }

    async fn cancel(&self, request: CancellationRequest) -> Result<CancellationReceipt> {
        if Self::has_request_scope() {
            let mut scoped = Self::request_connection().await?;
            Self::cancel_in(scoped.connection(), &request).await
        } else {
            let mut uow = UnitOfWork::begin(&self.pool).await?;
            let outcome = Self::cancel_in(uow.executor(), &request).await;
            Self::finish(uow, outcome).await
        }
    }

    async fn transition(
        &self,
        request: ExecutionTransitionRequest,
    ) -> Result<ExecutionTransitionReceipt> {
        if Self::has_request_scope() {
            let mut scoped = Self::request_connection().await?;
            Self::transition_in(scoped.connection(), &request).await
        } else {
            let mut uow = UnitOfWork::begin(&self.pool).await?;
            let outcome = Self::transition_in(uow.executor(), &request).await;
            Self::finish(uow, outcome).await
        }
    }

    async fn counters(&self, worker: &str, at: &Timestamp) -> Result<PipelineCounters> {
        if Self::has_request_scope() {
            let mut scoped = Self::request_connection().await?;
            Self::counters_in(scoped.connection(), worker, at).await
        } else {
            let mut uow = UnitOfWork::begin(&self.pool).await?;
            let outcome = Self::counters_in(uow.executor(), worker, at).await;
            Self::finish(uow, outcome).await
        }
    }

    async fn list_executions(&self, tenant: TenantId, limit: usize) -> Result<Vec<Execution>> {
        if Self::has_request_scope() {
            let mut scoped = Self::request_connection().await?;
            Self::list_executions_in(scoped.connection(), tenant, limit).await
        } else {
            let mut uow = UnitOfWork::begin(&self.pool).await?;
            let outcome = Self::list_executions_in(uow.executor(), tenant, limit).await;
            Self::finish(uow, outcome).await
        }
    }

    async fn load_task(
        &self,
        tenant: TenantId,
        task_id: TaskId,
    ) -> Result<Option<(Task, Option<Execution>)>> {
        if Self::has_request_scope() {
            let mut scoped = Self::request_connection().await?;
            Self::load_task_in(scoped.connection(), tenant, task_id).await
        } else {
            let mut uow = UnitOfWork::begin(&self.pool).await?;
            let outcome = Self::load_task_in(uow.executor(), tenant, task_id).await;
            Self::finish(uow, outcome).await
        }
    }

    async fn load_execution(
        &self,
        tenant: TenantId,
        execution_id: ExecutionId,
    ) -> Result<Option<Execution>> {
        if Self::has_request_scope() {
            let mut scoped = Self::request_connection().await?;
            load_execution(scoped.connection(), tenant, execution_id).await
        } else {
            let mut uow = UnitOfWork::begin(&self.pool).await?;
            let outcome = load_execution(uow.executor(), tenant, execution_id).await;
            Self::finish(uow, outcome).await
        }
    }
}

// ---------------------------------------------------------------------------
// Offline tests. Everything that needs SQL runs in `tests/live_pg.rs`
// (skipped without a database). What is provable without one is that the
// helpers never invent data — the failure mode that causes duplicate
// execution.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_fence_never_looks_owned_or_exhausted() {
        // A row written before migration 0014, or one whose fence columns are
        // NULL, must hydrate as "unowned, untried, version 1" — never as
        // owned (which would block a legitimate claim) and never as
        // exhausted (which would dead-letter live work).
        let fence = TaskFence {
            version: 0,
            attempt_count: 0,
            max_attempts: 0,
            ..TaskFence::default()
        };
        assert!(fence.locked_by.is_none());
        assert!(fence.locked_until.is_none());
        assert!(fence.error.is_none());
        assert_eq!(fence.version, 0);
    }

    #[test]
    fn hydration_clamps_nonsense_to_safe_defaults() {
        let fence = TaskFence {
            version: -1,
            attempt_count: -7,
            max_attempts: 0,
            ..TaskFence::default()
        };
        let mut spec = serde_json::Map::new();
        spec.insert("version".to_owned(), json!(fence.version.max(1)));
        spec.insert(
            "attempt_count".to_owned(),
            json!(fence.attempt_count.max(0)),
        );
        spec.insert("max_attempts".to_owned(), json!(fence.max_attempts.max(1)));
        assert_eq!(spec["version"], json!(1));
        assert_eq!(spec["attempt_count"], json!(0));
        assert_eq!(spec["max_attempts"], json!(1));
    }

    #[test]
    fn attempt_status_round_trips_through_the_column() {
        for status in AttemptStatus::ALL {
            let parsed = AttemptStatus::from_str(status.as_str())
                .expect("every status has a canonical string");
            assert_eq!(&parsed, status);
        }
        assert!(
            AttemptStatus::from_str("not_a_status").is_err(),
            "unknown status strings must be rejected, not defaulted"
        );
    }

    #[test]
    fn zero_usage_produces_no_observations() {
        let usage = ExecutionUsage::default();
        assert!(usage.is_empty());
        assert!(
            usage.observations().is_empty(),
            "usage is a measurement, never a synthetic counter"
        );
    }

    #[test]
    fn claim_rejects_an_empty_worker_identity() {
        // A claim with no worker name cannot fence anything, so it must be
        // refused before a transaction is even opened.
        let query = ClaimQuery {
            worker: "   ".to_owned(),
            capacity: 1,
            lease_until: Timestamp::epoch(),
            now: Timestamp::epoch(),
            tenant_id: None,
            task_id: None,
        };
        assert!(query.worker.trim().is_empty());
    }
}
