//! PURPOSE: `PgTaskBroker` — the durability-first broker for PostgreSQL-only
//! deployments.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: make rows the truth. `fetch` claims queued task
//! rows atomically (`FOR UPDATE SKIP LOCKED`) and `ack`/`nack` are row
//! transitions, so a crashed consumer's work is recovered by the database
//! rather than by broker metadata.
//!
//! It must NOT:
//!   • derive attempt counts from delivery metadata (they come from
//!     `task_attempts`);
//!   • delete a message row on completion — the row is the audit trail;
//!   • hand the same task to two workers.

//! `PgTaskBroker` — the durability-first broker for Pg-only deployments.
//!
//! Doctrine (recorded in `docs/production-wiring.md` §3):
//!
//! * Rows are truth. `fetch` claims `queued` task rows atomically
//!   (`FOR UPDATE SKIP LOCKED`), `ack(Ack|Nak|Term)` settles them, and
//!   crash-recovery works by policy, not hope: a claimed-but-never-settled
//!   row is *reaped* at the next fetch after its claim window expires (the
//!   same `not_before` column carries backoff and claim expiry — a claim
//!   sets it to `now + ack_wait`, an explicit Nak sets it to `now + delay`).
//! * Task subjects (`mas.tasks.>`, see `mas_messaging::subjects`) are
//!   durable over the `tasks` table.
//!   Event subjects (`mas.events.>`) are routed to an embedded volatile
//!   fan-out broker — a deliberate documented gap: durable event delivery
//!   is the outbox relay's job (state change + outbox row commit
//!   atomically, then publish), so the wake-up lane does not carry the
//!   persistence burden.
//! * Sequence numbers are surrogates: (uuid v7 → first 8 bytes) → u64. The
//!   survivor's ledger maps them to row ids while claims are in flight;
//!   restarts re-derive via the reaper.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_contracts::task::TaskQueueMessage;
use mas_messaging::broker::{
    AckInstruction, BrokerMessage, BrokerPort, ConsumerConfig, ConsumerStats, InMemoryBroker,
    PublishRequest,
};
use mas_messaging::headers::HeaderSet;
use mas_messaging::subjects::Subject;
use serde_json::json;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;

/// Default ack window when the consumer config isn't re-registered first.
const DEFAULT_ACK_WINDOW: Duration = Duration::from_secs(30);
/// Task lanes (the durable quarter of the subject space).
const TASK_SUBJECT_PREFIX: &str = "mas.tasks.";

/// Atomic claim: pick queued rows past their `not_before`, flip them to
/// `running` with an extended claim window, return full rows.
const CLAIM_SQL: &str = "WITH claimable AS (
    SELECT id FROM tasks
    WHERE status = 'queued' AND not_before <= now()
    ORDER BY priority DESC, created_at ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
    )
    UPDATE tasks t
    SET status = 'running', not_before = now() + make_interval(secs => $2), updated_at = now()
    FROM claimable WHERE t.id = claimable.id
    RETURNING t.id, t.tenant_id, t.organization_id, t.project_id, t.execution_id,
              t.idempotency_key, t.kind, t.priority, t.deadline_at,
              t.payload, t.created_at, t.correlation_id";

/// Return overdue claims to the queue (lease-expired 'running' rows).
const REAP_SQL: &str = "UPDATE tasks SET status = 'queued', not_before = now(), updated_at = now()
    WHERE status = 'running' AND not_before <= now()";

const NAK_SQL: &str = "UPDATE tasks SET status = 'queued', not_before = $2, updated_at = now()
    WHERE id = $1 AND status = 'running'";

const TERM_SQL: &str = "UPDATE tasks SET status = 'dead_lettered', updated_at = now()
    WHERE id = $1 AND status = 'running'";

const DEAD_LETTERS_SQL: &str = "SELECT id, payload, created_at FROM tasks
    WHERE status = 'dead_lettered' ORDER BY updated_at ASC LIMIT $1";

/// Task ids are UUIDs; uuid-v7 first 8 bytes → timestamp-bearing u64 seq.
fn sequence_of(id: Uuid) -> u64 {
    let bytes = id.as_bytes();
    u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}

/// In-flight claims (sequence ↔ row id), drained by settlement/reaping.
#[derive(Debug, Default)]
struct FlightLedger {
    by_sequence: Mutex<BTreeMap<u64, Uuid>>,
}

impl FlightLedger {
    fn record(&self, sequence: u64, row: Uuid) {
        self.by_sequence
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(sequence, row);
    }

    fn take(&self, sequence: u64) -> Option<Uuid> {
        self.by_sequence
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&sequence)
    }
}

/// Durable-first task broker + volatile event fan-out (see module docs).
#[derive(Debug)]
pub struct PgTaskBroker {
    pool: PgPool,
    events: InMemoryBroker,
    flight: FlightLedger,
    ack_window_seconds: i64,
}

impl PgTaskBroker {
    /// Binds a broker to a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        // `InMemoryBroker` starts UNAVAILABLE by design (see its chaos knob).
        // The event lane here is a real delegation target for non-durable
        // subjects, `fetch` remainders, `ack`, `dead_letters`, `ping` and
        // `consumer_stats` — so it must be switched on or every one of those
        // calls fails with "broker is currently unavailable".
        let events = InMemoryBroker::new();
        events.set_available(true);
        Self {
            pool,
            events,
            flight: FlightLedger::default(),
            ack_window_seconds: DEFAULT_ACK_WINDOW.as_secs() as i64,
        }
    }

    /// Overrides the ack window (from the consumer's config on registration).
    #[must_use]
    pub fn with_ack_window(mut self, window: Duration) -> Self {
        self.ack_window_seconds = window.as_secs() as i64;
        self
    }

    /// Whether the subject belongs to the durable (tasks) lane.
    #[must_use]
    pub fn is_durable_subject(subject: &Subject) -> bool {
        subject.as_str().starts_with(TASK_SUBJECT_PREFIX)
    }

    async fn fetch_durable(&self, max: usize) -> Result<Vec<BrokerMessage>> {
        // Recovery-first: expired claims return before fresh claims hand out.
        sqlx::query(REAP_SQL)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?;
        let rows = sqlx::query(CLAIM_SQL)
            .bind(i64::try_from(max).unwrap_or(i64::MAX))
            .bind(self.ack_window_seconds)
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx)?;
        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            let id: Uuid = row.try_get("id").map_err(map_sqlx)?;
            let payload: serde_json::Value = row.try_get("payload").map_err(map_sqlx)?;
            let created_at: chrono::DateTime<chrono::Utc> =
                row.try_get("created_at").map_err(map_sqlx)?;
            let correlation_id: String = row.try_get("correlation_id").map_err(map_sqlx)?;
            // `publish` decomposed the staged `TaskQueueMessage` into dedicated
            // columns (identity, routing, correlation) plus a payload jsonb
            // holding `input` and the `_mas_spec` envelope. Consumers decode
            // with the standard codec, so the frame MUST be re-assembled into
            // the full message here — returning the raw payload made every
            // durable frame undecodable (and therefore dead-lettered).
            let tenant_id: Uuid = row.try_get("tenant_id").map_err(map_sqlx)?;
            let organization_id: Uuid = row.try_get("organization_id").map_err(map_sqlx)?;
            let project_id: Uuid = row.try_get("project_id").map_err(map_sqlx)?;
            let execution_id: Option<Uuid> = row.try_get("execution_id").map_err(map_sqlx)?;
            let idempotency_key: String = row.try_get("idempotency_key").map_err(map_sqlx)?;
            let priority: String = row.try_get("priority").map_err(map_sqlx)?;
            let deadline_at: Option<chrono::DateTime<chrono::Utc>> =
                row.try_get("deadline_at").map_err(map_sqlx)?;

            let spec = payload.get(crate::rows::SPEC_KEY).cloned();
            let get = |key: &str| {
                spec.as_ref()
                    .and_then(|s| s.get(key))
                    .cloned()
                    .unwrap_or(serde_json::Value::Null)
            };
            let operation = get("operation")
                .as_str()
                .map(str::to_owned)
                .or_else(|| row.try_get::<String, _>("kind").ok())
                .unwrap_or_else(|| "execution.run".to_owned());
            let enqueued_at = get("queued_at")
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| Timestamp::from_datetime(created_at).to_rfc3339_millis());

            let mut frame = serde_json::Map::new();
            frame.insert("task_id".to_owned(), json!(id));
            frame.insert("tenant_id".to_owned(), json!(tenant_id));
            frame.insert("organization_id".to_owned(), json!(organization_id));
            frame.insert("project_id".to_owned(), json!(project_id));
            frame.insert("operation".to_owned(), json!(operation));
            frame.insert(
                "input".to_owned(),
                payload.get("input").cloned().unwrap_or(json!({})),
            );
            if let Some(execution) = execution_id {
                frame.insert("execution_id".to_owned(), json!(execution));
            }
            if let Some(agent) = get("agent_id").as_str() {
                frame.insert("agent_id".to_owned(), json!(agent));
            }
            if let Some(agent_version) = get("agent_version_id").as_str() {
                frame.insert("agent_version_id".to_owned(), json!(agent_version));
            }
            if let Some(workflow) = get("workflow_id").as_str() {
                frame.insert("workflow_id".to_owned(), json!(workflow));
            }
            frame.insert("idempotency_key".to_owned(), json!(idempotency_key));
            frame.insert("priority".to_owned(), json!(priority));
            frame.insert(
                "attempt_count".to_owned(),
                json!(get("attempt_count").as_u64().unwrap_or(0)),
            );
            frame.insert(
                "max_attempts".to_owned(),
                json!(get("max_attempts").as_u64().unwrap_or(1)),
            );
            if let Some(deadline) = deadline_at {
                frame.insert(
                    "deadline".to_owned(),
                    json!(Timestamp::from_datetime(deadline).to_rfc3339_millis()),
                );
            }
            frame.insert("correlation_id".to_owned(), json!(&correlation_id));
            if let Some(request_id) = get("request_id").as_str() {
                frame.insert("request_id".to_owned(), json!(request_id));
            }
            frame.insert("enqueued_at".to_owned(), json!(enqueued_at));
            let payload = serde_json::Value::Object(frame);

            let sequence = sequence_of(id);
            self.flight.record(sequence, id);
            let mut headers = HeaderSet::new();
            headers
                .insert("x-correlation-id", correlation_id.clone())
                .map_err(|err| AppError::validation(err.to_string()))?;
            headers
                .insert("x-task-id", id.to_string())
                .map_err(|err| AppError::validation(err.to_string()))?;
            // The durable lane's subject is the operation as the frame
            // originally carried it (derived on publish / `kind` column).
            let subject = Subject::parse(&format!("mas.tasks.{operation}"))
                .unwrap_or_else(|_| Subject::parse("mas.tasks.unknown").expect("static"));
            messages.push(BrokerMessage {
                sequence,
                subject,
                headers,
                payload: serde_json::to_vec(&payload)
                    .map_err(|e| AppError::serialization(format!("frame: {e}")))?,
                published_at: Timestamp::from_datetime(created_at),
                deliveries: 1,
            });
        }
        Ok(messages)
    }
}

#[async_trait::async_trait]
impl BrokerPort for PgTaskBroker {
    async fn publish(&self, request: PublishRequest) -> Result<u64> {
        if !Self::is_durable_subject(&request.subject) {
            return self.events.publish(request).await;
        }
        // Durable: decode the staged message contract and land the row.
        let message: TaskQueueMessage = serde_json::from_slice(&request.payload)
            .map_err(|err| AppError::serialization(format!("task frame: {err}")))?;
        let mut payload_input = serde_json::json!({ "input": message.input });
        let mut spec = serde_json::Map::new();
        spec.insert("attempt_count".to_owned(), serde_json::Value::from(0u64));
        spec.insert(
            "max_attempts".to_owned(),
            serde_json::Value::from(message.max_attempts),
        );
        spec.insert(
            "operation".to_owned(),
            serde_json::Value::from(message.operation.clone()),
        );
        spec.insert(
            "queued_at".to_owned(),
            serde_json::Value::from(message.enqueued_at.to_rfc3339_millis()),
        );
        if let Some(request_id) = message.request_id {
            spec.insert(
                "request_id".to_owned(),
                serde_json::Value::from(request_id.to_string()),
            );
        }
        if let Some(agent) = message.agent_id {
            spec.insert(
                "agent_id".to_owned(),
                serde_json::Value::from(v7_str(agent)),
            );
        }
        if let Some(agent_version) = message.agent_version_id {
            spec.insert(
                "agent_version_id".to_owned(),
                serde_json::Value::from(v7_str(agent_version)),
            );
        }
        if let Some(workflow) = message.workflow_id {
            spec.insert(
                "workflow_id".to_owned(),
                serde_json::Value::from(v7_str(workflow)),
            );
        }
        payload_input.as_object_mut().expect("object").insert(
            crate::rows::SPEC_KEY.to_owned(),
            serde_json::Value::Object(spec),
        );
        let result = sqlx::query(
            "INSERT INTO tasks (
                id, tenant_id, organization_id, project_id, execution_id, idempotency_key,
                kind, status, priority, payload, result, not_before, deadline_at,
                correlation_id, created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, 'queued', $8, $9, NULL, now(), $10, $11, now(), now()
            )
            ON CONFLICT (tenant_id, idempotency_key) DO NOTHING",
        )
        .bind(Uuid::from(message.task_id))
        .bind(Uuid::from(message.tenant_id))
        .bind(Uuid::from(message.organization_id))
        .bind(Uuid::from(message.project_id))
        .bind(message.execution_id.map(Uuid::from))
        .bind(&message.idempotency_key)
        .bind(&message.operation)
        .bind(message.priority.as_str())
        .bind(&payload_input)
        .bind(message.deadline.map(|ts| deadline_dt(&ts)))
        .bind(&message.correlation_id)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        if result.rows_affected() == 0 {
            // Duplicate publish: idempotent collapse to the original row.
            let existing: Option<Uuid> = sqlx::query_scalar(
                "SELECT id FROM tasks WHERE tenant_id = $1 AND idempotency_key = $2",
            )
            .bind(Uuid::from(message.tenant_id))
            .bind(&message.idempotency_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)?;
            return existing
                .map(sequence_of)
                .ok_or_else(|| AppError::database("idempotency conflict without surviving row"));
        }
        Ok(sequence_of(Uuid::from(message.task_id)))
    }

    async fn ensure_consumer(&self, config: ConsumerConfig) -> Result<()> {
        // The durable lane is configless (claim/reap is global), but `fetch`,
        // `ack`, `consumer_stats` and `dead_letters` all fan out to the event
        // lane too — so the consumer MUST exist there as well, or every one of
        // those calls fails with "broker consumer not found".
        self.events.ensure_consumer(config).await?;
        Ok(())
    }

    async fn fetch(&self, consumer: &str, max: usize) -> Result<Vec<BrokerMessage>> {
        let mut messages = self.fetch_durable(max).await?;
        if messages.len() < max {
            let remainder = self.events.fetch(consumer, max - messages.len()).await?;
            messages.extend(remainder);
        }
        Ok(messages)
    }

    async fn ack(&self, consumer: &str, sequence: u64, instruction: AckInstruction) -> Result<()> {
        let Some(row_id) = self.flight.take(sequence) else {
            // Not a durable-lane claim: route to the event lane.
            return self.events.ack(consumer, sequence, instruction).await;
        };
        match instruction {
            AckInstruction::Ack => {
                // Settlement of truth happened via the lifecycle (status was
                // already updated); ack only validates protocol consistency.
                let status: Option<String> =
                    sqlx::query_scalar("SELECT status FROM tasks WHERE id = $1")
                        .bind(row_id)
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(map_sqlx)?;
                match status.as_deref() {
                    Some("completed" | "failed" | "cancelled" | "dead_lettered") => Ok(()),
                    Some(other) => Err(AppError::conflict(format!(
                        "task {} acknowledged but is '{}' (lifecycle settlement was skipped)",
                        row_id, other
                    ))),
                    None => Err(AppError::not_found("task", row_id.to_string())),
                }
            },
            AckInstruction::Nak { delay } => {
                let not_before = chrono::Utc::now()
                    + delay.unwrap_or_else(|| Duration::from_secs(self.ack_window_seconds as u64));
                sqlx::query(NAK_SQL)
                    .bind(row_id)
                    .bind(not_before)
                    .execute(&self.pool)
                    .await
                    .map_err(map_sqlx)?;
                Ok(())
            },
            AckInstruction::Term => {
                sqlx::query(TERM_SQL)
                    .bind(row_id)
                    .execute(&self.pool)
                    .await
                    .map_err(map_sqlx)?;
                Ok(())
            },
            AckInstruction::InProgress => {
                // Lease extension: slide the claim window forward. The row
                // stays 'running'; the reaper only touches expired windows.
                sqlx::query(
                    "UPDATE tasks SET not_before = $2, updated_at = now()
                     WHERE id = $1 AND status = 'running'",
                )
                .bind(row_id)
                .bind(chrono::Utc::now() + Duration::from_secs(self.ack_window_seconds as u64))
                .execute(&self.pool)
                .await
                .map_err(map_sqlx)?;
                Ok(())
            },
        }
    }

    async fn consumer_stats(&self, consumer: &str) -> Result<ConsumerStats> {
        let durable: (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT
                coalesce(sum(case when status = 'queued' then 1 else 0 end), 0)::bigint,
                coalesce(sum(case when status = 'running' then 1 else 0 end), 0)::bigint,
                coalesce(sum(case when status in ('completed','failed','cancelled') then 1 else 0 end), 0)::bigint,
                coalesce(sum(case when status = 'dead_lettered' then 1 else 0 end), 0)::bigint
             FROM tasks",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx)?;
        let events = self.events.consumer_stats(consumer).await?;
        Ok(ConsumerStats {
            delivered: durable.1 as u64 + events.delivered,
            acked: durable.2 as u64 + events.acked,
            nacked: events.nacked,
            terminated: events.terminated,
            dead_lettered: durable.3 as u64 + events.dead_lettered,
            pending: durable.0 as usize + events.pending,
        })
    }

    async fn dead_letters(&self) -> Result<Vec<BrokerMessage>> {
        let rows = sqlx::query(DEAD_LETTERS_SQL)
            .bind(100i64)
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let id: Uuid = row.try_get("id").map_err(map_sqlx)?;
            let payload: serde_json::Value = row.try_get("payload").map_err(map_sqlx)?;
            let created_at: chrono::DateTime<chrono::Utc> =
                row.try_get("created_at").map_err(map_sqlx)?;
            out.push(BrokerMessage {
                sequence: sequence_of(id),
                subject: Subject::parse("mas.tasks.dlq").expect("static"),
                headers: HeaderSet::new(),
                payload: serde_json::to_vec(&payload)
                    .map_err(|e| AppError::serialization(format!("frame: {e}")))?,
                published_at: Timestamp::from_datetime(created_at),
                deliveries: 1,
            });
        }
        out.extend(self.events.dead_letters().await?);
        Ok(out)
    }

    async fn ping(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?;
        self.events.ping().await
    }
}

fn v7_str<T: Into<Uuid>>(id: T) -> String {
    id.into().to_string()
}

fn deadline_dt(ts: &Timestamp) -> chrono::DateTime<chrono::Utc> {
    let dt: chrono::DateTime<chrono::Utc> = (*ts).into_datetime();
    dt
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_messaging::subjects::SubjectFilter;

    #[test]
    fn v7_uuids_monotone_surrogate_sequences() {
        let a = Uuid::now_v7();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = Uuid::now_v7();
        assert!(
            sequence_of(b) >= sequence_of(a),
            "uuid-v7 ordering must lift"
        );
    }

    #[test]
    fn claim_sql_has_all_safety_guards() {
        for token in [
            "FOR UPDATE SKIP LOCKED",
            "status = 'queued'",
            "not_before <= now()",
            "make_interval(secs => $2)",
            "RETURNING",
        ] {
            assert!(CLAIM_SQL.contains(token), "claim SQL missing {token}");
        }
        assert!(
            REAP_SQL.contains("status = 'running'"),
            "reaper must target claims only"
        );
        assert!(
            NAK_SQL.contains("status = 'running'"),
            "nak guards against double claims"
        );
    }

    #[test]
    fn durable_lane_is_task_subjects_only() {
        assert!(PgTaskBroker::is_durable_subject(
            &Subject::parse("mas.tasks.execution.run").expect("subject")
        ));
        assert!(!PgTaskBroker::is_durable_subject(
            &Subject::parse("mas.events.agent.created").expect("subject")
        ));
    }

    #[test]
    fn subject_filter_wildcard_support_exists() {
        let filter = SubjectFilter::parse("mas.tasks.>").expect("filter");
        assert!(filter.matches(&Subject::parse("mas.tasks.execution.run").expect("subject")));
        assert!(!filter.matches(&Subject::parse("mas.events.agent.created").expect("subject")));
    }
}
