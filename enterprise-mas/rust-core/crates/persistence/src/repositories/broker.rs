//! PURPOSE: `PgTaskBroker` — the durability-first broker for PostgreSQL-only
//! deployments.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: make rows the truth. `fetch` discovers queued
//! task rows but never changes their lifecycle state; the worker's durable
//! attempt claim owns the only queued → running transition. `ack`/`nack` only
//! pace or settle already-authoritative work, so a broker delivery cannot race
//! a task attempt into an incompatible state.
//!
//! It must NOT:
//!   • derive attempt counts from delivery metadata (they come from
//!     `task_attempts`);
//!   • delete a message row on completion — the row is the audit trail;
//!   • grant task ownership: duplicate transport hints are resolved only by
//!     the durable attempt claim.

//! `PgTaskBroker` — the durability-first broker for Pg-only deployments.
//!
//! Doctrine (recorded in `docs/production-wiring.md` §3):
//!
//! * Rows are truth. `fetch` discovers `queued` task rows and maintains only
//!   a process-local duplicate-suppression flight map. The durable attempt
//!   coordinator performs the queued → running transition, records the owner
//!   lease and creates the attempt in one transaction. A restart may redeliver
//!   a hint, but it can never create a second durable owner.
//! * Task subjects (`mas.tasks.>`, see `mas_messaging::subjects`) are
//!   durable over the `tasks` table.
//!   Event subjects (`mas.events.>`) are routed to an embedded volatile
//!   fan-out broker — a deliberate documented gap: durable event delivery
//!   is the outbox relay's job (state change + outbox row commit
//!   atomically, then publish), so the wake-up lane does not carry the
//!   persistence burden.
//! * Sequence numbers are database-generated transport identities. The local
//!   flight ledger maps them to row ids while delivery hints are outstanding;
//!   after a restart a fresh fetch may re-emit a hint, and durable claiming
//!   resolves the duplicate safely.

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

/// Task lanes (the durable quarter of the subject space).
const TASK_SUBJECT_PREFIX: &str = "mas.tasks.";

/// Finds task rows that are ready for durable ownership or lease recovery.
/// This statement is deliberately read-only: `PgDurableExecution::claim_in`
/// is the sole writer of `queued → running`, worker ownership and
/// task-attempt rows. An expired `running` row is emitted as a recovery *hint*
/// rather than being rewritten here; the durable claim closes the old attempt
/// and either requeues or exhausts it atomically. Splitting those transitions
/// across the broker and the attempt coordinator previously left a
/// broker-fetched task in `running` before the coordinator could claim it, so
/// no attempt was ever created.
const FETCH_SQL: &str = "SELECT t.id, t.tenant_id, t.organization_id, t.project_id, t.execution_id,
              t.idempotency_key, t.kind, t.priority, t.deadline_at,
              t.payload, t.created_at, t.correlation_id,
              coalesce((
                  SELECT max(r.delivery_count)::bigint
                  FROM broker_delivery_receipts r
                  WHERE r.consumer_name = $1 AND r.task_id = t.id
              ), 0::bigint) AS prior_deliveries
    FROM tasks t
    WHERE (t.status = 'queued' AND t.not_before <= now())
       OR (t.status = 'running' AND t.locked_until IS NOT NULL AND t.locked_until <= now())
    ORDER BY t.priority DESC, t.created_at ASC
    LIMIT $2";

/// A NAK only paces a still-unowned queued task. A task that has already been
/// claimed is governed by its durable ownership lease and settlement path. A
/// live receipt is proof another replica is about to claim the queued row, so
/// a duplicate transport hint must not push that owner's `not_before` into the
/// future before it gets there.
const NAK_SQL: &str = "UPDATE tasks AS t SET not_before = $2, updated_at = now()
    WHERE t.id = $1 AND t.status = 'queued' AND t.locked_by IS NULL
      AND NOT EXISTS (
          SELECT 1 FROM broker_delivery_receipts r
          WHERE r.task_id = t.id
            AND r.status IN ('reserved', 'in_flight')
            AND r.lease_expires_at > now()
      )";

/// An explicit poison TERM is the one transport-only exception to durable
/// task settlement. It is used solely when a task frame cannot be decoded, so
/// no coordinator can identify its operation or construct a claim. Automatic
/// delivery-count exhaustion never calls this query: attempt budgeting belongs
/// to `PgDurableExecution`. The live-receipt predicate prevents one replica's
/// corrupt/stale hint from terminating work another replica has reserved.
const POISON_QUARANTINE_SQL: &str =
    "UPDATE tasks AS t SET status = 'dead_lettered', updated_at = now()
    WHERE t.id = $1 AND t.status = 'queued' AND t.locked_by IS NULL
      AND NOT EXISTS (
          SELECT 1 FROM broker_delivery_receipts r
          WHERE r.task_id = t.id
            AND r.status IN ('reserved', 'in_flight')
            AND r.lease_expires_at > now()
      )";

const DEAD_LETTERS_SQL: &str = "SELECT id, payload, created_at FROM tasks
    WHERE status = 'dead_lettered' ORDER BY updated_at ASC LIMIT $1";

/// One process-local broker hint. This is only duplicate suppression, never
/// ownership: a durable receipt and task attempt decide who may run the task.
#[derive(Debug, Clone, Copy)]
struct FlightEntry {
    task_id: Uuid,
    redeliver_after: Timestamp,
}

/// Outstanding local delivery hints (sequence ↔ row id), drained by broker
/// settlement or naturally reopened after the durable consumer's ack window.
#[derive(Debug, Default)]
struct FlightLedger {
    by_sequence: Mutex<BTreeMap<u64, FlightEntry>>,
}

impl FlightLedger {
    /// Records one process-local delivery hint only while its ack window is
    /// live. A no-signal duplicate (because another replica owns a live
    /// receipt) must be eligible for redelivery after that window; otherwise a
    /// local process map turns a temporary competing receipt into permanent
    /// recovery starvation.
    fn record_if_absent(
        &self,
        sequence: u64,
        task_id: Uuid,
        redeliver_after: Timestamp,
        now: &Timestamp,
    ) -> bool {
        let mut flights = self.by_sequence.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = flights.get(&sequence) {
            if existing.task_id == task_id && existing.redeliver_after.is_after(now) {
                return false;
            }
        }
        flights.insert(
            sequence,
            FlightEntry {
                task_id,
                redeliver_after,
            },
        );
        true
    }

    fn len(&self) -> usize {
        let now = Timestamp::now();
        let mut flights = self.by_sequence.lock().unwrap_or_else(|e| e.into_inner());
        flights.retain(|_, entry| entry.redeliver_after.is_after(&now));
        flights.len()
    }

    fn get(&self, sequence: u64) -> Option<Uuid> {
        self.by_sequence
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&sequence)
            .map(|entry| entry.task_id)
    }

    /// Extends only process-local duplicate suppression for a healthy worker
    /// heartbeat. The durable task lease remains owned by the coordinator;
    /// this merely gives the broker hint the same ack-window behaviour as a
    /// real transport so it is not re-emitted while that owner is alive.
    fn extend(&self, sequence: u64, redeliver_after: Timestamp) -> bool {
        let mut flights = self.by_sequence.lock().unwrap_or_else(|e| e.into_inner());
        let Some(entry) = flights.get_mut(&sequence) else {
            return false;
        };
        entry.redeliver_after = redeliver_after;
        true
    }

    fn take(&self, sequence: u64) -> Option<Uuid> {
        self.by_sequence
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&sequence)
            .map(|entry| entry.task_id)
    }
}

/// Durable-first task broker + volatile event fan-out (see module docs).
#[derive(Debug)]
pub struct PgTaskBroker {
    pool: PgPool,
    events: InMemoryBroker,
    flight: FlightLedger,
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
        }
    }

    /// Whether the subject belongs to the durable (tasks) lane.
    #[must_use]
    pub fn is_durable_subject(subject: &Subject) -> bool {
        subject.as_str().starts_with(TASK_SUBJECT_PREFIX)
    }

    /// Allocates or retrieves the database-generated transport identity for a
    /// task message. It is stable across process restarts and cannot collide
    /// like a truncated UUID timestamp prefix.
    async fn durable_message_sequence(&self, task_id: Uuid) -> Result<u64> {
        // The normal path is a read after the first delivery. Avoid a
        // conflicting INSERT on every fetch: a busy worker repeatedly polling
        // a queued row should not turn an immutable message identity into a
        // write hot spot. On the rare first-delivery race, the unique key is
        // still the arbiter and the second SELECT observes its winner.
        let existing: Option<i64> = sqlx::query_scalar(
            "SELECT message_sequence FROM broker_task_messages WHERE task_id = $1",
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        let sequence = match existing {
            Some(sequence) => sequence,
            None => {
                sqlx::query(
                    "INSERT INTO broker_task_messages (task_id)
                     VALUES ($1)
                     ON CONFLICT (task_id) DO NOTHING",
                )
                .bind(task_id)
                .execute(&self.pool)
                .await
                .map_err(map_sqlx)?;
                sqlx::query_scalar(
                    "SELECT message_sequence FROM broker_task_messages WHERE task_id = $1",
                )
                .bind(task_id)
                .fetch_one(&self.pool)
                .await
                .map_err(map_sqlx)?
            },
        };
        u64::try_from(sequence).map_err(|_| {
            AppError::database("broker message sequence must be a non-negative bigint")
        })
    }

    /// Reads the persisted delivery policy rather than trusting process-local
    /// defaults. Every replica must agree on these values or an ack window can
    /// silently shrink after a restart.
    async fn durable_policy(&self, consumer: &str) -> Result<(i64, u32)> {
        let row = sqlx::query(
            "SELECT ack_wait_ms, max_deliver FROM broker_consumers WHERE consumer_name = $1",
        )
        .bind(consumer)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        let Some(row) = row else {
            return Err(AppError::not_found("broker consumer", consumer));
        };
        let ack_wait_ms: i32 = row.try_get("ack_wait_ms").map_err(map_sqlx)?;
        let max_deliver: i32 = row.try_get("max_deliver").map_err(map_sqlx)?;
        Ok((
            i64::from(ack_wait_ms),
            u32::try_from(max_deliver).unwrap_or(1),
        ))
    }

    async fn fetch_durable(&self, consumer: &str, max: usize) -> Result<Vec<BrokerMessage>> {
        let (ack_wait_ms, max_deliver) = self.durable_policy(consumer).await?;
        if max == 0 {
            return Ok(Vec::new());
        }
        let now = Timestamp::now();
        let ack_wait = Duration::from_millis(u64::try_from(ack_wait_ms).unwrap_or(30_000));
        let redeliver_after = now.checked_add(ack_wait).ok_or_else(|| {
            AppError::validation("durable consumer ack_wait overflows receipt hint timestamp")
        })?;
        // Fetching is intentionally read-only. The next component, the durable
        // attempt coordinator, atomically installs status, worker fence and
        // attempt row together. That keeps the database lifecycle singular.
        // The query deliberately over-scans the local number of outstanding
        // hints. Otherwise a high-priority row already in this process's
        // flight ledger could occupy every SQL result slot and starve a later
        // ready row even though the consumer has capacity for it.
        let scan_limit = max.saturating_add(self.flight.len());
        let rows = sqlx::query(FETCH_SQL)
            .bind(consumer)
            .bind(i64::try_from(scan_limit).unwrap_or(i64::MAX))
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx)?;
        let mut messages = Vec::with_capacity(max);
        for row in rows {
            if messages.len() >= max {
                break;
            }
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

            // Delivery count is persisted by the worker's receipt port, not
            // inferred from this process's memory. It is transport telemetry
            // and backoff input only: `task_attempts` is the authoritative
            // attempt budget, so a burst of duplicate hints must never
            // dead-letter a task before a durable claim decides its outcome.
            // Keep the persisted counter within the configured broker bound;
            // saturation is intentionally non-terminal.
            let prior_deliveries: i64 = row.try_get("prior_deliveries").map_err(map_sqlx)?;
            let deliveries = u32::try_from(prior_deliveries.saturating_add(1))
                .unwrap_or(max_deliver)
                .clamp(1, max_deliver);
            // This sequence identifies the broker message, while the worker
            // allocates a separate receipt for every delivery before work.
            // Do not emit the same unacknowledged hint repeatedly from this
            // process; cross-replica duplicates remain safe because the
            // durable attempt claim is the ownership boundary.
            let sequence = self.durable_message_sequence(id).await?;
            if !self
                .flight
                .record_if_absent(sequence, id, redeliver_after, &now)
            {
                continue;
            }
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
                deliveries,
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
        let task_id = if result.rows_affected() == 0 {
            // Duplicate publish: idempotent collapse to the original row.
            sqlx::query_scalar("SELECT id FROM tasks WHERE tenant_id = $1 AND idempotency_key = $2")
                .bind(Uuid::from(message.tenant_id))
                .bind(&message.idempotency_key)
                .fetch_optional(&self.pool)
                .await
                .map_err(map_sqlx)?
                .ok_or_else(|| AppError::database("idempotency conflict without surviving row"))?
        } else {
            Uuid::from(message.task_id)
        };
        self.durable_message_sequence(task_id).await
    }

    async fn ensure_consumer(&self, config: ConsumerConfig) -> Result<()> {
        let config = config.validated()?;
        let ack_wait_ms = i32::try_from(config.ack_wait.as_millis()).map_err(|_| {
            AppError::validation("ack_wait is too large for durable consumer storage")
        })?;
        let max_deliver = i32::try_from(config.max_deliver).map_err(|_| {
            AppError::validation("max_deliver is too large for durable consumer storage")
        })?;
        // Insert once, then compare every registered field. Updating an
        // existing durable consumer would change recovery behavior underneath
        // live workers, so disagreement is a boot failure rather than a
        // last-writer-wins configuration change.
        sqlx::query(
            "INSERT INTO broker_consumers (consumer_name, ack_wait_ms, max_deliver)
             VALUES ($1, $2, $3)
             ON CONFLICT (consumer_name) DO NOTHING",
        )
        .bind(&config.name)
        .bind(ack_wait_ms)
        .bind(max_deliver)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        let (stored_ack_wait_ms, stored_max_deliver) = self.durable_policy(&config.name).await?;
        if stored_ack_wait_ms != i64::from(ack_wait_ms)
            || stored_max_deliver != u32::try_from(max_deliver).unwrap_or_default()
        {
            return Err(AppError::conflict(format!(
                "durable consumer '{}' exists with ack_wait_ms={} and max_deliver={}, \
                 not requested ack_wait_ms={} and max_deliver={}",
                config.name, stored_ack_wait_ms, stored_max_deliver, ack_wait_ms, max_deliver
            )));
        }
        // Event subjects still use the in-memory adapter in the PostgreSQL
        // task lane, so register the same validated policy there too.
        self.events.ensure_consumer(config).await
    }

    async fn fetch(&self, consumer: &str, max: usize) -> Result<Vec<BrokerMessage>> {
        let mut messages = self.fetch_durable(consumer, max).await?;
        if messages.len() < max {
            let remainder = self.events.fetch(consumer, max - messages.len()).await?;
            messages.extend(remainder);
        }
        Ok(messages)
    }

    async fn ack(&self, consumer: &str, sequence: u64, instruction: AckInstruction) -> Result<()> {
        // Read the shared persisted policy before changing local flight state.
        // A database outage must leave the mapping intact so the worker can
        // retry the same settlement or let the durable claim expire safely.
        let (ack_wait_ms, _max_deliver) = self.durable_policy(consumer).await?;
        let ack_wait = Duration::from_millis(u64::try_from(ack_wait_ms).unwrap_or(30_000));
        let Some(row_id) = self.flight.get(sequence) else {
            // Not a durable-lane claim: route to the event lane.
            return self.events.ack(consumer, sequence, instruction).await;
        };
        let terminal = !matches!(instruction, AckInstruction::InProgress);
        let result = match instruction {
            AckInstruction::Ack => {
                // A terminal lifecycle is the usual ACK case. `running` is
                // also valid here: another replica may have won the durable
                // claim for the same read-only delivery hint, and acknowledging
                // this losing hint must not interfere with the real owner.
                let status: Option<String> =
                    sqlx::query_scalar("SELECT status FROM tasks WHERE id = $1")
                        .bind(row_id)
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(map_sqlx)?;
                match status.as_deref() {
                    Some("completed" | "failed" | "cancelled" | "dead_lettered" | "running") => {
                        Ok(())
                    },
                    Some(other) => Err(AppError::conflict(format!(
                        "task {} acknowledged while still '{}' without durable ownership",
                        row_id, other
                    ))),
                    None => Err(AppError::not_found("task", row_id.to_string())),
                }
            },
            AckInstruction::Nak { delay } => {
                let not_before = chrono::Utc::now() + delay.unwrap_or(ack_wait);
                sqlx::query(NAK_SQL)
                    .bind(row_id)
                    .bind(not_before)
                    .execute(&self.pool)
                    .await
                    .map_err(map_sqlx)?;
                Ok(())
            },
            AckInstruction::Term => {
                // Normal worker TERM follows a durable terminal settlement and
                // this is a no-op. The guarded update only covers a poisoned,
                // undecodable queued frame: there is no usable payload from
                // which the coordinator can form a task claim. Crucially it is
                // never used for `max_deliver`; delivery count is telemetry,
                // not the durable retry budget.
                sqlx::query(POISON_QUARANTINE_SQL)
                    .bind(row_id)
                    .execute(&self.pool)
                    .await
                    .map_err(map_sqlx)?;
                let status: Option<String> =
                    sqlx::query_scalar("SELECT status FROM tasks WHERE id = $1")
                        .bind(row_id)
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(map_sqlx)?;
                match status.as_deref() {
                    Some("completed" | "failed" | "cancelled" | "dead_lettered" | "running") => {
                        Ok(())
                    },
                    Some(other) => Err(AppError::conflict(format!(
                        "task {} terminated while still '{}' without durable lifecycle settlement",
                        row_id, other
                    ))),
                    None => Err(AppError::not_found("task", row_id.to_string())),
                }
            },
            AckInstruction::InProgress => {
                // The task attempt heartbeat renews `locked_until` through the
                // durable execution port. The broker has no independent claim
                // lease to extend, so changing task state here would recreate
                // the split-ownership bug this adapter avoids. It does renew
                // this *process-local hint* window, matching normal broker
                // ack-wait semantics without mutating lifecycle state.
                let redeliver_after = Timestamp::now().checked_add(ack_wait).ok_or_else(|| {
                    AppError::validation("durable consumer ack_wait overflows hint timestamp")
                })?;
                let _ = self.flight.extend(sequence, redeliver_after);
                let _ = row_id;
                Ok(())
            },
        };
        if result.is_ok() && terminal {
            let _ = self.flight.take(sequence);
        }
        result
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
                sequence: self.durable_message_sequence(id).await?,
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
    fn local_hint_expiry_recovers_an_unsettled_duplicate_without_reemitting_a_heartbeat() {
        let ledger = FlightLedger::default();
        let task = Uuid::now_v7();
        let now = Timestamp::from_unix_seconds(1_700_000_000).expect("timestamp");
        let initial_expiry = now.checked_add(Duration::from_secs(10)).expect("timestamp");
        assert!(ledger.record_if_absent(17, task, initial_expiry, &now));
        assert!(
            !ledger.record_if_absent(17, task, initial_expiry, &now),
            "a live local hint is suppressed"
        );

        let renewed_expiry = now.checked_add(Duration::from_secs(20)).expect("timestamp");
        assert!(ledger.extend(17, renewed_expiry));
        let after_original_window = now.checked_add(Duration::from_secs(11)).expect("timestamp");
        assert!(
            !ledger.record_if_absent(17, task, renewed_expiry, &after_original_window),
            "a heartbeat extends only the local hint window"
        );
        let after_renewal = now.checked_add(Duration::from_secs(21)).expect("timestamp");
        assert!(
            ledger.record_if_absent(17, task, renewed_expiry, &after_renewal),
            "an unacknowledged duplicate becomes recoverable after its window"
        );
    }

    #[test]
    fn durable_message_ids_are_database_allocated() {
        let source =
            include_str!("../../../../migrations/0033_delivery_and_replay_receipts/up.sql");
        assert!(source.contains("broker_task_messages"));
        assert!(source.contains("message_sequence bigint GENERATED ALWAYS AS IDENTITY"));
        assert!(!source.contains("uuid-v7 first 8 bytes"));
    }

    #[test]
    fn fetch_sql_leaves_lifecycle_claiming_to_the_attempt_coordinator() {
        for token in [
            "status = 'queued'",
            "not_before <= now()",
            "ORDER BY",
            "LIMIT $2",
        ] {
            assert!(FETCH_SQL.contains(token), "fetch SQL missing {token}");
        }
        assert!(
            !FETCH_SQL.contains("UPDATE tasks"),
            "the broker must not transition a task before the durable attempt claim"
        );
        assert!(
            NAK_SQL.contains("t.status = 'queued' AND t.locked_by IS NULL"),
            "nak may pace only an unowned queued task"
        );
        assert!(
            POISON_QUARANTINE_SQL.contains("t.status = 'queued' AND t.locked_by IS NULL")
                && POISON_QUARANTINE_SQL.contains("broker_delivery_receipts"),
            "the explicit poison path must not overwrite owned or receipt-reserved work"
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
