//! PURPOSE: Turns due schedules into outbound calls, and signs them.
//!
//! Owning layer: `mas-scheduler-service` (the scheduling plane).
//!
//! Production responsibility: for each due run, build the request, prove the
//! scheduler's identity with the internal HMAC over the exact bytes about to
//! be sent, and record the dispatch in the in-flight ledger so a completion
//! can be matched to work this replica actually issued.
//!
//! Security boundary: every outbound call passes the SSRF guard, and every
//! call carries a signature bound to its body and to the moment it was made.
//! Being on the cluster network is not an identity, and a captured request
//! cannot be replayed an hour later.
//!
//! It must NOT:
//!   • sign a message other than the exact bytes transmitted;
//!   • send a static bearer token — a replayable credential is not an
//!     identity;
//!   • let the schedule row's tenant be overridden by anything in flight.

//! Dispatch adapters between the scheduler runtime and downstream
//! executors, plus the in-flight ledger that makes overlap honest.
//!
//! The runtime asks `is_running(schedule)` during overlap checks; the
//! dispatcher owns that answer. For [`HttpDispatcher`] the ledger is fed by
//! two directions:
//!
//! ```text
//! scheduler tick ──dispatch(w)──▶ API /v1/internal/schedule-runs
//!                                      │ 202 + execution_id
//!                                      ▼
//!                                 ledger.insert(schedule)   …now Running
//!
//! executor finishes ──POST──▶ this service's /v1/scheduler/completions
//!                                      ▼
//!                                 ledger.remove(schedule)   …dropped out
//! ```
//!
//! Downstream crashes never leak a stuck "running" flag permanently in this
//! design because stale ledger entries are bounded by
//! [`InFlightLedger::reap_older_than`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, ScheduleId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::value_objects::SafeUrl;
use mas_integrations::http_guard::{EgressPolicy, GuardedHttpClient, HttpRequest};
use mas_scheduling::due::DueWork;
use mas_scheduling::runner::{DispatchAck, DispatchPort};
use mas_security::internal_service::{sign, SERVICE_HEADER, SIGNATURE_HEADER};
use serde::{Deserialize, Serialize};

use crate::egress::ReqwestEgress;

/// Shared, cheap-to-duplicate in-flight ledger keyed by schedule. Each
/// entry remembers *when* it was inserted so a reaper can clear stranded
/// rows (downstream went away without posting completion).
#[derive(Debug, Clone, Default)]
pub struct InFlightLedger {
    inner: Arc<Mutex<HashMap<ScheduleId, Timestamp>>>,
}

impl InFlightLedger {
    /// Marks a schedule as running (insert is idempotent).
    pub fn start(&self, schedule: ScheduleId) {
        self.lock().insert(schedule, Timestamp::now());
    }

    /// Clears a schedule (finish is idempotent — unknown ids are fine).
    pub fn finish(&self, schedule: ScheduleId) {
        self.lock().remove(&schedule);
    }

    /// Whether the schedule is currently marked in flight.
    #[must_use]
    pub fn is_running(&self, schedule: ScheduleId) -> bool {
        self.lock().contains_key(&schedule)
    }

    /// Live count (observability).
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Is it empty?
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drops entries older than `age`, returning how many it removed.
    /// Scheduled-run completion is expected in minutes; anything older
    /// signals a dead downstream run.
    pub fn reap_older_than(&self, age: Duration) -> usize {
        let too_old = Timestamp::now()
            .checked_sub(age)
            .unwrap_or_else(|| Timestamp::from_unix_ms(0).unwrap_or_else(|_| Timestamp::now()));
        let mut inner = self.lock();
        let before = inner.len();
        inner.retain(|_, started| !started.is_before(&too_old));
        before - inner.len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<ScheduleId, Timestamp>> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Dev adapter: runs "start" and "finish" instantly. The ledger is
/// consulted transparently (`is_running` inside overlap checks) but a
/// loopback run is done before the next tick, so overlap never triggers.
#[derive(Debug, Clone)]
pub struct LoopbackDispatcher {
    ledger: InFlightLedger,
    /// Number of runs dispatched (dev/demo observability).
    pub dispatched: Arc<std::sync::atomic::AtomicU64>,
}

impl Default for LoopbackDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl LoopbackDispatcher {
    /// Empty ledger, zero counter.
    #[must_use]
    pub fn new() -> Self {
        Self {
            ledger: InFlightLedger::default(),
            dispatched: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Access the shared ledger.
    #[must_use]
    pub fn ledger(&self) -> &InFlightLedger {
        &self.ledger
    }
}

#[async_trait]
impl DispatchPort for LoopbackDispatcher {
    async fn dispatch(&self, work: &DueWork) -> Result<DispatchAck> {
        self.ledger.start(work.schedule_id);
        self.dispatched
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let execution = ExecutionId::new();
        // Loopback completes instantly: the run is gone before the next
        // overlap check.
        self.ledger.finish(work.schedule_id);
        Ok(DispatchAck::Started { execution })
    }

    async fn is_running(&self, schedule: ScheduleId) -> Result<bool> {
        Ok(self.ledger.is_running(schedule))
    }

    async fn mark_finished(&self, schedule: ScheduleId) -> Result<()> {
        self.ledger.finish(schedule);
        Ok(())
    }
}

/// Wire contract for this service's completion listener.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionNotice {
    /// The schedule whose run just finished.
    pub schedule_id: ScheduleId,
    /// The execution that carried it (informational).
    pub execution_id: ExecutionId,
    /// Terminal outcome string (`"succeeded"` | `"failed"` | `"cancelled"`).
    pub outcome: String,
}

/// POST body to the API's internal schedule-run endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleRunRequest {
    /// Schedule that fired.
    pub schedule_id: ScheduleId,
    /// Owning tenant (API scope check).
    pub tenant_id: mas_common::ids::TenantId,
    /// The planned fire time (idempotency key material for the API).
    pub planned_at: Timestamp,
    /// Opaque task template from the schedule aggregate.
    pub task_template: serde_json::Value,
}

impl ScheduleRunRequest {
    /// Projection from a [`DueWork`].
    #[must_use]
    pub fn from_work(work: &DueWork) -> Self {
        Self {
            schedule_id: work.schedule_id,
            tenant_id: work.tenant_id,
            planned_at: work.planned_at,
            task_template: work.task_template.clone(),
        }
    }

    /// Stable idempotency header value: schedule + planned tick.
    #[must_use]
    pub fn idempotency_key(&self) -> String {
        format!(
            "run:{}:{}",
            self.schedule_id,
            self.planned_at.to_unix_seconds()
        )
    }
}

/// Response body from the API when a run is accepted.
#[derive(Debug, Clone, Deserialize)]
pub struct ScheduleRunAccepted {
    /// Created execution tracking id.
    pub execution_id: ExecutionId,
}

/// Production adapter: dispatch over HTTP to the API service, with a local
/// ledger + completion listener (see module docs) for overlap hygiene.
///
/// Every request goes through the SSRF guard. The base URL comes from
/// configuration, and configuration is data: a misconfigured or tampered
/// value must not be able to turn the scheduler into a probe inside the
/// cluster's own network or against a cloud metadata endpoint.
#[derive(Debug)]
pub struct HttpDispatcher {
    ledger: InFlightLedger,
    client: GuardedHttpClient<ReqwestEgress>,
    /// Endpoint accepting `POST ScheduleRunRequest` (path `/v1/internal/schedule-runs`).
    base_url: String,
    /// Shared secret used to sign internal calls to the API.
    ///
    /// Being on the cluster network is not an identity, so the scheduler
    /// proves itself with a body-bound, time-bounded HMAC rather than with a
    /// static bearer token that any process on the network could replay.
    internal_secret: Option<mas_security::SecretString>,
    /// The service id this process presents.
    service_id: String,
}

impl HttpDispatcher {
    /// Builds a dispatcher against an API base URL (`http://host:port`).
    ///
    /// The destination is validated once here, and again by the guard on
    /// every request and every redirect hop.
    ///
    /// # Errors
    /// Fails when the base URL is not `http(s)`, when it cannot be parsed as a
    /// `SafeUrl`, or when the egress client cannot be constructed.
    pub fn new(
        ledger: InFlightLedger,
        base_url: &str,
        internal_secret: Option<mas_security::SecretString>,
        service_id: &str,
        policy: EgressPolicy,
    ) -> Result<Self> {
        if !(base_url.starts_with("http://") || base_url.starts_with("https://")) {
            return Err(AppError::validation(
                "base_url must start with http:// or https://",
            ));
        }
        let base_url = base_url.trim_end_matches('/').to_owned();
        // The API is an operator-declared internal endpoint: it lives on the
        // cluster network, so the URL is parsed as one and the policy decides
        // whether that class of destination is permitted at all.
        //
        // Fail at construction, not at the first dispatch: a scheduler that
        // cannot reach its API is a scheduler that should not have started.
        let runs =
            SafeUrl::parse_internal_endpoint(&format!("{base_url}/v1/internal/schedule-runs"))?;
        policy.check_destination(&runs)?;
        let client = GuardedHttpClient::new(ReqwestEgress::new(policy.clone()), policy);
        Ok(Self {
            ledger,
            client,
            base_url,
            internal_secret,
            service_id: service_id.to_owned(),
        })
    }

    /// The shared ledger (hand a clone to [`crate::server::completion_router`]).
    #[must_use]
    pub fn ledger(&self) -> &InFlightLedger {
        &self.ledger
    }

    /// Dispatch endpoint URL (path joined once).
    #[must_use]
    pub fn runs_url(&self) -> String {
        format!("{}/v1/internal/schedule-runs", self.base_url)
    }

    /// The egress policy this dispatcher enforces.
    #[must_use]
    pub fn policy(&self) -> &EgressPolicy {
        self.client.policy()
    }
}

#[async_trait]
impl DispatchPort for HttpDispatcher {
    async fn dispatch(&self, work: &DueWork) -> Result<DispatchAck> {
        let body = ScheduleRunRequest::from_work(work);
        let url = SafeUrl::parse_internal_endpoint(&self.runs_url())?;
        let payload = serde_json::to_vec(&body)
            .map_err(|err| AppError::internal(format!("schedule-run body: {err}")))?;
        let mut request = HttpRequest::post(url, payload)
            .with_header("content-type", "application/json")
            .with_header("idempotency-key", &body.idempotency_key())
            .with_header("accept", "application/json")
            .with_header(SERVICE_HEADER, &self.service_id);
        if let Some(secret) = &self.internal_secret {
            // Sign the exact bytes that are about to be sent. Re-serializing
            // after signing would produce a MAC over a different message than
            // the receiver verifies.
            let signature = sign(
                secret.expose().as_bytes(),
                &self.service_id,
                &Timestamp::now(),
                &request.body,
            );
            request = request.with_header(SIGNATURE_HEADER, &signature);
        }
        // The guard validates the destination, pins DNS, re-validates every
        // redirect hop and caps the body.
        let response = self.client.send(&request).await.map_err(|err| {
            AppError::external_service("mas-api", format!("schedule-run request failed: {err}"))
        })?;
        let status = response.status;
        if response.is_success() {
            let accepted: ScheduleRunAccepted =
                serde_json::from_slice(&response.body).map_err(|err| {
                    AppError::external_service("mas-api", format!("invalid acceptance body: {err}"))
                })?;
            self.ledger.start(work.schedule_id);
            return Ok(DispatchAck::Started {
                execution: accepted.execution_id,
            });
        }
        // Capacity/fairness back-pressure → Deferred: the runtime requeues.
        if status == 409 || status == 429 {
            return Ok(DispatchAck::Deferred);
        }
        let text = String::from_utf8_lossy(&response.body);
        Err(AppError::external_service(
            "mas-api",
            format!("schedule-run rejected ({status}): {}", truncate(&text, 256)),
        ))
    }

    async fn is_running(&self, schedule: ScheduleId) -> Result<bool> {
        Ok(self.ledger.is_running(schedule))
    }

    async fn mark_finished(&self, schedule: ScheduleId) -> Result<()> {
        self.ledger.finish(schedule);
        Ok(())
    }
}

fn truncate(text: &str, max: usize) -> &str {
    if text.len() <= max {
        text
    } else {
        &text[..max.min(text.len())]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::TenantId;

    #[test]
    fn ledger_lifecycle_and_reap() {
        let ledger = InFlightLedger::default();
        let schedule = ScheduleId::new();
        ledger.start(schedule);
        assert!(ledger.is_running(schedule));
        assert_eq!(ledger.len(), 1);
        // Finish is idempotent.
        ledger.finish(schedule);
        ledger.finish(schedule);
        assert!(!ledger.is_running(schedule));
        assert!(ledger.is_empty());

        // Backdate an entry so the reaper clears it.
        ledger.start(schedule);
        {
            let mut inner = ledger.inner.lock().expect("lock");
            inner.insert(schedule, Timestamp::from_unix_seconds(0).expect("epoch"));
        }
        assert_eq!(ledger.reap_older_than(Duration::from_secs(60)), 1);
        assert!(ledger.is_empty());
    }

    #[test]
    fn request_projection_carries_identity() {
        let work = DueWork {
            schedule_id: ScheduleId::new(),
            tenant_id: TenantId::new(),
            planned_at: Timestamp::from_unix_seconds(1_700_000_000).expect("ts"),
            task_template: serde_json::json!({"operation": "overview.refresh"}),
        };
        let request = ScheduleRunRequest::from_work(&work);
        assert!(request.idempotency_key().contains("1700000000"));
        assert_eq!(request.schedule_id, work.schedule_id);
    }

    #[tokio::test]
    async fn loopback_starts_and_finishes_before_the_next_tick() {
        let dispatcher = LoopbackDispatcher::new();
        let work = DueWork {
            schedule_id: ScheduleId::new(),
            tenant_id: TenantId::new(),
            planned_at: Timestamp::now(),
            task_template: serde_json::json!({}),
        };
        let ack = DispatcherGuard::dispatch(&dispatcher, &work)
            .await
            .expect("ack");
        assert!(matches!(ack, DispatchAck::Started { .. }));
        // Instant completion: the ledger must be empty (never wedged).
        assert!(!dispatcher.ledger.is_running(work.schedule_id));
        assert_eq!(
            dispatcher
                .dispatched
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    // Thin alias keeps the trait method disambiguated and greppable.
    struct DispatcherGuard;
    impl DispatcherGuard {
        async fn dispatch(d: &LoopbackDispatcher, w: &DueWork) -> Result<DispatchAck> {
            DispatchPort::dispatch(d, w).await
        }
    }
}
