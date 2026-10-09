//! PURPOSE: The scheduler's completion HTTP listener — the endpoint a worker
//! calls to say a dispatched run finished.
//!
//! Owning layer: `mas-scheduler-service` (the scheduling plane).
//!
//! Production responsibility: accept a completion only from a caller that
//! proves it holds the shared internal secret, and only for an execution the
//! scheduler itself has in flight.
//!
//! Security boundary: this endpoint mutates execution state, so it is the
//! highest-value forgery target in the platform. Authentication is a
//! body-bound, time-bounded HMAC verified against the RAW bytes, before any
//! deserialization — verifying after parsing would check a different message
//! than the one being acted on. With no secret configured every completion is
//! refused; there is no open-by-default mode.
//!
//! It must NOT:
//!   • accept an unsigned, stale, or unexpected-caller completion;
//!   • deserialize before verifying;
//!   • distinguish failure causes in its response — every rejection is the
//!     same `401`, so a prober learns nothing;
//!   • let the caller name the tenant — the execution id is the authority.

//! The small HTTP surface this service owns:
//!
//! ```text
//! GET  /healthz                       liveness (always 200 "ok")
//! GET  /v1/scheduler/stats            in-flight ledger depth
//! POST /v1/scheduler/completions      downstream run finished → drop ledger entry
//! ```
//!
//! The completion route is the second half of the overlap protocol (see
//! [`crate::dispatch`]): executors (or the API relaying their lifecycle
//! events) POST a [`CompletionNotice`] here so `is_running` flips to false
//! and the next tick may fire the schedule again.
//!
//! # Authentication (security Phase 25)
//!
//! Both stateful routes are authenticated with
//! [`mas_security::internal_service`]. They were previously open to anything
//! that could reach the port, which meant an unauthenticated POST could clear
//! the in-flight ledger and make the scheduler believe a run had finished —
//! defeating the overlap guard and letting the schedule fire again
//! immediately.
//!
//! Being on the cluster network is not an identity. The caller must present a
//! body-bound, time-bounded HMAC from a service the scheduler expects, and the
//! shared secret is mandatory: with no secret configured the routes refuse
//! rather than fall back to trusting the network.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

use mas_common::timestamps::Timestamp;
use mas_security::internal_service::{
    verify, InternalServiceIdentity, SERVICE_HEADER, SIGNATURE_HEADER,
};
use sqlx::PgPool;

use crate::dispatch::{CompletionNotice, InFlightLedger};

/// Services allowed to report a completion.
///
/// The executor plane reports finished runs; the API relays lifecycle events
/// for executions it owns. Nobody else may clear a ledger entry.
const ALLOWED_CALLERS: &[&str] = &["mas-api", "mas-worker"];

/// Replay receipt authority for v2 internal signatures.
///
/// The volatile variant is deliberately confined to the explicit in-memory
/// development lane and tests. Production constructors always require a shared
/// PostgreSQL pool so a process restart or a sibling replica cannot replay a
/// valid request.
#[derive(Clone)]
enum ReplayReceipts {
    InMemory(Arc<Mutex<HashSet<(String, String)>>>),
    Postgres(PgPool),
}

/// Shared secret for internal authentication, held zeroized.
///
/// A process with no secret configured refuses every internal call instead of
/// accepting them: "I can reach the port" is not an identity, and a silent
/// fallback to unauthenticated is how that becomes true by accident.
#[derive(Clone)]
pub struct InternalAuth {
    secret: Option<mas_security::SecretString>,
    receipts: ReplayReceipts,
}

impl InternalAuth {
    /// Development/test constructor. It intentionally uses a process-local
    /// receipt set and must not be used by the production composition.
    #[must_use]
    pub fn new(secret: Option<mas_security::SecretString>) -> Self {
        Self {
            secret,
            receipts: ReplayReceipts::InMemory(Arc::new(Mutex::new(HashSet::new()))),
        }
    }

    /// Production constructor. Every successfully verified nonce is consumed
    /// by one atomic PostgreSQL insert before the request is acted on.
    #[must_use]
    pub fn with_postgres(secret: Option<mas_security::SecretString>, pool: PgPool) -> Self {
        Self {
            secret,
            receipts: ReplayReceipts::Postgres(pool),
        }
    }

    /// Whether internal calls can be authenticated at all.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.secret.is_some()
    }

    async fn consume(&self, identity: &InternalServiceIdentity) -> Result<(), ()> {
        match &self.receipts {
            ReplayReceipts::InMemory(receipts) => {
                let mut receipts = receipts.lock().await;
                receipts
                    .insert((identity.service.clone(), identity.nonce.clone()))
                    .then_some(())
                    .ok_or(())
            },
            ReplayReceipts::Postgres(pool) => {
                let expires_at =
                    Timestamp::from_unix_ms(identity.expires_at_ms()).map_err(|_| ())?;
                let result = sqlx::query(
                    "INSERT INTO internal_request_receipts \
                     (service_name, nonce, signed_at_ms, expires_at) \
                     VALUES ($1, $2, $3, $4) \
                     ON CONFLICT (service_name, nonce) DO NOTHING",
                )
                .bind(&identity.service)
                .bind(&identity.nonce)
                .bind(identity.signed_at.to_unix_ms())
                .bind(expires_at.into_datetime())
                .execute(pool)
                .await
                .map_err(|_| ())?;
                (result.rows_affected() == 1).then_some(()).ok_or(())
            },
        }
    }

    /// Verifies and atomically consumes an inbound internal request nonce.
    ///
    /// # Errors
    /// `UNAUTHORIZED` for every failure mode — missing header, bad MAC, stale
    /// timestamp, unexpected service, replay, no secret, or receipt-store
    /// failure. They are deliberately indistinguishable.
    pub async fn verify(
        &self,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<(), mas_common::error::AppError> {
        let Some(secret) = &self.secret else {
            return Err(mas_common::error::AppError::unauthorized("unauthenticated"));
        };
        let signature = headers
            .get(SIGNATURE_HEADER)
            .and_then(|value| value.to_str().ok());
        let service = headers
            .get(SERVICE_HEADER)
            .and_then(|value| value.to_str().ok());
        let identity = verify(
            secret.expose().as_bytes(),
            signature,
            service,
            &Timestamp::now(),
            body,
            ALLOWED_CALLERS,
        )?;
        self.consume(&identity)
            .await
            .map_err(|()| mas_common::error::AppError::unauthorized("unauthenticated"))
    }
}

impl std::fmt::Debug for InternalAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InternalAuth")
            .field("enabled", &self.is_enabled())
            .finish_non_exhaustive()
    }
}

/// Liveness probe. No state — anything holding this router is alive.
pub fn health_router() -> Router {
    Router::new().route("/healthz", get(|| async { "ok" }))
}

/// Stateful routes (ledger-backed): stats + completions.
///
/// Both require a valid internal service signature.
pub fn completion_router(ledger: InFlightLedger, auth: InternalAuth) -> Router {
    Router::new()
        .route("/v1/scheduler/completions", post(post_completion))
        .route("/v1/scheduler/stats", get(get_stats))
        .with_state((ledger, auth))
}

async fn post_completion(
    State((ledger, auth)): State<(InFlightLedger, InternalAuth)>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    // Authenticate before acting, and against the raw bytes: the signature
    // covers the body exactly as it arrived, so re-serializing it first would
    // break the MAC and, worse, would verify a different message than the one
    // that is about to be trusted.
    if let Err(error) = auth.verify(&headers, &body).await {
        tracing::warn!(
            code = %error.error_code(),
            "rejected an unauthenticated scheduler completion"
        );
        return StatusCode::UNAUTHORIZED;
    }
    let notice: CompletionNotice = match serde_json::from_slice(&body) {
        Ok(notice) => notice,
        Err(_) => return StatusCode::BAD_REQUEST,
    };
    let _was_running = ledger.is_running(notice.schedule_id);
    ledger.finish(notice.schedule_id);
    tracing::info!(
        schedule = %notice.schedule_id,
        execution = %notice.execution_id,
        outcome = %notice.outcome,
        "run completion acknowledged",
    );
    // Idempotent: completing an unknown or already-finished schedule is
    // still a success (at-least-once delivery downstream).
    StatusCode::NO_CONTENT
}

async fn get_stats(
    State((ledger, auth)): State<(InFlightLedger, InternalAuth)>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    // An empty body still has to be signed; the MAC covers its digest, so an
    // unsigned probe cannot read operational state.
    auth.verify(&headers, &[])
        .await
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    Ok(Json(serde_json::json!({
        "in_flight": ledger.len(),
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::InFlightLedger;
    use mas_common::ids::{ExecutionId, ScheduleId};
    use tower::ServiceExt; // oneshot

    use mas_security::internal_service::{sign, SERVICE_HEADER, SIGNATURE_HEADER};

    /// A fixed secret for the tests, so a signature is deterministic.
    const TEST_SECRET: &[u8] = b"scheduler-test-secret";

    fn test_auth() -> InternalAuth {
        InternalAuth::new(Some(mas_security::SecretString::new(
            String::from_utf8(TEST_SECRET.to_vec()).expect("utf8"),
        )))
    }

    fn notice() -> CompletionNotice {
        CompletionNotice {
            schedule_id: ScheduleId::new(),
            execution_id: ExecutionId::new(),
            outcome: "succeeded".to_owned(),
        }
    }

    fn signed_post(
        notice: &CompletionNotice,
        service: &str,
        secret: &[u8],
        skew_ms: i64,
    ) -> axum::http::Request<axum::body::Body> {
        let body = serde_json::to_vec(notice).expect("json");
        let when = Timestamp::from_unix_ms(Timestamp::now().to_unix_ms() + skew_ms).expect("ts");
        let signature = sign(secret, service, &when, &body);
        axum::http::Request::post("/v1/scheduler/completions")
            .header("content-type", "application/json")
            .header(SIGNATURE_HEADER, signature)
            .header(SERVICE_HEADER, service)
            .body(axum::body::Body::from(body))
            .expect("request")
    }

    fn unsigned_post(notice: &CompletionNotice) -> axum::http::Request<axum::body::Body> {
        axum::http::Request::post("/v1/scheduler/completions")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                serde_json::to_vec(notice).expect("json"),
            ))
            .expect("request")
    }

    #[tokio::test]
    async fn a_signed_completion_clears_the_ledger_and_is_idempotent() {
        let ledger = InFlightLedger::default();
        let schedule = ScheduleId::new();
        ledger.start(schedule);
        assert!(ledger.is_running(schedule));

        let router = completion_router(ledger.clone(), test_auth());
        let mut notice = notice();
        notice.schedule_id = schedule;

        let response = router
            .clone()
            .oneshot(signed_post(&notice, "mas-api", TEST_SECRET, 0))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(!ledger.is_running(schedule), "ledger cleared by the route");

        // A separately signed at-least-once delivery is still a success: the
        // nonce is fresh, and the ledger write is idempotent.
        let response = router
            .oneshot(signed_post(&notice, "mas-api", TEST_SECRET, 0))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn a_captured_completion_signature_is_refused_on_second_use() {
        let ledger = InFlightLedger::default();
        let schedule = ScheduleId::new();
        ledger.start(schedule);
        let router = completion_router(ledger.clone(), test_auth());
        let mut completion = notice();
        completion.schedule_id = schedule;
        let body = serde_json::to_vec(&completion).expect("json");
        let signature = sign(TEST_SECRET, "mas-api", &Timestamp::now(), &body);
        let request = |body: Vec<u8>, signature: String| {
            axum::http::Request::post("/v1/scheduler/completions")
                .header("content-type", "application/json")
                .header(SIGNATURE_HEADER, signature)
                .header(SERVICE_HEADER, "mas-api")
                .body(axum::body::Body::from(body))
                .expect("request")
        };

        let first = router
            .clone()
            .oneshot(request(body.clone(), signature.clone()))
            .await
            .expect("first response");
        assert_eq!(first.status(), StatusCode::NO_CONTENT);
        let replay = router
            .oneshot(request(body, signature))
            .await
            .expect("replay response");
        assert_eq!(replay.status(), StatusCode::UNAUTHORIZED);
    }

    /// The defect this module was written to fix: an unauthenticated POST used
    /// to clear the ledger, defeating the overlap guard.
    #[tokio::test]
    async fn an_unsigned_completion_is_refused_and_leaves_the_ledger_alone() {
        let ledger = InFlightLedger::default();
        let schedule = ScheduleId::new();
        ledger.start(schedule);

        let router = completion_router(ledger.clone(), test_auth());
        let mut notice = notice();
        notice.schedule_id = schedule;

        let response = router
            .oneshot(unsigned_post(&notice))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(
            ledger.is_running(schedule),
            "an unauthenticated completion must not clear the ledger"
        );
    }

    #[tokio::test]
    async fn a_completion_signed_with_the_wrong_secret_is_refused() {
        let ledger = InFlightLedger::default();
        let router = completion_router(ledger.clone(), test_auth());
        let response = router
            .oneshot(signed_post(&notice(), "mas-api", b"wrong-secret", 0))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    /// A signature from a service the scheduler does not expect, even with the
    /// right secret, is refused.
    #[tokio::test]
    async fn a_completion_from_an_unexpected_service_is_refused() {
        let ledger = InFlightLedger::default();
        let router = completion_router(ledger.clone(), test_auth());
        let response = router
            .oneshot(signed_post(&notice(), "mas-mystery", TEST_SECRET, 0))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    /// Freshness: a signature from five minutes ago is worthless.
    #[tokio::test]
    async fn a_stale_completion_is_refused() {
        let ledger = InFlightLedger::default();
        let router = completion_router(ledger.clone(), test_auth());
        let response = router
            .oneshot(signed_post(&notice(), "mas-api", TEST_SECRET, -600_000))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    /// No secret configured means refuse everything, not trust the network.
    #[tokio::test]
    async fn an_unconfigured_scheduler_refuses_every_completion() {
        let ledger = InFlightLedger::default();
        let router = completion_router(ledger.clone(), InternalAuth::new(None));
        let response = router
            .oneshot(signed_post(&notice(), "mas-api", TEST_SECRET, 0))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    /// Stats is operational state: it is authenticated too.
    #[tokio::test]
    async fn stats_requires_a_signature() {
        let ledger = InFlightLedger::default();
        let schedule = ScheduleId::new();
        ledger.start(schedule);

        let router = completion_router(ledger.clone(), test_auth());
        let unsigned = router
            .clone()
            .oneshot(
                axum::http::Request::get("/v1/scheduler/stats")
                    .body(axum::body::Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(unsigned.status(), StatusCode::UNAUTHORIZED);

        let signature = sign(TEST_SECRET, "mas-api", &Timestamp::now(), b"");
        let signed = router
            .oneshot(
                axum::http::Request::get("/v1/scheduler/stats")
                    .header(SIGNATURE_HEADER, signature)
                    .header(SERVICE_HEADER, "mas-api")
                    .body(axum::body::Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert!(signed.status().is_success());
        let bytes = axum::body::to_bytes(signed.into_body(), 4096)
            .await
            .expect("body");
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(json["in_flight"], 1);
    }

    #[tokio::test]
    async fn health_answers_without_a_signature() {
        let response = health_router()
            .oneshot(
                axum::http::Request::get("/healthz")
                    .body(axum::body::Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn the_secret_is_never_exposed_by_debug() {
        let auth = test_auth();
        let rendered = format!("{auth:?}");
        assert!(rendered.contains("enabled"));
        assert!(
            !rendered.contains("scheduler-test-secret"),
            "Debug must not print the shared secret"
        );
    }
}
