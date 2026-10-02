//! HTTP + gRPC **api processes**: thin transports over `mas-application`
//! use-cases. This crate contains NO business logic — every handler maps a
//! request onto a `ServiceContext` + service call, and maps the result onto
//! the stable contracts (`ApiEnvelope<T>` / `StableApiError`).
//!
//! ## Transport invariants (mirrored identically across HTTP and gRPC)
//!
//! * **Identity** — `Authorization: Bearer …` verified via the injected
//!   [`TokenVerifierPort`]; unverified requests on protected routes are
//!   `401 UNAUTHENTICATED`. The verifier is a seam: deployments bind the IVF
//!   token stack, tests bind a fixture verifier.
//! * **Scope** — `X-Tenant-Id` / `X-Organization-Id` (uuid v7 strings).
//!   Routes that operate on tenant-owned data require both; cross-tenant
//!   or cross-organization ids resolve as `404` (existence hiding) because
//!   `mas-application` re-checks every aggregate against the scope.
//! * **Correlation** — `X-Correlation-Id` honored when present and valid
//!   (1–256 chars, no control bytes); otherwise a fresh uuid v7. Always
//!   echoed back in the response envelope meta + `X-Correlation-Id` header.
//! * **Idempotency** — `Idempotency-Key` on execution submission; a replay
//!   returns the original execution with `X-Idempotent-Replay: true`.
//! * **Errors** — every failure is an `ApiEnvelope { error: StableApiError }`
//!   carrying only the public AppError surface (code + safe message).
//!
//! ## Process
//!
//! [`router::router`] builds the HTTP `axum::Router`; [`grpc`] hosts the
//! tonic service implementations for `mas.api.v1`; [`server`] binds them
//! with graceful shutdown. `src/main.rs` is the thin composition root.

pub mod auth;
pub mod context;
pub mod grpc;
pub mod handlers;
pub mod middleware;
pub mod prod_auth;
pub mod response;
pub mod router;
pub mod server;
pub mod state;

pub use auth::{Principal, TokenVerifierPort};
pub use context::RequestContext;
pub use state::AppState;
