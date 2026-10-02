//! # mas-common
//!
//! Shared foundation crate for the entire `rust-core` workspace.
//!
//! Exports:
//! * [`AppError`] / [`Result`] — the single error- and result-model used by
//!   every crate.
//! * Strongly typed IDs (`OrganizationId`, `TenantId`, `UserId`, …) —
//!   UUIDv7-backed newtypes with `Display` / `FromStr` / serde support.
//! * [`Timestamp`] — the only UTC timestamp type allowed across the codebase.
//! * Shared state enums ([`enums`]).
//! * Pagination primitives ([`pagination`]).
//! * Validation helpers and composition ([`validation`]).
//! * Protocol/limits constants ([`constants`]).
//! * Secret/PII redaction primitives ([`redaction`]) so sensitive values never
//!   reach logs or events.

pub mod config;
pub mod constants;
pub mod enums;
pub mod error;
pub mod ids;
pub mod pagination;
pub mod redaction;
pub mod result;
pub mod timestamps;
pub mod validation;

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Flat re-exports for the most commonly used items.
// ---------------------------------------------------------------------------
pub use error::{AppError, ValidationIssue};
pub use ids::{
    AgentId, AgentVersionId, ApiKeyId, AuditEventId, ConnectorId, CredentialId, EventId,
    ExecutionId, ExecutionStepId, MembershipId, OrganizationId, PolicyId, ProjectId, QuotaId,
    ScheduleId, SecretReferenceId, SessionId, TaskAttemptId, TaskId, TenantId, ToolId, TypedId,
    UsageRecordId, UserId, WebhookId, WorkflowId, WorkflowNodeId, WorkflowVersionId,
};
pub use pagination::{
    Cursor, CursorPayload, PageRequest, PageResponse, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE,
};
pub use redaction::{SecretRedactor, SensitiveField, REDACTED};
pub use result::{ExecutionOutcome, OperationResult, Result};
pub use timestamps::Timestamp;
