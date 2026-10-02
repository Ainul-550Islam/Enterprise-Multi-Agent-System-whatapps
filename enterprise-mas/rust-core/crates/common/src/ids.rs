//! Strongly typed, UUIDv7-backed identifiers for every domain resource.
//!
//! All IDs are generated as UUIDv7 (time-ordered, k-sortable), giving stable
//! index locality in PostgreSQL and safe ordering in pagination cursors.
//!
//! `Serialize`/`Deserialize`/`Display`/`FromStr` are implemented for every ID.
//! "Database conversion helpers" are the `as_uuid` / `into_uuid` /
//! `as_bytes` / `to_database_string` family used by the persistence layer;
//! concrete `sqlx::Type` impls are added behind a feature when the
//! persistence crate lands.

use crate::error::AppError;
use crate::result::Result;
use std::fmt::Display;
use std::hash::Hash;
use std::str::FromStr;

/// Common interface implemented by every typed identifier. Enables generic
/// utilities (e.g. cursor encoding, log enrichment) over arbitrary IDs.
pub trait TypedId:
    Copy + Eq + Hash + Ord + Display + FromStr<Err = AppError> + Send + Sync + 'static
{
    /// Name of the identifier type, e.g. `"TenantId"` (for logs/errors).
    const TYPE_NAME: &'static str;

    /// Generate a new time-ordered identifier.
    fn new_id() -> Self;
    /// Wrap an existing UUID.
    fn from_uuid(uuid: uuid::Uuid) -> Self;
    /// Borrow the wrapped UUID.
    fn as_uuid(&self) -> &uuid::Uuid;
    /// Consume the wrapper, returning the raw UUID.
    fn into_uuid(self) -> uuid::Uuid;
    /// Parse from its canonical string form.
    fn parse(s: &str) -> Result<Self> {
        s.parse()
    }
}

/// Defines a newtype identifier around `uuid::Uuid` with the full standard
/// API surface. Use `define_id!` exactly once per ID type.
#[macro_export]
macro_rules! define_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            ::serde::Serialize,
            ::serde::Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(::uuid::Uuid);

        impl $name {
            /// Generates a new time-ordered (UUIDv7) identifier.
            #[must_use]
            pub fn new() -> Self {
                Self(::uuid::Uuid::now_v7())
            }

            /// Wraps an existing UUID (used when loading from storage).
            #[must_use]
            pub const fn from_uuid(uuid: ::uuid::Uuid) -> Self {
                Self(uuid)
            }

            /// The all-zero sentinel identifier. For tests and well-known
            /// singleton rows only; never generate one at runtime.
            #[must_use]
            pub const fn nil() -> Self {
                Self(::uuid::Uuid::nil())
            }

            /// Borrows the wrapped UUID.
            #[must_use]
            pub const fn as_uuid(&self) -> &::uuid::Uuid {
                &self.0
            }

            /// Consumes the wrapper and returns the UUID (column-ready value).
            #[must_use]
            pub const fn into_uuid(self) -> ::uuid::Uuid {
                self.0
            }

            /// Raw big-endian bytes, for binary columns and sortable keys.
            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; 16] {
                self.0.as_bytes()
            }

            /// Canonical lowercase hyphenated string form used by `TEXT`
            /// columns, URLs and logs.
            #[must_use]
            pub fn to_database_string(self) -> ::std::string::String {
                self.0.to_string()
            }

            /// Parses the canonical string form (trimming outer whitespace).
            pub fn parse_str(s: &str) -> $crate::result::Result<Self> {
                s.parse()
            }

            /// `true` when this is the all-zero sentinel identifier.
            #[must_use]
            pub fn is_nil(&self) -> bool {
                self.0.is_nil()
            }
        }

        impl $crate::ids::TypedId for $name {
            const TYPE_NAME: &'static str = ::std::stringify!($name);

            fn new_id() -> Self {
                Self::new()
            }

            fn from_uuid(uuid: ::uuid::Uuid) -> Self {
                Self(uuid)
            }

            fn as_uuid(&self) -> &::uuid::Uuid {
                &self.0
            }

            fn into_uuid(self) -> ::uuid::Uuid {
                self.0
            }
        }

        impl ::std::default::Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                ::std::fmt::Display::fmt(&self.0, f)
            }
        }

        impl ::std::str::FromStr for $name {
            type Err = $crate::error::AppError;

            fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
                let trimmed = s.trim();
                ::uuid::Uuid::try_parse(trimmed)
                    .map(Self)
                    .map_err(|_| $crate::error::AppError::Validation {
                        message: ::std::format!(
                            "invalid {}: expected a canonical UUID string, got {:?}",
                            ::std::stringify!($name),
                            s
                        ),
                        issues: ::std::vec![$crate::error::ValidationIssue::new(
                            ::std::stringify!($name),
                            "invalid_format",
                            "expected a UUID in canonical hyphenated form",
                        )],
                    })
            }
        }

        impl ::std::convert::From<::uuid::Uuid> for $name {
            fn from(uuid: ::uuid::Uuid) -> Self {
                Self(uuid)
            }
        }

        impl ::std::convert::From<$name> for ::uuid::Uuid {
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl ::std::borrow::Borrow<::uuid::Uuid> for $name {
            fn borrow(&self) -> &::uuid::Uuid {
                &self.0
            }
        }

        impl ::std::convert::AsRef<::uuid::Uuid> for $name {
            fn as_ref(&self) -> &::uuid::Uuid {
                &self.0
            }
        }
    };
}

define_id!(/// Identifies an `Organization` (top-level billing/administrative boundary).
OrganizationId);
define_id!(/// Identifies a `Tenant` (isolated customer workspace inside an organization).
TenantId);
define_id!(/// Identifies a human or service `User`.
UserId);
define_id!(/// Identifies a `Membership` joining a user to an organization/tenant.
MembershipId);
define_id!(/// Identifies a `Project` (logical workspace grouping agents/workflows/tools).
ProjectId);
define_id!(/// Identifies an `Agent`.
AgentId);
define_id!(/// Identifies an immutable `AgentVersion`.
AgentVersionId);
define_id!(/// Identifies a `Workflow`.
WorkflowId);
define_id!(/// Identifies an immutable `WorkflowVersion`.
WorkflowVersionId);
define_id!(/// Identifies a persisted `WorkflowNode` row.
WorkflowNodeId);
define_id!(/// Identifies a `Task` (unit of dispatched work).
TaskId);
define_id!(/// Identifies one `TaskAttempt` of a task.
TaskAttemptId);
define_id!(/// Identifies an `Execution` (one complete workflow/agent run).
ExecutionId);
define_id!(/// Identifies one `ExecutionStep` inside an execution.
ExecutionStepId);
define_id!(/// Identifies a registered `Tool`.
ToolId);
define_id!(/// Identifies a `Connector` to an external system.
ConnectorId);
define_id!(/// Identifies non-secret credential metadata.
CredentialId);
define_id!(/// Identifies a `SecretReference` pointing into a secret manager.
SecretReferenceId);
define_id!(/// Identifies a versioned `Policy`.
PolicyId);
define_id!(/// Identifies an `ApprovalRequest` raised by a require-approval rule.
ApprovalRequestId);
define_id!(/// Identifies a quota definition row.
QuotaId);
define_id!(/// Identifies an append-only `UsageRecord`.
UsageRecordId);
define_id!(/// Identifies an immutable `AuditEvent`.
AuditEventId);
define_id!(/// Identifies a `Schedule`.
ScheduleId);
define_id!(/// Identifies a `WebhookEndpoint`.
WebhookId);
define_id!(/// Identifies API-key metadata (never the raw key).
ApiKeyId);
define_id!(/// Identifies an authenticated `Session`.
SessionId);
define_id!(/// Identifies an event in the eventing infrastructure.
EventId);
