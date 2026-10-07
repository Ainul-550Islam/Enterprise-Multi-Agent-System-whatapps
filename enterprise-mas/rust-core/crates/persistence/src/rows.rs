//! PURPOSE: Row models and the column ↔ domain hydration bridge.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: keep materialized columns strongly typed in SQL
//! (ids as UUID, statuses as text, timestamps as `timestamptz`) and rebuild
//! the domain aggregate from them, so a schema drift fails loudly at the
//! boundary instead of silently producing a default.
//!
//! It must NOT:
//!   • let a hydration failure default to a plausible-looking value;
//!   • duplicate column lists across repositories (they are shared constants);
//!   • carry secrets — rows are what get logged and exported.

//! Row models and the column ↔ domain hydration bridge.
//!
//! Domain aggregates are serde-shaped (enums as strings, ids as UUID strings,
//! timestamps as RFC-3339). Row models keep *materialized* columns (ids,
//! statuses, timestamps, FKs) as strong SQL types and rebuild the aggregate
//! JSON from them — so hydration re-runs every domain-level invariant that
//! `Deserialize` enforces, and the mapping is a pure function unit tests can
//! cover without a database.
//!
//! Row → domain: [`TenantRow::into_domain`] etc. assemble the aggregate JSON
//! and call [`hydrate`]. Domain → row: [`TenantRow::from_domain`] etc.
//! serialize once and extract the materialized columns.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use mas_common::enums::EventStatus;
use mas_common::error::AppError;
use mas_common::ids::{
    EventId, OrganizationId, ProjectId, RollbackRecordId, TenantId, UserId, WorkflowId,
    WorkflowVersionId,
};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::membership::Membership;
use mas_domain::organization::Organization;
use mas_domain::project::{Project, ProjectConfig};
use mas_domain::tenant::{IsolationMode, Tenant};
use mas_domain::user::User;
use mas_domain::{
    DataSource, Document, ExecutionStep, RollbackRecord, RollbackResourceKind, WorkflowVersion,
};
use mas_events::outbox::OutboxRecord;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Map, Value};
use uuid::Uuid;

/// Re-runs domain invariants while converting assembled JSON to an aggregate.
pub fn hydrate<T: DeserializeOwned>(table: &'static str, value: Value) -> Result<T> {
    serde_json::from_value(value).map_err(|err| {
        AppError::database(format!(
            "row hydration failed for table {table} at line {} column {}",
            err.line(),
            err.column()
        ))
    })
}

/// Serializes an aggregate for column extraction.
pub fn dehydrate<T: Serialize>(table: &'static str, value: &T) -> Result<Value> {
    serde_json::to_value(value).map_err(|err| {
        AppError::serialization(format!(
            "failed to serialize aggregate for table {table}: {err}"
        ))
    })
}

/// Extracts a required string column from a serialized aggregate.
fn take_string(map: &Map<String, Value>, table: &'static str, key: &'static str) -> Result<String> {
    map.get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            AppError::database(format!("missing/invalid column '{key}' for table {table}"))
        })
}

/// Extracts a required UUID column.
fn take_opt_string(
    map: &Map<String, Value>,
    table: &'static str,
    key: &'static str,
) -> Result<Option<String>> {
    match map.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        _ => Err(AppError::database(format!(
            "invalid nullable string column '{key}' for table {table}"
        ))),
    }
}

fn take_uuid(map: &Map<String, Value>, table: &'static str, key: &'static str) -> Result<Uuid> {
    let raw = take_string(map, table, key)?;
    Uuid::parse_str(&raw).map_err(|_| {
        AppError::database(format!("invalid uuid in column '{key}' for table {table}"))
    })
}

/// Extracts a required timestamp column.
fn take_ts(
    map: &Map<String, Value>,
    table: &'static str,
    key: &'static str,
) -> Result<DateTime<Utc>> {
    Timestamp::parse_rfc3339(&take_string(map, table, key)?)
        .map(Timestamp::into_datetime)
        .map_err(|_| AppError::database(format!("invalid timestamp '{key}' for table {table}")))
}

/// Extracts an optional timestamp column.
fn take_opt_ts(
    map: &Map<String, Value>,
    table: &'static str,
    key: &'static str,
) -> Result<Option<DateTime<Utc>>> {
    match map.get(key) {
        Some(Value::String(s)) => Timestamp::parse_rfc3339(s)
            .map(|ts| Some(ts.into_datetime()))
            .map_err(|_| {
                AppError::database(format!("invalid timestamp '{key}' for table {table}"))
            }),
        _ => Ok(None),
    }
}

/// Serializes a timestamp for JSON assembly.
fn ts_json(ts: DateTime<Utc>) -> Value {
    Value::String(Timestamp::from_datetime(ts).to_rfc3339_millis())
}

fn object(value: &Value) -> Result<&Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| AppError::database("aggregate did not serialize to an object"))
}

// ---------------------------------------------------------------------------
// organizations
// ---------------------------------------------------------------------------

/// Row model for the `organizations` table.
#[derive(Debug, Clone, PartialEq)]
pub struct OrganizationRow {
    /// Primary key.
    pub id: Uuid,
    /// Registered legal entity name.
    pub legal_name: String,
    /// Display name.
    pub display_name: String,
    /// Unique slug.
    pub slug: String,
    /// Status string (`active` | `suspended` | `archived`).
    pub status: String,
    /// Settings payload.
    pub settings: Value,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

impl OrganizationRow {
    /// Hydrates the domain aggregate.
    pub fn into_domain(self) -> Result<Organization> {
        hydrate(
            "organizations",
            json!({
                "id": self.id,
                "legal_name": self.legal_name,
                "display_name": self.display_name,
                "slug": self.slug,
                "status": self.status,
                "settings": self.settings,
                "created_at": ts_json(self.created_at),
                "updated_at": ts_json(self.updated_at),
            }),
        )
    }

    /// Extracts the row from a domain aggregate.
    pub fn from_domain(org: &Organization) -> Result<Self> {
        let value = dehydrate("organizations", org)?;
        let map = object(&value)?;
        Ok(Self {
            id: take_uuid(map, "organizations", "id")?,
            legal_name: take_string(map, "organizations", "legal_name")?,
            display_name: take_string(map, "organizations", "display_name")?,
            slug: take_string(map, "organizations", "slug")?,
            status: take_string(map, "organizations", "status")?,
            settings: map.get("settings").cloned().unwrap_or(Value::Null),
            created_at: take_ts(map, "organizations", "created_at")?,
            updated_at: take_ts(map, "organizations", "updated_at")?,
        })
    }
}

// ---------------------------------------------------------------------------
// tenants
// ---------------------------------------------------------------------------

/// Row model for the `tenants` table.
#[derive(Debug, Clone, PartialEq)]
pub struct TenantRow {
    /// Primary key.
    pub id: Uuid,
    /// Owning organization (immutable).
    pub organization_id: Uuid,
    /// Display name.
    pub name: String,
    /// Unique-within-org slug.
    pub slug: String,
    /// Status string.
    pub status: String,
    /// Environment (`development` | `staging` | `production`).
    pub environment: String,
    /// Isolation mode (`shared_rls` | `schema_per_tenant` | `dedicated_database`).
    pub isolation_mode: String,
    /// Settings payload.
    pub settings: Value,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

impl TenantRow {
    /// Hydrates the domain aggregate.
    pub fn into_domain(self) -> Result<Tenant> {
        hydrate(
            "tenants",
            json!({
                "id": self.id,
                "organization_id": self.organization_id,
                "name": self.name,
                "slug": self.slug,
                "status": self.status,
                "environment": self.environment,
                "isolation": self.isolation_mode,
                "settings": self.settings,
                "created_at": ts_json(self.created_at),
                "updated_at": ts_json(self.updated_at),
            }),
        )
    }

    /// Extracts the row from a domain aggregate.
    pub fn from_domain(tenant: &Tenant) -> Result<Self> {
        let value = dehydrate("tenants", tenant)?;
        let map = object(&value)?;
        // Domain field is `isolation`; the column name is `isolation_mode`.
        let isolation = take_string(map, "tenants", "isolation")?;
        if IsolationMode::from_str(&isolation).is_err() {
            return Err(AppError::database(
                "unknown isolation mode serialized in tenant aggregate",
            ));
        }
        Ok(Self {
            id: take_uuid(map, "tenants", "id")?,
            organization_id: take_uuid(map, "tenants", "organization_id")?,
            name: take_string(map, "tenants", "name")?,
            slug: take_string(map, "tenants", "slug")?,
            status: take_string(map, "tenants", "status")?,
            environment: take_string(map, "tenants", "environment")?,
            isolation_mode: isolation,
            settings: map.get("settings").cloned().unwrap_or(Value::Null),
            created_at: take_ts(map, "tenants", "created_at")?,
            updated_at: take_ts(map, "tenants", "updated_at")?,
        })
    }
}

// ---------------------------------------------------------------------------
// projects
// ---------------------------------------------------------------------------

/// Row model for the `projects` table.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectRow {
    /// Primary key.
    pub id: Uuid,
    /// Owning tenant.
    pub tenant_id: Uuid,
    /// Owning organization (dual link).
    pub organization_id: Uuid,
    /// Display name.
    pub name: String,
    /// Unique-within-tenant slug.
    pub slug: String,
    /// Optional description.
    pub description: Option<String>,
    /// Status string.
    pub status: String,
    /// Project config payload (limits and labels).
    pub config: Value,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

impl ProjectRow {
    /// Hydrates the domain aggregate.
    pub fn into_domain(self) -> Result<Project> {
        // `description` uses skip_serializing_if — omitting rather than nulling
        // keeps semantics identical through the serde boundary.
        let mut value = json!({
            "id": self.id,
            "tenant_id": self.tenant_id,
            "organization_id": self.organization_id,
            "name": self.name,
            "slug": self.slug,
            "status": self.status,
            "config": self.config,
            "created_at": ts_json(self.created_at),
            "updated_at": ts_json(self.updated_at),
        });
        if let Some(description) = self.description {
            value["description"] = Value::String(description);
        }
        let project: Project = hydrate("projects", value)?;
        // Re-verify the dual link (defense in depth under the FK/RLS guards).
        if ProjectId::from_uuid(self.id) != project.id
            || TenantId::from_uuid(self.tenant_id) != project.tenant_id
            || OrganizationId::from_uuid(self.organization_id) != project.organization_id
        {
            return Err(AppError::database("project row dual-link mismatch"));
        }
        Ok(project)
    }

    /// Extracts the row from a domain aggregate.
    pub fn from_domain(project: &Project) -> Result<Self> {
        let value = dehydrate("projects", project)?;
        let map = object(&value)?;
        Ok(Self {
            id: take_uuid(map, "projects", "id")?,
            tenant_id: take_uuid(map, "projects", "tenant_id")?,
            organization_id: take_uuid(map, "projects", "organization_id")?,
            name: take_string(map, "projects", "name")?,
            slug: take_string(map, "projects", "slug")?,
            description: map
                .get("description")
                .and_then(Value::as_str)
                .map(str::to_owned),
            status: take_string(map, "projects", "status")?,
            config: map.get("config").cloned().unwrap_or_else(|| {
                serde_json::to_value(ProjectConfig::default()).unwrap_or(Value::Null)
            }),
            created_at: take_ts(map, "projects", "created_at")?,
            updated_at: take_ts(map, "projects", "updated_at")?,
        })
    }
}

// ---------------------------------------------------------------------------
// users
// ---------------------------------------------------------------------------

/// Row model for the `users` table (global identity, no tenant scope).
#[derive(Debug, Clone, PartialEq)]
pub struct UserRow {
    /// Primary key.
    pub id: Uuid,
    /// Primary, normalized email.
    pub email: String,
    /// Display name.
    pub display_name: String,
    /// Status string.
    pub status: String,
    /// Federated subject claim, when present.
    pub external_subject: Option<String>,
    /// Identity provider key, when present.
    pub identity_provider: Option<String>,
    /// Last successful login.
    pub last_login_at: Option<DateTime<Utc>>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

impl UserRow {
    /// Hydrates the domain aggregate.
    pub fn into_domain(self) -> Result<User> {
        let mut value = json!({
            "id": self.id,
            "email": self.email,
            "display_name": self.display_name,
            "status": self.status,
            "created_at": ts_json(self.created_at),
            "updated_at": ts_json(self.updated_at),
        });
        if let Some(subject) = self.external_subject {
            value["external_subject"] = Value::String(subject);
        }
        if let Some(provider) = self.identity_provider {
            value["identity_provider"] = Value::String(provider);
        }
        if let Some(last_login) = self.last_login_at {
            value["last_login_at"] = ts_json(last_login);
        }
        hydrate("users", value)
    }

    /// Extracts the row from a domain aggregate.
    pub fn from_domain(user: &User) -> Result<Self> {
        let value = dehydrate("users", user)?;
        let map = object(&value)?;
        Ok(Self {
            id: take_uuid(map, "users", "id")?,
            email: take_string(map, "users", "email")?,
            display_name: take_string(map, "users", "display_name")?,
            status: take_string(map, "users", "status")?,
            external_subject: map
                .get("external_subject")
                .and_then(Value::as_str)
                .map(str::to_owned),
            identity_provider: map
                .get("identity_provider")
                .and_then(Value::as_str)
                .map(str::to_owned),
            last_login_at: take_opt_ts(map, "users", "last_login_at")?,
            created_at: take_ts(map, "users", "created_at")?,
            updated_at: take_ts(map, "users", "updated_at")?,
        })
    }
}

// ---------------------------------------------------------------------------
// memberships
// ---------------------------------------------------------------------------

/// Row model for the `memberships` table.
#[derive(Debug, Clone, PartialEq)]
pub struct MembershipRow {
    /// Primary key.
    pub id: Uuid,
    /// Member user.
    pub user_id: Uuid,
    /// Membership organization.
    pub organization_id: Uuid,
    /// Tenant scope (NULL = organization-wide).
    pub tenant_id: Option<Uuid>,
    /// Role strings (`owner|admin|developer|operator|viewer|service_account`).
    pub roles: Vec<String>,
    /// Status string.
    pub status: String,
    /// Invitation time.
    pub invited_at: DateTime<Utc>,
    /// Join time (NULL until accepted).
    pub joined_at: Option<DateTime<Utc>>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

impl MembershipRow {
    /// Hydrates the domain aggregate.
    pub fn into_domain(self) -> Result<Membership> {
        let mut value = json!({
            "id": self.id,
            "user_id": self.user_id,
            "organization_id": self.organization_id,
            "roles": self.roles,
            "status": self.status,
            "invited_at": ts_json(self.invited_at),
            "created_at": ts_json(self.created_at),
            "updated_at": ts_json(self.updated_at),
        });
        if let Some(tenant) = self.tenant_id {
            value["tenant_id"] = json!(tenant);
        }
        if let Some(joined) = self.joined_at {
            value["joined_at"] = ts_json(joined);
        }
        let membership: Membership = hydrate("memberships", value)?;
        if membership.roles.is_empty() {
            return Err(AppError::database(
                "membership row hydrated with zero roles (schema CHECK violated)",
            ));
        }
        Ok(membership)
    }

    /// Extracts the row from a domain aggregate.
    pub fn from_domain(membership: &Membership) -> Result<Self> {
        let value = dehydrate("memberships", membership)?;
        let map = object(&value)?;
        let roles: Vec<String> = map
            .get("roles")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .ok_or_else(|| AppError::database("invalid roles payload for table memberships"))?;
        Ok(Self {
            id: take_uuid(map, "memberships", "id")?,
            user_id: take_uuid(map, "memberships", "user_id")?,
            organization_id: take_uuid(map, "memberships", "organization_id")?,
            tenant_id: map
                .get("tenant_id")
                .and_then(Value::as_str)
                .and_then(|s| Uuid::parse_str(s).ok()),
            roles,
            status: take_string(map, "memberships", "status")?,
            invited_at: take_ts(map, "memberships", "invited_at")?,
            joined_at: take_opt_ts(map, "memberships", "joined_at")?,
            created_at: take_ts(map, "memberships", "created_at")?,
            updated_at: take_ts(map, "memberships", "updated_at")?,
        })
    }
}

// ---------------------------------------------------------------------------
// outbox_events
// ---------------------------------------------------------------------------

/// Row model for the `outbox_events` table.
#[derive(Debug, Clone, PartialEq)]
pub struct OutboxRow {
    /// Primary key; equals the envelope's event id.
    pub event_id: Uuid,
    /// The full event envelope JSON.
    pub envelope: Value,
    /// Owning tenant (RLS + dispatcher reporting).
    pub tenant_id: Uuid,
    /// Dotted event type (denormalized for debugging/reporting).
    pub event_type: String,
    /// Aggregate type.
    pub aggregate_type: String,
    /// Aggregate id.
    pub aggregate_id: String,
    /// Event status string.
    pub status: String,
    /// Delivery attempts so far.
    pub attempts: i32,
    /// Next eligible publish attempt.
    pub next_attempt_at: DateTime<Utc>,
    /// Last failure message.
    pub last_error: Option<String>,
    /// Enqueue time.
    pub enqueued_at: DateTime<Utc>,
    /// Publish time (NULL until published).
    pub published_at: Option<DateTime<Utc>>,
    /// Relay that currently owns the row (NULL when unclaimed).
    pub claimed_by: Option<String>,
    /// When the current claim was taken (NULL when unclaimed).
    pub claimed_at: Option<DateTime<Utc>>,
}

impl OutboxRow {
    /// Hydrates the outbox record (envelope re-validated through serde).
    pub fn into_record(self) -> Result<OutboxRecord> {
        let envelope = serde_json::from_value(self.envelope.clone()).map_err(|err| {
            AppError::database(format!(
                "outbox row {} holds an unreadable envelope: {err}",
                self.event_id
            ))
        })?;
        let status = EventStatus::from_str(&self.status).map_err(|_| {
            AppError::database(format!(
                "outbox row {} holds unknown status {:?}",
                self.event_id, self.status
            ))
        })?;
        Ok(OutboxRecord {
            event_id: EventId::from_uuid(self.event_id),
            envelope,
            status,
            attempts: u32::try_from(self.attempts)
                .map_err(|_| AppError::database("outbox attempts column is negative"))?,
            next_attempt_at: Timestamp::from_datetime(self.next_attempt_at),
            last_error: self.last_error,
            enqueued_at: Timestamp::from_datetime(self.enqueued_at),
            published_at: self.published_at.map(Timestamp::from_datetime),
            claimed_by: self.claimed_by,
            claimed_at: self.claimed_at.map(Timestamp::from_datetime),
        })
    }

    /// Extracts the row from a record (the authoritative source of truth for
    /// `tenant_id` etc. is the envelope — denormalized columns mirror it).
    pub fn from_record(record: &OutboxRecord) -> Result<Self> {
        Ok(Self {
            event_id: record.event_id.into_uuid(),
            envelope: serde_json::to_value(&record.envelope).map_err(|err| {
                AppError::serialization(format!("outbox envelope not serializable: {err}"))
            })?,
            tenant_id: record.envelope.tenant_id.into_uuid(),
            event_type: record.envelope.event_type.clone(),
            aggregate_type: record.envelope.aggregate_type.clone(),
            aggregate_id: record.envelope.aggregate_id.clone(),
            status: record.status.to_string(),
            attempts: i32::try_from(record.attempts).unwrap_or(i32::MAX),
            next_attempt_at: record.next_attempt_at.into_datetime(),
            last_error: record.last_error.clone(),
            enqueued_at: record.enqueued_at.into_datetime(),
            published_at: record.published_at.map(Timestamp::into_datetime),
            // A record being enqueued is by definition unclaimed: ownership
            // only exists once a relay has taken the row.
            claimed_by: None,
            claimed_at: None,
        })
    }
}

// ---------------------------------------------------------------------------
// audit_events
// ---------------------------------------------------------------------------

/// Row model for the append-only `audit_events` table.
#[derive(Debug, Clone, PartialEq)]
pub struct AuditRow {
    /// Primary key.
    pub id: Uuid,
    /// Tenant scope (NULL = platform-global).
    pub tenant_id: Option<Uuid>,
    /// Organization scope.
    pub organization_id: Option<Uuid>,
    /// Actor payload (`{kind, id, display?}`).
    pub actor: Value,
    /// `subject.verb` action.
    pub action: String,
    /// Resource category.
    pub resource_type: String,
    /// Resource identifier.
    pub resource_id: Option<String>,
    /// Outcome string.
    pub outcome: String,
    /// Severity string.
    pub severity: String,
    /// Compliance class string.
    pub compliance_class: String,
    /// End-to-end correlation id.
    pub correlation_id: String,
    /// Pre-redacted metadata map.
    pub metadata: Value,
    /// When the event occurred.
    pub occurred_at: DateTime<Utc>,
}

impl AuditRow {
    /// Hydrates the audit event domain object.
    pub fn into_domain(self) -> Result<mas_domain::audit_event::AuditEvent> {
        let mut value = json!({
            "id": self.id,
            "actor": self.actor,
            "action": self.action,
            "resource_type": self.resource_type,
            "outcome": self.outcome,
            "severity": self.severity,
            "compliance_class": self.compliance_class,
            "correlation_id": self.correlation_id,
            "metadata": self.metadata,
            "occurred_at": ts_json(self.occurred_at),
        });
        if let Some(tenant) = self.tenant_id {
            value["tenant_id"] = json!(tenant);
        }
        if let Some(org) = self.organization_id {
            value["organization_id"] = json!(org);
        }
        if let Some(resource) = self.resource_id {
            value["resource_id"] = Value::String(resource);
        }
        hydrate("audit_events", value)
    }

    /// Extracts the row from a domain audit event (insert path).
    pub fn from_domain(event: &mas_domain::audit_event::AuditEvent) -> Result<Self> {
        let value = dehydrate("audit_events", event)?;
        let map = object(&value)?;
        Ok(Self {
            id: take_uuid(map, "audit_events", "id")?,
            tenant_id: map
                .get("tenant_id")
                .and_then(Value::as_str)
                .and_then(|s| Uuid::parse_str(s).ok()),
            organization_id: map
                .get("organization_id")
                .and_then(Value::as_str)
                .and_then(|s| Uuid::parse_str(s).ok()),
            actor: map.get("actor").cloned().unwrap_or(Value::Null),
            action: take_string(map, "audit_events", "action")?,
            resource_type: take_string(map, "audit_events", "resource_type")?,
            resource_id: map
                .get("resource_id")
                .and_then(Value::as_str)
                .map(str::to_owned),
            outcome: take_string(map, "audit_events", "outcome")?,
            severity: take_string(map, "audit_events", "severity")?,
            compliance_class: take_string(map, "audit_events", "compliance_class")?,
            correlation_id: take_string(map, "audit_events", "correlation_id")?,
            metadata: map.get("metadata").cloned().unwrap_or(Value::Null),
            occurred_at: take_ts(map, "audit_events", "occurred_at")?,
        })
    }
}

/// Row model for a tenant-scoped execution timeline step.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionStepRow {
    /// Step primary key.
    pub id: Uuid,
    /// RLS scope.
    pub tenant_id: Uuid,
    /// Parent execution.
    pub execution_id: Uuid,
    /// Workflow node key, when present.
    pub node_key: Option<String>,
    /// Zero-based position in the timeline.
    pub ordinal: i32,
    /// Stable lifecycle token.
    pub status: String,
    /// One-based execution attempt.
    pub attempt: i32,
    /// Safe step metadata and external references.
    pub detail: Value,
    /// Start time.
    pub started_at: Option<DateTime<Utc>>,
    /// Finish time.
    pub finished_at: Option<DateTime<Utc>>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

impl ExecutionStepRow {
    /// Hydrates the domain step from fixed columns and the safe detail object.
    pub fn into_domain(self) -> Result<ExecutionStep> {
        let ordinal = u64::try_from(self.ordinal)
            .map_err(|_| AppError::database("execution_steps.ordinal is negative"))?;
        let attempt = u32::try_from(self.attempt)
            .map_err(|_| AppError::database("execution_steps.attempt is not positive"))?;
        let detail = self
            .detail
            .as_object()
            .ok_or_else(|| AppError::database("execution_steps.detail must be a JSON object"))?;
        let step_type = detail
            .get("step_type")
            .and_then(Value::as_str)
            .ok_or_else(|| AppError::database("execution_steps.detail.step_type is missing"))?;
        let optional = |key: &str| detail.get(key).filter(|value| !value.is_null()).cloned();
        hydrate(
            "execution_steps",
            json!({
                "id": self.id,
                "execution_id": self.execution_id,
                "node_key": self.node_key,
                "step_type": step_type,
                "sequence": ordinal,
                "attempt": attempt,
                "status": self.status,
                "input_ref": optional("input_ref"),
                "output_ref": optional("output_ref"),
                "error": optional("error"),
                "tool_id": optional("tool_id"),
                "agent_id": optional("agent_id"),
                "started_at": self.started_at.map(ts_json),
                "finished_at": self.finished_at.map(ts_json),
                "created_at": ts_json(self.created_at),
                "updated_at": ts_json(self.updated_at),
            }),
        )
    }

    /// Extracts the step and its tenant from the trusted execution row.
    pub fn from_domain(step: &ExecutionStep, tenant_id: TenantId) -> Result<Self> {
        if tenant_id.is_nil() || step.execution_id.is_nil() || step.id.is_nil() {
            return Err(AppError::invalid_field(
                "execution_step",
                "required",
                "tenant, execution and step identifiers must be concrete",
            ));
        }
        let ordinal = i32::try_from(step.sequence)
            .map_err(|_| AppError::invalid_field("sequence", "out_of_range", "too large"))?;
        let attempt = i32::try_from(step.attempt)
            .map_err(|_| AppError::invalid_field("attempt", "out_of_range", "too large"))?;
        let detail = json!({
            "step_type": step.step_type,
            "input_ref": step.input_ref,
            "output_ref": step.output_ref,
            "error": step.error,
            "tool_id": step.tool_id,
            "agent_id": step.agent_id,
        });
        Ok(Self {
            id: Uuid::from(step.id),
            tenant_id: Uuid::from(tenant_id),
            execution_id: Uuid::from(step.execution_id),
            node_key: step.node_key.clone(),
            ordinal,
            status: step.status.to_string(),
            attempt,
            detail,
            started_at: step.started_at.map(Timestamp::into_datetime),
            finished_at: step.finished_at.map(Timestamp::into_datetime),
            created_at: step.created_at.into_datetime(),
            updated_at: step.updated_at.into_datetime(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::enums::{Environment, TenantStatus};
    use mas_domain::audit_event::{AuditActor, AuditActorKind, AuditEvent, AuditOutcome};
    use mas_events::envelope::EventEnvelope;

    fn tenant_json(id: Uuid, org: Uuid) -> Value {
        json!({
            "id": id,
            "organization_id": org,
            "name": "Acme Prod",
            "slug": "acme-prod",
            "status": "active",
            "environment": "production",
            "isolation": "shared_rls",
            "settings": {},
            "created_at": "2026-09-29T04:00:00.000Z",
            "updated_at": "2026-09-29T04:30:00.000Z",
        })
    }

    #[test]
    fn tenant_row_roundtrips_through_domain() {
        let id = Uuid::now_v7();
        let org = Uuid::now_v7();
        let tenant: Tenant = hydrate("tenants", tenant_json(id, org)).expect("hydrate");
        assert_eq!(tenant.id, TenantId::from_uuid(id));
        assert_eq!(tenant.status, TenantStatus::Active);
        assert_eq!(tenant.environment, Environment::Production);

        let row = TenantRow::from_domain(&tenant).expect("dehydrate");
        assert_eq!(row.id, id);
        assert_eq!(row.organization_id, org);
        assert_eq!(row.isolation_mode, "shared_rls");
        assert_eq!(row.slug, "acme-prod");

        let back = row.into_domain().expect("rehydrate");
        let before = dehydrate("tenants", &tenant).expect("json");
        let after = dehydrate("tenants", &back).expect("json");
        assert_eq!(before, after);
    }

    #[test]
    fn hydration_rejects_invalid_states() {
        let id = Uuid::now_v7();
        let org = Uuid::now_v7();
        let mut bad = tenant_json(id, org);
        bad["status"] = json!("bogus");
        assert!(hydrate::<Tenant>("tenants", bad).is_err());

        let mut bad_env = tenant_json(id, org);
        bad_env["environment"] = json!("moon");
        assert!(hydrate::<Tenant>("tenants", bad_env).is_err());
    }

    #[test]
    fn membership_row_maps_roles_and_optional_scope() {
        let id = Uuid::now_v7();
        let value = json!({
            "id": id,
            "user_id": Uuid::now_v7(),
            "organization_id": Uuid::now_v7(),
            "roles": ["admin", "developer"],
            "status": "active",
            "invited_at": "2026-09-01T00:00:00.000Z",
            "joined_at": "2026-09-02T00:00:00.000Z",
            "created_at": "2026-09-01T00:00:00.000Z",
            "updated_at": "2026-09-02T00:00:00.000Z",
        });
        let membership: Membership = hydrate("memberships", value).expect("hydrate");
        let row = MembershipRow::from_domain(&membership).expect("dehydrate");
        assert_eq!(row.tenant_id, None, "org-wide membership has no tenant");
        let mut roles = row.roles.clone();
        roles.sort();
        assert_eq!(roles, ["admin".to_owned(), "developer".to_owned()]);
        assert!(row.joined_at.is_some());
        let back = row.into_domain().expect("rehydrate");
        assert_eq!(back.roles.len(), 2);
    }

    #[test]
    fn outbox_row_roundtrips() {
        let envelope = EventEnvelope::builder("task.failed", 2, "task", "task-9", TenantId::new())
            .expect("builder")
            .with_correlation("corr-5")
            .expect("correlation")
            .build()
            .expect("envelope");
        let record = OutboxRecord::new(envelope);
        let row = OutboxRow::from_record(&record).expect("dehydrate");
        assert_eq!(row.event_id, record.event_id.into_uuid());
        assert_eq!(row.status, "pending");
        let back = row.into_record().expect("rehydrate");
        assert_eq!(back.event_id, record.event_id);
        assert_eq!(back.status, EventStatus::Pending);
        assert_eq!(back.envelope.id, record.envelope.id);
    }

    #[test]
    fn audit_row_roundtrips_with_optional_scopes() {
        let tenant = TenantId::new();
        let event = AuditEvent::new(
            Some(tenant),
            None,
            AuditActor::new(AuditActorKind::User, "user-1").expect("actor"),
            "agent.publish",
            "agent",
            None,
            AuditOutcome::Success,
            "corr-audit",
        )
        .expect("event");
        let row = AuditRow::from_domain(&event).expect("dehydrate");
        assert!(row.tenant_id.is_some());
        assert_eq!(row.outcome, "success");
        let back = row.into_domain().expect("rehydrate");
        assert_eq!(back.action, "agent.publish");
        assert_eq!(back.tenant_id, event.tenant_id);
        // No resource_id is omitted (not null) through the bridge.
        assert!(back.resource_id.is_none());
    }

    #[test]
    fn data_source_row_roundtrips_with_sync_metadata() {
        let tenant_id = TenantId::new();
        let project_id = ProjectId::new();
        let created_by = UserId::new();
        let mut source = DataSource::register(
            tenant_id,
            project_id,
            "Knowledge base",
            mas_domain::data_source::DataSourceKind::Upload,
            json!({"max_bytes": 1048576}),
            None,
            created_by,
        )
        .expect("source");
        source.activate().expect("activate");
        source.begin_sync().expect("begin sync");
        source
            .complete_sync(Some("cursor-safe-1".to_owned()), Timestamp::now())
            .expect("complete sync");
        // PostgreSQL timestamptz persists microseconds; this row adapter uses
        // the shared millisecond wire precision. Fix timestamps to millisecond
        // values so this test compares the exact persisted representation.
        let persisted_at =
            Timestamp::parse_rfc3339("2026-09-30T00:00:00.000Z").expect("fixed timestamp");
        source.created_at = persisted_at;
        source.updated_at = persisted_at;
        source.last_synced_at = Some(persisted_at);

        let row = DataSourceRow::from_domain(&source).expect("row");
        assert_eq!(row.tenant_id, tenant_id.into_uuid());
        assert_eq!(row.project_id, project_id.into_uuid());
        assert_eq!(row.sync_cursor.as_deref(), Some("cursor-safe-1"));
        let restored = row.into_domain().expect("restored source");
        assert_eq!(
            dehydrate("data_sources", &restored).expect("restored json"),
            dehydrate("data_sources", &source).expect("source json")
        );
    }

    #[test]
    fn document_row_roundtrips_ready_metadata_without_chunk_data() {
        let tenant_id = TenantId::new();
        let project_id = ProjectId::new();
        let source_id = mas_common::ids::DataSourceId::new();
        let created_by = UserId::new();
        let mut document = Document::new(
            tenant_id,
            project_id,
            source_id,
            "Runbook",
            "text/plain",
            4,
            "a".repeat(64),
            created_by,
            json!({"roles": ["operator"]}),
            Map::new(),
        )
        .expect("document");
        document.begin_processing().expect("begin processing");
        document.mark_ready(2).expect("ready");
        let persisted_at =
            Timestamp::parse_rfc3339("2026-09-30T00:00:00.000Z").expect("fixed timestamp");
        document.created_at = persisted_at;
        document.updated_at = persisted_at;

        let row = DocumentRow::from_domain(&document).expect("row");
        assert_eq!(row.size_bytes, 4);
        assert_eq!(row.chunk_count, 2);
        assert_eq!(row.access_policy, json!({"roles": ["operator"]}));
        let restored = row.into_domain().expect("restored document");
        assert_eq!(
            dehydrate("documents", &restored).expect("restored json"),
            dehydrate("documents", &document).expect("document json")
        );
    }

    #[test]
    fn workflow_version_row_roundtrips_and_preserves_checksum() {
        use mas_domain::{WorkflowEdge, WorkflowGraph, WorkflowNode, WorkflowNodeType};

        let tenant_id = TenantId::new();
        let workflow_id = WorkflowId::new();
        let graph = WorkflowGraph::new(
            vec![
                WorkflowNode::new("start", WorkflowNodeType::Start, "Start").expect("start node"),
                WorkflowNode::new("end", WorkflowNodeType::End, "End").expect("end node"),
            ],
            vec![WorkflowEdge::new("start", "end").expect("edge")],
        );
        let version = WorkflowVersion::publish(
            tenant_id,
            workflow_id,
            1,
            graph,
            vec!["start".to_owned(), "end".to_owned()],
            Some(UserId::new()),
            None,
        )
        .expect("version");

        let row = WorkflowVersionRow::from_domain(&version).expect("row");
        assert_eq!(row.version, 1);
        assert_eq!(row.checksum, version.checksum());
        let restored = row.into_domain().expect("restored version");
        assert_eq!(restored.checksum(), version.checksum());
        assert_eq!(restored.topo_order(), version.topo_order());
        assert_eq!(restored.graph(), version.graph());
    }

    #[test]
    fn rollback_record_row_roundtrips_append_only_audit_fields() {
        let record = RollbackRecord::new(
            TenantId::new(),
            RollbackResourceKind::Workflow,
            Uuid::now_v7(),
            2,
            3,
            AuditActor::system(),
            "Restore reviewed workflow snapshot",
            "rollback-request-3",
        )
        .expect("record");

        let row = RollbackRecordRow::from_domain(&record).expect("row");
        assert_eq!(row.from_version, 2);
        assert_eq!(row.to_version, 3);
        assert_eq!(row.idempotency_key, "rollback-request-3");
        let restored = row.into_domain().expect("restored record");
        assert_eq!(restored.id, record.id);
        assert_eq!(restored.resource_kind, record.resource_kind);
        assert_eq!(restored.actor, record.actor);
        assert_eq!(restored.reason, record.reason);
        assert_eq!(restored.created_at, record.created_at);
    }

    #[test]
    fn id_columns_roundtrip_through_rows() {
        let row = OrganizationRow {
            id: Uuid::now_v7(),
            legal_name: "Acme Inc".to_owned(),
            display_name: "Acme".to_owned(),
            slug: "acme".to_owned(),
            status: "active".to_owned(),
            settings: json!({}),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let domain = row.clone().into_domain().expect("hydrate");
        assert_eq!(domain.id, OrganizationId::from_uuid(row.id));
        let back = OrganizationRow::from_domain(&domain).expect("dehydrate");
        assert_eq!(back.slug, "acme");
        assert_eq!(back.status, "active");
    }
}

// ---------------------------------------------------------------------------
// spec-JSONB aggregates (agents, agent_versions, workflows, executions,
// schedules, tasks): materialized columns (id/tenant/status/timestamps) are
// authoritative; everything else lives in the row's `spec|payload|state`
// jsonb under the aggregate's serde projection with the materialized keys
// stripped. This keeps every domain invariant enforced at hydration while
// leaving queryable columns for indexes and RLS.
// ---------------------------------------------------------------------------

/// Name of the reserved `state`/`payload` subkey carrying the aggregate spec
/// on tables whose JSONB column has first-class sibling data (executions'
/// checkpoints, tasks' transport output).
pub const SPEC_KEY: &str = "_mas_spec";

/// Splits the materialized columns out of a serialized aggregate, returning
/// (materialized map, spec payload with the materialized keys removed).
fn split_spec(value: &Value, materialized: &[&'static str]) -> Result<(Map<String, Value>, Value)> {
    let object = object(value)?;
    let mut columns = Map::new();
    let mut spec = object.clone();
    for key in materialized {
        if let Some(taken) = spec.remove(*key) {
            columns.insert((*key).to_owned(), taken);
        }
    }
    Ok((columns, Value::Object(spec)))
}

/// Merges materialized columns over a spec payload (columns win on conflict
/// — the DB is the authority, never a stale spec from an older shape).
fn merge_spec(spec: &Value, columns: Map<String, Value>, table: &'static str) -> Result<Value> {
    let mut merged = spec
        .as_object()
        .cloned()
        .ok_or_else(|| AppError::database(format!("spec column for {table} is not an object")))?;
    for (key, value) in columns {
        merged.insert(key, value);
    }
    Ok(Value::Object(merged))
}

/// Extracts an optional UUID column.
fn take_opt_uuid(
    map: &Map<String, Value>,
    table: &'static str,
    key: &'static str,
) -> Result<Option<Uuid>> {
    match map.get(key) {
        Some(Value::String(raw)) => Uuid::parse_str(raw)
            .map(Some)
            .map_err(|_| AppError::database(format!("invalid uuid in '{key}' for {table}"))),
        _ => Ok(None),
    }
}

/// Extracts an optional signed 32-bit column value from an aggregate object.
fn take_opt_i32(
    map: &Map<String, Value>,
    table: &'static str,
    key: &'static str,
) -> Result<Option<i32>> {
    match map.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_i64()
            .and_then(|number| i32::try_from(number).ok())
            .map(Some)
            .ok_or_else(|| AppError::database(format!("invalid i32 '{key}' for table {table}"))),
        _ => Err(AppError::database(format!(
            "invalid optional i32 '{key}' for table {table}"
        ))),
    }
}

/// Extracts a required u64 column.
fn take_u64(map: &Map<String, Value>, table: &'static str, key: &'static str) -> Result<u64> {
    map.get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| AppError::database(format!("missing/invalid u64 '{key}' for table {table}")))
}

/// Extracts a required u32 column.
fn take_u32(map: &Map<String, Value>, table: &'static str, key: &'static str) -> Result<u32> {
    map.get(key)
        .and_then(Value::as_u64)
        .and_then(|n| u32::try_from(n).ok())
        .ok_or_else(|| AppError::database(format!("missing/invalid u32 '{key}' for table {table}")))
}

// ---------------------------------------------------------------------------
// agents
// ---------------------------------------------------------------------------

use mas_domain::agent::Agent;
use mas_domain::agent_version::AgentVersion;
use mas_domain::execution::Execution;
use mas_domain::schedule::Schedule;
use mas_domain::task::Task;
use mas_domain::workflow::Workflow;

/// Row model for `agents` (spec-JSONB style).
#[derive(Debug, Clone, PartialEq)]
pub struct AgentRow {
    /// Primary key.
    pub id: Uuid,
    /// Owning tenant (RLS scope).
    pub tenant_id: Uuid,
    /// Owning organization.
    pub organization_id: Uuid,
    /// Human name (uniqueness key per tenant with slug).
    pub name: String,
    /// Unique-per-tenant slug.
    pub slug: String,
    /// Status string (see `[AgentStatus]`).
    pub status: String,
    /// User-configured time-saving baseline; absent means unknown.
    pub baseline_minutes_per_task: Option<i32>,
    /// Ownership class (`user` or `system`).
    pub origin: String,
    /// Spec payload (aggregate minus materialized columns).
    pub spec: Value,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

impl AgentRow {
    const MATERIALIZED: &'static [&'static str] = &[
        "id",
        "tenant_id",
        "organization_id",
        "name",
        "slug",
        "status",
        "baseline_minutes_per_task",
        "origin",
        "created_at",
        "updated_at",
    ];

    /// Hydrates the domain aggregate (re-run invariants via serde).
    pub fn into_domain(self) -> Result<Agent> {
        let merged = merge_spec(
            &self.spec,
            json!({
                "id": self.id,
                "tenant_id": self.tenant_id,
                "organization_id": self.organization_id,
                "name": self.name,
                "slug": self.slug,
                "status": self.status,
                "baseline_minutes_per_task": self.baseline_minutes_per_task,
                "origin": self.origin,
                "created_at": ts_json(self.created_at),
                "updated_at": ts_json(self.updated_at),
            })
            .as_object()
            .cloned()
            .unwrap_or_default(),
            "agents",
        )?;
        hydrate("agents", merged)
    }

    /// Extracts the row (columns + spec) from the domain aggregate.
    pub fn from_domain(agent: &Agent) -> Result<Self> {
        let (map, spec) = split_spec(&dehydrate("agents", agent)?, Self::MATERIALIZED)?;
        Ok(Self {
            id: take_uuid(&map, "agents", "id")?,
            tenant_id: take_uuid(&map, "agents", "tenant_id")?,
            organization_id: take_uuid(&map, "agents", "organization_id")?,
            name: take_string(&map, "agents", "name")?,
            slug: take_string(&map, "agents", "slug")?,
            status: take_string(&map, "agents", "status")?,
            baseline_minutes_per_task: take_opt_i32(&map, "agents", "baseline_minutes_per_task")?,
            origin: take_string(&map, "agents", "origin")?,
            spec,
            created_at: take_ts(&map, "agents", "created_at")?,
            updated_at: take_ts(&map, "agents", "updated_at")?,
        })
    }
}

/// Row model for `agent_versions` (immutable snapshots).
#[derive(Debug, Clone, PartialEq)]
pub struct AgentVersionRow {
    /// Primary key.
    pub id: Uuid,
    /// Owning agent.
    pub agent_id: Uuid,
    /// Owning tenant (RLS scope).
    pub tenant_id: Uuid,
    /// Monotonic per-agent version number.
    pub version: i32,
    /// Deployment lifecycle string.
    pub deployment: String,
    /// Spec payload (snapshot minus materialized columns).
    pub spec: Value,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

impl AgentVersionRow {
    const MATERIALIZED: &'static [&'static str] = &[
        "id",
        "agent_id",
        "version_number",
        "deployment_status",
        "published_at",
    ];

    /// Hydrates the immutable version aggregate.
    pub fn into_domain(self) -> Result<AgentVersion> {
        let merged = merge_spec(
            &self.spec,
            json!({
                "id": self.id,
                "agent_id": self.agent_id,
                "version_number": u64::try_from(self.version).unwrap_or(0),
                "deployment_status": self.deployment,
                "published_at": ts_json(self.created_at),
            })
            .as_object()
            .cloned()
            .unwrap_or_default(),
            "agent_versions",
        )?;
        hydrate("agent_versions", merged)
    }

    /// Extracts the row from the domain aggregate.
    pub fn from_domain(version: &AgentVersion) -> Result<Self> {
        let (map, spec) = split_spec(&dehydrate("agent_versions", version)?, Self::MATERIALIZED)?;
        Ok(Self {
            id: take_uuid(&map, "agent_versions", "id")?,
            agent_id: take_uuid(&map, "agent_versions", "agent_id")?,
            // AgentVersion serializes tenant implicitly? it does not carry a
            tenant_id: take_opt_uuid(&map, "agent_versions", "tenant_id")?
                .unwrap_or_else(Uuid::nil),
            version: i32::try_from(take_u32(&map, "agent_versions", "version_number")?)
                .map_err(|_| AppError::database("version number beyond SQL range"))?,
            deployment: take_string(&map, "agent_versions", "deployment_status")?,
            spec,
            created_at: take_opt_ts(&map, "agent_versions", "published_at")?
                .unwrap_or_else(|| Timestamp::epoch().into_datetime()),
        })
    }
}

/// Row model for Rust-owned `data_sources` metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct DataSourceRow {
    /// Source primary key.
    pub id: Uuid,
    /// Row-level-security tenant.
    pub tenant_id: Uuid,
    /// Owning project.
    pub project_id: Uuid,
    /// User-visible source name.
    pub name: String,
    /// Stable source adapter kind.
    pub kind: String,
    /// Source lifecycle status.
    pub status: String,
    /// Non-secret adapter configuration.
    pub config: Value,
    /// Optional credential metadata link.
    pub credential_id: Option<Uuid>,
    /// User that created the source.
    pub created_by: Uuid,
    /// Opaque adapter synchronization cursor.
    pub sync_cursor: Option<String>,
    /// Last successful synchronization time.
    pub last_synced_at: Option<DateTime<Utc>>,
    /// Stable error code for the latest failed synchronization.
    pub last_failure_code: Option<String>,
    /// Row creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last row update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl DataSourceRow {
    /// Hydrates the aggregate and reruns source/config invariants.
    pub fn into_domain(self) -> Result<DataSource> {
        let source: DataSource = hydrate(
            "data_sources",
            json!({
                "id": self.id,
                "tenant_id": self.tenant_id,
                "project_id": self.project_id,
                "name": self.name,
                "kind": self.kind,
                "status": self.status,
                "config": self.config,
                "credential_id": self.credential_id,
                "created_by": self.created_by,
                "sync_cursor": self.sync_cursor,
                "last_synced_at": self.last_synced_at.map(ts_json),
                "last_failure_code": self.last_failure_code,
                "created_at": ts_json(self.created_at),
                "updated_at": ts_json(self.updated_at),
            }),
        )?;
        source.validate()?;
        Ok(source)
    }

    /// Extracts all persisted DataSource fields.
    pub fn from_domain(source: &DataSource) -> Result<Self> {
        source.validate()?;
        let value = dehydrate("data_sources", source)?;
        let map = object(&value)?;
        Ok(Self {
            id: take_uuid(map, "data_sources", "id")?,
            tenant_id: take_uuid(map, "data_sources", "tenant_id")?,
            project_id: take_uuid(map, "data_sources", "project_id")?,
            name: take_string(map, "data_sources", "name")?,
            kind: take_string(map, "data_sources", "kind")?,
            status: take_string(map, "data_sources", "status")?,
            config: map.get("config").cloned().unwrap_or(Value::Null),
            credential_id: take_opt_uuid(map, "data_sources", "credential_id")?,
            created_by: take_uuid(map, "data_sources", "created_by")?,
            sync_cursor: take_opt_string(map, "data_sources", "sync_cursor")?,
            last_synced_at: take_opt_ts(map, "data_sources", "last_synced_at")?,
            last_failure_code: take_opt_string(map, "data_sources", "last_failure_code")?,
            created_at: take_ts(map, "data_sources", "created_at")?,
            updated_at: take_ts(map, "data_sources", "updated_at")?,
        })
    }
}

/// Row model for Rust-owned `documents` metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct DocumentRow {
    /// Document primary key.
    pub id: Uuid,
    /// Row-level-security tenant.
    pub tenant_id: Uuid,
    /// Owning project.
    pub project_id: Uuid,
    /// Owning Rust-managed source.
    pub source_id: Uuid,
    /// Display title.
    pub title: String,
    /// Validated MIME type.
    pub mime_type: String,
    /// Exact document content size in bytes.
    pub size_bytes: i64,
    /// Lowercase SHA-256 content digest.
    pub content_sha256: String,
    /// Ingestion lifecycle status.
    pub status: String,
    /// Number of chunks reported by the RAG plane.
    pub chunk_count: i32,
    /// Stable failure code for the latest failed ingestion.
    pub failure_code: Option<String>,
    /// Role/principal filters applied to retrieval.
    pub access_policy: Value,
    /// Non-secret caller-supplied metadata.
    pub metadata: Value,
    /// User that created the document metadata.
    pub created_by: Uuid,
    /// Row creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last row update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl DocumentRow {
    /// Hydrates the aggregate and reruns metadata invariants.
    pub fn into_domain(self) -> Result<Document> {
        let size_bytes = u64::try_from(self.size_bytes)
            .map_err(|_| AppError::database("documents.size_bytes is negative"))?;
        let chunk_count = u32::try_from(self.chunk_count)
            .map_err(|_| AppError::database("documents.chunk_count is negative or out of range"))?;
        let metadata = self
            .metadata
            .as_object()
            .cloned()
            .ok_or_else(|| AppError::database("documents.metadata is not an object"))?;
        let document: Document = hydrate(
            "documents",
            json!({
                "id": self.id,
                "tenant_id": self.tenant_id,
                "project_id": self.project_id,
                "source_id": self.source_id,
                "title": self.title,
                "mime_type": self.mime_type,
                "size_bytes": size_bytes,
                "content_sha256": self.content_sha256,
                "status": self.status,
                "chunk_count": chunk_count,
                "failure_code": self.failure_code,
                "access_policy": self.access_policy,
                "metadata": metadata,
                "created_by": self.created_by,
                "created_at": ts_json(self.created_at),
                "updated_at": ts_json(self.updated_at),
            }),
        )?;
        document.validate()?;
        Ok(document)
    }

    /// Extracts all persisted Document fields.
    pub fn from_domain(document: &Document) -> Result<Self> {
        document.validate()?;
        let value = dehydrate("documents", document)?;
        let map = object(&value)?;
        let size_bytes = take_u64(map, "documents", "size_bytes")?;
        let chunk_count = take_u32(map, "documents", "chunk_count")?;
        let metadata = map
            .get("metadata")
            .and_then(Value::as_object)
            .cloned()
            .map(Value::Object)
            .ok_or_else(|| AppError::database("documents.metadata is not an object"))?;
        Ok(Self {
            id: take_uuid(map, "documents", "id")?,
            tenant_id: take_uuid(map, "documents", "tenant_id")?,
            project_id: take_uuid(map, "documents", "project_id")?,
            source_id: take_uuid(map, "documents", "source_id")?,
            title: take_string(map, "documents", "title")?,
            mime_type: take_string(map, "documents", "mime_type")?,
            size_bytes: i64::try_from(size_bytes)
                .map_err(|_| AppError::database("documents.size_bytes exceeds SQL range"))?,
            content_sha256: take_string(map, "documents", "content_sha256")?,
            status: take_string(map, "documents", "status")?,
            chunk_count: i32::try_from(chunk_count)
                .map_err(|_| AppError::database("documents.chunk_count exceeds SQL range"))?,
            failure_code: take_opt_string(map, "documents", "failure_code")?,
            access_policy: map
                .get("access_policy")
                .cloned()
                .unwrap_or_else(|| json!({})),
            metadata,
            created_by: take_uuid(map, "documents", "created_by")?,
            created_at: take_ts(map, "documents", "created_at")?,
            updated_at: take_ts(map, "documents", "updated_at")?,
        })
    }
}

/// Row model for the immutable `workflow_versions` snapshot table.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowVersionRow {
    /// Snapshot primary key.
    pub id: Uuid,
    /// Row-level-security tenant.
    pub tenant_id: Uuid,
    /// Owning workflow.
    pub workflow_id: Uuid,
    /// Monotonic per-workflow version number.
    pub version: i32,
    /// Immutable validated graph snapshot.
    pub graph: Value,
    /// Deterministic topological node order.
    pub topo_order: Vec<String>,
    /// Lowercase SHA-256 digest of graph and order.
    pub checksum: String,
    /// Earlier version this new snapshot rolls back to, if any.
    pub rollback_of: Option<Uuid>,
    /// Publishing user, absent for a system publication.
    pub created_by: Option<Uuid>,
    /// Snapshot creation timestamp.
    pub created_at: DateTime<Utc>,
}

impl WorkflowVersionRow {
    /// Hydrates the immutable snapshot and checks its checksum.
    pub fn into_domain(self) -> Result<WorkflowVersion> {
        let version = u64::try_from(self.version)
            .map_err(|_| AppError::database("workflow_versions.version must be positive"))?;
        let graph = hydrate("workflow_versions", self.graph)?;
        WorkflowVersion::rehydrate(
            WorkflowVersionId::from_uuid(self.id),
            TenantId::from_uuid(self.tenant_id),
            WorkflowId::from_uuid(self.workflow_id),
            version,
            graph,
            self.topo_order,
            self.checksum,
            self.created_by.map(UserId::from_uuid),
            Timestamp::from_datetime(self.created_at),
            self.rollback_of.map(WorkflowVersionId::from_uuid),
        )
    }

    /// Extracts the immutable snapshot from the aggregate.
    pub fn from_domain(version: &WorkflowVersion) -> Result<Self> {
        let graph = serde_json::to_value(version.graph())
            .map_err(|error| AppError::serialization(format!("workflow version graph: {error}")))?;
        Ok(Self {
            id: Uuid::from(version.id()),
            tenant_id: Uuid::from(version.tenant_id()),
            workflow_id: Uuid::from(version.workflow_id()),
            version: i32::try_from(version.version_number())
                .map_err(|_| AppError::database("workflow version exceeds SQL integer range"))?,
            graph,
            topo_order: version.topo_order().to_vec(),
            checksum: version.checksum().to_owned(),
            rollback_of: version.rollback_of().map(Uuid::from),
            created_by: version.created_by().map(Uuid::from),
            created_at: version.created_at().into_datetime(),
        })
    }
}

/// Row model for append-only `rollback_records`.
#[derive(Debug, Clone, PartialEq)]
pub struct RollbackRecordRow {
    /// Rollback history primary key.
    pub id: Uuid,
    /// Row-level-security tenant.
    pub tenant_id: Uuid,
    /// Stable governed resource family.
    pub resource_kind: String,
    /// UUID of the governed resource.
    pub resource_id: Uuid,
    /// Version current before the rollback was published.
    pub from_version: i64,
    /// New monotonic version produced by the rollback.
    pub to_version: i64,
    /// Audited principal snapshot.
    pub actor: Value,
    /// Operator-supplied rollback reason.
    pub reason: String,
    /// Tenant-unique key for replay-safe rollback requests.
    pub idempotency_key: String,
    /// Immutable record timestamp.
    pub created_at: DateTime<Utc>,
}

impl RollbackRecordRow {
    /// Hydrates the immutable rollback record and reruns its invariants.
    pub fn into_domain(self) -> Result<RollbackRecord> {
        let resource_kind: RollbackResourceKind = serde_json::from_value(json!(self.resource_kind))
            .map_err(|_| AppError::database("rollback_records.resource_kind is invalid"))?;
        let actor = hydrate("rollback_records", self.actor)?;
        let from_version = u64::try_from(self.from_version)
            .map_err(|_| AppError::database("rollback_records.from_version is negative"))?;
        let to_version = u64::try_from(self.to_version)
            .map_err(|_| AppError::database("rollback_records.to_version is negative"))?;
        RollbackRecord::rehydrate(
            RollbackRecordId::from_uuid(self.id),
            TenantId::from_uuid(self.tenant_id),
            resource_kind,
            self.resource_id,
            from_version,
            to_version,
            actor,
            self.reason,
            self.idempotency_key,
            Timestamp::from_datetime(self.created_at),
        )
    }

    /// Extracts the append-only rollback record.
    pub fn from_domain(record: &RollbackRecord) -> Result<Self> {
        Ok(Self {
            id: Uuid::from(record.id),
            tenant_id: Uuid::from(record.tenant_id),
            resource_kind: record.resource_kind.to_string(),
            resource_id: record.resource_id,
            from_version: i64::try_from(record.from_version)
                .map_err(|_| AppError::database("rollback from_version exceeds SQL range"))?,
            to_version: i64::try_from(record.to_version)
                .map_err(|_| AppError::database("rollback to_version exceeds SQL range"))?,
            actor: serde_json::to_value(&record.actor)
                .map_err(|error| AppError::serialization(format!("rollback actor: {error}")))?,
            reason: record.reason.clone(),
            idempotency_key: record.idempotency_key.clone(),
            created_at: record.created_at.into_datetime(),
        })
    }
}

// ---------------------------------------------------------------------------
// workflows
// ---------------------------------------------------------------------------

/// Row model for `workflows` (spec-JSONB style; the graph travels inside the
/// spec — normalized wf_nodes/edges mirroring is a deliberate later step).
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowRow {
    /// Primary key.
    pub id: Uuid,
    /// Owning tenant (RLS scope).
    pub tenant_id: Uuid,
    /// Owning organization.
    pub organization_id: Uuid,
    /// Optional project link.
    pub project_id: Option<Uuid>,
    /// Human name.
    pub name: String,
    /// Unique-per-tenant slug (derived from the domain-tolerated name).
    pub slug: String,
    /// Status string.
    pub status: String,
    /// Spec payload (aggregate minus materialized columns).
    pub spec: Value,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

impl WorkflowRow {
    const MATERIALIZED: &'static [&'static str] = &[
        "id",
        "tenant_id",
        "organization_id",
        "project_id",
        "name",
        "status",
        "created_at",
        "updated_at",
    ];

    /// Hydrates the workflow aggregate.
    pub fn into_domain(self) -> Result<Workflow> {
        let mut columns = json!({
            "id": self.id,
            "tenant_id": self.tenant_id,
            "organization_id": self.organization_id,
            "name": self.name,
            "status": self.status,
            "created_at": ts_json(self.created_at),
            "updated_at": ts_json(self.updated_at),
        })
        .as_object()
        .cloned()
        .unwrap_or_default();
        if let Some(project) = self.project_id {
            columns.insert("project_id".to_owned(), json!(project));
        }
        let merged = merge_spec(&self.spec, columns, "workflows")?;
        hydrate("workflows", merged)
    }

    /// Extracts the row; `slug` is supplied by the repository (derived).
    pub fn from_domain(workflow: &Workflow, slug: &str) -> Result<Self> {
        let (map, spec) = split_spec(&dehydrate("workflows", workflow)?, Self::MATERIALIZED)?;
        Ok(Self {
            id: take_uuid(&map, "workflows", "id")?,
            tenant_id: take_uuid(&map, "workflows", "tenant_id")?,
            organization_id: take_uuid(&map, "workflows", "organization_id")?,
            project_id: Some(take_uuid(&map, "workflows", "project_id")?),
            name: take_string(&map, "workflows", "name")?,
            slug: slug.to_owned(),
            status: take_string(&map, "workflows", "status")?,
            spec,
            created_at: take_ts(&map, "workflows", "created_at")?,
            updated_at: take_ts(&map, "workflows", "updated_at")?,
        })
    }
}

// ---------------------------------------------------------------------------
// executions (state jsonb under `_mas_spec`)
// ---------------------------------------------------------------------------

/// Row model for `executions` (spec under `state._mas_spec`).
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionRow {
    /// Primary key.
    pub id: Uuid,
    /// Owning tenant (RLS scope).
    pub tenant_id: Uuid,
    /// Owning organization.
    pub organization_id: Uuid,
    /// Optional project link.
    pub project_id: Option<Uuid>,
    /// Optional workflow reference.
    pub workflow_id: Option<Uuid>,
    /// Optional agent reference.
    pub agent_id: Option<Uuid>,
    /// Optional parent (delegation tree).
    pub parent_id: Option<Uuid>,
    /// Status string.
    pub status: String,
    /// Execution input payload.
    pub input: Value,
    /// Final output (redacted), if finished.
    pub output: Option<Value>,
    /// Classified failure record, if any.
    pub error: Option<Value>,
    /// `state` column value (may carry sibling checkpoint data).
    pub state: Value,
    /// End-to-end tracing key.
    pub correlation_id: String,
    /// Start time.
    pub started_at: Option<DateTime<Utc>>,
    /// Finish time.
    pub finished_at: Option<DateTime<Utc>>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

impl ExecutionRow {
    const MATERIALIZED: &'static [&'static str] = &[
        "id",
        "tenant_id",
        "organization_id",
        "project_id",
        "workflow_id",
        "agent_id",
        "parent_execution_id",
        "status",
        "input",
        "output",
        "correlation_id",
        "started_at",
        "finished_at",
        "created_at",
        "updated_at",
    ];

    /// Hydrates the aggregate (siblings of `_mas_spec` in `state` are kept).
    pub fn into_domain(self) -> Result<Execution> {
        let spec = self
            .state
            .get(SPEC_KEY)
            .cloned()
            .unwrap_or(Value::Object(Map::new()));
        let mut columns = json!({
            "id": self.id,
            "tenant_id": self.tenant_id,
            "organization_id": self.organization_id,
            "status": self.status,
            "input": self.input,
            "correlation_id": self.correlation_id,
            "created_at": ts_json(self.created_at),
            "updated_at": ts_json(self.updated_at),
        })
        .as_object()
        .cloned()
        .unwrap_or_default();
        if let Some(project) = self.project_id {
            columns.insert("project_id".to_owned(), json!(project));
        }
        if let Some(workflow) = self.workflow_id {
            columns.insert("workflow_id".to_owned(), json!(workflow));
        }
        if let Some(agent) = self.agent_id {
            columns.insert("agent_id".to_owned(), json!(agent));
        }
        if let Some(parent) = self.parent_id {
            columns.insert("parent_execution_id".to_owned(), json!(parent));
        }
        if let Some(output) = self.output {
            columns.insert("output".to_owned(), output);
        }
        if let Some(error) = self.error {
            columns.insert("failure".to_owned(), error);
        }
        if let Some(started) = self.started_at {
            columns.insert("started_at".to_owned(), ts_json(started));
        }
        if let Some(finished) = self.finished_at {
            columns.insert("finished_at".to_owned(), ts_json(finished));
        }
        let merged = merge_spec(&spec, columns, "executions")?;
        hydrate("executions", merged)
    }

    /// Extracts the row; checkpoint siblings under `state` are preserved if
    /// the caller merges them back (the repository owns full overwrite).
    pub fn from_domain(execution: &Execution) -> Result<Self> {
        let (map, spec) = split_spec(&dehydrate("executions", execution)?, Self::MATERIALIZED)?;
        let project = take_opt_uuid(&map, "executions", "project_id")?;
        Ok(Self {
            id: take_uuid(&map, "executions", "id")?,
            tenant_id: take_uuid(&map, "executions", "tenant_id")?,
            organization_id: take_uuid(&map, "executions", "organization_id")?,
            project_id: project,
            workflow_id: take_opt_uuid(&map, "executions", "workflow_id")?,
            agent_id: take_opt_uuid(&map, "executions", "agent_id")?,
            parent_id: take_opt_uuid(&map, "executions", "parent_execution_id")?,
            status: take_string(&map, "executions", "status")?,
            input: map
                .get("input")
                .cloned()
                .unwrap_or(Value::Object(Map::new())),
            output: map.get("output").cloned().filter(|v| !v.is_null()),
            error: None, // failure string rides inside spec (domain Error)
            state: json!({ SPEC_KEY: spec }),
            correlation_id: take_string(&map, "executions", "correlation_id")?,
            started_at: take_opt_ts(&map, "executions", "started_at")?,
            finished_at: take_opt_ts(&map, "executions", "finished_at")?,
            created_at: take_ts(&map, "executions", "created_at")?,
            updated_at: take_ts(&map, "executions", "updated_at")?,
        })
    }
}

// ---------------------------------------------------------------------------
// schedules
// ---------------------------------------------------------------------------

/// Row model for `schedules` (target/rule JSONB carry the aggregate's
/// task_template + kind projection).
#[derive(Debug, Clone, PartialEq)]
pub struct ScheduleRow {
    /// Primary key.
    pub id: Uuid,
    /// Owning tenant (RLS scope).
    pub tenant_id: Uuid,
    /// Owning organization.
    pub organization_id: Uuid,
    /// Unique-per-tenant name.
    pub name: String,
    /// Target payload (`{"task_template": …, "extra_spec": {…}}`).
    pub target: Value,
    /// Rule payload (`{"kind": …}`).
    pub rule: Value,
    /// Status string.
    pub status: String,
    /// IANA timezone (always "UTC" today; structural).
    pub timezone: String,
    /// Next fire plan.
    pub next_run_at: Option<DateTime<Utc>>,
    /// Last fire record.
    pub last_run_at: Option<DateTime<Utc>>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

impl ScheduleRow {
    const MATERIALIZED: &'static [&'static str] = &[
        "id",
        "tenant_id",
        "organization_id",
        "name",
        "status",
        "next_run_at",
        "last_run_at",
        "created_at",
        "updated_at",
    ];

    /// Reserved keys inside `target` carrying the non-task_template part.
    pub const TARGET_SPEC_KEY: &'static str = SPEC_KEY;

    /// Hydrates the schedule aggregate.
    pub fn into_domain(self) -> Result<Schedule> {
        let mut spec = self
            .target
            .get(Self::TARGET_SPEC_KEY)
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        if let Some(kind) = self.rule.get("kind") {
            spec.insert("kind".to_owned(), kind.clone());
        }
        if let Some(template) = self.target.get("task_template") {
            spec.insert("task_template".to_owned(), template.clone());
        }
        spec.insert("id".to_owned(), json!(self.id));
        spec.insert("tenant_id".to_owned(), json!(self.tenant_id));
        spec.insert("organization_id".to_owned(), json!(self.organization_id));
        spec.insert("name".to_owned(), json!(self.name));
        spec.insert("status".to_owned(), json!(self.status));
        if let Some(next) = self.next_run_at {
            spec.insert("next_run_at".to_owned(), ts_json(next));
        }
        if let Some(last) = self.last_run_at {
            spec.insert("last_run_at".to_owned(), ts_json(last));
        }
        spec.insert("created_at".to_owned(), ts_json(self.created_at));
        spec.insert("updated_at".to_owned(), ts_json(self.updated_at));
        hydrate("schedules", Value::Object(spec))
    }

    /// Extracts the row from the domain aggregate (organization is NOT a
    /// domain field — the repository passes it through explicitly).
    pub fn from_domain(schedule: &Schedule, organization: Uuid) -> Result<Self> {
        let full = dehydrate("schedules", schedule)?;
        let (map, spec) = split_spec(&full, Self::MATERIALIZED)?;
        // `task_template` and `kind` are domain fields but NOT materialized
        // columns: they belong to the `target` / `rule` jsonb columns, so they
        // must be read from the full aggregate — reading them from `map` (the
        // materialized projection) silently dropped `task_template`.
        let mut target = full
            .get("task_template")
            .map(|t| json!({ "task_template": t.clone() }))
            .and_then(|v| v.as_object().cloned())
            .unwrap_or_default();
        let mut spec_rest = spec.as_object().cloned().unwrap_or_default();
        spec_rest.remove("task_template");
        spec_rest.remove("kind");
        spec_rest.remove("tenant_id");
        if !spec_rest.is_empty() {
            target.insert(Self::TARGET_SPEC_KEY.to_owned(), Value::Object(spec_rest));
        }
        let rule = match full.get("kind").cloned() {
            Some(kind) => json!({ "kind": kind }),
            None => json!({}),
        };
        Ok(Self {
            id: take_uuid(&map, "schedules", "id")?,
            tenant_id: take_uuid(&map, "schedules", "tenant_id")?,
            organization_id: organization,
            name: take_string(&map, "schedules", "name")?,
            target: Value::Object(target),
            rule,
            status: take_string(&map, "schedules", "status")?,
            timezone: "UTC".to_owned(),
            next_run_at: take_opt_ts(&map, "schedules", "next_run_at")?,
            last_run_at: take_opt_ts(&map, "schedules", "last_run_at")?,
            created_at: take_ts(&map, "schedules", "created_at")?,
            updated_at: take_ts(&map, "schedules", "updated_at")?,
        })
    }
}

// ---------------------------------------------------------------------------
// tasks (payload jsonb under `_mas_spec`)
// ---------------------------------------------------------------------------

/// Row model for `tasks` (spec under `payload._mas_spec`).
#[derive(Debug, Clone, PartialEq)]
pub struct TaskRow {
    /// Primary key.
    pub id: Uuid,
    /// Owning tenant (RLS scope).
    pub tenant_id: Uuid,
    /// Owning organization.
    pub organization_id: Uuid,
    /// Optional project link.
    pub project_id: Option<Uuid>,
    /// Optional owning execution.
    pub execution_id: Option<Uuid>,
    /// Client-supplied duplicate guard (unique per tenant).
    pub idempotency_key: String,
    /// Task discriminator (the domain `operation` string).
    pub kind: String,
    /// Status string.
    pub status: String,
    /// Priority string.
    pub priority: String,
    /// Payload (input + `_mas_spec` remainder).
    pub payload: Value,
    /// Optional result output.
    pub result: Option<Value>,
    /// Earliest delivery time.
    pub not_before: DateTime<Utc>,
    /// Optional deadline.
    pub deadline_at: Option<DateTime<Utc>>,
    /// Correlation key.
    pub correlation_id: String,
    /// Who created the task. Materialized: `domain::Task::created_by` is a
    /// required field, so hydration must always be able to read it. Rows
    /// written by the durable broker carry the platform principal default.
    pub created_by: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

impl TaskRow {
    /// Materialized columns for `tasks`. Exposed so the repository test can
    /// assert that a required domain field is actually materialized.
    pub const MATERIALIZED: &'static [&'static str] = &[
        "id",
        "tenant_id",
        "organization_id",
        "project_id",
        "execution_id",
        "correlation_id",
        "idempotency_key",
        "operation",
        "status",
        "priority",
        "deadline",
        "created_by",
        "created_at",
        "updated_at",
    ];

    /// Hydrates the task aggregate.
    pub fn into_domain(self) -> Result<Task> {
        let mut spec = self
            .payload
            .get(SPEC_KEY)
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        if let Some(input) = self.payload.get("input") {
            spec.insert("input".to_owned(), input.clone());
        }
        spec.insert("id".to_owned(), json!(self.id));
        spec.insert("tenant_id".to_owned(), json!(self.tenant_id));
        spec.insert("organization_id".to_owned(), json!(self.organization_id));
        if let Some(project) = self.project_id {
            spec.insert("project_id".to_owned(), json!(project));
        }
        if let Some(execution) = self.execution_id {
            spec.insert("execution_id".to_owned(), json!(execution));
        }
        spec.insert("correlation_id".to_owned(), json!(self.correlation_id));
        spec.insert("idempotency_key".to_owned(), json!(self.idempotency_key));
        spec.insert("operation".to_owned(), json!(self.kind));
        spec.insert("status".to_owned(), json!(self.status));
        spec.insert("priority".to_owned(), json!(self.priority));
        if let Some(deadline) = self.deadline_at {
            spec.insert("deadline".to_owned(), ts_json(deadline));
        }
        // Round-trip the retry backoff: without it a requeued task is
        // claimable the instant it is written.
        spec.insert("not_before".to_owned(), ts_json(self.not_before));
        spec.insert("created_by".to_owned(), json!(self.created_by));
        spec.insert("created_at".to_owned(), ts_json(self.created_at));
        spec.insert("updated_at".to_owned(), ts_json(self.updated_at));
        hydrate("tasks", Value::Object(spec))
    }

    /// Extracts the row from the domain aggregate.
    pub fn from_domain(task: &Task) -> Result<Self> {
        let (map, spec_value) = split_spec(&dehydrate("tasks", task)?, Self::MATERIALIZED)?;
        let mut spec = spec_value.as_object().cloned().unwrap_or_default();
        spec.remove("input");
        spec.remove("result");
        // `not_before` is materialized onto the column, NOT dropped: it is the
        // retry backoff, and dropping it made every retry immediately
        // claimable again (a retry storm instead of a backoff).
        let not_before = match spec.remove("not_before") {
            Some(Value::String(raw)) => Timestamp::parse_rfc3339(&raw)
                .map(Timestamp::into_datetime)
                .map_err(|_| {
                    AppError::database("invalid timestamp 'not_before' for table tasks")
                })?,
            _ => Timestamp::epoch().into_datetime(),
        };
        let mut payload = map
            .get("input")
            .map(|i| json!({ "input": i.clone() }))
            .and_then(|v| v.as_object().cloned())
            .unwrap_or_default();
        if !spec.is_empty() {
            payload.insert(SPEC_KEY.to_owned(), Value::Object(spec));
        }
        Ok(Self {
            id: take_uuid(&map, "tasks", "id")?,
            tenant_id: take_uuid(&map, "tasks", "tenant_id")?,
            organization_id: take_uuid(&map, "tasks", "organization_id")?,
            project_id: Some(take_uuid(&map, "tasks", "project_id")?),
            execution_id: take_opt_uuid(&map, "tasks", "execution_id")?,
            idempotency_key: take_string(&map, "tasks", "idempotency_key")?,
            kind: take_string(&map, "tasks", "operation")?,
            status: take_string(&map, "tasks", "status")?,
            priority: take_string(&map, "tasks", "priority")?,
            payload: Value::Object(payload),
            result: map.get("result").cloned().filter(|v| !v.is_null()),
            not_before,
            deadline_at: take_opt_ts(&map, "tasks", "deadline")?,
            correlation_id: map
                .get("correlation_id")
                .and_then(Value::as_str)
                .unwrap_or("00000000-0000-0000-0000-000000000000")
                .to_owned(),
            created_by: map
                .get("created_by")
                .and_then(Value::as_str)
                .filter(|raw| !raw.is_empty())
                .unwrap_or("system")
                .to_owned(),
            created_at: take_ts(&map, "tasks", "created_at")?,
            updated_at: take_ts(&map, "tasks", "updated_at")?,
        })
    }
}

#[cfg(test)]
mod spec_rows_tests {
    use super::*;
    use mas_common::enums::{AgentStatus, DeploymentStatus, TaskPriority, TaskStatus};

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-09-30T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn execution_step_row_roundtrip_preserves_optional_node_and_zero_attempt() {
        let tenant = TenantId::new();
        let execution_id = mas_common::ids::ExecutionId::new();
        let step = ExecutionStep::new(execution_id, "tool.invoke", 2, None);
        let row = ExecutionStepRow::from_domain(&step, tenant).expect("step row");
        assert_eq!(row.node_key, None);
        assert_eq!(row.attempt, 0);
        let restored = row.into_domain().expect("step hydration");
        assert_eq!(restored.id, step.id);
        assert_eq!(restored.execution_id, step.execution_id);
        assert_eq!(restored.node_key, None);
        assert_eq!(restored.sequence, 2);
        assert_eq!(restored.attempt, 0);
        assert_eq!(restored.step_type, "tool.invoke");
    }

    #[test]
    fn agent_row_roundtrip_via_synthetic_aggregate() {
        let agent_json = json!({
            "id": Uuid::now_v7(),
            "tenant_id": Uuid::now_v7(),
            "organization_id": Uuid::now_v7(),
            "project_id": Uuid::now_v7(),
            "name": "Triage Copilot",
            "slug": "triage-copilot",
            "kind": "standard",
            "status": "active",
            "config": {
                "model_hint": "model-catalog:gpt-5-mini",
                "resource_limits": {
                    "timeout_ms": 30000,
                    "max_steps": 64,
                    "max_parallel_tasks": 4,
                    "max_tool_calls": 32,
                    "max_payload_bytes": 262144,
                    "max_retries": 3,
                },
                "tags": ["triage"],
            },
            "capabilities": [],
            "created_at": "2026-09-30T00:00:00.000Z",
            "updated_at": "2026-09-30T00:00:00.000Z",
        });
        let agent: Agent = serde_json::from_value(agent_json).expect("synthetic agent");
        let row = AgentRow::from_domain(&agent).expect("row");
        assert_eq!(row.status, "active");
        assert_eq!(row.name, "Triage Copilot");
        assert_eq!(
            row.spec.get("kind").and_then(Value::as_str),
            Some("standard")
        );
        assert!(
            row.spec.get("name").is_none(),
            "materialized key leaked into spec"
        );
        let back = row.into_domain().expect("hydrate");
        let roundtrip = serde_json::to_value(&back).expect("dehydrate");
        assert_eq!(roundtrip, serde_json::to_value(&agent).expect("expect"));
    }

    #[test]
    fn task_row_roundtrip_preserves_correlation_and_request_identity() {
        let tenant = TenantId::new();
        let organization = OrganizationId::new();
        let project = ProjectId::new();
        let request_id = Uuid::now_v7();
        let agent_version_id = mas_common::ids::AgentVersionId::new();
        let correlation_id = "corr-durable-task-roundtrip";
        let task = mas_domain::Task::new(
            tenant,
            organization,
            project,
            Some(mas_common::ids::AgentId::new()),
            None,
            "agent.run",
            json!({"prompt": "safe test input"}),
            TaskPriority::Normal,
            "idem-task-row-roundtrip",
            "test-user",
        )
        .expect("task")
        .with_agent_version_id(agent_version_id)
        .expect("version pin")
        .with_request_id(request_id)
        .with_correlation_id(correlation_id)
        .expect("correlation id");

        let row = TaskRow::from_domain(&task).expect("row");
        assert_eq!(row.correlation_id, correlation_id);
        let restored = row.into_domain().expect("restored task");
        assert_eq!(restored.correlation_id, correlation_id);
        assert_eq!(restored.request_id, Some(request_id));
        assert_eq!(restored.agent_version_id, Some(agent_version_id));
    }

    #[test]
    fn every_agent_status_string_satisfies_the_schema_check() {
        // The status CHECK in 0004_agents: draft, active, disabled, archived.
        let allowed = ["draft", "active", "disabled", "archived"];
        for status in AgentStatus::ALL {
            assert!(
                allowed.contains(&status.as_str()),
                "schema drift: {}",
                status.as_str()
            );
        }
        // agent_versions deployment CHECK parity:
        let deployments = [
            "not_deployed",
            "deploying",
            "deployed",
            "failed",
            "rolled_back",
        ];
        for status in DeploymentStatus::ALL {
            assert!(deployments.contains(&status.as_str()));
        }
        // tasks status/priority parity with 0006_execution:
        let task_statuses = [
            "pending",
            "queued",
            "running",
            "completed",
            "failed",
            "cancelled",
            "dead_lettered",
        ];
        for status in TaskStatus::ALL {
            assert!(task_statuses.contains(&status.as_str()));
        }
        let priorities = ["low", "normal", "high", "critical"];
        for priority in TaskPriority::ALL {
            assert!(priorities.contains(&priority.as_str()));
        }
    }

    #[test]
    fn split_and_merge_spec_are_inverse() {
        let full = json!({"a": 1, "b": {"nested": true}, "c": "x"});
        let (columns, spec) = split_spec(&full, &["a", "b"]).expect("split");
        assert_eq!(columns["a"], json!(1));
        assert_eq!(spec, json!({"c": "x"}));
        let merged = merge_spec(&spec, columns, "t").expect("merge");
        assert_eq!(merged, full);
    }

    #[test]
    fn merge_spec_prefers_columns_over_stale_spec() {
        let spec = json!({"status": "stale_value", "keep": 1});
        let merged = merge_spec(
            &spec,
            json!({"status": "current"}).as_object().cloned().unwrap(),
            "t",
        )
        .expect("merge");
        assert_eq!(merged["status"], "current");
        assert_eq!(merged["keep"], json!(1));
    }

    #[test]
    fn timestamps_format_stable_milliseconds() {
        assert_eq!(ts_json(now()), json!("2026-09-30T00:00:00.000Z"));
    }
}
