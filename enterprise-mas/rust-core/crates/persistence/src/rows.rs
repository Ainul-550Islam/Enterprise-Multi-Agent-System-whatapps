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
use mas_common::ids::{EventId, OrganizationId, ProjectId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::membership::Membership;
use mas_domain::organization::Organization;
use mas_domain::project::{Project, ProjectConfig};
use mas_domain::tenant::{IsolationMode, Tenant};
use mas_domain::user::User;
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
        let (map, spec) = split_spec(&dehydrate("schedules", schedule)?, Self::MATERIALIZED)?;
        let mut target = map
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
        let rule = match map
            .get("kind")
            .cloned()
            .or_else(|| dehydrate("schedules", schedule).ok()?.get("kind").cloned())
        {
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
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

impl TaskRow {
    const MATERIALIZED: &'static [&'static str] = &[
        "id",
        "tenant_id",
        "organization_id",
        "project_id",
        "execution_id",
        "idempotency_key",
        "operation",
        "status",
        "priority",
        "deadline",
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
        spec.insert("idempotency_key".to_owned(), json!(self.idempotency_key));
        spec.insert("operation".to_owned(), json!(self.kind));
        spec.insert("status".to_owned(), json!(self.status));
        spec.insert("priority".to_owned(), json!(self.priority));
        if let Some(deadline) = self.deadline_at {
            spec.insert("deadline".to_owned(), ts_json(deadline));
        }
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
        spec.remove("not_before");
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
            not_before: Timestamp::epoch().into_datetime(),
            deadline_at: take_opt_ts(&map, "tasks", "deadline")?,
            correlation_id: map
                .get("correlation_id")
                .and_then(Value::as_str)
                .unwrap_or("00000000-0000-0000-0000-000000000000")
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
