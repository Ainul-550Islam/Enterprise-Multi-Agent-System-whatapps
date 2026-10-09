//! PostgreSQL adapter for the tenant-scoped analytics snapshot read port.
//!
//! Dashboard calculations remain in `mas-application`; this adapter only
//! reads authoritative aggregates under the request's RLS-scoped transaction.
//! It deliberately returns domain objects rather than dashboard-specific SQL
//! so the in-memory and PostgreSQL paths share identical KPI/delta rules.

use mas_application::stores::{AnalyticsSnapshot, AnalyticsStorePort};
use mas_common::error::AppError;
use mas_common::ids::{OrganizationId, TenantId, UsageRecordId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{ActorRef, QuotaDimension, UsageRecord};
use sqlx::PgPool;

use crate::repositories::agents::AgentStore;
use crate::repositories::control_plane::usage::{PostgresUsageLedger, UsageRow};
use crate::repositories::executions::ExecutionStore;

/// Upper bound for raw metering records loaded into one dashboard snapshot.
///
/// A range is already bounded by the contract; a second hard ceiling protects
/// the API process from a tenant with an unexpectedly dense historical usage
/// stream. Operators can increase this only by replacing the read model with
/// the hourly rollup projection, never through request input.
const MAX_USAGE_RECORDS_PER_SNAPSHOT: i64 = 100_000;

/// Tenant-scoped PostgreSQL analytics read adapter.
#[derive(Debug, Clone)]
pub struct PostgresAnalyticsStore {
    agents: AgentStore,
    executions: ExecutionStore,
    usage: PostgresUsageLedger,
}

impl PostgresAnalyticsStore {
    /// Binds all authoritative read surfaces to the shared application pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            agents: AgentStore::new(pool.clone()),
            executions: ExecutionStore::new(pool.clone()),
            usage: PostgresUsageLedger::new(pool),
        }
    }
}

#[async_trait::async_trait]
impl AnalyticsStorePort for PostgresAnalyticsStore {
    async fn snapshot(
        &self,
        tenant: TenantId,
        previous_from: Timestamp,
        current_to: Timestamp,
    ) -> Result<AnalyticsSnapshot> {
        if previous_from >= current_to {
            return Err(AppError::invalid_field(
                "analytics range",
                "invalid_range",
                "analytics snapshot start must be before its end",
            ));
        }
        let scope = crate::scoped_request::current_scope()?;
        if scope.tenant_id() != tenant {
            return Err(AppError::not_found("tenant", tenant.to_string()));
        }
        let agents = self.agents.list_for_tenant(tenant).await?;
        let executions = self
            .executions
            .list_for_tenant(tenant)
            .await?
            .into_iter()
            .filter(|row| {
                let observed = row.finished_at.unwrap_or(row.created_at);
                observed >= previous_from && observed < current_to
            })
            .collect();
        let rows = self
            .usage
            .for_tenant_between(
                tenant,
                &previous_from,
                &current_to,
                MAX_USAGE_RECORDS_PER_SNAPSHOT.saturating_add(1),
            )
            .await?;
        if i64::try_from(rows.len()).unwrap_or(i64::MAX) > MAX_USAGE_RECORDS_PER_SNAPSHOT {
            return Err(AppError::rate_limited(
                "analytics usage range is too dense; narrow the requested period",
            ));
        }
        let usage_records = rows
            .into_iter()
            .map(usage_record_from_row)
            .collect::<Result<Vec<_>>>()?;
        Ok(AnalyticsSnapshot {
            agents,
            executions,
            usage_records,
        })
    }
}

fn usage_record_from_row(row: UsageRow) -> Result<UsageRecord> {
    let organization_id = row.organization_id.ok_or_else(|| {
        AppError::database("usage_records.organization_id is required for analytics projection")
    })?;
    let quantity = u64::try_from(row.quantity)
        .map_err(|_| AppError::database("usage_records.quantity is negative"))?;
    let dimension = row
        .resource
        .parse::<QuotaDimension>()
        .map_err(|_| AppError::database("usage_records.resource is not a quota dimension"))?;
    let mut metadata = row
        .dimensions
        .as_object()
        .cloned()
        .ok_or_else(|| AppError::database("usage_records.dimensions is not an object"))?;
    if let Some(cost_micros) = row.cost_micros {
        metadata.insert(
            "cost_micros".to_owned(),
            serde_json::Value::from(cost_micros),
        );
    }
    if let Some(currency) = row.cost_currency {
        metadata.insert("currency".to_owned(), serde_json::Value::String(currency));
    }
    if let Some(input) = row.input_quantity {
        metadata.insert("input_quantity".to_owned(), serde_json::Value::from(input));
    }
    if let Some(output) = row.output_quantity {
        metadata.insert(
            "output_quantity".to_owned(),
            serde_json::Value::from(output),
        );
    }
    Ok(UsageRecord {
        id: UsageRecordId::from_uuid(row.id),
        tenant_id: TenantId::from_uuid(row.tenant_id),
        organization_id: OrganizationId::from_uuid(organization_id),
        project_id: row.project_id.map(mas_common::ids::ProjectId::from_uuid),
        actor: ActorRef::system(),
        dimension,
        quantity,
        execution_id: row
            .execution_id
            .map(mas_common::ids::ExecutionId::from_uuid),
        metadata,
        occurred_at: Timestamp::from_datetime(row.occurred_at),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_projection_rejects_missing_organization_instead_of_inventing_scope() {
        let row = UsageRow {
            id: uuid::Uuid::now_v7(),
            tenant_id: uuid::Uuid::now_v7(),
            organization_id: None,
            project_id: None,
            resource: "tokens".to_owned(),
            quantity: 1,
            unit: "units".to_owned(),
            source_id: None,
            dimensions: serde_json::json!({}),
            execution_id: None,
            task_id: None,
            task_attempt_id: None,
            model: None,
            provider: None,
            input_quantity: None,
            output_quantity: None,
            duration_ms: None,
            cost_micros: None,
            cost_currency: None,
            estimated: false,
            metering_key: Some("test".to_owned()),
            occurred_at: chrono::Utc::now(),
            created_at: chrono::Utc::now(),
        };
        assert!(usage_record_from_row(row).is_err());
    }
}
