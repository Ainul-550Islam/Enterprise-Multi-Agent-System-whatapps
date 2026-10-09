//! RLS-scoped PostgreSQL connector repository.
//!
//! Connector configuration is metadata only. This store persists the Rust
//! aggregate after migration 0037 normalized the legacy integrations schema;
//! raw credential material never enters these columns or this adapter.

use chrono::{DateTime, Utc};
use mas_application::stores::ConnectorStorePort;
use mas_common::error::AppError;
use mas_common::ids::{ConnectorId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::Connector;
use mas_integrations::registry::{ConnectorCursor, ConnectorRegistryPort};
use serde_json::{Map, Value};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;

const CONNECTOR_COLUMNS: &str = "id, tenant_id, organization_id, provider, name, display_name, \
    auth_mode, status, credential_id, config, last_tested_at, last_error, created_at, updated_at";

/// Production implementation of the connector catalogue port.
#[derive(Debug, Clone)]
pub struct PostgresConnectorStore {
    pool: PgPool,
}

impl PostgresConnectorStore {
    /// Binds the store to the application's request-scoped pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Pool accessor for diagnostics only; requests use `scoped_request`.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait::async_trait]
impl ConnectorStorePort for PostgresConnectorStore {
    async fn save(&self, connector: &Connector) -> Result<()> {
        validate_connector(connector)?;
        let scope = crate::scoped_request::current_scope()?;
        if scope.tenant_id() != connector.tenant_id
            || scope.organization_id() != connector.organization_id
        {
            return Err(AppError::not_found("connector", connector.id.to_string()));
        }
        let mut scoped = crate::scoped_request::connection().await?;
        let result = sqlx::query(
            "INSERT INTO connectors (
                id, tenant_id, organization_id, name, kind, provider,
                display_name, auth_mode, status, credential_id, config,
                last_tested_at, last_error, created_at, updated_at
             ) VALUES (
                $1, $2, $3, $4, $5, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
             )
             ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                kind = EXCLUDED.kind,
                provider = EXCLUDED.provider,
                display_name = EXCLUDED.display_name,
                auth_mode = EXCLUDED.auth_mode,
                status = EXCLUDED.status,
                credential_id = EXCLUDED.credential_id,
                config = EXCLUDED.config,
                last_tested_at = EXCLUDED.last_tested_at,
                last_error = EXCLUDED.last_error,
                updated_at = EXCLUDED.updated_at
             WHERE connectors.tenant_id = EXCLUDED.tenant_id
               AND connectors.organization_id = EXCLUDED.organization_id",
        )
        .bind(connector.id.into_uuid())
        .bind(connector.tenant_id.into_uuid())
        .bind(connector.organization_id.into_uuid())
        .bind(&connector.name)
        .bind(&connector.provider)
        .bind(&connector.display_name)
        .bind(connector.auth_mode.to_string())
        .bind(connector.status.to_string())
        .bind(connector.credential_id.map(|id| id.into_uuid()))
        .bind(Value::Object(connector.config.clone()))
        .bind(
            connector
                .last_tested_at
                .map(|timestamp| timestamp.into_datetime()),
        )
        .bind(&connector.last_error)
        .bind(connector.created_at.into_datetime())
        .bind(connector.updated_at.into_datetime())
        .execute(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        if result.rows_affected() != 1 {
            return Err(AppError::not_found("connector", connector.id.to_string()));
        }
        Ok(())
    }

    async fn get(&self, id: ConnectorId) -> Result<Option<Connector>> {
        let tenant = crate::scoped_request::current_scope()?.tenant_id();
        let mut scoped = crate::scoped_request::connection().await?;
        let row = sqlx::query(&format!(
            "SELECT {CONNECTOR_COLUMNS} FROM connectors WHERE tenant_id = $1 AND id = $2"
        ))
        .bind(tenant.into_uuid())
        .bind(id.into_uuid())
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(connector_from_row).transpose()
    }

    async fn list(&self, tenant: TenantId) -> Result<Vec<Connector>> {
        if crate::scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::not_found("tenant", tenant.to_string()));
        }
        let mut scoped = crate::scoped_request::connection().await?;
        let rows = sqlx::query(&format!(
            "SELECT {CONNECTOR_COLUMNS}
             FROM connectors WHERE tenant_id = $1 ORDER BY name ASC, id ASC LIMIT 1001"
        ))
        .bind(tenant.into_uuid())
        .fetch_all(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(connector_from_row).collect()
    }
}

/// Adapts the canonical application connector repository to the integration
/// registry seam. The current scoped request remains the authority: callers
/// cannot name a foreign tenant in the integration API and use it to escape
/// RLS/existence hiding.
#[async_trait::async_trait]
impl ConnectorRegistryPort for PostgresConnectorStore {
    async fn get(&self, tenant: TenantId, id: ConnectorId) -> Result<Option<Connector>> {
        if crate::scoped_request::current_scope()?.tenant_id() != tenant {
            return Ok(None);
        }
        <Self as ConnectorStorePort>::get(self, id).await
    }

    async fn list(&self, tenant: TenantId) -> Result<Vec<Connector>> {
        if crate::scoped_request::current_scope()?.tenant_id() != tenant {
            return Ok(Vec::new());
        }
        <Self as ConnectorStorePort>::list(self, tenant).await
    }

    async fn list_page(
        &self,
        tenant: TenantId,
        after: Option<ConnectorCursor>,
        limit: u16,
    ) -> Result<Vec<Connector>> {
        if crate::scoped_request::current_scope()?.tenant_id() != tenant {
            return Ok(Vec::new());
        }
        let limit = i64::from(limit.clamp(1, 201));
        let mut scoped = crate::scoped_request::connection().await?;
        let rows = sqlx::query(&format!(
            "SELECT {CONNECTOR_COLUMNS}
             FROM connectors
             WHERE tenant_id = $1
               AND (
                    $2::timestamptz IS NULL
                    OR created_at < $2
                    OR (created_at = $2 AND id < $3)
               )
             ORDER BY created_at DESC, id DESC
             LIMIT $4"
        ))
        .bind(tenant.into_uuid())
        .bind(after.map(|cursor| cursor.created_at.into_datetime()))
        .bind(after.map(|cursor| cursor.id.into_uuid()))
        .bind(limit)
        .fetch_all(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(connector_from_row).collect()
    }

    async fn save(&self, connector: &Connector) -> Result<()> {
        <Self as ConnectorStorePort>::save(self, connector).await
    }

    async fn delete(&self, tenant: &TenantId, id: ConnectorId) -> Result<bool> {
        if crate::scoped_request::current_scope()?.tenant_id() != *tenant {
            return Ok(false);
        }
        let mut scoped = crate::scoped_request::connection().await?;
        let result = sqlx::query(
            "DELETE FROM connectors
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant.into_uuid())
        .bind(id.into_uuid())
        .execute(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        Ok(result.rows_affected() == 1)
    }
}

fn connector_from_row(row: sqlx::postgres::PgRow) -> Result<Connector> {
    let config: Value = row.try_get("config").map_err(map_sqlx)?;
    let config: Map<String, Value> = config
        .as_object()
        .cloned()
        .ok_or_else(|| AppError::database("connector config must be a JSON object"))?;
    let connector = Connector {
        id: ConnectorId::from_uuid(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        tenant_id: TenantId::from_uuid(row.try_get::<Uuid, _>("tenant_id").map_err(map_sqlx)?),
        organization_id: mas_common::ids::OrganizationId::from_uuid(
            row.try_get::<Uuid, _>("organization_id")
                .map_err(map_sqlx)?,
        ),
        provider: row.try_get("provider").map_err(map_sqlx)?,
        name: row.try_get("name").map_err(map_sqlx)?,
        display_name: row.try_get("display_name").map_err(map_sqlx)?,
        auth_mode: parse_enum(
            row.try_get::<String, _>("auth_mode").map_err(map_sqlx)?,
            "connector auth mode",
        )?,
        status: parse_enum(
            row.try_get::<String, _>("status").map_err(map_sqlx)?,
            "connector status",
        )?,
        credential_id: row
            .try_get::<Option<Uuid>, _>("credential_id")
            .map_err(map_sqlx)?
            .map(mas_common::ids::CredentialId::from_uuid),
        config,
        last_tested_at: row
            .try_get::<Option<DateTime<Utc>>, _>("last_tested_at")
            .map_err(map_sqlx)?
            .map(Timestamp::from_datetime),
        last_error: row.try_get("last_error").map_err(map_sqlx)?,
        created_at: Timestamp::from_datetime(row.try_get("created_at").map_err(map_sqlx)?),
        updated_at: Timestamp::from_datetime(row.try_get("updated_at").map_err(map_sqlx)?),
    };
    validate_connector(&connector)
        .map_err(|_| AppError::database("connector row violates domain invariants"))?;
    Ok(connector)
}

fn parse_enum<T>(value: String, label: &str) -> Result<T>
where
    T: std::str::FromStr,
{
    value
        .parse()
        .map_err(|_| AppError::database(format!("stored {label} is invalid")))
}

fn validate_connector(connector: &Connector) -> Result<()> {
    if connector.id.is_nil()
        || connector.tenant_id.is_nil()
        || connector.organization_id.is_nil()
        || connector.credential_id.is_some_and(|id| id.is_nil())
    {
        return Err(AppError::invalid_field(
            "connector",
            "required",
            "connector scope and identifiers must be concrete",
        ));
    }
    mas_common::validation::validate_length("provider", &connector.provider, 1, 64)?;
    if !connector
        .provider
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-'))
    {
        return Err(AppError::invalid_field(
            "provider",
            "invalid_format",
            "provider keys may contain only ASCII letters, digits, dot, underscore, and dash",
        ));
    }
    mas_common::validation::validate_resource_name("name", &connector.name)?;
    if connector
        .display_name
        .as_deref()
        .is_some_and(|name| name.trim().is_empty() || name.len() > 200)
    {
        return Err(AppError::invalid_field(
            "display_name",
            "invalid_format",
            "display name must be non-empty and at most 200 bytes",
        ));
    }
    if connector.last_error.as_deref().is_some_and(|error| {
        error.is_empty() || error.len() > 1024 || error.chars().any(char::is_control)
    }) {
        return Err(AppError::invalid_field(
            "last_error",
            "invalid_format",
            "connector error must be a bounded printable summary",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::enums::ConnectorStatus;
    use mas_domain::ConnectorAuthMode;

    #[test]
    fn parser_rejects_legacy_statuses_after_parity_migration() {
        assert!(parse_enum::<ConnectorStatus>("degraded".to_owned(), "connector status").is_err());
        assert!(
            parse_enum::<ConnectorAuthMode>("oauth2".to_owned(), "connector auth mode").is_ok()
        );
    }
}
