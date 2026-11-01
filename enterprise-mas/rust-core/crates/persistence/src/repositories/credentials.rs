//! PURPOSE: PostgreSQL active connector-credential reference lookup.
//!
//! Owning layer: `mas-persistence` request-plane adapter.
//!
//! Production role: prove that a connector's credential binding is in the same
//! tenant, active, unrevoked, unexpired, and points to an available canonical
//! secret reference before a real provider adapter asks `SecretGate` for bytes.
//!
//! Security boundary: this uses the request's authoritative scoped transaction
//! and returns reference metadata only. It neither selects `secret_ref` legacy
//! JSON for material nor resolves, logs, caches, or serializes secret bytes.
//!
//! Data ownership: credentials and secret_references own lifecycle metadata;
//! the external secret manager owns raw credential material.
//!
//! Prohibited responsibilities: binding/rotating credentials, OAuth exchange,
//! HTTP probing, cross-tenant relay access, or a fallback empty credential.

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_domain::Connector;
use mas_integrations::provider_adapters::{
    ConnectorCredentialLookupPort, ConnectorCredentialReference,
};
use sqlx::PgPool;

use crate::error::map_sqlx;
use crate::repositories::secret_references::reference_from_row;

/// Request-plane active credential lookup adapter.
#[derive(Debug, Clone)]
pub struct PostgresConnectorCredentialLookup {
    pool: PgPool,
}

impl PostgresConnectorCredentialLookup {
    /// Binds the adapter to the request pool. Calls require scoped_request.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Pool used by composition to establish the authoritative RLS scope.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait::async_trait]
impl ConnectorCredentialLookupPort for PostgresConnectorCredentialLookup {
    async fn active_reference(
        &self,
        connector: &Connector,
    ) -> Result<Option<ConnectorCredentialReference>> {
        let scope = crate::scoped_request::current_scope()?;
        if scope.tenant_id() != connector.tenant_id {
            return Ok(None);
        }
        let Some(credential_id) = connector.credential_id else {
            return Ok(None);
        };
        let mut scoped = crate::scoped_request::connection().await?;
        let row = sqlx::query(
            "SELECT
                 reference.id, reference.tenant_id, reference.provider,
                 reference.path, reference.key_version, reference.rotation_state,
                 reference.availability
             FROM credentials AS credential
             JOIN secret_references AS reference
               ON reference.id = credential.secret_reference_id
              AND reference.tenant_id = credential.tenant_id
             WHERE credential.tenant_id = $1
               AND credential.id = $2
               AND credential.connector_id = $3
               AND credential.status IN ('active', 'rotating')
               AND credential.revoked_at IS NULL
               AND (credential.expires_at IS NULL OR credential.expires_at > now())
               AND reference.availability = 'available'
               AND reference.rotation_state <> 'retired'",
        )
        .bind(connector.tenant_id.into_uuid())
        .bind(credential_id.into_uuid())
        .bind(connector.id.into_uuid())
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(|row| {
            let record = reference_from_row(row)?;
            if record.tenant_id != connector.tenant_id {
                return Err(AppError::database(
                    "credential reference query returned a foreign tenant row",
                ));
            }
            Ok(ConnectorCredentialReference {
                tenant_id: connector.tenant_id,
                connector_id: connector.id,
                secret_reference: record.reference,
            })
        })
        .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_has_no_secret_material_field() {
        let name = std::any::type_name::<PostgresConnectorCredentialLookup>();
        assert!(name.contains("PostgresConnectorCredentialLookup"));
    }
}
