//! PURPOSE: PostgreSQL implementation of `ConnectorDisablePort` — the single
//! durable transition behind `DELETE /v1/connectors/{id}`: disable the
//! connector, revoke its credentials, retire in-flight OAuth work, and enqueue
//! external secret cleanup, all as ONE request-scoped transaction.
//!
//! Owning layer: `mas-persistence`.
//! Production responsibility: mirror the in-memory port's domain semantics
//! (idempotent `Connector::disable`) and add the revocation side effects a
//! production deployment must never skip: credentials move to `revoked`,
//! half-open authorization flows are consumed, and a cleanup job is enqueued
//! for every materialized token secret so the cleanup worker performs the
//! idempotent secret-manager call. Token *refresh* work needs no touch — its
//! claim already requires `credential.status IN ('active','rotating')`, so a
//! revoked credential halts refreshing on the next claim cycle.
//! Data ownership: `connectors`, `credentials`,
//! `oauth_authorization_states`, `oauth_token_materialization_jobs`, and
//! `oauth_token_cleanup_jobs` are authoritative here; every statement binds
//! `tenant_id` and runs through the request-scoped connection (forced RLS).
//! MUST NOT: invent a second disable path beside the domain aggregate, revoke
//! credentials outside the tenant's own rows, mark cleanup rows terminal (only
//! the cleanup worker changes secret availability), or bypass the store's
//! scope validation with raw pool queries.

use async_trait::async_trait;
use mas_application::connector_service::ConnectorDisablePort;
use mas_application::stores::ConnectorStorePort;
use mas_common::{
    error::AppError,
    ids::{ConnectorId, TenantId},
    result::Result,
};
use mas_domain::Connector;
use sqlx::PgPool;

use crate::repositories::connectors::PostgresConnectorStore;
use crate::{error::map_sqlx, scoped_request};

/// Revocation predicate shared by the house OAuth paths: only a live
/// credential moves to `revoked`, and only once (`revoked_at IS NULL`), so a
/// repeated admin disable cannot rewrite history or trip the partial-index
/// invariants of the reference lifecycle.
const REVOKE_CREDENTIALS_SQL: &str = "UPDATE credentials
        SET status = 'revoked', revoked_at = now(), updated_at = now()
      WHERE tenant_id = $1
        AND connector_id = $2
        AND status IN ('active', 'rotating')
        AND revoked_at IS NULL";

/// Enqueues external-material cleanup for every materialized token secret of
/// this connector. `ON CONFLICT (credential_id) DO NOTHING` makes the enqueue
/// idempotent across repeated disables; the cleanup worker alone performs the
/// secret-manager call and flips reference availability afterwards.
const ENQUEUE_CLEANUP_SQL: &str = "INSERT INTO oauth_token_cleanup_jobs (
             credential_id, tenant_id, connector_id, secret_reference_id,
             state, attempts, next_attempt_at, created_at, updated_at
         )
         SELECT m.credential_id, m.tenant_id, m.connector_id, m.secret_reference_id,
                'pending', 0, now(), now(), now()
           FROM oauth_token_materialization_jobs AS m
           JOIN credentials AS c
             ON c.id = m.credential_id AND c.tenant_id = m.tenant_id
          WHERE m.tenant_id = $1
            AND m.connector_id = $2
            AND m.credential_id IS NOT NULL
         ON CONFLICT (credential_id) DO NOTHING";

/// Consumes every unfinished authorization flow bound to the connector so a
/// disabled connector cannot complete an OAuth dance started before the
/// disable. Already-consumed rows are untouched (`consumed_at IS NULL`).
const CONSUME_AUTH_STATES_SQL: &str = "UPDATE oauth_authorization_states
        SET consumed_at = now()
      WHERE tenant_id = $1
        AND connector_id = $2
        AND consumed_at IS NULL";

/// Durable connector disable with credential/OAuth revocation.
#[derive(Debug, Clone)]
pub struct PostgresConnectorLifecycle {
    pool: PgPool,
}

impl PostgresConnectorLifecycle {
    /// Binds the lifecycle port to the shared request pool (also used for the
    /// catalogue store so both halves observe the same scoped transaction).
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ConnectorDisablePort for PostgresConnectorLifecycle {
    async fn disable_and_revoke(&self, tenant: TenantId, id: ConnectorId) -> Result<Connector> {
        let store = PostgresConnectorStore::new(self.pool.clone());

        // 1) Domain transition through the catalogue store: same idempotence,
        //    scope validation, and `updated_at` handling as the in-memory
        //    port — the raw statements below only add revocation side effects
        //    around this canonical write.
        let mut connector = store
            .get(id)
            .await?
            .filter(|connector| connector.tenant_id == tenant)
            .ok_or_else(|| AppError::not_found("connector", id.to_string()))?;
        connector.disable()?;
        store.save(&connector).await?;

        // 2) Revoke live credentials bound to this connector (all purposes:
        //    an API-key style credential must stop working when the connector
        //    is disabled, not only OAuth tokens).
        let mut scoped = scoped_request::connection().await?;
        let revoked = sqlx::query(REVOKE_CREDENTIALS_SQL)
            .bind(tenant.into_uuid())
            .bind(id.into_uuid())
            .execute(scoped.connection().as_mut())
            .await
            .map_err(map_sqlx)?;
        // 0 rows is the idempotent case (already revoked); more rows than the
        // tenant owns cannot happen under the bound + RLS, but a positive
        // sanity bound still fails closed against a predicate regression.
        if revoked.rows_affected() > 64 {
            return Err(AppError::database(
                "connector credential revocation affected an implausible row count",
            ));
        }

        // 3) Enqueue secret cleanup for materialized tokens (idempotent).
        sqlx::query(ENQUEUE_CLEANUP_SQL)
            .bind(tenant.into_uuid())
            .bind(id.into_uuid())
            .execute(scoped.connection().as_mut())
            .await
            .map_err(map_sqlx)?;

        // 4) Close half-open authorization flows so no in-flight state can be
        //    exchanged after the disable.
        sqlx::query(CONSUME_AUTH_STATES_SQL)
            .bind(tenant.into_uuid())
            .bind(id.into_uuid())
            .execute(scoped.connection().as_mut())
            .await
            .map_err(map_sqlx)?;

        Ok(connector)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tripwires against rewriting the SQL constants into unscoped or
    /// widening statements: every predicate the port depends on must stay in
    /// place, because the unit tests below cannot execute PostgreSQL.
    #[test]
    fn revocation_statements_stay_tenant_bound_and_idempotent() {
        assert!(REVOKE_CREDENTIALS_SQL.contains("tenant_id = $1"));
        assert!(REVOKE_CREDENTIALS_SQL.contains("connector_id = $2"));
        assert!(REVOKE_CREDENTIALS_SQL.contains("status IN ('active', 'rotating')"));
        assert!(REVOKE_CREDENTIALS_SQL.contains("revoked_at IS NULL"));

        assert!(ENQUEUE_CLEANUP_SQL.contains("ON CONFLICT (credential_id) DO NOTHING"));
        assert!(ENQUEUE_CLEANUP_SQL.contains("tenant_id = $1"));
        assert!(ENQUEUE_CLEANUP_SQL.contains("connector_id = $2"));
        assert!(ENQUEUE_CLEANUP_SQL.contains("'pending'"));

        assert!(CONSUME_AUTH_STATES_SQL.contains("tenant_id = $1"));
        assert!(CONSUME_AUTH_STATES_SQL.contains("connector_id = $2"));
        assert!(CONSUME_AUTH_STATES_SQL.contains("consumed_at IS NULL"));
    }
}
