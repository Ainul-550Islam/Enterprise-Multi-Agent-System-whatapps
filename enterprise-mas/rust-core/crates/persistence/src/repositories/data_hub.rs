//! RLS-scoped repositories for Rust-owned Data Hub metadata.
//!
//! Sources and documents are catalogue state owned by Rust. The Python RAG
//! plane gets neither these stores nor this pool; it receives only validated
//! snapshots through the gRPC bridge. Every query below runs through the
//! already-armed request transaction and carries an explicit tenant predicate
//! as defense in depth.

use chrono::{DateTime, Utc};
use mas_application::stores::{DataSourceStorePort, DocumentStorePort};
use mas_common::error::AppError;
use mas_common::ids::{DataSourceId, DocumentId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{DataSource, Document};
use serde_json::{Map, Value};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;

/// The application service returns at most 1,000 rows. Fetch one additional
/// row at most so a damaged or unusually dense catalogue cannot turn one list
/// request into an unbounded allocation before that service-level bound runs.
const MAX_CATALOGUE_ROWS: i64 = 1_001;

const DATA_SOURCE_COLUMNS: &str = "id, tenant_id, project_id, name, kind, status, config, \
    credential_id, created_by, sync_cursor, last_synced_at, last_failure_code, created_at, updated_at";
const DOCUMENT_COLUMNS: &str = "id, tenant_id, project_id, source_id, title, mime_type, \
    size_bytes, content_sha256, status, access_policy, metadata, created_by, chunk_count, \
    failure_code, created_at, updated_at";

/// PostgreSQL implementation of the Data Hub source metadata port.
#[derive(Debug, Clone)]
pub struct PostgresDataSourceStore {
    pool: PgPool,
}

impl PostgresDataSourceStore {
    /// Binds the store to the request-scoped application pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Diagnostic-only pool handle. Statements must use `scoped_request`.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// PostgreSQL implementation of the Data Hub document metadata port.
#[derive(Debug, Clone)]
pub struct PostgresDocumentStore {
    pool: PgPool,
}

impl PostgresDocumentStore {
    /// Binds the store to the request-scoped application pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Diagnostic-only pool handle. Statements must use `scoped_request`.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait::async_trait]
impl DataSourceStorePort for PostgresDataSourceStore {
    async fn save(&self, source: &DataSource) -> Result<()> {
        source.validate()?;
        ensure_tenant_scope(source.tenant_id)?;
        let mut scoped = crate::scoped_request::connection().await?;
        let result = sqlx::query(
            "INSERT INTO data_sources (
                id, tenant_id, project_id, name, kind, status, config,
                credential_id, created_by, sync_cursor, last_synced_at,
                last_failure_code, created_at, updated_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
             )
             ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                kind = EXCLUDED.kind,
                status = EXCLUDED.status,
                config = EXCLUDED.config,
                credential_id = EXCLUDED.credential_id,
                sync_cursor = EXCLUDED.sync_cursor,
                last_synced_at = EXCLUDED.last_synced_at,
                last_failure_code = EXCLUDED.last_failure_code,
                updated_at = EXCLUDED.updated_at
             WHERE data_sources.tenant_id = EXCLUDED.tenant_id",
        )
        .bind(source.id.into_uuid())
        .bind(source.tenant_id.into_uuid())
        .bind(source.project_id.into_uuid())
        .bind(&source.name)
        .bind(source.kind.to_string())
        .bind(source.status.to_string())
        .bind(&source.config)
        .bind(source.credential_id.map(|id| id.into_uuid()))
        .bind(source.created_by.into_uuid())
        .bind(&source.sync_cursor)
        .bind(
            source
                .last_synced_at
                .map(|timestamp| timestamp.into_datetime()),
        )
        .bind(&source.last_failure_code)
        .bind(source.created_at.into_datetime())
        .bind(source.updated_at.into_datetime())
        .execute(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        if result.rows_affected() != 1 {
            return Err(AppError::not_found("data source", source.id.to_string()));
        }
        Ok(())
    }

    async fn get(&self, id: DataSourceId) -> Result<Option<DataSource>> {
        let tenant = crate::scoped_request::current_scope()?.tenant_id();
        let mut scoped = crate::scoped_request::connection().await?;
        let row = sqlx::query(&format!(
            "SELECT {DATA_SOURCE_COLUMNS}
             FROM data_sources
             WHERE tenant_id = $1 AND id = $2"
        ))
        .bind(tenant.into_uuid())
        .bind(id.into_uuid())
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(data_source_from_row).transpose()
    }

    async fn list(
        &self,
        tenant: TenantId,
        project: mas_common::ids::ProjectId,
    ) -> Result<Vec<DataSource>> {
        ensure_tenant_scope(tenant)?;
        let mut scoped = crate::scoped_request::connection().await?;
        let rows = sqlx::query(&format!(
            "SELECT {DATA_SOURCE_COLUMNS}
             FROM data_sources
             WHERE tenant_id = $1 AND project_id = $2
             ORDER BY name ASC, id ASC
             LIMIT $3"
        ))
        .bind(tenant.into_uuid())
        .bind(project.into_uuid())
        .bind(MAX_CATALOGUE_ROWS)
        .fetch_all(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(data_source_from_row).collect()
    }
}

#[async_trait::async_trait]
impl DocumentStorePort for PostgresDocumentStore {
    async fn save(&self, document: &Document) -> Result<()> {
        document.validate()?;
        ensure_tenant_scope(document.tenant_id)?;
        let mut scoped = crate::scoped_request::connection().await?;
        let result = sqlx::query(
            "INSERT INTO documents (
                id, tenant_id, project_id, source_id, title, mime_type,
                size_bytes, content_sha256, status, access_policy, metadata,
                created_by, chunk_count, failure_code, created_at, updated_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
             )
             ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                mime_type = EXCLUDED.mime_type,
                size_bytes = EXCLUDED.size_bytes,
                content_sha256 = EXCLUDED.content_sha256,
                status = EXCLUDED.status,
                access_policy = EXCLUDED.access_policy,
                metadata = EXCLUDED.metadata,
                chunk_count = EXCLUDED.chunk_count,
                failure_code = EXCLUDED.failure_code,
                updated_at = EXCLUDED.updated_at
             WHERE documents.tenant_id = EXCLUDED.tenant_id",
        )
        .bind(document.id.into_uuid())
        .bind(document.tenant_id.into_uuid())
        .bind(document.project_id.into_uuid())
        .bind(document.source_id.into_uuid())
        .bind(&document.title)
        .bind(&document.mime_type)
        .bind(i64::try_from(document.size_bytes).map_err(|_| {
            AppError::invalid_field(
                "size_bytes",
                "out_of_range",
                "document size exceeds PostgreSQL BIGINT",
            )
        })?)
        .bind(&document.content_sha256)
        .bind(document.status.to_string())
        .bind(&document.access_policy)
        .bind(Value::Object(document.metadata.clone()))
        .bind(document.created_by.into_uuid())
        .bind(i32::try_from(document.chunk_count).map_err(|_| {
            AppError::invalid_field(
                "chunk_count",
                "out_of_range",
                "document chunk count exceeds PostgreSQL INTEGER",
            )
        })?)
        .bind(&document.failure_code)
        .bind(document.created_at.into_datetime())
        .bind(document.updated_at.into_datetime())
        .execute(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        if result.rows_affected() != 1 {
            return Err(AppError::not_found("document", document.id.to_string()));
        }
        Ok(())
    }

    async fn get(&self, id: DocumentId) -> Result<Option<Document>> {
        let tenant = crate::scoped_request::current_scope()?.tenant_id();
        let mut scoped = crate::scoped_request::connection().await?;
        let row = sqlx::query(&format!(
            "SELECT {DOCUMENT_COLUMNS}
             FROM documents
             WHERE tenant_id = $1 AND id = $2"
        ))
        .bind(tenant.into_uuid())
        .bind(id.into_uuid())
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(document_from_row).transpose()
    }

    async fn list(&self, tenant: TenantId, source: DataSourceId) -> Result<Vec<Document>> {
        ensure_tenant_scope(tenant)?;
        let mut scoped = crate::scoped_request::connection().await?;
        let rows = sqlx::query(&format!(
            "SELECT {DOCUMENT_COLUMNS}
             FROM documents
             WHERE tenant_id = $1 AND source_id = $2
             ORDER BY created_at DESC, id ASC
             LIMIT $3"
        ))
        .bind(tenant.into_uuid())
        .bind(source.into_uuid())
        .bind(MAX_CATALOGUE_ROWS)
        .fetch_all(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(document_from_row).collect()
    }
}

fn ensure_tenant_scope(tenant: TenantId) -> Result<()> {
    if crate::scoped_request::current_scope()?.tenant_id() != tenant {
        return Err(AppError::not_found("tenant", tenant.to_string()));
    }
    Ok(())
}

fn data_source_from_row(row: sqlx::postgres::PgRow) -> Result<DataSource> {
    let source = DataSource {
        id: DataSourceId::from_uuid(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        tenant_id: TenantId::from_uuid(row.try_get::<Uuid, _>("tenant_id").map_err(map_sqlx)?),
        project_id: mas_common::ids::ProjectId::from_uuid(
            row.try_get::<Uuid, _>("project_id").map_err(map_sqlx)?,
        ),
        name: row.try_get("name").map_err(map_sqlx)?,
        kind: parse_database_enum(
            row.try_get::<String, _>("kind").map_err(map_sqlx)?,
            "data source kind",
        )?,
        status: parse_database_enum(
            row.try_get::<String, _>("status").map_err(map_sqlx)?,
            "data source status",
        )?,
        config: row.try_get("config").map_err(map_sqlx)?,
        credential_id: row
            .try_get::<Option<Uuid>, _>("credential_id")
            .map_err(map_sqlx)?
            .map(mas_common::ids::CredentialId::from_uuid),
        created_by: mas_common::ids::UserId::from_uuid(
            row.try_get::<Uuid, _>("created_by").map_err(map_sqlx)?,
        ),
        sync_cursor: row.try_get("sync_cursor").map_err(map_sqlx)?,
        last_synced_at: row
            .try_get::<Option<DateTime<Utc>>, _>("last_synced_at")
            .map_err(map_sqlx)?
            .map(Timestamp::from_datetime),
        last_failure_code: row.try_get("last_failure_code").map_err(map_sqlx)?,
        created_at: Timestamp::from_datetime(row.try_get("created_at").map_err(map_sqlx)?),
        updated_at: Timestamp::from_datetime(row.try_get("updated_at").map_err(map_sqlx)?),
    };
    source
        .validate()
        .map_err(|_| AppError::database("data source row violates domain invariants"))?;
    Ok(source)
}

fn document_from_row(row: sqlx::postgres::PgRow) -> Result<Document> {
    let size_bytes: i64 = row.try_get("size_bytes").map_err(map_sqlx)?;
    let chunk_count: i32 = row.try_get("chunk_count").map_err(map_sqlx)?;
    let metadata: Value = row.try_get("metadata").map_err(map_sqlx)?;
    let metadata = metadata
        .as_object()
        .cloned()
        .ok_or_else(|| AppError::database("document metadata must be stored as a JSON object"))?;
    let document = Document {
        id: DocumentId::from_uuid(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        tenant_id: TenantId::from_uuid(row.try_get::<Uuid, _>("tenant_id").map_err(map_sqlx)?),
        project_id: mas_common::ids::ProjectId::from_uuid(
            row.try_get::<Uuid, _>("project_id").map_err(map_sqlx)?,
        ),
        source_id: DataSourceId::from_uuid(row.try_get::<Uuid, _>("source_id").map_err(map_sqlx)?),
        title: row.try_get("title").map_err(map_sqlx)?,
        mime_type: row.try_get("mime_type").map_err(map_sqlx)?,
        size_bytes: u64::try_from(size_bytes)
            .map_err(|_| AppError::database("document size_bytes is negative"))?,
        content_sha256: row.try_get("content_sha256").map_err(map_sqlx)?,
        status: parse_database_enum(
            row.try_get::<String, _>("status").map_err(map_sqlx)?,
            "document status",
        )?,
        chunk_count: u32::try_from(chunk_count)
            .map_err(|_| AppError::database("document chunk_count is negative"))?,
        failure_code: row.try_get("failure_code").map_err(map_sqlx)?,
        access_policy: row.try_get("access_policy").map_err(map_sqlx)?,
        metadata: Map::from_iter(metadata),
        created_by: mas_common::ids::UserId::from_uuid(
            row.try_get::<Uuid, _>("created_by").map_err(map_sqlx)?,
        ),
        created_at: Timestamp::from_datetime(row.try_get("created_at").map_err(map_sqlx)?),
        updated_at: Timestamp::from_datetime(row.try_get("updated_at").map_err(map_sqlx)?),
    };
    document
        .validate()
        .map_err(|_| AppError::database("document row violates domain invariants"))?;
    Ok(document)
}

fn parse_database_enum<T>(value: String, label: &str) -> Result<T>
where
    T: std::str::FromStr,
{
    value
        .parse()
        .map_err(|_| AppError::database(format!("stored {label} is invalid")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalogue_reads_are_bounded_before_application_projection() {
        assert_eq!(MAX_CATALOGUE_ROWS, 1_001);
    }
}
