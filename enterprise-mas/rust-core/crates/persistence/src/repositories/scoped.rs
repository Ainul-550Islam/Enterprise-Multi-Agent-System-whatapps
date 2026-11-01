//! RLS-scoped store plumbing for tenant-owned JSON-document tables.
//!
//! Many aggregates (agents, workflows, tools, connectors, …) persist as a
//! hybrid: materialized identity/status columns plus a `spec`/`config`/`data`
//! JSONB payload carrying the full aggregate. [`ScopedStore`] centralizes the
//! two dangerous parts of that pattern:
//!
//! * table names are a **closed whitelist** ([`DocumentTable`]) — callers can
//!   never smuggle a dynamic table name into SQL;
//! * every operation runs against a connection that already had
//!   [`crate::rls::RlsContext`] applied, so cross-tenant reads fail closed at
//!   the database regardless of where the query was built.

use mas_common::error::AppError;
use mas_common::result::Result;
use serde_json::Value;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;
use crate::rls::RlsContext;
use crate::transaction::UnitOfWork;

/// The closed whitelist of tenant-owned document tables addressable through
/// the generic document helpers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentTable {
    /// `agents` (`spec` payload).
    Agents,
    /// `agent_versions` (`spec` payload, immutable).
    AgentVersions,
    /// `workflows` (`spec` payload).
    Workflows,
    /// `tools` (`spec` payload).
    Tools,
    /// `connectors` (`config` payload).
    Connectors,
    /// `policies` (`spec` payload).
    Policies,
    /// `schedules` (target/rule columns handled separately; `id` fetch only).
    Schedules,
}

impl DocumentTable {
    /// The physical table name (compile-time constant → no SQL injection
    /// surface through the generic path).
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Agents => "agents",
            Self::AgentVersions => "agent_versions",
            Self::Workflows => "workflows",
            Self::Tools => "tools",
            Self::Connectors => "connectors",
            Self::Policies => "policies",
            Self::Schedules => "schedules",
        }
    }

    /// The JSONB payload column for the document shape.
    #[must_use]
    pub fn payload_column(self) -> &'static str {
        match self {
            Self::Agents | Self::AgentVersions | Self::Workflows | Self::Tools | Self::Policies => {
                "spec"
            },
            Self::Connectors => "config",
            Self::Schedules => "target",
        }
    }

    /// All whitelisted tables (tests, inventory).
    #[must_use]
    pub fn all() -> &'static [DocumentTable] {
        &[
            Self::Agents,
            Self::AgentVersions,
            Self::Workflows,
            Self::Tools,
            Self::Connectors,
            Self::Policies,
            Self::Schedules,
        ]
    }
}

/// Store entry point for RLS-scoped operations.
#[derive(Debug, Clone)]
pub struct ScopedStore {
    pool: PgPool,
}

impl ScopedStore {
    /// Binds the store to a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Opens a transaction with the RLS context applied — the canonical way
    /// to reach tenant-owned tables.
    pub async fn scoped_tx<'c>(&self, ctx: &RlsContext) -> Result<UnitOfWork<'c>> {
        let mut uow = UnitOfWork::begin(&self.pool).await?;
        ctx.apply(uow.executor()).await?;
        Ok(uow)
    }

    /// Fetches one JSONB document payload by id (RLS confines the lookup).
    pub async fn fetch_document(
        &self,
        ctx: &RlsContext,
        table: DocumentTable,
        id: Uuid,
    ) -> Result<Option<Value>> {
        // Table/column names come from the whitelist — string composition is
        // safe here by construction.
        let sql = format!(
            "SELECT {} FROM {} WHERE id = $1",
            table.payload_column(),
            table.name()
        );
        let mut uow = self.scoped_tx(ctx).await?;
        let row = sqlx::query(&sql)
            .bind(id)
            .fetch_optional(uow.executor())
            .await
            .map_err(map_sqlx)?;
        uow.commit().await?;
        match row {
            Some(row) => Ok(Some(row.try_get(0).map_err(map_sqlx)?)),
            None => Ok(None),
        }
    }

    /// Inserts the `metadata` + `payload` columns of a document row inside an
    /// existing scoped transaction. ON CONFLICT DO NOTHING semantics return
    /// `false` on duplicates.
    pub async fn insert_document_in(
        executor: &mut PgConnection,
        table: DocumentTable,
        id: Uuid,
        tenant_id: Uuid,
        payload: &Value,
        extra_columns: &[(String, String)],
    ) -> Result<bool> {
        if table == DocumentTable::Schedules {
            return Err(AppError::validation(
                "schedules require rule/zone columns; use the schedule repository",
            ));
        }
        if extra_columns.len() > 8 {
            return Err(AppError::validation(
                "document inserts accept at most 8 extra columns",
            ));
        }
        let mut columns = vec![
            "id".to_owned(),
            "tenant_id".to_owned(),
            table.payload_column().to_owned(),
        ];
        let mut placeholders = vec!["$1".to_owned(), "$2".to_owned(), "$3".to_owned()];
        for (index, (name, _)) in extra_columns.iter().enumerate() {
            Self::require_ident(name)?;
            columns.push(name.clone());
            placeholders.push(format!("${}", index + 4));
        }
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (id) DO NOTHING",
            table.name(),
            columns.join(", "),
            placeholders.join(", ")
        );
        let mut query = sqlx::query(&sql).bind(id).bind(tenant_id).bind(payload);
        for (_, value) in extra_columns {
            query = query.bind(value);
        }
        let result = query.execute(executor).await.map_err(map_sqlx)?;
        Ok(result.rows_affected() == 1)
    }

    /// Whitelist validation for dynamic column names (identifiers only).
    fn require_ident(name: &str) -> Result<()> {
        let ok = !name.is_empty()
            && name.len() <= 63
            && name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
            && name.chars().next().is_some_and(|c| c.is_ascii_lowercase());
        if !ok {
            return Err(AppError::validation(format!(
                "invalid column identifier {name:?}"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_tables_are_a_closed_whitelist() {
        // Table names must exist in the migration set and agree on payloads.
        let names: Vec<&str> = DocumentTable::all().iter().map(|t| t.name()).collect();
        assert_eq!(
            names,
            [
                "agents",
                "agent_versions",
                "workflows",
                "tools",
                "connectors",
                "policies",
                "schedules"
            ]
        );
        assert_eq!(DocumentTable::Agents.payload_column(), "spec");
        assert_eq!(DocumentTable::Connectors.payload_column(), "config");
        // No duplicates — a duplicated entry could shadow a schema change.
        let mut seen = std::collections::BTreeSet::new();
        for name in names {
            assert!(seen.insert(name), "duplicate whitelist entry {name}");
        }
    }

    #[test]
    fn identifier_whitelist_is_strict() {
        for good in ["name", "status_v2", "executor_config"] {
            ScopedStore::require_ident(good).expect("valid ident");
        }
        for bad in [
            "",
            "Name",
            "with space",
            "with-dash",
            "x\"; DROP TABLE users;--",
            "toolongidentifierthatexceedspostgreslimitof63charsaaaaaaaaaaaaaaaaaaaa",
        ] {
            assert!(ScopedStore::require_ident(bad).is_err(), "{bad:?} rejected");
        }
    }
}
