//! Pool configuration, construction, and health probing.
//!
//! The database URL is a credential: [`DatabaseUrl`] redacts itself in
//! `Debug`/`Display` and can only be consumed explicitly by the pool builder.
//! Platform rule: raw secrets never enter domain objects, logs, or errors.

use std::fmt;
use std::time::Duration;

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_contracts::health::DependencyHealth;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::str::FromStr;

use crate::error::map_sqlx;

/// The logical dependency name used in readiness responses.
pub const POSTGRES_DEPENDENCY_NAME: &str = "postgres";

/// A database connection string that redacts itself in every display path.
#[derive(Clone)]
pub struct DatabaseUrl(String);

impl DatabaseUrl {
    /// Wraps a URL after a structural sanity check (scheme + a host-ish part).
    /// We deliberately do **not** log or echo the value on failure.
    pub fn new(raw: impl Into<String>) -> Result<Self> {
        let raw = raw.into();
        let plausible = (raw.starts_with("postgres://") || raw.starts_with("postgresql://"))
            && raw.len() > "postgres://".len() + 3;
        if !plausible {
            return Err(AppError::validation(
                "database URL must use postgres:// or postgresql:// scheme",
            ));
        }
        Ok(Self(raw))
    }

    /// Explicit consumption (pool builder only).
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }

    /// A redacted rendering safe for logs and errors.
    #[must_use]
    pub fn redacted(&self) -> String {
        // scheme://<host>/<db> only — userinfo, params, and password removed.
        let without_query = self.0.split('?').next().unwrap_or_default();
        if let Some(rest) = without_query
            .strip_prefix("postgres://")
            .or_else(|| without_query.strip_prefix("postgresql://"))
        {
            let after_auth = rest.rsplit('@').next().unwrap_or(rest);
            return format!("postgres://{after_auth}");
        }
        "postgres://<redacted>".to_owned()
    }
}

impl fmt::Debug for DatabaseUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DatabaseUrl({})", self.redacted())
    }
}

impl fmt::Display for DatabaseUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.redacted())
    }
}

/// Pool sizing and lifecycle knobs, validated at construction.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Connection credential (redacted rendering).
    pub url: DatabaseUrl,
    /// Maximum connections (hard cap per process).
    pub max_connections: u32,
    /// Warm connections kept ready.
    pub min_connections: u32,
    /// Max wait for a free connection before `PoolTimedOut`.
    pub acquire_timeout: Duration,
    /// Idle connections are closed after this.
    pub idle_timeout: Duration,
    /// Connections are recycled after this regardless of idleness.
    pub max_lifetime: Duration,
    /// Visible in `pg_stat_activity` — invaluable in incident response.
    pub application_name: String,
}

impl PoolConfig {
    /// Sensible production defaults over a validated URL.
    pub fn for_url(url: DatabaseUrl, application_name: impl Into<String>) -> Result<Self> {
        let config = Self {
            url,
            max_connections: 20,
            min_connections: 2,
            acquire_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(600),
            max_lifetime: Duration::from_secs(1800),
            application_name: application_name.into(),
        };
        config.validated()
    }

    /// Validates internal consistency. Bounds exist because a single pod
    /// asking Postgres for thousands of connections is a client bug, not a
    /// deployment profile.
    pub fn validated(self) -> Result<Self> {
        if self.max_connections == 0 || self.max_connections > 512 {
            return Err(AppError::validation("max_connections must be in 1..=512"));
        }
        if self.min_connections > self.max_connections {
            return Err(AppError::validation(
                "min_connections must be <= max_connections",
            ));
        }
        if self.acquire_timeout.is_zero() || self.acquire_timeout > Duration::from_secs(120) {
            return Err(AppError::validation(
                "acquire_timeout must be in (0s..=120s]",
            ));
        }
        if self.application_name.is_empty() || self.application_name.len() > 64 {
            return Err(AppError::validation(
                "application_name must be 1..=64 characters",
            ));
        }
        Ok(self)
    }
}

/// Snapshot of pool utilization for observability endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolStats {
    /// Currently open connections (busy + idle).
    pub size: u32,
    /// Open connections not checked out.
    pub idle: usize,
    /// Configured maximum.
    pub max_connections: u32,
}

/// The shared database handle: a thin, instrumented wrapper over `PgPool`.
#[derive(Debug, Clone)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    /// Builds a pool from validated config (does not block on first connect;
    /// use [`Database::probe`] to verify reachability at startup).
    pub async fn connect(config: &PoolConfig) -> Result<Self> {
        let config = config.clone().validated()?;
        // Connect options carry the application_name; the URL string is
        // parsed once here and never logged (sqlx error chains may embed it,
        // so connection failures are wrapped below).
        let connect_options = PgConnectOptions::from_str(config.url.expose())
            .map_err(|_| AppError::validation("database URL cannot be parsed (format invalid)"))?
            .application_name(&config.application_name);
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(config.acquire_timeout)
            .idle_timeout(config.idle_timeout)
            .max_lifetime(config.max_lifetime)
            .connect_with(connect_options)
            .await
            .map_err(map_sqlx)
            .map_err(|e| {
                // Never propagate the URL-bearing sqlx message verbatim.
                AppError::database(format!(
                    "failed to connect to database at {}",
                    config.url.redacted()
                ))
                .with_context(e.error_code())
            })?;
        Ok(Self { pool })
    }

    /// The underlying pool (repositories borrow executors from here).
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Liveness probe (`SELECT 1`) for readiness reporting.
    pub async fn probe(&self) -> DependencyHealth {
        let start = std::time::Instant::now();
        match sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&self.pool)
            .await
        {
            Ok(1) => {
                let latency_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
                DependencyHealth::healthy(POSTGRES_DEPENDENCY_NAME, Some(latency_ms))
            },
            Ok(_) => {
                DependencyHealth::unhealthy(POSTGRES_DEPENDENCY_NAME, "unexpected probe result")
            },
            Err(_) => {
                DependencyHealth::unhealthy(POSTGRES_DEPENDENCY_NAME, "database is not reachable")
            },
        }
    }

    /// Utilization snapshot for metrics.
    #[must_use]
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            size: self.pool.size(),
            idle: self.pool.num_idle(),
            max_connections: self.pool.options().get_max_connections(),
        }
    }

    /// Graceful shutdown: stop accepting checkouts and drain.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_url_redacts_credentials() {
        let url = DatabaseUrl::new("postgres://app:s3cr3t@db.internal:5432/mas?sslmode=require")
            .expect("valid");
        let shown = url.to_string();
        assert_eq!(shown, "postgres://db.internal:5432/mas");
        assert!(!shown.contains("s3cr3t"));
        let debug = format!("{url:?}");
        assert!(!debug.contains("s3cr3t"));

        assert!(DatabaseUrl::new("mysql://x").is_err());
        assert!(DatabaseUrl::new("").is_err());
    }

    #[test]
    fn config_validation_bounds() {
        let url = DatabaseUrl::new("postgres://u:p@h:5432/d").expect("url");
        let base = PoolConfig::for_url(url.clone(), "mas-test").expect("defaults");
        assert_eq!(base.max_connections, 20);

        let mut bad = base.clone();
        bad.max_connections = 0;
        assert!(bad.validated().is_err());
        let mut bad = base.clone();
        bad.max_connections = 1000;
        assert!(bad.validated().is_err());
        let mut bad = base.clone();
        bad.min_connections = 21;
        assert!(bad.validated().is_err());
        let mut bad = base;
        bad.application_name = String::new();
        assert!(bad.validated().is_err());
    }
}
