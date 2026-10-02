//! SQL error → platform error mapping.
//!
//! Rules:
//! * integrity violations map to domain-meaningful `AppError` variants so
//!   services can branch on them (unique → `Conflict`, FK → `Validation`, …);
//! * transient infrastructure failures map to retryable `Database` errors;
//! * **raw SQL, query text, and server detail never leak** into the message —
//!   only the constraint name (already part of our schema contract) is kept.

use mas_common::error::AppError;

/// Maps an arbitrary `sqlx::Error` to the platform error taxonomy.
#[must_use]
pub fn map_sqlx(error: sqlx::Error) -> AppError {
    match error {
        sqlx::Error::RowNotFound => AppError::not_found("record", "no row matched the query"),
        sqlx::Error::Database(db) => map_database(db.as_ref()),
        sqlx::Error::PoolTimedOut => {
            AppError::timeout("database pool acquisition timed out; the pool is saturated")
        },
        sqlx::Error::PoolClosed => AppError::database("database pool is closed"),
        sqlx::Error::Io(io) => {
            AppError::database(format!("database transport error ({})", io.kind()))
        },
        sqlx::Error::Tls(tls) => AppError::database("database TLS negotiation failed")
            .with_context(sanitize_one_line(&tls.to_string())),
        _ => AppError::database("database operation failed"),
    }
}

fn map_database(db: &dyn sqlx::error::DatabaseError) -> AppError {
    let code = db.code().map(|c| c.into_owned());
    match code.as_deref() {
        // unique_violation
        Some("23505") => AppError::conflict(format!(
            "a record already exists (constraint {})",
            db.constraint().unwrap_or("unknown")
        )),
        // foreign_key_violation
        Some("23503") => AppError::validation(format!(
            "referenced record does not exist (constraint {})",
            db.constraint().unwrap_or("unknown")
        )),
        // check_violation
        Some("23514") => AppError::invalid_field(
            db.constraint().unwrap_or("column"),
            "check_violation",
            "value violates a database check constraint",
        ),
        // not_null_violation
        Some("23502") => AppError::invalid_field(
            db.constraint().unwrap_or("column"),
            "required",
            "required column was NULL",
        ),
        // raise_exception (our append-only triggers, RLS guards)
        Some("P0001") => AppError::forbidden(sanitize_one_line(db.message())),
        // serialization_failure / deadlock_detected / lock_not_available:
        // retryable by definition of the SQLSTATE class 40xxx/55P03.
        Some("40001" | "40P01" | "55P03") => {
            AppError::database("transient database contention; retry the unit of work")
        },
        Some(other) => AppError::database(format!("database error (sqlstate {other})")),
        None => AppError::database("database error"),
    }
}

/// Keeps one safe line of a vendor message (no newlines, capped length) so
/// trigger-raised business messages survive without protocol noise.
fn sanitize_one_line(message: &str) -> String {
    let line: String = message.chars().take(200).collect();
    line.split(['\n', '\r'])
        .next()
        .unwrap_or_default()
        .to_owned()
}

/// Convenience for `Result<T, sqlx::Error>` → `Result<T>` (mas-common).
pub trait MapSqlx<T> {
    /// Maps the error channel with [`map_sqlx`].
    fn map_db(self) -> mas_common::result::Result<T>;
}

impl<T> MapSqlx<T> for Result<T, sqlx::Error> {
    fn map_db(self) -> mas_common::result::Result<T> {
        self.map_err(map_sqlx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_not_found_maps_to_not_found() {
        let mapped = map_sqlx(sqlx::Error::RowNotFound);
        assert_eq!(mapped.error_code(), "RESOURCE_NOT_FOUND");
    }

    #[test]
    fn transport_failures_are_retryable_database_errors() {
        let mapped = map_sqlx(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        )));
        assert_eq!(mapped.error_code(), "DATABASE_ERROR");
    }

    #[test]
    fn pool_timeouts_map_to_timeout() {
        let mapped = map_sqlx(sqlx::Error::PoolTimedOut);
        assert_eq!(mapped.error_code(), "TIMEOUT");
    }

    #[test]
    fn sanitizer_strips_newlines_and_caps_length() {
        assert_eq!(sanitize_one_line("line1\nSECRET-DETAIL"), "line1");
        let long = "x".repeat(1000);
        assert_eq!(sanitize_one_line(&long).len(), 200);
    }
}
