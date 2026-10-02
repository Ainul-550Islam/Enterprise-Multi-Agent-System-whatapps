//! Idempotency keys and the store preventing duplicate task/execution
//! creation.
//!
//! Flow: `reserve` (atomically claims the key) → work → `complete` (stores
//! the resulting reference) or `release` (on failure, frees the key for a
//! genuine retry). A second `reserve` on a completed key replays the stored
//! result; on a still-reserved key it reports an in-flight duplicate.

use mas_common::constants;
use mas_common::error::AppError;
use mas_common::ids::TenantId;
use mas_common::result::Result;
use mas_common::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;
use std::time::Duration;

/// 1..=128-char caller-supplied dedup key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        mas_common::validation::validate_non_empty("idempotency_key", &value)?;
        mas_common::validation::validate_length(
            "idempotency_key",
            &value,
            1,
            constants::MAX_IDEMPOTENCY_KEY_LENGTH,
        )?;
        // Printable, no control characters; whitespace allowed inside.
        if value.chars().any(char::is_control) {
            return Err(AppError::invalid_field(
                "idempotency_key",
                "invalid_format",
                "key must not contain control characters",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for IdempotencyKey {
    type Error = AppError;

    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}

impl From<IdempotencyKey> for String {
    fn from(key: IdempotencyKey) -> Self {
        key.0
    }
}

/// Lifecycle of a stored key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum IdempotencyRecord {
    /// Key claimed; work in progress. Re-submission is a conflict.
    Reserved { reserved_until: Timestamp },
    /// Work finished; carries the reference of the result
    /// (e.g. `task:<uuid>` / `execution:<uuid>`) to replay.
    Completed { result_ref: String },
}

/// Outcome of [`IdempotencyStore::reserve`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Reservation {
    /// Key acquired — proceed with the work.
    Acquired,
    /// Work already completed — replay `result_ref` instead of re-working.
    ReplayCompleted { result_ref: String },
    /// Work in flight for this key — caller must not duplicate it.
    ConflictInFlight,
}

/// Durable idempotency store (production impl: PostgreSQL; dev/test: memory).
#[async_trait::async_trait]
pub trait IdempotencyStore: Send + Sync + fmt::Debug {
    /// Atomically claims `(tenant, key)` for `ttl`.
    async fn reserve(
        &self,
        tenant_id: TenantId,
        key: &IdempotencyKey,
        ttl: Duration,
    ) -> Result<Reservation>;

    /// Reads the current record for `(tenant, key)`.
    async fn get_existing(
        &self,
        tenant_id: TenantId,
        key: &IdempotencyKey,
    ) -> Result<Option<IdempotencyRecord>>;

    /// Marks the key completed with its result reference.
    async fn complete(
        &self,
        tenant_id: TenantId,
        key: &IdempotencyKey,
        result_ref: &str,
    ) -> Result<()>;

    /// Frees a reservation after a failed attempt (allows a later retry).
    async fn release(&self, tenant_id: TenantId, key: &IdempotencyKey) -> Result<()>;
}

#[derive(Debug)]
struct MemoryEntry {
    record: IdempotencyRecord,
}

/// Process-local store for development and tests.
///
/// Semantics mirror the PostgreSQL implementation: reservations expire; a
/// completed record never expires implicitly (cleanup is a maintenance job).
#[derive(Debug, Default)]
pub struct InMemoryIdempotencyStore {
    entries: Mutex<HashMap<(TenantId, String), MemoryEntry>>,
}

impl InMemoryIdempotencyStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `true` when no idempotency records are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.entries.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

#[async_trait::async_trait]
impl IdempotencyStore for InMemoryIdempotencyStore {
    async fn reserve(
        &self,
        tenant_id: TenantId,
        key: &IdempotencyKey,
        ttl: Duration,
    ) -> Result<Reservation> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let now = Timestamp::now();
        match entries.get(&(tenant_id, key.as_str().to_owned())) {
            Some(entry) => match &entry.record {
                IdempotencyRecord::Completed { result_ref } => Ok(Reservation::ReplayCompleted {
                    result_ref: result_ref.clone(),
                }),
                IdempotencyRecord::Reserved { reserved_until } => {
                    if now.is_before(reserved_until) {
                        Ok(Reservation::ConflictInFlight)
                    } else {
                        // Expired reservation: takeover.
                        let reserved_until = now
                            .checked_add(ttl)
                            .ok_or_else(|| AppError::internal("reservation expiry overflow"))?;
                        entries.insert(
                            (tenant_id, key.as_str().to_owned()),
                            MemoryEntry {
                                record: IdempotencyRecord::Reserved { reserved_until },
                            },
                        );
                        Ok(Reservation::Acquired)
                    }
                },
            },
            None => {
                let reserved_until = now
                    .checked_add(ttl)
                    .ok_or_else(|| AppError::internal("reservation expiry overflow"))?;
                entries.insert(
                    (tenant_id, key.as_str().to_owned()),
                    MemoryEntry {
                        record: IdempotencyRecord::Reserved { reserved_until },
                    },
                );
                Ok(Reservation::Acquired)
            },
        }
    }

    async fn get_existing(
        &self,
        tenant_id: TenantId,
        key: &IdempotencyKey,
    ) -> Result<Option<IdempotencyRecord>> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        Ok(entries
            .get(&(tenant_id, key.as_str().to_owned()))
            .map(|entry| entry.record.clone()))
    }

    async fn complete(
        &self,
        tenant_id: TenantId,
        key: &IdempotencyKey,
        result_ref: &str,
    ) -> Result<()> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        match entries.get_mut(&(tenant_id, key.as_str().to_owned())) {
            Some(entry) => {
                entry.record = IdempotencyRecord::Completed {
                    result_ref: result_ref.to_owned(),
                };
                Ok(())
            },
            None => Err(AppError::conflict(format!(
                "cannot complete idempotency key '{key}': no active reservation"
            ))),
        }
    }

    async fn release(&self, tenant_id: TenantId, key: &IdempotencyKey) -> Result<()> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        // Never delete a completed record; only reservations are released.
        let should_remove = matches!(
            entries.get(&(tenant_id, key.as_str().to_owned())),
            Some(MemoryEntry {
                record: IdempotencyRecord::Reserved { .. }
            })
        );
        if should_remove {
            entries.remove(&(tenant_id, key.as_str().to_owned()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant() -> TenantId {
        TenantId::new()
    }

    #[tokio::test]
    async fn reserve_complete_replay_flow() {
        let store = InMemoryIdempotencyStore::new();
        let tenant = tenant();
        let key = IdempotencyKey::new("order-42").unwrap();

        // First reservation wins.
        assert_eq!(
            store
                .reserve(tenant, &key, Duration::from_secs(60))
                .await
                .unwrap(),
            Reservation::Acquired
        );
        // Second is an in-flight conflict.
        assert_eq!(
            store
                .reserve(tenant, &key, Duration::from_secs(60))
                .await
                .unwrap(),
            Reservation::ConflictInFlight
        );
        // Complete, then replays hit the stored result.
        store.complete(tenant, &key, "task:123").await.unwrap();
        assert_eq!(
            store
                .reserve(tenant, &key, Duration::from_secs(60))
                .await
                .unwrap(),
            Reservation::ReplayCompleted {
                result_ref: "task:123".to_owned()
            }
        );
        // Release does not delete completed records.
        store.release(tenant, &key).await.unwrap();
        assert!(store.get_existing(tenant, &key).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn release_allows_retry_after_failure() {
        let store = InMemoryIdempotencyStore::new();
        let tenant = tenant();
        let key = IdempotencyKey::new("flaky-1").unwrap();
        store
            .reserve(tenant, &key, Duration::from_secs(60))
            .await
            .unwrap();
        store.release(tenant, &key).await.unwrap();
        assert_eq!(
            store
                .reserve(tenant, &key, Duration::from_secs(60))
                .await
                .unwrap(),
            Reservation::Acquired
        );
        assert!(store.complete(tenant, &key, "task:9").await.is_ok());
    }

    #[tokio::test]
    async fn expired_reservation_can_be_taken_over() {
        let store = InMemoryIdempotencyStore::new();
        let tenant = tenant();
        let key = IdempotencyKey::new("ttl-1").unwrap();
        store
            .reserve(tenant, &key, Duration::from_millis(1))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(
            store
                .reserve(tenant, &key, Duration::from_secs(1))
                .await
                .unwrap(),
            Reservation::Acquired
        );
    }

    #[tokio::test]
    async fn tenancy_isolation() {
        let store = InMemoryIdempotencyStore::new();
        let key = IdempotencyKey::new("shared-key").unwrap();
        let t1 = tenant();
        let t2 = tenant();
        store
            .reserve(t1, &key, Duration::from_secs(60))
            .await
            .unwrap();
        // Same key under another tenant is independent.
        assert_eq!(
            store
                .reserve(t2, &key, Duration::from_secs(60))
                .await
                .unwrap(),
            Reservation::Acquired
        );
        assert!(store.complete(t1, &key, "task:x").await.is_ok());
        // Completing an unknown (t2) key conflicts… wait, t2 reserved above:
        assert!(store.complete(t2, &key, "task:y").await.is_ok());
    }
}
