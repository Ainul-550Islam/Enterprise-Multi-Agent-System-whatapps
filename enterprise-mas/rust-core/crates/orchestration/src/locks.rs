//! Distributed lock abstraction with fencing tokens.
//!
//! Used for execution-level and scheduler-level coordination (e.g. exactly
//! one scheduler instance fires a schedule). The spike is backed by
//! `acquire`/`try_acquire`/`renew`/`release`, and every guard carries a
//! monotonic fencing token so callers can reject stale holders.

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

/// Errors specific to lock coordination (mapped into `AppError` where useful).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockError {
    /// Lock is currently held by another guard.
    Contention,
    /// Guard token no longer matches (expired/taken over).
    LostOwnership,
}

impl fmt::Display for LockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contention => f.write_str("lock is held by another guard"),
            Self::LostOwnership => f.write_str("lock ownership was lost"),
        }
    }
}

impl std::error::Error for LockError {}

impl From<LockError> for AppError {
    fn from(err: LockError) -> Self {
        match err {
            LockError::Contention => AppError::conflict(err.to_string()),
            LockError::LostOwnership => AppError::conflict(err.to_string()),
        }
    }
}

/// An acquired lock. Guards are *not* released implicitly by dropping (a
/// dropped guard must not unlock a taken-over lock); call
/// [`DistributedLock::release`] or let the TTL expire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockGuard {
    /// Locked resource key.
    pub key: String,
    /// Unique token identifying this acquisition (fencing is `u64`, growing).
    pub token: String,
    /// Monotonic fencing number for consumers (e.g. DB compare-and-write).
    pub fencing: u64,
    /// When the lock expires unless renewed.
    pub expires_at: Timestamp,
    pub acquired_at: Timestamp,
}

impl LockGuard {
    /// Whether the guard is (locally) within its TTL.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.expires_at.is_future()
    }

    /// Remaining TTL.
    #[must_use]
    pub fn remaining(&self) -> Option<Duration> {
        self.expires_at.duration_since(&Timestamp::now())
    }
}

/// A distributed lock provider (production impl: Redis/PostgreSQL advisory).
#[async_trait::async_trait]
pub trait DistributedLock: Send + Sync + fmt::Debug {
    /// Acquires `key`, waiting up to `wait` for contention to clear.
    async fn acquire(&self, key: &str, ttl: Duration, wait: Duration) -> Result<LockGuard>;

    /// Attempts immediate acquisition; `Ok(None)` on contention.
    async fn try_acquire(&self, key: &str, ttl: Duration) -> Result<Option<LockGuard>>;

    /// Extends the guard's TTL. Fails with `CONFLICT` when ownership was lost.
    async fn renew(&self, guard: &LockGuard, ttl: Duration) -> Result<LockGuard>;

    /// Releases the lock if (and only if) `guard` still owns it.
    async fn release(&self, guard: &LockGuard) -> Result<()>;
}

#[derive(Debug)]
struct MemoryLockEntry {
    token: String,
    fencing: u64,
    expires_at: Timestamp,
}

/// Single-process lock provider (dev/tests/single-replica deployments).
///
/// Honors the same contract as the Redis implementation, including fencing
/// and takeover-on-expiry.
#[derive(Debug)]
pub struct InMemoryDistributedLock {
    entries: Mutex<HashMap<String, MemoryLockEntry>>,
    fencing_source: AtomicU64,
}

impl Default for InMemoryDistributedLock {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryDistributedLock {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            fencing_source: AtomicU64::new(1),
        }
    }

    fn next_fencing(&self) -> u64 {
        self.fencing_source.fetch_add(1, Ordering::SeqCst)
    }

    /// Forces expiry of all locks (test/maintenance helper).
    #[must_use]
    pub fn held_locks(&self) -> usize {
        let now = Timestamp::now();
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|entry| entry.expires_at.is_after(&now))
            .count()
    }

    fn insert_new_guard(&self, key: &str, ttl: Duration) -> Result<LockGuard> {
        let fencing = self.next_fencing();
        let now = Timestamp::now();
        let expires_at = now
            .checked_add(ttl)
            .ok_or_else(|| AppError::invalid_field("ttl", "out_of_range", "TTL overflows"))?;
        let token = format!("memlock:{fencing}:{}", uuid::Uuid::now_v7());
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                key.to_owned(),
                MemoryLockEntry {
                    token: token.clone(),
                    fencing,
                    expires_at,
                },
            );
        Ok(LockGuard {
            key: key.to_owned(),
            token,
            fencing,
            expires_at,
            acquired_at: now,
        })
    }

    fn try_once(&self, key: &str, ttl: Duration) -> Result<Option<LockGuard>> {
        mas_common::validation::validate_non_empty("key", key)?;
        if ttl.is_zero() {
            return Err(AppError::invalid_field(
                "ttl",
                "out_of_range",
                "lock TTL must be positive",
            ));
        }
        {
            let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
            let now = Timestamp::now();
            if let Some(entry) = entries.get(key) {
                if entry.expires_at.is_after(&now) {
                    return Ok(None); // live lock: contention
                }
            }
        }
        Ok(Some(self.insert_new_guard(key, ttl)?))
    }
}

#[async_trait::async_trait]
impl DistributedLock for InMemoryDistributedLock {
    async fn acquire(&self, key: &str, ttl: Duration, wait: Duration) -> Result<LockGuard> {
        let give_up = Timestamp::now()
            .checked_add(wait)
            .ok_or_else(|| AppError::internal("wait window overflow"))?;
        let mut backoff = Duration::from_millis(5);
        loop {
            match self.try_once(key, ttl)? {
                Some(guard) => return Ok(guard),
                None => {
                    if !give_up.is_future() {
                        return Err(LockError::Contention.into());
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_millis(100));
                },
            }
        }
    }

    async fn try_acquire(&self, key: &str, ttl: Duration) -> Result<Option<LockGuard>> {
        self.try_once(key, ttl)
    }

    async fn renew(&self, guard: &LockGuard, ttl: Duration) -> Result<LockGuard> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let now = Timestamp::now();
        match entries.get_mut(&guard.key) {
            Some(entry) if entry.token == guard.token && entry.expires_at.is_after(&now) => {
                let expires_at = now.checked_add(ttl).ok_or_else(|| {
                    AppError::invalid_field("ttl", "out_of_range", "TTL overflows")
                })?;
                entry.expires_at = expires_at;
                Ok(LockGuard {
                    key: guard.key.clone(),
                    token: guard.token.clone(),
                    fencing: entry.fencing, // fencing is stable across renewals
                    expires_at,
                    acquired_at: guard.acquired_at,
                })
            },
            _ => Err(LockError::LostOwnership.into()),
        }
    }

    async fn release(&self, guard: &LockGuard) -> Result<()> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let owns = matches!(
            entries.get(&guard.key),
            Some(entry) if entry.token == guard.token
        );
        if owns {
            entries.remove(&guard.key);
        }
        // Releasing an unowned lock is a no-op (never unlock someone else's).
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn acquire_release_cycle() {
        let lock = InMemoryDistributedLock::new();
        let guard = lock
            .try_acquire("schedule:42", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("first acquisition wins");
        assert_eq!(guard.fencing, 1);
        assert!(guard.is_valid());
        assert_eq!(lock.held_locks(), 1);
        lock.release(&guard).await.unwrap();
        assert_eq!(lock.held_locks(), 0);
    }

    #[tokio::test]
    async fn contention_and_expiry_takeover() {
        let lock = InMemoryDistributedLock::new();
        let first = lock
            .try_acquire("exec:1", Duration::from_millis(10))
            .await
            .unwrap()
            .unwrap();
        // Contention while held.
        assert!(lock
            .try_acquire("exec:1", Duration::from_secs(1))
            .await
            .unwrap()
            .is_none());
        // After TTL expiry another guard takes over with a higher fence.
        tokio::time::sleep(Duration::from_millis(15)).await;
        let second = lock
            .try_acquire("exec:1", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert!(second.fencing > first.fencing);
    }

    #[tokio::test]
    async fn renew_requires_ownership() {
        let lock = InMemoryDistributedLock::new();
        let guard = lock
            .try_acquire("exec:2", Duration::from_millis(50))
            .await
            .unwrap()
            .unwrap();
        let renewed = lock.renew(&guard, Duration::from_secs(60)).await.unwrap();
        assert_eq!(renewed.fencing, guard.fencing);
        assert!(renewed.expires_at.is_after(&guard.expires_at));
        // A forged guard must not renew.
        let forged = LockGuard {
            token: "memlock:forged".to_owned(),
            ..guard.clone()
        };
        assert!(lock.renew(&forged, Duration::from_secs(1)).await.is_err());
    }

    #[tokio::test]
    async fn acquire_waits_then_wins() {
        let lock = std::sync::Arc::new(InMemoryDistributedLock::new());
        let guard = lock
            .try_acquire("exec:3", Duration::from_millis(40))
            .await
            .unwrap()
            .unwrap();
        let clone = lock.clone();
        let releaser = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            clone.release(&guard).await.unwrap();
        });
        let won = lock
            .acquire("exec:3", Duration::from_secs(30), Duration::from_secs(2))
            .await
            .unwrap();
        assert!(won.is_valid());
        releaser.await.unwrap();
    }
}
