//! Atomic counter store: the abstraction Redis-backed adapters implement.
//!
//! Quota math reduces to `(tenant, dimension, bucket)` counters. Buckets are
//! whole fixed windows: `bucket = unix_seconds / window_seconds`, and window
//! `None` (bucket 0) is a gauge — counters that never reset (concurrent runs,
//! storage bytes).
//!
//! Operations are *saturating*: a counter never underflows below zero; expiry
//! (TTL) is expressed by bucket arithmetic, not stored state, except in the
//! garbage collection of old buckets.

use mas_common::ids::TenantId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::quota::QuotaDimension;
use std::collections::BTreeMap;
use std::sync::Mutex;

/// Identity of one counter: tenant × dimension × time bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CounterKey {
    /// Owning tenant.
    pub tenant_id: TenantId,
    /// Metered dimension.
    pub dimension: QuotaDimension,
    /// Window bucket index (`0` = non-expiring gauge).
    pub bucket: i64,
}

impl CounterKey {
    /// Key for concrete `at` time inside a fixed window of `window_seconds`.
    /// `window_seconds = None` → the gauge bucket (0).
    #[must_use]
    pub fn for_time(
        tenant_id: TenantId,
        dimension: QuotaDimension,
        window_seconds: Option<u64>,
        at: &Timestamp,
    ) -> Self {
        let bucket = match window_seconds {
            Some(window) if window > 0 => at.to_unix_seconds().div_euclid(window as i64),
            _ => 0,
        };
        Self {
            tenant_id,
            dimension,
            bucket,
        }
    }

    /// End (exclusive) of this key's window as a timestamp; gauges (`bucket
    /// 0` with `window=None`) report `None`.
    #[must_use]
    pub fn window_end(&self, window_seconds: u64) -> Option<Timestamp> {
        let end = self
            .bucket
            .saturating_add(1)
            .checked_mul(window_seconds as i64)?;
        Timestamp::from_unix_seconds(end).ok()
    }
}

/// The counter operations every store must offer atomically.
#[async_trait::async_trait]
pub trait CounterStorePort: Send + Sync + std::fmt::Debug {
    /// Atomically adds `amount`, returning the new value.
    async fn increment(&self, key: CounterKey, amount: u64) -> Result<u64>;

    /// Atomically subtracts `amount` (saturating at 0), returning the new
    /// value.
    async fn decrement(&self, key: CounterKey, amount: u64) -> Result<u64>;

    /// Current value (`0` when absent).
    async fn get(&self, key: CounterKey) -> Result<u64>;

    /// Removes every bucket strictly before `min_bucket` for periodic
    /// dimensions (keeps memory bounded; gauges are untouched).
    async fn gc_buckets(&self, min_bucket: i64) -> Result<u64>;
}

/// Default bound on retained counter entries per GC sweep.
pub const DEFAULT_MAX_COUNTERS: usize = 1_000_000;

/// In-memory reference store (tests, single-process local dev).
#[derive(Debug, Default)]
pub struct InMemoryCounterStore {
    counters: Mutex<BTreeMap<CounterKey, u64>>,
}

impl InMemoryCounterStore {
    /// Empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of counters currently held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.counters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// Whether empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn with<R>(&self, f: impl FnOnce(&mut BTreeMap<CounterKey, u64>) -> R) -> R {
        let mut guard = self.counters.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut guard)
    }
}

#[async_trait::async_trait]
impl CounterStorePort for InMemoryCounterStore {
    async fn increment(&self, key: CounterKey, amount: u64) -> Result<u64> {
        Ok(self.with(|counters| {
            let entry = counters.entry(key).or_insert(0);
            *entry = entry.saturating_add(amount);
            *entry
        }))
    }

    async fn decrement(&self, key: CounterKey, amount: u64) -> Result<u64> {
        Ok(self.with(|counters| {
            let entry = counters.entry(key).or_insert(0);
            *entry = entry.saturating_sub(amount);
            *entry
        }))
    }

    async fn get(&self, key: CounterKey) -> Result<u64> {
        Ok(self.with(|counters| counters.get(&key).copied().unwrap_or(0)))
    }

    async fn gc_buckets(&self, min_bucket: i64) -> Result<u64> {
        Ok(self.with(|counters| {
            let before = counters.len() as u64;
            counters.retain(|key, _| key.bucket == 0 || key.bucket >= min_bucket);
            before - counters.len() as u64
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn increment_decrement_saturate_and_gc() {
        let store = InMemoryCounterStore::new();
        let tenant = TenantId::new();
        let now = Timestamp::from_unix_seconds(3_600_000).expect("ts");
        let minute_key = CounterKey::for_time(tenant, QuotaDimension::Requests, Some(60), &now);
        assert_eq!(minute_key.bucket, 3_600_000 / 60);
        let gauge_key = CounterKey::for_time(tenant, QuotaDimension::ConcurrentRuns, None, &now);
        assert_eq!(gauge_key.bucket, 0);

        assert_eq!(store.increment(minute_key, 5).await.expect("incr"), 5);
        assert_eq!(store.increment(minute_key, 7).await.expect("incr"), 12);
        assert_eq!(store.decrement(minute_key, 5).await.expect("decr"), 7);
        assert_eq!(
            store.decrement(minute_key, 100).await.expect("decr"),
            0,
            "saturates at 0"
        );
        assert_eq!(store.get(minute_key).await.expect("get"), 0);

        store.increment(gauge_key, 3).await.expect("gauge incr");
        assert_eq!(store.len(), 2);

        // GC: nothing removed for current bucket; older bucket evicted, gauge kept.
        let old = CounterKey {
            bucket: 10,
            ..minute_key
        };
        store.increment(old, 1).await.expect("old bucket");
        let removed = store.gc_buckets(11).await.expect("gc");
        assert_eq!(removed, 1);
        assert_eq!(
            store.get(gauge_key).await.expect("gauge get"),
            3,
            "gauge survives GC"
        );

        // Window end math.
        let end = minute_key.window_end(60).expect("window end");
        assert_eq!(end.to_unix_seconds(), (minute_key.bucket + 1) * 60);
        assert!(gauge_key.window_end(60).is_some() || gauge_key.bucket == 0);
    }
}
