//! TTL decision cache with stats and bounded size.
//!
//! Cached keys must incorporate the *active policy fingerprint*; the engine
//! computes it, so a policy flip invalidates naturally without an explicit
//! purge path (entries age out on TTL too).

use mas_common::timestamps::Timestamp;
use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;
use std::time::Duration;

/// Cached evaluation bytes (decision + provenance; latency/from_cache are
/// recomputed on read).
#[derive(Debug, Clone)]
pub struct CachedDecision<T> {
    pub value: T,
    pub stored_at: Timestamp,
}

/// Hit/miss counters for observability.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub puts: u64,
    pub evictions: u64,
}

/// Bounded TTL cache for policy decisions.
pub struct DecisionCache<T: Clone> {
    entries: Mutex<HashMap<String, CachedDecision<T>>>,
    ttl: Duration,
    max_entries: usize,
    stats: Mutex<CacheStats>,
}

impl<T: Clone + fmt::Debug> fmt::Debug for DecisionCache<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DecisionCache")
            .field(
                "entries",
                &self.entries.lock().map(|m| m.len()).unwrap_or(0),
            )
            .field("ttl", &self.ttl)
            .field("max_entries", &self.max_entries)
            .finish()
    }
}

impl<T: Clone> Default for DecisionCache<T> {
    fn default() -> Self {
        Self::new(Duration::from_secs(30), 10_000)
    }
}

impl<T: Clone> DecisionCache<T> {
    pub fn new(ttl: Duration, max_entries: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl,
            max_entries: max_entries.max(1),
            stats: Mutex::new(CacheStats::default()),
        }
    }

    /// Fetch, honouring TTL.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<T> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        match entries.get(key) {
            Some(cached) if cached.stored_at.elapsed() <= Some(self.ttl) => {
                self.lock_stats(|s| s.hits += 1);
                Some(cached.value.clone())
            },
            Some(_) => {
                entries.remove(key); // expired
                self.lock_stats(|s| {
                    s.misses += 1;
                    s.evictions += 1;
                });
                None
            },
            None => {
                self.lock_stats(|s| s.misses += 1);
                None
            },
        }
    }

    pub fn put(&self, key: impl Into<String>, value: T) {
        let key = key.into();
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        // Opportunistic sweep when full: evict expired first, then the
        // oldest entry (FDR-lite — cheap and branch-predictable).
        if entries.len() >= self.max_entries {
            let ttl = self.ttl;
            let before = entries.len();
            entries.retain(|_, cached| cached.stored_at.elapsed() <= Some(ttl));
            if entries.len() >= self.max_entries {
                if let Some(oldest) = entries
                    .iter()
                    .min_by_key(|(_, cached)| cached.stored_at.to_unix_ms())
                    .map(|(key, _)| key.clone())
                {
                    entries.remove(&oldest);
                }
            }
            let evicted = before.saturating_sub(entries.len());
            if evicted > 0 {
                let mut stats = self.stats.lock().unwrap_or_else(|e| e.into_inner());
                stats.evictions += u64::try_from(evicted).unwrap_or(u64::MAX);
            }
        }
        entries.insert(
            key,
            CachedDecision {
                value,
                stored_at: Timestamp::now(),
            },
        );
        self.lock_stats(|s| s.puts += 1);
    }

    fn lock_stats(&self, f: impl FnOnce(&mut CacheStats)) {
        let mut stats = self.stats.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut stats);
    }

    #[must_use]
    pub fn stats(&self) -> CacheStats {
        *self.stats.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drops everything (policy-set force-refresh).
    pub fn clear(&self) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        self.lock_stats(|s| s.evictions += entries.len() as u64);
        entries.clear();
    }
}
