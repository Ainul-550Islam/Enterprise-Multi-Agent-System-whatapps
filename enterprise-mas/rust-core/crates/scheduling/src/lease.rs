//! Fencing-token leases for multi-replica safety.
//!
//! With two scheduler replicas, the naive "whoever scans wins" doubles runs.
//! Before dispatching a run, a replica must hold the lease for a resource
//! key (`schedule:<id>` — one per schedule). Leases expire (crash tolerance),
//! renew only by holder, and every acquisition issues a **monotonically
//! increasing fencing token**: if a stale holder wakes up and tries to act,
//! its lower token proves obsolescence to downstream consumers.

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

/// Default lease time-to-live: 30 seconds (renewed every tick, 5s scans).
pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);

/// Resource key prefix for schedule leases.
pub const LEASE_PREFIX_SCHEDULE: &str = "schedule";

/// Builds the canonical resource key for one schedule.
#[must_use]
pub fn schedule_resource(schedule_id: &mas_common::ids::ScheduleId) -> String {
    format!("{LEASE_PREFIX_SCHEDULE}:{}", schedule_id.as_uuid())
}

/// A held lease.
#[derive(Debug, Clone, PartialEq)]
pub struct Lease {
    /// What is leased (`schedule:<uuid>`).
    pub resource: String,
    /// Replica identity holding it.
    pub holder: String,
    /// Monotonic fencing token issued at acquisition.
    pub fencing_token: u64,
    /// Expiry (crash fail-safe); renew before this passes.
    pub expires_at: Timestamp,
}

impl Lease {
    /// Whether the lease is still valid at `now`.
    #[must_use]
    pub fn is_active(&self, now: &Timestamp) -> bool {
        self.expires_at.is_after(now)
    }
}

/// The lease store the scheduler replicas share.
#[async_trait::async_trait]
pub trait LeaseStorePort: Send + Sync + std::fmt::Debug {
    /// Tries to acquire `resource` for `holder` until `now + ttl`.
    ///
    /// Succeeds when the resource is free or its lease expired; returns the
    /// lease (with a fresh fencing token). Fails with `Conflict` when another
    /// live holder owns it.
    async fn try_acquire(
        &self,
        resource: &str,
        holder: &str,
        ttl: Duration,
        now: &Timestamp,
    ) -> Result<Lease>;

    /// Extends a held lease. Only the current holder may renew, presenting
    /// its fencing token (also a stale-holder guard).
    async fn renew(&self, lease: &Lease, ttl: Duration, now: &Timestamp) -> Result<Lease>;

    /// Releases a held lease (owner-honored fencing token required).
    async fn release(&self, lease: &Lease, now: &Timestamp) -> Result<()>;

    /// REAP expired leases (recovery). Returns the resources freed.
    async fn reap_expired(&self, now: &Timestamp) -> Result<Vec<Lease>>;

    /// Reads the current lease of a resource, if any.
    async fn get(&self, resource: &str) -> Result<Option<Lease>>;
}

/// Validate holder/resource labels (no surprise whitespace, bounded).
fn validate_label(kind: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | ':' | '.'))
    {
        return Err(AppError::invalid_field(
            kind,
            "invalid_format",
            format!("{kind} must be 1..=128 URL-safe characters"),
        ));
    }
    Ok(())
}

/// In-memory lease store (single-process reference; production adapters
/// implement the same port against Postgres advisory locks / Redis).
#[derive(Debug, Default)]
pub struct InMemoryLeaseStore {
    inner: Mutex<LeaseState>,
}

#[derive(Debug, Default)]
struct LeaseState {
    leases: BTreeMap<String, Lease>,
    next_token: u64,
}

impl InMemoryLeaseStore {
    /// Empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn with<R>(&self, f: impl FnOnce(&mut LeaseState) -> Result<R>) -> Result<R> {
        let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut state)
    }

    fn issue(
        state: &mut LeaseState,
        resource: &str,
        holder: &str,
        ttl: Duration,
        now: &Timestamp,
    ) -> Lease {
        state.next_token = state.next_token.saturating_add(1);
        let expires_at = now.checked_add(ttl).unwrap_or(*now);
        let lease = Lease {
            resource: resource.to_owned(),
            holder: holder.to_owned(),
            fencing_token: state.next_token,
            expires_at,
        };
        state.leases.insert(resource.to_owned(), lease.clone());
        lease
    }
}

#[async_trait::async_trait]
impl LeaseStorePort for InMemoryLeaseStore {
    async fn try_acquire(
        &self,
        resource: &str,
        holder: &str,
        ttl: Duration,
        now: &Timestamp,
    ) -> Result<Lease> {
        validate_label("resource", resource)?;
        validate_label("holder", holder)?;
        self.with(|state| {
            if let Some(existing) = state.leases.get(resource) {
                if existing.is_active(now) && existing.holder != holder {
                    return Err(AppError::conflict(format!(
                        "resource {resource} is leased by another holder until {}",
                        existing.expires_at.to_rfc3339_millis()
                    )));
                }
            }
            Ok(Self::issue(state, resource, holder, ttl, now))
        })
    }

    async fn renew(&self, lease: &Lease, ttl: Duration, now: &Timestamp) -> Result<Lease> {
        self.with(|state| {
            let Some(existing) = state.leases.get(&lease.resource) else {
                return Err(AppError::not_found("lease", lease.resource.clone()));
            };
            if existing.holder != lease.holder {
                return Err(AppError::forbidden(format!(
                    "lease for {} is held by another replica",
                    lease.resource
                )));
            }
            if existing.fencing_token != lease.fencing_token {
                return Err(AppError::conflict(format!(
                    "stale fencing token {} (current {}) — this replica is no longer the holder",
                    lease.fencing_token, existing.fencing_token
                )));
            }
            Ok(Self::issue(state, &lease.resource, &lease.holder, ttl, now))
        })
    }

    async fn release(&self, lease: &Lease, now: &Timestamp) -> Result<()> {
        let _ = now;
        self.with(|state| {
            let Some(existing) = state.leases.get(&lease.resource) else {
                return Ok(()); // already gone (expired/released) — idempotent
            };
            if existing.holder == lease.holder && existing.fencing_token == lease.fencing_token {
                state.leases.remove(&lease.resource);
            }
            Ok(())
        })
    }

    async fn reap_expired(&self, now: &Timestamp) -> Result<Vec<Lease>> {
        self.with(|state| {
            let expired: Vec<String> = state
                .leases
                .iter()
                .filter(|(_, lease)| !lease.is_active(now))
                .map(|(resource, _)| resource.clone())
                .collect();
            let mut out = Vec::with_capacity(expired.len());
            for resource in expired {
                if let Some(lease) = state.leases.remove(&resource) {
                    out.push(lease);
                }
            }
            Ok(out)
        })
    }

    async fn get(&self, resource: &str) -> Result<Option<Lease>> {
        self.with(|state| Ok(state.leases.get(resource).cloned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn acquire_conflict_renew_release_cycle() {
        let store = InMemoryLeaseStore::new();
        let now = Timestamp::now();
        let ttl = Duration::from_secs(30);

        let mine = store
            .try_acquire("schedule:1", "replica-a", ttl, &now)
            .await
            .expect("first acquire");
        assert_eq!(mine.holder, "replica-a");
        assert!(mine.fencing_token > 0);

        // Another live holder is refused.
        let conflict = store
            .try_acquire("schedule:1", "replica-b", ttl, &now)
            .await
            .expect_err("occupied");
        assert_eq!(conflict.error_code(), "CONFLICT");

        // Same holder may re-acquire (its own lease), getting a NEW token.
        let again = store
            .try_acquire("schedule:1", "replica-a", ttl, &now)
            .await
            .expect("re-acquire by same holder");
        assert!(again.fencing_token > mine.fencing_token);

        // Renew with the stale token now fails (this is the fencing guard).
        let stale = store
            .renew(&mine, ttl, &now)
            .await
            .expect_err("stale renew");
        assert_eq!(stale.error_code(), "CONFLICT");
        // Renew with the current token works.
        let renewed = store.renew(&again, ttl, &now).await.expect("renew");
        assert!(renewed.fencing_token > again.fencing_token);
        assert!(
            renewed.expires_at.is_after(&again.expires_at)
                || renewed.expires_at == again.expires_at
        );

        // Release honors token ownership; a foreign release is a silent no-op.
        let foreign = Lease {
            resource: "schedule:1".to_owned(),
            holder: "replica-b".to_owned(),
            fencing_token: renewed.fencing_token,
            expires_at: renewed.expires_at,
        };
        store.release(&foreign, &now).await.expect("foreign noop");
        assert!(store.get("schedule:1").await.expect("get").is_some());
        store.release(&renewed, &now).await.expect("release");
        assert!(store.get("schedule:1").await.expect("get").is_none());

        // Now another replica may acquire freely.
        store
            .try_acquire("schedule:1", "replica-b", ttl, &now)
            .await
            .expect("free after release");
    }

    #[tokio::test]
    async fn expiry_and_reaping_free_resources() {
        let store = InMemoryLeaseStore::new();
        let t0 = Timestamp::now();
        let ttl = Duration::from_secs(10);
        store
            .try_acquire("schedule:1", "a", ttl, &t0)
            .await
            .expect("acquire 1");
        store
            .try_acquire("schedule:2", "b", Duration::from_secs(3600), &t0)
            .await
            .expect("acquire 2");

        let t1 = t0.checked_add(Duration::from_secs(11)).expect("t1");
        // schedule:1 expired → re-acquirable by anyone even without reap.
        store
            .try_acquire("schedule:1", "b", ttl, &t1)
            .await
            .expect("expired lease re-acquirable");

        let reaped = store.reap_expired(&t1).await.expect("reap");
        assert!(
            reaped.is_empty(),
            "schedule:1 was re-taken; schedule:2 is still live: nothing to reap"
        );

        let t2 = t1.checked_add(Duration::from_secs(11)).expect("t2");
        let reaped = store.reap_expired(&t2).await.expect("reap");
        let names: Vec<&str> = reaped.iter().map(|l| l.resource.as_str()).collect();
        assert_eq!(names, ["schedule:1"], "only the short-TTL lease expires");
        assert!(store.get("schedule:2").await.expect("get").is_some());

        // Labels are validated.
        assert!(store
            .try_acquire("bad resource", "a", ttl, &t0)
            .await
            .is_err());
    }
}
