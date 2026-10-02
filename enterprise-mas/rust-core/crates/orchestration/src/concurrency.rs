//! Concurrency enforcement: global + per-tenant + per-project execution
//! limits, and an execution-level semaphore.
//!
//! Permits are RAII: dropping them releases the slot automatically, which
//! makes leaks impossible even on panic paths.

use mas_common::constants;
use mas_common::error::AppError;
use mas_common::ids::{ProjectId, TenantId};
use mas_common::result::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Configured concurrency ceilings (all values are caps; `0` disables that
/// dimension… *except* `global`, which must be ≥ 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConcurrencyLimits {
    /// Platform-wide simultaneous executions.
    pub global: u32,
    /// Per-tenant simultaneous executions (`0` = only the global cap).
    pub per_tenant: u32,
    /// Per-project simultaneous executions (`0` = only the tenant cap).
    pub per_project: u32,
}

impl Default for ConcurrencyLimits {
    fn default() -> Self {
        Self {
            global: 1_000,
            per_tenant: constants::HARD_MAX_CONCURRENT_EXECUTIONS_PER_TENANT,
            per_project: 0,
        }
    }
}

impl ConcurrencyLimits {
    pub fn validate(&self) -> Result<()> {
        if self.global == 0 {
            return Err(AppError::invalid_field(
                "global",
                "out_of_range",
                "the global concurrency cap must be at least 1",
            ));
        }
        if self.per_tenant > constants::HARD_MAX_CONCURRENT_EXECUTIONS_PER_TENANT {
            return Err(AppError::invalid_field(
                "per_tenant",
                "out_of_range",
                format!(
                    "per-tenant cap exceeds the hard platform limit of {}",
                    constants::HARD_MAX_CONCURRENT_EXECUTIONS_PER_TENANT
                ),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct Counters {
    global: AtomicUsize,
    per_tenant: Mutex<HashMap<TenantId, usize>>,
    per_project: Mutex<HashMap<ProjectId, usize>>,
}

/// RAII slot: decrementing counters on drop.
#[derive(Debug)]
pub struct ConcurrencyPermit {
    counters: Arc<Counters>,
    tenant_id: TenantId,
    project_id: ProjectId,
    limits: ConcurrencyLimits,
}

impl Drop for ConcurrencyPermit {
    fn drop(&mut self) {
        self.counters.global.fetch_sub(1, Ordering::SeqCst);
        if self.limits.per_tenant > 0 {
            let mut tenants = self
                .counters
                .per_tenant
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(count) = tenants.get_mut(&self.tenant_id) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    tenants.remove(&self.tenant_id);
                }
            }
        }
        if self.limits.per_project > 0 {
            let mut projects = self
                .counters
                .per_project
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(count) = projects.get_mut(&self.project_id) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    projects.remove(&self.project_id);
                }
            }
        }
    }
}

/// Enforces global/tenant/project concurrency ceilings.
///
/// Acquisition is atomic from the caller's perspective; partial acquisitions
/// roll back when a higher scope rejects (global+tenant acquired but project
/// full ⇒ both released).
#[derive(Debug, Clone)]
pub struct TenantConcurrencyLimiter {
    limits: ConcurrencyLimits,
    counters: Arc<Counters>,
}

impl TenantConcurrencyLimiter {
    pub fn new(limits: ConcurrencyLimits) -> Result<Self> {
        limits.validate()?;
        Ok(Self {
            limits,
            counters: Arc::new(Counters::default()),
        })
    }

    #[must_use]
    pub const fn limits(&self) -> &ConcurrencyLimits {
        &self.limits
    }

    /// Current usage snapshot `(global, tenant, project)`.
    #[must_use]
    pub fn usage(&self, tenant_id: TenantId, project_id: ProjectId) -> (usize, usize, usize) {
        let global = self.counters.global.load(Ordering::SeqCst);
        let tenant = self
            .counters
            .per_tenant
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&tenant_id)
            .copied()
            .unwrap_or(0);
        let project = self
            .counters
            .per_project
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&project_id)
            .copied()
            .unwrap_or(0);
        (global, tenant, project)
    }

    /// Attempts to acquire a slot for `(tenant, project)`.
    ///
    /// # Errors
    /// [`AppError::RateLimited`] naming the rejecting scope.
    pub fn acquire(&self, tenant_id: TenantId, project_id: ProjectId) -> Result<ConcurrencyPermit> {
        // 1. Global (increment-then-check, roll back on overflow).
        let global = self.counters.global.fetch_add(1, Ordering::SeqCst) + 1;
        if global > self.limits.global as usize {
            self.counters.global.fetch_sub(1, Ordering::SeqCst);
            return Err(AppError::rate_limited(
                "global execution concurrency limit reached",
            ));
        }

        // 2. Tenant scope.
        if self.limits.per_tenant > 0 {
            let mut tenants = self
                .counters
                .per_tenant
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let count = tenants.entry(tenant_id).or_insert(0);
            if *count >= self.limits.per_tenant as usize {
                drop(tenants);
                self.counters.global.fetch_sub(1, Ordering::SeqCst);
                return Err(AppError::rate_limited(format!(
                    "tenant {tenant_id} reached its concurrent execution limit ({})",
                    self.limits.per_tenant
                )));
            }
            *count += 1;
        }

        // 3. Project scope.
        if self.limits.per_project > 0 {
            let mut projects = self
                .counters
                .per_project
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let count = projects.entry(project_id).or_insert(0);
            if *count >= self.limits.per_project as usize {
                drop(projects);
                if self.limits.per_tenant > 0 {
                    let mut tenants = self
                        .counters
                        .per_tenant
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    if let Some(tenant_count) = tenants.get_mut(&tenant_id) {
                        *tenant_count = tenant_count.saturating_sub(1);
                    }
                }
                self.counters.global.fetch_sub(1, Ordering::SeqCst);
                return Err(AppError::rate_limited(format!(
                    "project {project_id} reached its concurrent execution limit ({})",
                    self.limits.per_project
                )));
            }
            *count += 1;
        }

        Ok(ConcurrencyPermit {
            counters: Arc::clone(&self.counters),
            tenant_id,
            project_id,
            limits: self.limits,
        })
    }
}

/// A bounded execution semaphore (tokio-backed) with metadata, used to gate
/// step-level parallelism inside one execution (e.g. `Parallel` node fan-out
/// is capped by `max_parallel_tasks`).
#[derive(Debug)]
pub struct ExecutionSemaphore {
    name: String,
    capacity: usize,
    semaphore: Arc<tokio::sync::Semaphore>,
    in_use: Arc<AtomicI64>,
}

impl ExecutionSemaphore {
    /// Creates a semaphore allowing `capacity` simultaneous holders (≥ 1).
    pub fn new(name: impl Into<String>, capacity: u32) -> Result<Self> {
        if capacity == 0 {
            return Err(AppError::invalid_field(
                "capacity",
                "out_of_range",
                "semaphore capacity must be at least 1",
            ));
        }
        Ok(Self {
            name: name.into(),
            capacity: capacity as usize,
            semaphore: Arc::new(tokio::sync::Semaphore::new(capacity as usize)),
            in_use: Arc::new(AtomicI64::new(0)),
        })
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    #[must_use]
    pub fn in_use(&self) -> usize {
        self.in_use.load(Ordering::SeqCst).max(0) as usize
    }

    #[must_use]
    pub fn available(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Acquires one permit, waiting when exhausted.
    pub async fn acquire(&self) -> Result<SemaphoreSlot> {
        let permit =
            self.semaphore.clone().acquire_owned().await.map_err(|_| {
                AppError::cancelled(format!("semaphore '{}' was closed", self.name))
            })?;
        self.in_use.fetch_add(1, Ordering::SeqCst);
        Ok(SemaphoreSlot {
            _permit: Some(permit),
            in_use: Arc::clone(&self.in_use),
        })
    }

    /// Attempts to acquire without waiting.
    pub fn try_acquire(&self) -> Result<Option<SemaphoreSlot>> {
        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => {
                self.in_use.fetch_add(1, Ordering::SeqCst);
                Ok(Some(SemaphoreSlot {
                    _permit: Some(permit),
                    in_use: Arc::clone(&self.in_use),
                }))
            },
            Err(tokio::sync::TryAcquireError::NoPermits) => Ok(None),
            Err(tokio::sync::TryAcquireError::Closed) => Err(AppError::cancelled(format!(
                "semaphore '{}' was closed",
                self.name
            ))),
        }
    }
}

/// RAII permit for [`ExecutionSemaphore`].
#[derive(Debug)]
pub struct SemaphoreSlot {
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
    in_use: Arc<AtomicI64>,
}

impl Drop for SemaphoreSlot {
    fn drop(&mut self) {
        self.in_use.fetch_sub(1, Ordering::SeqCst);
        drop(self._permit.take());
    }
}

impl fmt::Display for SemaphoreSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("semaphore-slot")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limiter_enforces_scopes_and_releases_on_drop() {
        let limiter = TenantConcurrencyLimiter::new(ConcurrencyLimits {
            global: 3,
            per_tenant: 2,
            per_project: 1,
        })
        .unwrap();
        let t1 = TenantId::new();
        let p1 = ProjectId::new();
        let p2 = ProjectId::new();

        let a = limiter.acquire(t1, p1).unwrap();
        // Project cap of 1 blocks a second slot on p1…
        let err = limiter.acquire(t1, p1).unwrap_err();
        assert_eq!(err.error_code(), "RATE_LIMITED");
        // …another project works.
        let b = limiter.acquire(t1, p2).unwrap();
        // …tenant cap 2 blocks the third.
        assert!(limiter.acquire(t1, ProjectId::new()).is_err());

        // Tenant 2 unaffected.
        let t2 = TenantId::new();
        let c = limiter.acquire(t2, ProjectId::new()).unwrap();
        // Global cap 3 blocks tenant 3.
        assert!(limiter.acquire(TenantId::new(), ProjectId::new()).is_err());

        drop(b);
        // Freed tenant slot becomes acquirable again.
        let _d = limiter.acquire(t1, p2).unwrap();
        drop(a);
        drop(c);
    }

    #[test]
    fn usage_reports_counts() {
        let limiter = TenantConcurrencyLimiter::new(ConcurrencyLimits::default()).unwrap();
        let tenant = TenantId::new();
        let project = ProjectId::new();
        let permit = limiter.acquire(tenant, project).unwrap();
        let (global, ten, proj) = limiter.usage(tenant, project);
        assert_eq!(global, 1);
        assert_eq!(ten, 1);
        assert_eq!(proj, 0); // per_project disabled by default
        drop(permit);
        assert_eq!(limiter.usage(tenant, project).0, 0);
    }

    #[tokio::test]
    async fn semaphore_caps_parallelism_and_recovers() {
        let semaphore = ExecutionSemaphore::new("steps", 2).unwrap();
        let s1 = semaphore.acquire().await.unwrap();
        let s2 = semaphore.try_acquire().unwrap().unwrap();
        assert_eq!(semaphore.in_use(), 2);
        assert!(semaphore.try_acquire().unwrap().is_none());
        drop(s1);
        let s3 = semaphore.acquire().await.unwrap();
        assert_eq!(semaphore.in_use(), 2);
        drop(s2);
        drop(s3);
        assert_eq!(semaphore.in_use(), 0);
    }

    #[test]
    fn limit_validation() {
        assert!(TenantConcurrencyLimiter::new(ConcurrencyLimits {
            global: 0,
            per_tenant: 0,
            per_project: 0,
        })
        .is_err());
        assert!(TenantConcurrencyLimiter::new(ConcurrencyLimits {
            global: 10,
            per_tenant: constants::HARD_MAX_CONCURRENT_EXECUTIONS_PER_TENANT + 1,
            per_project: 0,
        })
        .is_err());
    }
}
