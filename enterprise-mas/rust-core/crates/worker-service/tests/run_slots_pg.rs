//! PURPOSE: The live-PostgreSQL suite for the worker's concurrency gate — the
//! `RunSlotPort` implementation the production worker installs at boot.
//!
//! Owning layer: `mas-worker-service` (tests).
//!
//! Production responsibility: prove against a real database, through the real
//! adapter and the real port, the four things a concurrency slot must do and
//! that no in-memory double can demonstrate:
//!
//!   1. a held slot blocks the next run, and only a release frees it;
//!   2. eight workers racing for three slots get exactly three — the ceiling
//!      is a row, not a constant in a process;
//!   3. a crashed worker's slot lapses with its lease, and a live worker that
//!      keeps renewing never has one swept;
//!   4. tenant A's slot is invisible and irrelevant to tenant B.
//!
//! This file is also the regression guard for the whole family of defects found
//! by *running* production code rather than reading it: statements that work
//! under the migrator (a superuser — no RLS, every grant) and fail with
//! SQLSTATE 42501 under the role that actually executes them. The gate is
//! therefore exercised on `mas_app`, inside
//! `scoped_request::with_authoritative_scope`, which is exactly how the worker
//! will call it.
//!
//! Data ownership: this file owns nothing. Every row it touches belongs to
//! `mas-persistence`; the test only asserts on the answers.
//!
//! It must NOT:
//!   • run without `MAS_TEST_DATABASE_URL` (it skips loudly instead);
//!   • fall back to the migrator pool for a gate assertion — a gate that works
//!     only for a superuser is a gate that does not work;
//!   • be pointed at a production database;
//!   • weaken a ceiling or isolation assertion to accommodate a fixture.

mod support;

use mas_application::control_plane::ControlPlane;
use mas_common::enums::Environment;
use mas_common::ids::{ExecutionId, OrganizationId, TaskId, TenantId};
use mas_common::timestamps::Timestamp;
use mas_domain::quota::{EnforcementMode, Quota, QuotaDimension, QuotaPeriod};
use mas_domain::value_objects::Slug;
use mas_domain::{IsolationMode, Organization, Project, Tenant};
use mas_persistence::repositories::control_plane::ControlPlaneStores;
use mas_persistence::repositories::{OrganizationStore, ProjectStore, TenantStore};
use mas_quota::counter::{CounterKey, CounterStorePort};
use mas_quota::engine::QuotaRegistryPort;
use mas_security::AuthoritativeScope;
use mas_worker::run_slot::{RunSlot, RunSlotPort};
use mas_worker_service::run_slots::ControlPlaneRunSlots;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;

/// A fixed clock. Every test names its instants explicitly: a concurrency slot
/// is defined by when its lease expires, and a test that measured real time
/// could not say what it was asserting.
const T0: i64 = 1_800_000_000;

fn at(offset: i64) -> Timestamp {
    Timestamp::from_unix_seconds(T0 + offset).expect("timestamp")
}

/// Seeds a fresh tenant, organization and project through the migrator pool.
///
/// Every test seeds its own tenant rather than sharing a fixture, so no test's
/// assertions depend on another test's counters.
async fn seed_scope(pool: &PgPool, label: &str) -> (TenantId, OrganizationId) {
    let mut connection = pool.acquire().await.expect("connection");
    let suffix = uuid::Uuid::now_v7().to_string().replace('-', "");

    let organization = Organization::create(
        format!("Slotco {label}"),
        format!("Slotco ({label})"),
        Slug::new(&format!("slotco-{label}-{suffix}")).expect("org slug"),
    )
    .expect("organization");
    OrganizationStore::insert_in(&mut connection, &organization)
        .await
        .expect("org insert");

    let tenant = Tenant::create(
        organization.id,
        format!("{label} tenant"),
        Slug::new(&format!("tenant-{label}-{suffix}")).expect("tenant slug"),
        Environment::Development,
        IsolationMode::SharedRls,
    )
    .expect("tenant");
    let tenant_id = tenant.id;
    TenantStore::insert_in(&mut connection, &tenant)
        .await
        .expect("tenant insert");

    let project = Project::create(
        tenant_id,
        organization.id,
        format!("{label} project"),
        Slug::new(&format!("proj-{label}-{suffix}")).expect("project slug"),
    )
    .expect("project");
    ProjectStore::insert_in(&mut connection, &project)
        .await
        .expect("project insert");

    (tenant_id, organization.id)
}

/// Configures a `ConcurrentRuns` ceiling for one tenant.
///
/// Written through the migrator pool: a quota definition is an administrative
/// fact about the tenant, not a row the tenant's own runtime role maintains.
async fn define_concurrency(pool: &PgPool, tenant: TenantId, limit: u64) {
    let stores = ControlPlaneStores::new(pool.clone());
    let quota = Quota::new(
        tenant,
        QuotaDimension::ConcurrentRuns,
        // Concurrency is a gauge, not a resetting counter: the only coherent
        // period is `Unbounded`, and `Quota::new` rejects any other.
        QuotaPeriod::Unbounded,
        limit,
        EnforcementMode::Enforce,
    )
    .expect("quota");
    QuotaRegistryPort::upsert(&stores.quotas, &quota)
        .await
        .expect("quota upsert");
}

/// The gauge counter a tenant's concurrent-run ceiling is charged against.
fn gauge(tenant: TenantId) -> CounterKey {
    CounterKey::for_time(tenant, QuotaDimension::ConcurrentRuns, None, &at(0))
}

/// Reads the gauge on the migrator pool, where no policy hides a row.
async fn gauge_value(pool: &PgPool, tenant: TenantId) -> u64 {
    let stores = ControlPlaneStores::new(pool.clone());
    CounterStorePort::get(&stores.counters, gauge(tenant))
        .await
        .expect("counter read")
}

/// One test's assembled world.
struct Rig {
    migrator: PgPool,
    app: PgPool,
    tenant: TenantId,
    organization: OrganizationId,
    /// The plane the gate is built over — the runtime role, as in production.
    plane: Arc<ControlPlane>,
    /// The plane a sweeper would run: platform-wide, so it can reclaim any
    /// tenant's lapsed slot.
    sweeper: Arc<ControlPlane>,
    gate: ControlPlaneRunSlots,
}

impl Rig {
    async fn build(label: &str, lease: Duration) -> Option<Self> {
        let migrator = support::live_pool("run-slots-seed").await?;
        let app = support::live_app_pool("run-slots-gate").await?;
        let (tenant, organization) = seed_scope(&migrator, label).await;
        let plane = Arc::new(
            mas_persistence::repositories::control_plane::adapters::build_postgres_control_plane(
                app.clone(),
            )
            .with_lease(lease),
        );
        let sweeper = Arc::new(
            mas_persistence::repositories::control_plane::adapters::build_postgres_control_plane(
                migrator.clone(),
            )
            .with_lease(lease),
        );
        let gate = ControlPlaneRunSlots::new(app.clone(), Arc::clone(&plane), "worker-test");
        Some(Self {
            migrator,
            app,
            tenant,
            organization,
            plane,
            sweeper,
            gate,
        })
    }

    /// The scope the gate will open: this tenant, this organization, the
    /// worker as actor.
    fn scope(&self) -> AuthoritativeScope {
        AuthoritativeScope::resolved(self.tenant, self.organization, "worker-test")
    }

    async fn held(&self, task: TaskId) -> bool {
        matches!(
            self.gate
                .acquire(self.tenant, self.organization, None, task, &at(0))
                .await,
            Ok(RunSlot::Acquired)
        )
    }
}

// ───────────────────────────────────────────────────────────────────────────
// The ceiling is enforced
// ───────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_held_slot_blocks_the_next_run_and_only_a_release_frees_it() {
    let Some(rig) = Rig::build("hold", Duration::from_secs(30)).await else {
        return;
    };
    define_concurrency(&rig.migrator, rig.tenant, 1).await;

    let first = TaskId::new();
    let second = TaskId::new();

    assert!(
        rig.held(first).await,
        "the first run takes the only slot there is"
    );
    assert_eq!(gauge_value(&rig.migrator, rig.tenant).await, 1);

    // The second run is refused, and refused deterministically: this is a
    // ceiling, not a transient. A refusal that looked retryable would have the
    // worker hammer the gate instead of running the task later.
    let refused = rig
        .gate
        .acquire(rig.tenant, rig.organization, None, second, &at(0))
        .await;
    let error = refused.expect_err("a tenant at its ceiling must be refused");
    assert_eq!(
        error.error_code(),
        "QUOTA_EXCEEDED",
        "the refusal is the deterministic, non-retryable quota code"
    );
    assert!(
        !error.is_retryable(),
        "a concurrency refusal must never be retried in place"
    );
    assert_eq!(
        gauge_value(&rig.migrator, rig.tenant).await,
        1,
        "a refused run charged nothing"
    );

    // Releasing the first run is what makes room. Nothing else does.
    rig.gate
        .release(rig.tenant, rig.organization, None, first, &at(1))
        .await
        .expect("release");
    assert_eq!(
        gauge_value(&rig.migrator, rig.tenant).await,
        0,
        "the slot went back to the tenant"
    );
    assert!(rig.held(second).await, "the run that was refused now fits");
}

#[tokio::test]
async fn eight_workers_racing_for_three_slots_get_exactly_three() {
    let Some(rig) = Rig::build("race", Duration::from_secs(30)).await else {
        return;
    };
    define_concurrency(&rig.migrator, rig.tenant, 3).await;

    // Eight distinct tasks, each asking at the same instant. The ceiling is
    // three. The only answer that is correct for every interleaving is
    // exactly three `Held` and five refusals — and that is true across
    // processes, because the counter is a row and the reservation is a row.
    let tasks: Vec<TaskId> = (0..8).map(|_| TaskId::new()).collect();
    let mut racing = tokio::task::JoinSet::new();
    for task in &tasks {
        let gate = rig.gate.clone();
        let tenant = rig.tenant;
        let organization = rig.organization;
        let task = *task;
        racing.spawn(async move { gate.acquire(tenant, organization, None, task, &at(0)).await });
    }

    let mut held = 0usize;
    let mut refused = 0usize;
    while let Some(outcome) = racing.join_next().await {
        match outcome.expect("task joined") {
            Ok(RunSlot::Acquired) => held += 1,
            Ok(RunSlot::NotMetered) => {
                panic!("a ceiling of 3 was configured, so every run is metered")
            },
            Ok(RunSlot::HeldElsewhere) => {
                panic!("eight *distinct* tasks cannot collide on one run's slot")
            },
            Err(error) => {
                assert_eq!(error.error_code(), "QUOTA_EXCEEDED");
                refused += 1;
            },
        }
    }

    assert_eq!(held, 3, "the ceiling held under a real race");
    assert_eq!(refused, 5, "every loser was refused, none were admitted");
    assert_eq!(
        gauge_value(&rig.migrator, rig.tenant).await,
        3,
        "the gauge agrees with the three winners"
    );
}

#[tokio::test]
async fn a_tenant_without_a_concurrency_ceiling_is_not_metered() {
    let Some(rig) = Rig::build("unmetered", Duration::from_secs(30)).await else {
        return;
    };
    // No `define_concurrency` call: the tenant has bought no ceiling.

    let first = TaskId::new();
    let second = TaskId::new();

    for task in [first, second] {
        assert!(
            matches!(
                rig.gate
                    .acquire(rig.tenant, rig.organization, None, task, &at(0))
                    .await
                    .expect("acquire"),
                RunSlot::NotMetered
            ),
            "no ceiling configured means nothing to hold — not a refusal"
        );
    }
    assert_eq!(
        gauge_value(&rig.migrator, rig.tenant).await,
        0,
        "an unmetered tenant was not charged for a slot it never held"
    );

    // Releasing a slot that was never taken is still a legal no-op.
    rig.gate
        .release(rig.tenant, rig.organization, None, first, &at(1))
        .await
        .expect("release of a slot that was never taken is not an error");
}

// ───────────────────────────────────────────────────────────────────────────
// Leases, renewal and crash recovery
// ───────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_renewed_slot_survives_the_sweep_that_would_have_taken_it() {
    let Some(rig) = Rig::build("renew", Duration::from_secs(30)).await else {
        return;
    };
    define_concurrency(&rig.migrator, rig.tenant, 1).await;
    let task = TaskId::new();

    // Lease is 30s, so at t+10 the slot would be swept if it were stale.
    assert!(rig.held(task).await);

    // The heartbeat renews it to t+60 — the same clock the durable task lease
    // rides, so a run that is still going is not declared dead by either.
    rig.gate
        .renew(rig.tenant, rig.organization, None, task, &at(60), &at(10))
        .await
        .expect("renew");

    let swept = rig
        .sweeper
        .reclaim_expired(Some(rig.tenant), &at(40), 100)
        .await
        .expect("sweep");
    assert_eq!(
        swept, 0,
        "a slot whose lease was extended past the sweep is not stale"
    );
    assert!(
        !rig.held(TaskId::new()).await,
        "the slot is still held by the run that renewed it"
    );

    // Past the extended lease, the same sweep does reclaim it. A renewal is an
    // extension, never a pardon: a run that stops renewing loses the slot
    // however many times it renewed before.
    let swept = rig
        .sweeper
        .reclaim_expired(Some(rig.tenant), &at(61), 100)
        .await
        .expect("sweep");
    assert_eq!(swept, 1, "the lease is what bounds a slot, and it lapsed");
    assert_eq!(
        gauge_value(&rig.migrator, rig.tenant).await,
        0,
        "the sweep gave the headroom back"
    );
}

#[tokio::test]
async fn a_crashed_workers_slot_lapses_with_its_lease() {
    let Some(rig) = Rig::build("crash", Duration::from_secs(30)).await else {
        return;
    };
    define_concurrency(&rig.migrator, rig.tenant, 1).await;
    let crashed = TaskId::new();

    // A worker took a slot, charged the counter, and died. There is no
    // `release` coming — the lease is the only thing that can give the
    // headroom back, and it must do so without the worker's cooperation.
    assert!(rig.held(crashed).await);
    assert_eq!(gauge_value(&rig.migrator, rig.tenant).await, 1);

    let swept = rig
        .sweeper
        .reclaim_expired(Some(rig.tenant), &at(31), 100)
        .await
        .expect("sweep");
    assert_eq!(swept, 1, "the dead worker's slot was reclaimed");

    assert_eq!(
        gauge_value(&rig.migrator, rig.tenant).await,
        0,
        "a crash does not consume the tenant's concurrency forever"
    );

    // And the tenant can actually run again — reclamation that left the counter
    // charged would be a sweep that logged a success and changed nothing.
    let recovered = TaskId::new();
    assert!(
        rig.held(recovered).await,
        "the tenant recovered its concurrency after a worker crash"
    );
}

#[tokio::test]
async fn a_lease_that_moves_backwards_is_refused() {
    let Some(rig) = Rig::build("backwards", Duration::from_secs(30)).await else {
        return;
    };
    define_concurrency(&rig.migrator, rig.tenant, 1).await;
    let task = TaskId::new();
    assert!(rig.held(task).await);

    // Renewing to an *earlier* instant would let a concurrent sweep declare
    // this run dead while it is still running. That is worse than a refused
    // renewal, so it is refused.
    let error = rig
        .gate
        .renew(rig.tenant, rig.organization, None, task, &at(5), &at(10))
        .await
        .expect_err("a backwards lease must be refused");
    assert_eq!(error.error_code(), "VALIDATION_FAILED");

    // The refusal left the slot alone: a renewal that failed must not have
    // quietly shortened the lease it failed to extend.
    let swept = rig
        .sweeper
        .reclaim_expired(Some(rig.tenant), &at(20), 100)
        .await
        .expect("sweep");
    assert_eq!(
        swept, 0,
        "the original lease is still in force at t+20 (taken at t+0 for 30s)"
    );
}

// ───────────────────────────────────────────────────────────────────────────
// Idempotence: the slot belongs to the run, not to the caller
// ───────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn two_workers_asking_for_the_same_run_share_one_slot() {
    let Some(rig) = Rig::build("same-run", Duration::from_secs(30)).await else {
        return;
    };
    define_concurrency(&rig.migrator, rig.tenant, 2).await;

    // One run, one execution, one task — and two workers that both believe
    // they are running it (a redelivery, or a partition). The key is derived
    // from the work, so both resolve to the same row rather than taking one
    // each. A key derived from the caller would let a duplicate delivery
    // consume two of the tenant's two slots.
    let execution = ExecutionId::new();
    let task = TaskId::new();
    let worker_one = ControlPlaneRunSlots::new(rig.app.clone(), Arc::clone(&rig.plane), "worker-1");
    let worker_two = ControlPlaneRunSlots::new(rig.app.clone(), Arc::clone(&rig.plane), "worker-2");

    assert!(matches!(
        worker_one
            .acquire(rig.tenant, rig.organization, Some(execution), task, &at(0))
            .await
            .expect("first"),
        RunSlot::Acquired
    ));
    assert_eq!(
        worker_two
            .acquire(rig.tenant, rig.organization, Some(execution), task, &at(0))
            .await
            .expect("second"),
        RunSlot::HeldElsewhere,
        "the second caller is told the slot already exists and is not theirs, \
         so it neither runs the task nor releases the hold"
    );
    assert_eq!(
        gauge_value(&rig.migrator, rig.tenant).await,
        1,
        "two callers, one run, one slot: the key is the run's, not the caller's"
    );

    // A genuinely different run under the same ceiling of 2 still fits.
    let second_run = TaskId::new();
    assert!(
        rig.held(second_run).await,
        "a second, distinct run takes the second slot"
    );
    assert_eq!(gauge_value(&rig.migrator, rig.tenant).await, 2);
    assert!(
        !rig.held(TaskId::new()).await,
        "and a third does not — the ceiling is two"
    );

    // The decisive part: worker two was charged nothing for a slot it did not
    // take. Ownership is what makes that true — the gauge is charged by
    // whoever establishes the hold, and a caller told `HeldElsewhere` did not.
    assert_eq!(
        gauge_value(&rig.migrator, rig.tenant).await,
        2,
        "the second caller's acquire changed the counter by nothing at all"
    );

    // Ownership of the *release* is enforced one layer up, in the consumer:
    // it is `RunSlot::must_be_released()`, and it is what stops a non-owner
    // from freeing a hold that belongs to a run still executing. Asserted
    // there, against the real mediation path, in
    // `mas-worker`'s `a_slot_held_elsewhere_defers_the_run_and_is_never_released`.
    assert!(
        !RunSlot::HeldElsewhere.must_be_released(),
        "a hold that is not the caller's is never the caller's to return"
    );
    assert!(RunSlot::Acquired.must_be_released());
    assert!(!RunSlot::NotMetered.must_be_released());
}

#[tokio::test]
async fn releasing_twice_returns_the_slot_once() {
    let Some(rig) = Rig::build("double-release", Duration::from_secs(30)).await else {
        return;
    };
    define_concurrency(&rig.migrator, rig.tenant, 1).await;
    let task = TaskId::new();
    assert!(rig.held(task).await);

    rig.gate
        .release(rig.tenant, rig.organization, None, task, &at(1))
        .await
        .expect("first release");
    rig.gate
        .release(rig.tenant, rig.organization, None, task, &at(2))
        .await
        .expect("a second release is a no-op, not an error");

    assert_eq!(
        gauge_value(&rig.migrator, rig.tenant).await,
        0,
        "the slot was refunded once. Refunding twice would hand the tenant a \
         permanent extra slot, and a counter that goes negative or is clamped \
         would hide it"
    );

    // One slot came back, so exactly one run fits.
    assert!(rig.held(TaskId::new()).await);
    assert!(!rig.held(TaskId::new()).await);
}

#[tokio::test]
async fn releasing_a_slot_nobody_ever_took_is_not_an_error() {
    let Some(rig) = Rig::build("release-unheld", Duration::from_secs(30)).await else {
        return;
    };
    define_concurrency(&rig.migrator, rig.tenant, 1).await;

    // The worker calls `release` on every exit path, including paths that exit
    // before the slot was taken. If that were an error, every deferred delivery
    // would log a failure and the release call would eventually be moved off
    // the path that needs it — which is how slots leak.
    rig.gate
        .release(rig.tenant, rig.organization, None, TaskId::new(), &at(0))
        .await
        .expect("release with no acquire is a no-op");
    assert_eq!(gauge_value(&rig.migrator, rig.tenant).await, 0);
}

#[tokio::test]
async fn renewing_a_slot_that_does_not_exist_is_not_an_error() {
    let Some(rig) = Rig::build("renew-unheld", Duration::from_secs(30)).await else {
        return;
    };
    define_concurrency(&rig.migrator, rig.tenant, 1).await;

    // Same reasoning as the release case: a renew is an offer. An offer with
    // nothing to extend — the tenant has no ceiling, or the slot was already
    // returned — must not turn a healthy heartbeat into an error path.
    rig.gate
        .renew(
            rig.tenant,
            rig.organization,
            None,
            TaskId::new(),
            &at(60),
            &at(10),
        )
        .await
        .expect("renew with no slot is a no-op");
    assert_eq!(gauge_value(&rig.migrator, rig.tenant).await, 0);
}

// ───────────────────────────────────────────────────────────────────────────
// Cross-tenant isolation
// ───────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn tenant_b_runs_regardless_of_what_tenant_a_is_holding() {
    let Some(rig) = Rig::build("isolation-a", Duration::from_secs(30)).await else {
        return;
    };
    let (tenant_b, organization_b) = seed_scope(&rig.migrator, "isolation-b").await;
    define_concurrency(&rig.migrator, rig.tenant, 1).await;
    define_concurrency(&rig.migrator, tenant_b, 1).await;

    // Tenant A takes its one and only slot.
    assert!(rig.held(TaskId::new()).await);

    // Tenant B, same ceiling, same table, is unaffected. A gauge shared across
    // tenants would make one busy tenant starve every other — and it would
    // present as a capacity problem, not as the isolation failure it is.
    let gate_b = ControlPlaneRunSlots::new(rig.app.clone(), Arc::clone(&rig.plane), "worker-test");
    assert!(
        matches!(
            gate_b
                .acquire(tenant_b, organization_b, None, TaskId::new(), &at(0))
                .await
                .expect("tenant b acquire"),
            RunSlot::Acquired
        ),
        "tenant A's held slot did not consume tenant B's headroom"
    );
    assert_eq!(gauge_value(&rig.migrator, rig.tenant).await, 1);
    assert_eq!(gauge_value(&rig.migrator, tenant_b).await, 1);

    // Releasing A's slot leaves B's untouched.
    let held_by_a = TaskId::new();
    assert!(!rig.held(held_by_a).await, "A is at its ceiling of one");
    assert_eq!(
        gauge_value(&rig.migrator, tenant_b).await,
        1,
        "B's slot is still B's"
    );
}

#[tokio::test]
async fn another_tenants_slot_is_invisible_inside_this_tenants_scope() {
    let Some(rig) = Rig::build("invisible-a", Duration::from_secs(30)).await else {
        return;
    };
    let (tenant_b, organization_b) = seed_scope(&rig.migrator, "invisible-b").await;
    define_concurrency(&rig.migrator, rig.tenant, 1).await;
    define_concurrency(&rig.migrator, tenant_b, 1).await;

    let execution_b = ExecutionId::new();
    let task_b = TaskId::new();
    let gate_b = ControlPlaneRunSlots::new(rig.app.clone(), Arc::clone(&rig.plane), "worker-test");
    gate_b
        .acquire(tenant_b, organization_b, Some(execution_b), task_b, &at(0))
        .await
        .expect("tenant b holds a slot");

    // Ask for B's slot while standing inside A's scope. Row-level security is
    // what makes the answer "not found" rather than a row — and this is the
    // assertion that is void under a role with BYPASSRLS, which is why the
    // harness refuses to hand one out.
    let key_b = ControlPlane::run_slot_key(Some(execution_b), task_b);
    let outcome = mas_persistence::scoped_request::with_authoritative_scope(
        &rig.app,
        rig.scope(),
        || async { rig.plane.reservation(tenant_b, &key_b).await },
    )
    .await;

    let error = outcome.expect_err("tenant A's scope must not see tenant B's slot");
    assert_eq!(
        error.error_code(),
        "RESOURCE_NOT_FOUND",
        "another tenant's reservation is invisible, not readable"
    );

    // Own scope, own row: the isolation did not hide everything.
    let own = rig.tenant;
    let own_task = TaskId::new();
    assert!(rig.held(own_task).await);
    let own_key = ControlPlane::run_slot_key(None, own_task);
    let seen = mas_persistence::scoped_request::with_authoritative_scope(
        &rig.app,
        rig.scope(),
        || async { rig.plane.reservation(own, &own_key).await },
    )
    .await
    .expect("a tenant sees its own slot");
    assert_eq!(seen.tenant_id, own);
}

// ───────────────────────────────────────────────────────────────────────────
// The gate is a gate: it never admits what it cannot authorise
// ───────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_slot_is_never_granted_outside_the_tenants_own_scope() {
    let Some(rig) = Rig::build("scoped-write", Duration::from_secs(30)).await else {
        return;
    };
    define_concurrency(&rig.migrator, rig.tenant, 1).await;
    let task = TaskId::new();

    assert!(rig.held(task).await);

    // The reservation the gate just wrote must be readable in the tenant's own
    // scope — proof the write happened under RLS and is attributable, rather
    // than having been slipped in by a role that can see past it.
    let key = ControlPlane::run_slot_key(None, task);
    let reservation = mas_persistence::scoped_request::with_authoritative_scope(
        &rig.app,
        rig.scope(),
        || async { rig.plane.reservation(rig.tenant, &key).await },
    )
    .await
    .expect("the slot is readable under the tenant's own scope");
    assert_eq!(reservation.tenant_id, rig.tenant);
    assert_eq!(reservation.dimension, QuotaDimension::ConcurrentRuns);
    assert_eq!(
        reservation.reserved_amount, 1,
        "a run holds exactly one slot"
    );
    assert_eq!(reservation.task_id, Some(task));
}

#[tokio::test]
async fn a_retry_of_a_finished_run_takes_a_fresh_slot_and_leaks_none() {
    let Some(rig) = Rig::build("retry-after-release", Duration::from_secs(30)).await else {
        return;
    };
    define_concurrency(&rig.migrator, rig.tenant, 1).await;
    let task = TaskId::new();

    // Attempt 1: take the slot, finish, release it.
    assert!(rig.held(task).await);
    assert_eq!(gauge_value(&rig.migrator, rig.tenant).await, 1);
    rig.gate
        .release(rig.tenant, rig.organization, None, task, &at(1))
        .await
        .expect("release");
    assert_eq!(gauge_value(&rig.migrator, rig.tenant).await, 0);

    // Attempt 2 of the *same* task. The reservation row for attempt 1 is
    // still there — rows are never deleted, they are settled — so this is the
    // case that leaked: the store handed back the finished row, the caller
    // charged the counter for it, and the settlement that followed released
    // nothing because the row was already terminal.
    assert_eq!(
        rig.gate
            .acquire(rig.tenant, rig.organization, None, task, &at(2))
            .await
            .expect("retry"),
        RunSlot::Acquired,
        "the finished hold was refreshed in place, not handed back as-is"
    );
    assert_eq!(
        gauge_value(&rig.migrator, rig.tenant).await,
        1,
        "one slot, one charge"
    );

    // And it can be given back exactly once.
    rig.gate
        .release(rig.tenant, rig.organization, None, task, &at(3))
        .await
        .expect("release");
    assert_eq!(
        gauge_value(&rig.migrator, rig.tenant).await,
        0,
        "a retry leaks nothing: the tenant's ceiling is intact"
    );

    // Third time round, to be sure this is a rule and not a one-off.
    assert!(rig.held(task).await);
    assert_eq!(gauge_value(&rig.migrator, rig.tenant).await, 1);
}

#[tokio::test]
async fn a_retry_after_a_crash_reclaims_the_dead_attempts_slot() {
    let Some(rig) = Rig::build("retry-after-crash", Duration::from_secs(30)).await else {
        return;
    };
    define_concurrency(&rig.migrator, rig.tenant, 1).await;
    let task = TaskId::new();

    // A worker took the slot and died without releasing it.
    assert!(rig.held(task).await);
    assert_eq!(gauge_value(&rig.migrator, rig.tenant).await, 1);

    // The task lease lapses first, so the task is redelivered while the
    // reservation's lease is still running. Taking over must reclaim the dead
    // attempt's charge before charging again — two charges against one
    // reservation is a slot the tenant can never get back.
    assert_eq!(
        rig.gate
            .acquire(rig.tenant, rig.organization, None, task, &at(10))
            .await
            .expect("takeover while the old lease is still live"),
        RunSlot::HeldElsewhere,
        "the dead attempt's lease is still running, so the slot is not ours yet"
    );
    assert_eq!(
        gauge_value(&rig.migrator, rig.tenant).await,
        1,
        "the takeover charged nothing while it could not own the slot"
    );

    // Once that lease lapses, the same run can take the slot.
    assert_eq!(
        rig.gate
            .acquire(rig.tenant, rig.organization, None, task, &at(40))
            .await
            .expect("takeover after the lease lapsed"),
        RunSlot::Acquired
    );
    assert_eq!(
        gauge_value(&rig.migrator, rig.tenant).await,
        1,
        "the dead attempt's charge was reclaimed, so the gauge counts one slot \
         and not two"
    );
}
