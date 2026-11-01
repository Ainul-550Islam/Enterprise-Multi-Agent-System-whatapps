//! PURPOSE: The live-PostgreSQL suite for the enterprise control plane.
//!
//! Owning layer: `mas-persistence` (tests).
//!
//! Production responsibility: prove against a real database the things an
//! in-memory store cannot prove at all — that two concurrent reservations
//! cannot both win, that duplicate work is metered exactly once, that a
//! crashed process does not consume quota forever, that a replayed billing
//! webhook applies once, and that tenant A cannot see tenant B's subscription,
//! usage, quota or billing data.
//!
//! Security boundary: the isolation assertions run on `mas_app`, a role with
//! `rolbypassrls = false`. Under the migrator URL every one of them would pass
//! vacuously, because a superuser bypasses the policies being tested.
//!
//! It must NOT:
//!   • run without `MAS_TEST_DATABASE_URL` (it skips cleanly instead);
//!   • be pointed at a production database;
//!   • weaken an isolation or atomicity assertion to accommodate a fixture.

//! Live PostgreSQL integration suite for the control plane.
//!
//! Gated by `MAS_TEST_DATABASE_URL` (migrator role) and
//! `MAS_TEST_DATABASE_APP_URL` (runtime role, no BYPASSRLS). The second is
//! required for the isolation assertions; without it those tests print a
//! banner and skip rather than passing vacuously.
//!
//! NEVER run against a production database.

mod support;

use mas_common::ids::{ExecutionId, TaskId, TenantId};
use mas_common::timestamps::Timestamp;
use mas_domain::entitlement::FeatureKey;
use mas_domain::overrides::{ActorKind, EntitlementOverride, OverrideActor, QuotaOverride};
use mas_domain::plan::{Plan, PlanEntitlement, PlanQuota};
use mas_domain::quota::{EnforcementMode, Quota, QuotaDimension, QuotaPeriod};
use mas_domain::reservation::ReservationState;
use mas_domain::subscription::{Subscription, SubscriptionState};
use mas_persistence::repositories::control_plane::{
    ControlPlaneStores, PostgresCounterStore, PostgresPolicyStore, ReceiptState, UsageWrite,
};
use mas_policy::store::PolicyStorePort;
use mas_quota::counter::{CounterKey, CounterStorePort};
use mas_quota::engine::QuotaRegistryPort;
use mas_security::AuthoritativeScope;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;

/// Seeds a fresh tenant (plus its organization and project) through the
/// migrator pool.
///
/// Every test seeds its own tenant rather than sharing a fixture, so no test's
/// assertions depend on another test's counter or usage rows.
async fn seed_scope(pool: &sqlx::PgPool, label: &str) -> TenantId {
    use mas_common::enums::Environment;
    use mas_domain::value_objects::Slug;
    use mas_domain::{IsolationMode, Organization, Project, Tenant};
    use mas_persistence::repositories::{OrganizationStore, ProjectStore, TenantStore};

    let mut connection = pool.acquire().await.expect("connection");
    let suffix = uuid::Uuid::now_v7().to_string().replace('-', "");

    let organization = Organization::create(
        format!("Acme {label}"),
        format!("Acme ({label})"),
        Slug::new(&format!("acme-{label}-{suffix}")).expect("org slug"),
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

    tenant_id
}

/// Two tenants seeded through the migrator pool.
async fn two_tenants(pool: &sqlx::PgPool, label: &str) -> (TenantId, TenantId) {
    let a = seed_scope(pool, &format!("{label}-a")).await;
    let b = seed_scope(pool, &format!("{label}-b")).await;
    (a, b)
}

/// SHA-256 hex, standing in for a provider's signed-payload digest.
fn sha256_hex(input: &[u8]) -> String {
    let digest = Sha256::digest(input);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn at(seconds: i64) -> Timestamp {
    Timestamp::from_unix_seconds(seconds).expect("timestamp")
}

// ───────────────────────────────────────────────────────────────────────────
// Phase 6 + 7 — distributed counters and atomic reservation
// ───────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn two_replicas_cannot_together_exceed_one_ceiling() {
    let Some(pool) = support::live_pool("cp-dq").await else {
        return;
    };
    let tenant = seed_scope(&pool, "cp-dq").await;
    let now = at(1_700_000_000);
    let key = CounterKey::for_time(tenant, QuotaDimension::Tokens, Some(3_600), &now);
    let counters = Arc::new(PostgresCounterStore::new(pool.clone()));

    // The scenario the spec names, driven as two concurrent callers on
    // independent connections: replica A reserves 70, replica B reserves 50,
    // the ceiling is 100. 70 + 50 = 120, so exactly one of them may win.
    let a = {
        let counters = Arc::clone(&counters);
        tokio::spawn(async move { counters.try_reserve(key, 70, 100).await })
    };
    let b = {
        let counters = Arc::clone(&counters);
        tokio::spawn(async move { counters.try_reserve(key, 50, 100).await })
    };
    let (a, b) = (
        a.await.expect("task a").expect("reserve a"),
        b.await.expect("task b").expect("reserve b"),
    );
    assert!(
        !(a.allowed && b.allowed),
        "70 and 50 together are 120; the ceiling is 100, so both must never be granted"
    );
    assert!(a.allowed || b.allowed, "one of them should have fit");
    let held = counters.get(key).await.expect("counter");
    assert_eq!(
        held,
        if a.allowed { 70 } else { 50 },
        "the counter holds exactly what was granted"
    );
}

/// The invariant, stated over a storm of concurrent callers.
///
/// The two-caller test above proves the named case. This one proves the
/// general property: however many callers race, the total granted never
/// exceeds the ceiling.
///
/// Note what is *not* asserted: that exactly one caller wins. With amounts of
/// 70 and 50 and a ceiling of 100, two 50s may both be granted — 50 + 50
/// fills the ceiling exactly and there is nothing illegal about it. An earlier
/// version of this test asserted "exactly one" and was therefore flaky for a
/// reason that had nothing to do with the implementation: it was asserting
/// something stronger than the guarantee. The guarantee is the sum.
#[tokio::test]
async fn a_concurrent_storm_never_breaches_the_ceiling() {
    let Some(pool) = support::live_pool("cp-dq-storm").await else {
        return;
    };
    let tenant = seed_scope(&pool, "cp-dq-storm").await;
    let now = at(1_700_000_000);
    let key = CounterKey::for_time(tenant, QuotaDimension::Tokens, Some(3_600), &now);
    let counters = Arc::new(PostgresCounterStore::new(pool.clone()));

    let mut tasks = Vec::new();
    for index in 0..24 {
        let counters = Arc::clone(&counters);
        let amount = if index % 2 == 0 { 70 } else { 50 };
        tasks.push(tokio::spawn(async move {
            counters.try_reserve(key, amount, 100).await
        }));
    }
    let mut granted_count = 0usize;
    for task in tasks {
        if task.await.expect("task").expect("reserve").allowed {
            granted_count += 1;
        }
    }

    let held = counters.get(key).await.expect("counter");
    assert!(
        held <= 100,
        "the counter must never exceed the ceiling, got {held}"
    );
    assert!(
        granted_count <= 2,
        "at most two callers can fit under 100 in amounts of 70 and 50, \
         got {granted_count}"
    );
    assert!(
        held == 50 || held == 70 || held == 100,
        "the counter is a sum of granted amounts of 70 and 50, got {held}"
    );
}

#[tokio::test]
async fn a_refused_reservation_does_not_move_the_counter() {
    let Some(pool) = support::live_pool("cp-refused-clean").await else {
        return;
    };
    let tenant = seed_scope(&pool, "cp-refused-clean").await;
    let counters = PostgresCounterStore::new(pool.clone());
    let now = at(1_700_000_000);
    let key = CounterKey::for_time(tenant, QuotaDimension::ApiCalls, Some(60), &now);

    let first = counters.try_reserve(key, 8, 10).await.expect("first");
    assert!(first.allowed && first.value == 8);
    let second = counters.try_reserve(key, 5, 10).await.expect("second");
    assert!(!second.allowed, "8 + 5 exceeds 10");
    assert_eq!(
        counters.get(key).await.expect("counter"),
        8,
        "the refusal wrote nothing"
    );
}

#[tokio::test]
async fn a_counter_never_goes_negative_on_a_double_release() {
    let Some(pool) = support::live_pool("cp-saturating").await else {
        return;
    };
    let tenant = seed_scope(&pool, "cp-saturating").await;
    let counters = PostgresCounterStore::new(pool.clone());
    let now = at(1_700_000_000);
    let key = CounterKey::for_time(tenant, QuotaDimension::ConcurrentRuns, None, &now);

    counters.increment(key, 3).await.expect("incr");
    counters.decrement(key, 3).await.expect("decr");
    counters.decrement(key, 3).await.expect("decr again");
    assert_eq!(
        counters.get(key).await.expect("counter"),
        0,
        "a double release must saturate, never mint headroom"
    );
}

// ───────────────────────────────────────────────────────────────────────────
// Phase 8 — reservation lifecycle and crash recovery
// ───────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_reservation_is_idempotent_settles_once_and_releases_the_rest() {
    let Some(pool) = support::live_pool("cp-reservation").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let tenant = seed_scope(&pool, "cp-reservation").await;
    let now = at(1_700_000_000);
    let later = at(1_700_000_500);
    let lease = at(1_700_000_900);
    let key = format!("reserve-{}", uuid::Uuid::new_v4());

    let first = stores
        .reservations
        .reserve(
            tenant,
            QuotaDimension::Tokens,
            0,
            100,
            &key,
            Some(ExecutionId::new()),
            Some(TaskId::new()),
            &lease,
            &now,
        )
        .await
        .expect("reserve");
    assert_eq!(first.state, ReservationState::Held);
    assert_eq!(first.reserved_amount, 100);

    // A replay of the same logical work returns the original reservation.
    let replay = stores
        .reservations
        .reserve(
            tenant,
            QuotaDimension::Tokens,
            0,
            100,
            &key,
            None,
            None,
            &lease,
            &now,
        )
        .await
        .expect("replay");
    assert_eq!(replay.id, first.id, "the retry must not reserve twice");

    // Settle 40 of the 100.
    let settlement = stores
        .reservations
        .settle(tenant, &key, 40, &later)
        .await
        .expect("settle");
    assert!(settlement.applied);
    assert_eq!(settlement.consumed, 40);
    assert_eq!(settlement.released, 60, "the unused headroom is returned");

    // A duplicate completion is recognised: no second metering, no second
    // refund. This is what stops a redelivered broker message from both
    // billing the work and crediting it.
    let duplicate = stores
        .reservations
        .settle(tenant, &key, 40, &later)
        .await
        .expect("duplicate settle");
    assert!(!duplicate.applied, "a duplicate must not settle twice");
    assert_eq!(duplicate.released, 0, "and must not refund twice");
}

#[tokio::test]
async fn a_reservation_that_outlives_its_lease_is_reclaimable() {
    let Some(pool) = support::live_pool("cp-crash-recovery").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let tenant = seed_scope(&pool, "cp-crash-recovery").await;
    let now = at(1_700_000_000);
    let lease = at(1_700_000_010);
    let after_lease = at(1_700_000_100);
    let key = format!("crash-{}", uuid::Uuid::new_v4());

    stores
        .reservations
        .reserve(
            tenant,
            QuotaDimension::Executions,
            0,
            10,
            &key,
            None,
            None,
            &lease,
            &now,
        )
        .await
        .expect("reserve");

    // Before the lease lapses: nothing to reclaim. A sweeper that ran early
    // must not steal work that is still legitimately in flight.
    let early = stores
        .reservations
        .reclaim_expired(Some(tenant), &now, 100)
        .await
        .expect("early sweep");
    assert!(early.is_empty(), "an un-expired lease is not reclaimable");

    // After the lease lapses: reclaimed, and the amount is reported so the
    // caller can give the headroom back.
    let reclaimed = stores
        .reservations
        .reclaim_expired(Some(tenant), &after_lease, 100)
        .await
        .expect("sweep");
    assert_eq!(reclaimed.len(), 1, "the lapsed reservation is reclaimed");
    assert_eq!(reclaimed[0].1, 10, "and the full amount is returned");
    assert_eq!(reclaimed[0].0.state, ReservationState::Expired);

    // A second sweep finds nothing: reclaiming twice would refund twice.
    let again = stores
        .reservations
        .reclaim_expired(Some(tenant), &after_lease, 100)
        .await
        .expect("second sweep");
    assert!(again.is_empty(), "reclaiming is idempotent");
}

#[tokio::test]
async fn a_settled_reservation_is_never_reclaimed() {
    let Some(pool) = support::live_pool("cp-no-double-reclaim").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let tenant = seed_scope(&pool, "cp-no-double-reclaim").await;
    let now = at(1_700_000_000);
    let lease = at(1_700_000_010);
    let after_lease = at(1_700_000_100);
    let key = format!("settled-{}", uuid::Uuid::new_v4());

    stores
        .reservations
        .reserve(
            tenant,
            QuotaDimension::Tokens,
            0,
            50,
            &key,
            None,
            None,
            &lease,
            &now,
        )
        .await
        .expect("reserve");
    stores
        .reservations
        .settle(tenant, &key, 50, &now)
        .await
        .expect("settle");

    let reclaimed = stores
        .reservations
        .reclaim_expired(Some(tenant), &after_lease, 100)
        .await
        .expect("sweep");
    assert!(
        reclaimed.is_empty(),
        "reclaiming a settled reservation would refund work that was already metered"
    );
}

// ───────────────────────────────────────────────────────────────────────────
// Phase 10 + 11 — the usage ledger and exactly-once metering
// ───────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn the_same_task_settled_twice_produces_one_usage_record() {
    let Some(pool) = support::live_pool("cp-exact-once-task").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let tenant = seed_scope(&pool, "cp-exact-once-task").await;
    let execution = ExecutionId::new();
    let task = TaskId::new();
    let now = at(1_700_000_000);

    let write = mas_persistence::repositories::control_plane::UsageWrite {
        tenant_id: tenant,
        organization_id: None,
        project_id: None,
        dimension: QuotaDimension::Tokens,
        quantity: 250,
        execution_id: Some(execution),
        task_id: Some(task),
        task_attempt_id: None,
        model: Some("model-a".to_owned()),
        provider: Some("provider-a".to_owned()),
        input_quantity: Some(100),
        output_quantity: Some(150),
        duration_ms: Some(1_250),
        cost_micros: Some(4_500),
        cost_currency: Some("USD".to_owned()),
        estimated: true,
        dimensions: serde_json::json!({}),
        metering_key: None,
        occurred_at: now,
    };

    assert!(
        stores.usage.record(&write).await.expect("first"),
        "the first record is written"
    );
    assert!(
        !stores.usage.record(&write).await.expect("second"),
        "the same task and dimension must not be metered twice"
    );

    let rows = stores
        .usage
        .for_execution(tenant, execution)
        .await
        .expect("rows");
    assert_eq!(rows.len(), 1, "exactly one usage record exists");
    assert_eq!(rows[0].quantity, 250);
    assert_eq!(rows[0].input_quantity, Some(100));
    assert_eq!(rows[0].output_quantity, Some(150));
    assert!(rows[0].estimated, "model-reported usage stays flagged");
}

#[tokio::test]
async fn a_metering_key_makes_taskless_usage_exactly_once() {
    let Some(pool) = support::live_pool("cp-exact-once-key").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let tenant = seed_scope(&pool, "cp-exact-once-key").await;
    let key = format!("meter-{}", uuid::Uuid::new_v4());
    let now = at(1_700_000_000);

    let write = mas_persistence::repositories::control_plane::UsageWrite {
        tenant_id: tenant,
        organization_id: None,
        project_id: None,
        dimension: QuotaDimension::ApiCalls,
        quantity: 1,
        execution_id: None,
        task_id: None,
        task_attempt_id: None,
        model: None,
        provider: None,
        input_quantity: None,
        output_quantity: None,
        duration_ms: None,
        cost_micros: None,
        cost_currency: None,
        estimated: false,
        dimensions: serde_json::json!({}),
        metering_key: Some(key.clone()),
        occurred_at: now,
    };
    assert!(stores.usage.record(&write).await.expect("first"));
    assert!(!stores.usage.record(&write).await.expect("second"));

    // Usage with neither a task nor a metering key is refused outright.
    let mut orphan = write.clone();
    orphan.metering_key = None;
    assert!(
        stores.usage.record(&orphan).await.is_err(),
        "un-keyable usage would be impossible to de-duplicate"
    );
}

#[tokio::test]
async fn a_zero_quantity_is_refused_rather_than_recorded() {
    let Some(pool) = support::live_pool("cp-zero-usage").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let tenant = seed_scope(&pool, "cp-zero-usage").await;
    let write = mas_persistence::repositories::control_plane::UsageWrite {
        tenant_id: tenant,
        organization_id: None,
        project_id: None,
        dimension: QuotaDimension::Tokens,
        quantity: 0,
        execution_id: None,
        task_id: Some(TaskId::new()),
        task_attempt_id: None,
        model: None,
        provider: None,
        input_quantity: None,
        output_quantity: None,
        duration_ms: None,
        cost_micros: None,
        cost_currency: None,
        estimated: false,
        dimensions: serde_json::json!({}),
        metering_key: None,
        occurred_at: at(1_700_000_000),
    };
    assert!(
        stores.usage.record(&write).await.is_err(),
        "'no usage' and 'usage of zero' must stay distinguishable"
    );
}

#[tokio::test]
async fn usage_totals_refuse_to_add_mixed_currencies() {
    let Some(pool) = support::live_pool("cp-mixed-currency").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let tenant = seed_scope(&pool, "cp-mixed-currency").await;
    let execution = ExecutionId::new();
    let now = at(1_700_000_000);

    for (quantity, currency, task) in [(10_i64, "USD", 1), (20_i64, "EUR", 2)] {
        let write = mas_persistence::repositories::control_plane::UsageWrite {
            tenant_id: tenant,
            organization_id: None,
            project_id: None,
            dimension: QuotaDimension::Tokens,
            quantity: u64::try_from(quantity).expect("positive"),
            execution_id: Some(execution),
            task_id: Some(TaskId::new()),
            task_attempt_id: None,
            model: None,
            provider: None,
            input_quantity: None,
            output_quantity: None,
            duration_ms: None,
            cost_micros: Some(1_000),
            cost_currency: Some(currency.to_owned()),
            estimated: false,
            dimensions: serde_json::json!({}),
            metering_key: None,
            occurred_at: now,
        };
        let _ = task;
        stores.usage.record(&write).await.expect("record");
    }

    let totals = stores
        .usage
        .totals_for_tenant_between(tenant, &at(1_600_000_000), &at(1_800_000_000))
        .await
        .expect("totals");
    let tokens = totals
        .iter()
        .find(|total| total.resource == "tokens")
        .expect("tokens total");
    assert_eq!(tokens.quantity, 30, "quantities are currency-independent");
    assert_eq!(
        tokens.cost(),
        None,
        "adding micro-units of two currencies would produce a meaningless total"
    );
}

// ───────────────────────────────────────────────────────────────────────────
// Phase 13 + 14 — plans and the subscription lifecycle
// ───────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_plan_is_data_and_resolves_to_its_grants() {
    let Some(pool) = support::live_pool("cp-plan").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let code = format!("plan_{}", uuid::Uuid::new_v4().simple());

    let plan = stores
        .plans
        .create(&Plan::new(&code, "Test Plan", None).expect("plan"))
        .await
        .expect("create");
    stores
        .plans
        .grant_feature(
            &PlanEntitlement::grant(plan.id, FeatureKey::AUDIT_EXPORT, Some(7)).expect("grant"),
        )
        .await
        .expect("grant feature");
    stores
        .plans
        .set_quota(
            &PlanQuota::new(
                plan.id,
                QuotaDimension::Tokens,
                QuotaPeriod::Monthly,
                5_000,
                EnforcementMode::Enforce,
            )
            .expect("quota"),
        )
        .await
        .expect("set quota");

    let resolved = stores.plan_resolver.resolve(&code).await.expect("resolve");
    assert_eq!(resolved.code(), code);
    assert_eq!(
        resolved.features.get(&FeatureKey::AUDIT_EXPORT),
        Some(&Some(7)),
        "the plan row, not a compiled-in constant, is what grants the feature"
    );
    assert!(
        resolved
            .quota_for(&QuotaDimension::Tokens, &QuotaPeriod::Monthly)
            .is_some(),
        "the plan's metering ceiling resolves too"
    );

    // The PlanMatrixPort is what the entitlement resolver actually reads.
    let grant = mas_tenancy::entitlements::PlanMatrixPort::features_for_plan(&stores.plans, &code)
        .await
        .expect("matrix")
        .expect("grant");
    assert!(grant.features.contains(&FeatureKey::AUDIT_EXPORT));
    assert_eq!(grant.default_limits.get("audit_export"), Some(&7));

    // Retiring stops it being sold but does not erase it: historical
    // subscriptions still point at it.
    let retired = stores.plans.retire(plan.id).await.expect("retire");
    assert!(!retired.is_sellable());
    assert!(
        stores
            .plan_resolver
            .resolve(&code)
            .await
            .expect("resolve")
            .plan
            .is_some(),
        "a retired plan still resolves for existing subscribers"
    );
}

#[tokio::test]
async fn a_subscription_transitions_and_every_step_is_recorded() {
    let Some(pool) = support::live_pool("cp-subscription").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let tenant = seed_scope(&pool, "cp-subscription").await;
    let now = at(1_700_000_000);

    let id = stores
        .subscriptions
        .create(
            &Subscription::new(tenant, "team", SubscriptionState::Trialing, None, None)
                .expect("subscription"),
        )
        .await
        .expect("create");
    assert!(
        stores
            .subscriptions
            .get(tenant)
            .await
            .expect("read")
            .is_some(),
        "the new subscription is visible"
    );

    // trialing → active
    let row = stores
        .subscriptions
        .transition(
            tenant,
            SubscriptionState::Active,
            "trial converted",
            "user-1",
            "user",
            None,
            "corr-1",
            1,
            &now,
        )
        .await
        .expect("activate");
    assert_eq!(row.state, "active");
    assert_eq!(row.version, 2, "every transition bumps the version");

    // A stale version loses: two concurrent transitions cannot both win.
    let stale = stores
        .subscriptions
        .transition(
            tenant,
            SubscriptionState::Cancelled,
            "duplicate request",
            "user-2",
            "user",
            None,
            "corr-2",
            1,
            &now,
        )
        .await;
    assert!(
        stale.is_err(),
        "a transition against a stale version must be refused, not silently applied"
    );

    // active → suspended → cancelled, each recorded.
    stores
        .subscriptions
        .transition(
            tenant,
            SubscriptionState::Suspended,
            "payment failed",
            "system",
            "service",
            Some("evt_1"),
            "corr-3",
            2,
            &now,
        )
        .await
        .expect("suspend");
    stores
        .subscriptions
        .transition(
            tenant,
            SubscriptionState::Cancelled,
            "churn",
            "user-1",
            "user",
            None,
            "corr-4",
            3,
            &now,
        )
        .await
        .expect("cancel");

    let history = stores
        .subscriptions
        .history(tenant, 50)
        .await
        .expect("history");
    assert_eq!(
        history.len(),
        3,
        "one history row per accepted transition, in the same transaction"
    );
    assert_eq!(history[0].to_state, "cancelled");
    assert_eq!(history[0].from_state.as_deref(), Some("suspended"));
    assert_eq!(history[0].actor_id, "user-1");
    assert_eq!(
        history[1].external_event_id.as_deref(),
        Some("evt_1"),
        "a provider-caused transition says so"
    );
    let _ = id;
}

#[tokio::test]
async fn an_illegal_subscription_transition_is_refused() {
    let Some(pool) = support::live_pool("cp-illegal-transition").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let tenant = seed_scope(&pool, "cp-illegal-transition").await;
    let now = at(1_700_000_000);

    stores
        .subscriptions
        .create(
            &Subscription::new(tenant, "team", SubscriptionState::Trialing, None, None)
                .expect("subscription"),
        )
        .await
        .expect("create");

    // trialing → suspended is not a legal edge in the domain state machine.
    let result = stores
        .subscriptions
        .transition(
            tenant,
            SubscriptionState::Suspended,
            "nope",
            "user-1",
            "user",
            None,
            "corr",
            1,
            &now,
        )
        .await;
    assert!(result.is_err(), "the domain state machine owns legality");
    assert_eq!(
        stores
            .subscriptions
            .history(tenant, 50)
            .await
            .expect("history")
            .len(),
        0,
        "a refused transition writes no history"
    );
}

// ───────────────────────────────────────────────────────────────────────────
// Phase 20 — enterprise overrides
// ───────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn an_override_changes_the_effective_limit_without_touching_the_plan() {
    let Some(pool) = support::live_pool("cp-override").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let tenant = seed_scope(&pool, "cp-override").await;
    let now = at(1_700_000_000);
    let later = at(1_700_010_000);
    let actor = OverrideActor::new(ActorKind::User, "account-manager-1").expect("actor");

    stores
        .overrides
        .grant_feature(
            &EntitlementOverride::new(
                tenant,
                FeatureKey::AUDIT_EXPORT,
                true,
                Some(90),
                "negotiated in the March renewal",
                actor.clone(),
                now,
                Some(later),
            )
            .expect("override"),
        )
        .await
        .expect("grant");

    let effective = stores
        .overrides
        .effective_features(tenant, &now)
        .await
        .expect("effective");
    assert_eq!(effective.len(), 1);
    assert_eq!(effective[0].numeric_limit, Some(90));
    assert_eq!(effective[0].reason, "negotiated in the March renewal");
    assert_eq!(effective[0].actor.id, "account-manager-1");

    // Outside the effective period it does not apply.
    let before = at(1_600_000_000);
    assert!(
        stores
            .overrides
            .effective_features(tenant, &before)
            .await
            .expect("before")
            .is_empty(),
        "an override is not in force before it starts"
    );
    let at_expiry = stores
        .overrides
        .effective_features(tenant, &later)
        .await
        .expect("at expiry");
    assert!(
        at_expiry.is_empty(),
        "the interval is half-open: expiry is not included"
    );

    // Ending it is recorded, and the history keeps the evidence.
    let midway = at(1_700_005_000);
    stores
        .overrides
        .end_feature(tenant, &FeatureKey::AUDIT_EXPORT, &midway)
        .await
        .expect("end");
    // Ending later than the scheduled end would *extend* the grant, and that
    // is refused rather than silently applied.
    let extend = stores
        .overrides
        .end_feature(tenant, &FeatureKey::AUDIT_EXPORT, &at(1_700_999_999))
        .await;
    assert!(
        extend.is_err(),
        "ending an override at a later date would extend the grant, and that \
         is the one outcome an operator must not be able to reach by accident"
    );
    assert!(
        stores
            .overrides
            .effective_features(tenant, &midway)
            .await
            .expect("after end")
            .is_empty(),
        "the override stopped being in force at the moment it was ended"
    );
    assert_eq!(
        stores
            .overrides
            .feature_history(tenant, 50)
            .await
            .expect("history")
            .len(),
        1,
        "the ended override remains in the audit view"
    );

    // Two live overrides for the same target cannot coexist.
    let second = stores
        .overrides
        .grant_feature(
            &EntitlementOverride::new(
                tenant,
                FeatureKey::AUDIT_EXPORT,
                true,
                Some(120),
                "second negotiation",
                actor.clone(),
                later,
                None,
            )
            .expect("override"),
        )
        .await
        .expect("a new override is allowed once the first has ended");
    let _ = second;
    let third = stores
        .overrides
        .grant_feature(
            &EntitlementOverride::new(
                tenant,
                FeatureKey::AUDIT_EXPORT,
                true,
                Some(1),
                "third negotiation",
                actor,
                later,
                None,
            )
            .expect("override"),
        )
        .await;
    assert!(
        third.is_err(),
        "two live overrides would make the effective limit depend on row order"
    );
}

#[tokio::test]
async fn a_quota_override_is_scoped_to_its_tenant_and_period() {
    let Some(pool) = support::live_pool("cp-quota-override").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let (tenant_a, tenant_b) = two_tenants(&pool, "cp-quota-override").await;
    let now = at(1_700_000_000);
    let actor = OverrideActor::new(ActorKind::Service, "provisioning").expect("actor");

    stores
        .overrides
        .grant_quota(
            &QuotaOverride::new(
                tenant_a,
                QuotaDimension::Tokens,
                QuotaPeriod::Monthly,
                999_999,
                EnforcementMode::Enforce,
                "enterprise contract",
                actor,
                now,
                None,
            )
            .expect("override"),
        )
        .await
        .expect("grant");

    let a = stores
        .overrides
        .effective_quotas(tenant_a, &now)
        .await
        .expect("a");
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].max_value, 999_999);

    // Tenant B has none — the override did not leak across the tenant
    // boundary, which is what RLS plus the tenant filter together guarantee.
    let b = stores
        .overrides
        .effective_quotas(tenant_b, &now)
        .await
        .expect("b");
    assert!(b.is_empty(), "tenant B must not see tenant A's override");
}

// ───────────────────────────────────────────────────────────────────────────
// Phase 3 + 4 — durable policies
// ───────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_policy_is_persisted_published_and_versioned() {
    let Some(pool) = support::live_pool("cp-policy").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let tenant = seed_scope(&pool, "cp-policy").await;
    let name = format!("policy-{}", uuid::Uuid::new_v4().simple());

    let mut policy = mas_domain::Policy::new(
        Some(tenant),
        None,
        None,
        &name,
        mas_domain::PolicyType::Execution,
        mas_domain::PolicyScope::Tenant,
        10,
        serde_json::json!({
            "rules": [{
                "id": "deny-tools",
                "effect": "deny",
                "actions": ["tool.invoke"],
                "reason": "tools are disabled for this tenant"
            }]
        }),
    )
    .expect("policy");

    stores.policies.upsert(&policy).await.expect("upsert");
    assert!(
        !stores
            .policies
            .get(policy.id)
            .await
            .expect("read")
            .expect("exists")
            .active,
        "a draft is not enforced until it is published"
    );

    // Publishing compiles first: an incoherent definition cannot become
    // active, and this is the only place that guarantee can be enforced.
    let published = stores
        .policies
        .publish(policy.id, "admin-1")
        .await
        .expect("publish");
    assert!(published.active);
    assert!(
        published.compiled_checksum.is_some(),
        "the compiled fingerprint travels with the policy"
    );

    // The policy is now in the applicable set for this tenant.
    let applicable = mas_policy::store::PolicyStorePort::list_applicable(
        &stores.policies,
        &mas_policy::store::PolicyQuery::for_context(tenant, None, None),
    )
    .await
    .expect("applicable");
    assert!(
        applicable.iter().any(|candidate| candidate.id == policy.id),
        "a published tenant policy is in its own applicable set"
    );

    // Editing the definition bumps the version, which is what invalidates
    // cached decisions.
    policy
        .update_definition(serde_json::json!({
            "rules": [{
                "id": "deny-exports",
                "effect": "deny",
                "actions": ["data.export"],
                "reason": "exports are disabled for this tenant"
            }]
        }))
        .expect("update");
    stores.policies.upsert(&policy).await.expect("upsert");
    let reread = PolicyStorePort::get(&stores.policies, policy.id)
        .await
        .expect("read")
        .expect("exists");
    assert!(
        reread.version >= published.version,
        "a definition change never lowers the version"
    );

    // An active policy must be deactivated before it can be deleted.
    assert!(
        PostgresPolicyStore::delete(&stores.policies, Some(tenant), policy.id)
            .await
            .is_err(),
        "deleting a live policy outright would silently remove enforcement"
    );
    stores
        .policies
        .deactivate(policy.id, "admin-1")
        .await
        .expect("deactivate");
    PostgresPolicyStore::delete(&stores.policies, Some(tenant), policy.id)
        .await
        .expect("delete after deactivation");
}

#[tokio::test]
async fn a_malformed_policy_never_becomes_active() {
    let Some(pool) = support::live_pool("cp-policy-compile").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let tenant = seed_scope(&pool, "cp-policy-compile").await;
    let name = format!("bad-{}", uuid::Uuid::new_v4().simple());

    let policy = mas_domain::Policy::new(
        Some(tenant),
        None,
        None,
        &name,
        mas_domain::PolicyType::Execution,
        mas_domain::PolicyScope::Tenant,
        1,
        // A rule with no effect is incoherent; the compiler must refuse it.
        serde_json::json!({"rules": [{"id": "nonsense"}]}),
    )
    .expect("policy");
    stores.policies.upsert(&policy).await.expect("upsert");

    let result = stores.policies.publish(policy.id, "admin-1").await;
    assert!(
        result.is_err(),
        "an invalid definition must not become active on any caller path"
    );
    assert!(
        !stores
            .policies
            .get(policy.id)
            .await
            .expect("read")
            .expect("exists")
            .active,
        "the failed publish left the policy inactive"
    );
}

// ───────────────────────────────────────────────────────────────────────────
// Phase 19 — billing webhook receipts
// ───────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_replayed_webhook_event_is_recorded_once() {
    let Some(pool) = support::live_pool("cp-webhook").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let event_id = format!("evt_{}", uuid::Uuid::new_v4().simple());
    let hash = sha256_hex(b"{\"id\":\"evt\"}");
    let now = at(1_700_000_000);

    let first = stores
        .billing
        .claim("stripe", &event_id, "invoice.paid", &hash, true, None)
        .await
        .expect("first claim")
        .expect("claimed");
    assert_eq!(first.state, "received");
    assert!(first.signature_valid);

    // The same event delivered again is recognised, not applied twice.
    let replay = stores
        .billing
        .claim("stripe", &event_id, "invoice.paid", &hash, true, None)
        .await
        .expect("replay")
        .expect("the existing receipt comes back");
    assert_eq!(
        replay.id, first.id,
        "a duplicate delivery returns the original receipt, not a second one"
    );

    // Finish it, then confirm a duplicate finish cannot re-open it.
    let receipt = stores
        .billing
        .finish(
            mas_common::ids::BillingReceiptId::from_uuid(first.id),
            ReceiptState::Applied,
            None,
            None,
            &now,
        )
        .await
        .expect("finish");
    assert_eq!(receipt.state, "applied");
    let twice = stores
        .billing
        .finish(
            mas_common::ids::BillingReceiptId::from_uuid(first.id),
            ReceiptState::Applied,
            None,
            None,
            &now,
        )
        .await
        .expect("finish again");
    assert_eq!(
        twice.state, "applied",
        "finishing twice is a no-op, never a second state change"
    );

    // Scoped to this event, not to the table: the database accumulates
    // receipts across runs, and counting the whole table would assert
    // something about test ordering rather than about idempotency.
    let receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM billing_webhook_receipts WHERE provider = $1 AND event_id = $2",
    )
    .bind("stripe")
    .bind(&event_id)
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(
        receipts, 1,
        "one receipt per provider event, however many times it is delivered"
    );
}

#[tokio::test]
async fn an_unsigned_callback_is_recorded_as_rejected_not_applied() {
    let Some(pool) = support::live_pool("cp-webhook-unsigned").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let event_id = format!("evt_{}", uuid::Uuid::new_v4().simple());
    let hash = sha256_hex(b"forged");

    let receipt = stores
        .billing
        .claim("stripe", &event_id, "customer.updated", &hash, false, None)
        .await
        .expect("claim")
        .expect("receipt");
    assert_eq!(
        receipt.state, "rejected",
        "an unverified callback is rejected at receipt time"
    );
    assert!(!receipt.signature_valid);
    assert_eq!(
        receipt.rejection_reason.as_deref(),
        Some("signature verification failed"),
        "and the receipt says why"
    );
    assert_eq!(
        receipt.tenant_id, None,
        "no tenant is resolved from an untrusted callback"
    );
}

// ───────────────────────────────────────────────────────────────────────────
// Phase 29 — multi-tenant billing isolation, as the runtime role
// ───────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn tenant_b_cannot_read_or_write_tenant_a_billing_state() {
    let Some(pool) = support::live_pool("cp-biso").await else {
        return;
    };
    let Some(app) = support::live_app_pool("cp-billing-isolation-app").await else {
        eprintln!(
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\
             control_plane_pg: SKIPPING the tenant-isolation assertions.\n\
             Set MAS_TEST_DATABASE_APP_URL to a URL for the mas_app role.\n\
             Without it these tests can only pass vacuously, so they are not\n\
             run at all rather than being reported as green.\n\
             ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        );
        return;
    };
    let (tenant_a, tenant_b) = two_tenants(&pool, "cp-biso").await;

    // Seed tenant A's billing state through the migrator pool.
    let stores = ControlPlaneStores::new(pool.clone());
    let now = at(1_700_000_000);
    stores
        .subscriptions
        .create(
            &Subscription::new(
                tenant_a,
                "enterprise",
                SubscriptionState::Active,
                None,
                None,
            )
            .expect("subscription"),
        )
        .await
        .expect("create");
    stores
        .usage
        .record(&mas_persistence::repositories::control_plane::UsageWrite {
            tenant_id: tenant_a,
            organization_id: None,
            project_id: None,
            dimension: QuotaDimension::Tokens,
            quantity: 42,
            execution_id: None,
            task_id: Some(TaskId::new()),
            task_attempt_id: None,
            model: None,
            provider: None,
            input_quantity: None,
            output_quantity: None,
            duration_ms: None,
            cost_micros: None,
            cost_currency: None,
            estimated: false,
            dimensions: serde_json::json!({}),
            metering_key: None,
            occurred_at: now,
        })
        .await
        .expect("record");
    stores
        .overrides
        .grant_quota(
            &QuotaOverride::new(
                tenant_a,
                QuotaDimension::Tokens,
                QuotaPeriod::Monthly,
                10_000,
                EnforcementMode::Enforce,
                "contract",
                OverrideActor::system(),
                now,
                None,
            )
            .expect("override"),
        )
        .await
        .expect("grant");

    // Now connect as tenant B and try to reach all of it.
    let mut connection = app.acquire().await.expect("connection");
    sqlx::query("BEGIN")
        .execute(connection.as_mut())
        .await
        .expect("begin");
    sqlx::query("SELECT set_config('app.current_tenant', $1, true)")
        .bind(tenant_b.to_string())
        .execute(connection.as_mut())
        .await
        .expect("arm tenant b");

    for table in [
        "subscriptions",
        "subscription_events",
        "usage_records",
        "quota_overrides",
        "entitlement_overrides",
        "quota_counters",
        "quota_reservations",
        "quota_definitions",
        "policies",
    ] {
        let visible: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(connection.as_mut())
            .await
            .unwrap_or_else(|error| panic!("count on {table}: {error}"));
        assert_eq!(
            visible, 0,
            "tenant B must see no rows of tenant A's `{table}`"
        );
    }

    // And tenant B cannot forge a usage record for tenant A: the insert is
    // filtered by RLS, so nothing lands.
    let cross_tenant_write = sqlx::query(
        "INSERT INTO usage_records (tenant_id, resource, quantity, unit, task_id, occurred_at)
         VALUES ($1, 'tokens', 1, 'units', $2, now())",
    )
    .bind(tenant_a.into_uuid())
    .bind(TaskId::new().into_uuid())
    .execute(connection.as_mut())
    .await;
    assert!(
        cross_tenant_write.is_err() || cross_tenant_write.expect("exec").rows_affected() == 0,
        "tenant B must not be able to bill tenant A"
    );

    sqlx::query("ROLLBACK")
        .execute(connection.as_mut())
        .await
        .expect("rollback");
    drop(connection);

    // Tenant A's own data survived all of it.
    assert!(
        stores
            .subscriptions
            .get(tenant_a)
            .await
            .expect("read")
            .is_some(),
        "tenant A's subscription is untouched"
    );
}

#[tokio::test]
async fn the_runtime_role_cannot_switch_row_level_security_off() {
    let Some(pool) = support::live_pool("cp-no-bypass").await else {
        return;
    };
    let Some(app) = support::live_app_pool("cp-no-bypass-app").await else {
        eprintln!("control_plane_pg: SKIPPING no-bypass check (set MAS_TEST_DATABASE_APP_URL)");
        return;
    };
    let (tenant_a, _tenant_b) = two_tenants(&pool, "cp-no-bypass").await;
    let stores = ControlPlaneStores::new(pool.clone());
    stores
        .subscriptions
        .create(
            &Subscription::new(tenant_a, "team", SubscriptionState::Active, None, None)
                .expect("subscription"),
        )
        .await
        .expect("create");

    let mut connection = app.acquire().await.expect("connection");
    sqlx::query("BEGIN")
        .execute(connection.as_mut())
        .await
        .expect("begin");
    sqlx::query("SELECT set_config('app.current_tenant', $1, true)")
        .bind(tenant_a.to_string())
        .execute(connection.as_mut())
        .await
        .expect("arm");

    // The runtime role has no privilege that would let it disable the
    // policies, so this must fail rather than silently widening its view.
    let attempt = sqlx::query("SET row_security = off")
        .execute(connection.as_mut())
        .await;
    let bypass = sqlx::query("ALTER TABLE subscriptions DISABLE ROW LEVEL SECURITY")
        .execute(connection.as_mut())
        .await;
    assert!(
        bypass.is_err(),
        "mas_app must not be able to disable row-level security"
    );
    let _ = attempt;
    sqlx::query("ROLLBACK")
        .execute(connection.as_mut())
        .await
        .expect("rollback");
}

// ───────────────────────────────────────────────────────────────────────────
// Phase 30 — concurrency
// ───────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn concurrent_usage_settlement_records_each_observation_once() {
    let Some(pool) = support::live_pool("cp-concurrent-usage").await else {
        return;
    };
    let stores = Arc::new(ControlPlaneStores::new(pool.clone()));
    let tenant = seed_scope(&pool, "cp-concurrent-usage").await;
    let execution = ExecutionId::new();
    let task = TaskId::new();
    let now = at(1_700_000_000);

    // Sixteen replicas all reporting the same observation for the same task.
    // The task id is shared on purpose: this is one observation delivered
    // sixteen times, not sixteen observations.
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let stores = Arc::clone(&stores);
        tasks.push(tokio::spawn(async move {
            let write = mas_persistence::repositories::control_plane::UsageWrite {
                tenant_id: tenant,
                organization_id: None,
                project_id: None,
                dimension: QuotaDimension::ApiCalls,
                quantity: 3,
                execution_id: Some(execution),
                task_id: Some(task),
                task_attempt_id: None,
                model: None,
                provider: None,
                input_quantity: None,
                output_quantity: None,
                duration_ms: None,
                cost_micros: None,
                cost_currency: None,
                estimated: false,
                dimensions: serde_json::json!({}),
                metering_key: None,
                occurred_at: now,
            };
            stores.usage.record(&write).await
        }));
    }
    let mut written = 0usize;
    for task in tasks {
        if task.await.expect("task").expect("record") {
            written += 1;
        }
    }
    assert_eq!(
        written, 1,
        "sixteen duplicate deliveries must produce exactly one billable record"
    );
    let rows = stores
        .usage
        .for_execution(tenant, execution)
        .await
        .expect("rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].quantity, 3);
}

#[tokio::test]
async fn concurrent_quota_overrides_cannot_both_be_live() {
    let Some(pool) = support::live_pool("cp-concurrent-override").await else {
        return;
    };
    let stores = Arc::new(ControlPlaneStores::new(pool.clone()));
    let tenant = seed_scope(&pool, "cp-concurrent-override").await;
    let now = at(1_700_000_000);

    let mut tasks = Vec::new();
    for index in 0..12 {
        let stores = Arc::clone(&stores);
        tasks.push(tokio::spawn(async move {
            stores
                .overrides
                .grant_quota(
                    &QuotaOverride::new(
                        tenant,
                        QuotaDimension::Executions,
                        QuotaPeriod::Daily,
                        100 + index,
                        EnforcementMode::Enforce,
                        "concurrent negotiation",
                        OverrideActor::system(),
                        now,
                        None,
                    )
                    .expect("override"),
                )
                .await
        }));
    }
    let mut granted = 0usize;
    let mut refused = 0usize;
    for task in tasks {
        match task.await.expect("task") {
            Ok(_) => granted += 1,
            Err(_) => refused += 1,
        }
    }
    assert_eq!(granted, 1, "exactly one live override can exist");
    assert_eq!(refused, 11);

    let effective = stores
        .overrides
        .effective_quotas(tenant, &now)
        .await
        .expect("effective");
    assert_eq!(
        effective.len(),
        1,
        "the winner is unambiguous — no row-order dependence"
    );
}

#[tokio::test]
async fn concurrent_subscription_updates_serialize_on_the_version() {
    let Some(pool) = support::live_pool("cp-csub").await else {
        return;
    };
    let stores = Arc::new(ControlPlaneStores::new(pool.clone()));
    let tenant = seed_scope(&pool, "cp-csub").await;
    let now = at(1_700_000_000);
    stores
        .subscriptions
        .create(
            &Subscription::new(tenant, "team", SubscriptionState::Active, None, None)
                .expect("subscription"),
        )
        .await
        .expect("create");

    // Eight callers all try to move the *same* version forward. Exactly one
    // wins; the rest are told to re-read rather than silently overwriting.
    let mut tasks = Vec::new();
    for index in 0..8 {
        let stores = Arc::clone(&stores);
        tasks.push(tokio::spawn(async move {
            stores
                .subscriptions
                .transition(
                    tenant,
                    SubscriptionState::Suspended,
                    &format!("request {index}"),
                    "system",
                    "service",
                    None,
                    &format!("corr-{index}"),
                    1,
                    &now,
                )
                .await
        }));
    }
    let mut applied = 0usize;
    for task in tasks {
        if task.await.expect("task").is_ok() {
            applied += 1;
        }
    }
    assert_eq!(
        applied, 1,
        "only one transition may win a given version; the rest must re-read"
    );
    assert_eq!(
        stores
            .subscriptions
            .history(tenant, 50)
            .await
            .expect("history")
            .len(),
        1,
        "one transition means one history row"
    );
}

// ───────────────────────────────────────────────────────────────────────────
// Phase 31 — failure and recovery
// ───────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_leased_reservation_does_not_consume_quota_forever() {
    let Some(pool) = support::live_pool("cp-lease-recovery").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let tenant = seed_scope(&pool, "cp-lease-recovery").await;
    let now = at(1_700_000_000);
    let key = format!("lease-{}", uuid::Uuid::new_v4());

    // Simulate a worker that reserved and then crashed: the counter was
    // charged, and the reservation row is all that can give it back.
    let counter_key = CounterKey::for_time(tenant, QuotaDimension::ConcurrentRuns, None, &now);
    stores
        .counters
        .increment(counter_key, 5)
        .await
        .expect("charge");
    stores
        .reservations
        .reserve(
            tenant,
            QuotaDimension::ConcurrentRuns,
            counter_key.bucket,
            5,
            &key,
            None,
            None,
            &at(1_700_000_005),
            &now,
        )
        .await
        .expect("reserve");

    // The sweeper runs after the lease lapses and returns the headroom.
    let reclaimed = stores
        .reservations
        .reclaim_expired(Some(tenant), &at(1_700_000_600), 100)
        .await
        .expect("sweep");
    assert_eq!(reclaimed.len(), 1);
    for (_reservation, released) in reclaimed {
        stores
            .counters
            .decrement(counter_key, released)
            .await
            .expect("release");
    }
    assert_eq!(
        stores.counters.get(counter_key).await.expect("counter"),
        0,
        "a crashed worker's headroom came back"
    );
}

#[tokio::test]
async fn a_quota_decision_survives_a_stale_definition_change() {
    let Some(pool) = support::live_pool("cp-definition-change").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let tenant = seed_scope(&pool, "cp-definition-change").await;
    let now = at(1_700_000_000);
    let key = CounterKey::for_time(tenant, QuotaDimension::Executions, Some(3_600), &now);

    // Redefining a quota never rewrites what the tenant already consumed:
    // the counter and the definition are separate tables for exactly this
    // reason.
    stores.counters.increment(key, 9).await.expect("consume");
    let quota = Quota::new(
        tenant,
        QuotaDimension::Executions,
        QuotaPeriod::Hourly,
        10,
        EnforcementMode::Enforce,
    )
    .expect("quota");
    QuotaRegistryPort::upsert(&stores.quotas, &quota)
        .await
        .expect("upsert");
    assert_eq!(
        stores.counters.get(key).await.expect("counter"),
        9,
        "the definition change did not move the counter"
    );

    // Under the new ceiling of 10, one more unit fits and the next does not.
    assert!(
        stores
            .counters
            .try_reserve(key, 1, 10)
            .await
            .expect("fit")
            .allowed
    );
    assert!(
        !stores
            .counters
            .try_reserve(key, 1, 10)
            .await
            .expect("full")
            .allowed,
        "the ceiling is enforced against the new definition"
    );
}

/// An audit event outlives the transaction of the mutation it describes.
///
/// This is the positive statement of a design decision that reads, from the
/// outside, like a gap: a control-plane mutation and its audit event are
/// written on **different connections** and never share a transaction. That is
/// deliberate, for two reasons the composition root spells out — a denial must
/// be recordable for a tenant the caller was never scoped to, and the record of
/// an attempt must survive the failure of the attempt. Writing the audit inside
/// the request's transaction would erase the second kind of evidence exactly
/// when it matters, because a rolled-back transaction takes its audit row with
/// it.
///
/// So the test is two halves: the mutation really does roll back, and the audit
/// event describing it really is still there afterwards.
#[tokio::test]
async fn an_audit_event_survives_the_rollback_of_the_mutation_it_describes() {
    let Some(pool) = support::live_pool("cp-audit-survives").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let tenant = seed_scope(&pool, "cp-audit-survives").await;
    let organization = organization_of(&pool, tenant).await;
    let execution = ExecutionId::new();
    let task = TaskId::new();
    let correlation = "corr-audit-survives";

    // A request that mutated and then failed. The mutation must vanish with the
    // request's transaction — that half is what makes the audit half mean
    // anything.
    let outcome: mas_common::result::Result<()> =
        mas_persistence::scoped_request::with_authoritative_scope(
            &pool,
            AuthoritativeScope::resolved(tenant, organization, "test-actor"),
            || async {
                stores
                    .usage
                    .record(&UsageWrite {
                        tenant_id: tenant,
                        organization_id: Some(organization),
                        project_id: None,
                        dimension: QuotaDimension::Tokens,
                        quantity: 900,
                        execution_id: Some(execution),
                        task_id: Some(task),
                        task_attempt_id: None,
                        model: None,
                        provider: None,
                        input_quantity: None,
                        output_quantity: None,
                        duration_ms: None,
                        cost_micros: None,
                        cost_currency: None,
                        estimated: true,
                        dimensions: serde_json::json!({}),
                        metering_key: None,
                        occurred_at: at(1_700_000_000),
                    })
                    .await?;
                Err(mas_common::error::AppError::validation(
                    "the request failed after mutating",
                ))
            },
        )
        .await;
    assert!(outcome.is_err(), "the request failed, as scripted");

    let mutations: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM usage_records
          WHERE tenant_id = $1 AND execution_id = $2",
    )
    .bind(tenant.into_uuid())
    .bind(execution.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(
        mutations, 0,
        "the mutation rolled back with the transaction that made it"
    );

    // The audit event describing that attempt is appended on the audit plane —
    // a different connection, outside the request's transaction — and it is
    // still there.
    let audit = mas_persistence::repositories::audit::PostgresAuditLog::new(pool.clone());
    let actor =
        mas_domain::AuditActor::new(mas_domain::AuditActorKind::User, "operator-1").expect("actor");
    let event = mas_domain::AuditEvent::new(
        Some(tenant),
        Some(organization),
        actor,
        "usage.record",
        "usage_record",
        Some(execution.to_string()),
        mas_domain::AuditOutcome::Failure,
        correlation,
    )
    .expect("event");
    mas_application::audit::AuditSinkPort::record(&audit, &event)
        .await
        .expect("append");

    let recorded: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_events
          WHERE tenant_id = $1 AND correlation_id = $2",
    )
    .bind(tenant.into_uuid())
    .bind(correlation)
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(
        recorded, 1,
        "the record of the attempt survived the rollback that erased the \
         attempt itself — which is the whole reason the two do not share a \
         transaction"
    );
}

/// A sweep that runs with no expired work is a no-op, not an error — the
/// sweeper runs on a timer, so the common case is "nothing to do".
#[tokio::test]
async fn an_empty_sweep_is_a_no_op() {
    let Some(pool) = support::live_pool("cp-empty-sweep").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let tenant = seed_scope(&pool, "cp-empty-sweep").await;
    let swept = stores
        .reservations
        .reclaim_expired(Some(tenant), &at(1_700_000_000), 100)
        .await
        .expect("sweep");
    assert!(swept.is_empty());
}

/// Documents the lease default the gate uses, so a change to it is visible.
#[test]
fn the_default_reservation_lease_is_bounded() {
    assert!(
        mas_application::control_plane::DEFAULT_RESERVATION_LEASE <= Duration::from_secs(3_600),
        "a lease longer than an hour would hold a tenant's headroom for too long after a crash"
    );
    assert!(
        mas_application::control_plane::DEFAULT_RESERVATION_LEASE >= Duration::from_secs(60),
        "a lease shorter than a minute would reclaim work that is still in flight"
    );
}

// ───────────────────────────────────────────────────────────────────────────
// The control plane runs inside the request's scope
//
// Every table below is `FORCE ROW LEVEL SECURITY`, and the policies read
// `app.current_tenant`, which is `SET LOCAL` inside the one transaction a
// request opens. A control-plane repository that ran its own statement off the
// pool therefore ran with no GUC at all: inserts were refused with SQLSTATE
// 42501 and reads came back empty — and an empty usage list is
// indistinguishable from a tenant that consumed nothing.
//
// These two tests pin the fix from both sides: the write that must succeed
// inside a scope, and the write that must be refused across one.
// ───────────────────────────────────────────────────────────────────────────

/// The organization a seeded tenant belongs to.
///
/// `AuthoritativeScope` is built from both ids, and the tenant row is the one
/// place the pair is recorded.
async fn organization_of(pool: &sqlx::PgPool, tenant: TenantId) -> mas_common::ids::OrganizationId {
    let row: (uuid::Uuid,) = sqlx::query_as("SELECT organization_id FROM tenants WHERE id = $1")
        .bind(tenant.into_uuid())
        .fetch_one(pool)
        .await
        .expect("tenant row");
    mas_common::ids::OrganizationId::from_uuid(row.0)
}

/// One usage write for one tenant, at a fixed instant.
fn usage_write(tenant: TenantId, execution: ExecutionId, quantity: u64) -> UsageWrite {
    UsageWrite {
        tenant_id: tenant,
        organization_id: None,
        project_id: None,
        dimension: QuotaDimension::Tokens,
        quantity,
        execution_id: Some(execution),
        task_id: Some(TaskId::new()),
        task_attempt_id: None,
        model: Some("model-a".to_owned()),
        provider: Some("provider-a".to_owned()),
        input_quantity: Some(quantity / 2),
        output_quantity: Some(quantity / 2),
        duration_ms: Some(1_000),
        cost_micros: Some(1_000),
        cost_currency: Some("USD".to_owned()),
        estimated: true,
        dimensions: serde_json::json!({}),
        metering_key: None,
        occurred_at: at(1_700_000_000),
    }
}

#[tokio::test]
async fn a_control_plane_write_succeeds_inside_a_request_scope() {
    let Some(pool) = support::live_pool("cp-scoped-write").await else {
        return;
    };
    let Some(app) = support::live_app_pool("cp-scoped-write").await else {
        return;
    };
    let tenant = seed_scope(&pool, "cp-scoped-write").await;
    let organization = organization_of(&pool, tenant).await;
    let stores = ControlPlaneStores::new(app.clone());
    let scope = AuthoritativeScope::resolved(tenant, organization, "test-actor");
    let execution = ExecutionId::new();

    // Before this was fixed the call below failed with SQLSTATE 42501 under
    // `mas_app`: the role may insert into the table, but the INSERT ran on a
    // connection that had never seen `app.current_tenant`.
    let recorded =
        mas_persistence::scoped_request::with_authoritative_scope(&app, scope.clone(), || async {
            stores
                .usage
                .record(&usage_write(tenant, execution, 250))
                .await
        })
        .await
        .expect("a usage write inside the tenant's own scope must succeed");
    assert!(recorded, "the first record for this execution is written");

    let rows = mas_persistence::scoped_request::with_authoritative_scope(&app, scope, || async {
        stores.usage.for_execution(tenant, execution).await
    })
    .await
    .expect("the rows are readable back inside the same scope");
    assert_eq!(
        rows.len(),
        1,
        "a write that joined the request transaction is visible to it"
    );
}

#[tokio::test]
async fn a_control_plane_write_for_another_tenant_is_refused_by_the_database() {
    let Some(pool) = support::live_pool("cp-scoped-cross").await else {
        return;
    };
    let Some(app) = support::live_app_pool("cp-scoped-cross").await else {
        return;
    };
    let (home, victim) = two_tenants(&pool, "cp-scoped-cross").await;
    let organization = organization_of(&pool, home).await;
    let stores = ControlPlaneStores::new(app.clone());
    let scope = AuthoritativeScope::resolved(home, organization, "test-actor");

    // Tenant A's transaction, holding tenant B's id. There is no application
    // check that stops this — the handler is simply given a tenant id — so the
    // refusal has to come from the policy.
    let outcome =
        mas_persistence::scoped_request::with_authoritative_scope(&app, scope, || async {
            stores
                .usage
                .record(&usage_write(victim, ExecutionId::new(), 250))
                .await
        })
        .await;

    let error = outcome.expect_err("tenant A's transaction must not write tenant B's usage");
    assert_eq!(
        error.error_code(),
        "FORBIDDEN",
        "a row-level-security refusal is an authorization answer — deterministic and \
         non-retryable — not a database fault the caller is invited to retry: {error:?}"
    );

    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM usage_records WHERE tenant_id = $1")
        .bind(victim.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(
        rows, 0,
        "a refused cross-tenant write must not have left a row behind"
    );
}

// ───────────────────────────────────────────────────────────────────────────
// Concurrency, and the failures that must not be confused with success
//
// The two sections below are the ones the spec calls out by number: ten
// concurrency races that must resolve deterministically, and ten
// failure/recovery cases where the platform must never let a failure look
// like a grant.
// ───────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn two_replicas_cannot_both_take_the_exact_remainder() {
    let Some(pool) = support::live_pool("cp-dq-remainder").await else {
        return;
    };
    let tenant = seed_scope(&pool, "cp-dq-remainder").await;
    let now = at(1_700_000_000);
    let key = CounterKey::for_time(tenant, QuotaDimension::Tokens, Some(3_600), &now);
    let counters = Arc::new(PostgresCounterStore::new(pool.clone()));

    // 60 is taken first, leaving exactly 40. Two replicas then ask for exactly
    // 40 each: the first fills the ceiling to the brim, the second is one unit
    // over and must be refused. This is the boundary the "70 and 50" test
    // leaves untested — a ceiling that is *exactly* full.
    let first = counters.try_reserve(key, 60, 100).await.expect("first");
    assert!(first.allowed, "60 fits in 100");

    let a = {
        let counters = Arc::clone(&counters);
        tokio::spawn(async move { counters.try_reserve(key, 40, 100).await })
    };
    let b = {
        let counters = Arc::clone(&counters);
        tokio::spawn(async move { counters.try_reserve(key, 40, 100).await })
    };
    let (a, b) = (
        a.await.expect("task a").expect("reserve a"),
        b.await.expect("task b").expect("reserve b"),
    );
    assert!(
        !(a.allowed && b.allowed),
        "60 + 40 + 40 is 140; the ceiling is 100, so both must never be granted"
    );
    assert!(a.allowed || b.allowed, "one of them fits exactly");
    assert_eq!(
        counters.get(key).await.expect("counter"),
        100,
        "a ceiling that is exactly full is full, and a refusal wrote nothing"
    );
}

#[tokio::test]
async fn two_concurrent_subscription_creations_leave_one_live_subscription() {
    let Some(pool) = support::live_pool("cp-dq-subscription-race").await else {
        return;
    };
    let stores = Arc::new(ControlPlaneStores::new(pool.clone()));
    let tenant = seed_scope(&pool, "cp-dq-subscription-race").await;

    // Four replicas of the provisioning path, all reacting to the same event.
    // The partial unique index is what arbitrates them: at most one live
    // subscription per tenant, and a loser is told "already exists" rather
    // than being given a second row.
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let stores = Arc::clone(&stores);
        tasks.push(tokio::spawn(async move {
            let subscription =
                Subscription::new(tenant, "developer", SubscriptionState::Active, None, None)
                    .expect("subscription");
            stores.subscriptions.create(&subscription).await
        }));
    }

    let mut created = 0usize;
    let mut refused = 0usize;
    for task in tasks {
        match task.await.expect("task") {
            Ok(_) => created += 1,
            Err(error) => {
                assert_eq!(
                    error.error_code(),
                    "CONFLICT",
                    "a duplicate subscription is a conflict the caller can act on, \\
                     not a database fault: {error:?}"
                );
                refused += 1;
            },
        }
    }
    assert_eq!(
        created, 1,
        "exactly one replica may create the tenant's subscription"
    );
    assert_eq!(refused, 3, "and every other one is told why");

    let live: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM subscriptions WHERE tenant_id = $1")
        .bind(tenant.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(
        live, 1,
        "the losers must not have left rows behind — a tenant with two live \\
         subscriptions has two answers to 'what is this tenant entitled to?'"
    );
}

#[tokio::test]
async fn concurrent_observations_of_each_dimension_are_metered_once() {
    let Some(pool) = support::live_pool("cp-dq-dimensions").await else {
        return;
    };
    let stores = Arc::new(ControlPlaneStores::new(pool.clone()));
    let tenant = seed_scope(&pool, "cp-dq-dimensions").await;
    let execution = ExecutionId::new();
    let task = TaskId::new();

    // One execution observed eight times over: four deliveries reporting
    // tokens, four reporting API calls. The exactly-once key is
    // (task, dimension), so each dimension must land once — twice would
    // double-bill, once in total would silently drop a metered dimension.
    let mut tasks = Vec::new();
    for index in 0..8 {
        let stores = Arc::clone(&stores);
        let dimension = if index % 2 == 0 {
            QuotaDimension::Tokens
        } else {
            QuotaDimension::ApiCalls
        };
        tasks.push(tokio::spawn(async move {
            stores
                .usage
                .record(&UsageWrite {
                    tenant_id: tenant,
                    organization_id: None,
                    project_id: None,
                    dimension,
                    quantity: 10,
                    execution_id: Some(execution),
                    task_id: Some(task),
                    task_attempt_id: None,
                    model: None,
                    provider: None,
                    input_quantity: None,
                    output_quantity: None,
                    duration_ms: None,
                    cost_micros: None,
                    cost_currency: None,
                    estimated: false,
                    dimensions: serde_json::json!({}),
                    metering_key: None,
                    occurred_at: at(1_700_000_000),
                })
                .await
        }));
    }

    let mut written = 0usize;
    for task in tasks {
        if task.await.expect("task").expect("record") {
            written += 1;
        }
    }
    assert_eq!(
        written, 2,
        "eight deliveries of two observations produce two records, not eight"
    );

    let rows = stores
        .usage
        .for_execution(tenant, execution)
        .await
        .expect("rows");
    assert_eq!(rows.len(), 2, "and both dimensions are metered");
}

#[tokio::test]
async fn a_failure_before_the_provider_meters_nothing_and_returns_the_quota() {
    let Some(pool) = support::live_pool("cp-fail-before-provider").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let tenant = seed_scope(&pool, "cp-fail-before-provider").await;
    let now = at(1_700_000_000);
    let lease = at(1_700_000_900);
    let execution = ExecutionId::new();
    let key = format!("before-provider-{}", uuid::Uuid::new_v4());

    stores
        .reservations
        .reserve(
            tenant,
            QuotaDimension::Tokens,
            0,
            1_000,
            &key,
            Some(execution),
            Some(TaskId::new()),
            &lease,
            &now,
        )
        .await
        .expect("reserve");

    // The run never reached the provider, so there is nothing to meter: the
    // reservation is released in full and no usage row is written. Writing a
    // zero-quantity row here would be indistinguishable, to a billing export,
    // from a run that called the provider and was charged nothing.
    let settlement = stores
        .reservations
        .release(tenant, &key, &now)
        .await
        .expect("release");
    assert_eq!(settlement.consumed, 0, "nothing was consumed");
    assert_eq!(
        settlement.released, 1_000,
        "the whole reservation goes back"
    );

    let rows = stores
        .usage
        .for_execution(tenant, execution)
        .await
        .expect("rows");
    assert!(
        rows.is_empty(),
        "a run that never reached the provider leaves no usage record: {rows:?}"
    );
}

#[tokio::test]
async fn a_provider_failure_still_meters_what_was_reported() {
    let Some(pool) = support::live_pool("cp-fail-after-provider").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let tenant = seed_scope(&pool, "cp-fail-after-provider").await;
    let now = at(1_700_000_000);
    let lease = at(1_700_000_900);
    let execution = ExecutionId::new();
    let task = TaskId::new();
    let key = format!("after-provider-{}", uuid::Uuid::new_v4());

    stores
        .reservations
        .reserve(
            tenant,
            QuotaDimension::Tokens,
            0,
            1_000,
            &key,
            Some(execution),
            Some(task),
            &lease,
            &now,
        )
        .await
        .expect("reserve");

    // The provider ran and reported 370 tokens, then the call failed. "The
    // task failed" must not be flattened into "zero usage": the tokens were
    // spent, they are billable, and a ledger that says otherwise is a ledger
    // that cannot be reconciled against the provider's own invoice.
    let settlement = stores
        .reservations
        .settle(tenant, &key, 370, &now)
        .await
        .expect("settle");
    assert_eq!(settlement.consumed, 370);
    assert_eq!(settlement.released, 630, "only the unused headroom returns");

    assert!(
        stores
            .usage
            .record(&UsageWrite {
                tenant_id: tenant,
                organization_id: None,
                project_id: None,
                dimension: QuotaDimension::Tokens,
                quantity: 370,
                execution_id: Some(execution),
                task_id: Some(task),
                task_attempt_id: None,
                model: Some("model-a".to_owned()),
                provider: Some("provider-a".to_owned()),
                input_quantity: Some(120),
                output_quantity: Some(250),
                duration_ms: Some(900),
                cost_micros: Some(1_250),
                cost_currency: Some("USD".to_owned()),
                estimated: false,
                dimensions: serde_json::json!({}),
                metering_key: None,
                occurred_at: now,
            })
            .await
            .expect("record"),
        "the reported usage is written"
    );

    let rows = stores
        .usage
        .for_execution(tenant, execution)
        .await
        .expect("rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].quantity, 370,
        "a failed run is metered for what it actually consumed"
    );
    assert!(
        !rows[0].estimated,
        "provider-reported usage is not an estimate"
    );
}

#[tokio::test]
async fn a_failed_metering_unit_leaves_no_partial_usage() {
    let Some(pool) = support::live_pool("cp-fail-atomicity").await else {
        return;
    };
    let stores = ControlPlaneStores::new(pool.clone());
    let tenant = seed_scope(&pool, "cp-fail-atomicity").await;
    let organization = organization_of(&pool, tenant).await;
    let execution = ExecutionId::new();
    let task = TaskId::new();

    // A settle that meters two dimensions and then fails must leave neither.
    // A half-written meter is worse than none: it under-bills, and
    // under-billing stays invisible until someone reconciles the ledger
    // against the provider's invoice.
    //
    // The failure is raised *inside* the request scope, which is the point:
    // the two writes only share a fate because they share the request's
    // transaction. On a connection each write took for itself, the first
    // would survive the second's failure.
    let outcome: mas_common::result::Result<()> =
        mas_persistence::scoped_request::with_authoritative_scope(
            &pool,
            AuthoritativeScope::resolved(tenant, organization, "test-actor"),
            || async {
                stores
                    .usage
                    .record(&UsageWrite {
                        tenant_id: tenant,
                        organization_id: None,
                        project_id: None,
                        dimension: QuotaDimension::Tokens,
                        quantity: 250,
                        execution_id: Some(execution),
                        task_id: Some(task),
                        task_attempt_id: None,
                        model: None,
                        provider: None,
                        input_quantity: None,
                        output_quantity: None,
                        duration_ms: None,
                        cost_micros: None,
                        cost_currency: None,
                        estimated: true,
                        dimensions: serde_json::json!({}),
                        metering_key: None,
                        occurred_at: at(1_700_000_000),
                    })
                    .await?;
                stores
                    .usage
                    .record(&UsageWrite {
                        tenant_id: tenant,
                        organization_id: None,
                        project_id: None,
                        dimension: QuotaDimension::ApiCalls,
                        quantity: 3,
                        execution_id: Some(execution),
                        task_id: Some(task),
                        task_attempt_id: None,
                        model: None,
                        provider: None,
                        input_quantity: None,
                        output_quantity: None,
                        duration_ms: None,
                        cost_micros: None,
                        cost_currency: None,
                        estimated: true,
                        dimensions: serde_json::json!({}),
                        metering_key: None,
                        occurred_at: at(1_700_000_000),
                    })
                    .await?;
                Err(mas_common::error::AppError::internal(
                    "the settle failed after its writes",
                ))
            },
        )
        .await;
    assert!(
        outcome.is_err(),
        "the unit failed, so the request transaction rolled back"
    );

    let rows = stores
        .usage
        .for_execution(tenant, execution)
        .await
        .expect("rows");
    assert!(
        rows.is_empty(),
        "a failed multi-dimension settle leaves no usage at all: {rows:?}"
    );
}
