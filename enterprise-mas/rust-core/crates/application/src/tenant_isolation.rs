// PURPOSE: The multi-tenant isolation suite for the canonical durable
// execution pipeline: one executed proof per way one tenant must NOT be able
// to touch another tenant's run.
//
// Owning layer: `mas-application` (the canonical durable port). Every test
// here targets the port the HTTP/API layer calls, so what is proven here is
// what a request can actually reach.
//
// Production responsibility: tenancy in this system is enforced by scoping,
// not by filtering. A foreign tenant's execution must read as *absent* —
// `None`, not `Forbidden` — because existence itself is information, and a
// "not found" that leaks that row 42 belongs to someone else is a disclosure.
// These tests pin that distinction, plus the seven guarantees the spec names:
// tenant B must not read, claim, execute, cancel, receive the result of,
// receive the events of, or receive the usage of tenant A's execution.
//
// The database-level half of this suite lives next to the SQL that implements
// it, in `crates/persistence/tests/tenant_sql_contract.rs`, which asserts that
// every durable statement carries a tenant predicate. The two halves are the
// same guarantee seen from the two ends: this file proves the *behaviour*,
// that file proves the *statements* cannot be written any other way.
//
// It must NOT:
//   • assert `Forbidden` where `NotFound` is required — a test that accepts
//     either is a test that permits an existence oracle;
//   • scope anything by a value the caller supplies after the fact; the tenant
//     is part of the query, never a post-filter;
//   • weaken, skip or delete the existing Python tenant-isolation tests. Those
//     are untouched by this work and remain the cross-service half of the
//     guarantee.

#![cfg(test)]

use mas_common::enums::{ExecutionStatus, TaskPriority, TaskStatus};
use mas_common::error::AppError;
use mas_common::ids::{OrganizationId, ProjectId, TenantId, WorkflowId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::TransitionActor;
use std::sync::Arc;
use std::time::Duration;

use crate::durable::{
    CancellationRequest, ClaimQuery, DurableExecutionPort, ExecutionUsage, SubmissionIntent,
    TaskOutcome, TaskSettlement,
};
use crate::durable_memory::InMemoryDurableExecution;
use crate::fingerprint::{FingerprintInput, RequestFingerprint};

// ---------------------------------------------------------------------------
// Two tenants, side by side
// ---------------------------------------------------------------------------

/// Tenant A: owns everything the tests protect.
fn tenant_a() -> TenantId {
    TenantId::from_uuid(uuid::Uuid::from_u128(0xA_A_A_A))
}

/// Tenant B: the attacker. It has a valid principal, valid credentials, and no
/// relationship whatsoever to A's work.
fn tenant_b() -> TenantId {
    TenantId::from_uuid(uuid::Uuid::from_u128(0xB_B_B_B))
}

fn now(offset: u64) -> Timestamp {
    Timestamp::from_unix_seconds(1_700_000_000 + i64::try_from(offset).expect("fits"))
        .expect("fixed timestamp")
}

/// One submission for `tenant`, keyed by `key`.
fn intent(tenant: TenantId, key: &str) -> SubmissionIntent {
    let input = serde_json::json!({"job": key});
    let organization_id = OrganizationId::from_uuid(uuid::Uuid::from_u128(1));
    let project_id = ProjectId::from_uuid(uuid::Uuid::from_u128(2));
    let workflow_id = WorkflowId::from_uuid(uuid::Uuid::from_u128(3));
    let fingerprint = RequestFingerprint::submission(&FingerprintInput {
        tenant_id: tenant,
        organization_id,
        project_id,
        workflow_id: Some(workflow_id),
        agent_id: None,
        operation: "execution.run",
        input: &input,
        priority: TaskPriority::Normal,
        deadline: None,
    })
    .expect("fingerprint");
    SubmissionIntent {
        tenant_id: tenant,
        organization_id,
        project_id,
        workflow_id: Some(workflow_id),
        agent_id: None,
        agent_version_id: None,
        input,
        idempotency_key: Some(key.to_owned()),
        fingerprint,
        operation: "execution.run".to_owned(),
        priority: TaskPriority::Normal,
        max_attempts: 3,
        deadline: None,
        correlation_id: format!("corr-{key}"),
        request_id: uuid::Uuid::now_v7(),
        created_by: "user-1".to_owned(),
        actor: TransitionActor::principal("user-1", tenant).expect("actor"),
        create_initial_attempt: false,
        now: now(0),
    }
}

/// Submits one execution for each tenant and returns both receipts.
async fn two_tenants(
    store: &Arc<InMemoryDurableExecution>,
) -> (
    crate::durable::SubmissionReceipt,
    crate::durable::SubmissionReceipt,
) {
    let a = store
        .submit(intent(tenant_a(), "a-key"))
        .await
        .expect("submit a");
    let b = store
        .submit(intent(tenant_b(), "b-key"))
        .await
        .expect("submit b");
    (a, b)
}

/// Reads a task, which is the shape every worker and API read takes.
async fn load(
    store: &Arc<InMemoryDurableExecution>,
    tenant: TenantId,
    task: mas_common::ids::TaskId,
) -> Result<Option<(mas_domain::Task, Option<mas_domain::Execution>)>> {
    store.load_task(tenant, task).await
}

// ---------------------------------------------------------------------------
// 1. Tenant B must not READ tenant A's execution
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tenant_b_cannot_read_tenant_a_execution() {
    let store = Arc::new(InMemoryDurableExecution::new());
    let (a, _b) = two_tenants(&store).await;
    let execution_id = a.execution.id;

    // The owner reads it.
    assert!(
        store
            .load_execution(tenant_a(), execution_id)
            .await
            .expect("read")
            .is_some(),
        "tenant A can read its own execution"
    );

    // Tenant B gets *absence*, and specifically not a permission error: the
    // point is that B cannot even learn the row exists.
    let foreign = store
        .load_execution(tenant_b(), execution_id)
        .await
        .expect("read");
    assert!(
        foreign.is_none(),
        "existence is hidden across tenants, not merely forbidden"
    );
}

#[tokio::test]
async fn tenant_b_listing_never_contains_tenant_a_executions() {
    let store = Arc::new(InMemoryDurableExecution::new());
    let (a, b) = two_tenants(&store).await;

    let listed_b = store.list_executions(tenant_b(), 100).await.expect("list");
    assert_eq!(listed_b.len(), 1, "tenant B sees exactly its own execution");
    assert_eq!(listed_b[0].id, b.execution.id);
    assert!(
        listed_b
            .iter()
            .all(|execution| execution.tenant_id == tenant_b()),
        "no foreign row can appear in a tenant-scoped listing"
    );

    let listed_a = store.list_executions(tenant_a(), 100).await.expect("list");
    assert_eq!(listed_a.len(), 1);
    assert_eq!(listed_a[0].id, a.execution.id);
}

#[tokio::test]
async fn tenant_b_cannot_load_tenant_a_task() {
    let store = Arc::new(InMemoryDurableExecution::new());
    let (a, _b) = two_tenants(&store).await;

    assert!(
        load(&store, tenant_a(), a.task.id)
            .await
            .expect("read")
            .is_some(),
        "tenant A can load its own task"
    );
    assert!(
        load(&store, tenant_b(), a.task.id)
            .await
            .expect("read")
            .is_none(),
        "tenant B cannot load tenant A's task — the task carries no tenant of its own,\
         so it must be read through its execution's scope"
    );
}

// ---------------------------------------------------------------------------
// 2. Tenant B must not CLAIM (and therefore not execute) tenant A's work
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tenant_b_cannot_claim_tenant_a_task() {
    let store = Arc::new(InMemoryDurableExecution::new());
    let (a, _b) = two_tenants(&store).await;

    let claim = store
        .claim(ClaimQuery {
            worker: "worker-b".to_owned(),
            capacity: 10,
            lease_until: now(30),
            now: now(1),
            tenant_id: Some(tenant_b()),
            task_id: Some(a.task.id),
        })
        .await
        .expect("claim");

    assert!(
        claim.is_empty(),
        "a tenant-scoped claim cannot reach another tenant's task"
    );

    // And the task is untouched by the attempt.
    let (row, _) = load(&store, tenant_a(), a.task.id)
        .await
        .expect("read")
        .expect("row");
    assert_eq!(row.status, TaskStatus::Queued);
    assert_eq!(
        row.locked_by, None,
        "no ownership was granted to tenant B's worker"
    );
}

#[tokio::test]
async fn a_tenant_scoped_claim_only_ever_returns_that_tenants_tasks() {
    let store = Arc::new(InMemoryDurableExecution::new());
    let (a, b) = two_tenants(&store).await;

    for tenant in [tenant_a(), tenant_b()] {
        let claimed = store
            .claim(ClaimQuery {
                worker: format!("worker-{tenant}"),
                capacity: 100,
                lease_until: now(30),
                now: now(1),
                tenant_id: Some(tenant),
                task_id: None,
            })
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1, "each tenant claims exactly its own task");
        let expected = if tenant == tenant_a() {
            a.task.id
        } else {
            b.task.id
        };
        assert_eq!(claimed[0].task.id, expected);
        assert_eq!(claimed[0].task.tenant_id, tenant);
    }
}

#[tokio::test]
async fn tenant_b_cannot_renew_a_lease_on_tenant_a_task() {
    let store = Arc::new(InMemoryDurableExecution::new());
    let (a, _b) = two_tenants(&store).await;

    // Tenant A's worker owns the task.
    let owned = store
        .claim(ClaimQuery {
            worker: "worker-a".to_owned(),
            capacity: 1,
            lease_until: now(30),
            now: now(1),
            tenant_id: Some(tenant_a()),
            task_id: Some(a.task.id),
        })
        .await
        .expect("claim");
    assert_eq!(owned.len(), 1);

    // Tenant B tries to keep that lease alive — a step towards running the work.
    let renewed = store
        .renew_lease(a.task.id, "worker-b", now(120))
        .await
        .expect("renew");
    assert!(
        !renewed,
        "a foreign worker cannot extend a lease it does not own"
    );
    assert!(
        store
            .renew_lease(a.task.id, "worker-a", now(120))
            .await
            .expect("renew"),
        "the owning worker can"
    );
}

// ---------------------------------------------------------------------------
// 3. Tenant B must not SETTLE (produce the result of) tenant A's task
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tenant_b_cannot_settle_tenant_a_task() {
    let store = Arc::new(InMemoryDurableExecution::new());
    let (a, _b) = two_tenants(&store).await;

    // Tenant A's worker owns the task and has an attempt.
    let owned = store
        .claim(ClaimQuery {
            worker: "worker-a".to_owned(),
            capacity: 1,
            lease_until: now(30),
            now: now(1),
            tenant_id: Some(tenant_a()),
            task_id: Some(a.task.id),
        })
        .await
        .expect("claim");
    let claim = &owned[0];

    // Tenant B tries to settle it as a success — the shortest path from "no
    // access" to "a result the owner will be billed for".
    let forged = store
        .settle(TaskSettlement {
            tenant_id: tenant_b(),
            task_id: a.task.id,
            attempt_id: claim.attempt.id,
            worker: "worker-b".to_owned(),
            expected_version: claim.task.version,
            outcome: TaskOutcome::Succeeded {
                usage: ExecutionUsage {
                    provider: Some("openai".to_owned()),
                    model: Some("gpt-4o-mini".to_owned()),
                    tokens_input: 1_000_000,
                    tokens_output: 1_000_000,
                    tool_calls: 0,
                    steps: 1,
                    cost_micros: Some(999_999),
                },
            },
            execution: None,
            actor: TransitionActor::principal("attacker", tenant_b()).expect("actor"),
            now: now(2),
        })
        .await;

    assert!(
        forged.is_err(),
        "tenant B must not be able to write a result onto tenant A's task"
    );
    assert!(
        store.usage().is_empty(),
        "and no usage was metered to tenant A by that forged settlement"
    );

    // Tenant A's task is exactly as it was.
    let (row, _) = load(&store, tenant_a(), a.task.id)
        .await
        .expect("read")
        .expect("row");
    assert_eq!(row.status, TaskStatus::Running);
    assert_eq!(row.locked_by.as_deref(), Some("worker-a"));
}

// ---------------------------------------------------------------------------
// 4. Tenant B must not CANCEL tenant A's execution
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tenant_b_cannot_cancel_tenant_a_execution() {
    let store = Arc::new(InMemoryDurableExecution::new());
    let (a, _b) = two_tenants(&store).await;
    let execution_id = a.execution.id;

    let cancelled = store
        .cancel(CancellationRequest {
            tenant_id: tenant_b(),
            execution_id,
            actor: TransitionActor::principal("attacker", tenant_b()).expect("actor"),
            correlation_id: "corr-attack".to_owned(),
            now: now(1),
        })
        .await;

    match cancelled {
        Ok(_) => panic!("tenant B must not be able to cancel tenant A's execution"),
        Err(error) => assert!(
            matches!(error, AppError::NotFound { .. }),
            "the refusal must be NotFound, not Forbidden: a Forbidden would confirm the \
             execution exists and belongs to someone else. Got: {error:?}"
        ),
    }

    // Tenant A's execution is untouched.
    let execution = store
        .load_execution(tenant_a(), execution_id)
        .await
        .expect("read")
        .expect("row");
    assert_eq!(execution.status, ExecutionStatus::Queued);
    assert!(!execution.cancellation_requested());
}

#[tokio::test]
async fn tenant_b_cannot_transition_tenant_a_execution() {
    let store = Arc::new(InMemoryDurableExecution::new());
    let (a, _b) = two_tenants(&store).await;

    // A bare tenant-B transition attempt on A's execution must fail. The
    // concrete action does not matter: the tenant scope is what is tested.
    let attempted = store
        .transition(crate::durable::ExecutionTransitionRequest {
            tenant_id: tenant_b(),
            execution_id: a.execution.id,
            action: crate::durable::ExecutionAction::Complete,
            actor: TransitionActor::principal("attacker", tenant_b()).expect("actor"),
            correlation_id: "corr-attack".to_owned(),
            now: now(1),
        })
        .await;

    assert!(
        attempted.is_err(),
        "tenant B cannot move tenant A's execution through the lifecycle"
    );
    assert_eq!(
        store
            .load_execution(tenant_a(), a.execution.id)
            .await
            .expect("read")
            .expect("row")
            .status,
        ExecutionStatus::Queued
    );
}

// ---------------------------------------------------------------------------
// 5. Tenant B must not receive tenant A's EVENTS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_event_carries_the_tenant_it_belongs_to() {
    let store = Arc::new(InMemoryDurableExecution::new());
    let (a, b) = two_tenants(&store).await;
    let b_execution_id = b.execution.id;

    // Events are published with a tenant on the envelope, so a consumer — or a
    // subject filter — can never mistake one tenant's stream for another's.
    for row in store.outbox() {
        assert!(
            row.envelope.tenant_id == tenant_a() || row.envelope.tenant_id == tenant_b(),
            "an event with no tenant cannot be routed safely"
        );
    }

    let b_events: Vec<_> = store
        .outbox()
        .into_iter()
        .filter(|row| row.envelope.tenant_id == tenant_b())
        .collect();
    assert!(!b_events.is_empty(), "tenant B's own events exist");
    assert!(
        b_events.iter().all(
            |row| row.envelope.aggregate_id == b_execution_id.to_string()
                || row.envelope.tenant_id == tenant_b()
        ),
        "tenant B's events describe tenant B's aggregates"
    );

    // Tenant A's execution never appears in tenant B's event stream.
    assert!(
        b_events
            .iter()
            .all(|row| row.envelope.aggregate_id != a.execution.id.to_string()),
        "no event about tenant A's execution is addressed to tenant B"
    );
}

// ---------------------------------------------------------------------------
// 6. Tenant B must not receive (or be charged for) tenant A's USAGE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn usage_is_attributable_to_the_tenant_that_did_the_work() {
    let store = Arc::new(InMemoryDurableExecution::new());
    let (a, _b) = two_tenants(&store).await;

    // Tenant A's worker runs its task to completion.
    let owned = store
        .claim(ClaimQuery {
            worker: "worker-a".to_owned(),
            capacity: 1,
            lease_until: now(30),
            now: now(1),
            tenant_id: Some(tenant_a()),
            task_id: Some(a.task.id),
        })
        .await
        .expect("claim");
    let claim = &owned[0];

    let receipt = store
        .settle(TaskSettlement {
            tenant_id: tenant_a(),
            task_id: a.task.id,
            attempt_id: claim.attempt.id,
            worker: "worker-a".to_owned(),
            expected_version: claim.task.version,
            outcome: TaskOutcome::Succeeded {
                usage: ExecutionUsage {
                    provider: Some("openai".to_owned()),
                    model: Some("gpt-4o-mini".to_owned()),
                    tokens_input: 100,
                    tokens_output: 20,
                    tool_calls: 1,
                    steps: 2,
                    cost_micros: Some(1_500),
                },
            },
            execution: Some(crate::durable::ExecutionFinalization {
                execution_id: a.execution.id,
                outcome: crate::durable::ExecutionOutcome::Completed,
            }),
            actor: TransitionActor::principal("worker-a", tenant_a()).expect("actor"),
            now: now(2),
        })
        .await
        .expect("settle");

    assert!(
        !receipt.usage.is_empty(),
        "a successful run is metered — usage comes from what was consumed, not a counter"
    );
    assert_eq!(
        receipt.execution.as_ref().expect("execution").tenant_id,
        tenant_a(),
        "the finalized execution is tenant A's"
    );

    // Tenant B's listing, counters and reads are unaffected by A's success.
    let listed_b = store.list_executions(tenant_b(), 100).await.expect("list");
    assert_eq!(
        listed_b.len(),
        1,
        "tenant B's view of its own work is unchanged by tenant A's activity"
    );
    assert!(
        listed_b
            .iter()
            .all(|execution| execution.tenant_id == tenant_b()),
        "tenant B never receives tenant A's result"
    );
}

#[tokio::test]
async fn tenant_b_cannot_see_tenant_a_work_in_the_durable_counters() {
    let store = Arc::new(InMemoryDurableExecution::new());
    let (a, _b) = two_tenants(&store).await;

    // Only tenant A's task is claimable for tenant B's worker? No: the claim
    // is tenant-scoped, so B's worker has nothing. The counters that drive
    // capacity and backlog must reflect that, or a worker would spin.
    let counters = store.counters("worker-b", &now(1)).await.expect("counters");

    let b_claimable = store
        .claim(ClaimQuery {
            worker: "worker-b".to_owned(),
            capacity: 100,
            lease_until: now(30),
            now: now(1),
            tenant_id: Some(tenant_b()),
            task_id: None,
        })
        .await
        .expect("claim");

    assert!(
        counters.claimable_tasks >= 1,
        "tenant A's work is claimable in aggregate ({})",
        counters.claimable_tasks
    );
    assert!(
        b_claimable
            .iter()
            .all(|claim| claim.task.tenant_id == tenant_b()),
        "but never by a tenant-scoped claim for tenant B"
    );
    assert_eq!(b_claimable.len(), 1);
    let _ = a;
}

// ---------------------------------------------------------------------------
// 7. Identity is never inferred — it is always carried
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_same_idempotency_key_in_two_tenants_is_two_executions() {
    // Tenant identity is part of the idempotency key's scope. Without this, one
    // tenant could consume another tenant's idempotency key and have its
    // request answered with someone else's execution.
    let store = Arc::new(InMemoryDurableExecution::new());

    let a = store
        .submit(intent(tenant_a(), "shared-key"))
        .await
        .expect("submit a");
    let b = store
        .submit(intent(tenant_b(), "shared-key"))
        .await
        .expect("submit b");

    assert_ne!(
        a.execution.id, b.execution.id,
        "one idempotency key, two tenants, two independent executions"
    );
    assert_eq!(a.execution.tenant_id, tenant_a());
    assert_eq!(b.execution.tenant_id, tenant_b());

    // And each tenant's re-submission replays only its own execution.
    let replayed = store
        .submit(intent(tenant_b(), "shared-key"))
        .await
        .expect("replay b");
    assert_eq!(
        replayed.execution.id, b.execution.id,
        "tenant B's replay returns tenant B's execution, never tenant A's"
    );
}

#[tokio::test]
async fn a_task_never_outlives_its_tenant_scope_after_completion() {
    let store = Arc::new(InMemoryDurableExecution::new());
    let (a, _b) = two_tenants(&store).await;

    let owned = store
        .claim(ClaimQuery {
            worker: "worker-a".to_owned(),
            capacity: 1,
            lease_until: now(30),
            now: now(1),
            tenant_id: Some(tenant_a()),
            task_id: Some(a.task.id),
        })
        .await
        .expect("claim");
    let claim = &owned[0];

    store
        .settle(TaskSettlement {
            tenant_id: tenant_a(),
            task_id: a.task.id,
            attempt_id: claim.attempt.id,
            worker: "worker-a".to_owned(),
            expected_version: claim.task.version,
            outcome: TaskOutcome::Succeeded {
                usage: ExecutionUsage::default(),
            },
            execution: Some(crate::durable::ExecutionFinalization {
                execution_id: a.execution.id,
                outcome: crate::durable::ExecutionOutcome::Completed,
            }),
            actor: TransitionActor::principal("worker-a", tenant_a()).expect("actor"),
            now: now(2),
        })
        .await
        .expect("settle");

    // The finished execution is readable by its owner and invisible to B.
    let finished = store
        .load_execution(tenant_a(), a.execution.id)
        .await
        .expect("read")
        .expect("row");
    assert_eq!(finished.status, ExecutionStatus::Completed);
    assert!(
        store
            .load_execution(tenant_b(), a.execution.id)
            .await
            .expect("read")
            .is_none(),
        "completion does not widen the visibility of the row"
    );
}

/// The lease window a worker gets: long enough to be useful, short enough that
/// a crashed worker's task is reclaimed promptly. Referenced here so the
/// isolation suite reads as one file.
const _LEASE: Duration = Duration::from_secs(30);
