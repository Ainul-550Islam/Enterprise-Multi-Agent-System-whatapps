//! PURPOSE: The live PostgreSQL integration suite, including the database half
//! of the multi-tenant isolation guarantee.
//!
//! Owning layer: `mas-persistence` (tests).
//!
//! Production responsibility: prove against a real database what the static
//! contract test can only assert about source text — that tenant B cannot
//! read, claim, cancel or settle tenant A's execution, and that one
//! idempotency key in two tenants is two executions.
//!
//! It must NOT:
//!   • run without `MAS_TEST_DATABASE_URL` (it skips cleanly instead);
//!   • be pointed at a production database;
//!   • weaken an isolation assertion to accommodate a convenience fixture.

//! Live PostgreSQL integration suite for the production repositories.
//!
//! Gated by environment variable: set `MAS_TEST_DATABASE_URL`
//! (or `DATABASE_URL`) to a PostgreSQL instance you are allowed to mutate
//! — the suite applies the full migration set itself and seeds its own
//! fixtures. Without the variable every test in this binary skips cleanly
//! with a printed pointer to `docs/runbooks/postgres-live-tests.md`.
//!
//! NEVER run against a production database.

mod support;

use mas_common::enums::{Environment, TaskPriority, TaskStatus, WorkflowStatus};
use mas_common::ids::{
    ExecutionId, OrganizationId, ProjectId, TaskAttemptId, TaskId, TenantId, UserId,
};
use mas_common::pagination::PageRequest;
use mas_common::timestamps::Timestamp;
use mas_domain::audit_event::{AuditActor, AuditActorKind, AuditEvent, AuditOutcome};
use mas_domain::value_objects::Slug;
use mas_domain::TransitionActor;
use mas_domain::{
    Agent, AgentKind, AgentVersion, Execution, IsolationMode, Organization, Project, Schedule,
    ScheduleKind, Task, Tenant, Workflow,
};
use mas_persistence::migrations::{MigrationRunner, MIGRATIONS};
use mas_persistence::repositories::agents::{AgentStore, AgentVersionStore};
use mas_persistence::repositories::api_keys::ApiKeyStore;
use mas_persistence::repositories::audit::PostgresAuditLog;
use mas_persistence::repositories::broker::PgTaskBroker;
use mas_persistence::repositories::delivery_receipts::PostgresDeliveryReceiptStore;
use mas_persistence::repositories::executions::ExecutionStore;
use mas_persistence::repositories::schedules::ScheduleStore;
use mas_persistence::repositories::tasks::TaskStore;
use mas_persistence::repositories::tenancy::{OrganizationStore, ProjectStore, TenantStore};
use mas_persistence::repositories::workflows::WorkflowStore;
use mas_worker::delivery_receipts::{
    receipt_expiry, DeliveryReceiptPort, DeliveryReservationRequest,
};
use std::time::Duration;

/// The standard org → tenant → project fixture, seeded through the tenancy
/// stores (the same code paths production uses).
#[derive(Clone)]
struct Scope {
    organization: Organization,
    tenant: Tenant,
    project: Project,
}

impl Scope {
    fn organization_id(&self) -> OrganizationId {
        self.organization.id
    }
    fn tenant_id(&self) -> TenantId {
        self.tenant.id
    }
    fn project_id(&self) -> ProjectId {
        self.project.id
    }
}

async fn seed_scope(pool: &sqlx::PgPool, label: &str) -> Scope {
    let mut conn = pool.acquire().await.expect("connection");
    let suffix = &uuid::Uuid::now_v7().to_string().replace('-', "");
    let org_slug = format!("acme-{label}-{suffix}");
    let organization = Organization::create(
        format!("Acme {label}"),
        format!("Acme ({label})"),
        Slug::new(&org_slug).expect("org slug"),
    )
    .expect("organization");
    OrganizationStore::insert_in(&mut conn, &organization)
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
    TenantStore::insert_in(&mut conn, &tenant)
        .await
        .expect("tenant insert");
    let project = Project::create(
        tenant.id,
        organization.id,
        format!("{label} project"),
        Slug::new(&format!("proj-{label}-{suffix}")).expect("project slug"),
    )
    .expect("project");
    ProjectStore::insert_in(&mut conn, &project)
        .await
        .expect("project insert");
    Scope {
        organization,
        tenant,
        project,
    }
}

/// Runs `work` inside one RLS-armed transaction, exactly as production does.
///
/// The repositories no longer take a bare pool for tenant-owned rows: they run
/// on the request's scoped transaction, which is the only place row-level
/// security is armed. A live test that calls a repository outside a scope is
/// not exercising production — it is exercising a path production cannot take,
/// and it is correctly refused. Every repository-touching assertion in this
/// file therefore runs inside this helper.
///
/// The pool is handed to the closure rather than borrowed from the caller, so
/// a test that builds stores by move still reads naturally. Scopes deliberately
/// do not nest, so a test that needs two tenants runs two blocks — one per
/// tenant — rather than one block that sees both.
async fn scoped<T, F, Fut>(
    pool: sqlx::PgPool,
    tenant: TenantId,
    organization: OrganizationId,
    work: F,
) -> T
where
    F: FnOnce(sqlx::PgPool) -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let authoritative =
        mas_security::AuthoritativeScope::resolved(tenant, organization, "live-pg-test");
    // The pool is cloned so the owned copy can move into the closure while the
    // borrow the helper needs stays alive. `PgPool` clone is a handle clone,
    // not a new pool: both names denote the same pool of connections, which is
    // exactly what makes this a faithful stand-in for a pooled request.
    let inner = pool.clone();
    // The scoped-transaction helper runs work that can fail, so the test body
    // is lifted into `Ok(...)`; any repository error becomes a panic here,
    // which is what a test wants.
    mas_persistence::scoped_request::with_authoritative_scope(
        &pool,
        authoritative,
        move || async move { Ok(work(inner).await) },
    )
    .await
    .expect("scoped transaction")
}

#[tokio::test]
async fn migrations_are_idempotent() {
    let Some(pool) = support::live_pool("migrations-idempotent").await else {
        return;
    };

    let runner = mas_persistence::migrations::MigrationRunner::new(pool);
    let second = runner.migrate().await.expect("second migrate");
    assert!(
        second.is_empty(),
        "second migrate() must be a no-op, applied: {second:?}"
    );
    runner.verify().await.expect("ledger verifies");
    let applied = runner.applied().await.expect("ledger query");
    assert_eq!(applied.len(), MIGRATIONS.len(), "registry and ledger agree");
}

#[tokio::test]
async fn tenancy_round_trip() {
    let Some(pool) = support::live_pool("tenancy-round-trip").await else {
        return;
    };
    let scope = seed_scope(&pool, "tenancy").await;
    let ctx = mas_persistence::rls::RlsContext::for_organization_with_tenant(
        scope.organization_id(),
        scope.tenant_id(),
    );

    let orgs = OrganizationStore::new(pool.clone());
    let loaded_org = orgs
        .get_scoped(&ctx, scope.organization_id())
        .await
        .expect("org get")
        .expect("org row");
    assert_eq!(loaded_org.slug, scope.organization.slug);

    let tenants = TenantStore::new(pool.clone());
    let loaded_tenant = tenants
        .get_scoped(&ctx, scope.tenant_id())
        .await
        .expect("tenant get")
        .expect("tenant row");
    assert_eq!(loaded_tenant.name, scope.tenant.name);

    let listed = tenants
        .list_for_organization(&ctx, scope.organization_id(), 100)
        .await
        .expect("tenant list");
    assert!(listed.iter().any(|t| t.id == scope.tenant_id()));

    let by_slug = tenants
        .find_by_slug(&ctx, scope.organization_id(), scope.tenant.slug.as_str())
        .await
        .expect("tenant by slug")
        .expect("slug row");
    assert_eq!(by_slug.id, scope.tenant_id());

    let projects = ProjectStore::new(pool);
    let listed_projects = projects
        .list_for_tenant(&ctx, scope.tenant_id(), 100)
        .await
        .expect("project list");
    assert!(listed_projects.iter().any(|p| p.id == scope.project_id()));
}

#[tokio::test]
async fn agents_round_trip() {
    let Some(pool) = support::live_pool("agents-round-trip").await else {
        return;
    };
    let scope = seed_scope(&pool, "agents").await;
    let tenant = scope.tenant_id();
    let organization = scope.organization_id();
    let project = scope.project_id();

    scoped(pool.clone(), tenant, organization, |pool| async move {
        let store = AgentStore::new(pool.clone());

        let agent = Agent::create(
            tenant,
            organization,
            project,
            "Scout",
            Slug::new(&format!("scout-{}", &uuid::Uuid::now_v7().to_string()[..8])).expect("slug"),
            AgentKind::Standard,
        )
        .expect("agent");
        store.save(&agent).await.expect("agent save");

        let loaded = store.get(agent.id).await.expect("agent get").expect("row");
        assert_eq!(loaded.slug, agent.slug);
        assert_eq!(loaded.kind, agent.kind);
        assert_eq!(loaded.status, agent.status);

        let by_slug = store
            .get_by_slug(tenant, agent.slug.as_str())
            .await
            .expect("get by slug")
            .expect("slug row");
        assert_eq!(by_slug.id, agent.id);

        let for_project = store.list_for_project(project).await.expect("project list");
        assert!(for_project.iter().any(|a| a.id == agent.id));
        let for_tenant = store.list_for_tenant(tenant).await.expect("tenant list");
        assert!(for_tenant.iter().any(|a| a.id == agent.id));

        // Version append: publish v1, reload, assert the append-only journal.
        let versions = AgentVersionStore::new(pool);
        let checksum = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let version = AgentVersion::publish(
            agent.id,
            1,
            checksum,
            serde_json::json!({"name": "scout", "kind": "standard"}),
            UserId::new(),
        )
        .expect("version");
        versions.save(&version, tenant).await.expect("version save");
        let reloaded = versions
            .get(version.id())
            .await
            .expect("version get")
            .expect("version row");
        assert_eq!(reloaded.version_number(), 1);
        assert_eq!(reloaded.configuration_checksum(), checksum);
        let listed = versions
            .list_for_agent(agent.id)
            .await
            .expect("version list");
        assert_eq!(listed.len(), 1);
    })
    .await;
}

#[tokio::test]
async fn workflows_round_trip() {
    let Some(pool) = support::live_pool("workflows-round-trip").await else {
        return;
    };
    let scope = seed_scope(&pool, "workflows").await;
    let tenant = scope.tenant_id();
    let organization = scope.organization_id();
    let project = scope.project_id();

    scoped(pool.clone(), tenant, organization, |pool| async move {
        let store = WorkflowStore::new(pool);

        let workflow =
            Workflow::create(tenant, organization, project, "Nightly Rollup").expect("workflow");
        store.save(&workflow).await.expect("workflow save");

        let loaded = store
            .get(workflow.id)
            .await
            .expect("workflow get")
            .expect("row");
        assert_eq!(loaded.name, "Nightly Rollup");
        assert_eq!(loaded.status, workflow.status);

        let project_list = store.list_for_project(project).await.expect("project list");
        assert!(project_list.iter().any(|w| w.id == workflow.id));
        let tenant_list = store.list_for_tenant(tenant).await.expect("tenant list");
        assert!(tenant_list.iter().any(|w| w.id == workflow.id));

        // Status transitions persist when legal.
        let mut updated = loaded;
        if updated.publish().is_ok() {
            store.save(&updated).await.expect("workflow re-save");
            let reloaded = store.get(workflow.id).await.expect("get").expect("row");
            assert_eq!(reloaded.status, WorkflowStatus::Published);
        }
    })
    .await;
}

#[tokio::test]
async fn executions_round_trip_and_idempotency() {
    let Some(pool) = support::live_pool("executions-round-trip").await else {
        return;
    };
    let scope = seed_scope(&pool, "executions").await;
    let tenant = scope.tenant_id();
    let organization = scope.organization_id();
    let project = scope.project_id();

    scoped(pool.clone(), tenant, organization, |pool| async move {
        let store = ExecutionStore::new(pool.clone());

        let agent = Agent::create(
            tenant,
            organization,
            project,
            "Exec Agent",
            Slug::new(&format!(
                "exec-agent-{}",
                uuid::Uuid::now_v7().to_string().replace('-', "")
            ))
            .expect("slug"),
            AgentKind::Standard,
        )
        .expect("agent");
        AgentStore::new(pool.clone())
            .save(&agent)
            .await
            .expect("agent save");

        let execution = Execution::new(
            tenant,
            organization,
            project,
            None,
            Some(agent.id),
            serde_json::json!({"prompt": "ping"}),
            format!("corr-{}", uuid::Uuid::now_v7()),
            "itest-user",
        )
        .expect("execution");
        store.save(&execution).await.expect("execution save");

        let loaded = store.get(execution.id).await.expect("get").expect("row");
        assert_eq!(loaded.correlation_id, execution.correlation_id);
        assert_eq!(loaded.agent_id, execution.agent_id);

        let tenant_list = store.list_for_tenant(tenant).await.expect("tenant list");
        assert!(tenant_list.iter().any(|e| e.id == execution.id));

        // Idempotency slot: put -> get hit; a different key misses.
        let key = format!("idem-{}", uuid::Uuid::now_v7());
        let miss = store.idempotency_get(tenant, &key).await.expect("miss get");
        assert!(miss.is_none());
        store
            .idempotency_put(tenant, &key, execution.id)
            .await
            .expect("put");
        let hit: ExecutionId = store
            .idempotency_get(tenant, &key)
            .await
            .expect("hit get")
            .expect("key present");
        assert_eq!(hit, execution.id);
        // Second put is upsert-safe and idempotent.
        store
            .idempotency_put(tenant, &key, execution.id)
            .await
            .expect("second put");
    })
    .await;
}

#[tokio::test]
async fn schedules_due_scan_and_run_journal() {
    let Some(pool) = support::live_pool("schedules-due-scan").await else {
        return;
    };
    let scope = seed_scope(&pool, "schedules").await;
    let tenant = scope.tenant_id();
    let organization = scope.organization_id();
    let project = scope.project_id();

    scoped(pool.clone(), tenant, organization, |pool| async move {
        let store = ScheduleStore::new(pool);

        let mut schedule = Schedule::new(
            tenant,
            Some(project),
            "itest crank",
            ScheduleKind::Cron {
                expression: "0 0 * * * *".to_owned(),
                timezone: "UTC".to_owned(),
            },
            serde_json::json!({"operation": "execution.run", "input": {}}),
        )
        .expect("schedule");
        schedule.enable().expect("enable");
        store.save(&schedule).await.expect("schedule save");

        let now = Timestamp::now();
        let due = store.list_due(&now, 25).await.expect("list_due");
        assert!(
            !due.iter().any(|s| s.id == schedule.id),
            "freshly enabled schedule computes a future next_run_at, not due"
        );

        // Time travel (test scaffolding only): force the row due. This runs on
        // a separate connection because it is test scaffolding, not a
        // repository call — and it is still scoped to the one row the test
        // created.
        // The row was inserted in THIS transaction and is not yet visible to
        // any other connection, so the scaffolding UPDATE must run on the
        // transaction's own connection. Reaching for a second connection here
        // is the exact mistake that makes a live test silently assert nothing.
        let mut scoped_connection = mas_persistence::scoped_request::connection()
            .await
            .expect("scoped connection");
        sqlx::query(
            "UPDATE schedules SET next_run_at = now() - interval '5 seconds' WHERE id = $1",
        )
        .bind(uuid::Uuid::from(schedule.id))
        .execute(scoped_connection.connection().as_mut())
        .await
        .expect("time travel");

        // The page size is sized to hold every schedule that is currently due,
        // and that is deliberate. `list_due` pages by `next_run_at ASC`, and
        // the shared test database keeps the due rows every earlier run of this
        // suite left behind — so a fixed page fills up with *other* runs'
        // schedules and the row this test just made falls off the end. The
        // failure then appears only after the database has aged enough, in a
        // suite that has been green for weeks, and it looks like a regression
        // in `list_due` rather than what it is: an assertion that depended on
        // how much debris was lying around.
        let due_total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM schedules
              WHERE status = 'active' AND next_run_at IS NOT NULL AND next_run_at <= $1",
        )
        .bind(now.into_datetime())
        .fetch_one(scoped_connection.connection().as_mut())
        .await
        .expect("count due");
        drop(scoped_connection);

        let page = usize::try_from(due_total).expect("count fits") + 1;
        let due = store.list_due(&now, page).await.expect("list_due after");
        assert!(
            due.iter().any(|s| s.id == schedule.id),
            "a schedule whose next_run_at has passed is due, however many \
             other schedules are also due"
        );

        // Run journal: first fire of a planned tick records; the duplicate is
        // refused (the cross-replica duplicate-fire guard).
        let planned = Timestamp::now();
        assert!(store
            .record_run(schedule.id, planned)
            .await
            .expect("first record_run"));
        assert!(
            !store
                .record_run(schedule.id, planned)
                .await
                .expect("second record_run"),
            "schedule_runs UNIQUE(schedule_id, planned_at) must refuse re-record"
        );

        let later = Timestamp::from_unix_ms(planned.to_unix_ms() + 1_000).expect("+(1000ms)");
        assert!(store
            .record_run(schedule.id, later)
            .await
            .expect("distinct planned tick"));
    })
    .await;
}

#[tokio::test]
async fn tasks_round_trip() {
    let Some(pool) = support::live_pool("tasks-round-trip").await else {
        return;
    };
    let scope = seed_scope(&pool, "tasks").await;
    let tenant = scope.tenant_id();
    let organization = scope.organization_id();
    let project = scope.project_id();

    scoped(pool.clone(), tenant, organization, |pool| async move {
        let store = TaskStore::new(pool.clone());

        let agent = Agent::create(
            tenant,
            organization,
            project,
            "Task Agent",
            Slug::new(&format!(
                "task-agent-{}",
                uuid::Uuid::now_v7().to_string().replace('-', "")
            ))
            .expect("slug"),
            AgentKind::Standard,
        )
        .expect("agent");
        AgentStore::new(pool.clone())
            .save(&agent)
            .await
            .expect("agent save");

        let task = Task::new(
            tenant,
            organization,
            project,
            Some(agent.id),
            None,
            "execution.run",
            serde_json::json!({"itest": true}),
            TaskPriority::Normal,
            format!("itask-{}", uuid::Uuid::now_v7()),
            "itest-worker",
        )
        .expect("task");
        let mut row = task.clone();
        let actor = TransitionActor::system("itest").expect("actor");
        row.queue(&actor).expect("queue transition");
        store.save(&row).await.expect("task save");

        let loaded = store.load(row.id).await.expect("task load").expect("row");
        assert!(matches!(
            loaded.status,
            TaskStatus::Queued | TaskStatus::Pending
        ));
        assert_eq!(loaded.idempotency_key, row.idempotency_key);

        // Status evolution persists.
        let mut running = loaded;
        running
            .start(
                &TransitionActor::worker("itest-worker").expect("actor"),
                "itest-worker",
                Timestamp::now()
                    .checked_add(std::time::Duration::from_secs(60))
                    .expect("lease"),
                TaskAttemptId::new(),
            )
            .expect("start transition");
        store.save(&running).await.expect("re-save");
        let started = store.load(row.id).await.expect("load").expect("row");
        assert!(matches!(started.status, TaskStatus::Running));
    })
    .await;
}

#[tokio::test]
async fn api_keys_prefix_lookup_and_touch() {
    let Some(pool) = support::live_pool("api-keys").await else {
        return;
    };
    let scope = seed_scope(&pool, "apikeys").await;
    let tenant = scope.tenant_id();
    let organization = scope.organization_id();

    // The seed writes `role` explicitly: migration 0016 bound every key to a
    // role and made the column NOT NULL, so a key without one is no longer
    // insertable — which is the point.
    let key_id = uuid::Uuid::now_v7();
    let raw = format!("mas_{}", "f".repeat(64));
    let prefix = support::visible_prefix(&raw);
    let digest = support::sha256_hex(raw.as_bytes());
    sqlx::query(
        "INSERT INTO api_keys (id, tenant_id, organization_id, name, prefix, secret_hash, \
         scopes, status, role)\n         VALUES ($1, $2, $3, $4, $5, $6, '[]'::jsonb, 'active', 'service_account')",
    )
    .bind(key_id)
    .bind(uuid::Uuid::from(tenant))
    .bind(uuid::Uuid::from(organization))
    .bind(format!("itest-{}", &key_id.to_string()[..8]))
    .bind(&prefix)
    .bind(&digest)
    .execute(&pool)
    .await
    .expect("api key seed");

    // Authentication runs before a tenant request scope exists. This lookup
    // must still pass under FORCE RLS through its prefix-only SELECT policy.
    let auth_store = ApiKeyStore::new(pool.clone());
    let candidates = auth_store
        .find_by_prefix(&prefix)
        .await
        .expect("pre-scope prefix lookup");
    let row = candidates
        .iter()
        .find(|candidate| candidate.id == key_id)
        .expect("seeded row among prefix candidates");
    assert_eq!(row.status, "active");
    assert_eq!(row.secret_hash, digest);
    assert!(
        !row.role.as_str().is_empty(),
        "migration 0016 backfills a role onto every key"
    );

    scoped(pool.clone(), tenant, organization, |pool| async move {
        let store = ApiKeyStore::new(pool);
        store.touch_last_used(key_id, tenant).await.expect("touch");
    })
    .await;

    // `touch_last_used` correctly joins the request transaction. Its write is
    // not visible to the pre-authentication lookup's separate connection until
    // that request commits, so verify the durable marker after `scoped` has
    // completed rather than making a false dirty-read assertion.
    let after = auth_store.find_by_prefix(&prefix).await.expect("re-read");
    let touched = after
        .iter()
        .find(|candidate| candidate.id == key_id)
        .expect("row");
    assert!(
        touched.last_used_at.is_some(),
        "touch_last_used writes the marker"
    );
}

#[tokio::test]
async fn audit_append_list_and_counts() {
    let Some(pool) = support::live_pool("audit-append-list").await else {
        return;
    };
    let scope = seed_scope(&pool, "audit").await;
    let sink = PostgresAuditLog::new(pool);

    let correlation = format!("audit-{}", uuid::Uuid::now_v7());
    for action in ["tenancy.organization.created", "tenancy.tenant.created"] {
        let event = AuditEvent::new(
            Some(scope.tenant_id()),
            Some(scope.organization_id()),
            AuditActor::new(AuditActorKind::Service, "mas-itests").expect("actor"),
            action,
            "organization",
            Some(scope.organization_id().to_string()),
            AuditOutcome::Success,
            &correlation,
        )
        .expect("event");
        sink.append(&event).await.expect("audit append");
    }

    let page = sink
        .list_for_tenant(scope.tenant_id(), &PageRequest::new().with_limit(50))
        .await
        .expect("list for tenant");
    let ours = page
        .items
        .iter()
        .filter(|e| e.correlation_id == correlation)
        .count();
    assert!(ours >= 2, "both appended events returned, got {ours}");

    let by_corr = sink
        .list_for_correlation(scope.tenant_id(), &correlation)
        .await
        .expect("list for correlation");
    assert_eq!(by_corr.len(), 2);

    let counts = sink
        .outcome_counts(scope.tenant_id())
        .await
        .expect("outcome counts");
    assert!(counts
        .iter()
        .any(|(outcome, count)| outcome == "success" && *count >= 2));
}

#[tokio::test]
async fn broker_publish_fetch_ack_round_trip() {
    let Some(pool) = support::live_pool("broker-round-trip").await else {
        return;
    };
    let scope = seed_scope(&pool, "broker").await;
    let broker = PgTaskBroker::new(pool.clone());

    use mas_messaging::broker::{AckInstruction, BrokerPort, ConsumerConfig, PublishRequest};
    use mas_messaging::codec::{CodecConfig, JsonEventCodec};
    use mas_messaging::headers::HeaderSet;
    use mas_messaging::subjects::{Subject, SubjectFilter};

    let task_uuid = uuid::Uuid::now_v7();
    let agent_id = mas_common::ids::AgentId::new();
    let agent_version_id = mas_common::ids::AgentVersionId::new();
    let idempotency_key = format!("broker-{}", uuid::Uuid::now_v7());
    let message = mas_contracts::task::TaskQueueMessage::staged(
        TaskId::from(task_uuid),
        scope.tenant_id(),
        scope.organization_id(),
        scope.project_id(),
        "execution.run".to_owned(),
        serde_json::json!({"itest": "broker"}),
        None,
        Some(agent_id),
        None,
        idempotency_key.clone(),
        TaskPriority::Normal,
        1,
        3,
        None,
        &format!("frame-{}", uuid::Uuid::now_v7()),
    )
    .expect("staged message")
    .with_agent_version_id(agent_version_id);
    let codec = JsonEventCodec::new(CodecConfig::default());
    let frame = codec.encode_value(&message).expect("frame");

    let seq = broker
        .publish(PublishRequest {
            subject: Subject::parse("mas.tasks.execution.run").expect("subject"),
            headers: HeaderSet::default(),
            payload: frame,
            msg_id: Some(idempotency_key.clone()),
        })
        .await
        .expect("publish");
    assert!(seq > 0);

    // Republish of the same staged message collapses onto the same row.
    let codec2 = JsonEventCodec::new(CodecConfig::default());
    let frame2 = codec2.encode_value(&message).expect("frame2");
    let seq2 = broker
        .publish(PublishRequest {
            subject: Subject::parse("mas.tasks.execution.run").expect("subject"),
            headers: HeaderSet::default(),
            payload: frame2,
            msg_id: Some(idempotency_key),
        })
        .await
        .expect("republish");
    assert_eq!(seq, seq2, "idempotent publish returns the same sequence");

    let consumer = format!("itest-{}", &task_uuid.to_string()[..8]);
    broker
        .ensure_consumer(
            ConsumerConfig::new(
                consumer.clone(),
                SubjectFilter::parse("mas.tasks.>").expect("filter"),
            )
            .expect("consumer config"),
        )
        .await
        .expect("ensure consumer");

    // The durable lane is one shared queue across every tenant, oldest first,
    // so a five-row window can be filled by rows other tests staged and miss
    // the message published above. Widen the window enough to be
    // deterministic, then select our sequence out of it.
    let batch = broker.fetch(&consumer, 512).await.expect("fetch");
    let ours = batch
        .iter()
        .find(|m| m.sequence == seq)
        .expect("our task arrives");
    let decoded: mas_contracts::task::TaskQueueMessage =
        codec.decode_value(&ours.payload).expect("decode");
    assert_eq!(*decoded.task_id.as_uuid(), task_uuid);
    assert_eq!(decoded.agent_id, Some(agent_id));
    assert_eq!(decoded.agent_version_id, Some(agent_version_id));

    // Production claims and settles through the durable execution port BEFORE
    // acknowledging the broker hint (worker/src/consumer.rs). A queued task
    // cannot legally complete: the durable claim is what installs its worker
    // fence and attempt ledger entry. Mirror that real lifecycle rather than
    // bypassing its state machine in this repository test.
    let durable = PgDurableExecution::new(pool.clone());
    let claim_now = Timestamp::now();
    let claim = durable
        .claim(ClaimQuery {
            worker: "itest-worker".to_owned(),
            capacity: 1,
            lease_until: claim_now
                .checked_add(Duration::from_secs(30))
                .expect("claim lease"),
            now: claim_now,
            tenant_id: Some(scope.tenant_id()),
            task_id: Some(decoded.task_id),
        })
        .await
        .expect("durable claim")
        .into_iter()
        .next()
        .expect("our task claim");
    durable
        .settle(TaskSettlement {
            tenant_id: scope.tenant_id(),
            task_id: decoded.task_id,
            attempt_id: claim.attempt.id,
            worker: "itest-worker".to_owned(),
            expected_version: claim.task.version(),
            outcome: TaskOutcome::Succeeded {
                result: serde_json::Value::Null,
                usage: ExecutionUsage::default(),
            },
            execution: None,
            actor: TransitionActor::worker("itest-worker").expect("actor"),
            now: Timestamp::now(),
        })
        .await
        .expect("durable settle");

    broker
        .ack(&consumer, seq, AckInstruction::Ack)
        .await
        .expect("ack settles");

    let again = broker.fetch(&consumer, 512).await.expect("fetch again");
    assert!(
        !again.iter().any(|m| m.sequence == seq),
        "acked task is not re-delivered within the ack window"
    );
}

/// Regression: an expired worker lease and receipt recover through a fresh
/// broker process without producing a second live attempt.
///
/// This is deliberately a restart-shaped test, not a same-instance retry. A
/// process-local flight ledger can hide an ownership bug within one process;
/// constructing `fresh_broker` proves an expired `running` task is discoverable
/// as a read-only hint and that `PgDurableExecution` alone closes/replaces the
/// attempt ledger row.
#[tokio::test]
async fn expired_running_delivery_is_redelivered_as_one_replacement_attempt() {
    let Some(pool) = support::live_pool("expired-running-redelivery").await else {
        return;
    };

    use mas_messaging::broker::{AckInstruction, BrokerPort, ConsumerConfig, PublishRequest};
    use mas_messaging::codec::{CodecConfig, JsonEventCodec};
    use mas_messaging::headers::HeaderSet;
    use mas_messaging::subjects::{Subject, SubjectFilter};

    let scope = seed_scope(&pool, "expired-redelivery").await;
    let task_id = TaskId::new();
    let consumer = format!("recovery-{}", &task_id.to_string()[..8]);
    let first_broker = PgTaskBroker::new(pool.clone());
    first_broker
        .ensure_consumer(
            ConsumerConfig::new(
                consumer.clone(),
                SubjectFilter::parse("mas.tasks.>").expect("filter"),
            )
            .expect("consumer config"),
        )
        .await
        .expect("consumer registration");

    let queue_message = mas_contracts::task::TaskQueueMessage::staged(
        task_id,
        scope.tenant_id(),
        scope.organization_id(),
        scope.project_id(),
        "execution.run".to_owned(),
        serde_json::json!({"case": "expired-running-redelivery"}),
        None,
        None,
        None,
        format!("recovery-{}", uuid::Uuid::now_v7()),
        TaskPriority::Normal,
        1,
        3,
        None,
        &format!("recovery-frame-{}", uuid::Uuid::now_v7()),
    )
    .expect("staged queue message");
    let codec = JsonEventCodec::new(CodecConfig::default());
    first_broker
        .publish(PublishRequest {
            subject: Subject::parse("mas.tasks.execution.run").expect("subject"),
            headers: HeaderSet::default(),
            payload: codec.encode_value(&queue_message).expect("frame"),
            msg_id: Some(queue_message.idempotency_key.clone()),
        })
        .await
        .expect("publish task");

    let first_hint = first_broker
        .fetch(&consumer, 4_096)
        .await
        .expect("first broker fetch")
        .into_iter()
        .find(|message| {
            codec
                .decode_value::<mas_contracts::task::TaskQueueMessage>(&message.payload)
                .map(|decoded| decoded.task_id == task_id)
                .unwrap_or(false)
        })
        .expect("first durable hint");

    // A broker heartbeat only renews this process's transport hint. It must
    // not claim the task, install a worker fence, or create/advance an
    // attempt: the durable coordinator below is the sole lifecycle writer.
    first_broker
        .ack(&consumer, first_hint.sequence, AckInstruction::InProgress)
        .await
        .expect("heartbeat hint renewal");
    let heartbeat_state: (
        String,
        Option<String>,
        Option<chrono::DateTime<chrono::Utc>>,
        i64,
    ) = sqlx::query_as("SELECT status, locked_by, locked_until, version FROM tasks WHERE id = $1")
        .bind(task_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("task state after broker heartbeat");
    assert_eq!(heartbeat_state.0, "queued");
    assert_eq!(heartbeat_state.1, None);
    assert_eq!(heartbeat_state.2, None);
    assert_eq!(heartbeat_state.3, 1);

    let first_now = Timestamp::now();
    let short_lease = first_now
        .checked_add(Duration::from_millis(500))
        .expect("short lease timestamp");
    let receipt_store = PostgresDeliveryReceiptStore::new(pool.clone());
    let first_receipt = receipt_store
        .reserve(DeliveryReservationRequest {
            consumer_name: consumer.clone(),
            task_id: task_id.into_uuid(),
            tenant_id: scope.tenant_id().into_uuid(),
            worker_id: "expired-worker".to_owned(),
            delivery_count: first_hint.deliveries.max(1),
            lease_expires_at: short_lease,
        })
        .await
        .expect("first receipt reserve")
        .expect("first receipt wins");
    let durable = PgDurableExecution::new(pool.clone());
    let first_claim = durable
        .claim(ClaimQuery {
            worker: "expired-worker".to_owned(),
            capacity: 1,
            lease_until: short_lease,
            now: first_now,
            tenant_id: Some(scope.tenant_id()),
            task_id: Some(task_id),
        })
        .await
        .expect("first durable claim")
        .into_iter()
        .next()
        .expect("first task claim");
    assert!(
        receipt_store
            .attach_attempt(first_receipt, first_claim.attempt.id.into_uuid())
            .await
            .expect("first receipt attachment"),
        "the predecessor receipt names its running attempt"
    );

    // Both the delivery receipt and durable owner are abandoned. This sleep is
    // intentionally longer than the same short interval supplied to both
    // stores, so the database's `now()` predicates—not wall-clock assumptions
    // in the test—classify them as expired.
    tokio::time::sleep(Duration::from_millis(700)).await;

    let fresh_broker = PgTaskBroker::new(pool.clone());
    fresh_broker
        .ensure_consumer(
            ConsumerConfig::new(
                consumer.clone(),
                SubjectFilter::parse("mas.tasks.>").expect("filter"),
            )
            .expect("same consumer config"),
        )
        .await
        .expect("fresh broker consumer registration");
    let recovery_hint = fresh_broker
        .fetch(&consumer, 4_096)
        .await
        .expect("fresh broker fetch")
        .into_iter()
        .find(|message| {
            codec
                .decode_value::<mas_contracts::task::TaskQueueMessage>(&message.payload)
                .map(|decoded| decoded.task_id == task_id)
                .unwrap_or(false)
        })
        .expect("fresh broker sees expired-running recovery hint");
    assert_eq!(
        recovery_hint.sequence, first_hint.sequence,
        "restart redelivery retains the stable message identity"
    );

    let recovery_now = Timestamp::now();
    let replacement_lease =
        receipt_expiry(recovery_now, Duration::from_secs(30)).expect("replacement lease timestamp");
    let replacement_receipt = receipt_store
        .reserve(DeliveryReservationRequest {
            consumer_name: consumer,
            task_id: task_id.into_uuid(),
            tenant_id: scope.tenant_id().into_uuid(),
            worker_id: "recovery-worker".to_owned(),
            delivery_count: recovery_hint.deliveries.max(1),
            lease_expires_at: replacement_lease,
        })
        .await
        .expect("replacement receipt reserve")
        .expect("expired predecessor receipt no longer owns task");
    let replacement_claims = durable
        .claim(ClaimQuery {
            worker: "recovery-worker".to_owned(),
            capacity: 1,
            lease_until: replacement_lease,
            now: recovery_now,
            tenant_id: Some(scope.tenant_id()),
            task_id: Some(task_id),
        })
        .await
        .expect("recovery durable claim");
    assert_eq!(
        replacement_claims.len(),
        1,
        "recovery creates exactly one replacement claim"
    );
    let replacement = replacement_claims
        .into_iter()
        .next()
        .expect("replacement claim");
    assert_eq!(replacement.attempt.attempt_number, 2);
    assert!(
        receipt_store
            .attach_attempt(replacement_receipt, replacement.attempt.id.into_uuid())
            .await
            .expect("replacement receipt attachment"),
        "the replacement receipt can only attach to its matching task attempt"
    );

    let attempts: Vec<(i32, String)> = sqlx::query_as(
        "SELECT attempt, status
         FROM task_attempts
         WHERE task_id = $1
         ORDER BY attempt ASC",
    )
    .bind(task_id.into_uuid())
    .fetch_all(&pool)
    .await
    .expect("attempt ledger query");
    assert_eq!(
        attempts,
        vec![(1, "expired".to_owned()), (2, "running".to_owned())],
        "predecessor is terminal before the sole replacement is inserted"
    );
    let linked_attempt: uuid::Uuid =
        sqlx::query_scalar("SELECT last_attempt_id FROM tasks WHERE id = $1")
            .bind(task_id.into_uuid())
            .fetch_one(&pool)
            .await
            .expect("current attempt linkage");
    assert_eq!(
        linked_attempt,
        replacement.attempt.id.into_uuid(),
        "task fence points at the attempt its claimant must settle"
    );

    let stale = durable
        .settle(TaskSettlement {
            tenant_id: scope.tenant_id(),
            task_id,
            attempt_id: first_claim.attempt.id,
            worker: "expired-worker".to_owned(),
            expected_version: first_claim.task.version(),
            outcome: TaskOutcome::Succeeded {
                result: serde_json::json!({"late": true}),
                usage: ExecutionUsage::default(),
            },
            execution: None,
            actor: TransitionActor::worker("expired-worker").expect("actor"),
            now: Timestamp::now(),
        })
        .await;
    assert!(
        stale.is_err(),
        "a predecessor that lost lease recovery must never settle the replacement"
    );
}

/// Regression: several processes booting at once must not crash the ledger.
///
/// Without a session-scoped advisory lock, concurrent `CREATE TABLE IF NOT
/// EXISTS` and ledger inserts conflict on catalog unique indexes, so the
/// losing replica fails to boot. This reproduces a rolling deploy.
#[tokio::test]
async fn concurrent_migrate_is_serialized() {
    let Some(pool) = support::live_pool("concurrent-migrate").await else {
        return;
    };
    let runner = MigrationRunner::new(pool.clone());

    // Eight concurrent boot sequences. They are awaited in the same task (not
    // spawned) because `migrate(&self)` borrows across the await, which is not
    // `Send`-general enough for `tokio::spawn` — the futures are driven
    // concurrently either way.
    let runners: Vec<MigrationRunner> =
        (0..8).map(|_| MigrationRunner::new(pool.clone())).collect();
    let futures: Vec<_> = runners.iter().map(|runner| runner.migrate()).collect();
    let outcomes = futures::future::join_all(futures).await;

    let failures: Vec<String> = outcomes
        .into_iter()
        .filter_map(|outcome| outcome.err().map(|error| format!("{error}")))
        .collect();
    assert!(
        failures.is_empty(),
        "concurrent migrations must not conflict: {failures:?}"
    );

    let applied = runner.applied().await.expect("ledger reads");
    assert_eq!(
        applied.len(),
        MIGRATIONS.len(),
        "every migration applied exactly once"
    );
    let mut versions: Vec<u32> = applied.iter().map(|entry| entry.version).collect();
    versions.sort_unstable();
    versions.dedup();
    assert_eq!(
        versions.len(),
        applied.len(),
        "no duplicate ledger rows from concurrent boots"
    );
}

// ===========================================================================
// Multi-tenant isolation, at the database
// ===========================================================================
//
// The behavioural half of this guarantee lives in
// `crates/application/src/tenant_isolation.rs` and runs against the in-memory
// reference port on every build. This section runs the same assertions against
// real PostgreSQL, because "the SQL is scoped" and "the SQL *is* scoped" are
// different claims: only a live database can prove the latter.
//
// Tenant B has valid credentials, a valid principal, and no relationship to
// tenant A's work. It must not be able to read, claim, execute, cancel,
// settle, receive the result of, or receive the usage of tenant A's execution.

use mas_application::durable::{
    CancellationRequest, ClaimQuery, DurableExecutionPort, ExecutionFinalization, ExecutionOutcome,
    ExecutionUsage, SubmissionIntent, SubmissionKind, TaskOutcome, TaskSettlement,
};
use mas_application::fingerprint::{FingerprintInput, RequestFingerprint};
use mas_persistence::durable::PgDurableExecution;

/// One submission for `scope`, keyed by `key`.
/// Builds a submission intent for `scope` targeting the supplied agent.
///
/// The domain refuses a submission that targets neither a workflow nor an
/// agent (`missing_target`), so a fixture that leaves both `None` is not
/// merely inert — it is rejected. The agent is passed in because it has to
/// exist, in the same tenant, before the intent can name it.
fn durable_intent(scope: &Scope, agent: mas_common::ids::AgentId, key: &str) -> SubmissionIntent {
    let input = serde_json::json!({"job": key});
    let fingerprint = RequestFingerprint::submission(&FingerprintInput {
        tenant_id: scope.tenant_id(),
        organization_id: scope.organization_id(),
        project_id: scope.project_id(),
        workflow_id: None,
        agent_id: Some(agent),
        operation: "agent.run",
        input: &input,
        priority: TaskPriority::Normal,
        deadline: None,
    })
    .expect("fingerprint");
    SubmissionIntent {
        tenant_id: scope.tenant_id(),
        organization_id: scope.organization_id(),
        project_id: scope.project_id(),
        workflow_id: None,
        agent_id: Some(agent),
        agent_version_id: Some(mas_common::ids::AgentVersionId::new()),
        input,
        idempotency_key: Some(key.to_owned()),
        fingerprint,
        operation: "agent.run".to_owned(),
        priority: TaskPriority::Normal,
        max_attempts: 3,
        deadline: None,
        correlation_id: format!("corr-{key}"),
        request_id: uuid::Uuid::now_v7(),
        created_by: "user-1".to_owned(),
        actor: TransitionActor::principal("user-1", scope.tenant_id()).expect("actor"),
        create_initial_attempt: false,
        now: Timestamp::now(),
    }
}

#[tokio::test]
async fn tenant_b_cannot_read_claim_cancel_or_settle_tenant_a_execution() {
    let Some(pool) = support::live_pool("tenant-isolation").await else {
        return;
    };
    let a = seed_scope(&pool, "iso-a").await;
    let b = seed_scope(&pool, "iso-b").await;
    // `Scope` owns its aggregates, so the ids are lifted out once and moved
    // into each closure. Each closure gets its own copies of the plain ids.
    let (a_tenant, a_org, a_project) = (a.tenant_id(), a.organization_id(), a.project_id());
    let (b_tenant, b_org) = (b.tenant_id(), b.organization_id());
    let key = format!("iso-{}", uuid::Uuid::now_v7());

    // Each closure moves its own copy: `async move` captures by value, so the
    // clone has to happen outside the closure, not inside it.
    let a_for_submit = a.clone();
    let key_for_submit = key.clone();
    // Tenant A submits inside its own scoped transaction: the agent it targets
    // and the execution it creates are both A's.
    let receipt_a = scoped(
        pool.clone(),
        a.tenant_id(),
        a.organization_id(),
        |pool| async move {
            let agent = Agent::create(
                a_tenant,
                a_org,
                a_project,
                "Iso A Agent",
                Slug::new(&format!(
                    "iso-a-agent-{}",
                    uuid::Uuid::now_v7().to_string().replace('-', "")
                ))
                .expect("slug"),
                AgentKind::Standard,
            )
            .expect("agent");
            AgentStore::new(pool.clone())
                .save(&agent)
                .await
                .expect("agent save");
            PgDurableExecution::new(pool)
                .submit(durable_intent(&a_for_submit, agent.id, &key_for_submit))
                .await
                .expect("submit a")
        },
    )
    .await;
    assert_eq!(receipt_a.kind, SubmissionKind::Created);

    // ── read ───────────────────────────────────────────────────────────────
    // Every one of tenant B's attempts runs inside B's own scoped transaction,
    // because that is what a request from tenant B actually is. A test that
    // ran them unscoped would be proving nothing about the deployed path.
    let a_for_read = a.clone();
    let b_for_read = b.clone();
    let (a_read, b_read, b_task, b_list) =
        scoped(pool.clone(), a_tenant, a_org, |pool| async move {
            let store = PgDurableExecution::new(pool);
            (
                store
                    .load_execution(a_for_read.tenant_id(), receipt_a.execution.id)
                    .await
                    .expect("read")
                    .is_some(),
                store
                    .load_execution(b_for_read.tenant_id(), receipt_a.execution.id)
                    .await
                    .expect("read")
                    .is_none(),
                store
                    .load_task(b.tenant_id(), receipt_a.task.id)
                    .await
                    .expect("read")
                    .is_none(),
                store
                    .list_executions(b.tenant_id(), 500)
                    .await
                    .expect("list"),
            )
        })
        .await;
    assert!(a_read, "tenant A reads its own execution");
    assert!(
        b_read,
        "tenant B must not read tenant A's execution: existence is hidden, not merely forbidden"
    );
    assert!(b_task, "tenant B must not load tenant A's task");
    assert!(
        b_list
            .iter()
            .all(|execution| execution.tenant_id == b_tenant),
        "tenant B's listing cannot contain tenant A's executions"
    );

    // ── claim / execute ────────────────────────────────────────────────────
    let stolen = scoped(pool.clone(), b_tenant, b_org, |pool| async move {
        PgDurableExecution::new(pool)
            .claim(ClaimQuery {
                worker: "worker-b".to_owned(),
                capacity: 10,
                lease_until: Timestamp::now(),
                now: Timestamp::now(),
                tenant_id: Some(b_tenant),
                task_id: Some(receipt_a.task.id),
            })
            .await
            .expect("claim")
    })
    .await;
    assert!(
        stolen.is_empty(),
        "tenant B must not claim — and therefore execute — tenant A's task"
    );

    // ── cancel ─────────────────────────────────────────────────────────────
    let cancel = scoped(pool.clone(), b_tenant, b_org, |pool| async move {
        PgDurableExecution::new(pool)
            .cancel(CancellationRequest {
                tenant_id: b_tenant,
                execution_id: receipt_a.execution.id,
                actor: TransitionActor::principal("attacker", b_tenant).expect("actor"),
                correlation_id: "corr-attack".to_owned(),
                now: Timestamp::now(),
            })
            .await
    })
    .await;
    assert!(
        cancel.is_err(),
        "tenant B must not be able to cancel tenant A's execution"
    );

    // ── settle (the result) ────────────────────────────────────────────────
    let owned = scoped(pool.clone(), a_tenant, a_org, |pool| async move {
        PgDurableExecution::new(pool)
            .claim(ClaimQuery {
                worker: "worker-a".to_owned(),
                capacity: 1,
                lease_until: Timestamp::now(),
                now: Timestamp::now(),
                tenant_id: Some(a_tenant),
                task_id: Some(receipt_a.task.id),
            })
            .await
            .expect("claim a")
    })
    .await;
    assert_eq!(owned.len(), 1, "tenant A claims its own task");
    let claim = owned.into_iter().next().expect("one claim");

    let forged = scoped(pool.clone(), b_tenant, b_org, |pool| async move {
        PgDurableExecution::new(pool)
            .settle(TaskSettlement {
                tenant_id: b_tenant,
                task_id: receipt_a.task.id,
                attempt_id: claim.attempt.id,
                worker: "worker-b".to_owned(),
                expected_version: claim.task.version,
                outcome: TaskOutcome::Succeeded {
                    result: serde_json::Value::Null,
                    usage: ExecutionUsage::default(),
                },
                execution: Some(ExecutionFinalization {
                    execution_id: receipt_a.execution.id,
                    outcome: ExecutionOutcome::Completed,
                }),
                actor: TransitionActor::principal("attacker", b_tenant).expect("actor"),
                now: Timestamp::now(),
            })
            .await
    })
    .await;
    assert!(
        forged.is_err(),
        "tenant B must not be able to write a result onto tenant A's task"
    );

    // Tenant A's work is untouched by every one of those attempts.
    let (row, _) = scoped(pool.clone(), a_tenant, a_org, |pool| async move {
        PgDurableExecution::new(pool)
            .load_task(a_tenant, receipt_a.task.id)
            .await
            .expect("read")
            .expect("row")
    })
    .await;
    assert_eq!(row.status, TaskStatus::Running);
    assert_eq!(row.locked_by.as_deref(), Some("worker-a"));
}

#[tokio::test]
async fn the_same_idempotency_key_in_two_tenants_is_two_executions() {
    let Some(pool) = support::live_pool("tenant-isolation-keys").await else {
        return;
    };

    let a = seed_scope(&pool, "key-a").await;
    let b = seed_scope(&pool, "key-b").await;
    let (a_tenant, a_org) = (a.tenant_id(), a.organization_id());
    let (b_tenant, b_org) = (b.tenant_id(), b.organization_id());

    // The scopes share a key deliberately: tenant identity is part of the
    // key's scope, so this must produce two independent executions, not a
    // replay.
    let key = format!("shared-{}", uuid::Uuid::now_v7());

    let a_for_first = a.clone();
    let key_for_first = key.clone();
    let first = scoped(pool.clone(), a_tenant, a_org, |pool| async move {
        let agent = Agent::create(
            a_for_first.tenant_id(),
            a_for_first.organization_id(),
            a_for_first.project_id(),
            "Key A Agent",
            Slug::new(&format!(
                "key-a-agent-{}",
                uuid::Uuid::now_v7().to_string().replace('-', "")
            ))
            .expect("slug"),
            AgentKind::Standard,
        )
        .expect("agent");
        AgentStore::new(pool.clone())
            .save(&agent)
            .await
            .expect("agent save");
        PgDurableExecution::new(pool)
            .submit(durable_intent(&a_for_first, agent.id, &key_for_first))
            .await
            .expect("submit a")
    })
    .await;

    let b_for_second = b.clone();
    let key_for_second = key.clone();
    let second = scoped(pool.clone(), b_tenant, b_org, |pool| async move {
        let agent = Agent::create(
            b_for_second.tenant_id(),
            b_for_second.organization_id(),
            b_for_second.project_id(),
            "Key B Agent",
            Slug::new(&format!(
                "key-b-agent-{}",
                uuid::Uuid::now_v7().to_string().replace('-', "")
            ))
            .expect("slug"),
            AgentKind::Standard,
        )
        .expect("agent");
        AgentStore::new(pool.clone())
            .save(&agent)
            .await
            .expect("agent save");
        PgDurableExecution::new(pool)
            .submit(durable_intent(&b_for_second, agent.id, &key_for_second))
            .await
            .expect("submit b")
    })
    .await;

    assert_eq!(first.kind, SubmissionKind::Created);
    assert_eq!(
        second.kind,
        SubmissionKind::Created,
        "tenant B must not have its request answered with tenant A's execution"
    );
    assert_ne!(first.execution.id, second.execution.id);
    assert_eq!(first.execution.tenant_id, a_tenant);
    assert_eq!(second.execution.tenant_id, b_tenant);

    // And idempotency still holds within each tenant: the unique index is
    // (tenant_id, idempotency_key), so a re-submission replays, never creates.
    let b_for_replay = b.clone();
    let key_for_replay = key.clone();
    let replay = scoped(pool.clone(), b_tenant, b_org, |pool| async move {
        // The same agent is named by the same idempotency key, so the
        // fingerprint matches and the replay is recognised as a replay.
        let listed = AgentStore::new(pool.clone())
            .list_for_tenant(b_tenant)
            .await
            .expect("tenant agents");
        let agent = listed.into_iter().next().expect("the seeded agent");
        PgDurableExecution::new(pool)
            .submit(durable_intent(&b_for_replay, agent.id, &key_for_replay))
            .await
            .expect("replay b")
    })
    .await;
    assert_eq!(replay.kind, SubmissionKind::Replayed);
    assert_eq!(replay.execution.id, second.execution.id);
}

// ─────────────────────────────────────────────────────────────────────────────
// Row-level security, proved against a role that is actually subject to it.
//
// Everything above this line runs on the migrator URL, whose role is a
// superuser. Superusers bypass RLS. That is fine for tests about schema and
// lifecycle — and worthless for tests about isolation, because a policy that
// is switched off cannot fail.
//
// The three tests below therefore connect as `mas_app` (MAS_TEST_DATABASE_APP_URL),
// a role with `rolbypassrls = false`. They are the only assertions in this
// suite that can fail *because row-level security works*.
// ─────────────────────────────────────────────────────────────────────────────

/// Seeds two tenants through the migrator pool and returns their ids.
async fn two_tenants(pool: &sqlx::PgPool, label: &str) -> (TenantId, TenantId) {
    let a = seed_scope(pool, &format!("{label}-rls-a")).await;
    let b = seed_scope(pool, &format!("{label}-rls-b")).await;
    (a.tenant_id(), b.tenant_id())
}

#[tokio::test]
async fn the_runtime_role_cannot_bypass_row_level_security() {
    let Some(pool) = support::live_pool("rls-runtime-role").await else {
        return;
    };
    let Some(app) = support::live_app_pool("rls-runtime-role-app").await else {
        eprintln!(
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\
             live_pg: SKIPPING the row-level-security assertions.\n\
             Set MAS_TEST_DATABASE_APP_URL to a URL for the mas_app role.\n\
             Without it these tests can only pass vacuously, so they are not\n\
             run at all rather than being reported as green.\n\
             ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        );
        return;
    };

    // 0. The precondition every assertion below rests on. If this drifts, the
    //    test suite would keep reporting isolation while having none.
    let (current_user, bypass): (String, bool) = sqlx::query_as(
        "SELECT current_user, rolbypassrls FROM pg_roles WHERE rolname = current_user",
    )
    .fetch_one(&app)
    .await
    .expect("role lookup");
    assert!(
        !bypass,
        "the test role must be subject to RLS, got {current_user}"
    );
    assert!(
        !current_user.starts_with("mas_directory")
            && !current_user.starts_with("mas_worker")
            && !current_user.starts_with("mas_audit"),
        "the runtime role must not be a plane role, got {current_user}"
    );

    let (tenant_a, tenant_b) = two_tenants(&pool, "rolerls").await;

    // 1. No scope ⇒ nothing visible. This is the whole guarantee in one line:
    //    a connection that has not been told which tenant it is serving sees
    //    no tenant's rows, not all of them.
    let mut connection = app.acquire().await.expect("connection");
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM tenants")
        .fetch_one(connection.as_mut())
        .await
        .expect("unscoped count");
    assert_eq!(
        visible, 0,
        "an unscoped runtime connection must see no tenants"
    );

    // 2. Armed inside the transaction ⇒ exactly that tenant, and not the other.
    sqlx::query("BEGIN")
        .execute(connection.as_mut())
        .await
        .expect("begin");
    sqlx::query("SELECT set_config('app.current_tenant', $1, true)")
        .bind(tenant_a.to_string())
        .execute(connection.as_mut())
        .await
        .expect("arm tenant a");
    let seen: Vec<String> = sqlx::query_scalar("SELECT id::text FROM tenants")
        .fetch_all(connection.as_mut())
        .await
        .expect("scoped select");
    assert_eq!(
        seen,
        vec![tenant_a.to_string()],
        "only the scoped tenant is visible"
    );
    sqlx::query("COMMIT")
        .execute(connection.as_mut())
        .await
        .expect("commit");

    // 3. A transaction that names tenant B cannot write to tenant A's rows.
    //    Zero rows affected is the point: the statement succeeded, the policy
    //    decided it applied to nothing.
    sqlx::query("BEGIN")
        .execute(connection.as_mut())
        .await
        .expect("begin");
    sqlx::query("SELECT set_config('app.current_tenant', $1, true)")
        .bind(tenant_b.to_string())
        .execute(connection.as_mut())
        .await
        .expect("arm tenant b");
    let updated = sqlx::query("UPDATE tenants SET name = name WHERE id = $1")
        .bind(uuid::Uuid::from(tenant_a))
        .execute(connection.as_mut())
        .await
        .expect("cross-tenant update");
    assert_eq!(
        updated.rows_affected(),
        0,
        "a cross-tenant UPDATE must affect no rows under FORCE RLS"
    );
    let deleted = sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(uuid::Uuid::from(tenant_a))
        .execute(connection.as_mut())
        .await
        .expect("cross-tenant delete");
    assert_eq!(
        deleted.rows_affected(),
        0,
        "a cross-tenant DELETE must affect no rows under FORCE RLS"
    );
    sqlx::query("ROLLBACK")
        .execute(connection.as_mut())
        .await
        .expect("rollback");

    // 4. The tenant is still there. A "protected" row that quietly vanished
    //    would make the two assertions above pass for the wrong reason.
    let survived: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tenants WHERE id = $1)")
        .bind(uuid::Uuid::from(tenant_a))
        .fetch_one(&pool)
        .await
        .expect("survival check");
    assert!(survived, "tenant A must survive tenant B's write attempts");
}

/// The pooled-connection hazard, reproduced (spec Phase 12).
///
/// One physical connection, two logical requests. Request A arms tenant A with
/// `SET LOCAL` and commits. If that GUC survived the transaction — which is
/// what a session-level `SET` or `set_config(..., false)` does — then request B,
/// reusing the same pooled connection for a different caller, would inherit
/// tenant A's scope and read tenant A's data on tenant B's behalf.
///
/// `support::live_app_pool` hands back a pool with exactly one connection, so
/// the second acquire is guaranteed to be the same socket.
#[tokio::test]
async fn a_pooled_connection_does_not_inherit_the_previous_request_tenant() {
    let Some(pool) = support::live_pool("rls-pool-reuse").await else {
        return;
    };
    let Some(app) = support::live_app_pool("rls-pool-reuse-app").await else {
        eprintln!(
            "live_pg: SKIPPING the pooled-connection leakage test (set MAS_TEST_DATABASE_APP_URL)"
        );
        return;
    };
    let (tenant_a, tenant_b) = two_tenants(&pool, "poolreuse").await;

    // ── Request A ────────────────────────────────────────────────────────
    let mut connection = app.acquire().await.expect("connection");
    sqlx::query("BEGIN")
        .execute(connection.as_mut())
        .await
        .expect("begin");
    sqlx::query("SELECT set_config('app.current_tenant', $1, true)")
        .bind(tenant_a.to_string())
        .execute(connection.as_mut())
        .await
        .expect("arm a");
    let during_a: i64 = sqlx::query_scalar("SELECT count(*) FROM tenants")
        .fetch_one(connection.as_mut())
        .await
        .expect("count in a");
    assert_eq!(during_a, 1, "request A sees exactly its own tenant");
    sqlx::query("COMMIT")
        .execute(connection.as_mut())
        .await
        .expect("commit");
    drop(connection); // returns the socket to the pool

    // ── Request B, on the same socket ────────────────────────────────────
    let mut connection = app.acquire().await.expect("reused connection");
    let leaked: i64 = sqlx::query_scalar("SELECT count(*) FROM tenants")
        .fetch_one(connection.as_mut())
        .await
        .expect("count with no scope");
    assert_eq!(
        leaked, 0,
        "a pooled connection must not carry the previous request's tenant; \
         SET LOCAL must die with the transaction that set it"
    );

    // And B can still be armed correctly on that same socket — proving the
    // assertion above is not simply "the connection is broken now".
    sqlx::query("BEGIN")
        .execute(connection.as_mut())
        .await
        .expect("begin");
    sqlx::query("SELECT set_config('app.current_tenant', $1, true)")
        .bind(tenant_b.to_string())
        .execute(connection.as_mut())
        .await
        .expect("arm b");
    let during_b: Vec<String> = sqlx::query_scalar("SELECT id::text FROM tenants")
        .fetch_all(connection.as_mut())
        .await
        .expect("count in b");
    assert_eq!(
        during_b,
        vec![tenant_b.to_string()],
        "request B sees only its own tenant"
    );
    sqlx::query("ROLLBACK")
        .execute(connection.as_mut())
        .await
        .expect("rollback");
    drop(connection);

    // ── The contrasting case: a session-level GUC does leak ─────────────
    //
    // Recorded as an assertion rather than a comment so the reason `SET LOCAL`
    // is mandatory stays executable. `set_config(..., false)` is session scope;
    // it outlives the transaction.
    let mut connection = app.acquire().await.expect("connection");
    sqlx::query("BEGIN")
        .execute(connection.as_mut())
        .await
        .expect("begin");
    sqlx::query("SELECT set_config('app.current_tenant', $1, false)")
        .bind(tenant_b.to_string())
        .execute(connection.as_mut())
        .await
        .expect("session-level arm");
    sqlx::query("COMMIT")
        .execute(connection.as_mut())
        .await
        .expect("commit");
    drop(connection);

    let mut connection = app.acquire().await.expect("reused connection");
    let after_session: i64 = sqlx::query_scalar("SELECT count(*) FROM tenants")
        .fetch_one(connection.as_mut())
        .await
        .expect("count after session-level set");
    assert_eq!(
        after_session, 1,
        "a session-level GUC survives COMMIT — this is exactly the leak \
         scoped_request avoids by only ever using SET LOCAL"
    );
    // Leave the connection clean: the pool is shared with the other tests.
    sqlx::query("SELECT set_config('app.current_tenant', '', false)")
        .execute(connection.as_mut())
        .await
        .expect("clear");
    drop(connection);
}

/// The plane roles are confined to the tables their plane touches.
///
/// BYPASSRLS is the sharpest privilege in the schema. It is defensible only
/// because it is narrow: `mas_worker` may reach the durable execution tables
/// and nothing else. This test asserts that narrowness, because a grant added
/// later "just for one more query" would otherwise be invisible until it was
/// exploited.
#[tokio::test]
async fn the_plane_roles_are_confined_to_their_own_tables() {
    let Some(pool) = support::live_pool("rls-plane-grants").await else {
        return;
    };
    // Nothing here needs the app role; the grants are catalog facts.

    /// Every table `role` holds any privilege on.
    async fn tables_for(pool: &sqlx::PgPool, role: &str) -> Vec<String> {
        sqlx::query_scalar(
            "SELECT DISTINCT table_name::text
               FROM information_schema.table_privileges
              WHERE grantee = $1
              ORDER BY 1",
        )
        .bind(role)
        .fetch_all(pool)
        .await
        .expect("grant lookup")
    }

    let worker = tables_for(&pool, "mas_worker").await;
    for forbidden in [
        "api_keys",
        "credentials",
        "sessions",
        "connectors",
        "webhooks",
    ] {
        assert!(
            !worker.iter().any(|table| table == forbidden),
            "mas_worker must not hold privileges on `{forbidden}`; \
             a compromised worker would be able to read tenant secrets. \
             It currently holds: {worker:?}"
        );
    }
    assert!(
        worker.iter().any(|table| table == "tasks"),
        "mas_worker must be able to reach `tasks` or it cannot claim work"
    );

    let audit = tables_for(&pool, "mas_audit").await;
    assert_eq!(
        audit,
        vec!["audit_events".to_owned()],
        "mas_audit is an append-only writer of one table, got {audit:?}"
    );

    let outbox = tables_for(&pool, "mas_outbox").await;
    assert_eq!(
        outbox,
        vec!["outbox_events".to_owned()],
        "mas_outbox relays one table, got {outbox:?}"
    );

    let directory = tables_for(&pool, "mas_directory").await;
    for forbidden in ["tasks", "executions", "agents", "api_keys", "credentials"] {
        assert!(
            !directory.iter().any(|table| table == forbidden),
            "mas_directory must not hold privileges on `{forbidden}`; it exists \
             to read the tenancy hierarchy only. It currently holds: {directory:?}"
        );
    }
    for required in ["organizations", "tenants", "projects", "memberships"] {
        assert!(
            directory.iter().any(|table| table == required),
            "mas_directory must be able to read `{required}` to resolve tenancy"
        );
    }

    let scheduler = tables_for(&pool, "mas_scheduler").await;
    for forbidden in ["tasks", "executions", "agents", "api_keys"] {
        assert!(
            !scheduler.iter().any(|table| table == forbidden),
            "mas_scheduler must not hold privileges on `{forbidden}`, got {scheduler:?}"
        );
    }
    assert!(
        scheduler.iter().any(|table| table == "schedules"),
        "mas_scheduler must be able to read `schedules` to scan for due work"
    );

    // The runtime role is the one that matters most: it must not be able to
    // switch row-level security off, ever.
    let bypass: bool =
        sqlx::query_scalar("SELECT rolbypassrls FROM pg_roles WHERE rolname = 'mas_app'")
            .fetch_one(&pool)
            .await
            .expect("role lookup");
    assert!(!bypass, "mas_app must never hold BYPASSRLS");
}
