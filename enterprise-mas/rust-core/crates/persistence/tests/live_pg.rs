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
    AgentId, ExecutionId, OrganizationId, ProjectId, ScheduleId, TaskId, TenantId, UserId,
};
use mas_common::pagination::PageRequest;
use mas_common::timestamps::Timestamp;
use mas_domain::audit_event::{AuditActor, AuditActorKind, AuditEvent, AuditOutcome};
use mas_domain::value_objects::Slug;
use mas_domain::{
    Agent, AgentKind, AgentVersion, Execution, IsolationMode, Organization, Project, Schedule,
    ScheduleKind, Task, Tenant, Workflow,
};
use mas_persistence::repositories::agents::{AgentStore, AgentVersionStore};
use mas_persistence::repositories::api_keys::ApiKeyStore;
use mas_persistence::repositories::audit::PostgresAuditLog;
use mas_persistence::repositories::broker::PgTaskBroker;
use mas_persistence::repositories::executions::ExecutionStore;
use mas_persistence::repositories::schedules::ScheduleStore;
use mas_persistence::repositories::tasks::TaskStore;
use mas_persistence::repositories::tenancy::{OrganizationStore, ProjectStore, TenantStore};
use mas_persistence::repositories::workflows::WorkflowStore;

/// The standard org → tenant → project fixture, seeded through the tenancy
/// stores (the same code paths production uses).
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
    let suffix = &uuid::Uuid::now_v7().to_string()[..8];
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
    assert_eq!(
        applied.len(),
        mas_persistence::migrations::MIGRATIONS.len(),
        "registry and ledger agree"
    );
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
    let store = AgentStore::new(pool.clone());

    let agent = Agent::create(
        scope.tenant_id(),
        scope.organization_id(),
        scope.project_id(),
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
        .get_by_slug(scope.tenant_id(), agent.slug.as_str())
        .await
        .expect("get by slug")
        .expect("slug row");
    assert_eq!(by_slug.id, agent.id);

    let for_project = store
        .list_for_project(scope.project_id())
        .await
        .expect("project list");
    assert!(for_project.iter().any(|a| a.id == agent.id));
    let for_tenant = store
        .list_for_tenant(scope.tenant_id())
        .await
        .expect("tenant list");
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
    versions
        .save(&version, scope.tenant_id())
        .await
        .expect("version save");
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
}

#[tokio::test]
async fn workflows_round_trip() {
    let Some(pool) = support::live_pool("workflows-round-trip").await else {
        return;
    };
    let scope = seed_scope(&pool, "workflows").await;
    let store = WorkflowStore::new(pool);

    let workflow = Workflow::create(
        scope.tenant_id(),
        scope.organization_id(),
        scope.project_id(),
        "Nightly Rollup",
    )
    .expect("workflow");
    store.save(&workflow).await.expect("workflow save");

    let loaded = store
        .get(workflow.id)
        .await
        .expect("workflow get")
        .expect("row");
    assert_eq!(loaded.name, "Nightly Rollup");
    assert_eq!(loaded.status, workflow.status);

    let project_list = store
        .list_for_project(scope.project_id())
        .await
        .expect("project list");
    assert!(project_list.iter().any(|w| w.id == workflow.id));
    let tenant_list = store
        .list_for_tenant(scope.tenant_id())
        .await
        .expect("tenant list");
    assert!(tenant_list.iter().any(|w| w.id == workflow.id));

    // Status transitions persist when legal.
    let mut updated = loaded;
    if let Ok(_version) = updated.publish() {
        store.save(&updated).await.expect("workflow re-save");
        let reloaded = store.get(workflow.id).await.expect("get").expect("row");
        assert_eq!(reloaded.status, WorkflowStatus::Published);
    }
}

#[tokio::test]
async fn executions_round_trip_and_idempotency() {
    let Some(pool) = support::live_pool("executions-round-trip").await else {
        return;
    };
    let scope = seed_scope(&pool, "executions").await;
    let store = ExecutionStore::new(pool);

    let execution = Execution::new(
        scope.tenant_id(),
        scope.organization_id(),
        scope.project_id(),
        None,
        Some(AgentId::new()),
        serde_json::json!({"prompt": "ping"}),
        format!("corr-{}", uuid::Uuid::now_v7()),
        "itest-user",
    )
    .expect("execution");
    store.save(&execution).await.expect("execution save");

    let loaded = store.get(execution.id).await.expect("get").expect("row");
    assert_eq!(loaded.correlation_id, execution.correlation_id);
    assert_eq!(loaded.agent_id, execution.agent_id);

    let tenant_list = store
        .list_for_tenant(scope.tenant_id())
        .await
        .expect("tenant list");
    assert!(tenant_list.iter().any(|e| e.id == execution.id));

    // Idempotency slot: put → get hit; a different key misses.
    let key = format!("idem-{}", uuid::Uuid::now_v7());
    let miss = store
        .idempotency_get(scope.tenant_id(), &key)
        .await
        .expect("miss get");
    assert!(miss.is_none());
    store
        .idempotency_put(scope.tenant_id(), &key, execution.id)
        .await
        .expect("put");
    let hit: ExecutionId = store
        .idempotency_get(scope.tenant_id(), &key)
        .await
        .expect("hit get")
        .expect("key present");
    assert_eq!(hit, execution.id);
    // Second put is upsert-safe and idempotent.
    store
        .idempotency_put(scope.tenant_id(), &key, execution.id)
        .await
        .expect("second put");
}

#[tokio::test]
async fn schedules_due_scan_and_run_journal() {
    let Some(pool) = support::live_pool("schedules-due-scan").await else {
        return;
    };
    let scope = seed_scope(&pool, "schedules").await;
    let store = ScheduleStore::new(pool.clone());

    let mut schedule = Schedule::new(
        scope.tenant_id(),
        Some(scope.project_id()),
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

    // Time travel (test scaffolding only): force the row due.
    sqlx::query("UPDATE schedules SET next_run_at = now() - interval '5 seconds' WHERE id = $1")
        .bind(uuid::Uuid::from(schedule.id))
        .execute(&pool)
        .await
        .expect("time travel");
    let due = store.list_due(&now, 25).await.expect("list_due after");
    assert!(due.iter().any(|s| s.id == schedule.id));

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
    let _distinct = ScheduleId::new();
}

#[tokio::test]
async fn tasks_round_trip() {
    let Some(pool) = support::live_pool("tasks-round-trip").await else {
        return;
    };
    let scope = seed_scope(&pool, "tasks").await;
    let store = TaskStore::new(pool);

    let task = Task::new(
        scope.tenant_id(),
        scope.organization_id(),
        scope.project_id(),
        None,
        None,
        "execution.run",
        serde_json::json!({"itest": true}),
        TaskPriority::Normal,
        format!("itask-{}", uuid::Uuid::now_v7()),
        "itest-worker",
    )
    .expect("task");
    let mut row = task.clone();
    row.queue().expect("queue transition");
    store.save(&row).await.expect("task save");

    let loaded = store.load(row.id).await.expect("task load").expect("row");
    assert!(matches!(
        loaded.status,
        TaskStatus::Queued | TaskStatus::Pending
    ));
    assert_eq!(loaded.idempotency_key, row.idempotency_key);

    // Status evolution persists.
    let mut running = loaded;
    running.start().expect("start transition");
    store.save(&running).await.expect("re-save");
    let started = store.load(row.id).await.expect("load").expect("row");
    assert!(matches!(started.status, TaskStatus::Running));
    let _distinct = TaskId::new();
}

#[tokio::test]
async fn api_keys_prefix_lookup_and_touch() {
    let Some(pool) = support::live_pool("api-keys-lookup").await else {
        return;
    };
    let scope = seed_scope(&pool, "apikeys").await;

    // api_keys is lookup-only from the store's perspective (the mint CLI owns
    // inserts); seed a row directly, then exercise the read paths.
    let key_id = uuid::Uuid::now_v7();
    let raw = format!("mas_{}", "f".repeat(64));
    let prefix = support::visible_prefix(&raw);
    let digest = support::sha256_hex(raw.as_bytes());
    sqlx::query(
        "INSERT INTO api_keys (id, tenant_id, name, prefix, secret_hash, scopes, status)
         VALUES ($1, $2, $3, $4, $5, '[]'::jsonb, 'active')",
    )
    .bind(key_id)
    .bind(uuid::Uuid::from(scope.tenant_id()))
    .bind(format!("itest-{}", &key_id.to_string()[..8]))
    .bind(&prefix)
    .bind(&digest)
    .execute(&pool)
    .await
    .expect("api key seed");

    let store = ApiKeyStore::new(pool);
    let candidates = store.find_by_prefix(&prefix).await.expect("prefix lookup");
    let row = candidates
        .iter()
        .find(|c| c.id == key_id)
        .expect("seeded row among prefix candidates");
    assert_eq!(row.status, "active");
    assert_eq!(row.secret_hash, digest);

    store.touch_last_used(key_id).await.expect("touch");
    let after = store.find_by_prefix(&prefix).await.expect("re-read");
    let touched = after.iter().find(|c| c.id == key_id).expect("row");
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
        .list_for_correlation(&correlation)
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
    let broker = PgTaskBroker::new(pool);

    use mas_messaging::broker::{AckInstruction, BrokerPort, ConsumerConfig, PublishRequest};
    use mas_messaging::codec::{CodecConfig, JsonEventCodec};
    use mas_messaging::headers::HeaderSet;
    use mas_messaging::subjects::{Subject, SubjectFilter};

    let task_uuid = uuid::Uuid::now_v7();
    let idempotency_key = format!("broker-{}", uuid::Uuid::now_v7());
    let message = mas_contracts::task::TaskQueueMessage::staged(
        TaskId::from(task_uuid),
        scope.tenant_id(),
        scope.organization_id(),
        scope.project_id(),
        "execution.run".to_owned(),
        serde_json::json!({"itest": "broker"}),
        None,
        None,
        None,
        idempotency_key.clone(),
        TaskPriority::Normal,
        1,
        3,
        None,
        &format!("frame-{}", uuid::Uuid::now_v7()),
    )
    .expect("staged message");
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

    let batch = broker.fetch(&consumer, 5).await.expect("fetch");
    let ours = batch
        .iter()
        .find(|m| m.sequence == seq)
        .expect("our task arrives");
    let decoded: mas_contracts::task::TaskQueueMessage =
        codec.decode_value(&ours.payload).expect("decode");
    assert_eq!(*decoded.task_id.as_uuid(), task_uuid);

    broker
        .ack(&consumer, seq, AckInstruction::Ack)
        .await
        .expect("ack settles");

    let again = broker.fetch(&consumer, 5).await.expect("fetch again");
    assert!(
        !again.iter().any(|m| m.sequence == seq),
        "acked task is not re-delivered within the ack window"
    );
}
