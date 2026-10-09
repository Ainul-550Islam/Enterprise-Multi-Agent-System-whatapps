// PURPOSE: The database-level half of the multi-tenant isolation guarantee:
// a static contract test over the SQL the durable pipeline actually issues.
//
// Owning layer: `mas-persistence` (the production repositories).
//
// Production responsibility: behavioural tests prove that tenant B is refused
// *today*. They cannot prove that the next statement someone adds is scoped.
// This file closes that gap: it reads the repository source, extracts every SQL
// statement, and asserts that any statement touching a tenant-owned table
// carries a tenant predicate — or is named here, with the reason it is safe.
//
// It is deliberately a *contract* test rather than a live query test, so it
// runs on every `cargo test` with no database at all. The live query half of
// the same guarantee is `tests/live_pg.rs`, which runs against a real
// PostgreSQL when `MAS_TEST_DATABASE_URL` is set.
//
// What it enforces:
//   1. every statement naming a tenant-owned table scopes by `tenant_id`,
//      unless it appears in [`ALLOWED_UNSCOPED`] with a written reason;
//   2. the durable schema carries the constraints the pipeline's correctness
//      depends on (tenant-scoped idempotency uniqueness, the one-open-attempt
//      fence, the claim index, outbox claim indexes, usage anti-double-metering);
//   3. no migration in the set is destructive.
//
// It must NOT:
//   • become a place where violations are parked. Every entry in
//     [`ALLOWED_UNSCOPED`] states *why* the statement is safe, and the reason
//     must be a property of the statement (it runs under a tenant-scoped row
//     lock, it is keyed by an ownership fence, it is a global counter by
//     design), never "it is hard to fix";
//   • connect to a database. It is a source-level test.

use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// What is tenant-owned
// ---------------------------------------------------------------------------

/// Tables that hold one tenant's data and must never be read or written
/// without a tenant predicate.
const TENANT_OWNED_TABLES: [&str; 6] = [
    "executions",
    "tasks",
    "task_attempts",
    "outbox_events",
    "usage_records",
    "audit_events",
];

/// A statement that names a tenant-owned table without a tenant predicate,
/// together with the reason that is nevertheless correct.
///
/// Adding an entry here is a design decision and must be justified in the
/// `why` string. Three reasons recur, and they are the only ones in use:
///
/// * **row-locked** — the statement runs inside the same transaction as a
///   `SELECT ... FOR UPDATE WHERE id = $1 AND tenant_id = $2` on the same row,
///   so the tenant was checked while the row was locked and cannot change
///   between the check and the write;
/// * **ownership-fenced** — the statement is keyed by `worker_id`/`locked_by`,
///   and holding the fence is itself proof of a prior tenant-scoped claim;
/// * **global-by-design** — a counter or gauge that is intentionally summed
///   across all tenants (backlog, capacity, claimable work).
struct Allowed {
    /// A distinctive substring identifying the statement.
    matches: &'static str,
    /// Why this statement is safe without a tenant predicate.
    why: &'static str,
}

const ALLOWED_UNSCOPED: [Allowed; 7] = [
    Allowed {
        matches: "FROM task_attempts WHERE id = $1",
        why: "row-locked: the attempt is loaded by id immediately after its \
              task was loaded with `WHERE id = $1 AND tenant_id = $2` in the \
              same transaction, so the tenant was already checked",
    },
    Allowed {
        matches: "UPDATE tasks SET version = $2",
        why: "row-locked: the settlement writes this row only after a \
              `SELECT ... FOR UPDATE ... WHERE id = $1 AND tenant_id = $2` \
              plus a version and ownership check inside the same transaction",
    },
    Allowed {
        matches: "UPDATE executions SET status = 'running'",
        why: "row-locked: the execution id came from the tenant-scoped claim \
              CTE that selected the task in the same transaction",
    },
    Allowed {
        matches: "UPDATE tasks SET locked_until = $3",
        why: "ownership-fenced: renewing a lease requires `locked_by` and \
              `status = 'running'`, which only a worker that already passed a \
              tenant-scoped claim can hold",
    },
    Allowed {
        matches: "UPDATE task_attempts SET lease_expires = $3",
        why: "ownership-fenced: keyed by (task_id, worker_id, status='running'), \
              the same fence the lease renewal holds",
    },
    Allowed {
        matches: "SELECT COUNT(*) FROM task_attempts WHERE worker_id = $1",
        why: "global-by-design: active-attempt capacity is per worker, and a \
              worker is a deployment-level identity, not a tenant one",
    },
    Allowed {
        matches: "SELECT COUNT(*) FROM outbox_events WHERE status IN",
        why: "global-by-design: the outbox backlog is a relay health signal \
              summed across all tenants; the relay never reads payloads",
    },
];

/// Statements that intentionally span tenants because they are the *scheduler's*
/// view of all work, not one tenant's view.
const ALLOWED_GLOBAL: [&str; 1] = ["SELECT COUNT(*) FROM tasks WHERE status IN"];

// ---------------------------------------------------------------------------
// Extracting the statements
// ---------------------------------------------------------------------------

/// The repository root, resolved from this file's location.
fn workspace_file(relative: &str) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let candidate = root.join(relative);
    assert!(
        candidate.exists(),
        "expected {relative} to exist relative to {}",
        root.display()
    );
    candidate
}

/// Every string literal in a Rust source file.
///
/// `format!` bodies are plain literals before interpolation, so this captures
/// the SQL in both `sqlx::query("...")` and `sqlx::query(&format!("..."))`.
fn string_literals(source: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = source.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            let mut j = i + 1;
            let mut text = String::new();
            while j < bytes.len() {
                match bytes[j] {
                    b'\\' => {
                        if j + 1 < bytes.len() && bytes[j + 1] == b'"' {
                            text.push('"');
                        }
                        j += 2;
                    },
                    b'"' => break,
                    _ => {
                        // Source is ASCII SQL plus Rust; byte-wise slicing on a
                        // char boundary is safe for the files under test.
                        text.push(source[j..].chars().next().expect("char"));
                        j += 1;
                    },
                }
            }
            i = j + 1;
            out.push(text);
        } else {
            i += 1;
        }
    }
    out
}

/// A SQL statement pulled out of the repository source.
#[derive(Debug)]
struct Statement {
    /// Whitespace-collapsed SQL.
    sql: String,
    /// The tenant-owned tables it names.
    owned: Vec<&'static str>,
}

fn looks_like_sql(text: &str) -> bool {
    let upper = text.to_ascii_uppercase();
    ["SELECT", "INSERT INTO", "UPDATE", "DELETE FROM", "WITH "]
        .iter()
        .any(|keyword| upper.contains(keyword))
}

fn statements(source: &str) -> Vec<Statement> {
    string_literals(source)
        .into_iter()
        .filter(|text| looks_like_sql(text))
        .map(|text| {
            let sql = text.split_whitespace().collect::<Vec<_>>().join(" ");
            let mut owned = Vec::new();
            for table in TENANT_OWNED_TABLES {
                let pattern = format!("{table} ");
                if sql.contains(&pattern) || sql.ends_with(table) {
                    owned.push(table);
                }
            }
            Statement { sql, owned }
        })
        .filter(|statement| !statement.owned.is_empty())
        .collect()
}

// ---------------------------------------------------------------------------
// 1. Every tenant-owned statement is scoped — or is named, with a reason
// ---------------------------------------------------------------------------

#[test]
fn every_statement_naming_a_tenant_owned_table_is_tenant_scoped_or_allowlisted() {
    let source = std::fs::read_to_string(workspace_file("src/durable.rs"))
        .expect("read crates/persistence/src/durable.rs");

    let statements = statements(&source);
    assert!(
        statements.len() >= 20,
        "the extractor should find the durable pipeline's statements, found {}",
        statements.len()
    );

    for statement in &statements {
        if statement.sql.to_ascii_lowercase().contains("tenant_id") {
            continue;
        }
        let allowed = ALLOWED_UNSCOPED
            .iter()
            .any(|entry| statement.sql.contains(entry.matches))
            || ALLOWED_GLOBAL
                .iter()
                .any(|entry| statement.sql.contains(entry));
        assert!(
            allowed,
            "statement names tenant-owned table(s) {owned:?} with no tenant predicate and is \
             not allowlisted. Either scope it by tenant_id or add it to ALLOWED_UNSCOPED in \
             this file with a written reason.\n\n    {sql}",
            owned = statement.owned,
            sql = statement.sql
        );
    }
}

#[test]
fn every_allowlist_entry_still_matches_a_real_statement() {
    // A stale entry is a silent hole: it would license a statement that no
    // longer exists while someone else's new unscoped statement gets waved
    // through by a rule nobody remembers writing.
    let source = std::fs::read_to_string(workspace_file("src/durable.rs"))
        .expect("read crates/persistence/src/durable.rs");
    let statements = statements(&source);

    for entry in &ALLOWED_UNSCOPED {
        assert!(
            statements
                .iter()
                .any(|statement| statement.sql.contains(entry.matches)),
            "allowlist entry {:?} no longer matches any statement; remove it",
            entry.matches
        );
        assert!(
            !entry.why.trim().is_empty(),
            "allowlist entry {:?} has no written reason",
            entry.matches
        );
    }
    for entry in ALLOWED_GLOBAL {
        assert!(
            statements
                .iter()
                .any(|statement| statement.sql.contains(entry)),
            "global allowlist entry {entry:?} no longer matches any statement; remove it"
        );
    }
}

#[test]
fn the_idempotency_lookup_is_tenant_scoped_and_uses_the_unique_index() {
    // The single most important tenant-scoped statement in the pipeline: the
    // DB-enforced idempotency lookup. If this were scoped by key alone, one
    // tenant could consume another tenant's key.
    let source = std::fs::read_to_string(workspace_file("src/durable.rs"))
        .expect("read crates/persistence/src/durable.rs");
    let idempotent: Vec<_> = statements(&source)
        .into_iter()
        .filter(|statement| statement.sql.contains("idempotency_key = $2"))
        .collect();

    assert!(
        idempotent.len() >= 2,
        "both the execution and the task idempotency lookup must exist, found {}",
        idempotent.len()
    );
    for statement in &idempotent {
        assert!(
            statement.sql.contains("tenant_id = $1"),
            "an idempotency lookup must be scoped by tenant first: {}",
            statement.sql
        );
    }
}

#[test]
fn the_claim_statement_scopes_by_tenant_before_it_locks_anything() {
    let source = std::fs::read_to_string(workspace_file("src/durable.rs"))
        .expect("read crates/persistence/src/durable.rs");
    let claim = statements(&source)
        .into_iter()
        .find(|statement| {
            statement.sql.contains("FOR UPDATE SKIP LOCKED")
                && statement.sql.contains("UPDATE tasks t SET")
        })
        .expect("the durable claim-and-fence statement must exist");

    assert!(
        claim.sql.contains("tenant_id = $2"),
        "a claim must be able to scope by tenant: {}",
        claim.sql
    );
    assert!(
        claim.sql.contains("FOR UPDATE SKIP LOCKED"),
        "the claim must skip locked rows so replicas never block each other"
    );
    assert!(
        claim.sql.contains("UPDATE tasks t SET"),
        "the fence must be installed by the same statement that selects the row: {}",
        claim.sql
    );
}

#[test]
fn the_settlement_loads_the_task_for_update_under_a_tenant_predicate() {
    // Without `FOR UPDATE` the version fence is a check-then-write race; two
    // settlements could both read version 3 and both write version 4.
    let source = std::fs::read_to_string(workspace_file("src/durable.rs"))
        .expect("read crates/persistence/src/durable.rs");
    assert!(
        statements(&source).iter().any(|statement| {
            statement
                .sql
                .contains("FROM tasks WHERE id = $1 AND tenant_id = $2")
                && statement.sql.contains("FOR UPDATE")
        }),
        "settlement must load the task FOR UPDATE under a tenant predicate"
    );
}

// ---------------------------------------------------------------------------
// 2. The schema carries the constraints correctness depends on
// ---------------------------------------------------------------------------

/// The durable pipeline migration.
fn migration_sql() -> String {
    std::fs::read_to_string(workspace_file(
        "../../migrations/0014_durable_pipeline/up.sql",
    ))
    .expect("read migrations/0014_durable_pipeline/up.sql")
}

#[test]
fn idempotency_is_unique_per_tenant_in_the_database() {
    // Spec: idempotency is enforced by the database, not by a
    // SELECT-then-INSERT. The unique index is the enforcement.
    let sql = migration_sql();
    assert!(
        sql.contains("CREATE UNIQUE INDEX IF NOT EXISTS executions_idempotency_idx")
            && sql.contains("ON executions (tenant_id, idempotency_key)"),
        "the idempotency key must be unique per tenant, enforced by the database"
    );
}

#[test]
fn duplicate_parallel_attempts_are_unrepresentable() {
    let sql = migration_sql();
    assert!(
        sql.contains("CREATE UNIQUE INDEX IF NOT EXISTS task_attempts_one_open_per_task")
            && sql.contains("ON task_attempts (task_id)")
            && sql.contains("WHERE status = 'running'"),
        "two workers must not be able to hold an open attempt on one task: the \
         database, not the application, is what makes that unrepresentable"
    );
}

#[test]
fn the_claim_hot_path_and_ownership_fences_are_indexed() {
    let sql = migration_sql();
    for (index, table) in [
        ("tasks_claim_idx", "tasks"),
        ("tasks_tenant_status_idx", "tasks"),
        ("tasks_owner_idx", "tasks"),
        ("tasks_lease_expiry_idx", "tasks"),
        ("outbox_events_claim_idx", "outbox_events"),
        ("outbox_events_stale_claim_idx", "outbox_events"),
        ("task_attempts_worker_active_idx", "task_attempts"),
    ] {
        assert!(
            sql.contains(&format!("CREATE INDEX IF NOT EXISTS {index}"))
                || sql.contains(&format!("CREATE UNIQUE INDEX IF NOT EXISTS {index}")),
            "missing {index} on {table}: the claim and lease-recovery paths must be indexed"
        );
    }
}

#[test]
fn usage_cannot_be_double_metered_for_one_task() {
    let sql = migration_sql();
    assert!(
        sql.contains("CREATE UNIQUE INDEX IF NOT EXISTS usage_records_task_dimension_idx"),
        "usage must be fenced against a duplicate write for the same task and dimension: \
         a retried settlement must never bill twice"
    );
}

#[test]
fn version_columns_exist_for_optimistic_concurrency() {
    let sql = migration_sql();
    assert!(
        sql.contains("ADD COLUMN IF NOT EXISTS version             bigint NOT NULL DEFAULT 1"),
        "executions and tasks need a version column for the optimistic fence"
    );
}

#[test]
fn api_key_prefix_lookup_is_narrow_and_remains_under_force_rls() {
    let identity = std::fs::read_to_string(workspace_file("../../migrations/0003_identity/up.sql"))
        .expect("read api key table migration");
    let lookup = std::fs::read_to_string(workspace_file(
        "../../migrations/0032_api_key_prefix_lookup/up.sql",
    ))
    .expect("read api key prefix lookup migration");
    let repository = std::fs::read_to_string(workspace_file("src/repositories/api_keys.rs"))
        .expect("read API-key repository");

    assert!(identity.contains("ALTER TABLE api_keys ENABLE ROW LEVEL SECURITY"));
    assert!(identity.contains("ALTER TABLE api_keys FORCE ROW LEVEL SECURITY"));
    assert!(identity.contains("tenant_id::text = current_setting('app.current_tenant', true)"));
    assert!(lookup.contains("CREATE POLICY api_keys_prefix_lookup"));
    assert!(lookup.contains("ON api_keys\n    FOR SELECT"));
    assert!(lookup.contains("prefix = current_setting('app.api_key_prefix_lookup', true)"));
    assert!(lookup.contains("~ '^mas_[0-9a-f]{8}$'"));
    assert!(!lookup.contains("CREATE ROLE"));
    assert!(!lookup.contains("ALTER ROLE"));

    assert!(repository.contains("SELECT set_config($1, $2, true)"));
    assert!(repository.contains("app.api_key_prefix_lookup"));
    assert!(repository.contains("FROM api_keys WHERE prefix = $1"));
    assert!(repository.contains(".fetch_all(&mut *transaction)"));
    assert!(repository.contains("transaction.commit().await"));
    assert!(repository.contains("set_config('app.current_tenant', $1, true)"));
    assert!(repository
        .contains("UPDATE api_keys SET last_used_at = now() WHERE id = $1 AND tenant_id = $2"));
}

// ---------------------------------------------------------------------------
// 3. No migration in the set is destructive
// ---------------------------------------------------------------------------

#[test]
fn no_migration_is_destructive() {
    let migrations_dir = workspace_file("../../migrations");
    let mut checked = 0;
    for entry in std::fs::read_dir(&migrations_dir).expect("read migrations") {
        let entry = entry.expect("entry");
        let up = entry.path().join("up.sql");
        if !up.exists() {
            continue;
        }
        let sql = std::fs::read_to_string(&up).expect("read up.sql");
        let upper = sql.to_ascii_uppercase();
        for forbidden in ["DROP TABLE", "DROP COLUMN", "TRUNCATE", "DELETE FROM"] {
            assert!(
                !upper.contains(forbidden),
                "{} contains `{forbidden}`: a production migration must be additive",
                up.display()
            );
        }
        checked += 1;
    }
    assert!(
        checked >= 14,
        "expected to check the whole migration set, checked {checked}"
    );
}

#[test]
fn oauth_lifecycle_jobs_are_fenced_rls_protected_and_path_blind() {
    let refresh_migration = std::fs::read_to_string(workspace_file(
        "../../migrations/0044_oauth_token_refresh_jobs/up.sql",
    ))
    .expect("read OAuth refresh migration");
    let cleanup_migration = std::fs::read_to_string(workspace_file(
        "../../migrations/0045_oauth_token_cleanup_jobs/up.sql",
    ))
    .expect("read OAuth cleanup migration");
    let opaque_migration = std::fs::read_to_string(workspace_file(
        "../../migrations/0046_oauth_opaque_secret_locators/up.sql",
    ))
    .expect("read OAuth opaque locator migration");
    let materialization_migration = std::fs::read_to_string(workspace_file(
        "../../migrations/0047_oauth_materialization_reconciliation/up.sql",
    ))
    .expect("read OAuth materialization migration");
    let invariants_migration = std::fs::read_to_string(workspace_file(
        "../../migrations/0048_oauth_reference_invariants/up.sql",
    ))
    .expect("read OAuth reference invariant migration");
    let refresh_repository =
        std::fs::read_to_string(workspace_file("src/repositories/oauth_refresh.rs"))
            .expect("read OAuth refresh repository");
    let cleanup_repository =
        std::fs::read_to_string(workspace_file("src/repositories/oauth_cleanup.rs"))
            .expect("read OAuth cleanup repository");
    let materialization_repository =
        std::fs::read_to_string(workspace_file("src/repositories/oauth_materialization.rs"))
            .expect("read OAuth materialization repository");

    for (migration, table) in [
        (&refresh_migration, "oauth_token_refresh_jobs"),
        (&cleanup_migration, "oauth_token_cleanup_jobs"),
        (
            &materialization_migration,
            "oauth_token_materialization_jobs",
        ),
    ] {
        assert!(migration.contains(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")));
        assert!(migration.contains(&format!("ALTER TABLE {table} FORCE ROW LEVEL SECURITY")));
        assert!(migration.contains("tenant_id::text = current_setting('app.current_tenant', true)"));
        assert!(migration.contains("fence bigint NOT NULL DEFAULT 0"));
    }
    for repository in [
        &refresh_repository,
        &cleanup_repository,
        &materialization_repository,
    ] {
        assert!(
            repository.contains("FOR UPDATE OF job SKIP LOCKED"),
            "each OAuth worker claim must lock only its job row with SKIP LOCKED"
        );
        assert!(repository.contains("lease_expires_at > now()"));
        assert!(repository.contains("reap_expired_exhausted_leases"));
    }
    assert!(refresh_repository.contains("oauth_token_cleanup_jobs"));
    assert!(cleanup_repository.contains("availability = 'deleted'"));
    assert!(opaque_migration
        .contains("REVOKE ALL PRIVILEGES ON secret_references FROM mas_oauth_refresh"));
    assert!(opaque_migration.contains("oauth-token/' || reference.id::text"));
    assert!(invariants_migration.contains("mas_assert_oauth_credential_reference"));
    assert!(invariants_migration.contains("mas_assert_oauth_pkce_reference"));
}
