# Upgrade & migrations runbook

## Migration ordering

Migrations apply in strictly ascending version order (0001–0053), each inside
its own transaction, with the ledger row committed atomically
(`crates/persistence/src/migrations.rs`). The runner refuses:

* database ahead of the binary (unknown applied version);
* a gap in the applied prefix;
* checksum drift on an already-applied migration.

No `DROP TABLE` / `DROP COLUMN` / `TRUNCATE` is permitted in the set.

## Up/down policy

Up migrations are additive. Down migrations exist only where the original
migration had one (0018–0051); use for rollback testing, not production
degrade.

## Zero-downtime constraints

* Run `mas_migrate up` (one-shot migrate image/Job) before deploying new code
  so old code never sees an un-applied schema.
* The advisory lock serializes concurrent boots.
* `MAS_DATABASE_MIGRATE_ON_BOOT` enables the deliberate apply path at boot;
  otherwise the binary verifies the ledger and fail-fasts on drift.

## Using mas_migrate

```bash
scripts/migrate.sh plan     # SHA-256 manifest preview (no DB contact)
scripts/migrate.sh up       # apply pending (requires DATABASE_URL)
scripts/migrate.sh status   # reconcile against the database
```

## Rollback limits

Rollback is bounded to migrations that shipped a `down.sql`. After a forward
deploy with new application code, do not roll back migrations below the
version the running binary expects (ledger verification fails).
