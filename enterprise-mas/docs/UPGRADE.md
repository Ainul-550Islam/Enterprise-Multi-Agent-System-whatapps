# Upgrade and migration policy

## Before an upgrade

1. Read the release notes and deployment changes for the target version.
2. Take and verify a restorable database backup.
3. Confirm the target application image and migration bundle are from the same release.
4. Plan a maintenance or rollout window appropriate to the schema changes.

## Apply an upgrade

1. Deploy the target release's migration runner with the required database credentials.
2. Apply registered migrations in order; the runner is forward-only and records checksums.
3. Confirm the runner completed and inspect `mas_schema_migrations` for the expected registered versions and checksums.
4. Deploy the matching application services and run documented health checks.
5. Keep the prior application image and verified database backup available until the upgrade is accepted.

Do not run an older binary against a database that contains migrations unknown to that binary. The runner intentionally treats that state as an error.

## Repository verification

The migration registry currently ends at version 0064. The Rust integration test `crates/persistence/tests/migration_registry.rs` checks migration-directory registration, unique numeric prefixes, `up.sql` presence, and the required `down.sql` files for versions 0042 onward. Run `python scripts/dev/verify_manifest.py --tier full` and the Rust migration-registry test after changing migration files. A passing repository check does not replace testing against a production-like database or a verified backup.
