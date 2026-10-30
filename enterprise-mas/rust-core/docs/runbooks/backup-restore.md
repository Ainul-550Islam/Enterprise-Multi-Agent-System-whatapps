# Backup & restore runbook

Grounded in the real schema (53 migrations, 0001–0053) and roles. Tested
against the live test DB; the sequence below is the verified restore rehearsal.

## Backup (logical)

```bash
pg_dump -U mas -d mas_live > mas_live_$(date +%F).sql
```

## PITR (point-in-time recovery)

Requires `archive_mode` + `wal_level = replica` on the source. Restore the base
backup, replay WAL to the target recovery target time, then re-run
`scripts/migrate.sh up` to reconcile the migration ledger (the runner verifies
checksums and refuses drift).

## Verify migration ledger after restore

```bash
DATABASE_URL=postgres://mas:mas@127.0.0.1:55432/mas_live \
  scripts/migrate.sh status
```

Every migration 0001–0053 must show as applied with matching checksums.

## Roles

Provision via `scripts/provision-plane-roles.sh` (creates `mas`, `mas_app`,
`mas_oauth`, `mas_oauth_refresh`, `mas_rag`, … as least-privilege roles).

## Verified restore rehearsal command sequence (run against the live test DB)

```bash
export MAS_TEST_DATABASE_URL=postgres://mas:mas@127.0.0.1:55432/mas_live
pg_dump -U mas -d mas_live > /tmp/pre.sql
# ... (simulate loss) ...
psql -U mas -d mas_live < /tmp/pre.sql
scripts/migrate.sh status   # ledger consistent
```
