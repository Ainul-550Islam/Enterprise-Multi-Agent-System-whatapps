# Internal service authentication v2 and durable delivery receipts

This document is the deployment contract for service-to-service authentication and worker delivery receipt storage. Both use PostgreSQL as a shared authority. A local cache, a process-local sequence, or a timestamp-only MAC is not an acceptable substitute in a replicated production deployment.

## v2 request format

Every internal request uses one shared secret and these headers:

```text
x-mas-internal-service: <allowed service id>
x-mas-internal-signature: v2:<service>:<unix-millis>:<nonce>:<hmac-sha256-hex>
```

The HMAC input, encoded as ASCII, is:

```text
v2:<service>:<unix-millis>:<nonce>:<sha256-hex-of-exact-body-bytes>
```

`nonce` is a fresh 24-byte URL-safe random value without padding. Receivers reject v1; there is no downgrade compatibility path. A receiver verifies the body MAC, caller allowlist, and sixty-second symmetric clock window, then atomically inserts `(service_name, nonce)` into `internal_request_receipts` before parsing or performing work. Duplicate insertions, unavailable receipt storage, missing secrets, and malformed signatures are all unauthenticated failures.

The Rust and Python implementations share this deterministic vector for cross-language verification:

```text
secret:  s3cr3t
service: mas-api
millis:  1700000000000
nonce:   0123456789abcdefghijklmnopqrstuv
body:    {"a":1}
signature:
v2:mas-api:1700000000000:0123456789abcdefghijklmnopqrstuv:ad29671c18b6926ff5c86fa205004b7b9e8675b6324bef6459aedead1e775766
```

## Production configuration

Apply migrations through `0034_delivery_receipt_integrity` before enabling v2 callers. Migration 0033 creates the receipt tables, unique indexes, expiry index, and machine-plane grants; 0034 adds the task/attempt linkage and integrity constraints that make a delivery receipt an authoritative durable ownership record.

### Python orchestrator

Configure both secret references in the `security` block:

```toml
[security]
internal_service_secret_ref = "env://MAS_INTERNAL_SERVICE_SECRET"
internal_replay_database_url_secret_ref = "env://MAS_INTERNAL_REPLAY_DATABASE_URL"
```

`MAS_INTERNAL_REPLAY_DATABASE_URL` must resolve to a PostgreSQL DSN for a role with `INSERT` on `internal_request_receipts`. Production validation rejects a missing reference. The Python gRPC bridge uses `PostgresReplayGuard`; its in-memory guard exists only for tests and explicitly non-production composition.

### Rust API, scheduler, and worker

The API and scheduler inbound HTTP surfaces use `MAS_INTERNAL_SERVICE_SECRET`. The worker's Rust-to-Python gRPC client reads `MAS_AI_BRIDGE_INTERNAL_SIGNING_SECRET`; it must contain the same shared value resolved by Python's `security.internal_service_secret_ref`. The API receipt insert uses its runtime database pool. The production scheduler completion listener requires all of:

```text
MAS_INTERNAL_SERVICE_SECRET
MAS_SCHEDULER_COMPLETION_ADDR
MAS_DATABASE_SCHEDULER_URL
```

The scheduler uses the scheduler-plane PostgreSQL pool for replay nonce inserts. The worker requires `MAS_DATABASE_WORKER_URL`; it uses that worker-plane pool for `broker_delivery_receipts` and never uses a UUID prefix or a process-local broker handle as a durable delivery identity.

The scheduler completion listener is intentionally configured separately from the tick loop. It binds only after its secret, database migration, and durable replay store are all available. Do not expose it through a public ingress; place it on the private service network and retain the HMAC check as the mandatory identity boundary.

## Maintenance and retention

`internal_request_receipts` are needed only through the signed request expiry window. Retain them for one extra hour to preserve diagnostics. Run this bounded statement every hour from the deployment's database maintenance job until it affects zero rows:

```sql
WITH expired AS (
    SELECT ctid
    FROM internal_request_receipts
    WHERE expires_at < now() - interval '1 hour'
    ORDER BY expires_at
    LIMIT 10000
)
DELETE FROM internal_request_receipts receipts
USING expired
WHERE receipts.ctid = expired.ctid;
```

The expiry index keeps each bounded batch cheap, and a catch-up job can repeat it safely. The maintenance query is intentionally operational policy rather than a destructive schema migration. Do not delete unexpired rows: doing so restores a replay window.

`broker_delivery_receipts` are operational/audit records. Keep them for the organization’s task-execution retention period. A separate maintenance job may delete settled records older than that policy, but must never delete `reserved` or `in_flight` records. Expired in-flight rows are transitioned to `expired` atomically when a worker reserves a replacement delivery.

## Rollout order

1. Apply migrations 0033 and 0034, then verify the migration ledger on every runtime database.
2. Provision the shared signing secret and separate least-privilege database DSNs.
3. Configure Python replay storage and deploy the Python bridge before sending v2 traffic.
4. Deploy Rust services that generate and verify v2 signatures. v1 is intentionally rejected.
5. Configure and health-check the scheduler completion listener.
6. Start workers with the PostgreSQL receipt store and observe receipt insert, duplicate, expiry, and settlement metrics/logs.
7. Enable the hourly replay-receipt maintenance job.

A service must fail closed during any of these steps. Accepting a request without a durable nonce or delivery receipt is a security failure, not an availability fallback.
