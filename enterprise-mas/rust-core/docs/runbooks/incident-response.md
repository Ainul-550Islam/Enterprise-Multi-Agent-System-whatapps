# Incident-response runbook

Triage by symptom using real health endpoints, metrics names, and tables.

## Symptom: API not responding

1. `GET /v1/health/live` — 200 means the process edge is up.
2. `GET /v1/health/ready` — dependency-aggregated readiness; if down, check
   Postgres probe (`PgHealthCheck` in `crates/api/src/main.rs`).
3. Check `mas-api` container/process logs; readiness fails closed on DB probe.

## Symptom: executions stuck in `running`

1. Worker leases expire at `lease_ttl_ms` (`worker` config) and are reaped;
   check `crates/worker/src/lifecycle.rs`.
2. Delivery ceiling (`retry_max_deliver`) sends poison tasks to DLQ
   (`mas.tasks.dlq` subject / dead-letter table).
3. Inspect `mas_audit_events` for the execution transitions.

## Symptom: foreign tenant can read data

Should not happen: foreign-tenant reads answer 404, never 403
(`execution_service.rs` + `authorized_service_context`). If observed, treat as
severity-critical and roll back the deployment.

## Symptom: bridge calls failing

Verify `MAS_AI_BRIDGE_INTERNAL_SIGNING_SECRET` matches between Rust worker and
Python orchestrator; production requires body-bound signatures
(`AgentBridgeSettings::require_internal_signing_secret`).

## Metrics names (grep crates/observability)

`mas_audit_events_total{outcome,class}`, denial counters. Metrics exposed via
Prometheus text exposition at the metrics addr (`MAS_METRICS_ADDR`).

## Tables involved

`mas_audit_events`, `mas_executions`, `mas_workflow_versions`, `mas_rollback_records`,
`mas_outbox`, `mas_schema_migrations` (ledger).
