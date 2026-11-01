# Scaling runbook

Only parameters that exist in config are documented here.

## Worker concurrency / run slots / capacity

* `worker.max_in_flight` — in-flight handler concurrency cap (16 dev / 64 prod)
  (`crates/worker` config).
* `worker.lease_ttl_ms` / `worker.ack_wait_ms` — lease TTL and ack window
  (heartbeat renews at `ack_wait/2`).
* `worker.retry_max_deliver` — delivery ceiling before DLQ.

## Scheduler leasing

* `scheduler.tick_interval_ms` — tick driver interval (1000ms dev / 5000ms prod)
* `scheduler.recover_on_start` — reap dead leases + requeue overdue work
* `scheduler.error_backoff_cap_ms` — capped exponential backoff after
  consecutive whole-tick failures
* One active scheduler (replicas respect lease/leader semantics in
  `crates/scheduling`).

## NATS consumer scaling

* Pull consumers with `AckWait` = `ack_wait_ms`; `Nak{delay}` maps to
  `nack_with_delay`; dead letters to `mas.tasks.dlq` on delivery ceiling.
* Stream `MAS_TASKS` covers `mas.tasks.>` (see `docs/nats-wiring.md`,
  `deploy/compose/nats.conf`).

## Postgres pool sizing

* `database.max_connections` — 10 dev / 32 prod
* `database.connect_timeout_ms` — 5000

## Dedicated roles

Scheduler/worker use separate least-privilege role URLs
(`MAS_DATABASE_SCHEDULER_URL` / `MAS_DATABASE_WORKER_URL`); never fall back to
`database.url`.
