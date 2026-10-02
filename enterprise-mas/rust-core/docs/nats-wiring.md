# NATS/JetStream wiring plan (Phase 20) — real broker lifecycle

Decision record + execution ledger for the `nats-wiring` phase: the
`BrokerPort` gains a real adapter over NATS JetStream, and the binaries can
run their task lane against a broker cluster instead of the PostgreSQL
polling lane. The `BrokerPort` contract (sequenced publishes, durable
pull-consumers, explicit ack/nak, dead letters on delivery ceiling) was
designed against JetStream semantics from day one; this phase binds it to
the real thing.

## Key decisions

1. **Dependency direction**: `async-nats` enters as an *optional* dependency
   of `mas-messaging` behind feature `nats`. Default builds stay lean; the
   three binaries opt in. No other crate gains a NATS dependency — the port
   remains the only seam.
2. **One stream, two shoulders**: stream `MAS_TASKS` covers `mas.tasks.>`;
   the volatile event fan-out (`mas.events.>`) remains the in-process
   fallback in this phase and is documented as such (durable events = the
   outbox relay job, unchanged from phase 19).
3. **Ack discipline**: explicit-ack pull consumers; `Nak{delay}` maps to
   JetStream `nack_with_delay`; the ack window is the consumer's `AckWait`.
   `dead_letters()` ships the platform wrapper: on `num_delivered >=
   max_deliver` the adapter republishes to the `mas.tasks.dlq` subject
   before terminal-acking the original (JetStream has no native DLQ;
   this preserves the port's contract without pretending the broker has one).
4. **Stream/consumer provisioning is explicit**: `NatsBroker::provision`
   calls `create_stream`/`create_or_update_consumer` at boot (idempotent,
   like migrations). Admins may pre-provision; the adapter verifies shape
   (subjects, retention) and fails loudly on drift rather than silently
   working around it.
5. **Config keys**: `broker.kind = "pg" | "nats"` (default `pg` to keep
   phase-19 deployments behaviorally identical), `broker.nats_url`,
   `broker.stream`, `broker.ack_wait_ms`. `MAS_BROKER_KIND` etc. mirror the
   same merger convention already used for `database.url`.
6. **Verification**: env-gated integration suite (`MAS_TEST_NATS_URL`)
   alongside the PostgreSQL one; the workspace sandbox has no nats-server,
   so gates stay compile/clippy/unit + pure mapping tests, with the live
   path documented in the runbook.
7. **Compatibility**: `PgTaskBroker` stays the default for production slices
   without a broker cluster; both adapters implement the identical port and
   both ship in the same binaries (runtime-selected).

## Execution ledger

| Step | Status |
|---|---|
| Plan recorded | done |
| `async-nats` optional dep + `nats` feature on mas-messaging | done |
| `messaging::nats` adapter: publish/ensure_consumer/fetch/ack/stats | done |
| Dead-letter wrapper (max_deliver → DLQ subject republish) | done |
| Provisioning + drift verification | done |
| worker-service composition: `broker.kind` runtime select | done |
| Health probe on the NATS lane (worker `ping` at boot; api/scheduler own no broker) | done (observer: api/scheduler have no broker to probe) |
| Env-gated integration tests (`MAS_TEST_NATS_URL`) + unit coverage | done (5 live tests skip-clean sans env; 4 pure unit tests; 2×2 lane-resolution bin tests) |
| Runbook + config samples updated | done (`docs/runbooks/nats-live-tests.md`) |
| Gates (fmt/clippy/test/doc) | done (default space + nats feature space) |
