# Runbook: live NATS JetStream integration suite + broker lane

**Location:** `crates/messaging/tests/live_nats.rs` (5 tests, feature `nats`)
**Cost:** one `nats-server` with JetStream you are allowed to mutate.
**Never point at production.**

## 1. Bring up JetStream

```bash
docker run --rm --name mas-itest-nats -p 4222:4222 nats:2-alpine -js
```

(or `nats-server -js -p 4222` from a local install). Dry-run validation
without a server: `cargo test -p mas-messaging --features nats` — the unit
suite (URL/stream sanitization, header projection, DLQ subject) runs
server-free and the live tests skip with a one-line note.

## 2. Run the suite

```bash
cd rust-core
source .cargo-env
export MAS_TEST_NATS_URL=nats://127.0.0.1:4222   # MAS_NATS_URL honoured too

cargo test -p mas-messaging --features nats --test live_nats
```

Covers, against a unique per-run stream:

* publish dedup via `Nats-Msg-Id` (duplicate key ⇒ same stream sequence);
* `ensure_consumer` idempotency + consumer stats flow;
* fetch → ack round trip, double-ack refusal (sequence not in flight);
* `Nak{delay}` redelivery with growing delivery counts;
* poison `Term` landing on the `mas.tasks.dlq` lane via the adapter's
  republish wrapper, and `dead_letters()` cumulative snapshot semantics;
* `ping` liveness + idempotent provisioning across repeated connects.

## 3. Run `mas-worker` against the nats lane

```bash
export MAS_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:55432/mas_itest
export MAS_BROKER_KIND=nats
export MAS_NATS_URL=nats://127.0.0.1:4222
# optional: MAS_BROKER_STREAM=MAS_TASKS  MAS_BROKER_ACK_WAIT_MS=30000

cargo run -p mas-worker-service --features nats
```

Boot does: lane resolution → connect + stream provisioning (`mas.tasks.>`)
→ health `ping` → durable pull-consumer ensure by the `TaskConsumer`.
Any failure stops boot with the knob named (`MAS_NATS_URL`,
`--features nats`, `broker.kind` values).

| Key                  | Env                       | Default      |
| -------------------- | ------------------------- | ------------ |
| `broker.kind`        | `MAS_BROKER_KIND`         | `pg`         |
| `broker.nats_url`    | `MAS_NATS_URL`            | — (required) |
| `broker.stream`      | `MAS_BROKER_STREAM`       | `MAS_TASKS`  |
| `broker.ack_wait_ms` | `MAS_BROKER_ACK_WAIT_MS`  | `30000`      |

Config-drift policy: an existing stream not covering `mas.tasks.>`, or a
durable consumer whose `max_deliver`/filter diverges from config, fails the
ensure loudly — reconcile with `nats consumer edit` rather than letting the
platform silently re-own a live lane.

## 4. Semantics notes for operators

* The adapter settles by sequence via a bounded in-process flight ledger
  (8 192 entries); the server's `AckWait` (+5s margin over the client
  window) remains the real timeout arbiter — a crashed worker's in-flight
  messages are redelivered exactly like the in-memory lane.
* `consumer_stats` maps honestly to JetStream's model: `nacked` counts
  redelivered-so-far; `terminated`/`dead_lettered` are platform-level (DLQ
  lane + audit), not counters JetStream tracks per consumer.
* Feature propagation: `mas-messaging/nats` → `mas-worker-service/nats`.
  Build without it and `broker.kind=nats` tells you what flag to add.
