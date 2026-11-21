# Changelog

All notable changes to this project are documented in this file.

## [1.1.0] — 2026-11-15

### Added
- WhatsApp plane end to end: `mas-channel-whatsapp` Rust channel crate with
  AWS Nitro Enclaves attestation for webhook signing keys, 13-type inbound
  normalisation pipeline, wamid-idempotent message storage, and the signed
  `/webhooks/whatsapp` receive + `/v1/whatsapp/...` tenant APIs.
- Billing plane: `mas-billing-stripe` (Stripe client, webhook signature
  verification, plan price snapshot + proration) and the persistence-backed
  billing API with live-PG round trip tests.
- `sandbox-runner/` with hard resource limits (memory/cpu/output) and a
  default-deny seccomp profile, plus limit/security tests.
- SDKs: `sdk/typescript` (`MasClient`, SSE `openEventStream`) and
  `sdk/python` (`MasClient`, scope guard, event parsing) with unit tests.
- Next.js dashboard (`web/`) — 19 routes, typed API client generated from
  `openapi.json`, SSE subscriptions, quota/usage/WhatsApp inbox views, and
  unit + Playwright e2e coverage.
- Observability pack: Grafana dashboards (overview/executions/quota),
  Prometheus alert rules, and demo walkthrough/troubleshooting docs.
- Completeness gate (`completeness` job) cross-checking manifest
  `scripts/dev/manifest.txt` in CI.

### Changed
- Product version is `1.1.0` across `enterprise-mas/rust-core` workspace.package,
  `enterprise-mas/python-orchestrator`, `web/package.json`, and both SDKs.
- `config.enums.ProviderKind` now classifies gemini and azure as first-class
  provider kinds with validation enforcement.
- WhatsApp audio intake validates size/mime and raises an explicit
  `UnsupportedTranscriptionError` when no transcriber is installed.

### Fixed
- Deep-review remediation (`docs/REVIEW_RESOLUTION.md`): the C-01…C-12
  critical set and the W-01…W-08 warning set are resolved — durable broker
  protocol and delivery receipts, output-preserving settlement, lease-loss
  and shutdown cancellation wiring, quota idempotency, HMAC v2 + one-time
  nonce replay receipts on both sides of the bridge, SSRF-safe egress,
  authoritative execution state reader + durable terminal-state reporter
  (RLS-scoped `RustStateTransitionReporter`), cooperative cancellation
  propagation, corrupt-task containment, resumable Data Hub sync with
  pre-download admission, and isolated document parsing.
- Web crawler link farms can no longer exceed the frontier, the scheduled
  set or the URL-length limit (`data_hub/sources.py`: 2048-char cap before
  `urljoin`/queueing, per-page fanout cap, hostile links dropped
  individually).

## [1.0.0] — 2026-11-15

### Added
- Initial release of the Enterprise Multi-Agent System core
  (`enterprise-mas`).
