# Known limitations

This page records limitations visible in the repository and verification environment. It is not a security certification or an independent audit.

## Assurance and deployment

- No SOC 2, ISO 27001, or other certification is claimed.
- This repository is source code, not a hosted SaaS service. Operators own deployment, secrets management, backups, monitoring, and incident response.
- Compose example secrets and tokens are development values. Replace them with operator-managed secrets before any non-development deployment.
- A checked-in migration or test does not prove that the migration has been applied successfully to a production database.

## Database migrations

- The forward-only migration runner applies only entries registered in `enterprise-mas/rust-core/crates/persistence/src/migrations.rs`.
- The registry currently contains 64 consecutive migrations, through `0064_billing_receipt_signature_check`.
- The integration test `enterprise-mas/rust-core/crates/persistence/tests/migration_registry.rs` verifies that migration directories are registered exactly once, version prefixes are unique, each migration has `up.sql`, and migrations 0042 onward have `down.sql`.
- Live PostgreSQL migration and integration tests require operator-provided database URLs and a compatible running database. Those live database tests were not run in the current workspace environment.

## Verification status

The latest full Python check passed Ruff formatting, Ruff lint, strict mypy, compileall, and pytest (2,324 passed, 17 skipped). Database-backed Python integration tests skipped because their `MAS_TEST_*_DATABASE_URL` variables are unset. Rust formatting, clippy, workspace tests (1,284 passed, 0 failed, 13 ignored), rustdoc, the all-target NATS feature check, and migration-registry tests passed. Database-gated Rust test functions returned early because their database URLs are unset, so live PostgreSQL behavior remains unverified. The canonical root `web/` uses Next.js 16.4.6 and React 19, with the Next.js `proxy` convention. It passed typecheck, ESLint, 40 unit tests, coverage (87.89% statements/lines), production build, API-client freshness check, and Playwright E2E (7 tests) after updating async framework APIs. `npm audit --omit=dev --audit-level=high` reports zero production vulnerabilities. The duplicate `enterprise-mas/web/` project still has typecheck/lint/test failures and is not the canonical dashboard path named by the root README. The full-tier manifest verification passed, including the migration-registration check. These results describe this workspace run only; they do not claim that unrun gates pass.

## Feature boundaries

- Features listed in product documentation should be treated as supported only to the extent that corresponding implementation and tests exist. The repository does not guarantee that every configured provider, external service, or optional integration is available in every deployment.
- WhatsApp media sending, full inbound-message type coverage, and template parameter validation are not claimed by this document as implemented; verify current source and tests before advertising them.
- Billing behavior depends on operator-supplied Stripe configuration and provider credentials. The repository does not provide a Stripe account or production price identifiers.
