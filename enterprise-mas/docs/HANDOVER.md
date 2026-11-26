# Buyer handover

## What is delivered

This repository contains the Enterprise MAS source tree: a Rust API and service workspace, a Python orchestrator, Python and TypeScript SDKs, a Next.js operations dashboard, a sandbox runner, Compose and Helm deployment material, and examples. It is software source, not a hosted service. The license file is still marked as a template and requires approved legal terms before commercial distribution.

## Local startup

Prerequisites: Docker Compose, Git, and network access to retrieve the configured container images. Use only development values for a local demo.

1. From the repository root, copy `deploy/compose/.env.example` to `.env`.
2. Review every value in `.env`; do not put real production credentials in the example configuration or commit `.env`.
3. Start the core stack:

   ```bash
   docker compose -f deploy/compose/docker-compose.yml up -d
   ```

4. Check service status and API liveness:

   ```bash
   docker compose -f deploy/compose/docker-compose.yml ps
   curl -fsS http://127.0.0.1:8080/v1/health/live
   ```

5. For a dashboard/demo overlay, review `deploy/compose/docker-compose.demo.yml` and its extra services, then start both Compose files together as described in the root README.

The demo token and secret values in the example files are not production credentials. Successful startup depends on Docker, image availability, port availability, and the configuration appropriate to the operator's host.

## Environment variables

The canonical development template is `deploy/compose/.env.example`. Selected variables:

| Variable | Purpose |
|---|---|
| `MAS_ENV` | Runtime environment label; use `development` only for local development. |
| `MAS_DB_PASSWORD` | Development database role password used by Compose interpolation. |
| `MAS_DATABASE_URL` | API connection string to PostgreSQL. |
| `MAS_TEST_DATABASE_URL` | Elevated database URL used by integration tests. |
| `MAS_TEST_DATABASE_APP_URL` | Least-privilege application database URL used by integration tests. |
| `MAS_API_HTTP_ADDR` | HTTP listener address for the API. |
| `MAS_API_GRPC_ADDR` | gRPC listener address for the API. |
| `MAS_AI_BRIDGE_ENDPOINT` | Rust-to-Python bridge endpoint. |
| `MAS_AI_BRIDGE_TOKEN` | Development bridge bearer token; replace with managed secret material outside local development. |
| `MAS_AI_BRIDGE_INTERNAL_SIGNING_SECRET` | Development internal signing secret; replace with managed secret material outside local development. |
| `MAS_NATS_URL` | NATS service URL. |
| `MAS_BROKER_KIND` | Broker selection, configured as `nats` in the example. |
| `MAS_ORCHESTRATOR_PROVIDERS_ENABLED_PROVIDERS` | Operator-selected AI provider set. |
| `MAS_ORCHESTRATOR_PROVIDERS_DEFAULT_MODEL` | Optional configured default model. |

The template contains the complete set currently used by the Compose services. Verify the template and service configuration when upgrading; names and requirements can change.

## Day-one checklist

- [ ] Obtain and review the approved commercial license; the repository currently contains a license template.
- [ ] Replace development secrets with values from an approved secret manager before non-development deployment.
- [ ] Configure and test PostgreSQL backups and restoration.
- [ ] Configure production identity, authorization, TLS, logging, metrics, alerting, and incident response for the target environment.
- [ ] Review `KNOWN_LIMITATIONS.md`, the release notes, and migration registration status.
- [ ] Verify that only intended migrations are registered, and test upgrades against a restored staging database.
- [ ] Run the relevant Rust, Python, web, OpenAPI, and live-database CI gates in a fully provisioned environment before release.
- [ ] Confirm external provider accounts, credentials, quotas, and billing configuration with the relevant operators.
- [ ] Do not advertise SOC 2, ISO certification, or capabilities that have not been verified in code and tests.
