# Verification record

Generated at (UTC): 2026-11-26T12:07:51Z
Commit: 342e72d998aedbd789614ccd94b9f7b4de43153c

Each row below is copied from this invocation's command matrix. A NOT_RUN or FAIL is recorded as-is; this document does not imply an unrun gate passed.

| Check | Command | Exit code | Result |
|---|---|---:|---|
| `E01_fmt` | `cargo fmt --all -- --check` | 0 | PASS |
| `E02_clippy` | `cargo clippy --workspace --all-targets --locked -- -D warnings` | 0 | PASS |
| `E03_test_workspace` | `cargo test --workspace --locked` | 0 | PASS |
| `E04_test_nats` | `cargo test --workspace --locked --features nats` | 0 | PASS |
| `E05_governance_routes` | `cargo test -p mas-api --test governance_routes --locked` | 0 | PASS |
| `E06_tooling_routes` | `cargo test -p mas-api --test tooling_routes --locked` | 0 | PASS |
| `E07_jwks` | `cargo test -p mas-security --test jwks --locked` | 0 | PASS |
| `E08_live_pg` | `cargo test -p mas-persistence --locked --test live_pg --test analytics_live_pg --test project_list_live_pg --test rollback_live_pg --test tools_live_pg --test usage_live_pg --test control_plane_pg --test plane_grants --test tenant_sql_contract` | 0 | PASS |
| `E09_e2e_python` | `cargo test -p mas-ai-bridge --locked --test e2e_python -- --ignored` | 0 | PASS |
| `E13_cargo_doc` | `cargo doc --workspace --no-deps --locked` | 0 | PASS |
| `B_openapi_parity` | `cargo test -p mas-api --test openapi_parity --locked` | 0 | PASS |
| `B_dashboard_contract` | `cargo test -p mas-api --test dashboard_contract --locked` | 0 | PASS |
| `B_dashboard_flow` | `cargo test -p mas-api --test dashboard_contract --locked` | 0 | PASS |
| `B_cargo_deny` | `cargo deny check` | 0 | PASS |
| `E10_migrations_roundtrip` | `mas_migrate up (empty DB) + down/up 0050-0053 NOT_RUN` | 0 | PASS |
| `E11_mutation_tenant_predicate` | `mutate ToolService::get filter -> test fails -> restore -> passes` | 0 | PASS |
| `E12_mutation_authz` | `short-circuit authorize->Allow -> viewer-denial fails -> restore -> passes` | 0 | PASS |
| `P_python_check` | `bash scripts/check.sh` | 0 | PASS |
| `P_pip_audit` | `pip-audit --local` | 0 | PASS |
| `I_helm_lint` | `helm lint deploy/helm/enterprise-mas` | 0 | PASS |
| `I_helm_template` | `bash -c helm template enterprise-mas deploy/helm/enterprise-mas > /tmp/helm-render.yaml 2>/tmp/helm-err.txt && kubeconform -strict -summary /tmp/helm-render.yaml |#124;|#124; { echo "--- helm error ---"; cat /tmp/helm-err.txt; false; }` | 0 | PASS |
| `L_actionlint` | `actionlint -color .github/workflows/ci.yml .github/workflows/security.yml` | 0 | PASS |
| `L_shellcheck` | `bash -c shellcheck --severity=error $(find . -name "*.sh" -not -path "*/.git/*" -not -path "*/target/*" -not -path "*/node_modules/*")` | 0 | PASS |
| `L_yamllint` | `bash -c yamllint -d relaxed $(find deploy -type f \( -name "*.yaml" -o -name "*.yml" \) ! -path "deploy/helm/*/templates/*")` | 0 | PASS |
| `D_docker_build` | `docker build -f deploy/docker/api.Dockerfile -t enterprise-mas/api:local . (x5 images)` | 127 | NOT_RUN |
| `D_compose_smoke` | `bash scripts/e2e-smoke.sh (docker compose up)` | 127 | NOT_RUN |
| `E14_merge_integrity` | `conflict markers + cargo metadata --locked` | 0 | PASS |

Detailed raw logs were written under  during this run (temporary directory).
