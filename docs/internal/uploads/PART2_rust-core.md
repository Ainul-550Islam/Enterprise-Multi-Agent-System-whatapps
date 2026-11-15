# Part 2/4 — rust-core (Rust workspace, 22 crates + migrations)

The authoritative platform core. New work here: dashboard endpoints, rollback, approvals persistence, data-hub plumbing, JWKS. Paths are relative to enterprise-mas/.

Files: **427** = 310 `[OK]` + 34 `[GAP]` + 83 `[NEW]`

Copy the block below as-is.

```text
# PART 2/4 — RUST-CORE (RUST WORKSPACE, 22 CRATES + MIGRATIONS)
# Files: 427 = 310 [OK] + 34 [GAP] + 83 [NEW]
#
# LEGEND
#   [OK]  exists and is complete. Do NOT rewrite. Read/reuse it and keep its tests green. Comment = the file's own header + its public items.
#   [GAP] exists. Keep ALL current logic; apply ONLY the 'CHANGE:' in the comment; return the COMPLETE modified file.
#   [NEW] missing. Create the COMPLETE file (no ellipsis, no placeholder, no mock/fake on a production path) exactly as the comment describes.
#   Directories: [NEW] = whole directory is new, [GAP] = contains GAP/NEW files, [OK] = untouched.
#   Names such as [...nextauth] and [...path] are literal Next.js catch-all route directories, not omitted content.
#
# RULES
#   Rust owns state, tenancy, authz, policy, quota, audit, persistence. Python owns AI execution only: gRPC to Rust, never SQL on Rust-owned tables.
#   Browser -> Next.js BFF (/api/mas) -> Rust API. The Rust API is never public. Tenant/org scope is resolved server-side from the token; request headers are hints only.
#   Secrets are SecretReference only; nothing secret reaches the browser. A mutation that cannot be audited is a failed mutation.
#
enterprise-mas/                               # [GAP] Monorepo root. (this part only: rust-core (Rust workspace, 22 crates + migrations)).
└── rust-core/                                # [GAP] Rust workspace (22 crates): orchestration, execution, security, tenancy, policy, quota, events, persistence, API, worker, scheduler, CLI, AI bridge.
    ├── .cargo-env                            # [OK] Toolchain environment for this workspace (points at the shared Rust install).
    ├── Cargo.lock                            # [OK] Locked dependency graph; committed and required (every build uses --locked).
    ├── Cargo.toml                            # [OK] The rust-core workspace manifest — the single place every crate's identity.
    ├── clippy.toml                           # [OK] Project-wide Clippy configuration.
    ├── deny.toml                             # [NEW] cargo-deny policy: allowed licenses, banned crates (no LLM provider SDKs in rust-core), advisory database settings, duplicate-version bans.
    ├── README.md                             # [GAP] Rust core overview: architecture boundaries, crate responsibilities, build/verify. CHANGE: FIX stale facts: says 20 crates / 12 migrations / 372 tests / 27 typed IDs (real: 22 crates, 19 migrations now 26, 28 define_id! IDs); references scripts/dev.sh, scripts/test.sh, scripts/generate-proto.sh and /contracts/proto which do not exist (real: dev-up.sh/dev-down.sh, check.sh, python-orchestrator/scripts/generate_proto.sh, crates/api/proto).
    ├── rust-toolchain.toml                   # [OK] Pins the exact toolchain used by CI and local development.
    ├── rustfmt.toml                          # [OK] rustfmt configuration (stable options only so stable rustfmt never errors).
    ├── .cargo/                               # [OK] Cargo configuration.
    │   └── config.toml                       # [OK] Cargo-level configuration for reproducible compilation of rust-core.
    ├── benches/                              # [OK] Criterion benchmarks.
    │   ├── backoff_bench.rs                  # [OK] Criterion bench of the worker retry policy: full-jitter delay computation throughput.
    │   ├── codec_bench.rs                    # [OK] Criterion bench of JsonEventCodec encode/decode throughput on a realistic task frame.
    │   └── topological_bench.rs              # [OK] Criterion bench of workflow topological_order over a 1,000-node layered DAG.
    ├── config/                               # [GAP] Layered typed configuration (default → environment).
    │   ├── default.toml                      # [GAP] MAS runtime configuration — DEFAULT layer. CHANGE: ADD sections: oidc (issuer, jwks_url, algs), stream (heartbeat, buffer), analytics (rollup refresh), data_hub (upload caps).
    │   ├── development.toml                  # [OK] DEVELOPMENT overlay (default when MAS_ENV is unset).
    │   ├── production.toml                   # [GAP] PRODUCTION overlay (MAS_ENV=production). CHANGE: ADD the same sections with fail-closed production requirements (HTTPS issuer, no dev tokens).
    │   └── test.toml                         # [OK] TEST overlay (MAS_ENV=test): *deterministic and fast*.
    ├── crates/                               # [GAP] Workspace member crates; one responsibility each, dependency rules compile-enforced.
    │   ├── ai-bridge/                        # [GAP] Crate ai-bridge: gRPC adapter implementing the AgentBridge port toward the Python orchestrator.
    │   │   ├── build.rs                      # [OK] Generates the Rust mas.api.v1 client stubs from the canonical proto.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the ai-bridge crate: dependencies, features, lint inheritance.
    │   │   ├── src/                          # [GAP] Source of the ai-bridge crate.
    │   │   │   └── lib.rs                    # [GAP] Crate root of ai-bridge: module declarations and public re-exports. CHANGE: ADD gRPC client adapter implementing DataHubBridgePort (same metadata, deadline and error-mapping rules as the AgentBridge adapter).
    │   │   └── tests/                        # [OK] Integration tests of the ai-bridge crate.
    │   │       └── e2e_python.rs             # [OK] End-to-end test: the real Rust bridge against a real Python server.
    │   ├── api/                              # [GAP] Crate api: Axum HTTP + tonic gRPC servers, middleware, scope resolution, handlers, graceful shutdown.
    │   │   ├── build.rs                      # [OK] Compiles the v1 proto into tonic server descriptors using a vendored protoc.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the api crate: dependencies, features, lint inheritance.
    │   │   ├── proto/                        # [GAP] Protobuf sources owned by the API crate.
    │   │   │   └── mas/                      # [GAP] Protobuf package root.
    │   │   │       └── api/                  # [GAP]
    │   │   │           └── v1/               # [GAP] Authoritative v1 wire contract (Rust owns the wire).
    │   │   │               └── mas.proto     # [GAP] The canonical wire contract between Rust and Python. CHANGE: ADD service DataHubBridgeApi (SyncSource, IngestDocument, PurgeDocument, Health); bump minor protocol version; regenerate Python bindings.
    │   │   ├── src/                          # [GAP] Source of the api crate.
    │   │   │   ├── auth.rs                   # [OK] Bearer-token verification. Defines: TokenVerifierPort, StaticTokenVerifier, bearer_token.
    │   │   │   ├── context.rs                # [OK] The request-scoped context every handler runs with: the verified principal, the resolved scope. Defines: RequestContext.
    │   │   │   ├── grpc.rs                   # [OK] The gRPC surface of the api process. Defines: GrpcServices.
    │   │   │   ├── inmemory_directory.rs     # [OK] The tenancy directory for the in-memory development and test lane. Defines: InMemoryTenantDirectory, InMemoryMembershipDirectory.
    │   │   │   ├── lib.rs                    # [OK] Crate root of api: module declarations and public re-exports. Scope — Axum HTTP + tonic gRPC servers, middleware, scope resolution, handlers, graceful shutdown.
    │   │   │   ├── main.rs                   # [GAP] mas-api process bootstrap: HTTP + gRPC servers over the application layer with graceful. CHANGE: ADD wiring of the new services in compose_production and the --dev-inmemory lane; keep fail-closed composition.
    │   │   │   ├── middleware.rs             # [OK] Per-request authentication, scope resolution, and the transaction every repository call in that request. Defines: request_context_layer, authenticated_layer.
    │   │   │   ├── prod_auth.rs              # [OK] Production API-key verification — the credential-to-principal boundary. Defines: PostgresApiKeyVerifier.
    │   │   │   ├── response.rs               # [OK] The stable response envelope and the conversion from internal errors to the public error contract. Defines: ApiResponse, ApiError, ApiResult, IntoApiResult, +6 more.
    │   │   │   ├── router.rs                 # [GAP] The one HTTP route table for the api process. Defines: router. CHANGE: ADD routes for analytics, tools, policies, approvals, connectors, webhooks, usage, audit, api-keys, notifications, auth/me, rollbacks, search, stream, assistant, data-hub, agent versions list, execution steps.
    │   │   │   ├── scope_resolution.rs       # [OK] Turn an authenticated principal plus the scope a request *claims* into the one scope the request is. Defines: ScopeClaims, ResolvedRequest, RequestScopeResolver.
    │   │   │   ├── server.rs                 # [OK] Process bootstrap: binds the HTTP and gRPC listeners and runs them with graceful shutdown on SIGINT/SIGTERM.
    │   │   │   ├── state.rs                  # [GAP] Process state shared by every handler — the application services, the token verifier. Defines: AppState. CHANGE: ADD handles for the new application services.
    │   │   │   └── handlers/                 # [GAP] HTTP handlers: thin transport over application services.
    │   │   │       ├── agents.rs             # [GAP] Agent routes. Defines: PublishVersionBody, ListQuery, register, +3 more. CHANGE: ADD GET /v1/agents/{id}/versions (list); rollback route lives in rollbacks.rs (domain rollback_of and contracts RollbackAgentRequest already exist, no route does).
    │   │   │       ├── analytics.rs          # [NEW] Dashboard read endpoints: GET /v1/analytics/overview (total_agents, tasks_completed, success_rate, time_saved, each with delta vs the previous equal window + sparkline points), /performance (tasks_completed + success_rate series, hour|day|week), /tasks-by-agent (top slices + other), /usage-cost. Query: from, to, granularity. Scope from the resolved principal only.
    │   │   │       ├── api_keys.rs           # [NEW] POST /v1/api-keys (plaintext returned exactly once, only the hash stored), GET (prefix + metadata only), POST revoke, POST rotate; permission: manage credentials.
    │   │   │       ├── approvals.rs          # [NEW] Human-in-the-loop queue: GET /v1/approvals (pending/decided), POST /v1/approvals/{id}/approve|deny with reason; backed by policy::approval; every decision audited; approver cannot be the requester.
    │   │   │       ├── assistant.rs          # [NEW] GET /v1/assistant/insights returns the deterministic InsightDto list from analytics_service. POST /v1/assistant/messages builds a tenant-scoped context snapshot server-side (analytics overview, agent list, recent failures), then submits an execution of the seeded platform_assistant agent through the canonical durable pipeline (no second execution path) and returns the execution id to follow on the stream.
    │   │   │       ├── audit.rs              # [NEW] GET /v1/audit-events with filters (actor, action, resource, outcome, time range) and keyset pagination; read-only over the append-only log; requires the view-audit permission.
    │   │   │       ├── auth.rs               # [NEW] GET /v1/auth/me (authenticated principal, resolved scope(s), effective permission list for UI gating — a hint only, every request is re-authorized server-side) and GET /v1/auth/permission-matrix (the role x permission grid straight from the canonical authorization service). No login or token issuance here — the IdP issues tokens.
    │   │   │       ├── connectors.rs         # [NEW] Integrations endpoints: GET /v1/connector-manifests, POST /v1/connectors (ConnectorFactory install), POST /v1/connectors/{id}/probe, disable/delete, OAuth start + callback (PKCE state sessions); no secret material accepted in config.
    │   │   │       ├── data_hub.rs           # [NEW] Data Hub endpoints: CRUD /v1/data-sources, POST /v1/data-sources/{id}/sync, GET /v1/documents (status, chunk_count), DELETE document (purges chunks), POST upload (size/type caps); sync is delegated to Python over gRPC.
    │   │   │       ├── executions.rs         # [GAP] Execution routes. Defines: submit, start, pause, +4 more. CHANGE: ADD GET /v1/executions/{id}/steps (execution_steps timeline: kind, tool call, tokens, latency, status); the table exists but nothing serves it.
    │   │   │       ├── health.rs             # [OK] Health routes (public — no auth, caller of last resort for k8s probes). Defines: live, ready.
    │   │   │       ├── internal.rs           # [OK] Internal service routes: calls issued by the platform's own services (scheduler schedule-runs), signature-authenticated.
    │   │   │       ├── mod.rs                # [GAP] The HTTP handler module root — the Ctx extractor and the body size cap shared by every handler. CHANGE: ADD module declarations and shared pagination/filter extractors for the new handlers.
    │   │   │       ├── notifications.rs      # [NEW] Bell menu endpoints: GET /v1/notifications (unread first), POST mark-read / mark-all-read, GET/PUT preferences; recipient is always the authenticated user.
    │   │   │       ├── policies.rs           # [NEW] Policy endpoints: CRUD + versioned publish for /v1/policies, POST /v1/policies/{id}/evaluate (dry-run, not recorded as a real decision), GET /v1/policy-decisions (append-only decision log, keyset pagination).
    │   │   │       ├── rollbacks.rs          # [NEW] Governance rollback endpoints: POST /v1/agents/{id}/versions/{v}/rollback, /v1/workflows/{id}/versions/{v}/rollback, /v1/policies/{id}/versions/{v}/rollback, GET /v1/rollbacks (history). Idempotency-keyed and audited; creates a NEW version pointing back via rollback_of, history is never rewritten.
    │   │   │       ├── schedules.rs          # [OK] Schedule routes. Defines: ScheduleResponse, RegisterScheduleBody, register, pause, +4 more.
    │   │   │       ├── search.rs             # [NEW] GET /v1/search?q=&kinds= for the ⌘K palette across agents, workflows, executions, connectors; runs inside the scoped transaction so RLS bounds results; result count capped.
    │   │   │       ├── stream.rs             # [NEW] GET /v1/stream (Server-Sent Events): execution status, notifications and agent activity for the caller's resolved tenant. Per connection it creates an EPHEMERAL, tenant-subject-filtered consumer through the existing BrokerPort (ensure_consumer + fetch loop) — no new broker; dev lane uses InMemoryBroker. Re-authorizes on connect, heartbeat comments, Last-Event-ID resume, bounded per-connection buffer, closes on token expiry.
    │   │   │       ├── tenancy.rs            # [OK] Tenancy/identity routes. Defines: RegisterOrganizationBody, RegisterTenantBody, RegisterProjectBody, RegisterUserBody, +13 more.
    │   │   │       ├── tools.rs              # [NEW] Tool catalog endpoints: POST/GET /v1/tools, GET/PATCH /v1/tools/{id}, PUT /v1/agents/{id}/tool-permissions; runtime config validated by the domain Tool aggregate; secrets by SecretReference only.
    │   │   │       ├── usage.rs              # [NEW] Plan/usage read endpoints: GET /v1/usage (tokens, tool calls, active time by window), /v1/quotas, /v1/subscription (plan name, renewal date, percent used for the sidebar plan card), /v1/entitlements. Read-only; no payment processing.
    │   │   │       ├── webhooks.rs           # [NEW] Outbound webhook endpoints: CRUD /v1/webhooks, GET deliveries, POST replay of a dead-lettered delivery; HMAC secret generated server-side and shown once; target URL validated by the SSRF guard.
    │   │   │       └── workflows.rs          # [GAP] Workflow routes. Defines: WorkflowResponse, WorkflowGraphUpdateResponse, ListQuery, +5 more. CHANGE: ADD GET /v1/workflows/{id}/versions (list); record a workflow_versions row on publish.
    │   │   └── tests/                        # [GAP] API integration, contract and security-matrix tests.
    │   │       ├── dashboard_contract.rs     # [NEW] Golden-shape tests for every new dashboard endpoint (envelope, pagination, error codes) against tests/fixtures/dashboard/*.json.
    │   │       ├── openapi_parity.rs         # [NEW] Fails when a router route is missing from docs/openapi.yaml, or the spec lists a route the router does not serve.
    │   │       ├── root_contract.rs          # [OK] Cargo anchor compiling the workspace-root contract suite (tests/contract/*.rs) into mas-api.
    │   │       ├── root_integration.rs       # [OK] Cargo anchor for the workspace-root integration suite.
    │   │       └── security_matrix.rs        # [GAP] Cargo anchor for the workspace-root security matrix. CHANGE: EXTEND the matrix with every new route: unauthenticated 401, cross-tenant 404, insufficient role 403.
    │   ├── application/                      # [GAP] Crate application: Use-case services for every aggregate plus the canonical durable execution pipeline and DTO mapping.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the application crate: dependencies, features, lint inheritance.
    │   │   └── src/                          # [GAP] Source of the application crate.
    │   │       ├── agent_service.rs          # [GAP] Agent lifecycle use-cases: registration (per-project slug uniqueness, project coherence). Defines: AgentService. CHANGE: ADD rollback_version using the existing AgentVersion::as_rollback_of.
    │   │       ├── analytics_service.rs      # [NEW] Read-model service behind /v1/analytics: aggregates execution_rollups_hourly into KPI cards, deltas, time series and tasks-by-agent. time_saved = tasks_completed x the agent's configured baseline_minutes_per_task (null when not configured, never guessed). Also derives deterministic Smart Insights (largest week-over-week mover per agent) with metric_refs — no LLM on the dashboard path. Tenant-scoped, no writes.
    │   │       ├── api_key_service.rs        # [NEW] API key issue/list/revoke/rotate use cases on top of security::api_keys; plaintext leaves this service exactly once.
    │   │       ├── approval_service.rs       # [NEW] Tenant-scoped wrapper around the EXISTING policy::approval::ApprovalService (state machine, quorum and expiry already implemented) bound to the Postgres store; audits every decision, enforces approver != requester and the approver-role whitelist, runs the expiry sweep.
    │   │       ├── audit.rs                  # [OK] Audit recording for use-cases: a port every mutating service writes through. Defines: AuditSinkPort, InMemoryAuditSink, record_mutation.
    │   │       ├── audit_query_service.rs    # [NEW] Read-only audit query use case: filter validation, keyset pagination, view-audit permission check, redacted view only.
    │   │       ├── connector_service.rs      # [NEW] Connector install/probe/disable use cases over mas-integrations (ConnectorFactory, registry, OAuth sessions); returns sanitized probe errors only.
    │   │       ├── context.rs                # [OK] ServiceContext: the acting principal, scope and correlation data threaded through every use-case call. Defines: ServiceContext.
    │   │       ├── data_hub_service.rs       # [NEW] Data source/document use cases; starts a sync through DataHubBridgePort (gRPC to Python), records status transitions durably, never reads chunk vectors itself.
    │   │       ├── dto.rs                    # [GAP] Internal mapping from domain aggregates to the versioned contracts wire shapes. CHANGE: ADD domain→wire mappers for the new contracts.
    │   │       ├── durable.rs                # [OK] The durable execution port: every multi-write state change in the execution pipeline. Defines: SubmissionIntent, SubmissionKind, SubmissionReceipt, ClaimQuery, +21 more.
    │   │       ├── durable_events.rs         # [OK] The one canonical event-ownership path for the durable execution pipeline. Defines: EventContext, execution_submitted, execution_cancellation_requested, execution_started, +10 more.
    │   │       ├── durable_memory.rs         # [OK] A faithful in-memory reference implementation of crate::durable::DurableExecutionPort. Defines: InMemoryDurableExecution, transient_failure.
    │   │       ├── execution_service.rs      # [OK] Execution lifecycle use-cases — submission, transitions, cancellation and reads. Defines: SubmissionOutcome, ExecutionService.
    │   │       ├── fingerprint.rs            # [OK] Canonical request fingerprints for durable submission. Defines: FingerprintInput, RequestFingerprint, canonical_json, canonical_json_string.
    │   │       ├── lib.rs                    # [GAP] Crate root of application: module declarations and public re-exports. CHANGE: ADD module declarations/re-exports for the new services.
    │   │       ├── notification_service.rs   # [NEW] Notification list, read-state and preference use cases; fan-out from domain events into notifications rows.
    │   │       ├── pipeline_log.rs           # [OK] The durable execution pipeline's safe structured logging surface. Defines: PipelineLogContext, safe_value.
    │   │       ├── pipeline_metrics.rs       # [OK] The named observability surface of the canonical durable execution pipeline. Defines: Outcome, PipelineMetrics.
    │   │       ├── policy_service.rs         # [NEW] Policy draft → publish → rollback use cases and dry-run evaluation through the existing policy engine; publish is immutable-versioned and audited.
    │   │       ├── rollback_service.rs       # [NEW] The single rollback use case for agent, workflow and policy versions: re-publishes the target as a new version with rollback_of set, writes a rollback_records row and an audit event in ONE transaction; idempotent per key.
    │   │       ├── schedule_service.rs       # [OK] Schedule lifecycle use-cases: register (activates), idempotent pause/resume/disable, existence-hiding lookups.
    │   │       ├── search_service.rs         # [NEW] Cross-resource search use case over scoped repository queries; ranking by prefix match then recency.
    │   │       ├── stores.rs                 # [GAP] Application-facing store ports, one per aggregate the use-cases compose. Defines: OrganizationStorePort, TenantStorePort, ProjectStorePort, +8 more. CHANGE: ADD store ports (tools, policies, approvals, connectors, webhooks, usage, notifications, analytics, workflow_versions, rollbacks, data_sources, documents, execution_steps) with InMemoryServices reference implementations.
    │   │       ├── tenancy_service.rs        # [GAP] Tenancy-management use-cases: organizations → tenants → projects. Defines: TenancyService. CHANGE: ADD seeding of the system-owned platform_assistant agent (published v1, origin=system) when a project is registered.
    │   │       ├── tenant_isolation.rs       # [OK] The multi-tenant isolation suite for the canonical durable execution pipeline.
    │   │       ├── tool_service.rs           # [NEW] Tool register/update/grant use cases; validates runtime config, refuses a grant the project policy denies; audit sink on every mutation (a mutation that cannot be audited fails).
    │   │       ├── usage_service.rs          # [NEW] Usage/quota/subscription read use cases for the plan card and usage page; percent-used computed server-side from usage_records, quotas, entitlements.
    │   │       ├── webhook_service.rs        # [NEW] Webhook endpoint registration and delivery inspection/replay over the integrations outbound dispatcher; generates signing secrets and returns them once.
    │   │       └── workflow_service.rs       # [GAP] Workflow lifecycle use-cases: creation and versioned graph updates with structural validation. CHANGE: ADD immutable workflow_versions snapshot on publish; list/rollback hooks used by rollback_service.
    │   ├── cli/                              # [GAP] Crate cli: The mas operator binary: typed API client, clap commands, rendering and a strict exit-code contract.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the cli crate: dependencies, features, lint inheritance.
    │   │   ├── src/                          # [GAP] Source of the cli crate.
    │   │   │   ├── args.rs                   # [GAP] clap-parsed argument surface (parse-only; execution lives in crate::commands). Defines: GlobalOpts, Format, SubmitArgs, +6 more. CHANGE: ADD subcommands: tool, policy, connector, usage, audit, rollback, api-key.
    │   │   │   ├── client.rs                 # [OK] The typed HTTP client: builds requests, decodes ApiEnvelope<T>, surfaces StableApiError wholesale. Defines: ClientConfig, Response, ApiFailure, Outcome, +1 more.
    │   │   │   ├── commands.rs               # [GAP] Command execution: maps parsed subcommands onto the client and the renderer. Defines: ExitCode, run. CHANGE: ADD handlers for the new subcommands with the same exit-code contract.
    │   │   │   ├── lib.rs                    # [OK] Crate root of cli: module declarations and public re-exports. Scope — The mas operator binary: typed API client, clap commands, rendering and a strict exit-code contract.
    │   │   │   ├── main.rs                   # [OK] mas — operator CLI binary.
    │   │   │   └── render.rs                 # [GAP] Presentation: data payload to stdout; a StableApiError-shaped failure → stderr. Defines: render_data, render_envelope_error. CHANGE: ADD brief/json renderers for the new resources.
    │   │   └── tests/                        # [OK] Integration tests of the cli crate.
    │   │       └── e2e.rs                    # [OK] Fake-server integration: a local axum app answering with real ApiEnvelope shapes.
    │   ├── common/                           # [OK] Crate common: Error/Result types, typed UUIDv7 IDs, UTC Timestamp, shared enums, cursor pagination, validation helpers, redaction.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the common crate: dependencies, features, lint inheritance.
    │   │   └── src/                          # [OK] Source of the common crate.
    │   │       ├── config.rs                 # [OK] Layered runtime configuration loader (TOML, no external config crate). Defines: Merger, config_dir, environment_name, layer_exists, +6 more.
    │   │       ├── constants.rs              # [OK] Protocol versions, limits and queue names shared across the workspace.
    │   │       ├── enums.rs                  # [OK] The shared state enums whose string values are the platform's wire contract.
    │   │       ├── error.rs                  # [OK] The unified, transport-agnostic error model every crate converges on. Defines: ValidationIssue, AppError.
    │   │       ├── ids.rs                    # [OK] Strongly typed, UUIDv7-backed identifiers for every domain resource. Defines: TypedId.
    │   │       ├── lib.rs                    # [OK] Crate root of common: module declarations and public re-exports. Scope — Error/Result types, typed UUIDv7 IDs, UTC Timestamp, shared enums, cursor pagination, validation helpers, redaction.
    │   │       ├── pagination.rs             # [OK] Cursor pagination primitives. Defines: CursorPayload, Cursor, PageRequest, PageResponse.
    │   │       ├── redaction.rs              # [OK] Secret/PII redaction primitives. Defines: SensitiveField, SecretRedactor, is_sensitive_key, classify_key, +4 more.
    │   │       ├── result.rs                 # [OK] Result aliases and lightweight execution-outcome helpers. Defines: Result, OperationResult, ExecutionOutcome, partition_results, +1 more.
    │   │       ├── tests.rs                  # [OK] Unit tests for the foundation primitives: IDs, timestamps, enums, validation, pagination.
    │   │       ├── timestamps.rs             # [OK] The single timestamp type of the platform. Defines: Timestamp, utc_now.
    │   │       └── validation.rs             # [OK] Reusable validation helpers shared by DTOs and domain aggregates. Defines: ValidationBuilder, validate_non_empty, validate_length, validate_slug, +3 more.
    │   ├── contracts/                        # [GAP] Crate contracts: Versioned API DTOs, envelopes, auth claims, health/usage DTOs and the stable error contract.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the contracts crate: dependencies, features, lint inheritance.
    │   │   └── src/                          # [GAP] Source of the contracts crate.
    │   │       ├── agent.rs                  # [OK] Agent lifecycle DTOs. Defines: CreateAgentRequest, UpdateAgentRequest, PublishAgentRequest, DeployAgentRequest, +2 more.
    │   │       ├── analytics.rs              # [NEW] DTOs: OverviewResponse (KpiValue{value, delta_pct, sparkline}), PerformancePoint, TasksByAgentSlice, UsageCostResponse, InsightDto{title, body, metric_refs, severity}; ISO-8601 timestamps; deny_unknown_fields on requests.
    │   │       ├── api.rs                    # [OK] API envelope, versions and request metadata shared by all endpoints. Defines: ApiVersion, RequestMetadata, TenantRequestContext, ApiEnvelope, +4 more.
    │   │       ├── api_key.rs                # [NEW] CreateApiKeyRequest, ApiKeyCreated (plaintext field present only here), ApiKeySummary (prefix, scopes, last_used_at, revoked_at).
    │   │       ├── approval.rs               # [NEW] ApprovalDto, ApprovalDecisionRequest{decision, reason}.
    │   │       ├── assistant.rs              # [NEW] AssistantMessageRequest{text, quick_action}, AssistantMessageAccepted{execution_id}, AgentDraftProposal (validated structured output the UI uses to prefill the Create Agent form).
    │   │       ├── audit.rs                  # [NEW] AuditEventDto, AuditQuery (filters + cursor), AuditPage.
    │   │       ├── auth.rs                   # [GAP] Authentication/authorization contract DTOs. Defines: AuthnMethod, AuthenticateRequest, AuthenticationClaims, +3 more. CHANGE: ADD MeResponse and PermissionMatrix DTOs.
    │   │       ├── connector.rs              # [OK] Connector DTOs. Defines: RegisterConnectorRequest, TestConnectorRequest, ConnectorResponse, ConnectorStatusResponse.
    │   │       ├── data_hub.rs               # [NEW] DataSourceDto, CreateDataSourceRequest, SyncRequest, DocumentDto{status, chunk_count, failure_code}.
    │   │       ├── errors.rs                 # [OK] The stable error contract. Defines: StableApiError.
    │   │       ├── execution.rs              # [OK] Execution DTOs. Defines: StartExecutionRequest, CancelExecutionRequest, PauseExecutionRequest, ResumeExecutionRequest, +3 more.
    │   │       ├── health.rs                 # [OK] Health and readiness DTOs. Defines: LivenessResponse, DependencyHealth, ReadinessResponse, SystemHealthResponse.
    │   │       ├── lib.rs                    # [GAP] Crate root of contracts: module declarations and public re-exports. CHANGE: ADD module declarations for the new DTO files.
    │   │       ├── notification.rs           # [NEW] NotificationDto, MarkReadRequest, NotificationPreferences.
    │   │       ├── policy.rs                 # [OK] Policy DTOs. Defines: CreatePolicyRequest, UpdatePolicyRequest, PolicyResponse, EvaluatePolicyRequest, +2 more.
    │   │       ├── rollback.rs               # [NEW] RollbackRequest{reason, idempotency_key}, RollbackResult{new_version, rolled_back_to}, RollbackHistoryItem. (Agent-specific RollbackAgentRequest stays in agent.rs.).
    │   │       ├── search.rs                 # [NEW] SearchQuery, SearchHit{kind, id, title, subtitle}.
    │   │       ├── task.rs                   # [OK] Task DTOs and query filters. Defines: SubmitTaskRequest, CancelTaskRequest, RetryTaskRequest, TaskQuery, +2 more.
    │   │       ├── tool.rs                   # [OK] Tool DTOs and schema payloads. Defines: ToolSchemaDto, RegisterToolRequest, ToolResponse, InvokeToolRequest, +1 more.
    │   │       ├── usage.rs                  # [OK] Usage/quota DTOs. Defines: Granularity, UsageQuery, UsageRecordResponse, UsageSummaryResponse, +1 more.
    │   │       ├── webhook.rs                # [NEW] WebhookEndpointDto, CreateWebhookRequest, WebhookDeliveryDto, ReplayRequest.
    │   │       └── workflow.rs               # [OK] Workflow lifecycle DTOs. Defines: WorkflowNodeDto, WorkflowEdgeDto, WorkflowGraphDto, CreateWorkflowRequest, +3 more.
    │   ├── domain/                           # [GAP] Crate domain: Pure aggregates (organization → session) and value objects; business invariants only, no I/O.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the domain crate: dependencies, features, lint inheritance.
    │   │   └── src/                          # [GAP] Source of the domain crate.
    │   │       ├── agent.rs                  # [GAP] Agent aggregate: orchestration metadata for an executable agent. Defines: AgentConfig, Agent. CHANGE: ADD baseline_minutes_per_task: Option<u32> and AgentOrigin {User, System}; System agents cannot be edited, deleted or republished through the API.
    │   │       ├── agent_capability.rs       # [OK] Agent capabilities: what an agent is allowed to access/execute. Defines: AgentCapability.
    │   │       ├── agent_version.rs          # [OK] Immutable agent versions. Defines: AgentVersion.
    │   │       ├── api_key.rs                # [OK] API key metadata. Defines: ApiKeyMetadataView, ApiKey.
    │   │       ├── audit_event.rs            # [OK] Immutable audit events. Defines: AuditActor, AuditEvent.
    │   │       ├── connector.rs              # [OK] Connector aggregate: configuration for talking to an external system. Defines: Connector.
    │   │       ├── credential.rs             # [OK] Credential metadata: everything about a credential *except* the secret. Defines: RotationMetadata, CredentialMetadata.
    │   │       ├── data_source.rs            # [NEW] DataSource aggregate: kind (upload|object_storage|web), config without secrets (credential via SecretReference), state machine draft → active → syncing → error → disabled.
    │   │       ├── document.rs               # [NEW] Document aggregate: source ref, content hash, ingestion status (pending|processing|ready|failed), chunk_count, failure_code; chunks are owned by the Python RAG plane.
    │   │       ├── entitlement.rs            # [OK] Entitlements: tenant-level feature access derived from plan/contract. Defines: FeatureKey, Entitlement.
    │   │       ├── execution.rs              # [OK] The Execution aggregate: one complete workflow/agent run. Defines: ResourceUsageSummary, Execution.
    │   │       ├── execution_step.rs         # [OK] Execution steps: individual runtime actions inside an execution. Defines: StepError, ExecutionStep.
    │   │       ├── idempotency.rs            # [OK] The durable idempotency contract for execution submission. Defines: IdempotencyRecord, IdempotencyDecision.
    │   │       ├── lib.rs                    # [GAP] Crate root of domain: module declarations and public re-exports. CHANGE: ADD module declarations for workflow_version, rollback_record, data_source, document.
    │   │       ├── membership.rs             # [OK] Membership aggregate: joins a user to an organization (and optionally one of its tenants) with a set of. Defines: Membership.
    │   │       ├── notification.rs           # [OK] Notifications and their delivery lifecycle. Defines: Notification.
    │   │       ├── organization.rs           # [OK] Organization aggregate: the top-level administrative/billing boundary. Defines: OrganizationSettings, Organization.
    │   │       ├── policy.rs                 # [OK] Policy aggregate: versioned, scoped rule sets evaluated by the policy engine. Defines: Policy.
    │   │       ├── project.rs                # [OK] Project aggregate: a logical workspace grouping agents, workflows, tools and executions under a tenant. Defines: ProjectConfig, Project.
    │   │       ├── quota.rs                  # [OK] Quota definitions and their *definition-time* state. Defines: Quota.
    │   │       ├── rollback_record.rs        # [NEW] RollbackRecord value object: kind (agent|workflow|policy), resource id, from_version, new to_version, actor, reason, idempotency key.
    │   │       ├── schedule.rs               # [OK] Schedule aggregate: cron / interval / one-time triggers that materialize tasks. Defines: ScheduleKind, Schedule.
    │   │       ├── secret_reference.rs       # [OK] Secret references: typed pointers into external secret stores. Defines: SecretReference.
    │   │       ├── session.rs                # [OK] Session aggregate: authentication session metadata. Defines: Session.
    │   │       ├── state_machine.rs          # [OK] The single canonical state machine for the durable execution pipeline. Defines: AggregateKind, ActorKind, TransitionActor, TransitionFailure, +8 more.
    │   │       ├── subscription.rs           # [OK] Subscription state relevant to authorization. Defines: Subscription.
    │   │       ├── task.rs                   # [OK] The Task aggregate: one unit of dispatched durable work. Defines: Task.
    │   │       ├── task_attempt.rs           # [OK] TaskAttempt: one execution try of one task, durably recorded. Defines: TaskAttempt.
    │   │       ├── tenant.rs                 # [OK] Tenant aggregate: an isolated customer workspace inside an organization. Defines: Tenant.
    │   │       ├── tool.rs                   # [OK] Tool aggregate: registered invocable capabilities (HTTP endpoints, code, connector-mediated operations) with validated runtime config.
    │   │       ├── tool_permission.rs        # [OK] Tool permissions: bindings deciding who may invoke which tool, how. Defines: ToolInvocationContext, ToolPermission.
    │   │       ├── usage.rs                  # [OK] Append-only usage records (metering/billing source) and their aggregation keys. Defines: ActorRef, UsageAggregationKey, UsageRecord.
    │   │       ├── user.rs                   # [OK] User aggregate: identity metadata only. Defines: User.
    │   │       ├── webhook.rs                # [OK] Webhook endpoints and deliveries. Defines: SignatureConfig, WebhookRetryPolicy, WebhookEndpoint, WebhookDelivery.
    │   │       ├── workflow.rs               # [OK] Workflow aggregate and its embedded graph. Defines: WorkflowGraph, Workflow.
    │   │       ├── workflow_edge.rs          # [OK] Workflow edges: directed transitions between nodes. Defines: WorkflowEdge.
    │   │       ├── workflow_node.rs          # [OK] Workflow nodes: the executable units of a workflow graph. Defines: NodeCapability, WorkflowNode.
    │   │       ├── workflow_version.rs       # [NEW] WorkflowVersion aggregate: immutable snapshot of a published graph (nodes, edges, topological order, checksum, created_by, rollback_of); monotonic version number, never mutated after creation.
    │   │       └── value_objects/            # [OK] Typed value objects: Email, Slug, SemanticVersion, SafeUrl, TokenBudget, ResourceLimits.
    │   │           ├── email.rs              # [OK] Validated email address value object. Defines: Email.
    │   │           ├── mod.rs                # [OK] Domain value objects: validated, immutable-by-convention primitives shared by aggregates.
    │   │           ├── resource_limits.rs    # [OK] Per-execution resource limits. Defines: ResourceLimits.
    │   │           ├── semver.rs             # [OK] Semantic version wrapper for agent/workflow versions. Defines: SemanticVersion.
    │   │           ├── slug.rs               # [OK] Slug value object: URL/identifier-safe short names. Defines: Slug.
    │   │           ├── token_budget.rs       # [OK] Token budget for LLM token consumption control. Defines: TokenBudget.
    │   │           └── url.rs                # [OK] Safe URL value object with scheme/host validation and basic SSRF guards. Defines: SafeUrl.
    │   ├── events/                           # [OK] Crate events: Event envelope, in-memory bus, transactional outbox and publisher.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the events crate: dependencies, features, lint inheritance.
    │   │   └── src/                          # [OK] Source of the events crate.
    │   │       ├── bus.rs                    # [OK] In-process pub/sub event bus with pattern-matched delivery. Defines: ConsumerId, SubscriptionFilter, EventHandler, ConsumerStats, +2 more.
    │   │       ├── envelope.rs               # [OK] The immutable event envelope: ids, type + version, aggregate, tenant, causation/correlation. Defines: EventTypeError, EventMetadata, EventEnvelope, EventEnvelopeBuilder, +1 more.
    │   │       ├── lib.rs                    # [OK] Crate root of events: module declarations and public re-exports. Scope — Event envelope, in-memory bus, transactional outbox and publisher.
    │   │       ├── outbox.rs                 # [OK] The transactional outbox: the ONLY path by which durable state changes become events on the bus. Defines: RetryPolicy, OutboxRecord, OutboxStorePort, InMemoryOutbox, +3 more.
    │   │       └── publisher.rs              # [OK] Adapter: orchestration's EventPublisher trait onto the event bus. Defines: BusEventPublisher.
    │   ├── execution/                        # [GAP] Crate execution: Executor, step/tool runners, agent-bridge port, result processing, output validation, resource guard, sandbox.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the execution crate: dependencies, features, lint inheritance.
    │   │   └── src/                          # [GAP] Source of the execution crate.
    │   │       ├── agent_bridge.rs           # [OK] The port to the Python agent runtime — the contract the Rust side uses to ask Python to run an agent. Defines: AgentRunStatus, AgentUsage, AgentRunRequest, AgentRunVerdict, +3 more.
    │   │       ├── data_hub_bridge.rs        # [NEW] DataHubBridgePort trait + typed request/result (sync_source, ingest_document, purge_document) with deadlines and cancellation; the gRPC adapter lives in crates/ai-bridge.
    │   │       ├── executor.rs               # [OK] The canonical execution runtime: claims one execution and drives it end-to-end through the workflow. Defines: ExecutorConfig, StepStorePort, RunOutcome, AgentRunOutcome, +3 more.
    │   │       ├── lib.rs                    # [OK] Crate root of execution: module declarations and public re-exports. Scope — Executor, step/tool runners, agent-bridge port, result processing, output validation, resource guard, sandbox.
    │   │       ├── node_adapters.rs          # [OK] Bind the execution runners into the orchestration node registry so a workflow graph is runnable. Defines: RunScope, SharedScopes, AdapterWiring, AgentNodeExecutor, +4 more.
    │   │       ├── output_validator.rs       # [OK] Node output validation: contracts that outputs must satisfy before they flow downstream or into. Defines: PortabilityLimits, OutputValidator, OutputRejection.
    │   │       ├── resource_guard.rs         # [OK] Per-execution resource guard: converts ResourceLimits + token budgets into enforceable runtime decisions. Defines: GuardSnapshot, ResourceGuard.
    │   │       ├── result_processor.rs       # [OK] Result processing and the failure taxonomy: the last gate before anything a runner produced is persisted. Defines: FailureClass, ResultProcessor, classify_failure.
    │   │       ├── sandbox.rs                # [OK] Sandbox policies for hermetic execution. Defines: EgressPolicy, SandboxPolicy.
    │   │       ├── step_runner.rs            # [OK] Step runner: execution-step bookkeeping around unit work attempts. Defines: StepTransition, StepRunner.
    │   │       └── tool_runner.rs            # [OK] Tool runner: policy → sandbox → bridge, with payload and timeout guards. Defines: ToolCatalogPort, ToolInvocationRequest, ToolInvocationReport, HttpToolBridge, +2 more.
    │   ├── integrations/                     # [OK] Crate integrations: Connector registry and manifests, SSRF-hardened HTTP client, webhooks, OAuth.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the integrations crate: dependencies, features, lint inheritance.
    │   │   └── src/                          # [OK] Source of the integrations crate.
    │   │       ├── connectors.rs             # [OK] Category-driven connector factory and connection probes. Defines: ConnectionProbePort, ConnectorFactory, validate_category_config, clean_error_fragment.
    │   │       ├── http_guard.rs             # [OK] The SSRF egress guard — the policy every production outbound HTTP call goes through. Defines: EgressPolicy, HttpMethod, HttpRequest, HttpResponse, +4 more.
    │   │       ├── lib.rs                    # [OK] Crate root of integrations: module declarations and public re-exports. Scope — Connector registry and manifests, SSRF-hardened HTTP client, webhooks, OAuth.
    │   │       ├── manifest.rs               # [OK] Provider manifests: declarative descriptors telling the integration layer what a provider IS before any. Defines: ProviderManifest, builtin_manifests.
    │   │       ├── oauth.rs                  # [OK] OAuth 2.0 authorization-code + PKCE core. Defines: TokenSecret, PkcePair, OAuth2Config, PendingAuthorization, +5 more.
    │   │       ├── registry.rs               # [OK] Registries: provider manifests and tenant-installed connectors. Defines: ProviderRegistryPort, ConnectorRegistryPort, InMemoryProviderRegistry, InMemoryConnectorRegistry.
    │   │       └── webhooks.rs               # [OK] Webhook plumbing. Defines: InboundSignature, StoredEvent, WebhookSecretPort, WebhookStorePort, +7 more.
    │   ├── messaging/                        # [OK] Crate messaging: NATS/JetStream-shaped broker port, codecs, headers, subjects, gRPC helpers, broker health.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the messaging crate: dependencies, features, lint inheritance.
    │   │   ├── src/                          # [OK] Source of the messaging crate.
    │   │   │   ├── broker.rs                 # [OK] JetStream-shaped broker port with an in-memory reference implementation. Defines: PublishRequest, BrokerMessage, AckInstruction, ConsumerConfig, +4 more.
    │   │   │   ├── codec.rs                  # [OK] Framed payload codecs with hard size caps. Defines: CodecConfig, CodecError, EventFrameCodec, JsonEventCodec, +1 more.
    │   │   │   ├── connection.rs             # [OK] Connection lifecycle: explicit state machine with bounded backoff reconnection. Defines: ConnectionState, ReconnectPolicy, ConnectionController.
    │   │   │   ├── grpc.rs                   # [OK] Dependency-free gRPC helpers. Defines: GrpcCode, GrpcMetadata, MetadataError, TraceContext, +4 more.
    │   │   │   ├── headers.rs                # [OK] Validated message-header sets and tracing-context propagation. Defines: HeaderError, HeaderSet, PropagationContext, normalize_header_key, +2 more.
    │   │   │   ├── health.rs                 # [OK] Broker health probes. Defines: BrokerHealthProbe.
    │   │   │   ├── lib.rs                    # [OK] Crate root of messaging: module declarations and public re-exports. Scope — NATS/JetStream-shaped broker port, codecs, headers, subjects, gRPC helpers, broker health.
    │   │   │   ├── nats.rs                   # [OK] NATS broker adapter, enabled by the nats feature. Defines: NatsBrokerConfig, NatsBroker, NatsEventBus, event_subject, +1 more.
    │   │   │   └── subjects.rs               # [OK] NATS-shaped subject names: construction, validation, wildcard matching. Defines: SubjectError, Subject, SubjectFilter.
    │   │   └── tests/                        # [OK] Integration tests of the messaging crate.
    │   │       └── live_nats.rs              # [OK] Live NATS/JetStream integration suite for the nats feature's mas_messaging::nats::NatsBroker.
    │   ├── observability/                    # [OK] Crate observability: Redacting JSON logging, metrics, tracing, health registry, audit telemetry.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the observability crate: dependencies, features, lint inheritance.
    │   │   └── src/                          # [OK] Source of the observability crate.
    │   │       ├── audit_telemetry.rs        # [OK] Bridges append-only domain::AuditEvents onto the log/metrics planes. Defines: TelemetryAuditor, audit_log_level, register_audit_metrics, class_label.
    │   │       ├── health.rs                 # [OK] Component health registry: liveness vs readiness probes aggregated into a deterministic platform status. Defines: ComponentHealth, ProbeKind, HealthCheckPort, AggregateHealth, +3 more.
    │   │       ├── lib.rs                    # [OK] Crate root of observability: module declarations and public re-exports. Scope — Redacting JSON logging, metrics, tracing, health registry, audit telemetry.
    │   │       ├── logging.rs                # [OK] Structured JSON logging with mandatory redaction. Defines: LogEvent, LogSinkPort, JsonLogger, InMemoryLogSink, +1 more.
    │   │       ├── metrics.rs                # [OK] Cardinality-guarded metrics registry with Prometheus-compatible text exposition. Defines: LabelSet, MetricKind, MetricsRegistry, validate_family, +1 more.
    │   │       └── tracing_otel.rs           # [OK] W3C traceparent-compatible trace plumbing with an OTLP-shaped export pipeline. Defines: TraceId, SpanId, SpanContext, SpanStatus, +5 more.
    │   ├── orchestration/                    # [OK] Crate orchestration: OrchestrationEngine, dispatcher, scheduler, DAG workflow runtime, retry/timeout/cancellation/idempotency/checkpoint/DLQ/locks.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the orchestration crate: dependencies, features, lint inheritance.
    │   │   └── src/                          # [OK] Source of the orchestration crate.
    │   │       ├── cancellation.rs           # [OK] Cooperative cancellation propagation. Defines: CancellationToken.
    │   │       ├── checkpoint.rs             # [OK] Durable execution checkpoints for crash-safe recovery. Defines: NodeExecutionState, ExecutionCheckpoint, RestoredExecution.
    │   │       ├── concurrency.rs            # [OK] Concurrency enforcement: global + per-tenant + per-project execution limits. Defines: ConcurrencyLimits, ConcurrencyPermit, TenantConcurrencyLimiter, ExecutionSemaphore, +1 more.
    │   │       ├── coordinator.rs            # [OK] Execution coordination: parent/child trees (agent delegation). Defines: JoinRequirement, ChildOutcome, JoinDecision, ExecutionCoordinator.
    │   │       ├── dead_letter.rs            # [OK] Dead letters for tasks and events. Defines: DeadLetterRecordStatus, DeadLetterTask, DeadLetterEvent, DeadLetterAction, +2 more.
    │   │       ├── dependency_resolver.rs    # [OK] DAG dependency resolution for workflow graphs. Defines: NodeReadiness, DependencyResolver.
    │   │       ├── dispatcher.rs             # [OK] Task dispatch: from a stored, queued task to a transport hop. Defines: DispatchEnvelope, DispatchReceipt, DispatchTransport, TaskDispatcher, +2 more.
    │   │       ├── engine.rs                 # [OK] The orchestration engine: every lifecycle entry point of the platform funnels through here. Defines: NewTaskCommand, StartExecutionCommand, TaskStorePort, ExecutionStorePort, +8 more.
    │   │       ├── idempotency.rs            # [OK] Idempotency keys and the store preventing duplicate task/execution creation. Defines: IdempotencyKey, IdempotencyRecord, Reservation, IdempotencyStore, +1 more.
    │   │       ├── lib.rs                    # [OK] Crate root of orchestration: module declarations and public re-exports. Scope — OrchestrationEngine, dispatcher, scheduler, DAG workflow runtime, retry/timeout/cancellation/idempotency/checkpoint/DLQ/locks.
    │   │       ├── locks.rs                  # [OK] Distributed lock abstraction with fencing tokens. Defines: LockError, LockGuard, DistributedLock, InMemoryDistributedLock.
    │   │       ├── node_executor.rs          # [OK] The plugin surface of the workflow runtime: one executor per node type. Defines: NodeFailure, NodeOutcome, NodeExecutionInput, NodeExecutor, +1 more.
    │   │       ├── retry.rs                  # [OK] Retry policies: backoff strategies with jitter and retryable-error classification. Defines: BackoffStrategy, RetryDecision, RetryPolicy.
    │   │       ├── runtime_context.rs        # [OK] The immutable-per-request execution context threaded through the whole pipeline. Defines: RuntimeContext, RuntimeContextSnapshot.
    │   │       ├── scheduler.rs              # [OK] The runtime work scheduler: fair, priority-ordered selection of pending work. Defines: ScheduledWork, RuntimeScheduler.
    │   │       ├── state_machine.rs          # [OK] Generic state machine with an explicit transition table. Defines: TransitionTable, Transition, StateMachine.
    │   │       ├── timeout.rs                # [OK] Deadlines and timeout policies. Defines: Deadline, TimeoutPolicy.
    │   │       └── workflow_runtime.rs       # [OK] The workflow runtime: drives one validated graph for one execution. Defines: SuspensionReason, RuntimeRunResult, WorkflowRuntime.
    │   ├── persistence/                      # [GAP] Crate persistence: SQLx pool, transactions, migration runner, repositories, row models, RLS context.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the persistence crate: dependencies, features, lint inheritance.
    │   │   ├── examples/                     # [OK] Runnable examples.
    │   │   │   └── mas_migrate.rs            # [OK] mas_migrate — thin operator front over mas_persistence's migration planner + verified runner.
    │   │   ├── src/                          # [GAP] Source of the persistence crate.
    │   │   │   ├── directory.rs              # [OK] The PostgreSQL implementation of the tenancy directory ports. Defines: PgTenantDirectory, PgMembershipDirectory.
    │   │   │   ├── durable.rs                # [OK] The production implementation of DurableExecutionPort. Defines: PgDurableExecution.
    │   │   │   ├── error.rs                  # [OK] SQL error → platform error mapping. Defines: MapSqlx, map_sqlx.
    │   │   │   ├── lib.rs                    # [OK] Crate root of persistence: module declarations and public re-exports. Scope — SQLx pool, transactions, migration runner, repositories, row models, RLS context.
    │   │   │   ├── migrations.rs             # [GAP] The embedded migration registry and runner. Defines: Migration, AppliedMigration, MigrationPlanError, +3 more. CHANGE: ADD migrations 0020–0026 to the embedded registry (checksums, strict order).
    │   │   │   ├── pool.rs                   # [OK] Pool configuration, construction, and health probing. Defines: DatabaseUrl, PoolConfig, PoolStats, Database.
    │   │   │   ├── rls.rs                    # [OK] The row-level-security context — the GUC statements that arm PostgreSQL's policies inside a transaction. Defines: RlsContext, RlsStatement.
    │   │   │   ├── rows.rs                   # [GAP] Row models and the column ↔ domain hydration bridge. Defines: OrganizationRow, TenantRow, ProjectRow, +6 more. CHANGE: ADD row↔domain bridges for the new aggregates (re-run domain invariants on hydration).
    │   │   │   ├── scoped.rs                 # [OK] The one safe way to open a database transaction that touches tenant-owned rows. Defines: ScopedPoolStats, pool_stats.
    │   │   │   ├── scoped_request.rs         # [OK] One transaction per request, with row-level security armed inside it. Defines: ScopedExecutor, has_scope, current_scope, connection, +1 more.
    │   │   │   ├── transaction.rs            # [OK] Unit-of-work transactions with explicit lifecycle. Defines: UnitOfWork, atomic.
    │   │   │   └── repositories/             # [GAP] Postgres stores implementing the application ports.
    │   │   │       ├── agents.rs             # [GAP] The agents + agent_versions repositories. CHANGE: ADD the two new columns to every agents SELECT/INSERT/UPDATE and to row hydration.
    │   │   │       ├── analytics.rs          # [NEW] Dedicated Postgres store for execution_rollups_hourly reads and the rollup refresh call implementing the matching application port; transactional *_in writes, get_*_scoped reads refuse a mismatched RLS scope, every statement carries a tenant predicate (enforced by tenant_sql_contract).
    │   │   │       ├── api_keys.rs           # [OK] api_keys repository: the production bearer boundary's lookup surface. Defines: ApiKeyRow, ApiKeyStore.
    │   │   │       ├── approvals.rs          # [NEW] Postgres implementation of policy::approval::ApprovalStorePort over approval_requests/approval_votes; transactional *_in writes, get_*_scoped reads refuse a mismatched RLS scope, every statement carries a tenant predicate.
    │   │   │       ├── audit.rs              # [OK] Append-only audit log store. Defines: PostgresAuditLog.
    │   │   │       ├── broker.rs             # [OK] PgTaskBroker — the durability-first broker for PostgreSQL-only deployments. Defines: PgTaskBroker.
    │   │   │       ├── connectors.rs         # [NEW] Dedicated Postgres store for connectors + credentials (SecretReference only) implementing the matching application port; transactional *_in writes, get_*_scoped reads refuse a mismatched RLS scope, every statement carries a tenant predicate (enforced by tenant_sql_contract).
    │   │   │       ├── data_sources.rs       # [NEW] Dedicated Postgres store for data_sources implementing the matching application port; transactional *_in writes, get_*_scoped reads refuse a mismatched RLS scope, every statement carries a tenant predicate (enforced by tenant_sql_contract).
    │   │   │       ├── documents.rs          # [NEW] Dedicated Postgres store for documents metadata implementing the matching application port; transactional *_in writes, get_*_scoped reads refuse a mismatched RLS scope, every statement carries a tenant predicate (enforced by tenant_sql_contract).
    │   │   │       ├── execution_steps.rs    # [NEW] Dedicated Postgres store for execution_steps timeline reads implementing the matching application port; transactional *_in writes, get_*_scoped reads refuse a mismatched RLS scope, every statement carries a tenant predicate (enforced by tenant_sql_contract).
    │   │   │       ├── executions.rs         # [OK] The executions repository and its per-tenant idempotency lookup. Defines: ExecutionStore, fetch_execution_row.
    │   │   │       ├── mod.rs                # [GAP] The concrete PostgreSQL repositories, and the split between them. CHANGE: ADD module declarations/exports for the new stores.
    │   │   │       ├── notifications.rs      # [NEW] Dedicated Postgres store for notifications + read state implementing the matching application port; transactional *_in writes, get_*_scoped reads refuse a mismatched RLS scope, every statement carries a tenant predicate (enforced by tenant_sql_contract).
    │   │   │       ├── outbox.rs             # [OK] The PostgreSQL implementation of the transactional outbox store. Defines: PostgresOutboxStore.
    │   │   │       ├── policies.rs           # [NEW] Dedicated Postgres store for policies + policy_decisions implementing the matching application port; transactional *_in writes, get_*_scoped reads refuse a mismatched RLS scope, every statement carries a tenant predicate (enforced by tenant_sql_contract).
    │   │   │       ├── rollbacks.rs          # [NEW] Dedicated Postgres store for rollback_records (append-only) implementing the matching application port; transactional *_in writes, get_*_scoped reads refuse a mismatched RLS scope, every statement carries a tenant predicate (enforced by tenant_sql_contract).
    │   │   │       ├── schedules.rs          # [OK] The schedules + schedule_runs repositories. Defines: ScheduleStore.
    │   │   │       ├── scoped.rs             # [OK] RLS-scoped store plumbing for tenant-owned JSON-document tables. Defines: DocumentTable, ScopedStore.
    │   │   │       ├── services.rs           # [OK] PgServices — the production counterpart of InMemoryServices. Defines: PgServices.
    │   │   │       ├── tasks.rs              # [OK] The tasks repository — the worker's durability plane. Defines: TaskStore, fetch_task_row.
    │   │   │       ├── tenancy.rs            # [OK] Tenant-facing CRUD repositories: organizations, tenants, projects, users, memberships. Defines: OrganizationStore, TenantStore, ProjectStore, UserStore, +1 more.
    │   │   │       ├── tools.rs              # [NEW] Dedicated Postgres store for tools + tool_permissions implementing the matching application port; transactional *_in writes, get_*_scoped reads refuse a mismatched RLS scope, every statement carries a tenant predicate (enforced by tenant_sql_contract).
    │   │   │       ├── usage.rs              # [NEW] Dedicated Postgres store for subscriptions, entitlements, quotas, usage_records reads implementing the matching application port; transactional *_in writes, get_*_scoped reads refuse a mismatched RLS scope, every statement carries a tenant predicate (enforced by tenant_sql_contract).
    │   │   │       ├── webhooks.rs           # [NEW] Dedicated Postgres store for webhooks + webhook_deliveries implementing the matching application port; transactional *_in writes, get_*_scoped reads refuse a mismatched RLS scope, every statement carries a tenant predicate (enforced by tenant_sql_contract).
    │   │   │       ├── workflow_versions.rs  # [NEW] Dedicated Postgres store for workflow_versions snapshots implementing the matching application port; transactional *_in writes, get_*_scoped reads refuse a mismatched RLS scope, every statement carries a tenant predicate (enforced by tenant_sql_contract).
    │   │   │       └── workflows.rs          # [OK] The workflows repository. Defines: WorkflowStore, slugify.
    │   │   └── tests/                        # [GAP] Live-Postgres and SQL contract tests.
    │   │       ├── analytics_live_pg.rs      # [NEW] Live-Postgres: rollup refresh correctness, window boundaries, per-tenant isolation of KPI queries.
    │   │       ├── live_pg.rs                # [OK] The live PostgreSQL integration suite, including the database half of the multi-tenant isolation.
    │   │       ├── rollback_live_pg.rs       # [NEW] Live-Postgres: rollback creates a new version, history is append-only, cross-tenant rollback reads as not found, idempotent replay returns the first result.
    │   │       ├── tenant_sql_contract.rs    # [GAP] The database-level half of the multi-tenant isolation guarantee. CHANGE: EXTEND to scan the new repositories' SQL for tenant predicates.
    │   │       └── support/                  # [OK] Shared live-database test helpers.
    │   │           └── mod.rs                # [OK] Shared harness for the live-PostgreSQL suite: the environment gate.
    │   ├── policy/                           # [OK] Crate policy: Policy engine, expression evaluator, approvals, decision cache and decision audit.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the policy crate: dependencies, features, lint inheritance.
    │   │   └── src/                          # [OK] Source of the policy crate.
    │   │       ├── adapter.rs                # [OK] Adapter from the orchestration PolicyPort onto the PolicyEngine. Defines: EnginePolicyPort.
    │   │       ├── approval.rs               # [OK] Approval requests: raised by require_approval rules, decided by quorum. Defines: ApprovalVote, ApprovalOrigin, ApprovalRequest, ApprovalStorePort, +2 more.
    │   │       ├── cache.rs                  # [OK] TTL decision cache with stats and bounded size. Defines: CachedDecision, CacheStats, DecisionCache.
    │   │       ├── compiler.rs               # [OK] Compiles a domain Policy definition (JSON DSL) into a validated, canonical, checksummed CompiledPolicy. Defines: RuleId, Effect, Combining, CompiledRule, +3 more.
    │   │       ├── engine.rs                 # [OK] The evaluation engine (PDP): policy set → single decision. Defines: RuleRef, Evaluation, EvaluationBuilder, PolicyEngine.
    │   │       ├── expression.rs             # [OK] Condition DSL: attribute-path comparisons combined with all/any/not. Defines: Path, CmpOp, Expr.
    │   │       ├── lib.rs                    # [OK] Crate root of policy: module declarations and public re-exports. Scope — Policy engine, expression evaluator, approvals, decision cache and decision audit.
    │   │       ├── rate_limit.rs             # [OK] Rate limiting for rate_limit rules: spec parsing, key resolution and a thread-safe in-memory token. Defines: RateLimitSpec, RateLimitKey, RateLimiterPort, TokenBucketRateLimiter.
    │   │       ├── request.rs                # [OK] The evaluation request: subject + action + resource + attributes + scope. Defines: EvaluationSubject, EvaluationContext, EvaluationRequest.
    │   │       └── store.rs                  # [OK] Policy store port + in-memory implementation. Defines: PolicyQuery, PolicyStorePort, InMemoryPolicyStore.
    │   ├── quota/                            # [OK] Crate quota: Rate limiter, concurrency limiter, token meter, quota engine, counter store.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the quota crate: dependencies, features, lint inheritance.
    │   │   └── src/                          # [OK] Source of the quota crate.
    │   │       ├── concurrency.rs            # [OK] Concurrency limiter: leased execution slots per tenant. Defines: Permit, ConcurrencyLimiter.
    │   │       ├── counter.rs                # [OK] Atomic counter store: the abstraction Redis-backed adapters implement. Defines: CounterKey, CounterStorePort, InMemoryCounterStore.
    │   │       ├── engine.rs                 # [OK] The quota engine: registry-driven consumption decisions and the bridge into orchestration's QuotaPort. Defines: QuotaRegistryPort, InMemoryQuotaRegistry, ConsumptionDecision, QuotaEngine, +2 more.
    │   │       ├── lib.rs                    # [OK] Crate root of quota: module declarations and public re-exports. Scope — Rate limiter, concurrency limiter, token meter, quota engine, counter store.
    │   │       ├── meter.rs                  # [OK] Token-bucket metering: smooth burst control for metered consumption (LLM tokens, tool invocations), lazy refill, no partial spends.
    │   │       └── rate_limiter.rs           # [OK] Fixed-window rate limiter built on the counter store. Defines: RateLimit, RateDecision, RateLimiter.
    │   ├── scheduler-service/                # [OK] Crate scheduler-service: Process that ticks the scheduler runtime and dispatches due schedule runs.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the scheduler-service crate: dependencies, features, lint inheritance.
    │   │   ├── src/                          # [OK] Source of the scheduler-service crate.
    │   │   │   ├── dispatch.rs               # [OK] Turns due schedules into outbound calls, and signs them. Defines: InFlightLedger, LoopbackDispatcher, CompletionNotice, ScheduleRunRequest, +2 more.
    │   │   │   ├── driver.rs                 # [OK] The interval driver: recover once, tick every interval, drain on shutdown. Defines: DriverConfig, DriverStats, TickDriver, backoff_for.
    │   │   │   ├── durable.rs                # [OK] The production scheduler dispatcher: due schedules become durable executions and queued tasks through. Defines: DurableDispatcher.
    │   │   │   ├── egress.rs                 # [OK] The real network client behind the SSRF guard. Defines: ReqwestEgress.
    │   │   │   ├── lib.rs                    # [OK] Crate root of scheduler-service: module declarations and public re-exports. Scope — Process that ticks the scheduler runtime and dispatches due schedule runs.
    │   │   │   ├── main.rs                   # [OK] mas-scheduler process bootstrap: the schedule loop, the outbox relay and graceful shutdown.
    │   │   │   ├── plane_store.rs            # [OK] The scheduler's schedule store: two pools, one cross-tenant read. Defines: SchedulerPlaneStore.
    │   │   │   └── server.rs                 # [OK] The scheduler's completion HTTP listener — the endpoint a worker calls to say a dispatched run finished. Defines: InternalAuth, health_router, completion_router.
    │   │   └── tests/                        # [OK] Integration tests of the scheduler-service crate.
    │   │       └── driver_loop.rs            # [OK] End-to-end driver-loop test:due work → dispatch → completion.
    │   ├── scheduling/                       # [OK] Crate scheduling: Due scanner, catch-up/overlap policies, leases, delayed queue, scheduler runtime.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the scheduling crate: dependencies, features, lint inheritance.
    │   │   └── src/                          # [OK] Source of the scheduling crate.
    │   │       ├── due.rs                    # [OK] Due scanning with per-tenant fairness. Defines: DueWork, ScanConfig, DueScanner.
    │   │       ├── lease.rs                  # [OK] Fencing-token leases for multi-replica safety. Defines: Lease, LeaseStorePort, InMemoryLeaseStore, schedule_resource.
    │   │       ├── lib.rs                    # [OK] Crate root of scheduling: module declarations and public re-exports. Scope — Due scanner, catch-up/overlap policies, leases, delayed queue, scheduler runtime.
    │   │       ├── policies.rs               # [OK] The two judgment calls a scheduler makes every tick, as pure functions: * Catch-up. Defines: CatchUpPolicy, CatchUpDecision, OverlapPolicy, OverlapAction, +2 more.
    │   │       ├── queue.rs                  # [OK] The delayed queue: ordered, deduplicated, capacity-bounded, fair. Defines: DelayedQueue.
    │   │       ├── runner.rs                 # [OK] The deterministic scheduler runtime: which due schedules a tick fires, in what order. Defines: DispatchAck, DispatchPort, SchedulerConfig, TickReport, +2 more.
    │   │       └── store.rs                  # [OK] Schedule storage port + in-memory store. Defines: ScheduleStorePort, InMemoryScheduleStore.
    │   ├── security/                         # [GAP] Crate security: Authenticators, RBAC/ABAC authorization, API keys, tokens, secrets, internal service auth, authoritative scope.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the security crate: dependencies, features, lint inheritance.
    │   │   ├── src/                          # [GAP] Source of the security crate.
    │   │   │   ├── api_keys.rs               # [OK] API key lifecycle: generation, storage (hash + prefix only), verification and rotation. Defines: CreatedApiKey, StoredApiKey, ApiKeyStorePort, InMemoryApiKeyStore, +3 more.
    │   │   │   ├── audit.rs                  # [OK] Audit sink port: the mandatory evidence trail for security decisions. Defines: AuditSinkPort, NoopAuditSink, RecordingAuditSink, security_event.
    │   │   │   ├── auth_failure.rs           # [OK] The stable vocabulary for *why* an authentication or authorization decision failed. Defines: AuthFailureKind, AuthFailure.
    │   │   │   ├── authorization.rs          # [OK] The canonical authorization service: the ONE place a "may this principal do this to that resource?". Defines: AuthorizationRequest, AuthorizationDecision, AuthorizationService, AuthorizationServicePort, +1 more.
    │   │   │   ├── internal_service.rs       # [OK] Authentication for calls between the platform's own services. Defines: InternalServiceIdentity, sign, verify.
    │   │   │   ├── jwks.rs                   # [NEW] Asymmetric JWT verification for the IdP: JWKS fetched through the SSRF-guarded client, kid-keyed cache with bounded refresh, RS256/ES256 allow-list from config; alg=none and HS/RS key-confusion rejected. HS256 path in tokens.rs stays for service tokens.
    │   │   │   ├── lib.rs                    # [GAP] Crate root of security: module declarations and public re-exports. CHANGE: ADD jwks module.
    │   │   │   ├── principal.rs              # [OK] The ONE canonical principal — the authenticated actor, what it is bound to. Defines: TenantMembership, SecurityPrincipal.
    │   │   │   ├── scope.rs                  # [OK] The authoritative scope: the one value a database transaction is allowed to run under. Defines: AuthoritativeScope, machine_plane_scope.
    │   │   │   ├── scopes.rs                 # [OK] Scope vocabulary and matching: resource:verb with a trailing resource:* wildcard. Defines: ScopeSet, validate_scope.
    │   │   │   ├── secret_gate.rs            # [OK] The secret-manager abstraction — where a secret comes from. Defines: SecretResolverPort, InMemorySecretResolver, SecretGate.
    │   │   │   ├── secret_string.rs          # [OK] SecretString — the only legitimate home for raw secret bytes. Defines: SecretString, constant_time_eq.
    │   │   │   └── tokens.rs                 # [GAP] Token validation: HMAC-SHA256 (HS256) JWTs with strict claim checks. Defines: TokenClaims, JwtValidator, encode_hs256_for_test. CHANGE: ADD optional asymmetric verification path delegating to jwks.rs; HS256-only default and alg=none rejection stay.
    │   │   └── tests/                        # [NEW] Security-crate integration tests.
    │   │       └── jwks.rs                   # [NEW] Verification matrix: valid RS256, wrong kid, expired, wrong issuer/audience, alg=none, alg confusion, JWKS rotation, JWKS endpoint down (fail closed).
    │   ├── tenancy/                          # [OK] Crate tenancy: Tenant context/resolver, organization → tenant → project hierarchy, isolation enforcement, entitlements.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the tenancy crate: dependencies, features, lint inheritance.
    │   │   └── src/                          # [OK] Source of the tenancy crate.
    │   │       ├── context.rs                # [OK] The resolved request-time tenant context: tenant + organization + optional project + effective roles. Defines: ResolvedTenantContext, TenantContextSource.
    │   │       ├── entitlements.rs           # [OK] Entitlement gating: subscription → feature surface (and caps). Defines: EntitlementStorePort, PlanMatrixPort, PlanGrant, EntitlementProfile, +3 more.
    │   │       ├── hierarchy.rs              # [OK] Organization → tenant → project hierarchy: directory port, in-memory implementation. Defines: TenantDirectoryPort, MembershipDirectoryPort, InMemoryDirectory, InMemoryMemberships, +1 more.
    │   │       ├── isolation.rs              # [OK] Isolation guards: every domain object crossing a trust boundary is re-checked against the resolved. Defines: TenantScoped, IsolationPolicyPort, IsolationReport, IsolationService.
    │   │       ├── lib.rs                    # [OK] Crate root of tenancy: module declarations and public re-exports. Scope — Tenant context/resolver, organization → tenant → project hierarchy, isolation enforcement, entitlements.
    │   │       └── resolution.rs             # [OK] Resolution: (principal, requested tenant + project) → a proven context. Defines: MembershipView, ResolutionOutcome, TenantResolver.
    │   ├── worker/                           # [OK] Crate worker: Task consumer with leases, retries, heartbeats, the settlement protocol and graceful shutdown.
    │   │   ├── Cargo.toml                    # [OK] Manifest of the worker crate: dependencies, features, lint inheritance.
    │   │   └── src/                          # [OK] Source of the worker crate.
    │   │       ├── attempts.rs               # [OK] Durable attempt lifecycle: claim, own, heartbeat, settle. Defines: ClaimOneOutcome, AttemptCoordinator, OwnedAttempt, terminal_failure.
    │   │       ├── backoff.rs                # [OK] Retry pacing for Nak redeliveries: full-jitter exponential backoff. Defines: RetryPolicy.
    │   │       ├── capacity.rs               # [OK] Worker capacity accounting: how many *more* tasks this worker may take. Defines: CapacityGate.
    │   │       ├── classify.rs               # [OK] The canonical failure classification for the durable execution pipeline. Defines: FailureClass, ClassifiedFailure, classify, cancelled.
    │   │       ├── consumer.rs               # [OK] The bounded-concurrency task consumer: the plane between "a task message is on the subject" and "the. Defines: ConsumerConfig, ConsumerOutcome, TaskConsumer.
    │   │       ├── failure_matrix.rs         # [OK] The canonical durable execution pipeline's failure and recovery matrix.
    │   │       ├── grace.rs                  # [OK] Shutdown propagation: a tokio::sync::watch flipped by SIGINT/SIGTERM (or explicitly by tests). Defines: ShutdownWatch, ShutdownHandle, channel, watch_signals.
    │   │       ├── handler.rs                # [OK] Work execution seam: data arrives as a decoded TaskQueueMessage. Defines: TaskHandlerPort, OperationDispatcher.
    │   │       ├── lib.rs                    # [OK] Crate root of worker: module declarations and public re-exports. Scope — Task consumer with leases, retries, heartbeats, the settlement protocol and graceful shutdown.
    │   │       └── lifecycle.rs              # [OK] The task-row lifecycle port: keeping domain Task rows in lockstep with broker settlement. Defines: TaskLifecyclePort, InMemoryTaskStore.
    │   └── worker-service/                   # [OK] Crate worker-service: Worker process binary composing the worker crate with Postgres/NATS lanes.
    │       ├── Cargo.toml                    # [OK] Manifest of the worker-service crate: dependencies, features, lint inheritance.
    │       └── src/                          # [OK] Source of the worker-service crate.
    │           ├── agent_execution.rs        # [OK] The production execution.run handler: the real bridge between a durable task and the Python AI. Defines: AgentBridgeSettings, AgentRunDispatch, AgentRunHandler.
    │           └── main.rs                   # [OK] The production composition root for the worker process.
    ├── docs/                                 # [GAP] Documentation.
    │   ├── audit-adjudication-2026-10-01.md  # [OK] Line-by-line adjudication of audit.md (2026-09-30).
    │   ├── nats-wiring.md                    # [OK] NATS/JetStream wiring plan (Phase 20) — real broker lifecycle.
    │   ├── openapi.yaml                      # [NEW] OpenAPI 3.1 description of every /v1 HTTP route, schemas mirroring mas-contracts, security scheme (bearer JWT), error envelope. Source for the frontend's generated client; parity enforced by openapi_parity.rs.
    │   ├── production-wiring.md              # [OK] Production wiring plan (Phase 19+) — repository-complete production mode.
    │   ├── test-manifest.snapshot.txt        # [OK] Snapshot of the workspace test inventory (binary → test count) to detect silently dropped tests.
    │   └── runbooks/                         # [OK] Operator runbooks.
    │       ├── nats-live-tests.md            # [OK] Runbook: live NATS JetStream integration suite + broker lane.
    │       └── postgres-live-tests.md        # [OK] Runbook: live PostgreSQL integration suite.
    ├── migrations/                           # [GAP] Ordered SQL migrations (RLS on every tenant table).
    │   ├── 0001_extensions/                  # [OK] Base extensions, shared trigger functions, app roles.
    │   │   └── up.sql                        # [OK] 0001_extensions: base extensions, shared trigger functions, app roles.
    │   ├── 0002_tenancy/                     # [OK] Organizations, tenants, projects (Organization → Tenant → Project).
    │   │   └── up.sql                        # [OK] 0002_tenancy: organizations, tenants, projects (Organization → Tenant → Project).
    │   ├── 0003_identity/                    # [OK] Users, memberships, api keys, sessions.
    │   │   └── up.sql                        # [OK] 0003_identity: users, memberships, api keys, sessions.
    │   ├── 0004_agents/                      # [OK] Agents, agent versions, tools, tool permissions.
    │   │   └── up.sql                        # [OK] 0004_agents: agents, agent versions, tools, tool permissions.
    │   ├── 0005_workflows/                   # [OK] Workflows, nodes, edges.
    │   │   └── up.sql                        # [OK] 0005_workflows: workflows, nodes, edges.
    │   ├── 0006_execution/                   # [OK] Executions, execution steps, tasks, task attempts.
    │   │   └── up.sql                        # [OK] 0006_execution: executions, execution steps, tasks, task attempts.
    │   ├── 0007_policy/                      # [OK] Policy definitions and the append-only decision log.
    │   │   └── up.sql                        # [OK] 0007_policy: policy definitions and the append-only decision log.
    │   ├── 0008_billing/                     # [OK] Subscriptions, entitlements, quotas and usage records.
    │   │   └── up.sql                        # [OK] Migration 0008 — subscriptions, entitlements, quotas and usage records.
    │   ├── 0009_schedules/                   # [OK] Cron/interval schedules and their run history.
    │   │   └── up.sql                        # [OK] 0009_schedules: cron/interval schedules and their run history.
    │   ├── 0010_integrations/                # [OK] Connectors, credentials (SecretReference-only!), webhooks, webhook deliveries.
    │   │   └── up.sql                        # [OK] 0010_integrations: connectors, credentials (SecretReference-only!), webhooks, webhook deliveries.
    │   ├── 0011_audit/                       # [OK] The tamper-evident, append-only audit log.
    │   │   └── up.sql                        # [OK] 0011_audit: the tamper-evident, append-only audit log.
    │   ├── 0012_outbox/                      # [OK] Transactional outbox for domain events.
    │   │   └── up.sql                        # [OK] 0012_outbox: transactional outbox for domain events.
    │   ├── 0013_task_created_by/             # [OK] Materialize created_by on tasks.
    │   │   └── up.sql                        # [OK] Migration 0013 — materialize created_by on tasks.
    │   ├── 0014_durable_pipeline/            # [OK] The schema foundation of the canonical durable execution pipeline.
    │   │   └── up.sql                        # [OK] Migration 0014 — the schema foundation of the canonical durable execution pipeline.
    │   ├── 0015_directory_role/              # [OK] The tenancy-directory database role (security Phase 3 / 14).
    │   │   └── up.sql                        # [OK] The tenancy-directory database role (security Phase 3 / 14).
    │   ├── 0016_api_key_binding/             # [OK] Bind API keys to their tenant and organization.
    │   │   └── up.sql                        # [OK] Migration 0016 — bind API keys to their tenant and organization.
    │   ├── 0017_plane_roles/                 # [OK] The machine-plane database roles (security Phase 14).
    │   │   └── up.sql                        # [OK] The machine-plane database roles (security Phase 14).
    │   ├── 0018_api_key_owner/               # [OK] Add the owning-user column that api_keys projections already read.
    │   │   ├── down.sql                      # [OK] Reversal of migration 0018.
    │   │   └── up.sql                        # [OK] Add the owning-user column that api_keys projections already read.
    │   ├── 0019_scheduler_plane_role/        # [OK] The scheduler-plane database role (security Phase 14 / 25).
    │   │   ├── down.sql                      # [OK] Reversal of migration 0019.
    │   │   └── up.sql                        # [OK] The scheduler-plane database role (security Phase 14 / 25).
    │   ├── 0020_workflow_versions/           # [NEW] Immutable workflow version snapshots.
    │   │   ├── down.sql                      # [NEW] Reverses 0020_workflow_versions (drops objects it created; refuses to run when dependent data would be lost without an explicit flag).
    │   │   └── up.sql                        # [NEW] 0020_workflow_versions: workflow_versions(tenant_id, workflow_id, version, graph jsonb, topo_order, checksum, rollback_of, created_by) with RLS FORCE and an update/delete-forbidden trigger; backfills v1 from current published graphs (the domain already refers to this table but no migration creates it).
    │   ├── 0021_rollback_records/            # [NEW] Append-only rollback history.
    │   │   ├── down.sql                      # [NEW] Reverses 0021_rollback_records (drops objects it created; refuses to run when dependent data would be lost without an explicit flag).
    │   │   └── up.sql                        # [NEW] 0021_rollback_records: rollback_records append-only table (kind, resource_id, from_version, to_version, actor, reason, idempotency_key unique per tenant) with RLS and an append-only trigger.
    │   ├── 0022_analytics_rollups/           # [NEW] Hourly rollups for dashboard KPIs.
    │   │   ├── down.sql                      # [NEW] Reverses 0022_analytics_rollups (drops objects it created; refuses to run when dependent data would be lost without an explicit flag).
    │   │   └── up.sql                        # [NEW] 0022_analytics_rollups: execution_rollups_hourly(tenant_id, agent_id, bucket, tasks_completed, tasks_failed, tokens, cost_micros, p95_ms) plus a refresh function called by the worker plane; RLS; index on (tenant_id, bucket).
    │   ├── 0023_agent_dashboard_columns/     # [NEW] Agent columns needed by the dashboard: Time Saved baseline and system-agent origin.
    │   │   ├── down.sql                      # [NEW] Reverses 0023_agent_dashboard_columns.
    │   │   └── up.sql                        # [NEW] 0023_agent_dashboard_columns: ALTER agents ADD baseline_minutes_per_task numeric NULL (NULL = not configured, so the Time Saved KPI shows n/a instead of a guess) and origin text NOT NULL DEFAULT 'user' CHECK (origin IN ('user','system')) so built-in agents such as platform_assistant are read-only through the API.
    │   ├── 0024_data_hub/                    # [NEW] Data sources, documents and pgvector chunks.
    │   │   ├── down.sql                      # [NEW] Reverses 0024_data_hub (drops objects it created; refuses to run when dependent data would be lost without an explicit flag).
    │   │   └── up.sql                        # [NEW] 0024_data_hub: CREATE EXTENSION vector; data_sources and documents (Rust-owned metadata) and document_chunks(embedding vector, tenant_id, document_id, ordinal, text) with an HNSW index and RLS FORCE.
    │   ├── 0025_rag_plane_role/              # [NEW] Least-privilege DB role for the Python RAG plane.
    │   │   ├── down.sql                      # [NEW] Reverses 0025_rag_plane_role (drops objects it created; refuses to run when dependent data would be lost without an explicit flag).
    │   │   └── up.sql                        # [NEW] 0025_rag_plane_role: Least-privilege DB role for the Python RAG plane: SELECT/INSERT/DELETE on document_chunks only, nothing else; follows the 0017/0019 plane-role pattern.
    │   └── 0026_approvals/                   # [NEW] Persistent approval requests and votes.
    │       ├── down.sql                      # [NEW] Reverses 0026_approvals (refuses to drop while pending approvals exist unless forced).
    │       └── up.sql                        # [NEW] 0026_approvals: approval_requests (policy_id, origin, status, quorum, expires_at, decided_at) and approval_votes (one vote per approver per request) with RLS FORCE and a transition guard (terminal states are final); mirrors the invariants of policy::approval — today only InMemoryApprovalStore exists, so approvals vanish on restart.
    ├── scripts/                              # [GAP] Operator and CI helper scripts.
    │   ├── bench.sh                          # [OK] Runs every workspace Criterion bench, fast to slow; --quick = one pass per case; supports --save-baseline.
    │   ├── bootstrap-toolchain.sh            # [OK] Idempotently rebuilds the pinned Rust toolchain from rust-toolchain.toml (recovery after the sandbox wipes it).
    │   ├── check.sh                          # [GAP] CI gate in order: cargo fmt --check, clippy -D warnings, test --workspace --locked. CHANGE: ADD cargo-deny and openapi-parity steps.
    │   ├── dev-down.sh                       # [OK] Stops every process dev-up.sh started (SIGTERM, then SIGKILL after the drain budget).
    │   ├── dev-seed.sh                       # [NEW] Seeds a demo organization/tenant/project, sample agents, workflows and historical executions through the mas CLI so the dashboard shows real data in dev; refuses to run against production config.
    │   ├── dev-up.sh                         # [OK] Starts the in-memory dev trio: mas-api (HTTP :8080, gRPC :50051), mas-scheduler (:8090) and mas-worker (echo consumer over InMemoryBroker — dev lane only); logs in .dev-logs/.
    │   ├── migrate.sh                        # [OK] Applies (up) or exports migrations/NNNN_*/up.sql through the SQL-driven runner with checksums, strict order.
    │   ├── provision-plane-roles.sh          # [OK] Gives login credentials to the NOLOGIN plane roles (mas_directory, mas_worker, mas_outbox, mas_audit, mas_scheduler) so a deployed cluster can authenticate as them.
    │   └── smoke.sh                          # [OK] Post-deploy health gate against a running API using the production mas-cli client (BASE_URL, default http://127.0.0.1:8080).
    └── tests/                                # [GAP] Workspace-level contract, integration and security tests compiled into mas-api.
        ├── contract/                         # [OK] Wire-contract tests against golden fixtures.
        │   └── api_contract.rs               # [OK] Wire-contract assertions against the golden fixtures; red means the public contract changed.
        ├── fixtures/                         # [GAP] Golden JSON fixtures.
        │   ├── envelope_error.json           # [OK] Golden ApiEnvelope error fixture (VALIDATION_FAILED with field issues).
        │   ├── envelope_ok.json              # [OK] Golden ApiEnvelope success fixture (data + meta).
        │   ├── schedule_run_request.json     # [OK] Golden schedule-run request body the scheduler POSTs to the API.
        │   ├── workflow_graph.json           # [OK] Golden workflow graph (nodes + edges) used by contract tests.
        │   └── dashboard/                    # [NEW] Golden JSON fixtures for dashboard endpoints.
        │       ├── overview.json             # [NEW] Golden fixture for GET /v1/analytics/overview.
        │       └── rollback_result.json      # [NEW] Golden fixture for a rollback response.
        ├── integration/                      # [GAP] Cross-crate / cross-process integration tests.
        │   ├── dashboard_flow.rs             # [NEW] Full-stack flow: submit execution → worker settles → analytics overview reflects it → SSE event delivered → rollback of the agent version → audit trail complete.
        │   └── full_stack.rs                 # [OK] Full-stack test: real HTTP server with the production router driven by the real mas-cli client, no mocks.
        └── security/                         # [OK] Cross-cutting authorization/tenancy/RLS matrix.
            └── authorization_matrix.rs       # [OK] Security matrix: tenant naming, self-authorization, internal service identity, RLS guarantees.
```
