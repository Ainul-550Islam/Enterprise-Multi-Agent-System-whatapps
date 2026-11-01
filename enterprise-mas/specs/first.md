# FIRST.md — ENTERPRISE MULTI-AGENT SYSTEM + SaaS
# MASTER IMPLEMENTATION PROMPT — PYTHON AI ORCHESTRATOR + RUST INTEGRATION

## 0. ROLE

You are a Principal Software Architect, Staff Python Engineer, AI Platform Engineer, Security Engineer, Distributed Systems Engineer, and Production SRE.

Your task is to implement the missing Python AI Orchestrator layer for the existing Enterprise Multi-Agent System + SaaS codebase.

The uploaded ZIP is the source of truth.

DO NOT invent existing functionality.
DO NOT assume a component is implemented merely because a Rust trait, migration, interface, README statement, or filename exists.
DO NOT remove or silently rewrite existing working Rust logic.
DO NOT break existing APIs, database contracts, protobuf contracts, execution semantics, tests, or package boundaries.

This is an implementation task, but implementation must be performed against the real repository after inspecting the actual source tree.

---

# 1. NON-NEGOTIABLE CODE GENERATION RULES

## 1.1 NO SHORTENING — ABSOLUTE RULE

Never shorten code.

Never write:

```text
...
```

Never write:

```text
# Rest of the code here
```

Never write:

```text
// existing code omitted
```

Never write:

```text
# same as above
```

Never write placeholders instead of implementation.

When creating or replacing a file, output the COMPLETE FILE from the first line to the final line.

Every import, class, function, method, schema, type, exception, test, configuration field, and helper required by the file must be present.

The coding agent must preserve all existing logic when modifying an existing file.

If an existing file must change, inspect the complete file first and return the complete modified file, not a partial patch, unless the execution environment itself is applying a safe patch and the user explicitly permits patch output.

## 1.2 NO FAKE IMPLEMENTATIONS

Do not create fake production implementations merely to satisfy imports.

Do not use EchoHandler-like behavior for production AI execution.

Do not use an in-memory-only production agent bridge.

Do not silently fall back to mocked LLM responses.

Do not hard-code successful model responses.

Do not use synthetic usage counts.

Do not report successful execution when the provider call failed.

Any mock/fake/stub implementation must be explicitly marked test-only and must never be selected by production configuration.

## 1.3 NO SILENT FALLBACKS

Provider failures, authentication failures, policy denials, quota denials, malformed model output, timeouts, cancellation, and tool failures must produce explicit typed errors and deterministic state transitions.

A provider or tool outage must never silently become a successful execution.

## 1.4 PRESERVE EXISTING LOGIC

Before modifying Rust integration points, inspect and preserve existing Rust domain models, protobuf contracts, repository behavior, application services, security boundaries, tenancy behavior, migrations, tests, and public APIs.

Do not rewrite architecture merely for convenience.

Where a necessary production bridge is missing, add the smallest coherent adapter that integrates with the existing architecture.

## 1.5 SOURCE-DRIVEN IMPLEMENTATION

Before coding:

1. Read the repository tree.
2. Read Rust workspace manifests.
3. Read protobuf contracts.
4. Read domain execution models.
5. Read application execution service.
6. Read execution/orchestration traits.
7. Read event contracts.
8. Read security and tenancy contracts.
9. Read database schemas and migrations relevant to execution, tasks, attempts, agents, tools, credentials, policies, usage, audit, webhooks, and outbox.
10. Read existing tests.
11. Identify exact Rust integration points available today.
12. Only then create the Python layer.

Do not replace missing evidence with assumptions.

---

# 2. CURRENT ARCHITECTURAL FACT

The current repository contains a substantial Rust Core but does not contain the required Python AI Orchestrator implementation.

Therefore this implementation must add a real `python-orchestrator/` application rather than merely creating an empty package skeleton.

The target runtime is:

```text
Frontend / External Client
        |
        v
Rust API / Application Layer
        |
        v
Execution / Task / Event Infrastructure
        |
        +------ gRPC / durable messaging ------+
        |                                       |
        v                                       v
Rust Core                              Python AI Orchestrator
                                            |
                                            +--> Agent runtime
                                            +--> Planner / Router
                                            +--> Model provider layer
                                            +--> Prompt registry
                                            +--> Tool runtime
                                            +--> Memory
                                            +--> RAG
                                            +--> Guardrails
                                            +--> Evaluation hooks
                                            +--> Usage / cost metadata
                                            |
                                            v
                                       LLM Provider(s)
```

Rust remains the authoritative owner of platform execution state, tenancy, authorization, durable task state, audit, quotas, policy decisions, and platform persistence unless the existing architecture explicitly states otherwise.

Python owns AI execution concerns:

- agent behavior
- planning
- model routing
- prompt rendering
- tool orchestration
- AI memory management
- retrieval orchestration
- model output validation
- AI-specific guardrails
- AI provider adapters
- AI execution telemetry

Python must NOT bypass Rust ownership by directly mutating Rust-owned platform tables.

---

# 3. PYTHON ORCHESTRATOR OBJECTIVES

Implement a production-oriented Python AI Orchestrator with:

- typed configuration
- structured logging
- correlation/request IDs
- authenticated Rust communication
- tenant context propagation without trusting arbitrary caller headers
- agent registry
- agent runtime
- planner
- executor
- router
- supervisor
- delegation/handoff
- model provider abstraction
- provider registry
- provider failover
- timeout handling
- retry classification
- streaming abstraction
- token accounting
- model cost accounting
- versioned prompt registry
- safe prompt rendering
- tool abstraction
- tool schema validation
- tool permission checks delegated to authoritative platform policy where applicable
- tool timeout
- tool retries
- tool result validation
- HTTP tool guardrails
- memory abstractions
- short-term memory
- long-term memory adapter interface
- episodic memory
- semantic retrieval adapter
- memory compaction
- deletion/retention hooks
- RAG pipeline
- ingestion contracts
- chunking contracts
- embedding provider abstraction
- vector retrieval abstraction
- hybrid retrieval support
- reranking abstraction
- citation generation
- tenant/security filters
- input guardrails
- prompt injection mitigation
- output validation
- PII handling hooks
- policy bridge
- hallucination/citation validation hooks
- evaluation interfaces
- AI execution events
- health/readiness
- metrics/tracing hooks
- deterministic tests
- integration tests
- contract tests

---

# 4. TARGET FILE STRUCTURE

Implement the following structure exactly unless repository inspection demonstrates an existing package structure that must be preserved for compatibility.

Every file below MUST contain production-relevant functionality; do not create meaningless placeholder files.

```text
python-orchestrator/                                      # Python AI orchestration service root; owns AI runtime, provider adapters, tools, memory, RAG, guardrails, and Rust integration.
├── pyproject.toml                                        # Python package metadata, supported Python versions, dependencies, lint/type-check/test configuration, build configuration, and project tooling.
├── README.md                                             # Current implementation and operational documentation for the Python orchestrator; must describe real behavior only.
├── .python-version                                      # Explicit supported Python runtime version for reproducible local and CI execution.
├── .gitignore                                            # Python-specific generated artifacts, local environments, caches, and secret-bearing development files.
├── src/                                                  # Python application source tree.
│   └── mas_orchestrator/                                # Main Python package.
│       ├── __init__.py                                   # Package metadata and public package exports only; no side-effectful runtime startup.
│       ├── main.py                                      # Service entry point; builds configuration, dependencies, clients, health server, and controlled shutdown lifecycle.
│       ├── version.py                                   # Application/version metadata used for diagnostics and compatibility reporting.
│       ├── config/                                       # Configuration subsystem.
│       │   ├── __init__.py                              # Configuration package exports.
│       │   ├── settings.py                              # Strongly typed environment/configuration model with validation and safe defaults.
│       │   ├── enums.py                                 # Stable configuration enums such as environment and provider modes.
│       │   └── validation.py                            # Cross-field configuration validation and startup safety checks.
│       ├── api/                                          # Optional internal AI service HTTP API; must not bypass Rust authorization ownership.
│       │   ├── __init__.py                              # API package exports.
│       │   ├── app.py                                   # ASGI application factory and middleware registration.
│       │   ├── dependencies.py                          # Request-scoped dependencies, correlation IDs, authenticated Rust client, and runtime context.
│       │   ├── health.py                                # Liveness/readiness endpoints for the Python service.
│       │   ├── routes.py                                # API route registration and versioning.
│       │   ├── schemas.py                               # Pydantic request/response schemas for AI service operations.
│       │   └── errors.py                                # API error mapping that preserves typed error semantics.
│       ├── context/                                      # Execution context propagation.
│       │   ├── __init__.py                              # Context exports.
│       │   ├── execution_context.py                    # Immutable AI execution context containing execution/task/request/correlation identifiers and authoritative tenant metadata supplied by Rust.
│       │   └── contextvars.py                           # Safe async request/execution context propagation without mutable global state.
│       ├── contracts/                                    # Cross-language contracts.
│       │   ├── __init__.py                              # Contract exports.
│       │   ├── messages.py                              # Typed internal Python representations of Rust/Python messages.
│       │   ├── errors.py                                # Typed error codes shared conceptually with Rust contract errors.
│       │   ├── execution.py                             # Execution request/result/event contract models.
│       │   ├── agent.py                                 # Agent configuration/version/capability contract models.
│       │   ├── task.py                                  # Task/task-attempt contract models.
│       │   └── protocol_version.py                      # Explicit protocol compatibility/version negotiation rules.
│       ├── grpc/                                         # Rust Core gRPC integration.
│       │   ├── __init__.py                              # gRPC integration exports.
│       │   ├── client.py                                # Authenticated async Rust Core gRPC client with deadlines, cancellation, retries, and metadata propagation.
│       │   ├── channel.py                               # Secure gRPC channel creation, connection lifecycle, keepalive, TLS configuration, and shutdown.
│       │   ├── metadata.py                              # Correlation/request/tenant/service metadata propagation rules.
│       │   ├── errors.py                                # gRPC status to domain error mapping.
│       │   └── generated/                               # Generated protobuf Python artifacts if repository tooling requires committed generated files.
│       │       ├── __init__.py                          # Generated package exports.
│       │       └── mas_api_pb2.py                      # Generated protobuf message bindings; regenerate from authoritative Rust protobuf contract rather than hand-editing.
│       ├── agents/                                       # Multi-agent runtime.
│       │   ├── __init__.py                              # Agent package exports.
│       │   ├── base.py                                  # BaseAgent interface and lifecycle contract.
│       │   ├── models.py                                # Agent configuration, capabilities, runtime state, and result models.
│       │   ├── registry.py                              # Version-aware agent registry with deterministic lookup.
│       │   ├── factory.py                               # Agent construction/factory using validated configuration.
│       │   ├── runtime.py                               # Agent runtime coordinating planner, executor, tools, memory, model calls, and guardrails.
│       │   ├── planner.py                               # Planning abstraction and planning result schema.
│       │   ├── executor.py                              # Agent action execution coordinator.
│       │   ├── router.py                                # Intent/task routing to appropriate agent, workflow, model, or specialist.
│       │   ├── supervisor.py                            # Supervisor agent coordination, delegation, and termination conditions.
│       │   ├── delegation.py                            # Agent-to-agent delegation contract and propagation of execution context.
│       │   ├── handoff.py                               # Explicit handoff semantics, ownership transfer metadata, and loop prevention.
│       │   ├── state.py                                 # Agent state representation and bounded lifecycle transitions.
│       │   └── validation.py                            # Agent configuration/capability validation before execution.
│       ├── models/                                       # LLM/model provider abstraction.
│       │   ├── __init__.py                              # Model layer exports.
│       │   ├── base.py                                  # ModelProvider interface for chat/completion/structured output/streaming.
│       │   ├── types.py                                 # Typed model requests, messages, responses, usage, finish reasons, and cost metadata.
│       │   ├── registry.py                              # Provider/model registry and deterministic model selection.
│       │   ├── router.py                                # Policy-aware model routing based on task, capability, limits, and configured preference.
│       │   ├── failover.py                              # Provider/model failover logic using typed retryability and bounded fallback.
│       │   ├── retry.py                                 # Retry classification, exponential backoff, jitter, and retry budgets.
│       │   ├── streaming.py                             # Streaming event abstraction with cancellation and partial-result handling.
│       │   └── cost.py                                  # Token usage and model cost calculation hooks without fabricated values.
│       ├── providers/                                    # Concrete model providers.
│       │   ├── __init__.py                              # Provider registry/bootstrap exports.
│       │   ├── factory.py                               # Provider adapter factory based on validated configuration.
│       │   ├── openai.py                                # OpenAI-compatible provider implementation only if supported by the existing project requirements.
│       │   ├── anthropic.py                             # Anthropic provider implementation only if supported by the existing project requirements.
│       │   ├── compatible.py                            # Generic OpenAI-compatible endpoint adapter where explicitly configured.
│       │   └── base_http.py                             # Shared secure HTTP transport, timeouts, redaction, and error normalization for provider adapters.
│       ├── prompts/                                      # Prompt management.
│       │   ├── __init__.py                              # Prompt layer exports.
│       │   ├── models.py                                # Prompt/template/version metadata models.
│       │   ├── registry.py                              # Versioned prompt registry and immutable lookup semantics.
│       │   ├── renderer.py                              # Safe variable substitution and output size validation; no unsafe template evaluation.
│       │   ├── system.py                                # System-level prompt definitions and policies.
│       │   ├── safety.py                                # Safety/policy instruction templates.
│       │   └── hashing.py                               # Stable prompt/version fingerprinting for audit and reproducibility.
│       ├── tools/                                       # Tool execution layer.
│       │   ├── __init__.py                              # Tool layer exports.
│       │   ├── base.py                                  # Tool interface and invocation lifecycle contract.
│       │   ├── models.py                                # Tool definitions, schemas, calls, results, and permission models.
│       │   ├── registry.py                              # Tool registry with version/capability metadata.
│       │   ├── validation.py                            # JSON schema/input/output validation.
│       │   ├── permissions.py                           # Tool permission enforcement bridge to authoritative Rust policy/authorization checks.
│       │   ├── executor.py                              # Timeout/retry/concurrency-aware tool invocation runtime.
│       │   ├── http.py                                  # Secure outbound HTTP tool with SSRF-safe integration through a platform-approved policy boundary.
│       │   ├── database.py                              # Database tool adapter interface; must never bypass tenant/security controls.
│       │   ├── search.py                                # Document/search tool adapter interface.
│       │   ├── email.py                                 # Email tool adapter with explicit authorization and delivery result semantics.
│       │   ├── webhook.py                               # Webhook invocation adapter with signing/retry semantics delegated to platform policy where required.
│       │   └── sandbox.py                               # Sandboxed code/tool execution abstraction; production implementation must use an isolated execution boundary, never unrestricted subprocess execution.
│       ├── memory/                                      # Agent memory.
│       │   ├── __init__.py                              # Memory exports.
│       │   ├── models.py                                # Memory records, references, provenance, retention metadata, and sensitivity labels.
│       │   ├── base.py                                  # MemoryStore interface.
│       │   ├── short_term.py                            # Conversation/task-local bounded memory.
│       │   ├── long_term.py                             # Durable long-term memory adapter interface.
│       │   ├── episodic.py                              # Execution/episode memory storage interface and retrieval.
│       │   ├── semantic.py                              # Semantic memory retrieval abstraction.
│       │   ├── manager.py                               # Memory orchestration, prioritization, compaction, and lifecycle management.
│       │   ├── compaction.py                            # Context compaction and summarization rules.
│       │   ├── retrieval.py                             # Safe retrieval pipeline with tenant/security filters.
│       │   └── retention.py                             # Retention/deletion policy hooks and explicit purge behavior.
│       ├── rag/                                         # Retrieval augmented generation.
│       │   ├── __init__.py                              # RAG exports.
│       │   ├── models.py                                # Document/chunk/embedding/retrieval/citation models.
│       │   ├── ingestion.py                             # Document ingestion orchestration and validation.
│       │   ├── parsing.py                               # Document parsing abstraction.
│       │   ├── chunking.py                              # Deterministic text chunking with metadata preservation.
│       │   ├── embeddings.py                            # Embedding provider interface.
│       │   ├── index.py                                 # Vector index abstraction.
│       │   ├── retrieval.py                             # Vector and hybrid retrieval implementation interfaces.
│       │   ├── reranking.py                             # Reranking provider abstraction.
│       │   ├── citations.py                             # Citation generation with source provenance and validation.
│       │   └── security.py                              # Tenant/resource authorization filtering before and after retrieval.
│       ├── guardrails/                                  # AI safety and policy guardrails.
│       │   ├── __init__.py                              # Guardrail exports.
│       │   ├── models.py                                # Guardrail decision, violation, and validation models.
│       │   ├── input.py                                 # Input sanitization and pre-execution validation.
│       │   ├── prompt_injection.py                      # Prompt injection detection and mitigation controls.
│       │   ├── output.py                                # Model-output validation and schema checks.
│       │   ├── pii.py                                   # PII handling/detection abstraction with configurable redaction or blocking.
│       │   ├── tool_safety.py                           # Tool-use safety checks and untrusted tool-output handling.
│       │   ├── policy_bridge.py                         # Policy decision bridge to Rust authoritative policy service.
│       │   ├── citation_check.py                        # Citation/provenance validation hooks.
│       │   └── manager.py                               # Deterministic guardrail execution pipeline.
│       ├── workflows/                                   # AI-side workflow interpretation.
│       │   ├── __init__.py                              # Workflow exports.
│       │   ├── models.py                                # Workflow graph/node runtime DTOs consumed from Rust contracts.
│       │   ├── graph.py                                 # Graph validation and dependency resolution.
│       │   ├── node.py                                  # Node runtime abstractions.
│       │   ├── conditions.py                            # Condition expression evaluation under an allowlisted expression model.
│       │   ├── parallel.py                              # Bounded parallel execution helper.
│       │   ├── loops.py                                 # Safe bounded loop execution helper.
│       │   ├── state.py                                 # Workflow runtime state projection.
│       │   └── runtime.py                               # Orchestrates workflow nodes while respecting Rust execution ownership.
│       ├── services/                                    # Application orchestration services inside Python.
│       │   ├── __init__.py                              # Service exports.
│       │   ├── execution_service.py                     # Receives canonical AI execution requests from Rust and drives AI runtime lifecycle.
│       │   ├── agent_service.py                         # Agent lookup, lifecycle preparation, capability resolution, and validation.
│       │   ├── tool_service.py                          # Tool discovery/validation/authorization bridge.
│       │   ├── memory_service.py                        # Memory orchestration service facade.
│       │   ├── rag_service.py                           # RAG orchestration facade.
│       │   ├── policy_service.py                        # Policy bridge facade; Rust remains authoritative for platform decisions.
│       │   └── usage_service.py                         # Usage/cost event generation; authoritative billing persistence remains Rust-owned.
│       ├── events/                                      # AI runtime eventing.
│       │   ├── __init__.py                              # Event exports.
│       │   ├── models.py                                # Typed AI event envelopes.
│       │   ├── publisher.py                             # Event publisher interface for Rust/broker integration.
│       │   ├── consumer.py                              # Event consumer abstraction for durable message integration.
│       │   ├── handlers.py                              # Typed event handler registry.
│       │   └── idempotency.py                           # AI-side duplicate event protection.
│       ├── observability/                               # Python service telemetry.
│       │   ├── __init__.py                              # Observability exports.
│       │   ├── logging.py                               # Structured logging with correlation IDs and secret redaction.
│       │   ├── metrics.py                               # Metrics definitions for agent/model/tool/RAG/runtime operations.
│       │   ├── tracing.py                               # OpenTelemetry trace/context propagation.
│       │   └── health.py                                # Health component registration and dependency checks.
│       ├── security/                                    # Python-local security controls.
│       │   ├── __init__.py                              # Security exports.
│       │   ├── credentials.py                           # Secret/credential retrieval interface; no plaintext secrets in source.
│       │   ├── redaction.py                             # Deterministic sensitive-value redaction.
│       │   ├── identity.py                              # Validates service identity and authenticated Rust caller context.
│       │   └── transport.py                             # TLS/mTLS/client security configuration.
│       └── evaluation/                                  # AI evaluation and regression infrastructure.
│           ├── __init__.py                              # Evaluation exports.
│           ├── models.py                                # Dataset/example/evaluation result models.
│           ├── datasets.py                              # Dataset loading interfaces with tenant/security safeguards.
│           ├── metrics.py                               # Evaluation metric definitions.
│           ├── judge.py                                 # Evaluator/judge abstraction; external model usage must be explicitly configured.
│           ├── regression.py                            # Regression comparison and failure thresholds.
│           └── reports.py                               # Structured evaluation report generation.
├── tests/                                                # Test suite.
│   ├── unit/                                             # Fast deterministic unit tests for Python components.
│   ├── integration/                                     # Rust/Python/provider/broker/database integration tests.
│   ├── contract/                                        # Protobuf/API contract tests between Python and Rust.
│   ├── security/                                        # Authentication, tenant isolation, prompt injection, SSRF/tool abuse, and secret handling tests.
│   ├── resilience/                                      # Timeout, retry, cancellation, provider failure, duplicate, and restart tests.
│   └── e2e/                                              # End-to-end tests from canonical Rust execution request through Python AI result and event emission.
├── scripts/                                               # Developer and CI support scripts.
│   ├── check.sh                                          # Runs format/lint/type/test/package checks without hiding failures.
│   ├── generate_proto.sh                                 # Regenerates Python protobuf bindings from the authoritative protobuf contract.
│   └── verify_env.sh                                     # Validates required runtime/build environment variables and tools without printing secrets.
└── config/                                                # Example non-secret configuration templates.
    ├── development.toml                                   # Development configuration with safe local defaults only.
    ├── test.toml                                          # Test configuration designed for deterministic test infrastructure.
    └── production.example.toml                            # Production configuration template containing placeholders, never actual secrets.
```

IMPORTANT:

If the existing repository uses another package manager or generated-code strategy, preserve the project's established convention while keeping the same logical responsibilities.

Do not create duplicate implementations when an existing compatible implementation can be safely reused.

---

# 5. ARCHITECTURAL OWNERSHIP RULES

## Rust owns

- authenticated principal authority
- organization/tenant authority
- platform authorization
- canonical execution state
- task state
- task attempts
- leases/fencing
- quota state
- policy authority
- audit authority
- durable usage/billing records
- platform webhooks/outbox state
- durable platform persistence

## Python owns

- AI reasoning runtime
- agent orchestration
- model provider interactions
- prompt rendering
- AI tool selection/orchestration
- AI memory orchestration
- RAG orchestration
- guardrail execution
- AI-specific cost metadata
- AI-specific telemetry

## Python must NOT

- directly write Rust-owned execution tables
- directly alter tenant ownership
- bypass policy decisions
- trust a user-provided `tenant_id` header merely because it exists
- trust a user-provided organization header as authoritative identity
- access unrestricted internal services
- expose provider credentials to frontend clients
- silently create durable platform state that Rust does not know about

---

# 6. PYTHON ↔ RUST CONTRACT

The production communication path must be explicitly traceable:

```text
Rust canonical execution/task path
        |
        | authenticated gRPC or durable broker message
        v
Python Orchestrator
        |
        +--> validate protocol version
        +--> validate request identity/context
        +--> resolve agent/version
        +--> apply input guardrails
        +--> plan
        +--> model call
        +--> tool calls
        +--> memory/RAG
        +--> output validation
        +--> usage/cost metadata
        +--> publish result/event
        |
        v
Rust canonical state transition
```

Every request must carry, where supplied authoritatively by Rust:

- request_id
- correlation_id
- execution_id
- task_id
- task_attempt_id if available
- agent_id
- agent_version_id
- organization_id
- tenant_id
- protocol_version
- authenticated service identity

The Python service must reject missing mandatory execution identity fields.

Do not derive tenant ownership from arbitrary HTTP headers at the Python edge.

---

# 7. AGENT RUNTIME REQUIREMENTS

Implement a real lifecycle:

```text
load agent
→ validate version
→ resolve capabilities
→ load safe prompt configuration
→ build execution context
→ apply input guardrails
→ plan
→ execute plan
→ invoke tools when authorized
→ observe tool results
→ continue/stop according to bounded runtime policy
→ validate model output
→ apply output guardrails
→ emit result
```

Requirements:

- bounded recursion
- bounded delegation depth
- bounded tool count
- bounded wall-clock duration
- cancellation propagation
- retry budgets
- deterministic terminal states
- no infinite planner loops
- no infinite supervisor loops
- no unbounded context growth
- structured intermediate state

---

# 8. MODEL PROVIDER REQUIREMENTS

All providers implement one typed interface.

Provider adapter requirements:

- authentication through secure secret references
- connection/read/write timeouts
- cancellation
- retryable/non-retryable classification
- rate-limit handling
- provider 5xx handling
- malformed-response handling
- token usage extraction when provider reports it
- cost calculation only from known configured pricing data
- no fabricated usage
- no fabricated pricing
- no logging of provider credentials
- redaction of sensitive request content where configured

Failover must never bypass policy, tenant limits, model allowlists, or cost controls.

---

# 9. PROMPT SYSTEM REQUIREMENTS

Prompt templates must be:

- versioned
- identifiable
- hashable
- safely rendered
- bounded in size
- auditable

Template rendering must not execute arbitrary Python code.

Prompt variables must use an explicit allowlisted schema.

Prompt injection defenses must treat external documents, tool output, browser content, and retrieved content as untrusted data.

---

# 10. TOOL EXECUTION REQUIREMENTS

Every tool invocation must have:

- tool identifier
- tool version
- schema validation
- permission decision
- execution context
- timeout
- retry policy
- invocation ID
- bounded output size
- structured result
- failure classification

Tool output must be treated as untrusted data.

The model cannot directly bypass tool authorization.

HTTP tools must use the platform's SSRF-safe HTTP policy. Never introduce an unrestricted raw HTTP client for user-controlled URLs.

Code execution tools must be sandboxed. Never use unrestricted `subprocess`, shell execution, or host filesystem access as the production implementation.

---

# 11. MEMORY REQUIREMENTS

Memory must distinguish:

- task-local short-term context
- episodic execution memory
- durable long-term memory
- semantic retrieval

Every durable memory record must be associated with authoritative tenant/resource context.

Retention and deletion must be explicit.

Sensitive memory must support classification and redaction hooks.

Memory retrieval must not return another tenant's data.

---

# 12. RAG REQUIREMENTS

Implement a provider-agnostic RAG pipeline:

```text
ingest
→ parse
→ chunk
→ embed
→ index
→ retrieve
→ filter by security context
→ optionally hybrid search
→ rerank
→ construct context
→ cite sources
→ validate citations
```

The implementation may use adapter interfaces where external infrastructure is not yet present, but production adapters must not be disguised as complete when infrastructure is absent.

A vector database connection must be configurable.

A local in-memory vector implementation is acceptable only for tests/development and must be explicitly prevented from being selected for production.

---

# 13. GUARDRAILS REQUIREMENTS

Required guardrail stages:

### Before model execution

- input validation
- size limits
- prompt-injection analysis
- tool permission checks
- policy bridge

### During execution

- tool output treated as untrusted
- delegation limits
- budget limits
- timeouts
- cancellation

### After model execution

- schema validation
- output safety validation
- PII handling
- citation validation when RAG is used
- policy result handling

Guardrail failures must return typed denial/error results and must not become successful executions.

---

# 14. ERROR MODEL

Create typed Python errors such as:

- configuration_error
- authentication_error
- authorization_error
- tenant_context_error
- protocol_error
- invalid_request
- agent_not_found
- agent_version_not_found
- model_not_available
- model_rate_limited
- model_timeout
- model_provider_error
- tool_not_found
- tool_permission_denied
- tool_timeout
- tool_failed
- prompt_render_error
- prompt_injection_detected
- output_validation_failed
- pii_policy_denied
- quota_exceeded
- policy_denied
- memory_error
- retrieval_error
- citation_validation_error
- cancellation_requested
- execution_timeout
- duplicate_request
- dependency_unavailable
- internal_error

Every error must include stable machine-readable code, safe message, retryability, and optional correlation/request identifiers.

Never expose secrets or raw provider credentials in errors.

---

# 15. RETRY / TIMEOUT / CANCELLATION

Retry only errors classified as retryable.

Implement:

- exponential backoff
- jitter
- maximum attempts
- total retry budget
- provider-specific retry-after handling where available
- cancellation-aware waiting
- idempotent tool retry restrictions

Never blindly retry non-idempotent actions.

Never continue work after cancellation has been authoritatively received.

---

# 16. IDEMPOTENCY

Python may receive duplicate deliveries.

Therefore:

- every execution/task invocation needs a stable invocation identity
- duplicate model execution must be handled according to the authoritative Rust execution state
- duplicate tool execution must be classified by tool safety/idempotency
- event handling must be deduplicated
- retries must not manufacture duplicate durable outcomes

Python must not invent a competing platform-level idempotency system that conflicts with Rust's authoritative execution state.

---

# 17. OBSERVABILITY

Emit structured telemetry for:

- request received
- agent lookup
- planning duration
- model request duration
- model tokens when reported
- model cost when known
- tool invocation duration
- tool failures
- retries
- guardrail decisions
- policy denials
- quota denials
- RAG retrieval duration
- embedding duration
- reranking duration
- execution duration
- cancellation
- final result

Every log should safely carry correlation context.

Never log:

- API keys
- OAuth client secrets
- model provider secret keys
- access tokens
- raw authorization headers
- sensitive credential material

Avoid high-cardinality metric labels containing arbitrary user text, prompts, IDs, URLs, or document content.

---

# 18. CONFIGURATION / SECRETS

Secrets must be injected through environment/configuration or an existing approved secret-manager abstraction.

Never commit real credentials.

Never put secrets in frontend code.

Never echo secrets in health endpoints.

Never print complete environment variables in diagnostics.

Provider configuration must support enable/disable and model allowlists.

Production startup should fail closed if a configured mandatory provider has missing credentials.

---

# 19. TEST REQUIREMENTS

The implementation is incomplete until tests cover at minimum:

## Unit

- settings validation
- context propagation
- agent registry
- planner
- router
- supervisor
- delegation limits
- model routing
- retry classification
- cost accounting
- prompt rendering
- tool schema validation
- tool permissions
- guardrails
- memory manager
- RAG chunking
- citation validation
- error mapping

## Security

- forged tenant context rejected
- missing authoritative tenant context rejected
- tool permission bypass rejected
- prompt injection cases
- untrusted tool output cases
- secret redaction
- SSRF-sensitive URL cases
- oversized model/tool payloads

## Resilience

- provider timeout
- provider 429
- provider 5xx
- malformed model response
- tool timeout
- tool failure
- cancellation
- duplicate message
- duplicate execution delivery
- retry budget exhausted
- dependency unavailable

## Contract

- protobuf compatibility
- protocol version mismatch
- required execution identity fields
- Rust result/error mapping

## Integration

Use real test infrastructure where repository/environment supports it.

Do not count a mocked provider test as a live provider test.

Do not claim E2E success unless the full Rust→Python→provider/tool→Rust lifecycle actually executes.

---

# 20. PRODUCTION CONFIGURATION RULES

Production mode must reject:

- debug mode when unsafe
- fake providers
- Echo-like handlers
- in-memory-only production agent runtime
- test-only secret resolver
- unrestricted HTTP
- unrestricted shell/code execution
- wildcard tool permissions
- arbitrary tenant headers as identity authority

Development/test modes may use explicit fakes, but these must be selected through explicit test configuration and must be impossible to activate accidentally in production.

---

# 21. DEPENDENCY POLICY

Use a small, justified dependency set.

Pin/lock dependencies appropriately.

Use async I/O for network/provider operations.

Do not add a framework merely because it is convenient.

Before adding a dependency, verify whether the Rust project already defines a compatible protocol/library choice.

Document every production dependency and its operational purpose.

---

# 22. PYTHON/RUST VERSION COMPATIBILITY

Inspect the existing repository toolchain and CI intent before selecting the Python runtime.

The implementation must define an explicit supported Python version range.

Generated protobuf code must be compatible with that runtime.

Do not silently choose a Python feature unsupported by the declared runtime.

---

# 23. API / SERVICE BOUNDARY

Prefer Rust as the canonical public API and use Python as an internal AI service unless repository inspection demonstrates a different established contract.

If Python exposes an internal service endpoint, it must:

- require service authentication
- validate protocol version
- validate execution identity
- have request size limits
- have timeouts
- reject arbitrary tenant selection
- return stable typed errors
- expose only the minimum required operations

---

# 24. LIFECYCLE / SHUTDOWN

Implement graceful shutdown:

1. stop accepting new AI work
2. signal in-flight tasks
3. honor cancellation deadlines
4. close provider connections
5. close broker/gRPC connections
6. flush safe telemetry
7. exit with correct status

Do not terminate while silently abandoning authoritative execution state.

---

# 25. FILE IMPLEMENTATION REQUIREMENT

For every file listed in this prompt, the coding agent must determine:

- package/module responsibility
- exact classes
- exact functions
- type models
- dependencies
- error behavior
- test coverage
- integration boundary

The `#` comment next to each file in the tree is mandatory guidance and must be treated as part of the specification.

When implementation is requested, do not collapse several responsibilities into one giant file merely to reduce the number of files.

Do not create hundreds of unnecessary micro-files either; preserve coherent module boundaries.

---

# 26. EXISTING RUST INTEGRATION TASKS

After the Python implementation is added, inspect and integrate with existing Rust contracts for:

- execution request
- task request
- execution result
- error events
- agent metadata
- tool metadata
- policy decisions
- usage/cost metadata
- audit/event emission

Do not alter protobuf field numbers incompatibly.

Do not silently change existing message semantics.

If a required message is genuinely missing, report the exact Rust protobuf file, message/service, and required contract change before modifying it.

---

# 27. DATABASE OWNERSHIP RULE

The Python orchestrator may read information through authorized service interfaces.

Do not introduce direct SQL against Rust-owned execution tables merely because it is easier.

If a Python-owned persistence requirement is necessary for AI-specific state, first verify whether the product architecture already defines that ownership.

Avoid creating duplicate copies of canonical entities such as:

- tenant
- organization
- user
- execution
- task
- agent
- workflow
- policy
- entitlement

A Python-side cache/read model must be explicitly identified as a cache/read model, not a second source of truth.

---

# 28. SECURITY ACCEPTANCE CRITERIA

The implementation is not acceptable until:

- no user-controlled tenant header is trusted as authoritative identity
- all AI requests carry authoritative execution context
- no production provider secret is exposed to clients
- tool permissions are enforced
- tool output is treated as untrusted
- prompt injection controls exist
- SSRF-sensitive HTTP execution is guarded
- code execution is sandboxed or disabled
- durable execution authority remains Rust-owned
- cancellation propagates
- retry behavior is bounded
- duplicate delivery does not create uncontrolled duplicate outcomes
- logs do not leak credentials/tokens

---

# 29. PRODUCTION ACCEPTANCE CRITERIA

The implementation is not complete until the following can be demonstrated from source and tests:

```text
Rust canonical execution
    |
    v
Authenticated Python invocation
    |
    v
Agent resolution
    |
    v
Prompt + context construction
    |
    v
Guardrails
    |
    v
Planner / router
    |
    v
LLM provider
    |
    v
Tool calls when required
    |
    v
Memory/RAG when required
    |
    v
Output validation
    |
    v
Usage/cost metadata
    |
    v
Typed result/error
    |
    v
Rust authoritative state transition
```

Every transition must be identifiable in source code and represented by tests.

---

# 30. REQUIRED COMMANDS

Where the environment supports them, run and report actual results for:

```bash
python --version
python -m pip --version
python -m compileall python-orchestrator/src
python -m pytest
ruff check python-orchestrator
ruff format --check python-orchestrator
mypy python-orchestrator/src
```

Also run project-specific package/build commands discovered from `pyproject.toml`.

Do not claim tests pass unless they actually pass.

Do not claim type checking passes unless it actually passes.

If a command cannot run, state the exact reason.

---

# 31. IMPLEMENTATION ORDER

Do not randomly generate files.

Use this dependency order:

### Phase A — Repository inspection

1. inspect complete repository tree
2. inspect existing Python absence/presence
3. inspect Rust protobuf contracts
4. inspect Rust execution contracts
5. inspect security/tenancy contracts
6. inspect event/message contracts
7. inspect existing tests

### Phase B — Python foundation

8. pyproject
9. settings
10. error model
11. context
12. observability
13. secure transport

### Phase C — Rust bridge

14. protobuf generation
15. gRPC client/channel
16. metadata propagation
17. contract models
18. error mapping

### Phase D — Model layer

19. provider interfaces
20. provider adapters
21. model router
22. retry/failover
23. streaming
24. usage/cost

### Phase E — Agent runtime

25. BaseAgent
26. registry
27. factory
28. planner
29. executor
30. router
31. supervisor
32. delegation/handoff
33. runtime

### Phase F — Prompt/tool/runtime safety

34. prompt registry
35. safe renderer
36. tool registry
37. tool validation
38. permissions bridge
39. tool executor
40. HTTP/tool safeguards
41. sandbox boundary

### Phase G — Memory/RAG

42. memory models
43. memory manager
44. retention
45. RAG ingestion
46. parsing/chunking
47. embedding/index/retrieval
48. reranking
49. citations
50. tenant/security filtering

### Phase H — Guardrails

51. input
52. prompt injection
53. output validation
54. PII
55. policy bridge
56. citation checks
57. manager

### Phase I — Execution service and eventing

58. canonical execution service
59. typed AI events
60. idempotency handling
61. result/error emission
62. Rust state transition integration

### Phase J — Tests

63. unit suite
64. security suite
65. resilience suite
66. contract suite
67. integration suite
68. E2E suite

### Phase K — Production validation

69. lint
70. type-check
71. tests
72. package/build
73. health/readiness
74. production configuration validation
75. final source-level execution trace

---

# 32. CODING AGENT OUTPUT RULE

When asked to implement a phase, return COMPLETE FILE CONTENT for every file created or modified.

For each file use this format:

```text
FILE: python-orchestrator/src/mas_orchestrator/example.py
PURPOSE: <one concise sentence>

<full file content from first line to last line>
```

Do not use omitted sections.

Do not use ellipses.

Do not say “rest unchanged”.

Do not provide a pseudo-code substitute where real code is requested.

Do not silently modify unrelated files.

When multiple files are required, provide every file completely.

---

# 33. EXISTING LOGIC PRESERVATION RULE

For modified Rust files:

- read the whole original file
- preserve unrelated behavior
- preserve public APIs unless a contract change is explicitly required
- preserve existing tests
- do not remove tests to make a build pass
- do not reduce functionality to fit a shorter answer
- do not replace implementation with placeholders

If an existing implementation conflicts with the required production behavior, explain the conflict and update it coherently rather than silently deleting the old logic.

---

# 34. NO UNVERIFIED ENTERPRISE CLAIMS

Do not state:

- “enterprise-ready”
- “production-ready”
- “SOC 2 compliant”
- “ISO 27001 compliant”
- “GDPR compliant”
- “HIPAA compliant”

unless the evidence and scope genuinely support the statement.

The implementation should instead report measurable evidence:

- tests executed
- integrations exercised
- security controls present
- health checks passing
- failure scenarios covered
- deployment artifacts present

---

# 35. FINAL ACCEPTANCE CHECKLIST

Before declaring the Python AI Orchestrator implementation complete, verify:

[ ] Python source tree exists.
[ ] All required files contain real functionality.
[ ] No production fake provider is active.
[ ] No Echo/test runtime is selected in production.
[ ] Real provider adapters are implemented where configured.
[ ] Rust↔Python protocol is authenticated.
[ ] Correlation/request/execution/task IDs propagate.
[ ] Tenant context is authoritative.
[ ] Agent registry works.
[ ] Agent runtime works.
[ ] Planner works.
[ ] Router works.
[ ] Supervisor/delegation limits work.
[ ] Prompt registry/versioning works.
[ ] Tool schema validation works.
[ ] Tool permissions are enforced.
[ ] Tool timeout/retry behavior works.
[ ] HTTP tools use SSRF-safe boundaries.
[ ] Code execution is sandboxed or disabled.
[ ] Memory interfaces work.
[ ] RAG interfaces work.
[ ] Tenant filters are enforced for retrieval.
[ ] Guardrails work.
[ ] Prompt injection checks exist.
[ ] Output validation works.
[ ] PII handling hooks exist.
[ ] Citation validation exists.
[ ] Usage/cost metadata is real, not synthetic.
[ ] Typed errors are stable.
[ ] Cancellation works.
[ ] Retry budgets are bounded.
[ ] Duplicate delivery is handled.
[ ] Structured logs are redacted.
[ ] Metrics/tracing exist.
[ ] Liveness/readiness exist.
[ ] Unit tests exist.
[ ] Security tests exist.
[ ] Resilience tests exist.
[ ] Contract tests exist.
[ ] Integration tests exist where infrastructure is available.
[ ] E2E test exists for Rust→Python→Rust execution.
[ ] Python lint passes when executed.
[ ] Python type-check passes when executed.
[ ] Python tests pass when executed.
[ ] Build/package passes when executed.
[ ] No unrelated Rust functionality was removed.
[ ] No required file was shortened.
[ ] No `...`, “rest of code”, or placeholder implementation exists in generated files.

---

# 36. STOP CONDITIONS

STOP implementation and report a blocking dependency instead of inventing behavior when:

- the Rust protobuf contract does not provide required information
- authoritative tenant/security identity cannot be established
- a required production adapter is undefined
- an external infrastructure dependency is absent and a safe production adapter cannot be created from repository evidence
- an existing state transition would become ambiguous
- a database ownership boundary is unclear
- changing a contract would be breaking and no migration/versioning strategy exists

Never work around a security or consistency blocker with an insecure shortcut.

---

# 37. FINAL INSTRUCTION

Implement the Python AI Orchestrator as a real production-oriented layer integrated with the existing Rust architecture.

Do not create a disconnected demo.
Do not create a toy agent framework.
Do not create a collection of interfaces with no runtime.
Do not create test-only functionality and label it production.
Do not duplicate Rust-owned platform state.
Do not trust caller-controlled tenancy.
Do not omit files for brevity.
Do not shorten any generated file.

The final implementation must be source-complete, dependency-aware, testable, secure by default, and explicitly integrated with the existing Rust Core.

MOST IMPORTANT:

1. SOURCE OF TRUTH = ACTUAL UPLOADED REPOSITORY.
2. PRESERVE EXISTING LOGIC.
3. EVERY CREATED/MODIFIED FILE MUST BE COMPLETE.
4. NEVER USE `...` TO OMIT CODE.
5. NEVER USE “REST OF THE CODE HERE”.
6. NEVER USE FAKE PRODUCTION IMPLEMENTATIONS.
7. NEVER INVENT MISSING CONTRACTS.
8. NEVER CLAIM TESTS PASS WITHOUT RUNNING THEM.
9. NEVER CLAIM PRODUCTION READY WITHOUT EVIDENCE.
10. STOP AT THE REQUESTED PHASE AND DO NOT START THE NEXT PHASE WITHOUT EXPLICIT APPROVAL.
