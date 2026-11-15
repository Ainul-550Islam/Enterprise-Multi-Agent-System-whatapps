# NEXT PHASE — RUST ↔ PYTHON PRODUCTION AGENTBRIDGE INTEGRATION

## ROLE

Act as a Principal Rust Engineer + Python Systems Engineer + Distributed Systems Engineer.

The Python Orchestrator implementation is now reported complete and verified.

Current reported Python status:

- 155 source modules
- 45 test files
- 38,296 source LOC
- 16,396 test LOC
- 1,697 tests passed
- compileall passed
- Ruff passed
- Ruff format check passed
- mypy passed
- integration/resilience/E2E/security suites exist

The existing Python test coverage includes:

- Rust→Python gRPC identity propagation
- retry/backoff
- failover
- shutdown
- full execution
- workflow execution
- tenant-safe tool routing

Do NOT assume these claims are true merely because they are written here. Use the actual source tree and tests as evidence while implementing.

---

# PRIMARY OBJECTIVE

Implement the REAL production Rust ↔ Python AgentBridge.

The architecture decision is:

**Rust protobuf + gRPC AgentBridgeApi**

Do NOT switch to:

- JSON/HTTP
- REST bridge
- local subprocess RPC
- fake adapter
- EchoHandler
- InMemoryAgentBridge
- test-only bridge

The final production path MUST be:

Rust Execution Runtime
→ AgentBridge
→ gRPC
→ Python Orchestrator
→ Agent Runtime
→ Model Provider
→ Tool/Workflow execution where applicable
→ Python result
→ gRPC response
→ Rust execution state

---

# NON-NEGOTIABLE RULE — NO SHORTENING

When creating or modifying any file:

1. Write the COMPLETE file.
2. The file must be shown from the first line to the final line.
3. NEVER write `...`.
4. NEVER write `// rest of code`.
5. NEVER write `# existing code`.
6. NEVER write `/* omitted */`.
7. NEVER replace existing logic with placeholders.
8. NEVER silently delete existing functionality.
9. NEVER provide partial files.
10. Preserve all unrelated existing logic.

The coding output must contain FULL CODE for every modified file.

---

# NON-NEGOTIABLE RULE — FILE PURPOSE COMMENTS

For EVERY new file, and for EVERY major section where useful, put a clear comment describing its responsibility.

Examples:

Rust:

```rust
// PURPOSE:
// Production gRPC implementation of the AgentBridge contract.
// This adapter is the boundary between Rust execution runtime and Python AI runtime.
```

Python:

```python
# PURPOSE:
# gRPC server implementation for AgentBridgeApi.
# This module authenticates and validates Rust-originated execution requests
# before forwarding them to the Agent Runtime.
```

Every file must make its intended responsibility obvious to a coding agent and future maintainer.

---

# PHASE 1 — INSPECT BEFORE MODIFYING

First inspect the actual repository.

Find:

- `crates/api/proto/mas/api/v1/mas.proto`
- protobuf build configuration
- Rust generated protobuf modules
- `crates/execution`
- `crates/orchestration`
- `crates/application`
- `crates/worker-service`
- existing `AgentBridge`
- `InMemoryAgentBridge`
- `EchoHandler`
- gRPC server/client infrastructure
- Python gRPC-related files
- Python Agent Runtime entrypoints
- Python model routing
- Python execution context
- Python identity propagation
- existing contract tests

Do NOT create duplicate abstractions if an existing canonical abstraction already exists.

Identify:

- current AgentBridge trait
- request/response structures
- execution context
- tenant identity representation
- correlation/request IDs
- deadline representation
- cancellation semantics
- error taxonomy
- metadata conventions

---

# PHASE 2 — PROTOBUF CONTRACT

Modify the canonical protobuf contract:

`crates/api/proto/mas/api/v1/mas.proto`

Add:

`AgentBridgeApi`

The contract must support the real execution requirements.

At minimum the contract must carry:

- request ID
- execution ID
- task ID where applicable
- tenant ID
- organization ID where authoritative
- project ID where applicable
- authenticated principal/service identity
- correlation ID
- deadline
- cancellation semantics
- agent reference/version
- prompt/input
- runtime state where required
- tool execution context where required
- idempotency key where required

Response must support:

- successful agent result
- structured failure
- provider/model metadata where safe
- usage
- cost/usage accounting information where applicable
- finish state
- correlation/request identity
- retry classification
- machine-readable error code

DO NOT expose secrets through protobuf.

DO NOT transmit raw API keys or provider credentials.

---

# PHASE 3 — RUST GENERATED CODE

Update the Rust protobuf generation path.

Verify:

- generated service trait exists
- client exists
- server implementation compiles
- package/version names remain consistent
- existing services are not broken

Do not manually duplicate generated code unless the repository architecture explicitly requires committed generated output.

---

# PHASE 4 — RUST AGENTBRIDGE ADAPTER

Implement:

`GrpcAgentBridge`

as the production implementation of the existing Rust `AgentBridge` abstraction.

It MUST:

- create a gRPC client
- attach authenticated internal metadata
- propagate tenant context
- propagate organization context
- propagate principal/service identity
- propagate request ID
- propagate correlation ID
- propagate deadline
- map cancellation
- map transport failures
- map Python structured failures
- classify retryable vs terminal failure
- preserve idempotency semantics
- enforce request timeout
- redact sensitive information from logs

Do NOT trust caller-supplied tenant headers at this boundary.

The Rust execution context must be the authoritative source.

---

# PHASE 5 — REMOVE PRODUCTION INMEMORY BRIDGE

`InMemoryAgentBridge` may remain for tests where genuinely useful.

But:

**It MUST NOT be selected by the production composition root.**

Find the worker/application production constructor and replace:

`InMemoryAgentBridge`

with:

`GrpcAgentBridge`

Production startup must fail clearly if the Python bridge configuration is missing.

There must NOT be a silent in-memory fallback.

---

# PHASE 6 — REMOVE PRODUCTION ECHO EXECUTION

Find all production paths that can bind:

`EchoHandler`

`EchoHandler` is permitted only for explicit test/dev use.

It MUST NOT be reachable from the production execution configuration.

There must be no:

- implicit Echo fallback
- default Echo fallback
- missing-config → Echo fallback
- production environment variable that accidentally enables Echo

Production runtime must use the real execution handler.

---

# PHASE 7 — PYTHON gRPC SERVER

Implement the Python side of `AgentBridgeApi`.

The server must:

1. accept gRPC calls
2. validate metadata
3. authenticate the Rust service identity
4. reconcile identity/tenant scope
5. construct execution context
6. validate request
7. dispatch to Agent Runtime
8. enforce deadline
9. support cancellation
10. map Agent Runtime errors
11. return structured result
12. emit safe telemetry

Existing functions such as:

- `extract_call_metadata`
- `reconcile_tenant_scope`
- `build_execution_context`
- `build_outbound_metadata`

must be reused where they are the canonical implementation.

Do NOT create a second identity model.

---

# PHASE 8 — PYTHON AGENT RUNTIME BINDING

The gRPC service must invoke the real Agent Runtime.

Required path:

`AgentBridgeApi`
→ `Agent Runtime`
→ `Agent Executor`
→ `Model Router`
→ real provider adapter

Do NOT call:

- fake model
- Echo handler
- hard-coded response
- test fixture
- mock provider

inside the production runtime.

---

# PHASE 9 — MODEL PROVIDER BINDING

The provider layer must use the previously selected implementation level:

**OpenAI + Anthropic + OpenAI-compatible real adapters**

The production runtime must support the existing:

- provider abstraction
- model registry
- router
- failover
- retry classification
- timeout budget
- usage extraction
- redaction
- cost accounting

Do not regress the existing provider behavior.

The failover fix documented in:

`models/failover.py`

must remain preserved.

---

# PHASE 10 — IDENTITY AND TENANT SECURITY

This phase is security-critical.

The Rust side must derive identity from the authenticated internal/service context.

The Python side must reject:

- missing internal identity
- malformed identity
- forged tenant identity
- mismatched organization/tenant scope
- invalid principal
- unauthorized service identity

The Python server MUST NOT trust arbitrary client metadata simply because the field is named:

`tenant_id`

or:

`organization_id`

Validate the relationship between:

- principal
- organization
- tenant
- project
- execution

where the current model supports those relationships.

---

# PHASE 11 — DEADLINE / CANCELLATION

Implement end-to-end deadline propagation.

Rust:

`ExecutionContext.deadline`
→ gRPC deadline metadata / request deadline

Python:

gRPC deadline
→ Agent Runtime deadline
→ model timeout
→ tool timeout

Cancellation must propagate:

Rust cancellation
→ gRPC cancellation
→ Python task cancellation
→ model/tool cancellation
→ structured cancellation response

A cancellation must never be reported as successful execution.

---

# PHASE 12 — ERROR CONTRACT

Define a single structured error mapping.

At minimum distinguish:

- authentication failure
- authorization failure
- tenant scope mismatch
- validation failure
- model timeout
- model rate limit
- model provider unavailable
- model authentication error
- tool timeout
- tool denial
- policy denial
- quota exceeded
- cancellation
- dependency unavailable
- internal execution failure

Every error needs:

- stable machine-readable code
- retryable/terminal classification
- safe human-readable message
- correlation ID
- request ID

Never expose:

- provider API key
- secrets
- internal tokens
- sensitive prompts
- private tenant information

in error responses or logs.

---

# PHASE 13 — OBSERVABILITY

Add correlated telemetry across:

Rust API
→ execution
→ AgentBridge
→ gRPC
→ Python Agent Runtime
→ model provider
→ tool
→ response

Preserve:

- request ID
- correlation ID
- execution ID
- task ID
- tenant-safe telemetry identity

Measure at minimum:

- gRPC request latency
- Python runtime latency
- model latency
- tool latency
- timeout count
- cancellation count
- retry count
- failover count
- provider failures
- authorization failures
- tenant mismatch failures

Never include secrets in telemetry.

---

# PHASE 14 — TESTS

Add/update tests for the real contract.

Required categories:

## Contract tests

Verify Rust and Python agree on:

- protobuf package
- RPC names
- fields
- enum/error values
- request/response serialization

## Identity tests

Test:

- valid identity
- invalid identity
- missing tenant
- mismatched tenant
- mismatched organization
- unauthorized service identity

## Timeout tests

Test:

- Rust deadline
- Python deadline
- provider timeout
- tool timeout

## Cancellation tests

Test:

- Rust cancellation
- Python cancellation
- cancellation during model call
- cancellation during tool call

## Error mapping tests

Every important Rust error must map to the correct Python/gRPC error and back.

## Failover tests

Verify:

- retryable provider failure
- secondary provider success
- terminal provider failure
- cancellation stops failover

Preserve the existing failover semantics already covered by the current tests.

## Tenant isolation tests

Explicitly prove:

Tenant A cannot invoke:

- Tenant B's agent
- Tenant B's tool
- Tenant B's workflow
- Tenant B's data

## Production composition test

Verify production configuration uses:

`GrpcAgentBridge`

and NOT:

`InMemoryAgentBridge`

or:

`EchoHandler`

---

# PHASE 15 — BUILD / TEST VERIFICATION

Actually run available commands.

Rust:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --locked
cargo test --workspace --locked --features nats
cargo doc --workspace --all-features --no-deps
```

Python:

```bash
python3 -m compileall -q src tests
python3 -m ruff check src tests
python3 -m ruff format --check src tests
PYTHONPATH=src python3 -m mypy src/mas_orchestrator
PYTHONPATH=src python3 -m pytest tests -q
```

Run targeted Rust/Python contract integration tests as well.

If any command cannot run:

STATE EXACTLY WHY.

Never claim:

- build passed
- tests passed
- production verified

unless actually executed.

---

# PHASE 16 — FILE OUTPUT REQUIREMENT

For every modified file provide:

```text
FILE: exact/path/to/file
PURPOSE: one-line responsibility
```

Then provide the COMPLETE FILE CONTENT.

Example:

```text
FILE: crates/api/proto/mas/api/v1/mas.proto
PURPOSE: Canonical Rust↔Python AgentBridgeApi protobuf contract
```

Then the entire file.

For Python:

```text
FILE: python-orchestrator/src/mas_orchestrator/grpc/agent_bridge_server.py
PURPOSE: Production gRPC server exposing AgentBridgeApi to Rust
```

Then the entire file.

No omissions.

---

# PHASE 17 — DO NOT BREAK EXISTING WORK

Do not remove or weaken:

- current model provider adapters
- retry classification
- failover behavior
- identity reconciliation
- execution context
- tool tenant isolation
- CostLedger
- workflow execution
- security tests
- resilience tests
- contract tests

The current Python status reports:

1,697 tests passed.

After modification, rerun the entire suite.

The final state must preserve existing functionality.

---

# PHASE 18 — COMPLETION GATE

This phase is complete ONLY when all are true:

- `AgentBridgeApi` exists in canonical protobuf
- Rust gRPC client generated
- Rust `GrpcAgentBridge` implemented
- Python gRPC server implemented
- Rust production worker uses `GrpcAgentBridge`
- `InMemoryAgentBridge` is test-only/dev-only
- `EchoHandler` is test-only/dev-only
- Rust→Python identity propagation works
- tenant scope is authoritative
- request/correlation IDs propagate
- deadlines propagate
- cancellation propagates
- errors map correctly
- retry classification is preserved
- failover is preserved
- production telemetry is correlated
- contract tests pass
- security tests pass
- integration tests pass
- existing Python suite remains green
- Rust workspace checks pass where environment supports them

---

# FINAL STOP CONDITION

STOP after this phase.

Do NOT start unrelated work.

Do NOT implement frontend.

Do NOT implement billing.

Do NOT redesign the database.

Do NOT create a second AgentBridge abstraction.

Do NOT introduce JSON/HTTP.

Do NOT use an in-memory production fallback.

Do NOT shorten any file.

Wait for explicit approval before starting the next phase.