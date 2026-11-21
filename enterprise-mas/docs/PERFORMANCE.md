# Performance ceilings

Numbers on this page are the constants compiled into the tree, not
benchmarks. Configurable fields can be set *down* from these caps; they
cannot be set above the hard limits without a code change.

Sources:

- `enterprise-mas/rust-core/crates/common/src/constants.rs`
- `enterprise-mas/python-orchestrator/src/mas_orchestrator/config/settings.py`
- `sandbox-runner/runner.py`

## Request and payload size

| Ceiling | Value | Constant |
|---|---|---|
| Max JSON body accepted by any API endpoint | 1 MiB | `MAX_PAYLOAD_BYTES` |
| Max event payload | 256 KiB | `MAX_EVENT_PAYLOAD_BYTES` |
| Max tool argument blob | 256 KiB | `MAX_TOOL_ARGUMENT_BYTES` |
| Max tool output before offload | 4 MiB | `MAX_TOOL_OUTPUT_BYTES` |
| Max audit metadata | 16 KiB | `MAX_AUDIT_METADATA_BYTES` |
| Max JWT | 16 KiB | `MAX_JWT_SIZE_BYTES` |
| Orchestrator inbound body | 256 KiB default (cap 8 MiB) | `ServiceSettings.max_request_bytes` / `DEFAULT_MAX_PROMPT_BYTES` |
| Tool result accepted back into context | 128 KiB default | `DEFAULT_MAX_TOOL_RESULT_BYTES` |

## Timeouts and retries

| Ceiling | Value | Constant |
|---|---|---|
| Default HTTP request timeout | 30 000 ms | `DEFAULT_REQUEST_TIMEOUT_MS` |
| Default tool timeout | 60 000 ms | `DEFAULT_TOOL_TIMEOUT_MS` |
| Default gRPC timeout | 30 000 ms | `DEFAULT_GRPC_TIMEOUT_MS` |
| Heartbeat interval | 5 000 ms | `DEFAULT_HEARTBEAT_INTERVAL_MS` |
| Default max retries | 3 | `DEFAULT_MAX_RETRIES` |
| Hard max retries | 10 | `MAX_ALLOWED_RETRIES` |
| Default retry backoff | 500 ms | `DEFAULT_RETRY_BACKOFF_MS` |
| Max retry backoff | 60 000 ms | `MAX_RETRY_BACKOFF_MS` |
| Webhook signature skew | 300 s | `WEBHOOK_SIGNATURE_TOLERANCE_SECONDS` |
| Rust Core per-call deadline | 10 s default (cap 300 s) | `RustCoreSettings.request_timeout_seconds` |
| Rust Core total retry budget | 30 s default (cap 900 s) | `RustCoreSettings.total_timeout_seconds` |
| Orchestrator inbound execution | 120 s default (cap 3600 s) | `ServiceSettings.request_timeout_seconds` |
| Provider request timeout | 60 s default (cap 900 s) | `ProviderSettings.request_timeout_seconds` |
| Provider connect timeout | 5 s default (cap 60 s) | `ProviderSettings.connect_timeout_seconds` |
| Tool invocation timeout | 30 s default (cap 900 s) | `ToolSettings.invocation_timeout_seconds` |

## Runtime graph and concurrency

| Ceiling | Value | Constant |
|---|---|---|
| Nodes per workflow | 500 | `MAX_NODES_PER_WORKFLOW` |
| Steps per execution | 1 000 | `MAX_STEPS_PER_EXECUTION` |
| Tool calls per execution | 500 | `MAX_TOOL_CALLS_PER_EXECUTION` |
| Delegation depth | 5 | `MAX_DELEGATION_DEPTH` |
| Tokens per execution | 1 000 000 | `MAX_TOKENS_PER_EXECUTION` |
| Parallel tasks per execution | 64 | `MAX_PARALLEL_TASKS_PER_EXECUTION` |
| Execution wall clock | 86 400 000 ms (24 h) | `MAX_EXECUTION_DURATION_MS` |
| Concurrent executions per tenant | 100 | `HARD_MAX_CONCURRENT_EXECUTIONS_PER_TENANT` |
| Requests per second per tenant | 1 000 | `HARD_MAX_REQUESTS_PER_SECOND_PER_TENANT` |
| Child executions | 256 | `MAX_CHILD_EXECUTIONS` |
| Tool calls per execution (orchestrator) | 25 default (cap 1 000) | `ToolSettings.max_calls_per_execution` |
| Parallel tool calls | 4 default (cap 64) | `ToolSettings.max_parallel_calls` |

## Sandbox runner

| Ceiling | Value | Constant |
|---|---|---|
| Address space | 64 MiB | `DEFAULT_MEMORY_BYTES` |
| CPU | 2 s | `DEFAULT_CPU_SECONDS` |
| Captured output | 64 KiB | `DEFAULT_OUTPUT_BYTES` |
| Processes (RLIMIT_NPROC) | 32 | `DEFAULT_NPROC` |
| `/execute` body | 256 KiB | `MAX_EXECUTE_BYTES` |

Code-tool *requests* to the sandbox (`SandboxLimits`) default to 10 s /
256 MiB / 10 CPU s / 256 KiB out, with `network_access=False`. The runner
refuses `network_access=True`.

## Metrics

Pipeline families are the sixteen names in
`mas_application::pipeline_metrics::ALL_METRICS`. Histograms are in
**milliseconds** (`mas_execution_duration_ms`, `mas_task_claim_latency_ms`)
and Prometheus renders `{name}_bucket`. There is no
`mas_execution_duration_seconds` family. Quota remaining is read from
`GET /v1/quotas` / `GET /v1/usage`, not from a per-tenant Prometheus label
(tenant ids are forbidden as metric labels).
