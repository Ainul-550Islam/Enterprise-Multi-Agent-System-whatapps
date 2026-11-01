"""Enterprise MAS — Python AI Orchestrator.

The orchestrator owns *AI execution concerns only*:

* agent behaviour, planning, routing, and supervision
* model provider interaction, routing, failover, token/cost accounting
* prompt registry and safe rendering
* tool selection, schema validation, authorization bridge, and execution
* memory orchestration (short-term, episodic, long-term, semantic)
* RAG ingestion, retrieval, reranking, and citation
* AI-specific guardrails (input safety, injection, output validation, PII)
* AI-specific telemetry and evaluation hooks

It explicitly does **not** own platform state. Rust Core remains the
authoritative owner of:

* authenticated principal and tenancy authority
* canonical execution / task / attempt state
* leases, quotas, policy decisions, audit, and durable usage records

Consequences enforced throughout this package:

* Python never writes Rust-owned tables directly.
* A ``tenant_id`` supplied by a caller is never trusted as authoritative
  identity; it must arrive inside an authenticated Rust-issued context.
* Provider/tool failures produce typed errors and deterministic state
  transitions — never a silently successful execution.

Importing this package has no side effects. Nothing starts I/O, reads
configuration, or opens connections at import time; ``main.py`` performs
wiring explicitly.
"""

from __future__ import annotations

from mas_orchestrator.version import (
    AI_PROTOCOL_MAJOR,
    AI_PROTOCOL_VERSION,
    MIN_RUST_API_VERSION,
    SERVICE_NAME,
    __version__,
    build_info,
)

__all__ = [
    "AI_PROTOCOL_MAJOR",
    "AI_PROTOCOL_VERSION",
    "MIN_RUST_API_VERSION",
    "SERVICE_NAME",
    "__version__",
    "build_info",
]
