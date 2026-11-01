"""Memory layer.

Implements spec §11. Four distinct kinds of memory — task-local, episodic,
durable and semantic — each with its own lifetime, and every durable record
carrying authoritative tenant context.

Each kind has its own module, because the failure modes are different:
short-term memory must not outlive its execution, episodic memory must not be
readable across executions, semantic memory must be a fact rather than a
description of one run, and durable memory must be screened for injection
before it is persisted — it is replayed into every future execution.

Compaction (:mod:`.compaction`) reduces a working set to fit a prompt. It only
removes or restates, never invents: a summarised memory that adds a fact nobody
said is worse than a dropped one.

Two rules the code enforces rather than documents:

* **Tenancy is stamped from the execution context.** :meth:`MemoryManager.remember`
  takes no tenant argument, because a caller that can name a tenant can write
  into another tenant's memory.
* **The production store is not here.** Python does not write Rust-owned
  tables, so the only backend shipping in this package is in-process and it
  refuses to be constructed in production (spec §5).

Retrieved memory is fenced as untrusted before it reaches a prompt: content
written during an earlier execution may have originated in a tool or a
document, which makes it untrusted regardless of where it is stored now.
"""

from __future__ import annotations

from mas_orchestrator.memory.backends import (
    MAX_RECORDS_PER_TENANT,
    InProcessMemoryStore,
    MemoryStore,
    assert_tenant_scoped,
    evict_expired,
)
from mas_orchestrator.memory.compaction import (
    CompactionResult,
    Strategy,
    compact,
    deduplicate,
    needs_compaction,
    take_head_tail,
    take_newest,
    total_chars,
)
from mas_orchestrator.memory.episodic import (
    MAX_EPISODES_PER_EXECUTION,
    build_episode,
    episode_query,
    validate_summary,
)
from mas_orchestrator.memory.long_term import (
    DEFAULT_RETENTION_DAYS,
    PERSISTENCE_RISK_CEILING,
    build_durable_memory,
    durable_query,
    inspect,
    is_safe_to_persist,
)
from mas_orchestrator.memory.manager import (
    MAX_CONTEXT_RECORDS,
    MemoryConfig,
    MemoryManager,
    NoRedaction,
    RedactionHook,
)
from mas_orchestrator.memory.models import (
    MAX_MEMORY_CONTENT_CHARS,
    MAX_MEMORY_METADATA_KEYS,
    MemoryHit,
    MemoryQuery,
    MemoryRecord,
    MemoryScope,
    Retention,
    Sensitivity,
    new_record_id,
)
from mas_orchestrator.memory.retention import (
    RetentionPolicy,
    describe_retention,
    should_delete,
    task_records_for_execution,
)
from mas_orchestrator.memory.semantic import (
    build_fact,
    looks_run_specific,
    validate_fact,
)
from mas_orchestrator.memory.short_term import (
    MAX_TASK_RECORDS_PER_EXECUTION,
    assert_disposable,
    build_task_memory,
    is_task_scoped,
    task_query,
)

__all__ = [
    "DEFAULT_RETENTION_DAYS",
    "MAX_CONTEXT_RECORDS",
    "MAX_EPISODES_PER_EXECUTION",
    "MAX_MEMORY_CONTENT_CHARS",
    "MAX_MEMORY_METADATA_KEYS",
    "MAX_RECORDS_PER_TENANT",
    "MAX_TASK_RECORDS_PER_EXECUTION",
    "PERSISTENCE_RISK_CEILING",
    "CompactionResult",
    "InProcessMemoryStore",
    "MemoryConfig",
    "MemoryHit",
    "MemoryManager",
    "MemoryQuery",
    "MemoryRecord",
    "MemoryScope",
    "MemoryStore",
    "NoRedaction",
    "RedactionHook",
    "Retention",
    "RetentionPolicy",
    "Sensitivity",
    "Strategy",
    "assert_disposable",
    "assert_tenant_scoped",
    "build_durable_memory",
    "build_episode",
    "build_fact",
    "build_task_memory",
    "compact",
    "deduplicate",
    "describe_retention",
    "durable_query",
    "episode_query",
    "evict_expired",
    "inspect",
    "is_safe_to_persist",
    "is_task_scoped",
    "looks_run_specific",
    "needs_compaction",
    "new_record_id",
    "should_delete",
    "take_head_tail",
    "take_newest",
    "task_query",
    "task_records_for_execution",
    "total_chars",
    "validate_fact",
    "validate_summary",
]
