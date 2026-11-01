"""Prompt system.

Implements spec §9. Templates are versioned, identifiable, hashable, safely
rendered, bounded in size and auditable. Rendering does not execute arbitrary
Python: it recognises exactly one construct, ``{{ name }}``, and refuses
anything else rather than passing it through.

The trust model is the point. Every substituted value carries a provenance,
and anything not explicitly marked :func:`~.renderer.trusted` is fenced and
labelled as data. A model reading the rendered prompt can see where third-party
content begins and ends, which is what makes an injected instruction legible as
an injection rather than as a directive.
"""

from __future__ import annotations

from mas_orchestrator.prompts.builtin_assistant import (
    PLATFORM_ASSISTANT_BODY,
    PLATFORM_ASSISTANT_PROMPT,
    PLATFORM_ASSISTANT_VERSION,
    QUICK_ACTION_PROMPTS,
    register_builtin_assistant_prompt,
)
from mas_orchestrator.prompts.hashing import (
    FINGERPRINT_LENGTH,
    FINGERPRINT_PREFIX,
    fingerprint_rendering,
    fingerprint_template,
    fingerprints_match,
)
from mas_orchestrator.prompts.registry import (
    MAX_PROMPT_CHARS,
    PromptRegistry,
    PromptTemplate,
    create_prompt,
    next_version,
)
from mas_orchestrator.prompts.renderer import (
    MAX_RENDERED_CHARS,
    Trust,
    Value,
    describe_value,
    fence,
    is_placeholder_free,
    render,
    scan_template,
    trusted,
    untrusted,
    validate_template,
    validate_values,
)
from mas_orchestrator.prompts.safety import (
    AssemblyReport,
    Disposition,
    SafetyVerdict,
    assert_assembly,
    check_assembly,
    join,
    prepare,
    screen,
)
from mas_orchestrator.prompts.system import (
    PLANNER_SYSTEM,
    STANDARD_SYSTEM,
    SUMMARISATION,
    SUPERVISOR_SYSTEM,
    TOOL_USER_SYSTEM,
    build_default_registry,
    register_system_prompts,
    system_prompt_for,
)

__all__ = [
    "FINGERPRINT_LENGTH",
    "FINGERPRINT_PREFIX",
    "MAX_PROMPT_CHARS",
    "MAX_RENDERED_CHARS",
    "PLANNER_SYSTEM",
    "PLATFORM_ASSISTANT_BODY",
    "PLATFORM_ASSISTANT_PROMPT",
    "PLATFORM_ASSISTANT_VERSION",
    "QUICK_ACTION_PROMPTS",
    "STANDARD_SYSTEM",
    "SUMMARISATION",
    "SUPERVISOR_SYSTEM",
    "TOOL_USER_SYSTEM",
    "AssemblyReport",
    "Disposition",
    "PromptRegistry",
    "PromptTemplate",
    "SafetyVerdict",
    "Trust",
    "Value",
    "assert_assembly",
    "build_default_registry",
    "check_assembly",
    "create_prompt",
    "describe_value",
    "fence",
    "fingerprint_rendering",
    "fingerprint_template",
    "fingerprints_match",
    "is_placeholder_free",
    "join",
    "next_version",
    "prepare",
    "register_builtin_assistant_prompt",
    "register_system_prompts",
    "render",
    "scan_template",
    "screen",
    "system_prompt_for",
    "trusted",
    "untrusted",
    "validate_template",
    "validate_values",
]
