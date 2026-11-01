"""Built-in tools.

Only tools that can be made safe by construction belong here. Anything whose
safety depends on configuration or on the model behaving well does not.

* :mod:`~mas_orchestrator.tools.builtins.http` — outbound HTTP, funnelled
  through the platform's SSRF-guarded transport.
* :mod:`~mas_orchestrator.tools.builtins.code` — code execution behind an
  injected sandbox, refusing to run when none is configured.
* :mod:`~mas_orchestrator.tools.builtins.sandbox` — what "sandboxed" must
  mean, and the gate that refuses a configuration which does not meet it.
* :mod:`~mas_orchestrator.tools.builtins.database` — read-only SQL, vetted
  statement by statement.
* :mod:`~mas_orchestrator.tools.builtins.search` — search with fenced results.
* :mod:`~mas_orchestrator.tools.builtins.email` — mail to allowlisted
  recipients only, approval-required and never retried.
* :mod:`~mas_orchestrator.tools.builtins.webhook` — signed posts to
  pre-registered destinations; the model never supplies a URL.

No module here introduces its own HTTP client or process executor: outbound
HTTP goes through the platform transport, and code goes through an injected
sandbox.
"""

from __future__ import annotations

from mas_orchestrator.tools.builtins.code import (
    CodeSandbox,
    CodeTool,
    NoSandboxConfigured,
    SandboxLimits,
    SandboxResult,
    build_code_tool,
)
from mas_orchestrator.tools.builtins.database import (
    DatabaseBackend,
    DatabaseTool,
    DatabaseToolConfig,
    build_database_tool,
    normalise_statement,
    validate_statement,
)
from mas_orchestrator.tools.builtins.email import (
    EmailBackend,
    EmailTool,
    EmailToolConfig,
    build_email_tool,
    is_valid_address,
)
from mas_orchestrator.tools.builtins.http import (
    ALLOWED_METHODS,
    HttpTool,
    HttpToolConfig,
    build_http_tool,
)
from mas_orchestrator.tools.builtins.sandbox import (
    ACCEPTED_ISOLATION,
    DEFAULT_ALLOWED_LANGUAGES,
    FilesystemMode,
    IsolationKind,
    NetworkMode,
    SandboxContract,
    assert_may_execute,
    deny_reason,
    require_allowed_language,
    validate_contract,
)
from mas_orchestrator.tools.builtins.search import (
    SearchBackend,
    SearchResult,
    SearchTool,
    SearchToolConfig,
    build_search_tool,
    looks_like_url,
)
from mas_orchestrator.tools.builtins.webhook import (
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    StaticSecret,
    WebhookTarget,
    WebhookTool,
    WebhookToolConfig,
    build_webhook_tool,
    canonical_bytes,
)

__all__ = [
    "ACCEPTED_ISOLATION",
    "ALLOWED_METHODS",
    "DEFAULT_ALLOWED_LANGUAGES",
    "SIGNATURE_HEADER",
    "TIMESTAMP_HEADER",
    "CodeSandbox",
    "CodeTool",
    "DatabaseBackend",
    "DatabaseTool",
    "DatabaseToolConfig",
    "EmailBackend",
    "EmailTool",
    "EmailToolConfig",
    "FilesystemMode",
    "HttpTool",
    "HttpToolConfig",
    "IsolationKind",
    "NetworkMode",
    "NoSandboxConfigured",
    "SandboxContract",
    "SandboxLimits",
    "SandboxResult",
    "SearchBackend",
    "SearchResult",
    "SearchTool",
    "SearchToolConfig",
    "StaticSecret",
    "WebhookTarget",
    "WebhookTool",
    "WebhookToolConfig",
    "assert_may_execute",
    "build_code_tool",
    "build_database_tool",
    "build_email_tool",
    "build_http_tool",
    "build_search_tool",
    "build_webhook_tool",
    "canonical_bytes",
    "deny_reason",
    "is_valid_address",
    "looks_like_url",
    "normalise_statement",
    "require_allowed_language",
    "validate_contract",
    "validate_statement",
]
