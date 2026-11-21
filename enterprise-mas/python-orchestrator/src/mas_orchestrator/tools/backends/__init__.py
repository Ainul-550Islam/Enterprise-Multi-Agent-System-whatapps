"""Production adapters for the built-in tool backend protocols.

:mod:`~mas_orchestrator.tools.builtins` defines *what* a backend must do
(:class:`~mas_orchestrator.tools.builtins.email.EmailBackend`,
:class:`~mas_orchestrator.tools.builtins.search.SearchBackend`,
:class:`~mas_orchestrator.tools.builtins.database.DatabaseBackend`,
:class:`~mas_orchestrator.tools.builtins.code.CodeSandbox`) and ships fail-closed
null implementations. This package provides the real adapters:

* :mod:`~mas_orchestrator.tools.backends.email` -- ``SmtpEmailBackend`` and
  ``SendGridEmailBackend``.
* :mod:`~mas_orchestrator.tools.backends.search` -- ``BraveSearchBackend``,
  ``TavilySearchBackend``, ``SearxngSearchBackend``.
* :mod:`~mas_orchestrator.tools.backends.database` -- ``PostgresReadOnlyBackend``.
* :mod:`~mas_orchestrator.tools.backends.sandbox` -- ``RemoteSandboxBackend``.

Every adapter obeys the same rules as the built-in tools: no hidden retries
(one send is one send), no secret leaves the process (credentials are
:class:`~mas_orchestrator.security.credentials.Secret` values, revealed only at
the moment of use), and every HTTP call goes through the guarded
:class:`~mas_orchestrator.providers.base_http.HttpTransport` rather than a
private client.
"""

from __future__ import annotations

from mas_orchestrator.tools.backends.database import PostgresReadOnlyBackend
from mas_orchestrator.tools.backends.email import SendGridEmailBackend, SmtpEmailBackend
from mas_orchestrator.tools.backends.sandbox import RemoteSandboxBackend
from mas_orchestrator.tools.backends.search import (
    BraveSearchBackend,
    SearxngSearchBackend,
    TavilySearchBackend,
)

__all__ = [
    "BraveSearchBackend",
    "PostgresReadOnlyBackend",
    "RemoteSandboxBackend",
    "SearxngSearchBackend",
    "SendGridEmailBackend",
    "SmtpEmailBackend",
    "TavilySearchBackend",
]
