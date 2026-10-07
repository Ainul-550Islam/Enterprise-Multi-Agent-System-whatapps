"""Configuration subsystem for the Python AI Orchestrator.

Public surface:

* :mod:`mas_orchestrator.config.enums` — stable configuration enumerations.
* :mod:`mas_orchestrator.config.settings` — typed settings models and
  :func:`~mas_orchestrator.config.settings.load_settings`.
* :mod:`mas_orchestrator.config.validation` — cross-field and environment
  safety checks, plus :func:`~mas_orchestrator.config.validation.validate_settings_or_raise`.

Typical wiring in ``main.py``::

    settings = load_settings()
    validate_settings_or_raise(settings)

Startup must call validation: a misconfigured process has to fail loudly
rather than run a degraded, silently-unsafe AI runtime.
"""

from __future__ import annotations

from mas_orchestrator.config.enums import (
    DataSourceKind,
    Environment,
    GuardrailFailureMode,
    LogFormat,
    MemoryBackend,
    ProviderMode,
    StrEnum,
    ToolExecutionMode,
    TransportSecurity,
    VectorBackend,
)
from mas_orchestrator.config.settings import (
    DEFAULT_MAX_PROMPT_BYTES,
    DEFAULT_MAX_TOOL_RESULT_BYTES,
    ENV_PREFIX,
    AgentRuntimeSettings,
    DataHubSettings,
    GuardrailSettings,
    MemorySettings,
    ObservabilitySettings,
    ProviderSettings,
    RagSettings,
    RustCoreSettings,
    SecuritySettings,
    ServiceSettings,
    Settings,
    ToolSettings,
    load_settings,
)
from mas_orchestrator.config.validation import (
    TEST_ONLY_PROVIDERS,
    ConfigurationError,
    ConfigViolation,
    production_ready_settings,
    validate_settings,
    validate_settings_or_raise,
)

__all__ = [
    "DEFAULT_MAX_PROMPT_BYTES",
    "DEFAULT_MAX_TOOL_RESULT_BYTES",
    "ENV_PREFIX",
    "TEST_ONLY_PROVIDERS",
    "AgentRuntimeSettings",
    "ConfigViolation",
    "ConfigurationError",
    "DataHubSettings",
    "DataSourceKind",
    "Environment",
    "GuardrailFailureMode",
    "GuardrailSettings",
    "LogFormat",
    "MemoryBackend",
    "MemorySettings",
    "ObservabilitySettings",
    "ProviderMode",
    "ProviderSettings",
    "RagSettings",
    "RustCoreSettings",
    "SecuritySettings",
    "ServiceSettings",
    "Settings",
    "StrEnum",
    "ToolExecutionMode",
    "ToolSettings",
    "TransportSecurity",
    "VectorBackend",
    "load_settings",
    "production_ready_settings",
    "validate_settings",
    "validate_settings_or_raise",
]
