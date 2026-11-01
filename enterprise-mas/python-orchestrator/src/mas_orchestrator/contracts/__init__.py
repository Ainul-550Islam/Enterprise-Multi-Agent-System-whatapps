"""Cross-language contract models shared with the Rust Core.

This package holds the Python-side definition of everything that crosses the
Python<->Rust boundary: the typed error model, protocol negotiation, and the
message/execution/agent/task DTOs. Nothing here performs I/O or holds
configuration; it is pure data plus validation, so the contract can be tested
without infrastructure.

The wire encoding itself (protobuf) is generated from the authoritative Rust
``.proto`` into :mod:`mas_orchestrator.grpc.generated`. These models are the
*domain* representation: the gRPC layer parses bytes into protobuf objects and
then maps them onto these models via
:mod:`mas_orchestrator.contracts.messages`, so a protobuf change cannot quietly
alter domain semantics.
"""

from __future__ import annotations

from mas_orchestrator.contracts.errors import (
    AgentNotFoundError,
    AgentVersionNotFoundError,
    AuthenticationError,
    AuthorizationError,
    CancellationRequestedError,
    CitationValidationError,
    ConfigurationError,
    DependencyUnavailableError,
    DuplicateRequestError,
    ErrorCode,
    ExecutionTimeoutError,
    InternalError,
    InvalidRequestError,
    MemoryBackendError,
    ModelNotAvailableError,
    ModelProviderError,
    ModelRateLimitedError,
    ModelTimeoutError,
    OrchestratorError,
    OutputValidationFailedError,
    PiiPolicyDeniedError,
    PolicyDeniedError,
    PromptInjectionDetectedError,
    PromptRenderError,
    ProtocolError,
    QuotaExceededError,
    RetrievalError,
    TenantContextError,
    ToolFailedError,
    ToolNotFoundError,
    ToolPermissionDeniedError,
    ToolTimeoutError,
    error_from_code,
    is_retryable,
)
from mas_orchestrator.contracts.protocol_version import (
    CURRENT_PROTOCOL_VERSION,
    SUPPORTED_PROTOCOL_VERSIONS,
    ProtocolVersion,
    negotiate_protocol_version,
    parse_protocol_version,
)

__all__ = [
    "CURRENT_PROTOCOL_VERSION",
    "SUPPORTED_PROTOCOL_VERSIONS",
    "AgentNotFoundError",
    "AgentVersionNotFoundError",
    "AuthenticationError",
    "AuthorizationError",
    "CancellationRequestedError",
    "CitationValidationError",
    "ConfigurationError",
    "DependencyUnavailableError",
    "DuplicateRequestError",
    "ErrorCode",
    "ExecutionTimeoutError",
    "InternalError",
    "InvalidRequestError",
    "MemoryBackendError",
    "ModelNotAvailableError",
    "ModelProviderError",
    "ModelRateLimitedError",
    "ModelTimeoutError",
    "OrchestratorError",
    "OutputValidationFailedError",
    "PiiPolicyDeniedError",
    "PolicyDeniedError",
    "PromptInjectionDetectedError",
    "PromptRenderError",
    "ProtocolError",
    "ProtocolVersion",
    "QuotaExceededError",
    "RetrievalError",
    "TenantContextError",
    "ToolFailedError",
    "ToolNotFoundError",
    "ToolPermissionDeniedError",
    "ToolTimeoutError",
    "error_from_code",
    "is_retryable",
    "negotiate_protocol_version",
    "parse_protocol_version",
]
