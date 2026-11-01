"""Unit tests for the configuration subsystem.

Covered behaviours:

* defaults are safe (no provider, no tool execution, no durable backend)
* environment overlay and env-var layering
* cross-field validation (chunk window, backoff bounds, allowlists)
* production guardrails refuse unsafe combinations
* violation reporting is complete rather than fail-on-first
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from mas_orchestrator.config.enums import (
    Environment,
    GuardrailFailureMode,
    MemoryBackend,
    ProviderMode,
    ToolExecutionMode,
    TransportSecurity,
    VectorBackend,
)
from mas_orchestrator.config.settings import (
    ENV_PREFIX,
    AgentRuntimeSettings,
    ProviderSettings,
    RagSettings,
    RustCoreSettings,
    ServiceSettings,
    Settings,
    ToolSettings,
    load_settings,
)
from mas_orchestrator.config.validation import (
    ConfigurationError,
    ConfigViolation,
    production_ready_settings,
    validate_settings,
    validate_settings_or_raise,
)

# ---------------------------------------------------------------------------
# defaults
# ---------------------------------------------------------------------------


def test_defaults_are_safe() -> None:
    """A default-constructed configuration must not enable anything risky."""
    settings = Settings()

    assert settings.environment is Environment.DEVELOPMENT
    assert settings.providers.mode is ProviderMode.DISABLED
    assert settings.providers.enabled_providers == ()
    assert settings.providers.allowed_models == ()
    assert settings.tools.execution_mode is ToolExecutionMode.DISABLED
    assert settings.tools.allow_http_tools is False
    assert settings.memory.backend is MemoryBackend.DISABLED
    assert settings.rag.enabled is False
    assert settings.rag.vector_backend is VectorBackend.DISABLED
    assert settings.security.trust_caller_tenant_header is False
    assert settings.security.require_authenticated_caller is True
    assert settings.observability.redact_logs is True
    assert settings.guardrails.treat_tool_output_untrusted is True


def test_settings_are_immutable() -> None:
    """Runtime code must not be able to mutate configuration in place."""
    settings = Settings()
    with pytest.raises(ValidationError):
        settings.environment = Environment.PRODUCTION  # type: ignore[misc]


def test_unknown_key_is_rejected() -> None:
    """A typo in configuration must fail loudly, not be silently ignored."""
    with pytest.raises(ValidationError):
        Settings.model_validate({"provider_mode": "live"})


# ---------------------------------------------------------------------------
# single-field bounds
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("timeout", [0, -1])
def test_non_positive_timeout_rejected(timeout: float) -> None:
    with pytest.raises(ValidationError):
        RustCoreSettings(request_timeout_seconds=timeout)


def test_max_attempts_bounded() -> None:
    with pytest.raises(ValidationError):
        RustCoreSettings(max_attempts=0)
    with pytest.raises(ValidationError):
        RustCoreSettings(max_attempts=99)


def test_mtls_requires_both_halves_of_the_keypair() -> None:
    with pytest.raises(ValidationError, match="client_cert_secret_ref"):
        RustCoreSettings(transport=TransportSecurity.MTLS, client_cert_secret_ref="cert")


def test_mtls_accepts_a_complete_keypair_reference() -> None:
    settings = RustCoreSettings(
        transport=TransportSecurity.MTLS,
        client_cert_secret_ref="cert-ref",
        client_key_secret_ref="key-ref",
    )
    assert settings.transport.uses_tls


def test_inbound_grpc_tls_requires_server_certificate_and_key() -> None:
    with pytest.raises(ValidationError, match="grpc_certificate_chain_path"):
        ServiceSettings(grpc_transport_security=TransportSecurity.TLS)


def test_inbound_grpc_mtls_requires_a_client_ca_bundle() -> None:
    with pytest.raises(ValidationError, match="grpc_client_ca_bundle_path"):
        ServiceSettings(
            grpc_transport_security=TransportSecurity.MTLS,
            grpc_certificate_chain_path="/run/secrets/server.crt",
            grpc_private_key_path="/run/secrets/server.key",
        )


def test_inbound_grpc_tls_accepts_server_identity() -> None:
    settings = ServiceSettings(
        grpc_transport_security=TransportSecurity.TLS,
        grpc_certificate_chain_path="/run/secrets/server.crt",
        grpc_private_key_path="/run/secrets/server.key",
    )
    assert settings.grpc_transport_security.uses_tls


def test_negative_pricing_rejected() -> None:
    with pytest.raises(ValidationError, match="negative"):
        ProviderSettings(pricing={"gpt-x": {"input_per_1k": -1.0}})


def test_non_negative_pricing_accepted() -> None:
    settings = ProviderSettings(pricing={"gpt-x": {"input_per_1k": 0.001, "output_per_1k": 0.002}})
    assert settings.pricing["gpt-x"]["output_per_1k"] == 0.002


def test_chunk_overlap_must_be_smaller_than_chunk_size() -> None:
    with pytest.raises(ValidationError, match="chunk_overlap_chars"):
        RagSettings(chunk_size_chars=100, chunk_overlap_chars=100)


def test_rerank_top_k_cannot_exceed_retrieval_top_k() -> None:
    with pytest.raises(ValidationError, match="rerank_top_k"):
        RagSettings(retrieval_top_k=5, rerank_top_k=10)


def test_agent_runtime_bounds_are_positive() -> None:
    with pytest.raises(ValidationError):
        AgentRuntimeSettings(max_planning_steps=0)
    with pytest.raises(ValidationError):
        AgentRuntimeSettings(max_run_seconds=0)


def test_unknown_log_level_rejected() -> None:
    with pytest.raises(ValidationError, match="log_level"):
        Settings.model_validate({"observability": {"log_level": "verbose"}})


def test_log_level_is_normalised() -> None:
    settings = Settings.model_validate({"observability": {"log_level": "debug"}})
    assert settings.observability.log_level == "DEBUG"


# ---------------------------------------------------------------------------
# layering
# ---------------------------------------------------------------------------


def test_load_settings_reads_environment_overlay(tmp_path: object) -> None:
    """The environment overlay must win over default.toml."""
    import pathlib

    config_dir = pathlib.Path(str(tmp_path))
    (config_dir / "default.toml").write_text(
        '[rust_core]\nendpoint = "from-default:500"\n',
        encoding="utf-8",
    )
    (config_dir / "production.toml").write_text(
        '[rust_core]\nendpoint = "from-production:500"\n',
        encoding="utf-8",
    )

    settings = load_settings(environment=Environment.PRODUCTION, config_dir=config_dir)
    assert settings.rust_core.endpoint == "from-production:500"


def test_env_var_overrides_file(monkeypatch: pytest.MonkeyPatch, tmp_path: object) -> None:
    """``MAS_ORCH_*`` variables must outrank TOML files."""
    import pathlib

    config_dir = pathlib.Path(str(tmp_path))
    (config_dir / "default.toml").write_text(
        '[rust_core]\nendpoint = "from-file:500"\n',
        encoding="utf-8",
    )
    monkeypatch.setenv(f"{ENV_PREFIX}RUST_CORE__ENDPOINT", "from-env:900")

    settings = load_settings(environment=Environment.DEVELOPMENT, config_dir=config_dir)
    assert settings.rust_core.endpoint == "from-env:900"


def test_env_var_nesting_uses_double_underscore(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(f"{ENV_PREFIX}AGENTS__MAX_PLANNING_STEPS", "42")
    settings = load_settings(environment=Environment.DEVELOPMENT, config_dir=None)
    assert settings.agents.max_planning_steps == 42


def test_explicit_overrides_win_over_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(f"{ENV_PREFIX}RUST_CORE__ENDPOINT", "from-env:900")
    settings = load_settings(
        environment=Environment.DEVELOPMENT,
        config_dir=None,
        overrides={"rust_core": {"endpoint": "explicit:1"}},
    )
    assert settings.rust_core.endpoint == "explicit:1"


def test_mas_env_selects_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MAS_ENV", "production")
    settings = load_settings(config_dir=None)
    assert settings.environment is Environment.PRODUCTION
    assert settings.is_production


def test_unknown_mas_env_fails_loudly(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unrecognised MAS_ENV must fail startup with a self-describing error."""
    monkeypatch.setenv("MAS_ENV", "prodution")
    with pytest.raises(ValueError, match="unknown MAS_ENV"):
        load_settings(config_dir=None)


# ---------------------------------------------------------------------------
# cross-field validation
# ---------------------------------------------------------------------------


def test_tls_without_token_reference_is_a_violation() -> None:
    settings = Settings(environment=Environment.DEVELOPMENT)
    violations = validate_settings(settings)
    fields = {v.field for v in violations}
    assert "rust_core.bearer_token_secret_ref" in fields


def test_http_tools_require_a_host_allowlist() -> None:
    settings = Settings(tools=ToolSettings(allow_http_tools=True))
    fields = {v.field for v in validate_settings(settings)}
    assert "tools.allowed_http_hosts" in fields


def test_http_tools_with_allowlist_are_accepted() -> None:
    settings = Settings(
        tools=ToolSettings(allow_http_tools=True, allowed_http_hosts=("api.example.com",))
    )
    fields = {v.field for v in validate_settings(settings)}
    assert "tools.allowed_http_hosts" not in fields


def test_live_mode_requires_enabled_provider_and_allowlist() -> None:
    settings = Settings(providers=ProviderSettings(mode=ProviderMode.LIVE))
    fields = {v.field for v in validate_settings(settings)}
    assert "providers.enabled_providers" in fields
    assert "providers.allowed_models" in fields


def test_default_model_must_be_allowlisted() -> None:
    settings = Settings(
        providers=ProviderSettings(
            mode=ProviderMode.LIVE,
            enabled_providers=("openai",),
            allowed_models=("model-a",),
            default_model="model-b",
        )
    )
    fields = {v.field for v in validate_settings(settings)}
    assert "providers.default_model" in fields


def test_failover_model_must_be_allowlisted() -> None:
    settings = Settings(
        providers=ProviderSettings(
            mode=ProviderMode.LIVE,
            enabled_providers=("openai",),
            allowed_models=("model-a",),
            default_model="model-a",
            failover_models=("model-z",),
        )
    )
    fields = {v.field for v in validate_settings(settings)}
    assert "providers.failover_models" in fields


def test_backoff_ceiling_below_base_is_a_violation() -> None:
    settings = Settings(
        providers=ProviderSettings(backoff_base_seconds=10.0, backoff_max_seconds=2.0)
    )
    fields = {v.field for v in validate_settings(settings)}
    assert "providers.backoff_max_seconds" in fields


def test_rag_requires_a_vector_backend() -> None:
    settings = Settings.model_validate({"rag": {"enabled": True}})
    fields = {v.field for v in validate_settings(settings)}
    assert "rag.vector_backend" in fields


def test_rag_requires_known_embedding_dimensions() -> None:
    settings = Settings.model_validate(
        {"rag": {"enabled": True, "vector_backend": "in_memory", "embedding_dimensions": 0}}
    )
    fields = {v.field for v in validate_settings(settings)}
    assert "rag.embedding_dimensions" in fields


def test_trusting_caller_tenant_header_is_always_a_violation() -> None:
    settings = Settings.model_validate({"security": {"trust_caller_tenant_header": True}})
    fields = {v.field for v in validate_settings(settings)}
    assert "security.trust_caller_tenant_header" in fields


def test_unauthenticated_caller_never_allowed() -> None:
    settings = Settings.model_validate({"security": {"require_authenticated_caller": False}})
    fields = {v.field for v in validate_settings(settings)}
    assert "security.require_authenticated_caller" in fields


# ---------------------------------------------------------------------------
# production guardrails
# ---------------------------------------------------------------------------


def test_production_refuses_insecure_transport() -> None:
    settings = Settings(
        environment=Environment.PRODUCTION,
        rust_core=RustCoreSettings(
            transport=TransportSecurity.INSECURE, bearer_token_secret_ref="tok"
        ),
    )
    fields = {v.field for v in validate_settings(settings)}
    assert "rust_core.transport" in fields


def test_production_refuses_replay_provider_mode() -> None:
    settings = Settings(
        environment=Environment.PRODUCTION, providers=ProviderSettings(mode=ProviderMode.REPLAY)
    )
    fields = {v.field for v in validate_settings(settings)}
    assert "providers.mode" in fields


def test_production_refuses_subprocess_tool_execution() -> None:
    settings = Settings(
        environment=Environment.PRODUCTION,
        tools=ToolSettings(execution_mode=ToolExecutionMode.SUBPROCESS),
    )
    fields = {v.field for v in validate_settings(settings)}
    assert "tools.execution_mode" in fields


def test_production_refuses_in_memory_backends() -> None:
    settings = Settings.model_validate(
        {
            "environment": "production",
            "memory": {"backend": "in_memory"},
            "rag": {"vector_backend": "in_memory", "enabled": True, "embedding_dimensions": 1536},
        }
    )
    fields = {v.field for v in validate_settings(settings)}
    assert "memory.backend" in fields
    assert "rag.vector_backend" in fields


def test_production_refuses_audit_only_guardrails() -> None:
    settings = Settings.model_validate(
        {
            "environment": "production",
            "guardrails": {
                "input_failure_mode": "audit_only",
                "output_failure_mode": "audit_only",
                "citation_failure_mode": "audit_only",
                "injection_failure_mode": "audit_only",
            },
        }
    )
    fields = {v.field for v in validate_settings(settings)}
    assert "guardrails.input_failure_mode" in fields
    assert "guardrails.output_failure_mode" in fields


def test_production_refuses_disabled_log_redaction() -> None:
    settings = Settings.model_validate(
        {"environment": "production", "observability": {"redact_logs": False}}
    )
    fields = {v.field for v in validate_settings(settings)}
    assert "observability.redact_logs" in fields


def test_production_refuses_unauthenticated_inbound_service() -> None:
    settings = Settings(environment=Environment.PRODUCTION)
    fields = {v.field for v in validate_settings(settings)}
    assert "service.service_token_secret_ref" in fields


def test_production_requires_policy_bridge() -> None:
    settings = Settings.model_validate(
        {"environment": "production", "guardrails": {"policy_bridge_enabled": False}}
    )
    fields = {v.field for v in validate_settings(settings)}
    assert "guardrails.policy_bridge_enabled" in fields


def test_production_requires_tool_policy_decisions() -> None:
    settings = Settings.model_validate(
        {"environment": "production", "tools": {"require_policy_decision": False}}
    )
    fields = {v.field for v in validate_settings(settings)}
    assert "tools.require_policy_decision" in fields


def test_fake_providers_refused_outside_dev_and_test() -> None:
    settings = Settings.model_validate(
        {
            "environment": "staging",
            "providers": {"enabled_providers": ["fake"], "mode": "disabled"},
        }
    )
    fields = {v.field for v in validate_settings(settings)}
    assert "providers.enabled_providers" in fields


def test_fake_provider_allowed_in_test_environment() -> None:
    settings = Settings.model_validate(
        {"environment": "test", "providers": {"enabled_providers": ["fake"], "mode": "disabled"}}
    )
    fields = {v.field for v in validate_settings(settings)}
    assert "providers.enabled_providers" not in fields


# ---------------------------------------------------------------------------
# violation reporting
# ---------------------------------------------------------------------------


def test_configuration_error_reports_every_violation() -> None:
    settings = Settings.model_validate(
        {
            "environment": "production",
            "security": {"trust_caller_tenant_header": True},
            "observability": {"redact_logs": False},
            "memory": {"backend": "in_memory"},
        }
    )
    with pytest.raises(ConfigurationError) as excinfo:
        validate_settings_or_raise(settings)

    reported = excinfo.value.violations
    assert len(reported) >= 3
    assert all(isinstance(v, ConfigViolation) for v in reported)


def test_violation_is_serialisable() -> None:
    violation = ConfigViolation(field="a.b", message="bad", remediation="fix")
    assert violation.as_dict() == {
        "field": "a.b",
        "message": "bad",
        "remediation": "fix",
    }
    assert "a.b" in str(violation)
    assert "ConfigViolation" in repr(violation)


def test_validate_settings_or_raise_returns_settings_when_clean() -> None:
    clean = production_ready_settings(Environment.PRODUCTION)
    assert validate_settings_or_raise(clean) is clean


def test_production_ready_baseline_has_no_violations() -> None:
    settings = production_ready_settings(Environment.PRODUCTION)
    assert validate_settings(settings) == ()


def test_production_ready_also_valid_in_staging() -> None:
    settings = production_ready_settings(Environment.STAGING)
    assert validate_settings(settings) == ()


def test_plaintext_inbound_grpc_is_rejected_in_production() -> None:
    settings = production_ready_settings(Environment.PRODUCTION)
    insecure_service = settings.service.model_copy(
        update={"grpc_transport_security": TransportSecurity.INSECURE}
    )
    unsafe = settings.model_copy(update={"service": insecure_service})

    fields = {violation.field for violation in validate_settings(unsafe)}

    assert "service.grpc_transport_security" in fields


def test_remote_bridge_bind_requires_private_network_opt_in() -> None:
    settings = production_ready_settings(Environment.PRODUCTION)
    remote_service = settings.service.model_copy(update={"bind_host": "0.0.0.0"})
    unsafe = settings.model_copy(update={"service": remote_service})

    fields = {violation.field for violation in validate_settings(unsafe)}

    assert "service.allow_remote_bridge_clients" in fields

    explicitly_allowed_service = remote_service.model_copy(
        update={"allow_remote_bridge_clients": True}
    )
    allowed = settings.model_copy(update={"service": explicitly_allowed_service})

    assert validate_settings(allowed) == ()


def test_guardrail_failure_mode_semantics() -> None:
    assert GuardrailFailureMode.FAIL_CLOSED.denies_execution is True
    assert GuardrailFailureMode.REDACT.denies_execution is False
    assert GuardrailFailureMode.AUDIT_ONLY.denies_execution is False


def test_environment_semantics() -> None:
    assert Environment.PRODUCTION.is_production is True
    assert Environment.STAGING.is_production is False
    assert Environment.PRODUCTION.allows_fakes is False
    assert Environment.DEVELOPMENT.allows_fakes is True
    assert Environment.TEST.allows_fakes is True
