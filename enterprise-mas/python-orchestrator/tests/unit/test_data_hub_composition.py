"""Data Hub startup fails closed on incomplete or unsafe configuration."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from mas_orchestrator.config.enums import DataSourceKind, VectorBackend
from mas_orchestrator.config.settings import DataHubSettings, RagSettings, Settings, load_settings
from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.data_hub.composition import build_data_hub_service


def test_disabled_data_hub_has_no_fake_runtime() -> None:
    service = build_data_hub_service(Settings())
    assert not service.enabled


def test_enabled_data_hub_requires_durable_pgvector() -> None:
    settings = Settings(rag=RagSettings(enabled=True, vector_backend=VectorBackend.IN_MEMORY))
    with pytest.raises(ConfigurationError, match="dedicated pgvector"):
        build_data_hub_service(settings)


def test_enabled_data_hub_requires_secret_references() -> None:
    settings = Settings(
        rag=RagSettings(
            enabled=True,
            vector_backend=VectorBackend.PGVECTOR,
            embedding_dimensions=1536,
            embedding_api_key_secret_ref=None,
        )
    )
    with pytest.raises(ConfigurationError, match="secret references"):
        build_data_hub_service(settings)


def test_network_sources_require_explicit_egress_hosts() -> None:
    from mas_orchestrator.data_hub.sources import S3CompatibleAdapter, WebCrawlAdapter

    with pytest.raises(ConfigurationError, match="non-empty egress allow-list"):
        WebCrawlAdapter(
            allowed_hosts=(),
            max_depth=1,
            max_pages=10,
            max_document_bytes=4096,
            max_sync_bytes=8192,
        )
    with pytest.raises(ConfigurationError, match="non-empty egress allow-list"):
        S3CompatibleAdapter(
            allowed_hosts=(),
            max_pages=10,
            max_document_bytes=4096,
            max_sync_bytes=8192,
        )


def test_upload_and_document_byte_bounds_cannot_contradict() -> None:
    with pytest.raises(
        ValidationError, match="max_upload_bytes must not exceed max_document_bytes"
    ):
        DataHubSettings(max_upload_bytes=4096, max_document_bytes=2048)


def test_data_hub_rejects_non_dedicated_pgvector_role() -> None:
    settings = Settings(
        rag=RagSettings(
            enabled=True,
            vector_backend=VectorBackend.PGVECTOR,
            embedding_dimensions=1536,
        ),
        data_hub=DataHubSettings(
            database_url_secret_ref="env:TEST_DATAHUB_DB",
            database_role="application",
        ),
    )
    with pytest.raises(ConfigurationError, match="dedicated mas_rag database role"):
        build_data_hub_service(settings)


def test_object_storage_adapter_uses_configured_egress_allowlist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mas_orchestrator.data_hub.sources import S3CompatibleAdapter

    monkeypatch.setenv("TEST_DATAHUB_DB", "postgresql://unused:unused@localhost/unused")
    monkeypatch.setenv("TEST_DATAHUB_EMBED", "dummy-not-a-real-provider-key")
    settings = Settings(
        rag=RagSettings(
            enabled=True,
            vector_backend=VectorBackend.PGVECTOR,
            embedding_dimensions=1536,
            embedding_api_key_secret_ref="env:TEST_DATAHUB_EMBED",
        ),
        data_hub=DataHubSettings(
            allowed_source_kinds=(DataSourceKind.OBJECT_STORAGE,),
            database_url_secret_ref="env:TEST_DATAHUB_DB",
            object_storage_egress_allowlist=("storage.example.test",),
        ),
    )
    service = build_data_hub_service(settings)
    adapter = service._adapters[DataSourceKind.OBJECT_STORAGE].__self__  # type: ignore[attr-defined]
    assert isinstance(adapter, S3CompatibleAdapter)
    assert adapter._allowed_hosts == frozenset({"storage.example.test"})


def test_operator_settings_load_egress_allowlist_from_toml(tmp_path: Path) -> None:
    (tmp_path / "default.toml").write_text(
        """
[rag]
enabled = true
vector_backend = "pgvector"
embedding_dimensions = 1536
embedding_api_key_secret_ref = "env:TEST_DATAHUB_EMBED"
[data_hub]
database_url_secret_ref = "env:TEST_DATAHUB_DB"
allowed_source_kinds = ["object_storage"]
object_storage_egress_allowlist = ["storage.example.test"]
""",
        encoding="utf-8",
    )
    settings = load_settings(config_dir=tmp_path)
    assert settings.data_hub.object_storage_egress_allowlist == ("storage.example.test",)


def test_duplicate_source_kinds_are_rejected() -> None:
    with pytest.raises(ValidationError, match="must not contain duplicates"):
        DataHubSettings(
            allowed_source_kinds=(DataSourceKind.WEB, DataSourceKind.WEB),
        )
