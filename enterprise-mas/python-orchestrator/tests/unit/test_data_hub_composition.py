"""Data Hub startup fails closed on incomplete or unsafe configuration."""

import pytest
from pydantic import ValidationError

from mas_orchestrator.config.enums import DataSourceKind, VectorBackend
from mas_orchestrator.config.settings import DataHubSettings, RagSettings, Settings
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
            enabled=True, vector_backend=VectorBackend.PGVECTOR, embedding_dimensions=1536
        )
    )
    with pytest.raises(ConfigurationError, match="secret reference"):
        build_data_hub_service(settings)


def test_web_and_object_sources_require_explicit_egress_hosts() -> None:
    for kind in (DataSourceKind.WEB, DataSourceKind.OBJECT_STORAGE):
        with pytest.raises(ValidationError, match="allowlist"):
            DataHubSettings(allowed_source_kinds=(kind,))


def test_source_and_sync_byte_bounds_cannot_contradict() -> None:
    with pytest.raises(ValidationError, match="byte bounds"):
        DataHubSettings(max_upload_bytes=4096, max_document_bytes=2048)
    with pytest.raises(ValidationError, match="byte bounds"):
        DataHubSettings(max_document_bytes=4096, max_upload_bytes=2048, max_sync_bytes=2048)


def test_data_hub_rejects_vector_index_that_exposes_unowned_rows() -> None:
    from mas_orchestrator.data_hub.service import DataHubService
    from mas_orchestrator.rag.pgvector_index import PgVectorIndex

    settings = Settings(rag=RagSettings(enabled=True, vector_backend=VectorBackend.PGVECTOR))
    unsafe_index = PgVectorIndex(object(), dimensions=1536, allow_unowned_documents=True)
    with pytest.raises(ConfigurationError, match="cannot retrieve unowned"):
        DataHubService(
            settings=settings,
            pipeline=object(),  # type: ignore[arg-type]
            index=unsafe_index,
            embedder=object(),  # type: ignore[arg-type]
        )


def test_operator_object_bindings_load_from_toml_and_reach_production_adapter(
    monkeypatch: pytest.MonkeyPatch, tmp_path: object
) -> None:
    from pathlib import Path

    from mas_orchestrator.config.settings import load_settings
    from mas_orchestrator.data_hub.sources import S3CompatibleAdapter

    assert isinstance(tmp_path, Path)
    (tmp_path / "default.toml").write_text("""
[rag]
enabled = true
vector_backend = "pgvector"
embedding_dimensions = 1536
[data_hub]
database_url_secret_ref = "env:TEST_DATAHUB_DB"
embedding_api_key_secret_ref = "env:TEST_DATAHUB_EMBED"
allowed_source_kinds = ["object_storage"]
allowed_object_hosts = ["storage.example.test"]
[[data_hub.object_storage_bindings]]
tenant_id = "00000000-0000-4000-8000-000000000012"
organization_id = "00000000-0000-4000-8000-000000000013"
project_id = "00000000-0000-4000-8000-000000000014"
source_id = "00000000-0000-4000-8000-000000000011"
endpoint_url = "https://storage.example.test"
bucket = "test-bucket"
prefix = "docs/"
region = "us-east-1"
access_key_secret_ref = "env:TEST_ACCESS_KEY"
secret_key_secret_ref = "env:TEST_SECRET_KEY"
""")
    monkeypatch.setenv("TEST_DATAHUB_DB", "postgresql://unused:unused@localhost/unused")
    monkeypatch.setenv("TEST_DATAHUB_EMBED", "dummy-not-a-real-provider-key")
    settings = load_settings(config_dir=tmp_path)
    service = build_data_hub_service(settings)
    adapter = service._adapters[DataSourceKind.OBJECT_STORAGE].__self__  # type: ignore[attr-defined]
    assert isinstance(adapter, S3CompatibleAdapter)
    assert adapter._bindings == settings.data_hub.object_storage_bindings
    assert len(adapter._bindings) == 1


def test_object_bindings_reject_nil_ids_and_inline_secret_values() -> None:
    from mas_orchestrator.config.settings import DataHubObjectStorageBinding

    binding = {
        "tenant_id": "00000000-0000-4000-8000-000000000012",
        "organization_id": "00000000-0000-4000-8000-000000000013",
        "project_id": "00000000-0000-4000-8000-000000000014",
        "source_id": "00000000-0000-4000-8000-000000000011",
        "endpoint_url": "https://storage.example.test",
        "bucket": "test-bucket",
        "prefix": "docs/",
        "access_key_secret_ref": "env:TEST_ACCESS_KEY",
        "secret_key_secret_ref": "env:TEST_SECRET_KEY",
    }
    with pytest.raises(ValidationError, match="non-nil canonical UUID"):
        DataHubObjectStorageBinding(
            **{**binding, "tenant_id": "00000000-0000-0000-0000-000000000000"}
        )
    with pytest.raises(ValidationError, match="supported secret reference"):
        DataHubObjectStorageBinding(**{**binding, "secret_key_secret_ref": "literal-raw-secret"})


def test_object_bindings_load_from_operator_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    import json

    from mas_orchestrator.config.settings import load_settings

    binding = {
        "tenant_id": "00000000-0000-4000-8000-000000000012",
        "organization_id": "00000000-0000-4000-8000-000000000013",
        "project_id": "00000000-0000-4000-8000-000000000014",
        "source_id": "00000000-0000-4000-8000-000000000011",
        "endpoint_url": "https://storage.example.test",
        "bucket": "test-bucket",
        "prefix": "docs/",
        "access_key_secret_ref": "env:TEST_ACCESS_KEY",
        "secret_key_secret_ref": "env:TEST_SECRET_KEY",
    }
    monkeypatch.setenv("MAS_ORCH_DATA_HUB__OBJECT_STORAGE_BINDINGS", json.dumps([binding]))
    settings = load_settings()
    assert settings.data_hub.object_storage_bindings[0].source_id == binding["source_id"]
    monkeypatch.setenv("MAS_ORCH_DATA_HUB__OBJECT_STORAGE_BINDINGS", "not-json")
    with pytest.raises(ValidationError, match="must be a JSON array"):
        load_settings()
