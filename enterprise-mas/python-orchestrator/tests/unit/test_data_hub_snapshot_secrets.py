"""Data Hub snapshot metadata must not transport raw credentials."""

import pytest

from mas_orchestrator.config.enums import DataSourceKind
from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.data_hub.models import DataHubDocument, DataHubSource

_SCOPE = {
    "tenant_id": "00000000-0000-4000-8000-000000000001",
    "organization_id": "00000000-0000-4000-8000-000000000002",
    "project_id": "00000000-0000-4000-8000-000000000003",
    "source_id": "00000000-0000-4000-8000-000000000004",
}


def test_credentials_nested_in_source_arrays_are_rejected() -> None:
    with pytest.raises(InvalidRequestError, match="raw credentials"):
        DataHubSource(
            **_SCOPE,
            name="source",
            kind=DataSourceKind.WEB,
            config={"items": [[{"api-key": "not-a-real-key"}]]},
        )


def test_credentials_nested_in_document_arrays_are_rejected() -> None:
    with pytest.raises(InvalidRequestError, match="raw credentials"):
        DataHubDocument(
            **_SCOPE,
            document_id="00000000-0000-4000-8000-000000000005",
            title="document",
            mime_type="text/plain",
            source_kind=DataSourceKind.UPLOAD,
            content=b"example",
            metadata={"items": [{"token": "not-a-real-token"}]},
        )


def test_nested_secret_references_are_allowed() -> None:
    source = DataHubSource(
        **_SCOPE,
        name="source",
        kind=DataSourceKind.WEB,
        config={"items": [{"credential_secret_ref": "env://DATA_HUB_CREDENTIAL"}]},
    )
    assert source.config["items"]


@pytest.mark.parametrize("field", ["tenant_id", "organization_id", "project_id", "source_id"])
def test_source_scope_rejects_nil_uuid(field: str) -> None:
    scope = {**_SCOPE, field: "00000000-0000-0000-0000-000000000000"}
    with pytest.raises(InvalidRequestError, match="nil UUID"):
        DataHubSource(**scope, name="source", kind=DataSourceKind.WEB)


@pytest.mark.parametrize(
    "field", ["tenant_id", "organization_id", "project_id", "source_id", "document_id"]
)
def test_document_scope_rejects_nil_uuid(field: str) -> None:
    scope = {**_SCOPE, "document_id": "00000000-0000-4000-8000-000000000005"}
    scope[field] = "00000000-0000-0000-0000-000000000000"
    with pytest.raises(InvalidRequestError, match="nil UUID"):
        DataHubDocument(
            **scope,
            title="document",
            mime_type="text/plain",
            source_kind=DataSourceKind.UPLOAD,
            content=b"example",
        )
