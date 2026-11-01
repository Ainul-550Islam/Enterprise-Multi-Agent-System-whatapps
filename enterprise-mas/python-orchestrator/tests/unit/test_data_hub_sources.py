"""Resource-bound and checkpoint regressions for Data Hub source adapters."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

import mas_orchestrator.data_hub.sources as sources_module
from mas_orchestrator.config.enums import DataSourceKind
from mas_orchestrator.contracts.errors import InvalidRequestError, ModelProviderError
from mas_orchestrator.data_hub.models import DataHubSource
from mas_orchestrator.data_hub.sources import (
    S3CompatibleAdapter,
    WebCrawlAdapter,
    _decode_cursor,
    _encode_cursor,
)
from mas_orchestrator.providers.base_http import TransportResponse
from mas_orchestrator.security.credentials import Secret

_ROOT = "https://docs.example.test/root"
_TWO = "https://docs.example.test/two"
_THREE = "https://docs.example.test/three"


def _source(*, cursor: str | None = None) -> DataHubSource:
    """Builds a valid signed-source snapshot for one test crawler."""
    return DataHubSource(
        source_id="00000000-0000-4000-8000-000000000001",
        tenant_id="00000000-0000-4000-8000-000000000002",
        organization_id="00000000-0000-4000-8000-000000000003",
        project_id="00000000-0000-4000-8000-000000000004",
        name="test documentation",
        kind=DataSourceKind.WEB,
        config={"start_urls": [_ROOT], "max_depth": 1, "max_pages": 3},
        cursor=cursor,
    )


def _object_source() -> DataHubSource:
    """Builds a valid object-storage snapshot with secret references only."""
    return DataHubSource(
        source_id="00000000-0000-4000-8000-000000000011",
        tenant_id="00000000-0000-4000-8000-000000000012",
        organization_id="00000000-0000-4000-8000-000000000013",
        project_id="00000000-0000-4000-8000-000000000014",
        name="test object storage",
        kind=DataSourceKind.OBJECT_STORAGE,
        config={
            "endpoint_url": "https://storage.example.test",
            "bucket": "test-bucket",
            "prefix": "docs/",
            "region": "us-east-1",
            "access_key_secret_ref": "env:TEST_ACCESS_KEY",
            "secret_key_secret_ref": "env:TEST_SECRET_KEY",
        },
    )


def _body_at_limit(value: str, limit: int = 1024) -> bytes:
    """Produces a readable response with a precise transfer-accounting size."""
    encoded = value.encode("utf-8")
    assert len(encoded) <= limit
    return encoded + b" " * (limit - len(encoded))


@dataclass
class _RecordingTransport:
    """Minimal guarded-transport double retaining request budgets."""

    responses: dict[str, TransportResponse]
    calls: list[tuple[str, str, int | None]] = field(default_factory=list)

    async def request(
        self,
        method: str,
        url: str,
        **kwargs: object,
    ) -> TransportResponse:
        """Returns a preconfigured response and captures the per-call bound."""
        bound = kwargs.get("max_response_bytes")
        assert bound is None or isinstance(bound, int)
        self.calls.append((method, url, bound))
        return self.responses[url]


@pytest.mark.asyncio
async def test_web_sync_counts_unchanged_pages_against_aggregate_budget() -> None:
    """Stable ETags cannot bypass the total transfer budget before later pages."""
    root_body = _body_at_limit('<a href="/two">two</a><a href="/three">three</a>')
    transport = _RecordingTransport(
        {
            _ROOT: TransportResponse(
                status_code=200,
                body=root_body,
                headers={"content-type": "text/html", "etag": "root-v1"},
            ),
            _TWO: TransportResponse(
                status_code=200,
                body=_body_at_limit("changed document"),
                headers={"content-type": "text/plain", "etag": "two-v1"},
            ),
            _THREE: TransportResponse(
                status_code=200,
                body=_body_at_limit("must not be fetched"),
                headers={"content-type": "text/plain", "etag": "three-v1"},
            ),
        }
    )
    adapter = WebCrawlAdapter(
        allowed_hosts=("docs.example.test",),
        max_depth=1,
        max_pages=3,
        max_document_bytes=1024,
        max_sync_bytes=2048,
    )
    adapter._transport = transport  # type: ignore[assignment]

    result = await adapter.sync(_source(cursor=_encode_cursor({_ROOT: "root-v1"})))

    assert [call[1] for call in transport.calls] == [_ROOT, _TWO]
    assert [call[2] for call in transport.calls] == [1024, 1024]
    assert result.discovered_count == 2
    assert [document.locator for document in result.documents] == [_TWO]
    cursor = _decode_cursor(result.cursor)
    assert cursor[_ROOT] == "root-v1"
    assert cursor[_TWO] == "two-v1"


@pytest.mark.asyncio
async def test_web_sync_rejects_a_truncated_response() -> None:
    """A partial page cannot be parsed or checkpointed as complete input."""
    transport = _RecordingTransport(
        {
            _ROOT: TransportResponse(
                status_code=200,
                body=b"partial",
                headers={"content-type": "text/plain"},
                is_truncated=True,
            )
        }
    )
    adapter = WebCrawlAdapter(
        allowed_hosts=("docs.example.test",),
        max_depth=1,
        max_pages=3,
        max_document_bytes=1024,
        max_sync_bytes=1024,
    )
    adapter._transport = transport  # type: ignore[assignment]

    with pytest.raises(ModelProviderError, match="sync byte limit"):
        await adapter.sync(_source())


@pytest.mark.asyncio
async def test_object_sync_counts_listing_bytes_before_fetching_an_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An S3 listing cannot leave enough apparent budget for an oversized next GET."""
    listing = b"""<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Contents>
    <Key>docs/large.txt</Key>
    <ETag>\"etag-large\"</ETag>
    <Size>900</Size>
    <LastModified>2026-01-01T00:00:00.000Z</LastModified>
  </Contents>
  <IsTruncated>false</IsTruncated>
</ListBucketResult>"""
    calls: list[tuple[str, str, object]] = []

    async def request_signed(method: str, url: str, **kwargs: object) -> TransportResponse:
        calls.append((method, url, kwargs.get("max_response_bytes")))
        if "list-type=2" not in url:
            raise AssertionError("object GET must not run after the listing consumes the budget")
        return TransportResponse(
            status_code=200,
            body=listing,
            headers={"content-type": "application/xml"},
        )

    monkeypatch.setattr(
        sources_module,
        "resolve_secret",
        lambda reference: Secret("test-secret", reference=reference),
    )
    adapter = S3CompatibleAdapter(
        allowed_hosts=("storage.example.test",),
        max_pages=10,
        max_document_bytes=1024,
        max_sync_bytes=1024,
    )
    monkeypatch.setattr(adapter, "_request_signed", request_signed)

    result = await adapter.sync(_object_source())

    assert result.documents == ()
    assert result.discovered_count == 0
    assert len(calls) == 1
    assert calls[0][2] == 1024


def test_source_cursor_round_trips_resume_state_and_etags() -> None:
    """The v2 opaque cursor retains a bounded continuation and object checkpoint."""
    encoded = _encode_cursor(
        {"prefix/a.txt": "etag-a", "prefix/b.txt": "etag-b"},
        continuation="continuation-token",
        start_after="prefix/b.txt",
    )

    decoded = _decode_cursor(encoded)

    assert dict(decoded) == {"prefix/a.txt": "etag-a", "prefix/b.txt": "etag-b"}
    assert decoded.continuation == "continuation-token"
    assert decoded.start_after == "prefix/b.txt"


@pytest.mark.parametrize(
    "cursor",
    [
        "not-base64",
        "eyJ2IjoyLCJldGFncyI6W119",
    ],
)
def test_source_cursor_rejects_malformed_or_wrongly_shaped_state(cursor: str) -> None:
    """Untrusted cursor state fails closed before it controls source progress."""
    with pytest.raises(InvalidRequestError):
        _decode_cursor(cursor)
