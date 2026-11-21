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


def _object_source(*, cursor: str | None = None) -> DataHubSource:
    """Builds a valid object-storage snapshot with secret references only."""
    return DataHubSource(
        source_id="00000000-0000-4000-8000-000000000011",
        tenant_id="00000000-0000-4000-8000-000000000012",
        organization_id="00000000-0000-4000-8000-000000000013",
        project_id="00000000-0000-4000-8000-000000000014",
        name="test object storage",
        kind=DataSourceKind.OBJECT_STORAGE,
        cursor=cursor,
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


def _listing(*entries: tuple[str, str, int], truncated: bool = False) -> bytes:
    """Builds a ListBucketResult page for the fake signed transport."""
    contents = "".join(
        f"""  <Contents>
    <Key>{key}</Key>
    <ETag>{etag}</ETag>
    <Size>{size}</Size>
    <LastModified>2026-01-01T00:00:00.000Z</LastModified>
  </Contents>
"""
        for key, etag, size in entries
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
{contents}  <IsTruncated>{"true" if truncated else "false"}</IsTruncated>
</ListBucketResult>""".encode()


@pytest.mark.asyncio
async def test_object_sync_resumes_after_the_checkpointed_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C-11: unchanged leading keys cannot starve later keys forever.

    A prefix with more unchanged objects than ``max_pages`` must advance the
    persisted checkpoint past them, so the next sync reaches the changed key
    behind them instead of restarting lexically at the same prefix.
    """
    monkeypatch.setattr(
        sources_module,
        "resolve_secret",
        lambda reference: Secret("test-secret", reference=reference),
    )
    object_gets: list[str] = []

    async def request_signed(method: str, url: str, **kwargs: object) -> TransportResponse:
        if "list-type=2" in url:
            if "start-after" in url:
                body = _listing(("docs/c.txt", '"etag-c"', 8))
            else:
                body = _listing(("docs/a.txt", '"etag-a"', 8), ("docs/b.txt", '"etag-b"', 8))
            return TransportResponse(status_code=200, body=body, headers={})
        object_gets.append(url)
        return TransportResponse(status_code=200, body=b"c" * 8, headers={})

    adapter = S3CompatibleAdapter(
        allowed_hosts=("storage.example.test",),
        max_pages=2,
        max_document_bytes=1024,
        max_sync_bytes=4096,
    )
    monkeypatch.setattr(adapter, "_request_signed", request_signed)

    first = await adapter.sync(_object_source())
    assert [document.title for document in first.documents] == ["a.txt", "b.txt"]
    first_cursor = _decode_cursor(first.cursor)
    assert first_cursor.start_after == "docs/b.txt"

    second = await adapter.sync(_object_source(cursor=first.cursor))
    assert [document.title for document in second.documents] == ["c.txt"]
    # The changed key behind the unchanged prefix was reached without a
    # single lexical restart: no earlier object was refetched.
    assert len(object_gets) == 3
    second_cursor = _decode_cursor(second.cursor)
    assert second_cursor["docs/c.txt"] == "etag-c"


@pytest.mark.asyncio
async def test_object_sync_admits_the_aggregate_budget_before_downloading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C-11 regression: three changed objects cannot exceed the sync budget.

    The report's scenario (three 32 MiB objects against a 64 MiB aggregate
    cap) is scale-invariant byte accounting; this runs it at KiB scale. The
    third object must not be downloaded at all once its size would push the
    sync over the cap, and the checkpoint must end at the last retained
    object.
    """
    monkeypatch.setattr(
        sources_module,
        "resolve_secret",
        lambda reference: Secret("test-secret", reference=reference),
    )
    object_gets: list[str] = []

    async def request_signed(method: str, url: str, **kwargs: object) -> TransportResponse:
        if "list-type=2" in url:
            body = _listing(
                ("docs/o1.txt", '"etag-1"', 512),
                ("docs/o2.txt", '"etag-2"', 512),
                ("docs/o3.txt", '"etag-3"', 512),
            )
            return TransportResponse(status_code=200, body=body, headers={})
        object_gets.append(url)
        return TransportResponse(status_code=200, body=b"x" * 512, headers={})

    adapter = S3CompatibleAdapter(
        allowed_hosts=("storage.example.test",),
        max_pages=10,
        max_document_bytes=1024,
        max_sync_bytes=2048,
    )
    monkeypatch.setattr(adapter, "_request_signed", request_signed)

    result = await adapter.sync(_object_source())

    # The third object would exceed the aggregate budget, so it is never
    # requested: admission happens before download, not after collection.
    assert [document.title for document in result.documents] == ["o1.txt", "o2.txt"]
    assert len(object_gets) == 2
    assert all("o3" not in url for url in object_gets)
    retained = sum(len(document.content) for document in result.documents)
    assert retained <= 2048
    # Progress checkpoint: the cursor ends at the last retained object, so
    # the next sync resumes at the refused key rather than restarting.
    cursor = _decode_cursor(result.cursor)
    assert cursor.start_after == "docs/o2.txt"


@pytest.mark.asyncio
async def test_web_sync_tolerates_a_hostile_link_farm_within_its_bounds() -> None:
    """W-05: duplicates, junk schemes and oversized URLs cannot break the bounds."""
    dup = "https://docs.example.test/dup"
    oversized = "https://docs.example.test/" + "x" * 3000
    farm = "".join(
        [
            '<a href="http://docs.example.test/junk">junk</a>',
            f'<a href="{oversized}">long</a>',
            *(f'<a href="{dup}">dup</a>' for _ in range(20)),
            *(f'<a href="/p{i}">p</a>' for i in range(40)),
        ]
    )
    transport = _RecordingTransport(
        {
            _ROOT: TransportResponse(
                status_code=200,
                body=_body_at_limit(farm, 8192),
                headers={"content-type": "text/html", "etag": "root-v1"},
            ),
            dup: TransportResponse(
                status_code=200,
                body=_body_at_limit("duplicate target", 1024),
                headers={"content-type": "text/plain", "etag": "dup-v1"},
            ),
            "https://docs.example.test/p0": TransportResponse(
                status_code=200,
                body=_body_at_limit("first unique page", 1024),
                headers={"content-type": "text/plain", "etag": "p0-v1"},
            ),
        }
    )
    adapter = WebCrawlAdapter(
        allowed_hosts=("docs.example.test",),
        max_depth=1,
        max_pages=3,
        max_document_bytes=8192,
        max_sync_bytes=16384,
    )
    adapter._transport = transport  # type: ignore[assignment]

    result = await adapter.sync(_source())

    # The transport raises on any unscripted URL: a fetch of a junk scheme,
    # an oversized URL or any second copy of the duplicate fails the test.
    urls = [call[1] for call in transport.calls]
    assert urls == [_ROOT, dup, "https://docs.example.test/p0"]
    assert len(set(urls)) == len(urls)
    assert result.discovered_count == 3


@pytest.mark.asyncio
async def test_web_sync_keeps_link_farm_work_bounded_relative_to_max_pages() -> None:
    """The frontier and per-page fanout caps bound work by max_pages, not farm size."""
    validated: list[str] = []
    real_validate_link = sources_module._validate_link

    def counting_validate_link(
        raw: str,
        base_url: str,
        root_host: str,
        allowed_hosts: frozenset[str],
    ) -> str | None:
        validated.append(raw)
        return real_validate_link(raw, base_url, root_host, allowed_hosts)

    sources_module._validate_link = counting_validate_link  # type: ignore[assignment]
    try:
        counts: list[int] = []
        for farm_size in (24, 240):
            validated.clear()
            farm = "".join(f'<a href="/p{i}">p</a>' for i in range(farm_size))
            transport = _RecordingTransport(
                {
                    _ROOT: TransportResponse(
                        status_code=200,
                        body=_body_at_limit(farm, 8192),
                        headers={"content-type": "text/html", "etag": "root-v1"},
                    ),
                    **{
                        f"https://docs.example.test/p{i}": TransportResponse(
                            status_code=200,
                            body=_body_at_limit(f"page {i}", 1024),
                            headers={"content-type": "text/plain", "etag": f"p{i}-v1"},
                        )
                        for i in range(3)
                    },
                }
            )
            adapter = WebCrawlAdapter(
                allowed_hosts=("docs.example.test",),
                max_depth=1,
                max_pages=3,
                max_document_bytes=8192,
                max_sync_bytes=16384,
            )
            adapter._transport = transport  # type: ignore[assignment]

            await adapter.sync(_source())
            counts.append(len(validated))

        # A tenfold larger farm schedules exactly the same bounded work:
        # the per-page fanout cap (max_pages * 8) binds before the farm does.
        assert counts[0] == counts[1]
        assert counts[1] <= 3 * 8
    finally:
        sources_module._validate_link = real_validate_link  # type: ignore[assignment]


def test_web_links_drop_oversized_and_hostile_targets_individually() -> None:
    """URL-length and scheme limits drop bad links instead of failing the sync."""
    host = "docs.example.test"
    allow = frozenset({host})
    oversized = f"https://{host}/" + "x" * 3000
    assert sources_module._validate_link(oversized, _ROOT, host, allow) is None
    assert sources_module._validate_link("http://" + host + "/x", _ROOT, host, allow) is None
    assert sources_module._validate_link("https://evil.example.test/x", _ROOT, host, allow) is None
    assert (
        sources_module._validate_link("/ok", _ROOT, host, allow) == "https://docs.example.test/ok"
    )
