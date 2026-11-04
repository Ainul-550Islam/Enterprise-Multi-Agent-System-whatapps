"""Resource-bound and checkpoint regressions for Data Hub source adapters."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime

import pytest

import mas_orchestrator.data_hub.sources as sources_module
from mas_orchestrator.config.enums import DataSourceKind
from mas_orchestrator.config.settings import DataHubObjectStorageBinding
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


def _object_binding() -> DataHubObjectStorageBinding:
    source = _object_source()
    return DataHubObjectStorageBinding(
        tenant_id=source.tenant_id,
        organization_id=source.organization_id,
        project_id=source.project_id,
        source_id=source.source_id,
        endpoint_url="https://storage.example.test",
        bucket="test-bucket",
        prefix="docs/",
        region="us-east-1",
        access_key_secret_ref="env:TEST_ACCESS_KEY",
        secret_key_secret_ref="env:TEST_SECRET_KEY",
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

    result = await adapter.sync(
        _source(
            cursor=_encode_cursor(
                {_ROOT: "root-v1"}, fingerprint=sources_module._source_fingerprint(_source())
            )
        )
    )

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
        bindings=(_object_binding(),),
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


def test_s3_sigv4_matches_independent_botocore_signature_for_escaped_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """S3 path bytes are encoded once and query spaces never become plus signs.

    The expected signature was independently generated by botocore S3SigV4Auth
    using the same fixed test credential, timestamp, request and headers.
    """

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz: object = None) -> datetime:
            return datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)

    monkeypatch.setattr(sources_module, "datetime", FixedDatetime)
    url = sources_module._s3_url(
        "https://storage.example.test",
        "test-bucket",
        [("prefix", "a b+c"), ("list-type", "2")],
        key="docs/report v2+%文.txt",
    )
    assert url == (
        "https://storage.example.test/test-bucket/docs/report%20v2%2B%25%E6%96%87.txt"
        "?list-type=2&prefix=a%20b%2Bc"
    )
    headers = sources_module._sigv4_headers(
        "GET",
        url,
        region="us-east-1",
        access_key="TESTACCESSKEY",
        secret_key="fixture-secret-value",
    )
    assert headers["authorization"].endswith(
        "Signature=f33ad187f7da74e27ab06b1c1e2346b3fef2ddd287de8ef857063d97832e9600"
    )


def test_s3_canonicalization_preserves_slashes_and_sorts_encoded_query() -> None:
    assert sources_module._s3_canonical_path("/a//b%2Fc/%25") == "/a//b%2Fc/%25"
    assert sources_module._s3_canonical_query([("z", " "), ("é", "+"), ("a", "!")]) == (
        "%C3%A9=%2B&a=%21&z=%20"
    )


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


def _s3_listing(*, etag: str = '"listed-v1"', size: int = 4) -> bytes:
    return f"""<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Contents><Key>docs/a.txt</Key><ETag>{etag}</ETag><Size>{size}</Size></Contents>
        <IsTruncated>false</IsTruncated></ListBucketResult>""".encode()


def _s3_adapter() -> S3CompatibleAdapter:
    return S3CompatibleAdapter(
        allowed_hosts=("storage.example.test",),
        bindings=(_object_binding(),),
        max_pages=10,
        max_document_bytes=1024,
        max_sync_bytes=4096,
    )


@pytest.mark.asyncio
async def test_object_fetch_pins_listed_version_and_checkpoints_only_matching_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sources_module, "resolve_secret", lambda ref: Secret("test", reference=ref))
    calls: list[dict[str, object]] = []

    async def request_signed(method: str, url: str, **kwargs: object) -> TransportResponse:
        calls.append(kwargs)
        if "list-type=2" in url:
            return TransportResponse(200, _s3_listing(), {})
        return TransportResponse(
            206, b"data", {"ETag": '"listed-v1"', "Content-Range": "bytes 0-3/4"}
        )

    adapter = _s3_adapter()
    monkeypatch.setattr(adapter, "_request_signed", request_signed)
    result = await adapter.sync(_object_source())
    assert len(result.documents) == 1
    assert result.documents[0].content == b"data"
    assert result.documents[0].etag == "listed-v1"
    assert calls[1]["extra_headers"] == {"if-match": '"listed-v1"', "range": "bytes=0-3"}
    assert _decode_cursor(result.cursor)["docs/a.txt"] == "listed-v1"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        TransportResponse(412, b"", {}),
        TransportResponse(200, b"evil", {"etag": '"changed-v2"'}),
        TransportResponse(200, b"data", {}),
        TransportResponse(206, b"data", {"etag": '"listed-v1"'}),
        TransportResponse(206, b"data", {"etag": '"listed-v1"', "content-range": "bytes 0-2/4"}),
    ],
)
async def test_object_fetch_rejects_stale_or_ambiguous_provider_responses(
    monkeypatch: pytest.MonkeyPatch, response: TransportResponse
) -> None:
    monkeypatch.setattr(sources_module, "resolve_secret", lambda ref: Secret("test", reference=ref))

    async def request_signed(method: str, url: str, **kwargs: object) -> TransportResponse:
        return TransportResponse(200, _s3_listing(), {}) if "list-type=2" in url else response

    adapter = _s3_adapter()
    monkeypatch.setattr(adapter, "_request_signed", request_signed)
    with pytest.raises(ModelProviderError):
        await adapter.sync(_object_source())


@pytest.mark.asyncio
async def test_empty_s3_object_is_not_sent_an_unsatisfiable_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sources_module, "resolve_secret", lambda ref: Secret("test", reference=ref))

    async def request_signed(method: str, url: str, **kwargs: object) -> TransportResponse:
        if "list-type=2" in url:
            return TransportResponse(200, _s3_listing(size=0), {})
        raise AssertionError("empty objects must not be fetched or checkpointed")

    adapter = _s3_adapter()
    monkeypatch.setattr(adapter, "_request_signed", request_signed)
    with pytest.raises(ModelProviderError, match="object is empty"):
        await adapter.sync(_object_source())


@pytest.mark.parametrize(
    "value", ["", 'W/"abc"', '"abc', 'abc"', "abc\nfoo", "abc def", "a\\b", "x" * 513]
)
def test_s3_rejects_unsafe_etag_header_values(value: str) -> None:
    with pytest.raises(ModelProviderError):
        sources_module._s3_etag(value)


def test_s3_conditional_range_headers_are_in_sigv4_signature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Expected signature independently generated with botocore S3SigV4Auth."""

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz: object = None) -> datetime:
            return datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)

    monkeypatch.setattr(sources_module, "datetime", FixedDatetime)
    headers = sources_module._sigv4_headers(
        "GET",
        "https://storage.example.test/test-bucket/docs/a.txt",
        region="us-east-1",
        access_key="TESTACCESSKEY",
        secret_key="fixture-secret-value",
        extra_headers={"if-match": '"listed-v1"', "range": "bytes=0-3"},
    )
    assert (
        "SignedHeaders=host;if-match;range;x-amz-content-sha256;x-amz-date"
        in headers["authorization"]
    )
    assert headers["authorization"].endswith(
        "Signature=72d09bd345c97233817dca5367bfe06159443bd80599dca218ade4fef9f35f49"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "metadata",
    [
        "",
        "<IsTruncated>maybe</IsTruncated>",
        "<IsTruncated>true</IsTruncated>",
        "<IsTruncated>true</IsTruncated><NextContinuationToken>"
        + "a" * 4097
        + "</NextContinuationToken>",
        "<IsTruncated>false</IsTruncated><NextContinuationToken>unexpected</NextContinuationToken>",
    ],
)
async def test_s3_rejects_ambiguous_listing_pagination(
    monkeypatch: pytest.MonkeyPatch, metadata: str
) -> None:
    monkeypatch.setattr(sources_module, "resolve_secret", lambda ref: Secret("test", reference=ref))
    listing = f"<ListBucketResult>{metadata}</ListBucketResult>".encode()

    async def request_signed(method: str, url: str, **kwargs: object) -> TransportResponse:
        return TransportResponse(200, listing, {})

    adapter = S3CompatibleAdapter(
        allowed_hosts=("storage.example.test",),
        bindings=(_object_binding(),),
        max_pages=10,
        max_document_bytes=8192,
        max_sync_bytes=16384,
    )
    monkeypatch.setattr(adapter, "_request_signed", request_signed)
    with pytest.raises(ModelProviderError):
        await adapter.sync(_object_source())


@pytest.mark.asyncio
async def test_s3_rejects_repeated_continuation_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sources_module, "resolve_secret", lambda ref: Secret("test", reference=ref))
    calls = 0

    async def request_signed(method: str, url: str, **kwargs: object) -> TransportResponse:
        nonlocal calls
        calls += 1
        return TransportResponse(
            200,
            b"<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>same</NextContinuationToken></ListBucketResult>",
            {},
        )

    adapter = _s3_adapter()
    monkeypatch.setattr(adapter, "_request_signed", request_signed)
    with pytest.raises(ModelProviderError, match="invalid continuation"):
        await adapter.sync(_object_source())
    assert calls == 2


@pytest.mark.parametrize("field", ["continuation", "start_after"])
def test_source_cursor_never_silently_truncates_resume_values(field: str) -> None:
    with pytest.raises(InvalidRequestError):
        _encode_cursor({}, **{field: "x" * 5000})


@pytest.mark.asyncio
async def test_web_policy_change_reindexes_same_etag_and_preserves_retry_checkpoint() -> None:
    source = replace(_source(), required_roles=frozenset({"reader"}))
    transport = _RecordingTransport(
        {
            _ROOT: TransportResponse(
                200, b"confidential", {"content-type": "text/plain", "etag": "unchanged"}
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
    first = await adapter.sync(source)
    assert len(first.documents) == 1
    same = await adapter.sync(replace(source, cursor=first.cursor))
    assert not same.documents
    tightened = replace(
        source,
        cursor=first.cursor,
        required_roles=frozenset({"admin"}),
        denied_principals=frozenset({"revoked-user"}),
    )
    changed = await adapter.sync(tightened)
    assert len(changed.documents) == 1
    assert _decode_cursor(changed.cursor).fingerprint == sources_module._source_fingerprint(
        tightened
    )
    # Failure at indexing must resume with the new policy and without the
    # failed object's ETag marked complete.
    retry = await adapter.sync(replace(tightened, cursor=changed.documents[0].cursor_before))
    assert len(retry.documents) == 1
    assert not (await adapter.sync(replace(tightened, cursor=changed.cursor))).documents


@pytest.mark.asyncio
async def test_s3_policy_change_discards_etags_and_old_resume_position(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sources_module, "resolve_secret", lambda ref: Secret("test", reference=ref))
    source = _object_source()
    fingerprint = sources_module._source_fingerprint(source)
    old_cursor = _encode_cursor(
        {"docs/a.txt": "listed-v1"}, start_after="docs/z.txt", fingerprint=fingerprint
    )
    changed = replace(source, cursor=old_cursor, required_roles=frozenset({"admin"}))
    calls: list[str] = []

    async def request_signed(method: str, url: str, **kwargs: object) -> TransportResponse:
        calls.append(url)
        if "list-type=2" in url:
            return TransportResponse(200, _s3_listing(), {})
        return TransportResponse(200, b"data", {"etag": '"listed-v1"'})

    adapter = _s3_adapter()
    monkeypatch.setattr(adapter, "_request_signed", request_signed)
    result = await adapter.sync(changed)
    assert "start-after" not in calls[0]
    assert len(result.documents) == 1
    assert _decode_cursor(result.cursor).fingerprint == sources_module._source_fingerprint(changed)


def test_legacy_cursor_is_decoded_but_never_trusted_for_a_new_snapshot() -> None:
    import base64
    import json

    old = (
        base64.urlsafe_b64encode(
            json.dumps({"v": 2, "etags": {"x": "old"}, "start_after": "x"}).encode()
        )
        .decode()
        .rstrip("=")
    )
    assert _decode_cursor(old).start_after == "x"
    reset = sources_module._source_cursor(old, sources_module._source_fingerprint(_source()))
    assert not reset and reset.start_after is None


def test_source_fingerprint_is_canonical_for_role_order_and_sensitive_to_scope() -> None:
    first = replace(_source(), required_roles=frozenset({"a", "b"}))
    assert sources_module._source_fingerprint(first) == sources_module._source_fingerprint(
        replace(first, required_roles=frozenset({"b", "a"}))
    )
    assert sources_module._source_fingerprint(first) != sources_module._source_fingerprint(
        replace(first, project_id="00000000-0000-4000-8000-000000000099")
    )


@pytest.mark.asyncio
async def test_oversized_s3_replacement_fails_instead_of_checkpointing_stale_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sources_module, "resolve_secret", lambda ref: Secret("test", reference=ref))
    source = _object_source()
    old_cursor = _encode_cursor(
        {"docs/a.txt": "previous-version"}, fingerprint=sources_module._source_fingerprint(source)
    )
    calls = []

    async def request_signed(method: str, url: str, **kwargs: object) -> TransportResponse:
        calls.append(url)
        assert "list-type=2" in url, "oversized object must not be downloaded"
        return TransportResponse(200, _s3_listing(size=1025), {})

    adapter = _s3_adapter()
    monkeypatch.setattr(adapter, "_request_signed", request_signed)
    with pytest.raises(ModelProviderError, match="exceeds the configured document byte limit"):
        await adapter.sync(replace(source, cursor=old_cursor))
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "key",
    [
        "docs/../secret.txt",
        "docs/./file.txt",
        "docs/dir/../../secret.txt",
        "docs/a\\..\\secret.txt",
        "other/file.txt",
        "",
    ],
)
async def test_s3_unsafe_listing_keys_fail_before_any_object_get_or_checkpoint(
    monkeypatch: pytest.MonkeyPatch, key: str
) -> None:
    monkeypatch.setattr(sources_module, "resolve_secret", lambda ref: Secret("test", reference=ref))
    listing = f"""<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Contents><Key>{key}</Key><ETag>"listed-v1"</ETag><Size>4</Size></Contents>
        <IsTruncated>false</IsTruncated></ListBucketResult>""".encode()
    calls: list[str] = []

    async def request_signed(method: str, url: str, **kwargs: object) -> TransportResponse:
        calls.append(url)
        assert "list-type=2" in url, "unsafe listing key must not result in an object GET"
        return TransportResponse(200, listing, {})

    adapter = _s3_adapter()
    monkeypatch.setattr(adapter, "_request_signed", request_signed)
    with pytest.raises(ModelProviderError, match="unsafe key"):
        await adapter.sync(_object_source())
    assert len(calls) == 1


def test_s3_http_client_normalizes_dot_segments_which_can_escape_prefix() -> None:
    import httpx

    url = sources_module._s3_url(
        "https://storage.example.test", "test-bucket", (), key="docs/../secret.txt"
    )
    assert str(httpx.URL(url)) == "https://storage.example.test/test-bucket/secret.txt"


def test_large_etag_cache_is_trimmed_without_losing_resume_or_fingerprint() -> None:
    entries = {f"docs/{i:05d}-" + "x" * 150: f"etag-{i}-" + "y" * 100 for i in range(1000)}
    encoded = _encode_cursor(
        entries, continuation="token", start_after="docs/latest", fingerprint="a" * 64
    )
    assert len(encoded) <= 64 * 1024
    decoded = _decode_cursor(encoded)
    assert 0 < len(decoded) < len(entries)
    assert decoded.continuation == "token"
    assert decoded.start_after == "docs/latest"
    assert decoded.fingerprint == "a" * 64
    assert all(entries[key] == value for key, value in decoded.items())


def test_huge_etag_entry_is_dropped_not_truncated() -> None:
    huge = "z" * 60000
    encoded = _encode_cursor({"valid": "one", huge: "two"}, fingerprint="b" * 64)
    assert _decode_cursor(encoded) == {"valid": "one"}


def _multi_s3_listing(keys: list[str], *, continuation: str | None = None) -> bytes:
    objects = "".join(
        f'<Contents><Key>{key}</Key><ETag>"listed-v1"</ETag><Size>4</Size></Contents>'
        for key in keys
    )
    metadata = (
        f"<IsTruncated>true</IsTruncated><NextContinuationToken>{continuation}</NextContinuationToken>"
        if continuation
        else "<IsTruncated>false</IsTruncated>"
    )
    return f"<ListBucketResult>{objects}{metadata}</ListBucketResult>".encode()


@pytest.mark.asyncio
@pytest.mark.parametrize("keys", [["docs/b.txt", "docs/a.txt"], ["docs/a.txt", "docs/a.txt"]])
async def test_s3_unsorted_or_duplicate_listing_fails_before_checkpoint(
    monkeypatch: pytest.MonkeyPatch, keys: list[str]
) -> None:
    monkeypatch.setattr(sources_module, "resolve_secret", lambda ref: Secret("test", reference=ref))

    async def request_signed(method: str, url: str, **kwargs: object) -> TransportResponse:
        if "list-type=2" in url:
            return TransportResponse(200, _multi_s3_listing(keys), {})
        return TransportResponse(200, b"data", {"etag": '"listed-v1"'})

    adapter = _s3_adapter()
    monkeypatch.setattr(adapter, "_request_signed", request_signed)
    with pytest.raises(ModelProviderError, match="not strictly ordered"):
        await adapter.sync(_object_source())


@pytest.mark.asyncio
async def test_s3_cursor_retains_order_boundary_across_continuation_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sources_module, "resolve_secret", lambda ref: Secret("test", reference=ref))
    first_listing = _multi_s3_listing(["docs/b.txt"], continuation="page-two")
    first_listing += b" " * (1020 - len(first_listing))
    assert len(first_listing) == 1020
    calls: list[str] = []

    async def request_signed(method: str, url: str, **kwargs: object) -> TransportResponse:
        calls.append(url)
        if "continuation-token=page-two" in url:
            return TransportResponse(200, _multi_s3_listing(["docs/a.txt"]), {})
        if "list-type=2" in url:
            return TransportResponse(200, first_listing, {})
        return TransportResponse(200, b"data", {"etag": '"listed-v1"'})

    adapter = S3CompatibleAdapter(
        allowed_hosts=("storage.example.test",),
        bindings=(_object_binding(),),
        max_pages=3,
        max_document_bytes=1024,
        max_sync_bytes=1024,
    )
    monkeypatch.setattr(adapter, "_request_signed", request_signed)
    first = await adapter.sync(_object_source())
    cursor = _decode_cursor(first.cursor)
    assert cursor.continuation == "page-two"
    assert cursor.start_after == "docs/b.txt"
    with pytest.raises(ModelProviderError, match="not strictly ordered"):
        await adapter.sync(replace(_object_source(), cursor=first.cursor))
    assert "continuation-token=page-two" in calls[-1]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"tenant_id": "00000000-0000-4000-8000-000000000099"},
        {"organization_id": "00000000-0000-4000-8000-000000000099"},
        {"project_id": "00000000-0000-4000-8000-000000000099"},
        {"source_id": "00000000-0000-4000-8000-000000000099"},
        {"config": {**_object_source().config, "endpoint_url": "https://other.example.test"}},
        {"config": {**_object_source().config, "bucket": "other-bucket"}},
        {"config": {**_object_source().config, "prefix": "other/"}},
        {"config": {**_object_source().config, "region": "eu-west-1"}},
        {
            "config": {
                **_object_source().config,
                "access_key_secret_ref": "env:MAS_ORCH_INTERNAL_SECRET",
            }
        },
        {"config": {**_object_source().config, "secret_key_secret_ref": "file:/etc/passwd"}},
    ],
)
async def test_object_source_cannot_resolve_or_send_unbound_secrets(
    monkeypatch: pytest.MonkeyPatch, change: dict[str, object]
) -> None:
    from mas_orchestrator.contracts.errors import ToolPermissionDeniedError

    resolves: list[str] = []

    def resolve(reference: str) -> Secret:
        resolves.append(reference)
        return Secret("must-not-be-read", reference=reference)

    adapter = S3CompatibleAdapter(
        allowed_hosts=("storage.example.test", "other.example.test"),
        bindings=(_object_binding(),),
        max_pages=10,
        max_document_bytes=1024,
        max_sync_bytes=4096,
    )
    monkeypatch.setattr(sources_module, "resolve_secret", resolve)
    try:
        with pytest.raises(ToolPermissionDeniedError, match="credential binding"):
            await adapter.sync(replace(_object_source(), **change))
        assert resolves == []
    finally:
        await adapter.aclose()


@pytest.mark.asyncio
async def test_object_source_has_no_implicit_credential_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mas_orchestrator.contracts.errors import ToolPermissionDeniedError

    def forbidden_resolve(reference: str) -> Secret:
        raise AssertionError("no binding must be denied before secret resolution")

    monkeypatch.setattr(sources_module, "resolve_secret", forbidden_resolve)
    adapter = S3CompatibleAdapter(
        allowed_hosts=("storage.example.test",),
        max_pages=10,
        max_document_bytes=1024,
        max_sync_bytes=4096,
    )
    try:
        with pytest.raises(ToolPermissionDeniedError, match="credential binding"):
            await adapter.sync(_object_source())
    finally:
        await adapter.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "root",
    [
        "https://docs.example.test/root?token=abc",
        "https://docs.example.test/root?",
        "https://docs.example.test/root#",
        "https://docs.example.test/\nsecret",
        "https://docs.example.test/root#secret",
        "https://user:pass@docs.example.test/root",
    ],
)
async def test_web_source_root_cannot_embed_credentials_or_query(root: str) -> None:
    from mas_orchestrator.contracts.errors import ToolPermissionDeniedError

    adapter = WebCrawlAdapter(
        allowed_hosts=("docs.example.test",),
        max_depth=1,
        max_pages=3,
        max_document_bytes=1024,
        max_sync_bytes=1024,
    )
    try:
        with pytest.raises((InvalidRequestError, ToolPermissionDeniedError)):
            await adapter.sync(replace(_source(), config={"url": root}))
    finally:
        await adapter.aclose()


@pytest.mark.asyncio
async def test_web_crawl_skips_untrusted_bad_links_without_aborting_sync() -> None:
    body = b"""<a href="https://unlisted.example.test/">external</a>
    <a href="https://elsewhere.example.test/">other allowed host</a>
    <a href="http://docs.example.test/plain">insecure</a>
    <a href="https://user:pass@docs.example.test/">userinfo</a>
    <a href="https://docs.example.test:invalid/">invalid port</a>
    <a href="https://[broken/">invalid host</a>
    <a href="/bad\tpath">control</a>
    <a href="/bad\\path">backslash</a>
    <a href="/two">valid</a>"""
    transport = _RecordingTransport(
        {
            _ROOT: TransportResponse(
                status_code=200, body=body, headers={"content-type": "text/html"}
            ),
            _TWO: TransportResponse(
                status_code=200, body=b"valid", headers={"content-type": "text/plain"}
            ),
        }
    )
    adapter = WebCrawlAdapter(
        allowed_hosts=("docs.example.test", "elsewhere.example.test"),
        max_depth=1,
        max_pages=3,
        max_document_bytes=1024,
        max_sync_bytes=2048,
    )
    adapter._transport = transport  # type: ignore[assignment]
    result = await adapter.sync(_source())
    assert [call[1] for call in transport.calls] == [_ROOT, _TWO]
    assert [document.locator for document in result.documents] == [_ROOT, _TWO]


def test_discovered_url_length_is_bounded_after_resolution() -> None:
    assert (
        sources_module._validate_link(
            "/" + "x" * 2030, _ROOT, "docs.example.test", frozenset({"docs.example.test"})
        )
        is None
    )
