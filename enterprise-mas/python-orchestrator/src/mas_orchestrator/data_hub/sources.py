"""Bounded web and S3-compatible source adapters for Data Hub synchronization.

All network reads use :class:`HttpTransport`, which enforces HTTPS, exact
host allow-lists, DNS-address blocking, redirect revalidation, timeouts, and a
hard response-byte ceiling. S3 credentials are resolved from secret-reference
fields immediately before signing requests; raw values are never placed in
source configuration, cursor state, logs, or returned records.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import re
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from typing import TYPE_CHECKING, Final
from urllib.parse import parse_qsl, quote, urldefrag, urlencode, urljoin, urlsplit, urlunsplit

from defusedxml import ElementTree

from mas_orchestrator.config.enums import DataSourceKind
from mas_orchestrator.contracts.errors import (
    ConfigurationError,
    InvalidRequestError,
    ModelProviderError,
    ToolPermissionDeniedError,
)
from mas_orchestrator.data_hub.models import DataHubSource, DiscoveredDocument
from mas_orchestrator.providers.base_http import (
    HttpPolicy,
    HttpTransport,
    TransportResponse,
    classify_status,
)
from mas_orchestrator.security.credentials import resolve_secret

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from xml.etree.ElementTree import Element

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)
_DEFAULT_REGION: Final[str] = "us-east-1"
_S3_DATE_FORMAT: Final[str] = "%Y%m%dT%H%M%SZ"
_CURSOR_VERSION: Final[int] = 1
_MAX_CURSOR_ENTRIES: Final[int] = 10_000
_MAX_START_URLS: Final[int] = 25


@dataclass(frozen=True, slots=True)
class SourceSyncResult:
    """Changed source content and the opaque next cursor to persist in Rust."""

    documents: tuple[DiscoveredDocument, ...]
    cursor: str
    discovered_count: int


class _LinkCollector(HTMLParser):
    """Collects visible page title and bounded anchor targets from HTML."""

    def __init__(self, *, max_links: int) -> None:
        super().__init__(convert_charrefs=True)
        self.title_parts: list[str] = []
        self.links: list[str] = []
        self._in_title = False
        self._max_links = max_links

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "title":
            self._in_title = True
        if tag.lower() == "a" and len(self.links) < self._max_links:
            for name, value in attrs:
                if name.lower() == "href" and value:
                    self.links.append(value.strip())
                    break

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._in_title and len(self.title_parts) < 64:
            self.title_parts.append(data[:512])

    @property
    def title(self) -> str:
        return " ".join(part.strip() for part in self.title_parts if part.strip())[:512]


class WebCrawlAdapter:
    """Same-host HTTPS crawler with depth/page/byte bounds and ETag cursors."""

    def __init__(
        self,
        *,
        allowed_hosts: Sequence[str],
        max_depth: int,
        max_pages: int,
        max_document_bytes: int,
    ) -> None:
        hosts = frozenset(
            host.strip().rstrip(".").lower() for host in allowed_hosts if host.strip()
        )
        if not hosts:
            raise ConfigurationError("web crawler requires a non-empty egress allow-list")
        if max_depth < 0 or not 1 <= max_pages <= 10_000:
            raise ConfigurationError("web crawl bounds are invalid")
        self._allowed_hosts = hosts
        self._max_depth = max_depth
        self._max_pages = max_pages
        self._max_document_bytes = max_document_bytes
        self._transport = HttpTransport(
            HttpPolicy(
                allowed_hosts=hosts,
                max_response_bytes=max_document_bytes,
                max_redirects=3,
                require_https=True,
            ),
            connect_timeout_seconds=5.0,
            read_timeout_seconds=20.0,
            write_timeout_seconds=10.0,
        )

    async def aclose(self) -> None:
        """Closes the outbound HTTP pool."""
        await self._transport.aclose()

    async def sync(self, source: DataHubSource) -> SourceSyncResult:
        """Fetches approved pages and returns only changed content."""
        if source.kind is not DataSourceKind.WEB:
            raise InvalidRequestError("web crawler received a non-web source")
        roots = _start_urls(source.config)
        if len(roots) > _MAX_START_URLS:
            raise InvalidRequestError(
                "web source has too many start URLs",
                details={"limit": _MAX_START_URLS},
            )
        max_depth = _bounded_int(
            source.config.get("max_depth"), self._max_depth, 0, self._max_depth
        )
        max_pages = _bounded_int(
            source.config.get("max_pages"), self._max_pages, 1, self._max_pages
        )
        previous = _decode_cursor(source.cursor)
        next_etags: dict[str, str] = {}
        queue: deque[tuple[str, int, str]] = deque()
        for root in roots:
            canonical = _validate_https_url(root, self._allowed_hosts)
            queue.append((canonical, 0, urlsplit(canonical).hostname or ""))

        visited: set[str] = set()
        changed: list[DiscoveredDocument] = []
        discovered = 0
        while queue and len(visited) < max_pages:
            url, depth, root_host = queue.popleft()
            if url in visited:
                continue
            visited.add(url)
            response = await self._transport.request("GET", url)
            if not 200 <= response.status_code < 300:
                raise classify_status(response.status_code)
            if response.is_truncated:
                raise ModelProviderError(
                    "web source page exceeded the configured response limit",
                    details={"limit_bytes": self._max_document_bytes},
                )
            mime_type = _content_type(response.headers.get("content-type", ""))
            if mime_type not in {"text/html", "text/plain", "text/markdown", "application/json"}:
                continue
            discovered += 1
            etag = _header(response.headers, "etag")
            if etag:
                next_etags[url] = etag[:512]
            if not etag or previous.get(url) != etag:
                title = _header(response.headers, "x-document-title") or _title_from_html(
                    response.body
                )
                if not title:
                    title = urlsplit(url).path.rstrip("/").rsplit("/", 1)[-1] or source.name
                changed.append(
                    DiscoveredDocument(
                        locator=url,
                        title=title[:512],
                        mime_type=mime_type,
                        content=response.body,
                        etag=etag,
                        last_modified=_http_date(_header(response.headers, "last-modified")),
                    )
                )

            if depth < max_depth and mime_type == "text/html":
                collector = _collect_links(response.body, max_links=max_pages * 8)
                for target in collector.links:
                    absolute = _validate_link(target, url, root_host, self._allowed_hosts)
                    if absolute is not None and absolute not in visited:
                        queue.append((absolute, depth + 1, root_host))

        cursor = _encode_cursor(next_etags)
        return SourceSyncResult(tuple(changed), cursor, discovered)


def _collect_links(body: bytes, *, max_links: int) -> _LinkCollector:
    """Parses HTML links with a hard per-page fanout bound."""
    collector = _LinkCollector(max_links=max_links)
    try:
        collector.feed(body.decode("utf-8-sig", errors="strict"))
    except UnicodeDecodeError:
        return _LinkCollector(max_links=max_links)
    return collector


def _title_from_html(body: bytes) -> str:
    """Returns the HTML title without retaining other page content."""
    return _collect_links(body, max_links=0).title


def _validate_link(
    raw: str,
    base_url: str,
    root_host: str,
    allowed_hosts: frozenset[str],
) -> str | None:
    """Resolves one crawl target, staying on its configured origin host."""
    candidate = raw.strip()
    if not candidate or candidate.startswith(("#", "mailto:", "tel:", "javascript:", "data:")):
        return None
    absolute = urljoin(base_url, candidate)
    canonical = _validate_https_url(absolute, allowed_hosts)
    if (urlsplit(canonical).hostname or "").lower() != root_host.lower():
        return None
    return canonical


def _validate_https_url(url: str, allowed_hosts: frozenset[str]) -> str:
    """Rejects URL tricks before the guarded transport performs DNS checks."""
    candidate, _fragment = urldefrag(url.strip())
    parsed = urlsplit(candidate)
    if parsed.scheme.lower() != "https" or not parsed.hostname:
        raise ToolPermissionDeniedError("web source URL must use HTTPS")
    if parsed.username is not None or parsed.password is not None:
        raise ToolPermissionDeniedError("web source URL must not contain user credentials")
    try:
        port = parsed.port
    except ValueError as error:
        raise InvalidRequestError("web source URL has an invalid port") from error
    if port not in (None, 443):
        raise ToolPermissionDeniedError("web source URL uses a non-standard port")
    host = parsed.hostname.rstrip(".").lower()
    if host not in allowed_hosts:
        raise ToolPermissionDeniedError(
            "web source hostname is not in the configured egress allow-list",
            details={"reason": "host_not_allowlisted"},
        )
    path = parsed.path or "/"
    return urlunsplit(("https", host, path, parsed.query, ""))


def _start_urls(config: Mapping[str, object]) -> tuple[str, ...]:
    """Reads one URL or a bounded list from the Rust-authorized source config."""
    raw = config.get("start_urls")
    if raw is None:
        raw = config.get("url")
    if isinstance(raw, str):
        values = (raw,)
    elif isinstance(raw, list) and all(isinstance(value, str) for value in raw):
        values = tuple(raw)
    else:
        raise InvalidRequestError(
            "web source requires a url or start_urls list",
            details={"field": "config.url"},
        )
    if not values or any(not value.strip() for value in values):
        raise InvalidRequestError("web source has no usable start URL")
    return values


class S3CompatibleAdapter:
    """S3-compatible prefix sync with ETag change detection and range reads."""

    def __init__(
        self,
        *,
        allowed_hosts: Sequence[str],
        max_pages: int,
        max_document_bytes: int,
    ) -> None:
        hosts = frozenset(
            host.strip().rstrip(".").lower() for host in allowed_hosts if host.strip()
        )
        if not hosts:
            raise ConfigurationError("object storage requires a non-empty egress allow-list")
        self._allowed_hosts = hosts
        self._max_pages = max_pages
        self._max_document_bytes = max_document_bytes
        self._transport = HttpTransport(
            HttpPolicy(
                allowed_hosts=hosts,
                max_response_bytes=max_document_bytes,
                max_redirects=0,
                require_https=True,
            ),
            connect_timeout_seconds=5.0,
            read_timeout_seconds=30.0,
            write_timeout_seconds=10.0,
        )

    async def aclose(self) -> None:
        """Closes the outbound HTTP pool."""
        await self._transport.aclose()

    async def sync(self, source: DataHubSource) -> SourceSyncResult:
        """Lists changed objects under an approved prefix and reads them safely."""
        if source.kind is not DataSourceKind.OBJECT_STORAGE:
            raise InvalidRequestError("S3 adapter received a non-object-storage source")
        endpoint, bucket, prefix, region = _s3_config(source.config, self._allowed_hosts)
        access_ref = _required_config_string(source.config, "access_key_secret_ref")
        secret_ref = _required_config_string(source.config, "secret_key_secret_ref")
        access_key = resolve_secret(access_ref).reveal()
        secret_key = resolve_secret(secret_ref).reveal()
        previous = _decode_cursor(source.cursor)
        changed: list[DiscoveredDocument] = []
        next_etags: dict[str, str] = {}
        continuation: str | None = None
        discovered = 0

        while discovered < self._max_pages:
            query: list[tuple[str, str]] = [("list-type", "2"), ("prefix", prefix)]
            if continuation:
                query.append(("continuation-token", continuation))
            url = _s3_url(endpoint, bucket, query)
            response = await self._request_signed(
                "GET", url, region=region, access_key=access_key, secret_key=secret_key
            )
            if not 200 <= response.status_code < 300:
                raise classify_status(response.status_code)
            try:
                root = ElementTree.fromstring(response.body)
            except ElementTree.ParseError as error:
                raise ModelProviderError("object-storage listing is malformed") from error

            for item in _children(root, "Contents"):
                key = _child_text(item, "Key")
                if not key or not key.startswith(prefix):
                    continue
                etag = _child_text(item, "ETag").strip('"')
                size_text = _child_text(item, "Size")
                try:
                    size = int(size_text)
                except ValueError as error:
                    raise ModelProviderError(
                        "object-storage listing has an invalid size"
                    ) from error
                if size < 0 or size > self._max_document_bytes:
                    _LOGGER.warning(
                        "object skipped because it exceeds the configured byte cap",
                        extra={"event": "data_hub.object_skipped", "source_id": source.source_id},
                    )
                    continue
                discovered += 1
                locator = f"{endpoint.rstrip('/')}/{quote(bucket, safe='')}/{quote(key, safe='/')}"
                if etag:
                    next_etags[key] = etag[:512]
                if etag and previous.get(key) == etag:
                    if discovered >= self._max_pages:
                        break
                    continue
                last_modified = _http_date(_child_text(item, "LastModified"))
                object_url = _s3_url(endpoint, bucket, (), key=key)
                content = await self._read_object(
                    object_url,
                    size=size,
                    region=region,
                    access_key=access_key,
                    secret_key=secret_key,
                )
                mime_type = _mime_from_key(key)
                changed.append(
                    DiscoveredDocument(
                        locator=locator,
                        title=key.rsplit("/", 1)[-1] or key,
                        mime_type=mime_type,
                        content=content,
                        etag=etag or None,
                        last_modified=last_modified,
                    )
                )
                if discovered >= self._max_pages:
                    break

            if discovered >= self._max_pages:
                break
            is_truncated = _child_text(root, "IsTruncated").lower() == "true"
            continuation = _child_text(root, "NextContinuationToken") or None
            if not is_truncated or continuation is None:
                break

        cursor = _encode_cursor(next_etags)
        return SourceSyncResult(tuple(changed), cursor, discovered)

    async def _read_object(
        self,
        url: str,
        *,
        size: int,
        region: str,
        access_key: str,
        secret_key: str,
    ) -> bytes:
        """Uses a single bounded byte range and validates the server response."""
        end = max(0, min(size - 1, self._max_document_bytes - 1))
        response = await self._request_signed(
            "GET",
            url,
            region=region,
            access_key=access_key,
            secret_key=secret_key,
            extra_headers={"range": f"bytes=0-{end}"},
        )
        if response.status_code not in (200, 206):
            raise classify_status(response.status_code)
        if len(response.body) > self._max_document_bytes or len(response.body) != size:
            raise ModelProviderError(
                "object-storage response length does not match the bounded listing",
                details={"expected_bytes": size, "received_bytes": len(response.body)},
            )
        content_range = _header(response.headers, "content-range")
        if response.status_code == 206 and content_range:
            match = re.fullmatch(r"bytes 0-(\d+)/(\d+)", content_range.strip())
            if match is None or int(match.group(2)) != size:
                raise ModelProviderError("object-storage range response is inconsistent")
        return response.body

    async def _request_signed(
        self,
        method: str,
        url: str,
        *,
        region: str,
        access_key: str,
        secret_key: str,
        extra_headers: Mapping[str, str] | None = None,
    ) -> TransportResponse:
        """Signs one request using AWS Signature Version 4 and sends it guarded."""
        headers = _sigv4_headers(
            method,
            url,
            region=region,
            access_key=access_key,
            secret_key=secret_key,
            extra_headers=extra_headers,
        )
        return await self._transport.request(method, url, headers=headers)


def _s3_config(
    config: Mapping[str, object], allowed_hosts: frozenset[str]
) -> tuple[str, str, str, str]:
    """Validates endpoint, bucket, prefix, and AWS region configuration."""
    endpoint = _required_config_string(config, "endpoint_url").rstrip("/")
    parsed = urlsplit(endpoint)
    host = (parsed.hostname or "").rstrip(".").lower()
    if parsed.scheme.lower() != "https" or not host or host not in allowed_hosts:
        raise ToolPermissionDeniedError(
            "object-storage endpoint is not in the HTTPS egress allow-list",
            details={"reason": "host_not_allowlisted"},
        )
    if (
        parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise InvalidRequestError(
            "object-storage endpoint must not include credentials or query data"
        )
    if parsed.port not in (None, 443):
        raise ToolPermissionDeniedError("object-storage endpoint uses a non-standard port")
    bucket = _required_config_string(config, "bucket")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket) or ".." in bucket:
        raise InvalidRequestError("object-storage bucket name is invalid")
    prefix = str(config.get("prefix", ""))
    if prefix.startswith("/") or ".." in prefix.split("/"):
        raise InvalidRequestError("object-storage prefix is invalid")
    region = str(config.get("region", _DEFAULT_REGION))
    if not re.fullmatch(r"[a-z0-9-]{1,64}", region):
        raise InvalidRequestError("object-storage region is invalid")
    normalized_endpoint = urlunsplit(("https", host, parsed.path.rstrip("/"), "", ""))
    return normalized_endpoint, bucket, prefix, region


def _s3_url(endpoint: str, bucket: str, query: Sequence[tuple[str, str]], *, key: str = "") -> str:
    """Builds a path-style S3 URL with deterministic query encoding."""
    base = f"{endpoint}/{quote(bucket, safe='')}"
    if key:
        base += f"/{quote(key, safe='/')}"
    return f"{base}?{urlencode(sorted(query), doseq=True, safe='~')}" if query else base


def _sigv4_headers(
    method: str,
    url: str,
    *,
    region: str,
    access_key: str,
    secret_key: str,
    extra_headers: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Calculates S3 Signature V4 for a GET/HEAD request with no body."""
    if not access_key or not secret_key:
        raise ConfigurationError("object-storage credential reference resolved empty")
    parsed = urlsplit(url)
    host = parsed.netloc.lower()
    now = datetime.now(UTC)
    amz_date = now.strftime(_S3_DATE_FORMAT)
    date_stamp = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(b"").hexdigest()
    headers = {
        "host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    if extra_headers:
        headers.update({key.lower(): value.strip() for key, value in extra_headers.items()})
    signed_headers = ";".join(sorted(headers))
    canonical_headers = "".join(f"{key}:{headers[key]}\n" for key in sorted(headers))
    canonical_uri = quote(parsed.path or "/", safe="/-_.~!$&'()*+,;=:@")
    canonical_query = urlencode(
        sorted(parse_qsl(parsed.query, keep_blank_values=True)),
        doseq=True,
        safe="~",
    )
    canonical_request = "\n".join(
        (
            method.upper(),
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers,
            payload_hash,
        )
    )
    scope = f"{date_stamp}/{region}/s3/aws4_request"
    string_to_sign = "\n".join(
        (
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        )
    )
    key_date = _hmac_sha256(f"AWS4{secret_key}".encode(), date_stamp)
    key_region = _hmac_sha256(key_date, region)
    key_service = _hmac_sha256(key_region, "s3")
    signing_key = _hmac_sha256(key_service, "aws4_request")
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    headers["authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    return headers


def _hmac_sha256(key: bytes, value: str) -> bytes:
    """One AWS key-derivation step."""
    return hmac.new(key, value.encode("utf-8"), hashlib.sha256).digest()


def _decode_cursor(cursor: str | None) -> dict[str, str]:
    """Decodes a bounded versioned ETag cursor; malformed state fails closed."""
    if not cursor:
        return {}
    if len(cursor) > 64 * 1024:
        raise InvalidRequestError("Data Hub source cursor exceeds its size limit")
    try:
        payload = json.loads(base64.urlsafe_b64decode(cursor.encode("ascii")))
    except (ValueError, UnicodeEncodeError, json.JSONDecodeError) as error:
        raise InvalidRequestError("Data Hub source cursor is malformed") from error
    if (
        not isinstance(payload, dict)
        or payload.get("v") != _CURSOR_VERSION
        or not isinstance(payload.get("etags"), dict)
        or len(payload["etags"]) > _MAX_CURSOR_ENTRIES
        or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in payload["etags"].items()
        )
    ):
        raise InvalidRequestError("Data Hub source cursor has an unsupported shape")
    return dict(payload["etags"])


def _encode_cursor(etags: Mapping[str, str]) -> str:
    """Serializes the bounded ETag map without padding noise."""
    selected = sorted(etags.items())[:_MAX_CURSOR_ENTRIES]
    raw = json.dumps({"v": _CURSOR_VERSION, "etags": dict(selected)}, separators=(",", ":"))
    encoded = base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")
    if len(encoded) > 64 * 1024:
        raise InvalidRequestError("Data Hub source cursor exceeds its size limit")
    return encoded


def _bounded_int(value: object, default: int, minimum: int, maximum: int) -> int:
    """Validates per-source integer overrides against deployment caps."""
    if value is None:
        return default
    if not isinstance(value, int) or isinstance(value, bool) or not minimum <= value <= maximum:
        raise InvalidRequestError(
            "source-specific limit exceeds the configured Data Hub cap",
            details={"minimum": minimum, "maximum": maximum},
        )
    return value


def _required_config_string(config: Mapping[str, object], name: str) -> str:
    """Reads a required non-empty source config value."""
    value = config.get(name)
    if not isinstance(value, str) or not value.strip():
        raise InvalidRequestError(
            "source configuration is missing a required value",
            details={"field": f"config.{name}"},
        )
    return value.strip()


def _children(parent: Element, local_name: str) -> list[Element]:
    """Finds direct S3 XML children independent of the namespace URI."""
    return [child for child in parent if child.tag.rsplit("}", 1)[-1] == local_name]


def _child_text(parent: Element, local_name: str) -> str:
    """Returns the first matching child text, or an empty string."""
    for child in parent.iter():
        if child.tag.rsplit("}", 1)[-1] == local_name:
            return child.text or ""
    return ""


def _content_type(value: str) -> str:
    """Normalizes a Content-Type field to its media type."""
    return value.split(";", 1)[0].strip().lower()


def _header(headers: Mapping[str, str], name: str) -> str:
    """Case-insensitive header lookup on the transport's redacted map."""
    target = name.lower()
    return next((value for key, value in headers.items() if key.lower() == target), "")


def _http_date(value: str) -> datetime | None:
    """Parses an HTTP date into an aware UTC timestamp."""
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def _mime_from_key(key: str) -> str:
    """Maps common object file suffixes to exact supported media types."""
    suffix = key.rsplit(".", 1)[-1].lower() if "." in key else ""
    return {
        "txt": "text/plain",
        "md": "text/markdown",
        "markdown": "text/markdown",
        "html": "text/html",
        "htm": "text/html",
        "json": "application/json",
        "csv": "text/csv",
        "pdf": "application/pdf",
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    }.get(suffix, "application/octet-stream")
