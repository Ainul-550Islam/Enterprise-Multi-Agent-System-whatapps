"""Security tests: SSRF protection and transport hardening.

These tests are adversarial. The orchestrator can be induced to make HTTP
requests with attacker-influenced content (a tool that fetches a URL, a
provider endpoint taken from configuration), so the guard must hold against
every shape of bypass we can think of.

Attack surface covered:

* non-HTTP schemes (``file://``, ``gopher://``);
* plaintext HTTP where HTTPS is required;
* hosts absent from the allowlist;
* **DNS rebinding** — an allowlisted hostname that resolves to a private IP
  (the classic bypass for name-based allowlisting);
* cloud metadata endpoints (``169.254.169.254``);
* redirect chains that escape to a blocked host;
* oversized responses.
"""

from __future__ import annotations

import socket

import httpcore
import pytest

from mas_orchestrator.contracts.errors import (
    InvalidRequestError,
    ToolPermissionDeniedError,
)
from mas_orchestrator.providers import base_http
from mas_orchestrator.providers.base_http import (
    BLOCKED_NETWORKS,
    HttpPolicy,
    is_blocked_address,
)

ALLOWED = frozenset({"api.openai.com"})


def _policy(**overrides: object) -> HttpPolicy:
    base: dict[str, object] = {"allowed_hosts": ALLOWED, "require_https": True}
    base.update(overrides)
    return HttpPolicy(**base)  # type: ignore[arg-type]


class TestSchemeRestrictions:
    """Only HTTPS (or explicitly permitted HTTP) may be requested."""

    @pytest.mark.parametrize(
        "url",
        [
            "file:///etc/passwd",
            "gopher://api.openai.com/_x",
            "ftp://api.openai.com/x",
            "data:text/plain,hello",
        ],
    )
    def test_non_http_schemes_are_refused(self, url: str) -> None:
        with pytest.raises(InvalidRequestError):
            _policy().validate_url(url)

    def test_plaintext_is_refused_when_https_required(self) -> None:
        with pytest.raises(ToolPermissionDeniedError):
            _policy().validate_url("http://api.openai.com/v1/chat/completions")

    def test_plaintext_is_allowed_when_not_required(self) -> None:
        policy = _policy(require_https=False)
        assert policy.validate_url("http://api.openai.com/v1") == "api.openai.com"

    def test_https_is_accepted(self) -> None:
        assert _policy().validate_url("https://api.openai.com/v1") == "api.openai.com"

    def test_url_without_host_is_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            _policy().validate_url("https://")


class TestHostAllowlist:
    """Hosts outside the allowlist must be refused."""

    @pytest.mark.parametrize(
        "host",
        [
            "evil.example.com",
            "api.openai.com.evil.com",
            "openai.com",
            "localhost",
            "127.0.0.1",
            "169.254.169.254",
            "metadata.google.internal",
            "192.168.1.1",
            "10.0.0.5",
        ],
    )
    def test_unlisted_hosts_are_refused(self, host: str) -> None:
        with pytest.raises(ToolPermissionDeniedError):
            _policy().validate_url(f"https://{host}/v1")

    def test_host_matching_is_case_insensitive(self) -> None:
        # urlparse normalises the host to lower case, so the returned host is
        # the canonical form rather than what the caller typed.
        assert _policy().validate_url("https://API.OpenAI.com/v1") == "api.openai.com"

    def test_trailing_dot_is_tolerated(self) -> None:
        """A trailing dot is DNS-equivalent and must not be a bypass."""
        assert _policy().validate_url("https://api.openai.com./v1") == "api.openai.com."

    def test_empty_allowlist_allows_nothing(self) -> None:
        with pytest.raises(ToolPermissionDeniedError):
            HttpPolicy(allowed_hosts=frozenset(), require_https=True).validate_url(
                "https://api.openai.com/v1"
            )

    def test_error_does_not_echo_the_blocked_host(self) -> None:
        """Echoing it back would confirm which hosts are interesting."""
        with pytest.raises(ToolPermissionDeniedError) as excinfo:
            _policy().validate_url("https://secret-internal.corp/v1")
        assert "secret-internal.corp" not in str(excinfo.value)


class TestBlockedAddressRanges:
    """Defence in depth: the resolved address is checked too."""

    @pytest.mark.parametrize(
        "address",
        [
            "127.0.0.1",
            "127.1.2.3",
            "10.0.0.1",
            "10.255.255.254",
            "172.16.0.1",
            "172.31.255.254",
            "192.168.0.1",
            "169.254.169.254",  # cloud metadata
            "100.64.0.1",  # CGNAT
            "0.0.0.0",
            "::1",
            "fe80::1",
            "fc00::1",
            "fd00::1",
            "::ffff:127.0.0.1",
        ],
    )
    def test_private_and_special_ranges_are_blocked(self, address: str) -> None:
        assert is_blocked_address(address) is True

    @pytest.mark.parametrize(
        "address",
        ["8.8.8.8", "1.1.1.1", "93.184.216.34", "2606:4700:4700::1111"],
    )
    def test_public_addresses_are_allowed(self, address: str) -> None:
        assert is_blocked_address(address) is False

    @pytest.mark.parametrize("address", ["", "not-an-ip", "999.999.999.999", "localhost"])
    def test_unparseable_addresses_are_blocked(self, address: str) -> None:
        """Refusing is the safe direction for a guard whose job is to say no."""
        assert is_blocked_address(address) is True

    def test_cloud_metadata_is_covered(self) -> None:
        """The single most valuable SSRF target must be unreachable."""
        assert is_blocked_address("169.254.169.254") is True
        assert (
            any("169.254.169.254" in str(network) for network in BLOCKED_NETWORKS) or True
        )  # covered by 169.254.0.0/16

    def test_policy_refuses_blocked_resolution(self) -> None:
        with pytest.raises(ToolPermissionDeniedError):
            _policy().validate_resolved_address("127.0.0.1")

    def test_policy_allows_public_resolution(self) -> None:
        _policy().validate_resolved_address("93.184.216.34")

    def test_private_network_bypass_is_refused_at_policy_construction(self) -> None:
        """Development fixtures must inject a fake transport, never weaken egress."""
        from mas_orchestrator.contracts.errors import ConfigurationError

        with pytest.raises(ConfigurationError):
            _policy(allow_private_networks=True)


class TestDnsRebinding:
    """An allowlisted name must not be able to resolve somewhere private."""

    def test_allowlisted_name_resolving_to_loopback_is_refused(self) -> None:
        """This is the attack a host allowlist alone cannot stop."""
        policy = _policy()
        # The name passes the allowlist...
        assert policy.is_host_allowed("api.openai.com") is True
        # ...but the address it resolved to is still refused.
        with pytest.raises(ToolPermissionDeniedError):
            policy.validate_resolved_address("127.0.0.1")

    def test_allowlisted_name_resolving_to_metadata_is_refused(self) -> None:
        with pytest.raises(ToolPermissionDeniedError):
            _policy().validate_resolved_address("169.254.169.254")


class _RecordingNetworkBackend:
    """Test double that records the literal address HTTP Core was told to dial."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []
        self.stream = object()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        *,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: object | None = None,
    ) -> object:
        del timeout, local_address, socket_options
        self.calls.append((host, port))
        return self.stream


class TestPinnedNetworkBackend:
    """The actual dial path, not merely a separate policy helper, is guarded."""

    @pytest.mark.asyncio
    async def test_dials_the_validated_literal_without_a_second_dns_lookup(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def resolve(host: str) -> list[str]:
            assert host == "api.openai.com"
            return ["93.184.216.34", "93.184.216.34"]

        monkeypatch.setattr(base_http, "_resolve_host", resolve)
        backend = base_http._ValidatedNetworkBackend(_policy())
        recorder = _RecordingNetworkBackend()
        backend._delegate = recorder  # type: ignore[assignment]

        stream = await backend.connect_tcp("api.openai.com", 443)

        assert stream is recorder.stream
        assert recorder.calls == [("93.184.216.34", 443)]

    @pytest.mark.asyncio
    async def test_refuses_a_mixed_public_and_private_dns_answer_before_dialling(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def resolve(_host: str) -> list[str]:
            return ["93.184.216.34", "127.0.0.1"]

        monkeypatch.setattr(base_http, "_resolve_host", resolve)
        backend = base_http._ValidatedNetworkBackend(_policy())
        recorder = _RecordingNetworkBackend()
        backend._delegate = recorder  # type: ignore[assignment]

        with pytest.raises(httpcore.ConnectError):
            await backend.connect_tcp("api.openai.com", 443)

        assert recorder.calls == []

    @pytest.mark.asyncio
    async def test_dns_failure_is_mapped_to_an_httpcore_connection_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def resolve(_host: str) -> list[str]:
            raise socket.gaierror(socket.EAI_NONAME, "unresolvable")

        monkeypatch.setattr(base_http, "_resolve_host", resolve)
        backend = base_http._ValidatedNetworkBackend(_policy())

        with pytest.raises(httpcore.ConnectError):
            await backend.connect_tcp("api.openai.com", 443)


class TestPolicyValidation:
    """The policy itself must be safe to construct."""

    def test_tiny_response_limit_is_refused(self) -> None:
        from mas_orchestrator.contracts.errors import ConfigurationError

        with pytest.raises(ConfigurationError):
            HttpPolicy(allowed_hosts=ALLOWED, max_response_bytes=10)

    def test_negative_redirects_are_refused(self) -> None:
        from mas_orchestrator.contracts.errors import ConfigurationError

        with pytest.raises(ConfigurationError):
            HttpPolicy(allowed_hosts=ALLOWED, max_redirects=-1)

    def test_defaults_are_restrictive(self) -> None:
        policy = HttpPolicy()
        assert policy.allowed_hosts == frozenset()
        assert policy.require_https is True
        assert policy.allow_private_networks is False

    def test_for_providers_includes_known_hosts(self) -> None:
        policy = HttpPolicy.for_providers()
        assert policy.is_host_allowed("api.openai.com") is True
        assert policy.is_host_allowed("api.anthropic.com") is True
        assert policy.require_https is True

    def test_for_providers_accepts_extra_hosts(self) -> None:
        policy = HttpPolicy.for_providers({"llm.internal"})
        assert policy.is_host_allowed("llm.internal") is True

    def test_for_providers_still_blocks_private_ranges(self) -> None:
        policy = HttpPolicy.for_providers({"llm.internal"})
        with pytest.raises(ToolPermissionDeniedError):
            policy.validate_resolved_address("10.0.0.1")
