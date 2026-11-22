"""Keyset pagination iterator behaviour."""

from __future__ import annotations

from mas_sdk import MasClient, Scope

SCOPE = Scope("tenant", "org")


def test_iter_pages_follows_server_cursors() -> None:
    seen: list[str] = []

    def transport(method: str, url: str, headers: dict[str, str], body: bytes | None) -> tuple[int, str]:
        import json

        seen.append(url)
        if "cursor=next-1" in url:
            return 200, json.dumps({"data": {"items": [2, 3], "next_cursor": None}})
        return 200, json.dumps({"data": {"items": [1], "next_cursor": "next-1"}})

    client = MasClient("http://api.test", "tok", scope=SCOPE, transport=transport)
    assert list(client.iter_pages("/v1/agents", page_size=1)) == [1, 2, 3]
    assert len(seen) == 2
    assert "limit=1" in seen[0]
    assert "cursor=next-1" in seen[1]


def test_iter_pages_stops_on_a_repeated_cursor() -> None:
    import json

    def transport(method: str, url: str, headers: dict[str, str], body: bytes | None) -> tuple[int, str]:
        return 200, json.dumps({"data": {"items": [1], "next_cursor": "loop"}})

    client = MasClient("http://api.test", "tok", scope=SCOPE, transport=transport)
    assert list(client.iter_pages("/v1/agents")) == [1, 1]
