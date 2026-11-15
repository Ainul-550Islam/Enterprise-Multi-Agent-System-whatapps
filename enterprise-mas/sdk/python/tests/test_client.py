"""mas SDK client tests."""

import pytest
from mas_sdk.client import MasClient, MasAsyncClient
from mas_sdk.errors import ApiError


def test_client_init():
    client = MasClient(
        base_url="http://127.0.0.1:8080",
        token="test-token",
    )
    assert client.base_url == "http://127.0.0.1:8080"
    assert client.token == "test-token"
    assert client.tenant is None
    assert client.organization is None


def test_client_headers():
    client = MasClient(
        base_url="http://127.0.0.1:8080",
        token="test-token",
        tenant="tenant-123",
        organization="org-456",
    )
    headers = client._headers()
    assert headers["Authorization"] == "Bearer test-token"
    assert headers["X-Tenant-Id"] == "tenant-123"
    assert headers["X-Organization-Id"] == "org-456"
    assert headers["Accept"] == "application/json"


def test_client_post():
    client = MasClient(
        base_url="http://127.0.0.1:8080",
        token="test-token",
    )
    # Test that post method constructs the right URL
    # (we can't actually connect, just verify the method exists)
    assert hasattr(client, "post")


def test_async_client():
    client = MasAsyncClient(
        base_url="http://127.0.0.1:8080",
        token="test-token",
    )
    assert client.base_url == "http://127.0.0.1:8080"
    assert client.token == "test-token"
