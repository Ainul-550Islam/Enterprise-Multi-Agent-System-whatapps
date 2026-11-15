"""mas SDK - Keyset pagination."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TypeVar, Generic, Iterator, AsyncIterator
import httpx

from mas_sdk.models import ApiEnvelope, PaginationMeta, KeySetPage
from mas_sdk.errors import ApiError

T = TypeVar("T")


class KeySetPagination(Generic[T]):
    """Keyset-based pagination over API results.

    Uses a cursor (typically the last item's unique identifier) to page
    through results. More efficient than offset-based pagination for large
    datasets.
    
    Example::

        # python example
        >>> from mas_sdk import MasClient
        >>> client = MasClient("http://127.0.0.1:8080", "token")
        >>> paginator = client.paginate("/executions", limit=50)
        >>> async for page in paginator:
        ...     for execution in page.items:
        ...         print(execution.id)
    """
    
    def __init__(
        self,
        client: MasClient | MasAsyncClient,
        path: str,
        limit: int = 50,
        cursor: Optional[str] = None,
    ) -> None:
        self.client = client
        self.path = path
        self.limit = limit
        self.cursor = cursor
        self.has_more = True
        self.items: List[T] = []
    
    def _build_url(self) -> str:
        """Build the URL with pagination parameters."""
        url = f"{self.client.base_url}{self.path}"
        params: Dict[str, Any] = {"limit": self.limit}
        if self.cursor is not None:
            params["cursor"] = self.cursor
        # Append params to URL
        if "?" not in url:
            url += f"?{httpx.QueryParams(params).encode()}"
        else:
            separator = "&" if "?" in url.split("?")[1] else "?"
            url += f"{separator}{httpx.QueryParams(params).encode()}"
        return url
    
    def __iter__(self) -> "KeySetPagination[T]":
        return self
    
    def __next__(self) -> KeySetPage[T]:
        if not self.has_more:
            raise StopIteration
        
        # Make the request
        url = self._build_url()
        try:
            response = self.client._client.get(url, headers=self.client._headers())
            response.raise_for_status()
            body = response.json()
        except Exception as e:
            raise StopIteration from e
        
        # Parse the response
        if isinstance(body, dict) and "data" in body:
            items_data = body["data"]
            meta_dict = body.get("meta", {})
            
            # Convert to typed items
            items: List[T] = []
            for item in items_data:
                # Simple conversion - depends on the resource type
                if isinstance(item, dict):
                    items.append(item)  # type: ignore
                else:
                    items.append(item)  # type: ignore
            
            # Update pagination state
            self.has_more = meta_dict.get("has_more", False)
            self.cursor = meta_dict.get("next_key")
            
            return KeySetPage(items=items, meta=PaginationMeta(**meta_dict))
        
        self.has_more = False
        raise StopIteration

    def __aiter__(self) -> "KeySetPagination[T]":
        return self

    async def __anext__(self) -> KeySetPage[T]:
        if not self.has_more:
            raise StopAsyncIteration
        
        # Make the request asynchronously
        url = self._build_url()
        try:
            async_client = httpx.AsyncClient()
            response = await async_client.get(url, headers=self.client._headers())
            response.raise_for_status()
            body = response.json()
            await async_client.aclose()
        except Exception:
            raise StopAsyncIteration
        
        # Parse the response
        if isinstance(body, dict) and "data" in body:
            items_data = body["data"]
            meta_dict = body.get("meta", {})
            
            items: List[T] = []
            for item in items_data:
                if isinstance(item, dict):
                    items.append(item)  # type: ignore
                else:
                    items.append(item)  # type: ignore
            
            self.has_more = meta_dict.get("has_more", False)
            self.cursor = meta_dict.get("next_key")
            
            return KeySetPage(items=items, meta=PaginationMeta(**meta_dict))
        
        self.has_more = False
        raise StopAsyncIteration
