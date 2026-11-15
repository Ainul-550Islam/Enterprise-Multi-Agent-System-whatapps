"""mas SDK - Server-Sent Events client."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Generic, Optional, AsyncIterator, Dict, TypeVar, Any
T = TypeVar("T")

# SSE message types are defined locally in this module
class SseMessage:
    """An SSE event message."""
    data: str
    event: Optional[str] = None

class SseEventStream:
    """A parsed SSE event stream."""
    messages: List[SseMessage]
    has_more: bool = False
    next_key: Optional[str] = None


class SseClient(Generic[T]):
    """SSE client for real-time API events.

    Wraps an httpx GET request with "text/event-stream" and provides
    an async iterator over parsed SSE messages.
    """
    
    def __init__(self, client: MasClient | MasAsyncClient, method: str, path: str, **kwargs: Any) -> None:
        self.client = client
        self.method = method
        self.path = path
        self.kwargs = kwargs
        self._response: Optional[httpx.Response] = None
        self._done = False
    
    def __enter__(self) -> "SseClient[T]":
        # Synchronous context manager (if needed)
        return self
    
    def __exit__(self, *args: Any) -> None:
        self.close()
    
    def close(self) -> None:
        if self._response is not None:
            self._response.close()
            self._response = None
    
    def _get_response(self) -> httpx.Response:
        """Get or create the underlying HTTP response."""
        if self._response is None or self._response.is_closed:
            url = f"{self.client.base_url}{self.path}"
            self._response = self.client._client.open(
                self.method,
                url,
                headers=self.client._headers(),
                timeout=self.client.timeout,
                follow_redirects=True,
            )
        return self._response
    
    def __aiter__(self) -> "SseClient[T]":
        return self
    
    async def __anext__(self) -> Optional[SseMessage]:
        if self._done:
            raise StopAsyncIteration
        
        # Ensure we have a response
        if self._response is None or self._response.is_closed:
            self._response = self._get_response()
        
        # Try to read a message from the SSE stream
        try:
            line = await self._response.aread(4096)
        except Exception:
            self._done = True
            raise StopAsyncIteration
        
        if not line:
            self._done = True
            raise StopAsyncIteration
        
        self._buffer = line.decode("utf-8", errors="replace")
        
        # Parse SSE content
        data = ""
        event = ""
        
        while True:
            if not self._buffer:
                # Try reading more
                try:
                    more = await self._response.aread(4096)
                    if not more:
                        break
                    self._buffer += more.decode("utf-8", errors="replace")
                except Exception:
                    break
            
            # Process one line
            if "\n" in self._buffer:
                line, self._buffer = self._buffer.split("\n", 1)
            else:
                # Need more data
                break
            
            line = line.strip()
            
            if line == "":
                # Message delimiter - emit what we have
                if data or event:
                    msg = SseMessage(data=data, event=event)
                    self._data = ""
                    self._event = ""
                    return msg
            elif line.startswith(":"):
                # Comment - ignore
                continue
            else:
                # Key: value
                key, _, value = line.partition(":")
                key = key.strip()
                value = value.strip()
                if key == "data":
                    data = value
                elif key == "event":
                    event = value
        
        # If we exit the loop with data but no delimiter,
        # try to emit what we have
        if data:
            msg = SseMessage(data=data, event=event)
            self._data = ""
            self._event = ""
            return msg
        
        # Otherwise, continue reading
        return await self.__anext__()
