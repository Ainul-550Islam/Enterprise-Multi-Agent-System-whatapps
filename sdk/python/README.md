# mas-sdk (Python)

Stdlib-only client for the Enterprise MAS API.

## Install

```bash
pip install ./sdk/python
```

## Auth

```python
from mas_sdk import MasClient, Scope

client = MasClient(
    "https://api.example.test",
    token="operator-token",
    scope=Scope(tenant_id="...", organization_id="..."),
)
```

Tenant and organization are always sent together. A scoped call without them
fails locally with `MasScopeError`.

## Quickstart

```python
result = client.submit_execution(project_id="project-1", agent_id="agent-1", input={"prompt": "hello"})
if not result.ok:
    print(result.code, result.message)
else:
    print(result.data)
```

Typed resource clients mirror the OpenAPI operations:

```python
from mas_sdk.resources import Agents, WhatsApp

Agents(client).get("agent-1")
WhatsApp(client).send_text("channel-1", "+8801712345678", "hello")
```

## Errors

`request(..., raise_on_error=True)` raises `MasApiError(status, code,
request_id)`; rate limits after the retry budget raise
`MasRateLimited(retry_after)`. `MasScopeError` is raised before a network call
when the scope is missing. Retries cover 429/5xx with jittered exponential
backoff, honour a numeric `retry_after` field, and default to three attempts.
Default HTTP timeout is 10 seconds.

## Pagination

```python
for execution in client.iter_pages("/v1/executions", page_size=50):
    print(execution["id"])
```

The server owns the keyset cursor; the SDK never computes offsets and stops
if a cursor repeats.

## SSE

The SDK keeps the `parse_event_frame(raw)` helper for streams opened by your
HTTP client:

```python
from mas_sdk.client import parse_event_frame

frame = parse_event_frame('event: execution.completed\ndata: {"id":"e1"}')
```
