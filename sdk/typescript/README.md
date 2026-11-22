# mas-sdk (TypeScript)

Typed client for the Enterprise MAS API. ESM, Node 18+, no runtime deps.

## Install

```bash
npm install ./sdk/typescript
```

## Auth

```ts
import { MasClient } from "mas-sdk";

const client = new MasClient({
  baseUrl: "https://api.example.test",
  token: "operator-token",
  scope: { tenantId: "...", organizationId: "..." },
});
```

Scope is local and fail-closed: `requireScope()` throws `MasScopeError`
before any network call.

## Quickstart

```ts
import { Agents, WhatsApp } from "mas-sdk";

const agent = await new Agents(client).get("agent-1");
await new WhatsApp(client).sendText("channel-1", "+8801712345678", "hello");

const execution = await client.submitExecution({
  projectId: "project-1",
  agentId: "agent-1",
  input: { prompt: "Summarize the latest incident." },
});
```

## Errors and retries

Results stay discriminated (`{ ok: true, data } | { ok: false, code,
message }`). Set `raiseOnError: true` to throw `MasApiError` /
`MasRateLimited(retryAfter)` instead. 429 and 5xx responses are retried up to
three times with bounded jitter; a numeric `Retry-After` header wins. Default
HTTP timeout is 10 seconds.

## SSE

`openEventStream` is unchanged:

```ts
const stream = client.openEventStream?.("/v1/stream");
```
