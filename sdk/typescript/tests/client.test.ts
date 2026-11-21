import assert from "node:assert/strict";
import { test } from "node:test";

import { MasClient, MissingScopeError } from "../src/index.js";

function fakeFetch(status: number, envelope: unknown): typeof fetch {
  return (async () =>
    new Response(JSON.stringify(envelope), { status })) as unknown as typeof fetch;
}

const scope = { tenantId: "tenant-1", organizationId: "org-1" };

test("submits executions with an idempotency key and scope headers", async () => {
  const seen: { url: string; headers: Record<string, string>; body: string }[] = [];
  const fetchImpl = (async (url: unknown, init: unknown) => {
    const request = init as { headers: Record<string, string>; body: string };
    seen.push({ url: String(url), headers: request.headers, body: request.body });
    return new Response(JSON.stringify({ data: { executionId: "exec-1", status: "queued" } }), {
      status: 201,
    });
  }) as unknown as typeof fetch;

  const client = new MasClient({ baseUrl: "http://api.test", token: "tok", scope, fetchImpl });
  const result = await client.submitExecution({ agentId: "agent-1", input: "hello" });

  assert.equal(result.ok, true);
  assert.deepEqual(result.ok && result.data, { executionId: "exec-1", status: "queued" });
  assert.equal(seen.length, 1);
  assert.equal(seen[0].headers["x-tenant-id"], "tenant-1");
  assert.equal(seen[0].headers["x-organization-id"], "org-1");
  assert.ok(seen[0].headers["x-idempotency-key"].length > 8, "idempotency key generated");
  assert.equal(JSON.parse(seen[0].body).input, "hello");
});

test("decodes error envelopes into typed results", async () => {
  const client = new MasClient({
    baseUrl: "http://api.test",
    token: "tok",
    scope,
    fetchImpl: fakeFetch(404, { error: { code: "RESOURCE_NOT_FOUND", message: "gone" } }),
  });
  const result = await client.getExecution("missing");
  assert.equal(result.ok, false);
  assert.equal(!result.ok && result.code, "RESOURCE_NOT_FOUND");
});

test("scoped calls fail locally without a scope", async () => {
  const client = new MasClient({ baseUrl: "http://api.test", token: "tok", fetchImpl: fakeFetch(200, {}) });
  assert.throws(() => client.requireScope(), MissingScopeError);
  await assert.rejects(client.getExecution("x"), MissingScopeError);
});

test("signals route to the signal endpoint", async () => {
  const seen: string[] = [];
  const fetchImpl = (async (url: unknown) => {
    seen.push(String(url));
    return new Response(JSON.stringify({ data: { ok: true } }), { status: 200 });
  }) as unknown as typeof fetch;
  const client = new MasClient({ baseUrl: "http://api.test/", token: "tok", scope, fetchImpl });
  const result = await client.signalExecution("exec-9", "cancel");
  assert.equal(result.ok, true);
  assert.deepEqual(seen, ["http://api.test/v1/executions/exec-9/signal"]);
});
