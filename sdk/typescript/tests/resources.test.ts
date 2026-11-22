import assert from "node:assert/strict";
import { test } from "node:test";

import { MasClient } from "../src/index.js";
import {
  Agents,
  Billing,
  Executions,
  Quota,
  WhatsApp,
  Workflows,
} from "../src/resources/index.js";

interface Seen {
  method: string;
  url: string;
}

function client(): { client: MasClient; seen: Seen[] } {
  const seen: Seen[] = [];
  const fetchImpl = (async (url: unknown, init: unknown) => {
    seen.push({ method: (init as { method: string }).method, url: String(url).replace("http://api.test", "") });
    return new Response(JSON.stringify({ data: { ok: true } }), { status: 200 });
  }) as unknown as typeof fetch;
  return {
    client: new MasClient({
      baseUrl: "http://api.test",
      token: "tok",
      scope: { tenantId: "tenant", organizationId: "org" },
      fetchImpl,
    }),
    seen,
  };
}

test("resources call the documented paths", async () => {
  const { client: mas, seen } = client();
  await new Agents(mas).get("agent-1");
  await new Workflows(mas).publish("wf-1");
  await new Executions(mas).get("exec-1");
  await new WhatsApp(mas).submitTemplate({ name: "n", language: "en" });
  await new Billing(mas).checkout({ plan_code: "pro" });
  await new Quota(mas).quotas();
  assert.deepEqual(
    seen.map((call) => `${call.method} ${call.url}`),
    [
      "GET /v1/agents/agent-1",
      "POST /v1/workflows/wf-1/publish",
      "GET /v1/executions/exec-1",
      "POST /v1/whatsapp/templates",
      "POST /v1/billing/checkout",
      "GET /v1/quotas",
    ],
  );
});

test("client retries transient failures before succeeding", async () => {
  const seen: number[] = [];
  const fetchImpl = (async () => {
    seen.push(1);
    if (seen.length === 1) {
      return new Response(JSON.stringify({ error: { code: "UNAVAILABLE", message: "busy" } }), { status: 503 });
    }
    return new Response(JSON.stringify({ data: { ok: true } }), { status: 200 });
  }) as unknown as typeof fetch;
  const mas = new MasClient({
    baseUrl: "http://api.test",
    token: "tok",
    fetchImpl,
    retry: { attempts: 2, sleep: async () => {} },
  });
  const result = await mas.request("GET", "/v1/quotas");
  assert.equal(result.ok, true);
  assert.equal(seen.length, 2);
});
