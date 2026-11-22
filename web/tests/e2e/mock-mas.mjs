#!/usr/bin/env node
/**
 * Minimal MAS API stand-in for Playwright. Speaks the stable envelope and the
 * documented /v1 paths the dashboard e2e flow needs. Not a production fake.
 */
import http from "node:http";

const port = Number(process.env.MOCK_MAS_PORT ?? 18080);
const agents = [];
const executions = [];
const schedules = [];
const users = [
  { id: "user-1", email: "operator@example.test", display_name: "Operator", status: "active" },
];
const sources = [
  {
    id: "source-1",
    project_id: "33333333-3333-3333-3333-333333333333",
    name: "Product docs",
    kind: "web",
    status: "active",
    config: { url: "https://docs.example.test" },
    has_credential: false,
    updated_at: "2026-11-21T00:00:00Z",
  },
];
const whatsappChannels = [];
const webhookExecutions = [];
const deliveries = [
  { id: "delivery-1", subject: "Run failed", state: "failed", channel: "email", attempts: 2 },
];

function envelope(data, status = 200) {
  return {
    status,
    body: {
      data,
      error: null,
      meta: {
        request_id: "00000000-0000-0000-0000-000000000e2e",
        correlation_id: "e2e",
        api_version: "v1",
        responded_at: new Date().toISOString(),
      },
    },
  };
}

function error(status, code, message) {
  return {
    status,
    body: {
      data: null,
      error: { code, message },
      meta: {
        request_id: "00000000-0000-0000-0000-000000000e2e",
        correlation_id: "e2e",
        api_version: "v1",
        responded_at: new Date().toISOString(),
      },
    },
  };
}

function kpi() {
  return { value: 1, delta_percent: 0, sparkline: [0, 1], unit: "count" };
}

const server = http.createServer((request, response) => {
  const url = new URL(request.url ?? "/", "http://127.0.0.1");
  const auth = request.headers.authorization ?? "";
  const chunks = [];
  request.on("data", (chunk) => chunks.push(chunk));
  request.on("end", () => {
    let result;
    const json = chunks.length ? JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}") : {};
    // Public provider callbacks authenticate by signature, never by bearer.
    const isPublicWebhook = request.method === "POST" && url.pathname.startsWith("/v1/public/whatsapp/webhook/");
    if (!isPublicWebhook && !auth.startsWith("Bearer ")) {
      result = error(401, "UNAUTHENTICATED", "authentication required");
    } else if (request.method === "GET" && url.pathname === "/v1/auth/me") {
      result = envelope({
        subject: "api-key:e2e",
        actor: { type: "api_key" },
        tenant_id: "11111111-1111-1111-1111-111111111111",
        organization_id: "22222222-2222-2222-2222-222222222222",
        roles: ["owner"],
        permissions: ["agent:create"],
      });
    } else if (request.method === "GET" && url.pathname === "/v1/organizations") {
      result = envelope([{ id: "22222222-2222-2222-2222-222222222222", name: "Acme" }]);
    } else if (request.method === "GET" && url.pathname === "/v1/tenants") {
      result = envelope([{ id: "11111111-1111-1111-1111-111111111111", name: "Prod" }]);
    } else if (request.method === "GET" && url.pathname === "/v1/health/live") {
      result = envelope({ status: "ok" });
    } else if (request.method === "GET" && url.pathname === "/v1/whatsapp/channels") {
      result = envelope(whatsappChannels);
    } else if (request.method === "POST" && url.pathname === "/v1/whatsapp/channels") {
      whatsappChannels.push(json);
      result = envelope(json, 201);
    } else if (request.method === "POST" && url.pathname.startsWith("/v1/public/whatsapp/webhook/")) {
      if (!request.headers["x-hub-signature-256"]) {
        result = error(401, "UNAUTHENTICATED", "missing webhook signature");
      } else {
        webhookExecutions.push({ id: `exec-webhook-${webhookExecutions.length + 1}`, wamid: "wamid-e2e-001", status: "queued" });
        result = envelope({ accepted: true }, 202);
      }
    } else if (request.method === "GET" && url.pathname === "/v1/executions") {
      result = envelope([...executions, ...webhookExecutions]);
    } else if (request.method === "POST" && (url.pathname === "/v1/admin/plans" || url.pathname === "/v1/api-keys")) {
      result = envelope(json, 201);
    } else if (request.method === "GET" && url.pathname === "/v1/schedules") {
      result = envelope(schedules);
    } else if (request.method === "POST" && url.pathname === "/v1/schedules") {
      const item = { id: `schedule-${schedules.length + 1}`, name: json.name, status: "active", next_run_at: null };
      schedules.push(item);
      result = envelope(item, 201);
    } else if (request.method === "GET" && url.pathname === "/v1/users") {
      result = envelope(users);
    } else if (request.method === "GET" && url.pathname === "/v1/data-sources") {
      result = envelope(sources);
    } else if (request.method === "GET" && url.pathname.match(/^\/v1\/data-sources\/[^/]+\/documents$/)) {
      result = envelope([]);
    } else if (request.method === "GET" && url.pathname === "/v1/notifications/recipients/user-1") {
      result = envelope({ items: deliveries, next_cursor: null });
    } else if (request.method === "GET" && url.pathname === "/v1/policies") {
      result = envelope([]);
    } else if (request.method === "GET" && url.pathname === "/v1/agents") {
      result = envelope(agents);
    } else if (request.method === "POST" && url.pathname === "/v1/agents") {
      const agent = {
        id: `agent-${agents.length + 1}`,
        name: json.name,
        project_id: json.project_id,
        kind: "support",
        status: "active",
        slug: json.name,
        current_version: 1,
      };
      agents.push(agent);
      result = envelope(agent, 201);
    } else if (request.method === "POST" && url.pathname.match(/^\/v1\/projects\/[^/]+\/executions$/)) {
      const execution = {
        id: `exec-${executions.length + 1}`,
        status: "running",
        agent_id: json.agent_id,
        created_at: new Date().toISOString(),
      };
      executions.push(execution);
      result = envelope(execution, 201);
    } else if (request.method === "GET" && url.pathname.match(/^\/v1\/executions\/[^/]+$/)) {
      const id = url.pathname.split("/").pop();
      result = envelope(executions.find((item) => item.id === id) ?? { id, status: "running" });
    } else if (request.method === "GET" && url.pathname.match(/^\/v1\/executions\/[^/]+\/steps$/)) {
      result = envelope([
        { id: "step-1", sequence: 0, kind: "agent.run", status: "completed", latency_ms: 12 },
      ]);
    } else if (url.pathname.startsWith("/v1/analytics/")) {
      result = envelope({
        total_agents: kpi(),
        tasks_completed: kpi(),
        success_rate: { ...kpi(), unit: "percent" },
        time_saved_minutes: { ...kpi(), unit: "minutes" },
        granularity: "day",
        points: [{ bucket: "2026-11-15T00:00:00Z", tasks_completed: 1, success_rate: 100 }],
        slices: [{ agent_id: "agent-1", label: "support", tasks_completed: 1 }],
        total_completed: 1,
        total_cost: null,
        currency: null,
        tokens_input: 0,
        tokens_output: 0,
        tool_invocations: 0,
      });
    } else {
      result = envelope([]);
    }
    response.writeHead(result.status, { "content-type": "application/json" });
    response.end(JSON.stringify(result.body));
  });
});

server.listen(port, "127.0.0.1", () => {
  process.stdout.write(`mock-mas listening on 127.0.0.1:${port}\n`);
});
