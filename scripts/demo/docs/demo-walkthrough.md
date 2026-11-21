# Demo walkthrough

Ten minutes from a clean checkout to a working demo of the Enterprise MAS
platform (this demo uses the dev lane: `--dev-inmemory`, table-driven
`MAS_DEV_TOKENS`, no external infrastructure).

## 1. Boot the stack

```bash
bash scripts/e2e-smoke.sh                       # api must answer live+ready
export MAS_API_URL=http://localhost:8080
export MAS_API_TOKEN=dev-token
export MAS_TENANT_ID=<uuid> MAS_ORGANIZATION_ID=<uuid>
```

## 2. Register the demo agent and workflow

API-shaped demo documents (support-bot, research-team, rag-faq,
approval-flow, content-team) are posted by:

```bash
bash scripts/seed-demo.sh
```

The CRD-shaped `agent-definition.json` / `workflow-definition.json` are
documentation of the definition objects, not the HTTP POST body. To
register one agent by hand:

```bash
curl -X POST "$MAS_API_URL/v1/agents" -H "authorization: Bearer $MAS_API_TOKEN" \
  -H "x-tenant-id: $MAS_TENANT_ID" -H "x-organization-id: $MAS_ORGANIZATION_ID" \
  -H 'content-type: application/json' \
  --data @scripts/demo/support-bot.json

curl -X POST "$MAS_API_URL/v1/workflows" -H "authorization: Bearer $MAS_API_TOKEN" \
  -H "x-tenant-id: $MAS_TENANT_ID" -H "x-organization-id: $MAS_ORGANIZATION_ID" \
  -H 'content-type: application/json' \
  --data @scripts/demo/workflow-definition.json
```

## 3. Run and watch

```bash
python examples/01_hello_agent.py     # submit one execution
python examples/03_stream_events.py   # follow it on the SSE stream
python examples/04_quota_and_usage.py # honest quota + usage numbers
```

## 4. WhatsApp lane (optional)

```bash
bash scripts/e2e-whatsapp.sh
```

Every demo number printed by the examples comes from the API — empty
catalogues print empty, never fabricated rows.
