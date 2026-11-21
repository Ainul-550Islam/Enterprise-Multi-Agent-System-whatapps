# Demo FAQ

## Which files does `scripts/seed-demo.sh` POST?

API-shaped documents in `scripts/demo/`:

| File | Path |
|---|---|
| `support-bot.json` | `POST /v1/agents` |
| `research-team.json` | `POST /v1/agents` |
| `rag-faq.json` | `POST /v1/agents` |
| `content-team.json` | `POST /v1/agents` |
| `approval-flow.json` | `POST /v1/workflows` |

`agent-definition.json` and `workflow-definition.json` are CRD-shaped
(`kind`/`apiVersion`/`metadata`/`spec`). The seed script still offers them;
a 4xx on those two is expected against the HTTP agents/workflows API and is
not counted as a seed failure.

## Why did seed-demo exit 1 with 401?

The API was started without `MAS_DEV_TOKENS` (or you exported a different
`MAS_API_TOKEN`). There is no built-in token.

## Why 403 "no tenant scope"?

`MAS_TENANT_ID` and `MAS_ORGANIZATION_ID` must both be set. The API does
not default a tenant from the token.

## Why is `/v1/quotas` empty?

Unconfigured tenants have no quota rows. `examples/04_quota_and_usage.py`
prints the envelope as returned.

## WhatsApp 503?

`MAS_WHATSAPP_ACCESS_TOKEN`, `MAS_WHATSAPP_APP_SECRET`, and
`MAS_WHATSAPP_VERIFY_TOKEN` are all required or the WhatsApp plane stays
unprovisioned. See `enterprise-mas/docs/WHATSAPP_SETUP.md`.

## Stripe 503?

`MAS_STRIPE_SECRET_KEY` and `MAS_STRIPE_WEBHOOK_SECRET` must both be set.
See `enterprise-mas/docs/STRIPE_SETUP.md`.
