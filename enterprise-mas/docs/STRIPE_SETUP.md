# Stripe billing setup

How to wire the billing plane (`mas-api` billing routes and
`BillingService`) to a real Stripe account.

`mas-api` reads two environment variables (config keys in parentheses).
Both must be non-empty or `/v1/billing/checkout` and
`/webhooks/billing/stripe` answer **503**:

| Value | Environment variable | Config key |
|---|---|---|
| Stripe secret key (`sk_live_…` / `sk_test_…`) | `MAS_STRIPE_SECRET_KEY` | `billing.stripe_secret_key` |
| Webhook signing secret (`whsec_…`) | `MAS_STRIPE_WEBHOOK_SECRET` | `billing.stripe_webhook_secret` |

There is no `MAS_STRIPE_API_KEY` and no billing-prefixed Stripe env on the
API. The API talks to Stripe with a guarded egress client pinned at
`https://api.stripe.com`. Rotation is a process restart with the new
secret values; the constructor takes a single webhook secret (not a
previous-slot pair).

## 1. Register the webhook endpoint

Point Stripe at the public webhook path:

```
POST https://<api-host>/webhooks/billing/stripe
```

Subscribe to the events the billing service consumes:

- `checkout.session.completed`
- `customer.subscription.created`
- `customer.subscription.updated`
- `customer.subscription.deleted`
- `invoice.paid`
- `invoice.payment_failed`

The handler verifies `Stripe-Signature` over the **raw body** before
anything else. A failed verification is not applied; a verified event is
durably claimed by `(provider='stripe', event_id)` exactly once, applied
through `BillingService::handle_stripe_event`, and only then acknowledged
`2xx`.

## 2. Open a checkout

Authenticated tenants open a session at:

```
POST /v1/billing/checkout
```

Required JSON fields (OpenAPI): `plan_id`, `interval`
(`one_time` \| `subscription`), `success_url`, `cancel_url`,
`idempotency_key`. Optional: `email`.

Related routes that exist on the same plane:

- `POST /v1/billing/checkout-session`
- `POST /v1/billing/portal-session`
- `GET  /v1/billing/subscription`
- `GET  /v1/billing/entitlements`
- `GET  /v1/billing/usage-totals`
- `GET  /v1/billing/receipts`
- `GET  /v1/billing/receipts/{provider}/{event_id}`

## 3. Plans and prices

- `plan_prices` is the global catalogue table (no RLS: it is not tenant-owned).
- Prices are mirrored from Stripe price ids at provisioning time; tenant
  overrides live in the contract/entitlement layer.

## 4. Local development

```bash
export MAS_STRIPE_SECRET_KEY=sk_test_...
export MAS_STRIPE_WEBHOOK_SECRET=whsec_...
stripe listen --forward-to localhost:8080/webhooks/billing/stripe
```

`stripe trigger checkout.session.completed` exercises the happy path once
the API was started with both variables set.

## 5. Security checklist

- [ ] No `sk_live_`/`sk_test_`/`whsec_` literal in the repo (secret grep is a CI gate).
- [ ] `MAS_STRIPE_SECRET_KEY` and `MAS_STRIPE_WEBHOOK_SECRET` injected at
      runtime, never committed.
- [ ] Receipts auditable at `/v1/billing/receipts/{provider}/{event_id}`.
