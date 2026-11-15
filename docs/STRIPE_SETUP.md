# Stripe Billing Setup Guide

This guide walks through configuring the Stripe integration for the
Enterprise Multi-Agent System. It covers the developer steps, webhook
configuration, price/plan mapping, and go‑live checklist.

## 1. Create a Stripe Developer Account

1. Go to [dashboard.stripe.com](https://dashboard.stripe.com) and
   log in (or create a new account).
2. Navigate to **Developers → API keys**.
3. Note your **Secret key** (starts with `sk_test_` for test mode or
   `sk_live_` for live mode) and **Publishable key**.
2. In the same page, under **Webhooks**, click **Add endpoint**.

## 2. Configure the Webhook

1. **Callback URL**: Enter the public webhook endpoint of your MAS API,
   e.g.:
   ```
   https://<your-host>/v1/public/billing/stripe/webhook
   ```
2. **Select events to send**: Choose the following events (essential for
   subscription lifecycle):
   - `invoice.paid`
   - `invoice.payment_failed`
   - `customer.subscription.created`
   - `customer.subscription.updated`
   - `customer.subscription.deleted`
   - `checkout.session.completed`
3. **Secret**: Stripe will generate a webhook signing secret (starts with `whsec_`).
   Copy this value — you will reference it via `secret_ref` in the MAS config.
4. Click **Save endpoint**.

## 3. Create Prices & Plans

Stripe prices represent the billing details (amount, interval, currency),
while plan codes are your internal identifiers.

1. In the Stripe dashboard, go to **Products → Add product**.
   - Choose a product name (e.g. "Basic", "Pro", "Enterprise").
2. Create a **Recurring price** for each product:
   - **Amount**: the monthly/yearly fee in the smallest currency unit
     (e.g. `1999` = $19.99 USD)
   - **Interval**: `monthly` or `yearly`
   - **Currency**: `usd`, `eur`, etc.
3. After creating the price, note the **Price ID** (e.g. `price_123456789`).
4. Map your internal plan codes to Stripe Price IDs in the MAS config
   (see section 5).

## 3. MAS Configuration

Add the following entries to your MAS deployment configuration (environment
variables or `.env` file). These map to the `StripeConfig` structure:

| Variable | Description | Example |
|---|---|---|
| `MAS_STRIPE_API_KEY_REF` | Secret ref for the Stripe API key (sk_test_/sk_live_) | `stripe_api_key` |
| `MAS_STRIPE_WEBHOOK_SECRET_REF` | Secret ref for the Stripe webhook signing secret | `stripe_webhook_secret` |
| `MAS_STRIPE_PRICE_MAP` | JSON map of `plan_code` → `price_id` | `{"free":"price_0","pro":"price_1"}` |
| `MAS_STRIPE_SUCCESS_URL` | Default success URL for checkout sessions | `https://example.com/success` |
| `MAS_STRIPE_CANCEL_URL` | Default cancel URL for checkout sessions | `https://example.com/cancel` |

### Example `.env` entries

```env
MAS_STRIPE_API_KEY_REF=stripe_api_key
MAS_STRIPE_WEBHOOK_SECRET_REF=stripe_webhook_secret
MAS_STRIPE_PRICE_MAP='{"free":"price_0","pro":"price_1","business":"price_2"}'
MAS_STRIPE_SUCCESS_URL=https://example.com/success
MAS_STRIPE_CANCEL_URL=https://example.com/cancel
```

## 4. Go‑Live Checklist

☐ Stripe developer account created, API keys generated  
☐ Webhook endpoint configured with correct events (`invoice.paid`, `customer.subscription.*`, etc.)  
☐ Webhook signing secret stored as `MAS_STRIPE_WEBHOOK_SECRET_REF`  
☐ At least one price created in Stripe and mapped via `MAS_STRIPE_PRICE_MAP`  
☐ MAS `.env` / secret store populated with all required refs  
☐ `create_checkout_session` flow tested (success + cancel URLs)  
☐ `create_portal_session` flow tested (return URL works)  
☐ Webhook event handling tested with valid and tampered signatures  
☐ State machine transitions verified (created → active → past_due → canceled)  
☐ No inline secrets in source code or deployed manifests  
☐ Audit hooks confirmed (every webhook mutation writes to the audit log)  
☐ Rate limiting and 429 backoff tested  

## 5. Security Reminders

- **Never commit** `api_key`, `webhook_secret`, or `price_id` to version control.
- All secrets must be stored in the MAS secret manager and referenced by
  `secret_ref` (the crate policy enforces this at compile time).
- The webhook endpoint **must** verify the `X-Stripe-Signature` (or
  `X-Hub-Signature-256) on every incoming POST — never trust the payload
  body without signature verification.
- The `price_to_plan` map in `StripeConfig` is the **single source of truth**
  for which Stripe price belongs to which internal plan — do not hard-code
  plan IDs in business logic.
- Row‑level security (RLS) is armed on all billing‑related tables; tenants
  can only read/write their own data.
- Return **2xx** from the webhook handler immediately (durable receipt pattern);
  the actual processing (entitlement sync, audit) happens in a background worker.

## 6. Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| Webhook returns 400 | Signature verification failure | Check `MAS_STRIPE_WEBHOOK_SECRET_REF` matches the Stripe-generated secret; verify `X-Stripe-Signature` format |
| Checkout session returns 400 | Price ID not found | Ensure `MAS_STRIPE_PRICE_MAP` contains the correct `plan_code → price_id` mapping |
| Webhook returns 200 but no state change | Event not recognised | Add the event type to `EventType::from_stripe_str()` in `billing-stripe` |
| Portal session returns 400 | Missing or invalid customer ID | Verify the customer exists in Stripe and the ID is correct |
| Subscription stays `past_due` | Payment failed but not recovered | Check `invoice.payment_failed` webhook handling and grace period logic |

PYEOF