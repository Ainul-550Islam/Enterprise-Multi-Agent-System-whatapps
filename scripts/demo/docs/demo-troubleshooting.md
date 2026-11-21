# Demo troubleshooting

What to check when a demo step does not answer as documented, in the order
the failures actually happen.

## api answers 401 everywhere

The dev lane resolves tokens from the `MAS_DEV_TOKENS` table
(`token:subject:kind,...`) — there is no built-in token. If `api` was started
without it, every call is `UNAUTHENTICATED` by design. Restart with:

```bash
MAS_DEV_TOKENS=dev-token:dev-user:system ./target/debug/mas-api --dev-inmemory
```

## 400 VALIDATION_FAILED on scoped routes

Scoped routes need `x-tenant-id` and `x-organization-id` **together**. The
envelope names the missing header in `error.details.issues[].field`.

## `/v1/whatsapp/...` answers 503

The deployment has no WhatsApp service configured (secret references for the
access token / app secret are absent). That is fail-closed honesty, not a
bug — see `enterprise-mas/docs/WHATSAPP_SETUP.md`.

## webhook signature failures

Signature verification uses the **raw** body. If a proxy re-encodes JSON the
tag no longer matches. Send the exact provider bytes, and verify the secret
matches `MAS_WHATSAPP_APP_SECRET` (or the Stripe webhook secret on the
billing lane).

## the stream never delivers events

`/v1/stream` only emits while events occur — a quiet dev lane is a quiet
stream. Submit an execution first (`examples/03_stream_events.py` does both).

## quota answers look empty

They are empty: `/v1/quotas` reports the quota snapshot and an unconfigured
tenant has no rows. The answer is truthful; register quotas to see numbers.
