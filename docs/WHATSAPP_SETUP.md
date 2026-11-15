# WhatsApp Cloud API Setup Guide

This guide walks through configuring the WhatsApp integration for the
Enterprise Multi-Agent System. It covers the Meta developer steps, webhook
configuration, template approval, and go‑live checklist.

## 1. Create a Meta Developer Account

1. Go to [developers.facebook.com](https://developers.facebook.com) and
   create/or log in to a Meta developer account.
2. Create a new **app** (if you don't have one already).
   - Select "Business" as the app type.
   - Note the **App ID** and **App Secret** — you'll need these later.

## 2. Set Up a WhatsApp Business Account (WABA)

1. In the Meta for Developers dashboard, go to **Products → WhatsApp → Get started**.
2. Follow the flow to create a **WhatsApp Business Account (WABA)**.
3. You'll be prompted to add a **Phone Number**.
   - Use a phone number that can receive SMS/calls for verification.
   - Complete the verification step.
4. After setup, you'll receive:
   - **WABA ID** (UUID format)
   - **Phone Number ID** (UUID format)
   - **Permanent (long‑lived) access token**
   - **App Secret** (for HMAC signature verification)

## 3. Configure the Webhook

1. In the same WhatsApp product page, scroll to **Webhooks** and click **Add webhook**.
2. **Callback URL**: Enter the public webhook endpoint of your MAS API,
   e.g.:
   ```
   https://<your-host>/v1/public/whatsapp/webhook/{route_id}
   ```
   - `{route_id}` is a free‑form identifier you choose (e.g. `whatsapp‑default`).
3. **Subscribe to events**: Select the following checkboxes:
   - `messages` (inbound messages)
   - `message_deliveries` (delivery receipts)
   - `message_reads` (read receipts)
   - `message_optins` (opt‑in/opt‑out)
4. **Verify token**: Set a plain‑text string (e.g. `my‑verify‑token`). This
   must match the `verify_token_ref` in the MAS configuration.
5. Click **Verify and Save**. Meta will send a GET hub.challenge request;
   your endpoint must echo the challenge parameter unchanged.

## 4. Template Approval (Required for Outbound Messages)

WhatsApp requires that **all** outbound message templates be pre‑approved by
Meta. Without approved templates, the API will reject any template send
attempt.

1. In the Meta dashboard, go to **WhatsApp → Message Templates → Add Template**.
2. Choose a **Category**:
   - **Template usage**: Promotional, Utilities, Authentication, or Authentication (OTP).
3. Design the template using the approved variable names. WhatsApp accepts:
   - `{{1}}`, `{{2}}, …` as placeholder positions.
   - Free‑form text, phone numbers, currency amounts, or section names (for lists).
4. Submit for **Review**. Meta typically reviews within 24–48 hours.
5. Once **Approved**, the template status changes and the MAS system can
   send messages using it. If **Rejected**, review the feedback and resubmit.

## 5. MAS Configuration

Add the following entries to your MAS deployment configuration (environment
variables or .env file). These map to the `WhatsAppAccountConfig` structure:

| Variable | Description | Example |
|---|---|---|
| `MAS_TEST_WABA_ID` | WABA UUID from Meta | `12345678‑1234‑1234‑1234‑123456789012` |
| `MAS_TEST_PHONE_ID` | Phone Number ID from Meta | `87654321‑4321‑4321‑4321‑210987654321` |
| `MAS_TEST_APP_SECRET_REF` | Secret ref key for the app secret (looked up via secret store) | `whatsapp_app_secret` |
| `MAS_TEST_ACCESS_TOKEN_REF` | Secret ref for the long‑lived access token | `whatsapp_access_token` |
| `MAS_TEST_VERIFY_TOKEN` | Hub verify token (must match webhook config) | `my‑verify‑token` |

Additionally, ensure the following are configured in the MAS control plane:

- **Secret store entries** for `app_secret_ref` and `access_token_ref` — these
  must never be inline in code or environment variables (per the no‑inline‑secret
  policy).
- **Route ID** mapping — the `{route_id}` in the webhook URL must be known to
  the MAS routing layer so it can look up the associated `WhatsAppAccountConfig`.

## 6. Go‑Live Checklist

☐ Meta developer account created, app registered  
☐ WABA created with verified phone number  
☐ Webhook URL configured and verified (hub challenge passes)  
☐ At least one template submitted and **approved** by Meta  
☐ MAS `.env` / secret store populated with WABA/phone IDs and refs  
☐ Rate‑limit and 24h window logic tested (see `scripts/e2e-whatsapp.sh`)  
☐ Signed webhook verification tested with both valid and tampered signatures  
☐ Outbound template send test (using an approved template)  
☐ Human handoff flow tested (STOP/opt‑out, agent takeover)  
☐ All audit hooks confirmed (every mutation writes to the audit log)  
☐ No inline secrets in source code or deployed manifests  

## 7. Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| Webhook returns 401 | Verify token mismatch | Check `MAS_TEST_VERIFY_TOKEN` matches the webhook’s `hub.verify_token` |
| Template send returns 400 | Template not approved | Ensure template status is `Approved` in Meta dashboard |
| Signature verification fails | App secret mismatch / wrong HMAC computation | Confirm `app_secret_ref` resolves to the correct Meta App Secret; verify `X-Hub-Signature-256` format |
| Messages arrive but no execution created | Routing / account not linked | Verify the `route_id` used in the webhook URL maps to the correct `WhatsAppAccountConfig` |
| Rate limit (130429) exceeded | Too many messages sent too quickly | Back off per Meta 130429 pattern; the system has a token‑bucket limiter |

## 8. Security Reminders

- **Never commit** `app_secret`, `access_token`, or `verify_token` to version control.
- All secrets must be stored in the MAS secret manager and referenced by
  `secret_ref` (the crate policy enforces this at compile time).
- The webhook endpoint must HMAC‑verify every incoming POST — never trust the
  payload body without signature verification.
- Row‑level security (RLS) is armed on all WhatsApp‑related tables; tenants
  can only read/write their own data.