# WhatsApp channel setup

How to connect a WhatsApp Business channel to the MAS WhatsApp plane
(`mas-channel-whatsapp`, `mas-api` WhatsApp routes, `mas-application`
`WhatsappService`, persistence `repositories/whatsapp.rs`).

## 1. Prerequisites

- A WhatsApp Business Account (WABA) with a registered phone number.
- A Meta app with the *WhatsApp* product enabled.

## 2. Credentials (secret references only)

| Value | Secret ref | Used by |
|---|---|---|
| Permanent access token | `MAS_WHATSAPP_ACCESS_TOKEN` | outbound client (Graph calls to the fixed `graph.facebook.com` host) |
| WABA id | `MAS_WHATSAPP_WABA_ID` (config, not secret) | template + channel registration |
| App secret | `MAS_WHATSAPP_APP_SECRET` | inbound `X-Hub-Signature-256` verification |
| Verify token | `MAS_WHATSAPP_VERIFY_TOKEN` | webhook GET verification handshake |

No token material may appear in connector config; the connector registry
enforces a blanket no-secret-material rule at installation time.

## 3. Register the webhook

Meta callbacks land on the **public** webhook table (body-signature
authenticated, never bearer):

```
GET  https://<api-host>/webhooks/whatsapp   # hub.challenge verification
POST https://<api-host>/webhooks/whatsapp   # inbound messages + status updates
```

- `GET` answers the `hub.challenge` only when `hub.verify_token` matches the
  `MAS_WHATSAPP_VERIFY_TOKEN` reference; otherwise `403`.
- `POST` verifies `X-Hub-Signature-256` (HMAC-SHA256 over the raw body,
  constant-time compare). Unverified payloads are rejected `401` and recorded.

## 4. Channels and conversations

| Path | Purpose |
|---|---|
| `POST /v1/whatsapp/channels` | register a channel for the tenant |
| `GET /v1/whatsapp/channels` | list channels |
| `POST /v1/whatsapp/channels/{channel_id}/messages` | send templated/text outbound |
| `GET /v1/whatsapp/channels/{channel_id}/messages` | list stored messages |
| `GET /v1/whatsapp/conversations` | conversation view |
| `PUT /v1/whatsapp/messages/{wamid}/status` | delivery status transitions |

Every message row is tenant-scoped and RLS-protected; `record_message` is
idempotent on `wamid` so provider redeliveries cannot double-insert.

## 5. Media and transcription

Inbound media is fetched through the guarded egress policy (fixed Meta CDN
hosts). Audio transcription goes through the `Transcriber` protocol hook
(`channels/whatsapp_media.py`) — configure a provider implementation or the
hook raises a clear `unsupported` error; it never silently drops audio.

## 6. Local smoke

```bash
scripts/e2e-whatsapp.sh   # runs the webhook verify + inbound fixture flow
```
