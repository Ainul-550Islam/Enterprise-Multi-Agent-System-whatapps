# 05 — WhatsApp support bot (end-to-end walkthrough)

A complete, runnable design for a WhatsApp support bot on the Enterprise MAS
stack. `examples/05_whatsapp_bot.py` drives the tenant-facing API through the Python
SDK; this document composes the pieces the codebase already ships (`mas-channel-whatsapp`,
`WhatsappSupportAgent`, the WhatsApp API surface) into one deployable flow.

## 0. Run the tenant-facing SDK script

```bash
export MAS_API_URL=... MAS_API_TOKEN=... MAS_TENANT_ID=... MAS_ORGANIZATION_ID=...
export MAS_WHATSAPP_CHANNEL_ID=... MAS_WHATSAPP_TO=+8801712345678
python examples/05_whatsapp_bot.py
```

The script reads the channel list and, when a channel and recipient are
supplied, sends one real text message through the documented API. It never
handles provider tokens or webhook secrets.

## Architecture

```
Meta Cloud API ──signed──▶ POST /webhooks/whatsapp ──▶ WhatsappService
                                                          │
                                        (tenant resolved by phone handle)
                                                          │
   customer ◀──send── POST /v1/whatsapp/channels/{id}/messages ◀── reply
                                                          │
                                     WhatsappSupportAgent (triage + draft)
                                                          │
                                     split_by_limit → segmented sends
```

## 1. Wire the channel

Follow `enterprise-mas/docs/WHATSAPP_SETUP.md`: register the webhook, store
the access token / app secret / verify token as secret references, then:

```bash
curl -X POST "$MAS_API_URL/v1/whatsapp/channels" \
  -H "authorization: Bearer $MAS_API_TOKEN" \
  -H "x-tenant-id: $MAS_TENANT_ID" \
  -H "x-organization-id: $MAS_ORGANIZATION_ID" \
  -d '{"display_name":"Support","phone_number_id":"PN-1","waba_id":"WABA-1",
       "api_version":"v21.0","verify_token_ref":"ref://whatsapp/verify"}'
```

## 2. Behaviour of the built-in support agent

The product-owned `WhatsappSupportAgent` (`agents/builtin/whatsapp_support_agent.py`)
classifies each inbound message into `order_status`, `refund`, `account`,
`human_handoff` or `other`, drafts a reply that never invents account facts,
and segments it to WhatsApp's 4096-char limit. Refund and handoff intents are
flagged `needs_human` so your queue can escalate.

Audio messages go through the `Transcriber` hook (`channels/whatsapp_media.py`);
with no provider installed the pipeline raises an explicit
`UnsupportedTranscriptionError` instead of dropping the message.

## 3. Local smoke

```bash
scripts/e2e-whatsapp.sh   # webhook verify handshake + inbound fixture flow
```

## 4. Production checklist

- [ ] `X-Hub-Signature-256` verification on (constant-time compare).
- [ ] Reply segmentation on (`split_by_limit`) — no oversized sends.
- [ ] wamid idempotency on (provider redeliveries cannot double-insert).
- [ ] Human escalation queue consuming `needs_human`.
