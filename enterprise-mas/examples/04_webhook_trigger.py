# 04_webhook_trigger.py

An agent that responds to webhook triggers (e.g., from WhatsApp webhook).

## Usage

```bash
# Create a webhook-triggered agent
mas agent create   --name "webhook-handler"   --scope tenant   --capabilities "webhook-handler"

# The agent listens for POST events at /webhook/handler
# and processes incoming messages.

# Trigger via curl
curl -X POST http://localhost:8080/webhook/handler   -H "Content-Type: application/json"   -d '{"message": "Hello from WhatsApp", "from": "+1234567890"}'
```
