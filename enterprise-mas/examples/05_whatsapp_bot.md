# 05_whatsapp_bot.md

This example demonstrates a WhatsApp bot built with the mas framework
using the WhatsApp Cloud API (Batch A).

## Prerequisites

- mas-cli installed and configured
- WhatsApp Business API access
- MongoDB or PostgreSQL for state storage

## Running the bot

```bash
# 1. Bootstrap the system
mas admin bootstrap   --organization my-org   --tenant my-tenant   --project my-project   --owner admin-user

# 2. Create the WhatsApp bot agent
mas agent create   --name "whatsapp-bot"   --scope organization   --capabilities "whatsapp-messaging"

# 3. Start the bot service
python examples/05_whatsapp_bot.py

# The bot will:
# - Receive incoming WhatsApp messages via webhook
# - Process them through the agent pipeline
# - Send reply messages via the WhatsApp API
# - Log interactions for analytics
```
