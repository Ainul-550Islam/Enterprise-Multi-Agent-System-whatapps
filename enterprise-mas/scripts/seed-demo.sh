#!/usr/bin/env bash
set -euo pipefail

# seed-demo.sh - Bootstrap and seed demo data for the mas system

echo "=== mas Demo Bootstrap ==="

# Step 1: Bootstrap the organization/tenant/project structure
echo "1. Bootstrapping organization structure..."
mas admin bootstrap   --organization org-dawn   --tenant tenant-dawn   --project project-dawn   --owner owner-dawn   --default-plan plan-free

if [ $? -ne 0 ]; then
    echo "❌ Bootstrap failed"
    exit 1
fi

echo "✓ Bootstrap complete (idempotent: re-runs are safe)"

# Step 2: Create demo agents
echo "2. Creating demo agents..."

mas agent create   --name "support-bot"   --scope tenant   --capabilities "whatsapp-messaging,ticket-creation"   --idempotency-key "support-bot-v1"

mas agent create   --name "research-team"   --scope organization   --capabilities "web-search,report-generation"   --idempotency-key "research-team-v1"

mas agent create   --name "rag-faq"   --scope tenant   --capabilities "retrieval,chat,vector-search"   --idempotency-key "rag-faq-v1"

echo "✓ Demo agents created"

# Step 3: Create demo workflows
echo "3. Creating demo workflows..."

mas workflow create   --name "approval-flow"   --definition '{"steps": ["review", "approve", "notify"]}'   --idempotency-key "approval-flow-v1"

mas workflow create   --name "content-pipeline"   --definition '{"steps": ["generate", "review", "publish"]}'   --idempotency-key "content-pipeline-v1"

echo "✓ Demo workflows created"

# Step 4: Create API keys
echo "3. Creating API keys..."

mas api-key create   --name "operator-key"   --permissions "exec:read;exec:write;agent:read"   --idempotency-key "operator-key-v1"

mas api-key create   --name "frontend-key"   --permissions "exec:read"   --idempotency-key "frontend-key-v1"

echo "✓ API keys created"

# Step 5: Verify the setup
echo "4. Verifying setup..."
mas status
mas execution list

echo ""
echo "=== Demo setup complete! ==="
echo "You can now run: mas execution submit --project project-dawn --workflow approval-flow"
