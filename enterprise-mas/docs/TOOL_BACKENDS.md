# Tool Backends Documentation

This document describes the available tool backends for the Enterprise Multi-Agent System.

## Table of Contents

1. [Memory Backends](#memory-backends)
2. [Email Backends](#email-backends)
3. [Search Backends](#search-backends)
4. [Database Backends](#database-backends)
5. [Sandbox/Code Execution](#sandboxcode-execution)
6. [Provider Adapters](#provider-adapters)
7. [Configuration Guidelines](#configuration-guidelines)

---

## Memory Backends

### In-Process Memory Store (`in_process`)
- **Purpose**: Development and testing only
- **Scope**: Per-worker in-memory state
- **Production**: Refused by configuration validation
- **Isolation**: Tenant-scoped within single process
- **Use case**: Development, testing, rapid prototyping

### Postgres Memory Store (`postgres`)
- **Purpose**: Production-backed durable memory
- **Requirements**: PostgreSQL with pgvector extension
- **Isolation**: Row Level Security (RLS) with `mas_memory` role
- **Features**: pgvector similarity search, tenant isolation, expiry/TTL
- **Configuration**: Requires `connection_dsn`, refused in production (Rust-owned)

**Configuration example:**
```toml
[memory]
backend = "postgres"
connection_dsn = "postgresql://user:pass@host:5432/dbname"
```

---

## Email Backends

### SMTP Backend (`smtp`)
- **Purpose**: Send email via SMTP server with STARTTLS
- **Security**: Host allowlist (smtp.gmail.com, smtp.sendgrid.net, etc.)
- **Rate limiting**: Configurable emails per 60-second window
- **Cooldown**: Seconds between individual emails
- **Credentials**: Secret references only (`env:NAME`, `file:PATH`)

**Configuration example:**
```toml
[tools.backends.smtp]
smtp_host = "smtp.sendgrid.net"
smtp_port = 587
smtp_user = "env:SMTP_USER"
smtp_password = "env:SMTP_PASSWORD"
from_email = "noreply@enterprise-mas.local"
rate_limit = 10
cooldown = 6.0
```

### SendGrid Backend (`sendgrid`)
- **Purpose**: Send email via SendGrid API
- **Security**: Pinned API endpoint (`https://api.sendgrid.com/v3/mail/send`)
- **Rate limiting**: Configurable emails per 60-second window
- **Credentials**: Secret references only

**Configuration example:**
```toml
[tools.backends.sendgrid]
api_key = "env:SENDGRID_API_KEY"
from_email = "noreply@enterprise-mas.local"
rate_limit = 100
```

---

## Search Backends

### Brave Search (`brave`)
- **Purpose**: Web search via Brave Search API
- **Security**: Pinned API endpoint (`https://api.brave.com/`)
- **Rate limiting**: Configurable results per request
- **Credentials**: Secret references only

**Configuration example:**
```toml
[tools.backends.search_brave]
api_key = "env:BRAVE_API_KEY"
results = 10
timeout = 30
```

### Tavily Search (`tavily`)
- **Purpose**: Web search via Tavily API
- **Security**: Pinned API endpoint (`https://api.tavily.com/`)
- **Features**: Includes generated answers, search depth control
- **Rate limiting**: Configurable results per request

**Configuration example:**
```toml
[tools.backends.search_tavily]
api_key = "env:TAVILY_API_KEY"
results = 10
include_answer = true
timeout = 30
```

### Searxng Search (`searxng`)
- **Purpose**: Self-hosted search via Searxng instance
- **Security**: Operator-allowlisted instance URLs only
- **Configuration**: Instance URL and scopes must be configured at deployment

**Configuration example:**
```toml
[tools.backends.search_searxng]
searxng_url = "http://localhost:8888"
scopes = ["http://example.com"]
timeout = 30
```

---

## Database Backends

### Postgres Read-Only (`postgres_ro`)
- **Purpose**: Read-only SQL query execution
- **Security**: 
  - SELECT-only queries (INSERT/UPDATE/DELETE rejected)
  - Statement timeout enforcement
  - Row/byte result caps
  - Schema/table whitelist
  - Row-level security via `SET ROLE`
- **Configuration required**:
  - `connection_dsn`: PostgreSQL DSN (secret reference)
  - `allowlisted_schemas`: List of schema names
  - `allowlisted_tables`: List of table names

**Configuration example:**
```toml
[tools.backends.database_postgres_ro]
connection_dsn = "env:RO_DB_DSN"
allowlisted_schemas = ["public", "audit"]
allowlisted_tables = ["users", "orders", "logs"]
statement_timeout = 5000
max_rows = 1000
max_bytes = 1000000
default_scope = "tenant_id"
```

---

## Sandbox/Code Execution

### RemoteSandbox (`sandbox_remote`)
- **Purpose**: Execute user code in isolated sandbox via sandbox-runner service
- **Isolation**:
  - No network access from sandbox
  - Read-only filesystem with tmpfs workspace
  - Resource limits: CPU, memory, time, output size
  - HMAC-signed requests for integrity
- **Configuration required**:
  - `runner_url`: Base URL of sandbox-runner service
  - `hmac_secret`: Secret reference for signing requests

**Configuration example:**
```toml
[tools.backends.sandbox_remote]
runner_url = "http://localhost:8080"
hmac_secret = "env:SANDBOX_HMAC_SECRET"
cpus = 0.5
memory_mb = 512
time_limit = 30
output_size_limit = 100000
workspace_size_mb = 64
```

---

## Provider Adapters

### OpenAI (`openai`)
- **Model**: Various GPT-4/3.5 models
- **Credentials**: `env:OPENAI_API_KEY`

### Anthropic (`anthropic`)
- **Model**: Various Claude models
- **Credentials**: `env:ANTHROPIC_API_KEY`

### Gemini (`gemini`)
- **Model**: Gemini 1.5 Flash, 1.5 Pro
- **Credentials**: `env:GEMINI_API_KEY`

### Azure OpenAI (`azure_openai`)
- **Model**: Deployments of GPT-4/3.5 on Azure
- **Credentials**: `env:AZURE_OPENAI_KEY`
- **Configuration**: `api_version`, `deployment_name`

### Compatible (`compatible`)
- **Model**: Any OpenAI-compatible endpoint
- **Use case**: Local Ollama, vLLM, Together AI, etc.
- **Configuration**: `api_base`, `model`, `supports_tools`, etc.

**Presets available:**
- `ollama` - Local Llama3 via Ollama
- `vllm` - vLLM serving endpoint
- `groq` - Groq high-speed inference
- `together` - Together AI
- `openrouter` - OpenRouter aggregation
- `mistral` - Mistral AI
- `deepseek` - DeepSeek Coder
- `production` - OpenAI + Anthropic with allowlisting
- `development` - Full development suite

---

## Configuration Guidelines

### Secret Management
- **Never** store plaintext credentials in configuration
- Use secret references: `env:NAME` or `file:PATH`
- All backends and providers resolve credentials at startup
- Missing secrets cause startup failure (fail-closed)

### Host Allowlisting
- All outbound HTTP/HTTPS destinations must be pinned
- Caller-supplied hosts are never trusted
- DNS-based allowlisting where possible

### Rate Limiting
- All backends support configurable rate limiting
- Production deployments should set appropriate limits
- Rate limit violations cause startup ConfigurationError or runtime errors

### Production Readiness
- Run `validate_settings()` before deploying to production
- Ensure all required secrets are configured
- Verify host allowlists are properly configured
- Test all configured backends in a staging environment

---

## Security Summary

| Threat | Mitigation |
|--------|-----------|
| SSRF | Pin all outbound hosts, never use caller-supplied URLs |
| Credential leakage | Secret references only, no plaintext keys |
| Unbounded resource usage | Rate limits, time limits, output caps, resource quotas |
| Data exfiltration | Tenant isolation via RLS, schema/table whitelists |
| Injection attacks | Input validation, output encoding, sandbox isolation |

---

*Last updated: 2026-11-08*
