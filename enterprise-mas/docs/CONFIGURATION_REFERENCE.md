# Configuration Reference

This document lists every environment variable and configuration key used
by the mas system. Values are sourced from code (enums, settings, and
defaults) to keep this file in sync with the implementation.

## Environment Variables

### Global

| Variable | Default | Description |
|----------|---------|-------------|
| `MAS_API_URL` | `http://127.0.0.1:8080` | API base URL |
| `MAS_API_TOKEN` | `dev-token` | Bearer token for auth |
| `MAS_TENANT_ID` | (required) | Tenant scope header |
| `MAS_ORGANIZATION_ID` | (required) | Organization scope header |
| `MAS_CORRELATION_ID` | auto-generated | Correlates calls across services |

### Rust Core

| Variable | Default | Description |
|----------|---------|-------------|
| `RUST_LOG` | (none) | Tracing log level |
| `SANDBOX_HMAC_SECRET` | (required) | HMAC secret for sandbox runner |

### Memory

| Variable | Default | Description |
|----------|---------|-------------|
| `MASS_MEMORY_BACKEND` | `in_process` | `in_process` or `postgres` |
| `MASS_POSTGRES_DSN` | (required if postgres) | PostgreSQL connection string |

### Provider

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENAI_API_KEY` | (required) | OpenAI API key |
| `ANTHROPIC_API_KEY` | (required) | Anthropic API key |
| `GEMINI_API_KEY` | (required) | Google Gemini API key |
| `ANTHROPIC_API_KEY` | (required) | Anthropic API key |

### Tools

| Variable | Default | Description |
|----------|---------|-------------|
| `SMTP_HOST` | (required if using smtp) | SMTP server host |
| `SENDGRID_API_KEY` | (required if using sendgrid) | SendGrid API key |
| `BRAVE_API_KEY` | (required if using brave) | Brave Search API key |
| `TAVILY_API_KEY` | (required if using tavily) | Tavily API key |

### Observability

| Variable | Default | Description |
|----------|---------|-------------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | (optional) | OpenTelemetry endpoint |
| `PROMETHEUS_LISTEN_ADDRESS` | `:9090` | Prometheus metrics address |

## Rust Core Configuration (TOML)

```toml
[environment]
mode = "production"

[providers.openai]
api_key_secret_ref = "env:OPENAI_API_KEY"
allowed_models = ["gpt-4", "gpt-4-turbo"]

[providers.anthropic]
api_key_secret_ref = "env:ANTHROPIC_API_KEY"

[tools.smtp]
smtp_host = "smtp.sendgrid.net"
smtp_user = "env:SMTP_USER"
smtp_password = "env:SMTP_PASSWORD"

[observability]
redact_logs = true
