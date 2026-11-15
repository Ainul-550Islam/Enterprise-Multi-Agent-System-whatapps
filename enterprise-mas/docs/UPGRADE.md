# Upgrade Guide

## Upgrading from 0.x to 0.11 (this release)

### Breaking Changes

| Change | Migration |
|--------|-----------|
| `MAS_API_URL` format changed | Update to include `:port` if non-standard |
| Provider config format | Move `api_key` to `api_key_secret_ref` |
| Memory backend default | `in_process` → `postgres` in production |

### Migration Steps

1. Update `.env` file with new variable names
2. Run: `mas admin bootstrap --reconcile`
3. Re-configure providers using secret references
4. Migrate any in-memory data to PostgreSQL if applicable
5. Run `mas status` to verify

### New Features

- **Admin bootstrap**: Idempotent org→tenant→project→owner setup
- **API key rotation**: `mas api-key rotate --key-id <id>`
- **SSE events**: Real-time execution updates via SSE
- **Keyset pagination**: Efficient large-data traversal
- **Remote sandbox**: ISO-8080 isolated code execution

### Deprecated

- Direct `api_key` in config (use `api_key_secret_ref` instead)
- In-memory only memory backend for production
- Legacy execution transition verbs

## Upgrading from 0.11 to 0.12 (next release)

*Coming in the next release cycle.*

Check the changelog for detailed notes.
