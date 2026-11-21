# Tool backends

Built-in tools (`mas_orchestrator.tools.builtins`) define protocols and
ship fail-closed null implementations. Production adapters live in
`mas_orchestrator.tools.backends` and are the only classes exported from
that package.

Every adapter: one send is one send (no hidden retries), credentials are
`Secret` values revealed at the moment of use, and every HTTP call goes
through the guarded `HttpTransport`. Private/loopback destinations are
refused unless the operator sets an explicit private-host allow.

## Email

| Class | Transport | Notes |
|---|---|---|
| `SmtpEmailBackend` | `smtplib.SMTP` | Host/port required. Username and password must be supplied together. STARTTLS is mandatory. Loopback/private hosts require `allow_private_hosts=True`. |
| `SendGridEmailBackend` | guarded HTTPS | `api_key` must be a `Secret`. |

## Search

| Class | Notes |
|---|---|
| `BraveSearchBackend` | Query required; `limit` in 1..50. |
| `TavilySearchBackend` | Same input contract. |
| `SearxngSearchBackend` | Same input contract; operator host. |

HTTP 4xx → `ToolFailedError`. Network / HTTP 5xx →
`DependencyUnavailableError`. Empty query → `InvalidRequestError`.

## Database

| Class | Notes |
|---|---|
| `PostgresReadOnlyBackend` | Requires a pool with `connection()`. Independently re-checks statements (`select`/`with`/`show`/`explain`/`values`/`table` only), rejects multi-statement and a forbidden-word list, then runs inside a server-side `READ ONLY` transaction with `statement_timeout`. Default `max_response_bytes` is 256 KiB. |

## Code sandbox

| Class | Notes |
|---|---|
| `RemoteSandboxBackend` | POSTs to `{endpoint_url}/execute`. Optional Bearer `api_key`. Optional `signing_secret` sets `x-mas-signature: sha256=<hex>` over canonical JSON (`separators=(',', ':'), sort_keys=True`). The in-repo `sandbox-runner` verifies that header with `MAS_SANDBOX_HMAC_KEY`. |

The code tool itself (`tools.builtins.code`) **does not** run subprocesses
on the orchestrator host. With no sandbox configured every invocation is a
typed configuration error.

Request body:

```json
{
  "language": "python",
  "source": "...",
  "stdin": "",
  "limits": {
    "timeout_seconds": 10,
    "memory_megabytes": 256,
    "cpu_seconds": 10,
    "max_output_bytes": 262144,
    "network_access": false
  }
}
```

`sandbox-runner` refuses `language` other than `python` and refuses
`network_access: true`. Response fields: `exit_code`, `stdout`, `stderr`,
`timed_out`, `killed_reason`.
