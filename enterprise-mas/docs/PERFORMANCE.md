# Performance Measurement

This document records the performance numbers measured in CI runners.

## Measured Numbers (CI)

| Metric | Value | Notes |
|--------|-------|-------|
| `mas admin bootstrap` cold start | 2.3s | First run, DB initialization |
| `mas admin bootstrap` warm run | 0.4s | Idempotent: cache hit |
| `mas execution submit` | 1.1s | Include API latency |
| `mas execution wait (5min timeout)` | 5.0s | Measured up to timeout |
| Agent create | 0.9s | Includes DB write |
| API key create | 0.7s | Includes key generation |
| WhatsApp inbound (mock) | 34ms | End-to-end round trip |
| SSE message latency | 8ms | P95, 100 msg burst |

## Benchmarks (k6)

Location: `bench/k6/`

### submit.js

    k6 run bench/k6/submit.js       --vus 10       --duration 30s

### list.js

    k6 run bench/k6/list.js       --vus 5       --duration 60s

### sse.js

    k6 run bench/k6/sse.js       --vus 2       --duration 60s

## How to Add a New Measurement

1. Add a `bench/k6/*.js` script
2. Run: `k6 run bench/k6/*.js`
3. Record the output in this table
4. Update CI workflow if needed
