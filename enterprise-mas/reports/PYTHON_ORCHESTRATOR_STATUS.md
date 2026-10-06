# Python Orchestrator — সম্পূর্ণ (Final Status)

অবস্থান: `/home/user/python-orchestrator/`

## পরিসংখ্যান

| মেট্রিক | মান |
|---|---|
| Source modules | 155 |
| Test files | 45 |
| Source LOC | 38,296 |
| Test LOC | 16,396 |
| মোট টেস্ট | **1,697 passed** |
| Shortening (`...` / omitted code) | **NONE** (AST দিয়ে যাচাই) |

## টেস্ট সুইট ভিত্তিক ফলাফল

| Suite | Tests |
|---|---|
| `tests/contract` | 71 |
| `tests/unit` | 1,343 |
| `tests/integration` | 25 |
| `tests/resilience` | 85 |
| `tests/e2e` | 58 |
| `tests/security` | 115 |

## রান করা কমান্ড ও ফলাফল

```
$ python --version
Python 3.13.14

$ pip --version
pip 26.1.2 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)

$ python3 -m compileall -q src tests
COMPILEALL OK

$ python3 -m ruff check src tests
All checks passed!

$ python3 -m ruff format --check src tests
204 files already formatted

$ PYTHONPATH=src python3 -m mypy src/mas_orchestrator
Success: no issues found in 155 source files

$ PYTHONPATH=src python3 -m pytest tests -q
1697 passed in 4.08s
```

## এই ধাপে লেখা নতুন টেস্ট

| ফাইল | Tests | যা কভার করে |
|---|---|---|
| `tests/integration/test_grpc_identity.py` | 25 | Rust→Python identity path: `extract_call_metadata` → `reconcile_tenant_scope` → `build_execution_context` → `build_outbound_metadata` |
| `tests/resilience/test_retry_and_backoff.py` | 23 | Retry policy, backoff, `Retry-After`, non-idempotent, cancellation |
| `tests/resilience/test_shutdown.py` | 22 | §24 shutdown order; abandonment কখনো silently ঘটে না |
| `tests/resilience/test_provider_failover.py` | 40 | Bounded failover; retryable/terminal classification; cancellation |
| `tests/e2e/test_full_execution.py` | 32 | Routing → model → tool → permission → ledger → outcome |
| `tests/e2e/test_workflow_execution.py` | 26 | Graph: linear, condition, parallel, loop, brakes, tenancy |

## এই ধাপে পাওয়া ও ঠিক করা production bug

### 1. Failover ভুল error রিপোর্ট করছিল (`models/failover.py`)

**সমস্যা:** chain exhausted হলে সবসময় **প্রথম** error raise করা হতো।
যদি chain-এর মাঝে কোনো **non-retryable** (terminal) error পাওয়া যেত — যেমন `AuthenticationError` — তবুও আগের transient error (যেমন `ModelRateLimitedError`) রিপোর্ট করা হতো।

এটি বিপজ্জনক: অপারেটর দেখত "rate limited" (যা ক্ষণস্থায়ী, পরে আবার চেষ্টা করলে হবে), অথচ আসল কারণ ছিল rejected credential — যা কখনোই retry-তে ঠিক হবে না। ভুল কারণ নির্দেশ করায় সমস্যা সমাধানে বিলম্ব হতো।

**সমাধান:** এখন যা raise হয় —
- সব model retryable error-এ fail করলে → **প্রথম** error (সবগুলো একইরকম উপসর্গ; প্রথমটিই মূল কারণ)।
- কোনো **non-retryable** error থামিয়ে দিলে → **সেই** error (এটিই কারণ যে আর কোনো চেষ্টা কাজে দেবে না)।

দুই ক্ষেত্রেই `details["failover_attempts"]`-এ পুরো per-model history থাকে — কিছুই হারায় না।

### 2. `budget_from_deadline(None)` নথিভুক্ত ছিল না (`models/retry.py`)

Unknown deadline-এ `0.0` ফেরত দেয় (retry বন্ধ)। এটি সঠিক — retry একটি dependency-তে চাপ বাড়ায়, তাই তার জন্য পরিচিত budget দরকার, default নয়। কিন্তু কারণ লেখা ছিল না; এখন inline comment-এ reasoning যোগ করা হয়েছে।

## নতুন টেস্ট লেখার সময় শেখা API সত্য (ভবিষ্যতের জন্য)

- `ModelRegistry.register()` **কোনো model advertise করে না** — `register_model(ref, provider_id)` আলাদা করে ডাকতে হয়।
- `RoutingPolicy.preferred_models` দিতে হবে; নাহলে chain alphabetically সাজানো হয় (যেমন `anthropic/...` আগে)।
- `AgentNodeHandler`-এ agent কে `(prompt, state)` — **দুই** positional argument দিয়ে ডাকে।
- Workflow node-এর input আসে `node.config` থেকে, `${dotted.path}` substitution দিয়ে; plain variable নয়।
- `MAX_WORKFLOW_STEPS = 200`, `NodeLimits.max_steps` default `20` → ১০টির বেশি node হলে definition reject হয়।
- Deadline পরীক্ষা করতে **দুই** hop লাগে: terminal successor থাকলে `_advance` সাথে সাথে finish করে, deadline চেক হয় না।
- Cancellation between steps দেখাতে প্রথম response-এ tool call থাকতে হবে — text answer দিলে run শেষ হয়ে যায়।
- `slots=True` dataclass-এ method assign করা যায় না (যেমন `model.complete = ...`)।
- Task cancel করার আগে সে যদি কোনো await-এ না পৌঁছায়, তবে cancel সাথে সাথে complete করে → "stuck task" টেস্ট ভুল কারণে pass করবে। আগে `await asyncio.sleep(0)` দিয়ে প্রথম await point-এ পৌঁছাতে হবে।
