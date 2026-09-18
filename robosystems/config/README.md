# Configuration

Centralized configuration for the platform: environment variables, business rules, pricing, rate limits, and startup validation.

## Rules

**Never call `os.getenv()` directly.** Every environment variable is declared, typed, and defaulted in `env.py`:

```python
from robosystems.config import env

database_url = env.DATABASE_URL
if env.is_production():
    ...
```

`env` also exposes `is_development()`, `is_staging()`, and `is_test()`.

**Never hardcode a Valkey database number.** Databases are allocated in `valkey_registry.py` and referenced by enum member:

```python
from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client

client = create_redis_client(ValkeyDatabase.RATE_LIMITS)
```

| DB | Member | Purpose |
| -- | ------ | ------- |
| 0 | `AUTH` | JWT tokens, API key cache, sessions |
| 1 | `RATE_LIMITS` | Burst protection, download limits |
| 2 | `GRAPH_ROUTING` | Graph client factory (URLs, health) |
| 3 | `SSE` | Real-time event pub/sub and task state |
| 4 | `LOCKS` | Distributed locks (SSO, materialize) |
| 5 | `MCP_CACHE` | MCP tool result cache |
| 6 | `WORKER_QUEUE` | Background task queue |
| 7 | `OPERATION_IDEMPOTENCY` | Extensions operation idempotency cache |

**`.github/configs/graph.yml` is authoritative for tier and instance specs** — instance type, RAM, `databases_per_instance`. `graph_tier.py` reads it; don't duplicate its values in code.

**`config/billing/core.py` is authoritative for pricing and UI display names** (the `display_name` field). Read prices from there, never from a README or a frontend constant.

## Tier names

Tiers are named after the instance the customer gets, not a marketing label. The canonical names are `ladybug-standard`, `ladybug-large`, `ladybug-xlarge`, and `ladybug-shared` (platform-managed public repositories). **"Professional", "Enterprise", and "Premium" are not tier names** — don't introduce them in code, copy, or docs.

`GraphTier` in `graph_tier.py` is the enum; its values must stay in sync with `graph.yml`.

## Modules

| Module | Purpose |
| ------ | ------- |
| `env.py` | Environment variables with types, defaults, and validation |
| `constants.py` | Fixed operational values that never change at runtime |
| `defaults.py` | Defaults for tunables when SSM has no override |
| `tuning.py` / `parameter_store.py` | SSM Parameter Store accessors and client |
| `secrets_manager.py` | AWS Secrets Manager client |
| `billing/core.py` | Subscription tiers, prices, credit allocations, display names |
| `billing/ai.py` | Token-based AI pricing |
| `credits.py` | Credit costs and monthly allocations |
| `rate_limits.py` | Endpoint categories and per-tier burst limits |
| `graph_tier.py` | Tier config read from `.github/configs/graph.yml` |
| `operators.py` | AI Operator model registry and profiles (Bedrock Converse), execution profiles |
| `query_queue.py` | Query queue and admission control |
| `shared_repositories.py` | Shared repository registry, fed by adapter manifests |
| `valkey_registry.py` | Valkey database allocation and client factories |
| `validation.py` | Startup configuration checks |
| `deprovisioning.py` | Graph deprovisioning policy |
| `logging.py`, `openapi_tags.py` | Logging setup, Swagger tag ordering |
| `storage/` | S3 path helpers — see [`storage/README.md`](storage/README.md) |

## Three tiers of configuration

| | Constants | Tunables | Secrets |
| --- | --- | --- | --- |
| Changes at runtime | Never | Yes | Rotated |
| Examples | Protocol limits, business rules, API versions | Cache TTLs, queue sizes, thresholds, timeouts | `DATABASE_URL`, `JWT_SECRET_KEY`, API keys |
| Lives in | `constants.py` | SSM `/tuning/` + `defaults.py` | AWS Secrets Manager |

**Override priority: environment variable > SSM Parameter Store > default.**

### SSM parameters

Feature flags and tunables live under `/robosystems/{env}/`:

```
features/            # Boolean flags — RATE_LIMIT_ENABLED, BILLING_ENABLED, ...
tuning/
  cache/             # Cache TTLs (BALANCE_TTL, JWT_TTL, ...)
  admission/         # Main API thresholds (MEMORY_THRESHOLD, CPU_THRESHOLD)
  lbug_admission/    # LadybugDB thresholds
  queues/            # MAX_SIZE, MAX_CONCURRENT
  circuits/          # Circuit breakers (THRESHOLD, TIMEOUT)
  load_shedding/     # START_PRESSURE, STOP_PRESSURE
  mcp/               # MAX_RESULT_ROWS, MAX_RESULT_SIZE_MB
```

The `{NAME}` segment is UPPER_SNAKE_CASE, identical to the env var name.

```bash
just ssm-list prod tuning
just ssm-get prod tuning/admission/MEMORY_THRESHOLD
just ssm-set prod tuning/cache/BALANCE_TTL 600
```

Changes take effect within the application's cache TTL (a few minutes) — no redeploy.

## Rate limits

Burst protection only. All limits use 60-second windows; **volume is governed by credits, not by rate limits.**

`EndpointCategory` classifies each request path/method. `RateLimitConfig.SUBSCRIPTION_RATE_LIMITS` holds one limit table per tier, and the tables split into two kinds:

- **Dedicated categories** (`DEDICATED_RESOURCE_CATEGORIES`: `GRAPH_READ`, `GRAPH_WRITE`, `GRAPH_QUERY`, `GRAPH_MCP`, `GRAPH_OPERATOR`, `GRAPH_ANALYTICS`) hit the customer's own LadybugDB instance. They scale with the tier's vCPU count and are bucketed **per graph**, so per-graph pricing delivers per-graph throughput. A subgraph draws from its parent's budget (`kg123_dev` buckets as `kg123`).
- **Everything else** lands on infrastructure every tenant shares — OpenSearch, the extensions RDS, the API tier. Those stay flat across tiers and bucketed per user, so a customer can't multiply their share by creating graphs.

Shared repositories (`sec` and its subgraphs) stay user-keyed for the same reason: keying `sec` by graph would put every tenant in one budget.

```python
from robosystems.config.rate_limits import RateLimitConfig, EndpointCategory

category = RateLimitConfig.get_endpoint_category(path, method)
limit, window_seconds = RateLimitConfig.get_rate_limit("ladybug-large", category)
```

`GraphTierConfig.get_api_rate_multiplier(tier)` reports a tier's throughput relative to `ladybug-standard`. It is **derived from `SUBSCRIPTION_RATE_LIMITS`**, not from a standalone config key, so the number `/limits` and `/offering` report cannot drift from what the limiter enforces.

## Credits

Only AI Operator calls consume credits, priced per token. Database operations — queries, imports, backups, MCP tool access — are included with the subscription and return `Decimal("0")` from `CreditConfig.get_operation_cost`. Credit rates are the same for every tier; the tier determines the monthly allocation, not the price per token.

```python
from robosystems.config.billing.ai import AIBillingConfig

AIBillingConfig.TOKEN_PRICING["anthropic_claude_4_sonnet"]
# {"input": Decimal("3"), "output": Decimal("15")}  # credits per 1K tokens
```

## Operators

`operators.py` configures the model registry, the model profiles, and the execution profiles for the AI Operator system. Platform models run through Bedrock's Converse API, so adding or swapping a model is a registry row here, not a code change in the client.

`MODEL_REGISTRY` maps each `OperatorModel` to a `ModelSpec`: the regional inference-profile id on the wire (`us.*`), the rate-card key it bills under (`AIBillingConfig.TOKEN_PRICING`), and the behaviour the client needs to know — whether it takes explicit cache points, whether it accepts sampling parameters, any model-specific request fields, an output cap. `PROFILE_MODELS` names which row `economy`, `balanced`, and `quality` reach; customer surfaces bind to those names, never to a model id.

```python
from robosystems.config import OperatorModel, ModelProfile, OperatorConfig, OperatorExecutionMode

OperatorConfig.resolve_model()                                 # default profile (balanced → Sonnet 5)
OperatorConfig.resolve_model(ModelProfile.ECONOMY)             # a profile
OperatorConfig.resolve_model(OperatorModel.OPUS_5)              # a pinned model
OperatorConfig.resolve_model(operator_type="analyst")          # honors OPERATOR_MODEL_OVERRIDES
OperatorConfig.pricing_key_for("us.anthropic.claude-sonnet-5") # what the meter bills under

profile = OperatorConfig.get_execution_profile(OperatorExecutionMode.STANDARD)
# max_tool_calls=5, timeout_seconds=60, max_input_tokens=100_000

OperatorConfig.validate_configuration()  # {"valid": bool, "issues": [...], "summary": {...}}
```

Resolution is most-specific-wins: an explicit per-call model or profile, then the operator class's entry in `OPERATOR_MODEL_OVERRIDES`, then `DEFAULT_MODEL_CONFIG.default_profile`. An unregistered model or profile raises — the meter must never price an unknown model at some other model's rate. `validate_configuration()` checks every registry row against the rate card at startup.

**The self-hosted model.** A deployment can run the operators on its own model behind any OpenAI-compatible Chat Completions endpoint (Ollama, LM Studio, vLLM, NVIDIA NIM). It is the open-source runtime's alternative to Bedrock and is off by default. Hosted prod leaves it off, and a dedicated tenant turns it on only when it asks. `OPENAI_COMPAT_ENABLED` registers one extra row, `openai-compat`, whose wire id is `OPENAI_COMPAT_MODEL`. With the flag off, that row does not exist, so nothing can resolve to it and the meter refuses it. `OPERATOR_PROFILE_ECONOMY` / `_BALANCED` / `_QUALITY` point a tier at any registered short name, this row included. An unknown name, or a tier pointed at the row while the flag is off, fails the boot. The row bills under `openai_compat` at `OPENAI_COMPAT_CREDITS_PER_1K_INPUT` / `_OUTPUT`, which default to 0 because a self-hosted GPU has no per-token vendor cost. The client (`operations/operators/openai_compat.py`) translates Converse blocks to Chat Completions and back, so the tool loop and the operators never see which provider answered. The `.env.example` block has the variables. Only a `dev` stack runs with no AWS credentials at all. Outside `dev`, the client still checks its AWS credentials at startup, and every non-`dev` deployment runs on AWS anyway (S3, the DynamoDB graph registries).

Execution modes are `QUICK`, `STANDARD`, `EXTENDED`, and `STREAMING`; each has an `ExecutionProfile` bounding tool calls, tokens, and timeout. Re-point a profile in `PROFILE_MODELS` to move every caller of that profile; pin a single operator class by adding an entry to `OPERATOR_MODEL_OVERRIDES`.

## Startup validation

`validation.py` checks configuration when the app boots — stricter in production than in development.

```python
from robosystems.config.validation import EnvValidator

EnvValidator.validate_required_vars(env)  # raises ConfigValidationError on missing/insecure config
EnvValidator.validate_startup(env)        # returns False and logs warnings on soft problems
```

## Config-as-code

Business configuration lives in code, not in the database: version controlled, code reviewed, tested in CI, no runtime surprises. When adding configuration, put it in the module that owns the concern rather than reading an env var at the call site, and give it a name instead of a magic number.
