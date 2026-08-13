# Billing Middleware

Two things live here: a process-local credit balance cache (`cache.py`) and the
subscription/provisioning gates that routers call before doing expensive work
(`enforcement.py`). The credit *ledger* itself — balances, transactions,
consumption — is in `operations/graph/` (`CreditService`), not here.

## The rule everyone gets wrong

**Only AI operations consume credits. Every database operation is free.**

AI operations means Anthropic Claude calls through AWS Bedrock, billed on
actual token counts. Queries, analytics, imports, ingestion, materialization,
backups, restores, connection syncs, MCP calls, and plain API calls are all
included in the subscription tier and consume nothing. There is no credit
decorator to add to a query endpoint, and adding one would be a bug.

`CreditService.consume_ai_tokens()` is the only credit-consuming entry point in
the codebase. If you are writing a new endpoint and reaching for credits, first
check that it actually calls a model.

**Storage is limit-enforced, not metered.** There is no storage-to-credits and
no storage-to-USD path in the code. `GraphCredits.check_storage_limit()` — a
method on the model in `models/core/graph/graph_credits.py`, not on
`CreditService` — reports usage against the graph's `storage_limit_gb` (or
`storage_override_gb` when an admin has set one) and returns usage, limit,
percentage, and the `within_limit` / `approaching_limit` / `needs_warning`
booleans. It never computes a charge. Exceeding the limit gates the write path;
it does not bill.

## Credits

Monthly allocations by tier, from `config/billing/core.py`
(`monthly_credit_allocation`):

| Tier               | Credits/month |
| ------------------ | ------------- |
| `ladybug-standard` | 8,000         |
| `ladybug-large`    | 32,000        |
| `ladybug-xlarge`   | 100,000       |

Token pricing is in `config/billing/ai.py` (`AIBillingConfig.TOKEN_PRICING`),
which is authoritative. Today it holds a single tier — `anthropic_claude_4_sonnet`
at 3 credits per 1K input tokens and 15 per 1K output tokens — with a minimum
charge applied per operation via `apply_minimum_charge()`.

Consumption happens *after* the AI call, from the response's real token counts:

```python
from robosystems.operations.graph import CreditService

credit_service = CreditService(session)
usage = anthropic_response.usage
credit_service.consume_ai_tokens(
    graph_id="kg1a2b3c",
    input_tokens=usage.input_tokens,
    output_tokens=usage.output_tokens,
    model="us.anthropic.claude-sonnet-4-6",
    operation_description="operator query",
    user_id="user_456",
)
```

Note the argument is `operation_description` — free text for the transaction
record — not an `operation_type`. `model` takes the Bedrock inference-profile id
and is mapped through a hardcoded id → `TOKEN_PRICING`-key table inside
`consume_ai_tokens`.

**An unrecognised model does not fail — it bills at Sonnet rates.** The lookup
defaults to `anthropic_claude_4_sonnet`, and if even that key were missing it
falls back to a literal `3`/`15`, logging a warning either way. So adding a
model without adding its mapping mis-bills quietly rather than erroring. When
you introduce a new model, update the map in `consume_ai_tokens` and
`TOKEN_PRICING` together.

## Enforcement

`enforcement.py` holds the pre-flight gates. All three take a session; the first
two return a `(bool, error)` tuple rather than raising, so the caller controls
the error shape. `require_graph_access` is the exception — it raises.

`check_can_provision_graph(user_id, requested_tier, session) -> (bool, error)`
resolves the user's organization and asks its `BillingCustomer` whether it may
provision. **Billing is org-level, not user-level** — a user with no `OrgUser`
row cannot provision anything, and that is the failure you will see first in a
misconfigured local environment. The check also respects `BILLING_ENABLED`.

`check_graph_subscription_active(graph_id, session) -> (bool, error)` exists and
is exported, but **nothing calls it** — it is not a live gate. Its status→message
table (pending, paused, canceled, past due, unpaid, upgrading) duplicates the one
inside `require_graph_access`, which is where subscription status is actually
enforced. Treat it as dead surface pending removal; do not reach for it when
adding a new gate, or you will add a check that never fires.

`require_graph_access(graph_id, session, require_write=False)` returns the
`Graph` or raises. This is the billing-side gate; the *authorization* side —
whether the user's role permits writes — is `require_graph_write_role` in
[`../auth/`](../auth/README.md). Both apply.

Subscription lookups are cached; call `invalidate_subscription_cache(graph_id)`
after any change to a graph's subscription or its status.

## Cache

`CreditCache` (exported as the `credit_cache` singleton) is a **process-local,
thread-safe, in-memory TTL cache** — a plain dict plus a lock, not Valkey. Each
API process holds its own copy, so an invalidation in one process does not
reach the others; entries converge only as their TTLs expire. Treat a cached
balance as advisory and read through to the database before anything that must
be exact.

Key namespaces:

```
graph_credit:{graph_id}                # graph credit balance
credit_summary:{graph_id}              # usage summary
```

Two further prefixes are declared in `cache.py` but can never hold anything, and
are listed here only so you do not go looking for their contents:
`shared_credit:{user_id}:{repository}` has no setter or getter at all (shared
repository credits read straight through to the database, uncached), and
`op_cost:{operation_type}` is written on every `CreditService` construction with
a table of zeros that nothing reads back.

```python
from robosystems.middleware.billing import credit_cache

credit_cache.cache_graph_credit_balance(
    graph_id="kg1a2b3c",
    balance=Decimal("50000"),
    graph_tier="standard",
)
cached = credit_cache.get_cached_graph_credit_balance("kg1a2b3c")
if cached is not None:
    balance, graph_tier = cached
credit_cache.invalidate_graph_credit_balance("kg1a2b3c")
```

`cache_graph_credit_balance` takes exactly `(graph_id, balance, graph_tier)` —
there is no multiplier argument, and no multiplier is stored. The getter returns
a `(Decimal, str)` tuple or `None` on a miss, not a bare balance.

**Consumption invalidates; it does not update in place.** Every consumption path
in `credit_service.py` calls `invalidate_graph_credit_balance`, dropping the
balance entry and its summary, so the next read goes to the database.
`update_cached_balance_after_consumption()` would adjust in place and preserve
the TTL, but it has no callers — do not reason about cache freshness from it.

TTLs resolve through `TuningConfig`, which derives its environment-variable name
from the SSM path. The operation-cost TTL is a fixed constant read directly from
`CacheDefaults`, so neither its env var nor its SSM path has any effect:

| Setting        | SSM key             | Env override              | Default |
| -------------- | ------------------- | ------------------------- | ------- |
| Balance TTL    | `cache/BALANCE_TTL` | `TUNING_CACHE_BALANCE_TTL`| 300 s   |
| Summary TTL    | `cache/SUMMARY_TTL` | `TUNING_CACHE_SUMMARY_TTL`| 600 s   |
| Operation cost | *(not tunable)*     | *(not tunable)*           | 3600 s  |

The `CREDIT_BALANCE_CACHE_TTL` / `CREDIT_SUMMARY_CACHE_TTL` /
`CREDIT_OPERATION_COST_CACHE_TTL` attributes on `env` are read by nothing; set
the `TUNING_*` names above instead.

`BILLING_ENABLED` gates the enforcement checks; it is off in local development,
which is why provisioning works there without a subscription.

## Inspecting state

```bash
just admin dev stats
just admin dev subscriptions list
just admin dev credits --help
just admin dev invoices list
```

## Related

- `robosystems/operations/graph/` — `CreditService`, the credit ledger
- `robosystems/config/billing/` — plans, prices, allocations, AI token pricing
- [`../auth/README.md`](../auth/README.md) — `require_graph_write_role`
- [`../rate_limits/`](../rate_limits/) — burst limiting (separate from credits)
