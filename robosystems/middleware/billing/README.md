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
no storage-to-USD path in the code. `CreditService.check_storage_limit()`
reports usage against the graph's `storage_limit_gb` (with an optional admin
override) and returns a status plus recommendations — it never computes a
charge. Exceeding the limit gates the write path; it does not bill.

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
record — not an `operation_type`. `model` takes the Bedrock inference-profile
id and is mapped internally to the `TOKEN_PRICING` key. A model that has no
pricing entry is a configuration error worth surfacing loudly, not defaulting
silently.

## Enforcement

`enforcement.py` holds the pre-flight gates. All three take a session and
return a tuple rather than raising, so the caller controls the error shape.

`check_can_provision_graph(user_id, requested_tier, session) -> (bool, error)`
resolves the user's organization and asks its `BillingCustomer` whether it may
provision. **Billing is org-level, not user-level** — a user with no `OrgUser`
row cannot provision anything, and that is the failure you will see first in a
misconfigured local environment. The check also respects `BILLING_ENABLED`.

`check_graph_subscription_active(graph_id, session) -> (bool, error)` looks up
the graph's `BillingSubscription` and maps its status to a specific message
(pending, paused, canceled, past due, unpaid, upgrading). Two behaviors matter:
a graph with *no* subscription is allowed through when `BILLING_ENABLED` is
false, and the `UPGRADING` status permits reads while blocking writes.

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
shared_credit:{user_id}:{repository}   # shared repository credits
credit_summary:{graph_id}              # usage summary
op_cost:{operation_type}               # operation cost
```

```python
from robosystems.middleware.billing import credit_cache

credit_cache.cache_graph_credit_balance(
    graph_id="kg1a2b3c",
    balance=Decimal("50000"),
    multiplier=Decimal("1.0"),
    graph_tier="standard",
)
balance = credit_cache.get_cached_graph_credit_balance("kg1a2b3c")
credit_cache.invalidate_graph_credit_balance("kg1a2b3c")
```

`update_cached_balance_after_consumption()` adjusts a cached balance in place
and preserves the remaining TTL, so a consumption does not reset freshness.

TTLs resolve through `TuningConfig` and are SSM-tunable at runtime, except the
operation-cost TTL, which is a fixed constant because costs rarely change:

| Setting                           | SSM key                    | Default |
| --------------------------------- | -------------------------- | ------- |
| `CREDIT_BALANCE_CACHE_TTL`        | `cache/BALANCE_TTL`        | 300 s   |
| `CREDIT_SUMMARY_CACHE_TTL`        | `cache/SUMMARY_TTL`        | 600 s   |
| `CREDIT_OPERATION_COST_CACHE_TTL` | `cache/OPERATION_COST_TTL` | 3600 s  |

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
