# Credits & Billing

In RoboSystems, only AI operations consume credits — every database, graph, MCP, Cypher, import, sync, backup, and schema operation is free. Credits exist solely to meter the external model-inference cost of AI Operators (Anthropic Claude via AWS Bedrock), and each graph carries a monthly credit allocation tied to its tier.

## Table of Contents

- [The Credit Model](#the-credit-model)
- [Monthly Allocations by Tier](#monthly-allocations-by-tier)
- [Checking Your Credit Balance](#checking-your-credit-balance)
- [Reviewing Usage and Transaction History](#reviewing-usage-and-transaction-history)
- [Public Pricing and Offerings](#public-pricing-and-offerings)
- [Shared Repository Credits](#shared-repository-credits)
- [Alerts and Low-Balance Thresholds](#alerts-and-low-balance-thresholds)
- [Storage Is Billed Separately](#storage-is-billed-separately)
- [Organizations and Account Limits](#organizations-and-account-limits)
- [Rate Limits](#rate-limits)
- [Admin CLI Reference](#admin-cli-reference)
- [Related Documentation](#related-documentation)

## The Credit Model

The model has one rule worth memorizing: **credits meter AI, and nothing else.**

### What Consumes Credits

Only operations that call an external AI model consume credits. In production, that means AI Operators running Claude (Anthropic) through AWS Bedrock — the natural-language Cypher operator, the autonomous chart-of-accounts mapping operator, and any other operator-driven workflow. Each call is metered on the tokens it actually uses once it completes, then debited from the graph's credit pool.

### What Is Free

Everything that does not call an AI model is free and unmetered by credits:

- Cypher and graph queries (REST and MCP)
- GraphQL extension reads
- MCP tool calls (`read-graph-cypher`, `query-graphql`, `search-documents`, schema introspection, and the rest)
- Data imports, materialization, and connection syncs (SEC, QuickBooks)
- Backups and restores
- Schema operations and subgraph management
- Connection setup and credential management

**Free does not mean unthrottled.** Free operations are still subject to burst rate limiting (short rolling windows). Credits and rate limits are independent systems — running out of one has no effect on the other.

### Token-Based Pricing

AI operations are priced on the tokens consumed by the model call, not as a flat per-call fee. After an operation completes, the actual input and output token counts are converted to credits:

| Direction | Rate |
| --- | --- |
| **Input tokens** | 3 credits per 1,000 tokens |
| **Output tokens** | 15 credits per 1,000 tokens |
| **Minimum charge** | 1 credit per AI operation (every call rounds up to at least 1) |

The production model of record is Claude Sonnet served via AWS Bedrock. A typical AI Operator call lands around 38 credits — useful as a rough planning figure, but the charge is always the real measured token cost, never a fixed estimate.

## Monthly Allocations by Tier

Each graph has a credit pool that receives a fresh allocation every month based on its tier. The technical tier names and their authoritative display names are below. (Use **Standard / Large / XLarge** — not marketing labels.)

| Technical name | Display name | Instance | Price | Monthly credits | Approx. agent calls/month |
| --- | --- | --- | --- | --- | --- |
| `ladybug-standard` | **Standard** | Dedicated m7g.large | $149/mo | **8,000** | ~200 (~7/day) |
| `ladybug-large` | **Large** | Dedicated r7g.large | $299/mo | **32,000** | ~800 (~27/day) |
| `ladybug-xlarge` | **XLarge** | Dedicated r7g.xlarge | $699/mo | **100,000** | ~2,600 (~87/day) |

The "approx. agent calls/month" column is derived from the ~38-credit typical-call figure and is an estimate for planning only — actual usage depends on the size of each call.

A graph's **current balance** is its monthly allocation minus the credits consumed so far in the current calendar month. Subgraphs do not receive their own allocation — they draw from the parent graph's pool, so checking a subgraph's credits reports the parent pool's numbers. The single source of truth for these allocations is the [Billing Configuration](https://github.com/RoboFinSystems/robosystems/tree/main/robosystems/config/billing) in the codebase.

## Checking Your Credit Balance

A graph's credit summary is available over REST. All examples use the local stack at `http://localhost:8000` and read the API key from `.local/config.json` (created by `just demo-user`).

```bash
curl -X GET "http://localhost:8000/v1/graphs/kg1a2b3c4d5e/credits" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json"
```

The response is a credit summary for the graph:

```json
{
  "graph_id": "kg1a2b3c4d5e",
  "graph_tier": "ladybug-standard",
  "current_balance": 7620.0,
  "monthly_allocation": 8000.0,
  "consumed_this_month": 380.0,
  "transaction_count": 12,
  "usage_percentage": 4.75,
  "last_allocation_date": "2026-06-01T00:00:00+00:00"
}
```

**Note:** `consumed_this_month`, `current_balance`, and `usage_percentage` are computed from the consumption transactions recorded in the current calendar month — they reflect the live ledger, not a single stored counter. Storage limits are intentionally not part of this response (see [Storage Is Billed Separately](#storage-is-billed-separately)).

## Reviewing Usage and Transaction History

### Credit Transactions

To see the individual credit movements behind the balance, request the transaction history. It supports filtering by type and date range:

```bash
curl -X GET "http://localhost:8000/v1/graphs/kg1a2b3c4d5e/credits/transactions?transaction_type=consumption&start_date=2026-06-01&limit=50" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

The response carries a `transactions[]` list and a per-operation `summary` (with `total_amount`, `transaction_count`, `average_amount`, `first_transaction`, and `last_transaction` for each operation type), plus `total_count`, `filtered_count`, and the resolved `date_range`.

Each credit transaction has a type drawn from a fixed set:

| Type | Meaning |
| --- | --- |
| `allocation` | The monthly credit grant for the tier |
| `consumption` | An AI operation debit (token-based) |
| `bonus` | A referral or admin grant |
| `refund` | A reversal of a prior consumption |

### Usage Analytics

For a combined view of credits alongside storage, performance, and event metrics, use the analytics endpoint:

```bash
curl -X GET "http://localhost:8000/v1/graphs/kg1a2b3c4d5e/analytics/usage" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

This returns a usage report that includes both a `credit_summary` and a `storage_summary`, so it is the right call when you want the storage and credit pictures together. A graph-level metrics endpoint is also available at `GET /v1/graphs/{graph_id}/analytics`.

For the exact request parameters and response schemas of every endpoint on this page, see the live OpenAPI reference at [api.robosystems.ai/docs](https://api.robosystems.ai/docs) (or `http://localhost:8000/docs` against a local stack) rather than re-deriving them here.

## Public Pricing and Offerings

Public pricing — subscription tiers, per-model token pricing, and the list of included (free) operations — is exposed without needing a graph scope:

```bash
curl -X GET "http://localhost:8000/v1/offering" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

The response includes the subscription tiers, a `token_pricing` block per model, and an `included_operations` list enumerating the operations that do not consume credits. This is the authoritative, machine-readable source for current pricing.

## Shared Repository Credits

Shared public repositories — such as the SEC XBRL corpus — use a **separate per-user credit pool**, not a graph's pool. When you query a shared repository identifier (for example `sec`) at the credits endpoint, the response transparently routes to that per-user repository pool and reports a synthetic tier of `sec-repository`:

```bash
curl -X GET "http://localhost:8000/v1/graphs/sec/credits" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

This keeps shared-repository AI usage metered independently from the credits attached to your own graphs. As with graph pools, only AI operations against the repository consume these credits — browsing and querying the repository data is free.

## Alerts and Low-Balance Thresholds

The platform tracks balance against three thresholds so usage can be flagged before the pool runs dry:

| State | Remaining balance |
| --- | --- |
| **Low** | 20% of the monthly allocation remaining |
| **Critical** | 5% remaining |
| **Exhausted** | 0% remaining |

Because credits only gate AI operations, an exhausted pool stops AI Operator calls but leaves every free operation — queries, syncs, imports, backups — fully available.

## Storage Is Billed Separately

Storage is a distinct axis from credits. Each tier carries its own storage limits (with an optional override and auto-expand behavior), tracked independently of the credit pool. Storage is **never paid for with credits**, and storage limit fields are deliberately absent from the `/credits` response.

To see storage usage, use the analytics endpoint described above — storage figures appear in its `storage_summary`, not in the credit balance. Tier storage limits are defined alongside the rest of the billing configuration; see the [Billing Configuration](https://github.com/RoboFinSystems/robosystems/tree/main/robosystems/config/billing) and [Graph Configuration](https://github.com/RoboFinSystems/robosystems/blob/main/.github/configs/graph.yml) in the codebase.

## Organizations and Account Limits

Every user belongs to an **organization** — the account that owns your graphs and carries billing. Today an organization is a single-user account: there is one organization per user, and it is the entity your graph subscriptions and billing attach to. (Multi-user organizations are not yet available.)

The organization carries one account-level limit, independent of credits and storage: a **maximum number of graphs** it can hold (`max_graphs`, defaulting to the configured `ORG_GRAPHS_DEFAULT_LIMIT`). This caps how many graphs the account can provision at once — distinct from each graph's own storage limit and credit pool. Organization-level usage — graphs against the cap and aggregate activity — is available through the organization endpoints; see the [OpenAPI reference](https://api.robosystems.ai/docs) for the exact request and response shapes.

## Rate Limits

Rate limits are the third metering axis, alongside credits and storage — and the only one that throttles *free* operations. They are **burst-focused**: short sliding windows sized to absorb normal bursts while protecting shared infrastructure, rather than long-horizon quotas.

- Limits are defined per **endpoint category** (queries, MCP, search, agent calls, and so on) crossed with **subscription tier** — higher tiers get higher limits.
- A small **burst multiplier** (1.2×) allows brief spikes above the steady-state limit.
- Tier scaling: the Large and XLarge tiers carry higher API rate multipliers than Standard (roughly 1.5× and 2.5× respectively).
- Every rate-limited response carries standard rate-limit headers (limit, remaining, reset), so a client can back off gracefully.

Rate limits and credits are **independent** — hitting a rate limit does not consume credits, and an exhausted credit pool does not change your rate limits. Shared repositories carry their own per-plan rate limits; see [Shared Repositories](Shared-Repositories).

## Admin CLI Reference

For operators of a self-hosted deployment, the admin CLI exposes credit-pool inspection and management. The first argument is the environment (`dev`, `staging`, or `prod`):

```bash
just admin dev credits list          # List graph credit pools
just admin dev credits get <graph_id>  # Detail for one graph
just admin dev credits bonus         # Grant bonus credits
just admin dev credits analytics     # Tier-level analytics
just admin dev credits health        # Credit system health

just admin dev credits repos list    # Shared-repository credit pools
just admin dev credits repos get
just admin dev credits repos bonus
```

## Related Documentation

**Wiki Guides:**
- **[Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy)** - How credits scope per graph, and how subgraphs share the parent pool
- **[AI Operators and MCP](AI-Operators-and-MCP)** - The AI Operators that consume credits, and the free MCP tool surface
- **[Architecture Overview](Architecture-Overview)** - Platform architecture, tiers, and the operations layer

**Codebase Documentation:**
- **[Billing Configuration](https://github.com/RoboFinSystems/robosystems/tree/main/robosystems/config/billing)** - Subscription plans, AI token pricing, and storage limits
- **[Shared Repository Registry](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/config/shared_repositories.py)** - Repository plans, rate limits, and credit costs
- **[API Documentation](https://api.robosystems.ai/docs)** - API reference with machine-readable OpenAPI spec

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
