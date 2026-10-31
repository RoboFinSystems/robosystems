# Credits & Billing

In RoboSystems, only AI operations consume credits — every database, graph, MCP, Cypher, import, sync, backup, and schema operation is free. Credits exist solely to meter the external model-inference cost of AI Operators (models served through AWS Bedrock), and each graph carries a monthly credit allocation tied to its tier.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development.md).

## Table of Contents

- [The Credit Model](#the-credit-model)
- [Monthly Allocations by Tier](#monthly-allocations-by-tier)
- [Checking Your Credit Balance](#checking-your-credit-balance)
- [Reviewing Usage and Transaction History](#reviewing-usage-and-transaction-history)
- [Public Pricing and Offerings](#public-pricing-and-offerings)
- [Shared Repository Credits](#shared-repository-credits)
- [Alerts and Low-Balance Thresholds](#alerts-and-low-balance-thresholds)
- [Storage Is Not Paid For With Credits](#storage-is-not-paid-for-with-credits)
- [Organizations and Account Limits](#organizations-and-account-limits)
- [Billing Endpoints](#billing-endpoints)
- [Rate Limits](#rate-limits)
- [Self-hosted deployments](#self-hosted-deployments)
- [Related Documentation](#related-documentation)

## The Credit Model

The model has one rule worth memorizing: **credits meter AI, and nothing else.**

### What Consumes Credits

Only operations that call an external AI model consume credits. In production, that means AI Operators calling a model through AWS Bedrock — by default on the `balanced` model profile, Claude Sonnet 5.5 today — the analyst operator (which answers questions from a graph's read-only tools), the author operator (which makes changes a request asks for, on the `quality` profile, Claude Opus 5.5 today), the chart-of-accounts mapping operator, and any other operator-driven workflow. Each call is metered on the tokens it actually uses once it completes, then debited from the graph's credit pool.

### What Is Free

Everything that does not call an AI model is free and unmetered by credits:

- Cypher and graph queries (REST and MCP)
- GraphQL extension reads
- MCP tool calls (`read-graph-cypher`, `query-graphql`, `search-documents`, schema introspection, and the rest)
- Data imports, materialization, and connection syncs (SEC, QuickBooks)
- Backups
- Schema operations and subgraph management
- Connection setup and credential management

**Free does not mean unthrottled.** Free operations are still subject to burst rate limiting (short rolling windows). Credits and rate limits are independent systems — running out of one has no effect on the other.

### Token-Based Pricing

AI operations are priced on the tokens consumed by the model call, not as a flat per-call fee. Nothing is reserved up front: the charge is computed *after* the model responds, from the input and output token counts it actually reported, at a per-1,000-token rate for each direction. A **minimum charge of 1 credit** then applies, so a very small call still costs something — but a call that consumed nothing costs nothing rather than being rounded up. An unregistered model raises at billing time rather than guessing a rate, so a model has to be priced before it can be used.

The current rates are served as `operation_costs.token_pricing` from `GET /v1/offering`, which is where to read them rather than from a copy on this page. What a call costs depends on how many tokens it uses, so the way to plan is to run representative calls and read their actual charges from the transaction history (see [Reviewing Usage and Transaction History](#reviewing-usage-and-transaction-history)); the charge is always the real measured token cost, never a fixed estimate.

### Capping a Run

An operator request can carry `max_credits`, a soft ceiling on what that one run spends. It is checked before each model call, so the first call always runs and the run can end slightly above the ceiling; once reached, the operator stops calling tools and answers from what it has. The author operator applies a default of 750 credits when a request sets none, so every write run is bounded; a request's own `max_credits`, higher or lower, takes precedence. See [AI Operators & MCP](AI-Operators-and-MCP.md#the-operator-endpoint).

A run's first model call after a few idle minutes costs more than the ones that follow, because the operator's instructions and the graph's schema are written to the model's prompt cache on that call and read from it afterwards.

## Monthly Allocations by Tier

Each graph has a credit pool that receives a fresh allocation every month based on its tier. The technical tier names and their authoritative display names are below. (Use **Standard / Large / XLarge** — not marketing labels.)

The three customer tiers are **Standard** (`ladybug-standard`), **Large** (`ladybug-large`) and
**XLarge** (`ladybug-xlarge`). Their prices, monthly credit allocations and instance sizes are
deliberately **not restated here** — they are served live from configuration, so a copy on this page
could only ever be a copy that goes stale. Prices and monthly credits come from
[`robosystems/config/billing/core.py`](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/config/billing/core.py);
instance sizes and limits from
[`.github/configs/graph.yml`](https://github.com/RoboFinSystems/robosystems/blob/main/.github/configs/graph.yml),
the file that provisions the infrastructure. Read them live from:

- **`GET /v1/offering`** — `monthly_price_per_graph`, `monthly_credits_per_graph`, `infrastructure`,
  and the per-tier limits, all computed from the authoritative config
- **[robosystems.ai/pricing](https://robosystems.ai/pricing)** — the public pricing page

To estimate how many operator calls an allocation covers, divide the monthly credits by the average
charge of your own calls, from the transaction history's per-operation `summary`. That is a planning
estimate only; the charge is always the real measured token cost.

A graph's **current balance** is its monthly allocation minus the credits consumed so far in the current calendar month. Subgraphs do not receive their own allocation — they draw from the parent graph's pool, so checking a subgraph's credits reports the parent pool's numbers. The single source of truth for these allocations is the [Billing Configuration](https://github.com/RoboFinSystems/robosystems/tree/main/robosystems/config/billing) in the codebase.

## Checking Your Credit Balance

A graph's credit summary is available over REST. All examples call `https://api.robosystems.ai` and send your API key as `X-API-Key`:

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai
```

```bash
curl -X GET "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5e6f7a8b/credits" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json"
```

The response is a credit summary for the graph:

```json
{
  "graph_id": "kg1a2b3c4d5e6f7a8b",
  "graph_tier": "ladybug-standard",
  "current_balance": 7620.0,
  "monthly_allocation": 8000.0,
  "consumed_this_month": 380.0,
  "transaction_count": 12,
  "usage_percentage": 4.75,
  "last_allocation_date": "2026-06-01T00:00:00+00:00"
}
```

**Note:** `monthly_allocation` is whatever the graph's tier grants — the `8000.0` above is one tier's figure, not a platform constant. `consumed_this_month`, `current_balance`, and `usage_percentage` are computed from the consumption transactions recorded in the current calendar month — they reflect the live ledger, not a single stored counter. Storage limits are intentionally not part of this response (see [Storage Is Not Paid For With Credits](#storage-is-not-paid-for-with-credits)).

## Reviewing Usage and Transaction History

### Credit Transactions

To see the individual credit movements behind the balance, request the transaction history. It supports filtering by type and date range:

```bash
curl -X GET "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5e6f7a8b/credits/transactions?transaction_type=consumption&start_date=2026-06-01&limit=50" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

The response carries a `transactions[]` list and a per-operation `summary` (with `total_amount`, `transaction_count`, `average_amount`, `first_transaction`, and `last_transaction` for each operation type), plus `total_count`, `filtered_count`, and the resolved `date_range`.

Each credit transaction has a type drawn from a fixed set:

| Type | Meaning |
| --- | --- |
| `allocation` | The monthly credit grant for the tier |
| `consumption` | An AI operation debit (token-based) |
| `bonus` | A referral or admin grant |
| `refund` | A reversal of a prior consumption |
| `expiration` | The monthly reset, which clears an unspent balance |

### Usage Analytics

For a combined view of credits alongside storage, performance, and event metrics, use the analytics endpoint:

```bash
curl -X GET "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5e6f7a8b/usage" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

This returns a usage report that includes both a `credit_summary` and a `storage_summary`, so it is the right call when you want the storage and credit pictures together. A graph-level metrics endpoint is also available at `GET /v1/graphs/{graph_id}/metrics`.

For the exact request parameters and response schemas of every endpoint on this page, see the API reference at [robosystems.ai/docs/api](https://robosystems.ai/docs/api) rather than re-deriving them here.

## Public Pricing and Offerings

Public pricing — subscription tiers, per-model token pricing, and the list of included (free) operations — is exposed without a graph scope and without a key. It needs no sign-in at all, so a program can read the current offering before anyone has an account:

```bash
curl -X GET "https://api.robosystems.ai/v1/offering"
```

The response includes the subscription tiers, a `token_pricing` block per model, and an `included_operations` list enumerating the operations that do not consume credits. This is the authoritative, machine-readable source for current pricing.

## Shared Repository Credits

Shared public repositories — such as the SEC XBRL corpus — use a **separate per-user credit pool**, not a graph's pool. When you query a shared repository identifier (for example `sec`) at the credits endpoint, the response transparently routes to that per-user repository pool and reports a synthetic tier of `sec-repository`:

```bash
curl -X GET "https://api.robosystems.ai/v1/graphs/sec/credits" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
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

## Storage Is Not Paid For With Credits

Storage is a distinct axis from credits. Each tier **includes** its storage: there is no metering and no overage billing, only the tier's limit (with an optional override and auto-expand behavior), tracked independently of the credit pool. Storage is **never paid for with credits**, and storage limit fields are deliberately absent from the `/credits` response.

To see storage usage, use the analytics endpoint described above — storage figures appear in its `storage_summary`, not in the credit balance. Tier storage limits are defined alongside the rest of the billing configuration; see the [Billing Configuration](https://github.com/RoboFinSystems/robosystems/tree/main/robosystems/config/billing) and [Graph Configuration](https://github.com/RoboFinSystems/robosystems/blob/main/.github/configs/graph.yml) in the codebase.

## Organizations and Account Limits

Every user belongs to an **organization** — the account that owns your graphs and carries billing. Organizations are **multi-user**: one org owns the graphs, one org receives the bill, and people are added to it by invitation. Registering normally creates a personal org with you as its sole owner; registering through an invitation link joins you to the inviting org instead.

Each member holds one of three roles, and the role decides what they can do with money and membership:

| Role | Billing and membership powers |
| --- | --- |
| **`owner`** | Everything an admin can do, plus checkout, payment methods, and the Stripe customer portal |
| **`admin`** | Invite and manage members, view invoices, and manage another member's subscriptions |
| **`member`** | Works in the graphs they are granted; manages only their own subscriptions |

Graph *access* is a separate, narrower grant on top of org membership — being in the org does not by itself put you in every graph. Org owners and admins are the exception: they hold implicit admin on every graph their org owns. The full model, the invitation flow, and the per-graph member endpoints are covered in [Authentication and API Keys](Authentication-and-API-Keys.md#organizations-and-roles).

The organization carries one account-level limit, independent of credits and storage: a **maximum number of graphs** it can hold (`max_graphs`, defaulting to the configured `ORG_GRAPHS_DEFAULT_LIMIT`). This caps how many graphs the account can provision at once — distinct from each graph's own storage limit and credit pool. `GET /v1/orgs/{org_id}/limits` reports the cap and where you stand against it, and `GET /v1/orgs/{org_id}/usage` aggregates activity across every graph in the org (`days` sets the lookback, default 30). See the [API reference](https://robosystems.ai/docs/api) for the exact request and response shapes.

## Billing Endpoints

Credits meter AI usage inside a graph; the `/v1/billing/*` surface is where money moves. **Every route on it is scoped by `{org_id}`, not by graph** — the organization is the billing entity — and each is gated on the caller's org role:

| Endpoint | Purpose | Who can call it |
| --- | --- | --- |
| `POST /v1/billing/checkout` | Start a Stripe checkout session for a new subscription | Owner |
| `GET /v1/billing/checkout/{session_id}/status` | Poll after returning from Stripe | Any member of the session's org |
| `GET /v1/billing/customer/{org_id}` | Billing customer record | Any member; payment-method detail is owner-only |
| `POST /v1/billing/customer/{org_id}/portal` | A Stripe Customer Portal URL for cards and billing history | Owner |
| `GET /v1/billing/invoices/{org_id}` | Invoice history | Admin or owner |
| `GET /v1/billing/invoices/{org_id}/upcoming` | The next invoice, if there is billing activity yet | Admin or owner |
| `GET /v1/billing/subscriptions/{org_id}` | The org's subscriptions | Any member, but a plain member sees only their own rows |
| `GET /v1/billing/subscriptions/{org_id}/subscription/{subscription_id}` | One subscription in detail | Same scoping as the list |
| `POST /v1/billing/subscriptions/{org_id}/subscription/{subscription_id}/cancel` | Cancel a subscription | Owner |

The read scoping on the last two is deliberate rather than incidental: billing is org-level but repository access is per-user, so an unfiltered org-wide list would hand every member a roster of everybody else's seats. Owners and admins manage billing and see the whole org; a plain member sees the rows attributed to them.

Checkout is asynchronous. After the user returns from Stripe, the status endpoint progresses `pending_payment → provisioning → active`; once it reads `active` the `resource_id` is populated, and for a graph an `operation_id` tracks provisioning over SSE.

**Buying under contract instead of by card.** The managed platform can also be bought on paper: the public MSA with a one-page Order Form. A Dedicated Deployment — the platform in an AWS account dedicated to your organization — is bought under the MSA and an Order Form, or as an AWS Marketplace private offer billed on your AWS invoice. See [robosystems.ai/enterprise](https://robosystems.ai/enterprise).

**Cancellation lives with the resource, not here.** The billing-level cancel route is reserved for future subscriptions that are not tied to a resource — it deliberately rejects the two kinds that exist today and names the canonical path instead:

- **A graph** — `POST /v1/graphs/{graph_id}/operations/delete-graph`. This tears down **immediately** by default, crediting the unused part of the billed period (never a refund): to the organization's account balance at once on a card-billed plan, where it offsets the next invoice — a graph created right after pays the difference; on the period's invoice — or a separate credit invoice once that is paid — on an invoiced plan; pass `at_period_end: true` to keep the graph usable through the current billing period. The caller must be both an org owner (billing authority) and an admin on the graph (operational authority).
- **A shared-repository subscription** — `POST /v1/graphs/{repo_id}/subscriptions/cancel`. This cancels **at period end** by default; pass `immediate: true` to stop access right away.

Each guards itself with a `confirm` value that must equal the resource id — `delete-graph` always, the repository cancel when you ask for immediate termination — so a mistyped call cannot destroy the wrong thing. See [Shared Repositories](Shared-Repositories.md) for the repository half.

## Rate Limits

Rate limits are the third metering axis, alongside credits and storage — and the only one that throttles *free* operations. They are short, per-category windows that absorb normal bursts, scaled by tier for the categories served by a graph's own instance, and independent of credits: hitting a rate limit consumes nothing, and an exhausted credit pool does not change a rate limit. The headers, the 429 response, and where the live limits are served are in [Errors and Rate Limits](Errors-and-Rate-Limits.md#rate-limits), along with the 402 an AI operation returns when the pool is empty ([Insufficient Credits](Errors-and-Rate-Limits.md#insufficient-credits)).

## Self-hosted deployments

robosystems.ai runs with billing on. A self-hosted deployment controls this with `BILLING_ENABLED`, which is off by default — so a local development stack runs with billing off.

### Admin CLI Reference

For operators of a self-hosted deployment, the admin CLI exposes credit-pool inspection and management. It runs against a deployment you operate, not robosystems.ai. The first argument is that deployment's environment (`dev`, `staging`, or `prod`):

```bash
just admin dev credits list                       # List graph credit pools
just admin dev credits get <graph_id>             # Detail for one graph
just admin dev credits bonus <graph_id> --amount 500 --description "Pilot"   # Grant bonus credits
just admin dev credits reset <graph_id> --reason "Support"                   # Refill to the monthly allocation
just admin dev credits analytics                  # Tier-level analytics
just admin dev credits health                     # Credit system health

just admin dev credits repos list                 # Shared-repository credit pools
just admin dev credits repos get <user_repository_id>
just admin dev credits repos bonus <user_repository_id> --amount 500 --description "Pilot"
```

`credits reset` asks for confirmation, then forfeits the pool's remaining balance (recorded as an `expiration` transaction) and refills it to the monthly allocation. The scheduled monthly reset still runs when the month turns.

## Related Documentation

**Guides:**
- **[Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy.md)** - How credits scope per graph, and how subgraphs share the parent pool
- **[AI Operators and MCP](AI-Operators-and-MCP.md)** - The AI Operators that consume credits, and the free MCP tool surface
- **[Errors and Rate Limits](Errors-and-Rate-Limits.md)** - Rate-limit headers, 429 handling, and the insufficient-credits response
- **[Architecture Overview](Architecture-Overview.md)** - Platform architecture, tiers, and the operations layer

**Codebase Documentation:**
- **[Billing Configuration](https://github.com/RoboFinSystems/robosystems/tree/main/robosystems/config/billing)** - Subscription plans, AI token pricing, and storage limits
- **[Shared Repository Registry](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/config/shared_repositories.py)** - Repository plans, rate limits, and credit costs
- **[API reference](https://robosystems.ai/docs/api)** - API reference with machine-readable OpenAPI spec

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Technical docs](Home.md)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
