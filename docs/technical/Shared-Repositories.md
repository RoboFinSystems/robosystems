# Shared Repositories

A **shared repository** is a platform-managed, read-only public dataset you subscribe to and query alongside your own graphs. Where the dedicated tiers in [Graphs & Multi-Tenancy](Graphs-and-Multi-Tenancy) give each customer an isolated graph, a shared repository is a single large graph that every subscriber reads — served from its own infrastructure tier and billed per subscriber. SEC EDGAR is the one shared repository available today.

## Table of Contents

- [What a shared repository is](#what-a-shared-repository-is)
- [The ladybug-shared tier](#the-ladybug-shared-tier)
- [The registry and manifest model](#the-registry-and-manifest-model)
- [Subscribing and accessing](#subscribing-and-accessing)
- [The SEC shared repository](#the-sec-shared-repository)
- [Related Documentation](#related-documentation)
- [Support](#support)

## What a shared repository is

A shared repository is a **public dataset modeled as a graph** that the platform owns, maintains, and serves to all subscribers — as opposed to a customer graph (`kg…`), which holds one tenant's private data. The two differ on nearly every axis:

| | Customer graph (`kg…`) | Shared repository (e.g. `sec`) |
| --- | --- | --- |
| Data | Your private data | Public data, identical for everyone |
| Access | Owner + granted users | Any user with a subscription |
| Writes | Read + write | **Read-only** |
| Infrastructure | Dedicated per-customer instance | Shared master + read-only replica fleet |
| Scaling | Vertical (bigger instance) | Horizontal (more replicas) |
| Billing | Per-graph subscription | Per-subscriber repository plan |

Because it is just a graph, you query a shared repository through the **same surfaces** as your own — Cypher, the MCP tools, search — using its repository id as the `graph_id`. An AI Operator can traverse a shared repository and your own graph in a single workflow (for example, comparing your portfolio against SEC filings). It is strictly read-only: write, backup, restore, and admin operations are rejected.

## The ladybug-shared tier

Shared repositories run on a dedicated infrastructure tier, `ladybug-shared`, separate from the per-customer dedicated tiers:

- A **shared master** instance owns the build path — the ingestion pipeline materializes the graph here.
- A **read-only replica fleet** serves queries. Replicas download the materialized `.lbug` / `.duckdb` / vector artifacts from S3 on boot and sit behind a load balancer, so read volume scales by adding replicas rather than by resizing one instance.
- The tier is **opt-in** per deployment (`LBUG_SHARED_ENABLED`), since the replica fleet is separate infrastructure.

See the [Architecture Overview](Architecture-Overview) for the cluster topology and the S3-publish → replica-refresh flow.

## The registry and manifest model

Every shared repository is declared by a single **adapter manifest** and registered in `config/shared_repositories.py`. The manifest is the one source of truth for the repository — its identity, data source, schema, allowed and blocked endpoints, rate limits, subscription plans, and credit costs all live in one file. The registry lazy-loads manifests and exposes a query API (`is_shared_repository`, `get_manifest`, `get_all_repository_ids`, `get_plan_details`) used across billing, middleware, and operations.

Adding a new shared repository is therefore a two-step change — write the manifest, register it — with no separate billing config, database migrations, or hardcoded lists to update. The ingestion side (how a repository's data is downloaded, staged, materialized, and published to the replica fleet) is covered in the [Pipeline Guide](Pipeline-Guide). **SEC is the only shared repository registered today;** the model is built to host additional public datasets.

## Subscribing and accessing

Shared repository plans are discoverable without authentication at the public offering endpoint, which returns graph subscription tiers, shared repository plans, and AI credit costs:

```bash
curl http://localhost:8000/v1/offering
```

A customer graph's subscription is created automatically when the graph is provisioned. A **shared repository** is different — you subscribe to it explicitly, choosing one of its plans:

```bash
curl -X POST "http://localhost:8000/v1/graphs/sec/subscription" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"plan_name": "starter"}'
```

Checking your subscription uses the same endpoint, which auto-detects graphs versus repositories:

```bash
curl "http://localhost:8000/v1/graphs/sec/subscription" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

Once subscribed, you query the repository exactly like your own graph — its id (`sec`) is the `graph_id` in the URL:

```bash
curl -X POST "http://localhost:8000/v1/graphs/sec/query" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (e:Entity) RETURN e.name LIMIT 10"}'
```

Database operations (query, MCP, search) are **free** — they draw down rate-limit budget, not credits. Only AI operations consume credits, drawn from your repository plan's monthly allocation. See [Credits & Billing](Credits-and-Billing).

## The SEC shared repository

SEC EDGAR is the one shared repository available today — public-company filings and XBRL financial data, synced daily, with semantic enrichment for natural-language element resolution. Its plans are read-only and differ on throughput and backup-download allowance:

| Plan | Price | Monthly AI credits | Access |
| --- | --- | --- | --- |
| **Starter** | $29/month | 5,000 | Read |
| **Advanced** | $99/month | 17,000 | Read |

The Advanced plan carries roughly 5× the rate limits of Starter. Rate limits apply per category — queries, MCP calls, searches, and AI agent calls each have their own per-minute / per-hour / per-day budgets.

The repository id is `sec`, and it exposes a `sec_historical` subgraph for older filings. For a hands-on walkthrough — loading filings locally, querying them with Cypher and MCP, and the data model — see the [SEC XBRL Pipeline](SEC-XBRL-Pipeline) demo.

## Related Documentation

- **[Graphs & Multi-Tenancy](Graphs-and-Multi-Tenancy)** - The dedicated per-customer tiers, the other half of graph access
- **[SEC XBRL Pipeline](SEC-XBRL-Pipeline)** - Hands-on with the SEC shared repository
- **[Pipeline Guide](Pipeline-Guide)** - How a shared repository's data is ingested and published
- **[Credits & Billing](Credits-and-Billing)** - The credit model and subscription billing
- **[AI Operators & MCP](AI-Operators-and-MCP)** - Querying a repository alongside your own graphs
- **[Shared Repository Registry](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/config/shared_repositories.py)** - The registry in the codebase

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
