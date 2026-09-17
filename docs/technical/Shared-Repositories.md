# Shared Repositories

A **shared repository** is a platform-managed, read-only public dataset you subscribe to and query alongside your own graphs. Where the dedicated tiers in [Graphs & Multi-Tenancy](Graphs-and-Multi-Tenancy) give each customer an isolated graph, a shared repository is a single large graph that every subscriber reads — served from its own infrastructure tier and billed per subscriber. SEC EDGAR is the one shared repository available today.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development).

## Table of Contents

- [What a shared repository is](#what-a-shared-repository-is)
- [The ladybug-shared tier](#the-ladybug-shared-tier)
- [The registry and manifest model](#the-registry-and-manifest-model)
- [Subscribing and accessing](#subscribing-and-accessing)
- [The SEC shared repository](#the-sec-shared-repository)
- [Self-hosted deployments](#self-hosted-deployments)
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
- On robosystems.ai this tier serves the SEC repository; for a self-hosted deployment it is **opt-in** (`LBUG_SHARED_ENABLED`), since the replica fleet is separate infrastructure.

See the [Architecture Overview](Architecture-Overview) for the cluster topology and the S3-publish → replica-refresh flow.

## The registry and manifest model

Every shared repository is declared by a single **adapter manifest** and registered in `config/shared_repositories.py`. The manifest is the one source of truth for the repository — its identity, data source, schema, allowed and blocked endpoints, rate limits, subscription plans, and credit costs all live in one file. The registry lazy-loads manifests and exposes a query API (`is_shared_repository`, `get_manifest`, `get_all_repository_ids`, `get_plan_details`) used across billing, middleware, and operations.

Adding a new shared repository is therefore a two-step change — write the manifest, register it — with no separate billing config, database migrations, or hardcoded lists to update. The ingestion side (how a repository's data is downloaded, staged, materialized, and published to the replica fleet) is covered in the [Pipeline Guide](Pipeline-Guide). **SEC is the only shared repository registered today;** the model is built to host additional public datasets.

## Subscribing and accessing

Shared repository plans are discoverable without authentication at the public offering endpoint, which returns graph subscription tiers, shared repository plans, and AI credit costs:

```bash
curl https://api.robosystems.ai/v1/offering
```

A customer graph's subscription is created automatically when the graph is provisioned. A **shared repository** is different — you subscribe to it explicitly, choosing one of its plans, from **Repositories** in the app or over the API:

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai

curl -X POST "https://api.robosystems.ai/v1/graphs/sec/subscriptions" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"plan_name": "sec-starter"}'
```

Plan names are repository-prefixed: `sec-starter` and `sec-advanced`. Checking your subscription uses the same endpoint, which auto-detects graphs versus repositories:

```bash
curl "https://api.robosystems.ai/v1/graphs/sec/subscriptions" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

Changing plan and cancelling are the other two verbs on the same path:

```bash
# Move to the higher plan (synchronous for repositories)
curl -X PATCH "https://api.robosystems.ai/v1/graphs/sec/subscriptions" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"new_plan_name": "sec-advanced"}'

# Cancel at period end (access stays until the period closes)
curl -X POST "https://api.robosystems.ai/v1/graphs/sec/subscriptions/cancel" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

Cancellation defaults to end-of-period; pass `immediate: true` together with `confirm: "sec"` to stop access right away. All three write calls accept an optional `user_id`, so an org owner or admin can subscribe, change, or cancel on behalf of another member of their organization — repository access is per-user while the billing is org-level, so the subscriber is what determines who gets in. A member acting on their own subscription omits it.

Once subscribed, you query the repository exactly like your own graph — its id (`sec`) is the `graph_id` in the URL:

```bash
curl -X POST "https://api.robosystems.ai/v1/graphs/sec/query/cypher" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (e:Entity) RETURN e.name LIMIT 10"}'
```

Database operations (query, MCP, search) are **free** — they draw down rate-limit budget, not credits. Only AI operations consume credits, drawn from your repository plan's monthly allocation. See [Credits & Billing](Credits-and-Billing).

## The SEC shared repository

SEC EDGAR is the one shared repository available today — public-company filings and XBRL financial data, synced daily, with semantic enrichment for natural-language element resolution. Its plans are read-only and differ on throughput and backup-download allowance:

| Plan | `plan_name` | Price | Monthly AI credits | Access |
| --- | --- | --- | --- | --- |
| **Starter** | `sec-starter` | $29/month | 5,000 | Read |
| **Advanced** | `sec-advanced` | $99/month | 17,000 | Read |

Prices and credit allocations are served live and unauthenticated at **`GET /v1/offering`** — that response, not this table, is the authority if the two ever disagree.

The Advanced plan carries roughly 5× the rate limits of Starter. Rate limits apply per category — queries, MCP calls, searches, and AI agent calls each have their own per-minute / per-hour / per-day budgets.

The repository id is `sec`, and it exposes a `sec_historical` subgraph for older filings. For a hands-on walkthrough — querying filings with Cypher and MCP, and the data model — see the [SEC XBRL Pipeline](SEC-XBRL-Pipeline) demo.

### Downloading a snapshot

Subscribers whose plan carries a download allowance can pull the whole repository down as a LadybugDB file rather than querying it over the API. Ask for a presigned URL, then fetch it:

```bash
# Latest published snapshot — one backup record per repository, republished on each pipeline run
curl -s "https://api.robosystems.ai/v1/graphs/sec/backups" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"

curl -s "https://api.robosystems.ai/v1/graphs/sec/backups/BACKUP_ID/download" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"

curl -L -o sec.lbug.zst "PASTE_THE_DOWNLOAD_URL"
```

Snapshots arrive **zstd-compressed** (`.lbug.zst`), so you need the `zstd` tool — it ships with neither macOS nor most Linux distributions by default:

```bash
brew install zstd            # macOS
sudo apt-get install zstd    # Debian / Ubuntu
sudo dnf install zstd        # Amazon Linux / Fedora / RHEL
```

Then decompress it in the folder holding the download (on macOS, right-click the folder → **Services** → **New Terminal at Folder** to get there):

```bash
zstd -d sec.lbug.zst   # -> sec.lbug ; add --rm to delete the .zst afterwards
```

Plain `zstd -d` is enough — no `--long` flag — and the decompressed file is roughly 2× the download, so leave disk headroom. The result is a LadybugDB database you can query directly; see [Graph Operations](Graph-Operations#downloading-and-unpacking-a-backup) for the full download-and-unpack walkthrough.

## Self-hosted deployments

There is no free hosted SEC tier; self-hosting is the free path. On a stack you run, load the filings you want with `just sec-load` and query them through the same `sec` graph id — see [Local Development](Local-Development#sec-filings).

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
