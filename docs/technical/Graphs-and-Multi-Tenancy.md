# Graphs & Multi-Tenancy

RoboSystems is multi-tenant at the graph layer: every customer dataset lives in its own isolated graph database keyed by a `graph_id`. This page explains the `graph_id` model, the three stores a tenant spans, the available graph tiers, subgraphs, and the day-to-day tasks of creating, listing, and querying graphs.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development).

## Table of Contents

- [Overview](#overview)
- [The Three Stores](#the-three-stores)
- [Prerequisites](#prerequisites)
- [The `graph_id` Model and Per-Graph Isolation](#the-graph_id-model-and-per-graph-isolation)
- [Who Can Reach a Graph: Orgs, Roles, and Subscriptions](#who-can-reach-a-graph-orgs-roles-and-subscriptions)
- [Graph Tiers](#graph-tiers)
- [Creating a Graph](#creating-a-graph)
- [Schema Extensions and the Extensions Surface](#schema-extensions-and-the-extensions-surface)
- [Listing and Inspecting Graphs](#listing-and-inspecting-graphs)
- [Subgraphs](#subgraphs)
- [Querying a Graph](#querying-a-graph)
- [Crossing the Tenant Boundary](#crossing-the-tenant-boundary)
- [Troubleshooting](#troubleshooting)
- [Related Documentation](#related-documentation)

## Overview

A **graph** in RoboSystems is a single, isolated LadybugDB database. Each graph has a unique identifier, `graph_id`, that scopes every request to exactly one tenant's data. There is no shared table space across tenants and no cross-tenant read access at the storage layer.

The multi-tenancy model has a few load-bearing properties:

1. **One graph = one isolated database.** Each graph is its own embedded LadybugDB database. No shared tables, no cross-tenant leakage at the data layer.
2. **The `graph_id` is the tenant boundary — in two stores, not one.** It names a LadybugDB database *and* a PostgreSQL schema in the extensions OLTP database. See [The Three Stores](#the-three-stores).
3. **The `graph_id` is always a URL path parameter**, never a query argument or body field. Authentication and per-graph access are validated by FastAPI dependencies before any handler runs.
4. **Two kinds of graphs.** Customer/entity graphs hold your own data; **shared repositories** (such as `sec`) are platform-managed and read-only for everyone.
5. **Tiers are dedicated instances.** A graph runs on a dedicated EC2 instance sized by tier. Subgraphs share that same instance and its resource budget.
6. **Only AI operations consume credits.** Database operations (queries, ingestion, backups) are free. Subgraphs draw from their parent graph's credit pool.
7. **Every isolation primitive keys on `graph_id`** — caches, idempotency keys, rate-limit buckets, and credit pools all namespace on the graph (or the user), never on the organization.

## The Three Stores

A single tenant spans three stores, and knowing which one you are addressing determines both the transport you use and whether you may write. This is the most common source of confusion on the platform, so it is worth pinning down before anything else.

| Store | Holds | Tenancy mechanism | You reach it through |
| --- | --- | --- | --- |
| **Platform PostgreSQL** (`robosystems`) | Users, organizations, billing, the graph registry, connections, documents | Row-level, by `org_id` / `graph_id` foreign keys | `/v1/*` |
| **Extensions PostgreSQL** (`extensions`) | Per-graph OLTP product data — the chart of accounts, transactions, entries, reports, portfolios, positions | **Schema-per-graph**: one PostgreSQL schema named for the `graph_id` | `/extensions/*` |
| **LadybugDB** | The analytical graph — the queryable, materialized projection used for reporting, Cypher traversal, and AI retrieval | **Database-per-graph**: one embedded database per `graph_id` | `/v1/graphs/{graph_id}/query/*`, MCP |

Three consequences fall out of this, and each answers a question the rest of this page would otherwise leave hanging:

**The extensions database is the system of record for an entity graph; the LadybugDB graph is a projection of it.** Products write to PostgreSQL, and a Dagster sensor rebuilds the graph from those rows. That is why the main graph is read-only over Cypher (see [Querying a Graph](#querying-a-graph)) — you are not permitted to hand-edit a derived artifact. It is also why the recovery path for an entity graph is `materialize`, not a graph restore.

**A write reaches the graph only if it marks the graph stale.** There is no incremental sync and no change-data-capture — the projection is rebuilt wholesale. A row that lands in OLTP without a stale mark is invisible to every Cypher query and every MCP traversal until some *other* write happens to mark the graph. The platform's own operations handle this for you; it matters here because it explains the lag between an extensions write and its appearance in a query result.

**Tenancy in the extensions database is `search_path`, not a `WHERE` clause.** Each graph gets a PostgreSQL schema named for its `graph_id`, and every session opens by binding `search_path` to that schema with `public` as the fallback. The schema name is validated against a strict pattern before it is ever used, and the binding is cleared before the connection returns to the pool. One `search_path` expresses both halves of the model: tenant tables shadow `public`, so your rows are yours alone, while the shared taxonomy library resolves *through* `public` and is common to every tenant. The alternative — one shared table set with a tenant discriminator column — was deliberately not adopted: schema-per-graph costs more DDL and gives isolation that a forgotten `WHERE` clause cannot breach.

The two PostgreSQL databases have **independent migration histories** and live on the same RDS instance in deployed environments. The split is about blast radius: a product migration cannot lock the table that authorizes requests, and an extensions outage degrades the products without taking down authentication or billing. The cost is that anything needing both stores is two sessions and cannot be one transaction.

## Prerequisites

Before working with graphs, ensure you have:

- A RoboSystems account
- An API key, from **Settings → API keys** at robosystems.ai
- A graph, or the org owner or admin role needed to create one (see [Creating a Graph](#creating-a-graph))

[Quick Start](Quick-Start) walks through getting all three. All authenticated examples below target `https://api.robosystems.ai` and read the API key from `$ROBOSYSTEMS_API_KEY`. Use the `X-API-Key` header for backend testing; `Authorization: Bearer` is a frontend concern only.

## The `graph_id` Model and Per-Graph Isolation

The `graph_id` is the primary multi-tenant identifier. It appears as a URL path parameter on every graph-scoped route, and the platform resolves it — to a LadybugDB database, an extensions PostgreSQL schema, or both, depending on the route — before the handler executes.

### ID Formats

| Kind | Format | Example | Extensions tenant? |
| --- | --- | --- | --- |
| **Parent graph** | `kg` + 16 or more hex characters | `kg1234567890abcdef` | Yes |
| **Subgraph** | `{parent_id}_{subgraph_name}` | `kg1234567890abcdef_dev` | **No** |
| **Shared repository** | Fixed reserved name | `sec` | No |
| **Shared-repo subgraph** | `{repo_id}_{subgraph_name}`, platform-created only | `sec_archive2019` | No |
| **Taxonomy library sentinel** | The literal `library` | `library` | Reads the shared library |

The parent graph identifier follows the regex `kg[a-f0-9]{16,}`. A subgraph appends an underscore and an alphanumeric name (`[a-zA-Z0-9]{1,20}`) to its parent's ID, so the parent is always recoverable from the subgraph ID.

**Only a parent graph is an extensions tenant.** The rightmost column above is not a policy choice made per route — it falls out of the schema-name validator, which accepts `kg` plus hex and nothing else. A subgraph ID contains an underscore and therefore cannot name a schema, so no extensions session can ever open against one. See [A Subgraph Is Not an Extensions Tenant](#a-subgraph-is-not-an-extensions-tenant).

**`library` is a reserved sentinel, not a graph.** It occupies the `graph_id` position on extensions routes and resolves to the shared, read-only taxonomy library rather than to any tenant. Any authenticated user may read it.

### Why URL-Scoped Tenancy

Because the `graph_id` lives in the URL path, the tenant scope is unambiguous and is checked by middleware before your request reaches business logic. This applies uniformly across REST, GraphQL, and MCP:

- **REST**: `POST /v1/graphs/{graph_id}/query/cypher`
- **GraphQL**: `POST /extensions/{graph_id}/graphql` — the `graph_id` comes from the URL, so queries do **not** take a `graphId` argument. Write `{ entity { … } }`, not `{ entity(graphId: "kg_x") { … } }`.
- **MCP**: tools read the `graph_id` from connection context rather than as a tool argument.

Accepting a graph identifier in a query argument or request body would create a second, competing source of truth for which tenant a request addresses, and the two would eventually disagree. One anchor, checked once, before the handler.

## Who Can Reach a Graph: Orgs, Roles, and Subscriptions

Isolation answers *which data*; this answers *which people*. Access is resolved differently for the two kinds of graph.

**User graphs** are owned by exactly one **organization**. The org is the billing party and the only pool of users who can be granted access to graphs it owns — there are no cross-org access grants. Within that, access is role-based:

| Role on the graph | Can |
| --- | --- |
| `viewer` | Read only. Cypher writes on a subgraph are rejected with `403`. |
| `member` | Read and write |
| `admin` | Read, write, and manage the graph (including subgraph lifecycle) |

Two rules make the effective role non-obvious, and both are worth knowing before you debug a permissions surprise:

- **Org owners and admins hold implicit graph admin** on every graph their org owns — paying for a graph carries the right to manage it. The effective role is the stronger of the explicit grant and that implicit one, so a user with an explicit `viewer` row on a graph their org owns and administers is still an admin there.
- **Subgraphs have no permissions of their own.** An access check on `kg…_dev` resolves to `kg…` and is answered by the parent's membership. Granting access to a parent grants it to every subgraph beneath it.

**Shared repositories** do not use this model at all. They have no owning org and no member rows; access is **subscription-based** through a repository plan. See [Shared Repositories](Shared-Repositories).

`GET /v1/graphs/{graph_id}/members` reports who can reach a graph and at what role.

## Graph Tiers

A tier determines the instance size, storage budget, subgraph capacity, backup limits, and API rate multiplier for a graph. RoboSystems uses **instance-based naming** so the tier name reflects exactly what infrastructure you get.

| Tier (technical) | Display |
| --- | --- |
| **`ladybug-standard`** | Standard |
| **`ladybug-large`** | Large |
| **`ladybug-xlarge`** | XLarge |
| **`ladybug-shared`** | Shared Repository (platform-managed, not customer-selectable) |

**Instance type, RAM, vCPU, max subgraphs, storage limit, API rate multiplier and backup retention
are served, not documented here.** Call **`GET /v1/graphs/tiers`** for the per-tier limits as the
platform enforces them, **`GET /v1/offering`** for the same set alongside public pricing, or see
**[robosystems.ai/pricing](https://robosystems.ai/pricing)**. The limits are computed from
[`.github/configs/graph.yml`](https://github.com/RoboFinSystems/robosystems/blob/main/.github/configs/graph.yml)
— the same file the GHA deployment layer uses to provision the instances and the application imports
at runtime — and the prices and monthly credit allocations from
[`robosystems/config/billing/core.py`](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/config/billing/core.py).
A number in that response is a number the platform is actually enforcing.

Notes:

- **A graph and its subgraphs share one instance.** The subgraph cap, storage budget, and RAM are budgets for the whole instance, not per database — a parent plus its subgraphs draw on the same pool.
- **The API rate multiplier applies to dedicated-resource categories only**, and scales with the tier's instance vCPU using Standard as the anchor. **Shared categories are the same on every tier**: auth, status, billing, and SSE do not scale, because they consume platform resources rather than your instance. The multiplier in `/v1/offering` is derived from the limit table the rate limiter actually enforces, so what is advertised cannot drift from what a caller receives.
- **`ladybug-shared` is platform-managed and not user-creatable.** It backs read-only public repositories such as the `sec` corpus. You consume it through queries; you do not provision it. See [Shared Repositories](Shared-Repositories) for the subscription and consumption model.
- **Only Standard, Large, and XLarge are creatable tiers.** The `instance_tier` field on graph creation accepts `ladybug-standard`, `ladybug-large`, or `ladybug-xlarge`.
- **The tier sizes the graph, not the OLTP store.** Everything above describes the dedicated EC2 instance running your LadybugDB database. The extensions PostgreSQL schema (see [The Three Stores](#the-three-stores)) is per-graph regardless of tier and lives on shared managed infrastructure — tier does not change its isolation, which is the same schema-per-graph binding for every customer.

## Creating a Graph

Graphs are created with `POST /v1/graphs`. Creation is **asynchronous**: the call returns `202 Accepted` with an `OperationEnvelope` carrying an `operation_id`, and the graph is provisioned in the background.

There are two flavors of graph:

- **Entity graphs** — pre-wired for a financial entity (the default for RoboLedger). Supply an `initial_entity` and any `schema_extensions` (such as `roboledger`).
- **Custom graphs** — your own node and relationship model. Supply a `custom_schema`. See [Custom Graph Schema](Custom-Graph-Schema) for the full how-to.

### Quick Start

The fastest path to a working graph is the app: choose **Create Graph** on the Home page or in the graph selector at robosystems.ai. A RoboLedger graph can also come from roboledger.ai when you connect QuickBooks.

Creating a graph starts a subscription on the organization's payment method, and only organization owners and admins can create one. Over the API, a caller without that role gets `403 graph_creation_requires_admin`, an organization already holding as many graphs as its limit allows gets `403 graph_limit_reached`, and an organization with no payment method gets `402 payment_required`.

### Create an Entity Graph

This creates a RoboLedger-enabled entity graph on the Standard tier. The `Idempotency-Key` header makes retries safe.

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai

curl -X POST "https://api.robosystems.ai/v1/graphs" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Idempotency-Key: $(date +%s)" \
  -H "Content-Type: application/json" \
  -d '{
    "metadata": {
      "graph_name": "Acme Consulting LLC",
      "description": "Professional consulting services",
      "schema_extensions": ["roboledger"]
    },
    "instance_tier": "ladybug-standard",
    "initial_entity": {
      "name": "Acme Consulting LLC",
      "uri": "https://acmeconsulting.com",
      "ein": "12-3456789",
      "state_of_incorporation": "Delaware",
      "entity_type": "llc"
    },
    "create_entity": true,
    "tags": ["consulting"]
  }'
```

**Note:** The response is an `OperationEnvelope` with an `operation_id`, not a finished graph. Track progress over Server-Sent Events at `GET /v1/operations/{operation_id}/stream`. The `entity_type` (such as `llc`, `corporation`, or `partnership`) drives the default reporting style for entity graphs.

For the full request and response schema — every field, type, and validation rule — see the live OpenAPI docs rather than re-deriving it here: [robosystems.ai/docs/api](https://robosystems.ai/docs/api).

## Schema Extensions and the Extensions Surface

The `schema_extensions` field in the creation request above is the switch that decides what kind of tenant a graph is. `GET /v1/graphs/extensions` lists what is available; today that is `roboledger` (accounting) and `roboinvestor` (investment management).

### What `schema_extensions` Actually Controls

Declaring an extension on a graph does three things:

1. **Which API surfaces answer for it.** A `roboledger` graph responds on `/extensions/roboledger/{graph_id}/operations/*`; a graph without it does not.
2. **Which MCP tools appear.** The tool list is assembled from the graph's extensions, so a plain custom graph and a RoboLedger graph advertise very different sets.
3. **Which node types the analytical projection is built with** when the graph is materialized.

And — this is the part that surprises people — it does **not** control which tables exist. Tenant provisioning creates *every* extension table regardless of what the column says, and the graph's DDL is resolved from the column at rebuild time rather than at creation time.

That has a practical upside worth stating plainly: **`schema_extensions` is an access restriction plus a materialization scope, not a data-model switch.** Adding an extension to a live graph is a safe, in-place operation, and removing one withdraws surfaces rather than dropping rows.

### When the Tenant Schema Is Created

The extensions PostgreSQL schema for a graph is provisioned when an **entity graph** is created with `create_entity: true`, and lazily on first extensions access otherwise. Provisioning creates the schema, builds the tenant tables, copies the canonical taxonomy library into it per the graph's pin, and installs triggers so library-seeded rows cannot be mutated from tenant scope.

A **custom graph** with no extensions never gets one. That is the correct outcome, not a gap — it has no OLTP product data to hold, so `/extensions/*` routes are not part of its surface.

### The Three Extensions Sub-Surfaces

Everything under `/extensions/*` is graph-scoped at the URL and falls into one of three shapes:

| Sub-surface | Path | Read/Write | Reads or writes |
| --- | --- | --- | --- |
| **GraphQL typed reads** | `POST /extensions/{graph_id}/graphql` | Read | Extensions PostgreSQL |
| **Command writes** | `POST /extensions/{domain}/{graph_id}/operations/{op_name}` | Write | Extensions PostgreSQL |
| **Analytical view operations** | `POST /extensions/{domain}/{graph_id}/operations/{view_name}` | Read | LadybugDB (the projection) |

**For an entity graph, the command-write surface — not the Cypher endpoint — is how data enters the system.** This is the point that ties the whole page together: `/v1/graphs/{graph_id}/query/cypher` is read-only on the main graph *because* the write path is `/extensions/{domain}/{graph_id}/operations/*`, and the graph is rebuilt from what those operations write.

Both surfaces are gated per deployment: on robosystems.ai both extensions are enabled, and a self-hosted deployment controls each with `ROBOLEDGER_ENABLED` and `ROBOINVESTOR_ENABLED`. The GraphQL schema is composed at construction time — a ledger-only deployment has literally no investor fields to introspect, rather than fields that fail at runtime. Full detail, including the operation envelope and idempotency semantics, is in **[Extensions Surface Overview](Extensions-Surface-Overview)**.

## Listing and Inspecting Graphs

### List Your Graphs

`GET /v1/graphs` returns the graphs you can access, plus the shared repositories you are **subscribed to** — not the ones on offer. The response is `{graphs[], selectedGraphId}`, with subscribed repositories appended into the same `graphs[]` array carrying `isRepository: true`. To discover repositories you have *not* subscribed to, use `GET /v1/offering`.

```bash
curl "https://api.robosystems.ai/v1/graphs" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

`POST /v1/graphs/{graph_id}/select` sets the `selectedGraphId` that comes back on subsequent list calls — it is how the frontends remember which graph you were last working in.

### Inspect a Single Graph

`GET /v1/graphs/{graph_id}/info` returns database details for one graph.

Several more read endpoints answer the questions that come up around a single graph:

| Endpoint | Answers |
| --- | --- |
| `GET /v1/graphs/{graph_id}/health` | Is this graph's database reachable and healthy? |
| `GET /v1/graphs/{graph_id}/limits` | What operational limits does its tier enforce? |
| `GET /v1/graphs/{graph_id}/metrics` | What is its current activity profile? |
| `GET /v1/graphs/{graph_id}/usage` | Credits and storage together, as one report |
| `GET /v1/graphs/{graph_id}/members` | Who can reach it, and at what role |
| `GET /v1/graphs/{graph_id}/subgraphs/{subgraph_name}` | Detail on one subgraph |

And three that are about the platform rather than one graph: `GET /v1/graphs/tiers` (every creatable tier with its limits and `api_rate_multiplier`), `GET /v1/graphs/capacity` (fleet capacity per tier), and `GET /v1/graphs/extensions` (the schema extensions available to a new graph). `POST /v1/graphs/schema/validate` checks a custom schema before you provision anything with it.

## Subgraphs

A **subgraph** is an isolated child database that lives on the same instance as its parent. Subgraphs are useful for separating environments (dev / staging / prod), for holding agent scratch space and structured working data, or for forking a copy of a parent's data. They do not hold semantic memory — that store lives on the parent graph (see below).

### What a Subgraph Shares (and Doesn't)

| Property | Behavior |
| --- | --- |
| **Data** | Fully isolated from the parent and from sibling subgraphs |
| **Credit pool** | Shared with the parent graph — spending resolves to the parent before any lookup |
| **Permissions** | Inherited from the parent graph; a subgraph has no membership of its own |
| **Instance / RAM / storage** | Shared with the parent (same EC2 instance) |
| **LadybugDB database** | Its own, and **writable** — unlike the parent's |
| **Extensions OLTP schema** | **None.** A subgraph is not an extensions tenant (see below) |
| **Semantic memory** | **None of its own.** `remember` and `update-memory` are refused; memory lives on the parent |

### A Subgraph Is Not an Extensions Tenant

This is the single most important thing to know about subgraphs, and the relationship is the inverse of what most people assume:

> **The main graph is read-only over Cypher and has an extensions surface. A subgraph is writable over Cypher and has none.**

A subgraph is a **modality container** — a live, writable graph database for scratch work and knowledge kept as nodes. It is not a second copy of the product. Concretely:

- **`POST /extensions/{parent}_{name}/graphql` returns `403`.** The request is refused outright rather than quietly resolved to the parent, so a query cannot silently read the wrong tenant's books. The check runs after authentication, so the denial stays attributable to a user.
- **No extensions session can open against it.** The schema-name validator accepts `kg` plus hex only, and a subgraph ID contains an underscore — the exclusion is structural, not a rule someone has to remember to apply.
- **Its MCP tool list is trimmed to an allowlist.** A subgraph inherits its parent's `schema_extensions` column (the subgraph service copies it), so without a cut it would advertise the parent's entire RoboLedger tool surface. Instead it advertises a short set: schema DDL, Cypher in both directions, and navigation back out. Trimming happens over the assembled list rather than at each registration site, so a newly added OLTP tool is excluded by default.
- **It has no semantic memory store of its own.** `remember` and `update-memory` are refused on a subgraph (`403` over REST, and absent from its MCP tool list) — store memories on the parent graph, or keep the data in the subgraph as nodes. `recall` and `forget` stay available, so anything stored on a subgraph earlier can still be read and removed.

The inherited `schema_extensions` column is therefore best read as vestigial on a subgraph: it is copied for bookkeeping, and every surface that would act on it declines to.

### Naming and ID Rules

- **Name**: alphanumeric only, 1–20 characters, no hyphens or underscores. The name is normalized to lowercase. `dev`, `staging`, and `prod1` are valid; `dev-1` and `my_env` are rejected.
- **ID format**: `{parent_id}_{subgraph_name}`. A subgraph named `dev` under `kg1234567890abcdef` becomes `kg1234567890abcdef_dev`.
- **Single-level only**: you cannot create a subgraph of a subgraph.
- **Shared repositories are platform-managed.** They *can* have subgraphs, created by the platform rather than by you — but you cannot create or delete them; both operations return `403`. You query them exactly like the parent repository.
- **Capacity**: capped per tier. Exceeding the cap returns `403`. Call `GET /v1/graphs/tiers` or `GET /v1/graphs/{graph_id}/limits` for the cap your tier enforces.

### Create a Subgraph

Subgraph creation is a graph operation and returns an `OperationEnvelope`. Both `name` and `display_name` are **required**; the rest of the body is optional. By default it creates an empty subgraph; set `fork_parent: true` to clone the parent's data.

```bash
curl -X POST "https://api.robosystems.ai/v1/graphs/kg1234567890abcdef/operations/create-subgraph" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "dev",
    "display_name": "Development Environment",
    "description": "Sandbox for testing",
    "fork_parent": false
  }'
```

The resulting subgraph ID is `kg1234567890abcdef_dev`. Two further optional fields shape what you get:

- **`subgraph_type`** — `static` (the default: the parent's base schema plus the parent's extensions), `knowledge` (a knowledge-only schema), or `empty` (a bare database with no schema at all).
- **`metadata`** — a free-form object stored with the subgraph, for your own labelling.

The schema, extensions included, follows `subgraph_type` alone. The request model also carries a `schema_extensions` field, but create-subgraph does not read it, so do not rely on it to choose a subgraph's extensions.

### List Subgraphs

`GET /v1/graphs/{graph_id}/subgraphs` returns the parent's subgraphs along with the tier's `max_subgraphs`, the current `subgraph_count`, and total size.

```bash
curl "https://api.robosystems.ai/v1/graphs/kg1234567890abcdef/subgraphs" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

Subgraph lifecycle writes (create, delete) follow the same operation pattern as other graph operations. See [Graph Operations](Graph-Operations) for the complete operations surface.

## Querying a Graph

Graphs are queried with Cypher over `POST /v1/graphs/{graph_id}/query/cypher`. The sibling route `POST /v1/graphs/{graph_id}/query/sql` reads the graph's DuckDB staging tables — the columnar side of the same graph, used during ingestion and materialization rather than for graph traversal. **SQL is read-only**, and it is **blocked entirely on shared repositories**, which have no user columnar tables; use `/query/cypher` for those.

```bash
curl -X POST "https://api.robosystems.ai/v1/graphs/kg1234567890abcdef/query/cypher" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n) RETURN labels(n) AS label, count(*) AS count"}'
```

### Where Writes Go

The Cypher endpoint enforces a three-tier write policy, and knowing which tier you are in explains every `403` you are likely to see:

| Target | Cypher writes | Because |
| --- | --- | --- |
| **Main graph** (`kg…`) | Rejected | It is a **projection**, rebuilt from an upstream source of truth. Hand-editing a derived artifact would be silently undone at the next rebuild. |
| **Subgraph** (`kg…_dev`) | Allowed, for `member` and `admin` | It is a first-class writable database with no upstream — the only such surface on the platform. `viewer` is read-only and gets a `403`. |
| **Shared repository** (`sec`) | Rejected for everyone | Platform-managed public data. |

So how does data reach a main graph? It depends on what kind of graph it is — and this is exactly the distinction [The Three Stores](#the-three-stores) sets up:

- **Entity graphs (RoboLedger / RoboInvestor)** — write through the extensions command surface, `POST /extensions/{domain}/{graph_id}/operations/{op_name}`. Those writes land in the extensions PostgreSQL tenant schema, mark the graph stale, and are swept into the graph by the next materialization. The graph catches up; you never write to it directly.
- **Custom graphs** — write through the staging and materialization pipeline: upload a file, ingest it to a staging table, materialize. See [Custom Graph Schema](Custom-Graph-Schema) and [File Uploads](File-Uploads).

Regardless of tier, three classes of statement are refused on the query endpoint entirely: bulk operations (`COPY`, `LOAD`, `IMPORT`), administrative operations (`EXPORT`, `INSTALL`, `ATTACH`), and schema DDL (`CREATE`/`DROP`/`ALTER TABLE`). A main graph's schema is fixed at creation so it stays consistent with its staging tables; a subgraph's schema is extended through the MCP tools `add-node-table` and `add-relationship-table`, not through the query endpoint.

### Querying with MCP

Any MCP client (Claude, ChatGPT, Cursor, and others) can query a graph through the RoboSystems MCP server. The `graph_id` comes from the connection's context, so it is never passed as a tool argument.

The tool set is not fixed — it is assembled from the graph's schema extensions, so a plain custom graph and a RoboLedger graph see very different lists. **The MCP `tools/list` call over the transport (`POST /v1/graphs/{graph_id}/mcp`) is the authoritative answer for a given graph** — the old REST listing endpoint was removed when OAuth landed; what follows is the shape of it. On a plain graph the tools fall into six families:

| Family | Tools |
| --- | --- |
| **Graph reads** | `get-graph-schema` (run this first), `read-graph-cypher`, `get-graph-info` |
| **Graph writes** | `write-graph-cypher`, `add-node-table`, `add-relationship-table` |
| **GraphQL plane** | `get-graphql-schema`, `query-graphql` |
| **Lifecycle** | `list-subgraphs`, `create-subgraph`, `delete-subgraph`, `create-backup`, `set-write-policy`, `sync-connection` |
| **Memory** | `remember`, `recall`, `update-memory`, `forget` |
| **Documents** | `create-document`, `get-document`, `list-documents`, `update-document`, `delete-document`, `search-documents`, `get-document-section` |

`read-graph-cypher` is read-only: `CREATE`, `SET`, `DELETE`, `MERGE`, and `DROP`, along with `CALL db.` and `CALL apoc.`, are blocked — writes go through `write-graph-cypher`, which (like `add-node-table` and `add-relationship-table`) acts only on a subgraph and answers `subgraph_required` on a parent graph. `get-example-queries`, which returns sample queries tailored to a graph's schema, is contributed by the **RoboLedger extension** and is absent on a plain graph. Extension graphs add substantially more on top of this base. See [Custom Graph Schema](Custom-Graph-Schema) for a worked MCP example and [AI Operators and MCP](AI-Operators-and-MCP) for the full surface.

**Connecting MCP to a subgraph gives you a deliberately smaller list.** Because a subgraph is not an extensions tenant, its tool list is trimmed to what is genuinely meaningful there: schema DDL, Cypher in both directions, `get-example-queries` where the parent's extensions contribute it, `recall` and `forget` for anything stored there earlier, `get-graph-info`, and `list-subgraphs`. Tools that would need an OLTP schema or a materialization relationship are withheld rather than allowed to fail — the platform-lifecycle ones are the reason the cut exists, because they would otherwise fail *quietly by succeeding*, returning a confident sync status about a relationship the subgraph does not have.

## Crossing the Tenant Boundary

Isolation is the default everywhere on this page, with exactly one deliberate exception, and it is worth stating plainly so the model in your head is right.

**Report sharing** lets a graph distribute a published report to named subscriber graphs — typically in *other organizations*. The motivating case is investor relations: a company sends its quarterly statements to its cap table, and the investors are by definition outside the company's org.

What makes this consistent with everything above rather than a hole in it:

- **A subscriber receives a copy, never access.** No grant is created on the source graph; rows are written into the recipient's own tenant schema. The "no cross-org access grants" rule and this feature govern different things and do not conflict.
- **The surface is the report, never the ledger.** What crosses is the published report, its facts, the entity, and the concepts needed to read those facts. Transaction data does not leave. Shareholders get statements, not the general ledger.
- **Provenance is explicit at the destination.** Shared content lands under a provenance record naming the source graph and report, so a recipient's data is never silently commingled with their own.
- **Authorization is capability-style, and the recipient holds the exit.** A `graph_id` is unguessable, so the only way a sender holds yours is that you handed it over — the handover is the handshake, with no invite dance. Because a `graph_id` cannot be rotated, recipients get real controls rather than only an entry gate: an admin of the receiving graph can delete a shared report, and a per-graph block list (with optional purge) refuses future deliveries from a sender. Senders can also revoke a delivered copy.

This works within a single deployment. Cross-deployment distribution is the holon JSON-LD export path — see [Serialization and Export](Serialization-and-Export).

## Troubleshooting

### Subgraph Creation Fails: "Maximum subgraphs limit reached"

You have hit the tier's subgraph cap.

**Solution:** Delete an unused subgraph, or change the parent graph to a higher tier with a `change-tier` operation. Check the cap for your tier with `GET /v1/graphs/{graph_id}/limits`. See [Graph Operations](Graph-Operations).

### Subgraph Creation Fails: "Subgraphs are not available"

The graph's tier reports no subgraph capacity, or you are attempting to add a subgraph to a shared repository or to another subgraph.

**Solution:** Subgraphs are single-level and live only under creatable tiers. Confirm you are operating on a parent graph (`kg…`, no underscore) on a tier that allows subgraphs. Shared repositories do have subgraphs, but only the platform creates them.

### GraphQL Returns `403` on a Subgraph

`POST /extensions/{parent}_{name}/graphql` is refused. A subgraph is a modality container, not an extensions tenant — it has no OLTP schema for the resolvers to read.

**Solution:** Target the parent graph. See [A Subgraph Is Not an Extensions Tenant](#a-subgraph-is-not-an-extensions-tenant).

### An Extensions Write Succeeded But Cypher Doesn't See It

The extensions PostgreSQL database and the LadybugDB graph are two stores. A command write lands in OLTP immediately and reaches the graph only when the projection is rebuilt.

**Solution:** Wait for the materialization sensor, or force it with `POST /v1/graphs/{graph_id}/operations/materialize`. Confirm you are querying the right store: OLTP state reads through GraphQL, projected state through Cypher.

### SQL Query Rejected on a Shared Repository

`/query/sql` reads DuckDB staging tables, and shared repositories have none.

**Solution:** Use `POST /v1/graphs/{graph_id}/query/cypher` instead.

### Create Returns `202` But the Graph Isn't Ready

Graph creation is asynchronous.

**Solution:** Read the `operation_id` from the returned `OperationEnvelope` and stream progress at `GET /v1/operations/{operation_id}/stream`. The graph is usable once the operation completes.

### Invalid Subgraph Name

Names must be alphanumeric, 1–20 characters, with no hyphens or underscores.

**Solution:** Replace separators. Use `dev1` instead of `dev-1`, or `myenv` instead of `my_env`.

### Cypher Write Rejected on the Main Graph

The main graph is read-only over Cypher because it is a projection, not a source of truth.

**Solution:** Pick the write path that matches your graph. For an **entity graph**, write through `POST /extensions/{domain}/{graph_id}/operations/{op_name}` and let materialization carry it into the graph. For a **custom graph**, load through the staging and materialization pipeline. For scratch work, write to a **subgraph**, which supports full Cypher writes; semantic memory goes on the parent graph. See [Where Writes Go](#where-writes-go).

### Cypher Write Rejected on a Subgraph

Subgraphs allow writes, but only for `member` and `admin`. A `viewer` gets `403`.

**Solution:** Check your effective role with `GET /v1/graphs/{graph_id}/members`. Note that org owners and admins hold implicit graph admin, so the effective role may be stronger than the explicit grant — and never weaker.

## Related Documentation

**Wiki Guides:**

- **[Core Concepts](Core-Concepts)** - Orientation glossary: `graph_id`, OLTP vs OLAP, blocks, and the other recurring terms
- **[Extensions Surface Overview](Extensions-Surface-Overview)** - The `/extensions/*` surface in full: three sub-surfaces, feature flags, and the operation envelope
- **[Graph Operations](Graph-Operations)** - Lifecycle operations (create-subgraph, delete-subgraph, change-tier, backups, materialize) and the CQRS operation envelope
- **[GraphQL Reads](GraphQL-Reads)** - The typed read surface over the extensions OLTP database and its field catalog
- **[Shared Repositories](Shared-Repositories)** - The `ladybug-shared` tier, repository plans, and how to subscribe to and query public datasets
- **[Authentication and API Keys](Authentication-and-API-Keys)** - API key creation, the `X-API-Key` header, and per-graph access control
- **[Custom Graph Schema](Custom-Graph-Schema)** - Designing node and relationship schemas and querying a custom graph
- **[Querying the Analytical Graph](Querying-the-Analytical-Graph)** - Cypher patterns against the materialized projection
- **[Credits and Billing](Credits-and-Billing)** - Credit pools, what consumes them, and how subgraphs draw from the parent

**Codebase Documentation:**

- **[Graph Routing](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/graph/README.md)** - Graph ID parsing, subgraph resolution, and multi-tenant routing
- **[Authentication](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/auth/README.md)** - Authentication system internals
- **[Graph API](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graph_api/README.md)** - LadybugDB backend and query execution
- **[Extensions Models](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/extensions/README.md)** - Extensions OLTP models and schema-per-graph tenancy
- **[GraphQL Extensions](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md)** - Strawberry GraphQL surface and resolver patterns

**API Reference:**

- **[API reference](https://robosystems.ai/docs/api)** - Full endpoint and schema reference with machine-readable OpenAPI spec

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
