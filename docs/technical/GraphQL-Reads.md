# GraphQL Reads

This guide shows you how to run ad-hoc, strongly-typed reads against the operational (OLTP) extensions data of a graph using GraphQL. Every read goes through a single endpoint — `POST /extensions/{graph_id}/graphql` — that is scoped to one graph by its URL. It covers the endpoint and its auth model, how the schema is composed per deployment, worked `curl` queries against `fiscalCalendar` and `entity`, the GraphiQL explorer, and the `get-graphql-schema` / `query-graphql` MCP tools that give an AI agent the same surface.

**Quick Start:** With the stack running and a `graph_id` in hand, `curl -X POST "http://localhost:8000/extensions/$GRAPH_ID/graphql"` with `-d '{"query": "{ entity { id name } }"}'` returns the parent entity for that graph.

## The Two Read Planes

RoboSystems exposes a graph's data through two distinct read planes. This page is about the first one. Knowing which plane you want keeps you from reaching for the wrong tool.

| Plane | Endpoint / tool | Backs onto | Reads | Use when |
| --- | --- | --- | --- | --- |
| **GraphQL / OLTP** (this page) | `POST /extensions/{graph_id}/graphql` · MCP `query-graphql` | The per-tenant PostgreSQL **extensions** database | The live operational source of truth — ledger and investor records as they stand right now | You want "what's in the books right now": entity metadata, fiscal calendar state, agents, transactions, mappings, period-close status |
| **Cypher / OLAP** | `just lbug-query` · MCP `read-graph-cypher` | The materialized **LadybugDB** graph | The analytical projection, blue/green materialized from the OLTP database | You want an analytical scan over the materialized graph — multi-hop traversals, aggregate rollups, report rendering |

The split is deliberate. GraphQL reads hit PostgreSQL directly, so they always reflect the current operational state. Cypher reads hit LadybugDB, which is rebuilt from the OLTP data on a materialization cadence, so it is optimized for analytical scans but lags the OLTP database by one materialization cycle. GraphQL = the operational plane; Cypher = the analytical plane.

## Table of Contents

- [The Two Read Planes](#the-two-read-planes)
- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [The Endpoint](#the-endpoint)
- [Authentication](#authentication)
- [Schema-per-Flag Composition](#schema-per-flag-composition)
- [A Worked Query: Fiscal Calendar and Entity](#a-worked-query-fiscal-calendar-and-entity)
- [More Example Queries](#more-example-queries)
- [Discovering the Schema](#discovering-the-schema)
- [From an AI Agent: the MCP Tools](#from-an-ai-agent-the-mcp-tools)
- [Error Surface](#error-surface)
- [Troubleshooting](#troubleshooting)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Overview

The GraphQL surface gives you typed, ad-hoc reads over a graph's operational data. Four ideas make it work end-to-end:

1. **One endpoint, scoped by URL.** All queries go to `POST /extensions/{graph_id}/graphql`. The `graph_id` lives in the path, so a query is implicitly scoped to that one graph. You never pass `graphId` as a query argument — that "wrong graph" failure mode is designed out. Auth and per-graph access are checked before any resolver runs.
2. **Typed reads from shared Pydantic models.** The schema is built with [Strawberry](https://strawberry.rocks/) and auto-derived from the same Pydantic response models that the REST write operations return. REST writes and GraphQL reads share one schema by construction, so the shapes never drift.
3. **Schema-per-flag composition.** The schema you see depends on which extensions are enabled on the deployment. A ledger-only deployment has no investor fields at all — they are absent from introspection, not a runtime error.
4. **Thin resolvers, ops layer is the truth.** Resolvers open an extensions-database session and delegate to the operations layer — the same functions the MCP tools and REST endpoints call. There is no business logic in the GraphQL layer itself.

## Prerequisites

- A running local stack (`just start` from `robosystems/`). The GraphQL endpoint is mounted only when at least one extension (`ROBOLEDGER_ENABLED` or `ROBOINVESTOR_ENABLED`) is enabled, and is gated by the `EXTENSIONS_GRAPHQL_ENABLED` kill switch.
- A demo user with an API key. Run `just demo-user` from `robosystems/`; the key is written to `.local/config.json`.
- A `graph_id` to query against. The fastest way to get one with real ledger data is the [RoboLedger Demo Walkthrough](RoboLedger-Demo-Walkthrough) (`just demo-roboledger`), which provisions a graph and synthetic books. The `graph_id` is written into `.local/config.json` under `graphs.<slot>.graph_id`.
- `jq` installed (used to read the API key out of config in every example below).

## Quick Start

```bash
# 1. Provision a demo user (writes .local/config.json with your API key)
just demo-user

# 2. Provision a graph with synthetic ledger data
just demo-roboledger

# 3. List the graph slots in config.json. Each demo script writes its own
#    (e.g. cascade_demo, cascade_demo_<entity_type>, roboledger_skeleton,
#    roboinvestor_demo, saas_startup, custom_graph_demo, ...):
jq -r '.graphs | keys[]' .local/config.json

# 4. Pull the graph_id out by slot name. `just demo-roboledger` writes to
#    `cascade_demo`; substitute yours if you ran a different script:
GRAPH_ID=$(jq -r '.graphs.cascade_demo.graph_id' .local/config.json)

# 5. Run your first GraphQL read
curl -X POST "http://localhost:8000/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ entity { id name } }"}'
```

Slot names under `graphs.*` are not uniform across demo scripts, and picking one positionally (`to_entries[0]`) grabs whichever slot happens to sort first — often another demo's graph, which then fails every query on this page. List the slots and name the one you want.

## The Endpoint

```text
POST /extensions/{graph_id}/graphql
```

- `graph_id` is a FastAPI path parameter validated against the platform's graph-or-subgraph ID pattern. Three kinds of value work: an entity graph (`kg` + 16 or more hex chars), a shared-repository id (`sec`, `sec_historical`), or the reserved **`library`** sentinel, which browses the shared public taxonomy library rather than a tenant. It is **not** a GraphQL argument — resolvers read it from request context.
- **Subgraph IDs match the pattern but are rejected with HTTP 403.** A subgraph is a modality container with no extensions schema of its own, so `POST /extensions/{parent}_{name}/graphql` fails at runtime rather than at validation. Target the parent graph.
- The request body is a standard GraphQL POST payload: a JSON object with a `query` string, optional `variables` object, and optional `operationName`.
- In dev, opening the same URL in a browser (a GET request) renders the **GraphiQL** explorer with introspection enabled, so you can browse the schema and run queries interactively.
- The endpoint is mounted only when `ROBOLEDGER_ENABLED` or `ROBOINVESTOR_ENABLED` is set, and can be disabled entirely with `EXTENSIONS_GRAPHQL_ENABLED=false`.

This page shows usage examples. For the full machine-readable endpoint contract, see the live OpenAPI spec at [api.robosystems.ai/docs](https://api.robosystems.ai/docs) (or `http://localhost:8000/docs` locally), and the GraphQL surface README in the codebase ([graphql/README.md](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md)).

## Authentication

GraphQL reads use the same authentication as the rest of the local API: the `X-API-Key` header.

```bash
curl -X POST "http://localhost:8000/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ entity { id name } }"}'
```

Rules and behaviors:

- Always read the key from `.local/config.json` with `jq -r .api_key .local/config.json`. Don't stash it in a `TOKEN=` shell variable, and don't put it in the URL.
- `Authorization: Bearer` (JWT) is a frontend concern only — don't use it for backend `curl` testing.
- **Introspection works without credentials; data does not.** You can fetch the schema (for example via GraphiQL) without an API key. A query for real data with no valid credentials returns HTTP 200 with a GraphQL error whose `extensions.code` is `UNAUTHENTICATED` — not a transport-level 401. Invalid or expired credentials *do* produce an HTTP 401.
- Access is checked per graph before any resolver runs. A valid key that lacks access to the requested `graph_id` returns a `FORBIDDEN` error.

## Schema-per-Flag Composition

The schema is not static — it is composed at startup from the extensions enabled on the deployment. This means the set of available fields differs between a ledger-only deployment and a full ledger + investor deployment.

| Field group | Gated by | Examples |
| --- | --- | --- |
| Ledger fields | `ROBOLEDGER_ENABLED` | `entity`, `agents`, `transactions`, `fiscalCalendar`, `periodCloseStatus`, `trialBalance`, `reports` |
| Investor fields | `ROBOINVESTOR_ENABLED` | `portfolios`, `securities`, `positions`, `holdings`, `portfolioBlock` |
| Always-on fields | not flag-gated | `informationBlock` / `informationBlocks`, taxonomy blocks, the `library*` family, `hello` (an auth probe) |

The consequence: on a ledger-only deployment, a field like `portfolios` is **absent from the schema entirely**. Introspection won't list it, and querying it is a schema validation error rather than a runtime "not initialized" error. Clients should branch on the schema shape they discover through introspection rather than trial-and-error against fields that may not exist. This is why the recommended first step from any client — human or agent — is to read the schema (GraphiQL or `get-graphql-schema`) and query only the fields the deployment actually exposes.

### The always-on group in full

The always-on group is larger than one table cell suggests. Three query classes — information blocks, taxonomy blocks, and the library — are composed on every deployment regardless of flags, because they are cross-domain and not gated by a per-graph extension:

- **Information blocks** — `informationBlock`, `informationBlocks`
- **Taxonomy blocks** — `taxonomyBlock`, `taxonomyBlocks`
- **Library taxonomies** — `libraryTaxonomies`, `libraryTaxonomy`, `libraryTaxonomyArcs`, `libraryTaxonomyArcCount`
- **Library elements** — `libraryElements`, `libraryElement`, `searchLibraryElements`, `libraryElementTree`, `libraryElementEquivalents`, `libraryElementArcs`, `libraryElementClassifications`
- **Library structures** — `libraryStructures`, `libraryStructure`
- **Liveness** — `hello`

The `library*` family needs no flag because its visibility follows the session `search_path`, which follows from the URL's `graph_id`: a tenant graph sees its own taxonomy rows with public fallback, while the reserved `library` sentinel browses the shared public library on its own.

```bash
curl -X POST "http://localhost:8000/extensions/library/graphql" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ libraryTaxonomies { name } }"}'
```

## A Worked Query: Fiscal Calendar and Entity

The `fiscalCalendar` and `entity` reads are good first queries because they return a small, stable shape and exist on any RoboLedger-enabled graph.

### Fiscal calendar

```bash
GRAPH_ID=<your graph id>   # from .local/config.json -> graphs.<slot>.graph_id

curl -X POST "http://localhost:8000/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ fiscalCalendar { closedThrough closeTarget gapPeriods closeableNow blockers } }"}'
```

A response looks like this:

```json
{
  "data": {
    "fiscalCalendar": {
      "closedThrough": "2025-11",
      "closeTarget": "2025-12",
      "gapPeriods": 1,
      "closeableNow": true,
      "blockers": []
    }
  }
}
```

The wire field names are the camelCase form of the underlying Pydantic fields: `closed_through` becomes `closedThrough`, `close_target` becomes `closeTarget`, `gap_periods` becomes `gapPeriods`, and so on. Querying the snake_case names fails. The full `fiscalCalendar` shape carries more than the five fields above — `fiscalYearStartMonth`, `catchUpSequence`, blocker-detail fields like `pendingObligationCount` and `earliestPendingPeriod`, `lastCloseAt`, `lastSyncAt`, and a `periods` list — discover them through introspection.

### Parent entity

```bash
curl -X POST "http://localhost:8000/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ entity { id name legalName entityType fiscalYearEnd source } }"}'
```

A response looks like this:

```json
{
  "data": {
    "entity": {
      "id": "...",
      "name": "Cascade Advisory Group LLC",
      "legalName": "Cascade Advisory Group, LLC",
      "entityType": "corporation",
      "fiscalYearEnd": "12-31",
      "source": "native"
    }
  }
}
```

Here too, `legalName`, `entityType`, and `fiscalYearEnd` are the camelCase of `legal_name`, `entity_type`, and `fiscal_year_end`. The `source` field reports where the entity's data originated (`native`, `sec`, `quickbooks`, `xero`, or `plaid`). The full entity shape also exposes identifiers (`cik`, `ticker`, `exchange`, `sic`, `lei`, `taxId`), `status`, `isParent`, `parentEntityId`, address fields, and timestamps.

## More Example Queries

### A paginated list with arguments

GraphQL arguments *are* supported — the one thing you never pass is `graphId`. List resolvers take filtering and pagination arguments:

```bash
curl -X POST "http://localhost:8000/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ agents(agentType: \"customer\", limit: 10, offset: 0) { id name } }"}'
```

The `agents` resolver accepts `agentType`, `source`, `isActive`, `limit`, and `offset`. Pagination is bounded: `limit` must be between 1 and 1000 and `offset` must be 0 or greater, otherwise the query returns an `INVALID_PAGINATION` error.

### The available ledger reads

The ledger query root exposes a wide set of reads. Discover the full, deployment-specific list through introspection, but the available fields include:

- **Entity / agents** — `entity`, `entities`, `agent`, `agents`
- **Receivables / payables** — `openReceivables`, `openPayables`, `openReceivablesByAgent`, `openPayablesByAgent`
- **Events** — `eventBlock`, `eventBlocks`, `summary`
- **Chart of accounts / balances** — `accounts`, `accountTree`, `accountRollups`, `trialBalance`, `mappedTrialBalance`
- **Transactions** — `transactions`, `transaction`
- **Taxonomy / mappings** — `taxonomies`, `reportingTaxonomy`, `elements`, `mappingCandidates`, `unmappedElements`, `structures`, `mappings`, `mapping`, `mappingCoverage`
- **Close / fiscal** — `periodCloseStatus`, `fiscalCalendar`, `periodDrafts`, `closingBookStructures`
- **Reports** — `reports`, `report`, `reportPackage`, `reportDownloadUrl`, `statement`
- **Publishing / cross-graph sharing** — `publishLists`, `publishList`, `blockedSourceGraphs` (the recipient's block list — source graphs barred from sharing reports *into* this graph, managed by the `block-source-graph` / `unblock-source-graph` operations)

`reportDownloadUrl(reportId, format, expiresIn)` is worth calling out: it is the **only** way to download a published report's serialization bundle. The REST download route was retired, and a download is a read of stored state, so it lives here on the read surface rather than as a REST resource or a named operation. Every format resolves to a short-lived presigned S3 URL — JSON-LD is stamped at publish time, XBRL is materialized and cached on first request — and the resolver returns that URL, never the bytes. The client follows it to S3. `format` defaults to JSON-LD; `expiresIn` is the URL's lifetime in seconds, bounded between 60 and 3600, and an out-of-range value returns an `INVALID_EXPIRES_IN` error. A report that exists but has no published bundle yet raises `REPORT_BUNDLE_NOT_AVAILABLE`; a report id that doesn't exist returns `null`.

On an investor-enabled deployment, the investor query root adds `portfolios`, `securities`, `security`, `positions`, `position`, `holdings`, and `portfolioBlock`.

## Discovering the Schema

There are two ways to discover what fields a given deployment exposes.

### GraphiQL in the browser (dev)

In a dev environment, open the endpoint URL in a browser:

```text
http://localhost:8000/extensions/<your graph id>/graphql
```

GraphiQL renders with introspection enabled. Use the **Docs** / **Schema** panel to browse types and fields, and the editor to compose and run queries interactively. This is the fastest way to explore the schema by hand.

### Introspection over HTTP

Introspection is a normal GraphQL query and works without credentials:

```bash
curl -X POST "http://localhost:8000/extensions/$GRAPH_ID/graphql" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ __schema { queryType { fields { name } } } }"}'
```

This returns the names of every available top-level query field for that deployment — the authoritative answer to "what can I read here?"

## From an AI Agent: the MCP Tools

An AI agent reaches the same surface through two MCP tools on the RoboSystems MCP server. The `graph_id` comes from the agent's active workspace context, so — exactly as with the HTTP endpoint — it is never passed as a query argument.

| Tool | Purpose |
| --- | --- |
| `get-graphql-schema` | Returns the GraphQL schema. Default `format: "sdl"` returns the SDL text; `format: "introspection"` returns the full JSON introspection result. |
| `query-graphql` | Executes a read-only GraphQL query. Arguments: `query` (required), `variables` (optional object), `operationName` (optional). |

The intended flow is two steps: discover the schema, then query it.

```text
1. get-graphql-schema           # returns SDL; discover types and fields
2. query-graphql  query="{ fiscalCalendar { closedThrough closeTarget } }"
```

`query-graphql` is strictly read-only. It rejects mutations and subscriptions before execution, and enforces a complexity gate: maximum query depth 10, maximum 200 fields, and maximum 20 aliases. Queries that exceed these limits are rejected rather than executed.

Direct HTTP queries are bounded too, just more loosely, and on a different third axis. The endpoint applies Strawberry limiters for maximum depth 15, maximum 30 aliases, and maximum 2000 **tokens** — `EXTENSIONS_GRAPHQL_MAX_DEPTH`, `EXTENSIONS_GRAPHQL_MAX_ALIASES`, and `EXTENSIONS_GRAPHQL_MAX_TOKENS`, all tunable at runtime through SSM without a redeploy. They exist to bound query cost against the small extensions OLTP connection pool, where each resolved field can open a session. Introspection is exempt from the depth limiter, so SDK codegen is unaffected.

Note the easy-to-confuse pairing: `get-graphql-schema` returns the **GraphQL SDL** for this OLTP plane, while `get-graph-schema` (no `ql`) returns the **Cypher/graph** schema for the analytical LadybugDB plane. They are different tools for different planes. See [AI Operators and MCP](AI-Operators-and-MCP) for the full MCP tool surface.

## Error Surface

GraphQL data errors return HTTP 200 with a typed code in `extensions.code`, so clients branch on the code rather than the HTTP status. (Invalid or expired credentials are the exception — those produce a transport-level HTTP 401.)

| Code | Meaning |
| --- | --- |
| `UNAUTHENTICATED` | No valid credentials |
| `FORBIDDEN` | Valid credentials, but no access to this graph |
| `INVALID_PAGINATION` | `limit` or `offset` out of range (`limit` 1–1000, `offset` ≥ 0) |
| `LEDGER_NOT_INITIALIZED` | Graph has no ledger schema yet — connect a data source or run a sync first |
| `INVESTOR_NOT_INITIALIZED` | Same, for the investor surface |
| `EXTENSION_NOT_PROVISIONED` | Graph isn't provisioned for the extension you queried |

## Troubleshooting

### `extensions.code: "UNAUTHENTICATED"` even though the schema loads

Introspection works without credentials, but data resolvers require an API key. If schema browsing succeeds but every data query returns `UNAUTHENTICATED`, you're missing the `X-API-Key` header. Add it, reading the key from config:

```bash
-H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

If you *are* sending a key and get a transport-level HTTP 401 (not a GraphQL `UNAUTHENTICATED` error), the credential itself is invalid or expired — re-run `just demo-user` to refresh `.local/config.json`.

### `Cannot query field "..."` — schema validation error

The field you asked for isn't in this deployment's schema. Two common causes:

- **Wrong casing.** Wire field names are camelCase. `close_target` is wrong; `closeTarget` is correct.
- **Flag-gated field on a deployment that doesn't enable it.** Investor fields like `portfolios` simply don't exist on a ledger-only deployment. Run introspection or open GraphiQL and query only fields the deployment actually exposes.

### `graph_id` not found in config / can't build the URL

`graph_id` is nested under `graphs.<slot>.graph_id` in `.local/config.json` — it is not a top-level key. The slot name varies by which demo script created it (`cascade_demo`, `cascade_demo_<entity_type>`, `roboledger_skeleton`, `roboinvestor_demo`, `saas_startup`, `custom_graph_demo`, and others), so don't assume the slot name. List the slots and pull the id out:

```bash
jq -r '.graphs | keys[]' .local/config.json
GRAPH_ID=$(jq -r '.graphs.cascade_demo.graph_id' .local/config.json)   # substitute your slot
```

### `LEDGER_NOT_INITIALIZED` on a fresh graph

The graph exists but has no ledger schema yet. Connect a data source (for example, [Connecting QuickBooks Locally](Connecting-QuickBooks-Locally)) and run a sync, or provision a demo graph with `just demo-roboledger`. Once data is loaded, the ledger fields resolve.

### `EXTENSION_NOT_PROVISIONED` when querying ledger fields

You queried ledger fields against a graph that doesn't have the roboledger extension provisioned. Provision the extension on that graph, or query a graph that has it. (Shared-repository graphs such as the SEC repo deliberately declare the roboledger extension, so ledger-shaped reads work against shared data.)

### `INVALID_PAGINATION` on a list query

`limit` must be between 1 and 1000 and `offset` must be 0 or greater. A `limit` of 0 or a negative `offset` trips this. Set them within range.

### I passed `graphId` as an argument and it failed

The graph is scoped by the URL path, never by a query argument. The query body is just `{ entity { ... } }` — there is no `graphId` field on any resolver. Put the graph in the URL: `POST /extensions/$GRAPH_ID/graphql`.

## Related Documentation

**Wiki Guides:**

- **[Extensions Surface Overview](Extensions-Surface-Overview)** - The three sub-surfaces (GraphQL typed reads, command writes, analytical view operations), the shared `OperationEnvelope`, and the feature-flag story
- **[RoboLedger Operations](RoboLedger-Operations)** - The write counterpart at `/extensions/roboledger/{graph_id}/operations/*`, making the read/write split concrete
- **[Querying the Analytical Graph](Querying-the-Analytical-Graph)** - The Cypher/OLAP plane this page contrasts against, with `read-graph-cypher` and `get-graph-schema`
- **[AI Operators and MCP](AI-Operators-and-MCP)** - The MCP tool surface, including `get-graphql-schema` and `query-graphql`
- **[Connecting QuickBooks Locally](Connecting-QuickBooks-Locally)** - Shows the OLTP → materialize → Cypher flow this page's analytical plane contrasts against
- **[RoboLedger Demo Walkthrough](RoboLedger-Demo-Walkthrough)** - Provisions a graph with synthetic books to run these queries against

**Codebase Documentation:**

- **[GraphQL Surface](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md)** - Strawberry GraphQL surface, Pydantic auto-derivation, resolver patterns
- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business logic kernel the resolvers delegate to
- **[API Documentation](https://api.robosystems.ai/docs)** - API reference with machine-readable OpenAPI spec

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
