# GraphQL Reads

GraphQL gives you ad-hoc, strongly-typed reads over a graph's operational (OLTP) extensions data: ledger, investor and taxonomy records as they stand now. Every read goes through a single endpoint — `POST /extensions/{graph_id}/graphql` — that is scoped to one graph by its URL. This page covers the endpoint and its auth model, how the schema is composed per deployment, worked `curl` queries against `fiscalCalendar` and `entity`, schema discovery through introspection, and the `get-graphql-schema` / `query-graphql` MCP tools that give an AI agent the same surface.

**Quick Start:** With an API key and a `graph_id` in hand, `curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql"` with `-H "X-API-Key: $ROBOSYSTEMS_API_KEY"` and `-d '{"query": "{ entity { id name } }"}'` returns the parent entity for that graph.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development.md).

## The Two Read Planes

RoboSystems exposes a graph's data through two distinct read planes. This page is about the first one. Knowing which plane you want keeps you from reaching for the wrong tool.

| Plane | Endpoint / tool | Backs onto | Reads | Use when |
| --- | --- | --- | --- | --- |
| **GraphQL / OLTP** (this page) | `POST /extensions/{graph_id}/graphql` · MCP `query-graphql` | The per-tenant PostgreSQL **extensions** database | The live operational source of truth — ledger and investor records as they stand right now | You want "what's in the books right now": entity metadata, fiscal calendar state, agents, transactions, mappings, period-close status |
| **Cypher / OLAP** | `POST /v1/graphs/{graph_id}/query/cypher` · MCP `read-graph-cypher` | The materialized **LadybugDB** graph | The analytical projection, blue/green materialized from the OLTP database | You want an analytical scan over the materialized graph — multi-hop traversals, aggregate rollups, report rendering |

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
- [Self-hosted deployments](#self-hosted-deployments)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Overview

The GraphQL surface gives you typed, ad-hoc reads over a graph's operational data. Four ideas make it work end-to-end:

1. **One endpoint, scoped by URL.** All queries go to `POST /extensions/{graph_id}/graphql`. The `graph_id` lives in the path, so a query is implicitly scoped to that one graph. You never pass `graphId` as a query argument — that "wrong graph" failure mode is designed out. Auth and per-graph access are checked before any resolver runs.
2. **Typed reads from shared Pydantic models.** The schema is built with [Strawberry](https://strawberry.rocks/) and auto-derived from the same Pydantic response models that the REST write operations return. REST writes and GraphQL reads share one schema by construction, so the shapes never drift.
3. **Schema-per-flag composition.** The schema you see depends on which extensions are enabled on the deployment. A ledger-only deployment has no investor fields at all — they are absent from introspection, not a runtime error.
4. **Thin resolvers, ops layer is the truth.** Resolvers open an extensions-database session and delegate to the operations layer — the same functions the MCP tools and REST endpoints call. There is no business logic in the GraphQL layer itself.

## Prerequisites

- A RoboSystems account and an API key, created in the app under **Settings → API keys**. See [Quick Start](Quick-Start.md).
- A `graph_id` to query against — list yours with `GET /v1/graphs` or copy it from the app's graph selector. A RoboLedger graph comes from roboledger.ai when you connect QuickBooks, or from **Create Graph** in the app.
- On robosystems.ai the GraphQL endpoint is on, with both the ledger and investor field groups. A self-hosted deployment mounts it only when at least one extension (`ROBOLEDGER_ENABLED` or `ROBOINVESTOR_ENABLED`) is enabled, behind the `EXTENSIONS_GRAPHQL_ENABLED` kill switch.

## Quick Start

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai
export GRAPH_ID=kg...               # from GET /v1/graphs or the app's graph selector

# Run your first GraphQL read
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ entity { id name } }"}'
```

## The Endpoint

```text
POST /extensions/{graph_id}/graphql
```

- `graph_id` is a FastAPI path parameter validated against the platform's graph-or-subgraph ID pattern. Three kinds of value work: an entity graph (`kg` + 16 or more hex chars), a shared-repository id (`sec`), or the reserved **`library`** sentinel, which browses the shared public taxonomy library rather than a tenant. It is **not** a GraphQL argument — resolvers read it from request context.
- **Subgraph IDs match the pattern but are rejected with HTTP 403.** A subgraph is a modality container with no extensions schema of its own, so `POST /extensions/{parent}_{name}/graphql` fails at runtime rather than at validation. Target the parent graph.
- The request body is a standard GraphQL POST payload: a JSON object with a `query` string, optional `variables` object, and optional `operationName`.
- The in-browser **GraphiQL** explorer (a GET on the same URL) is development-only and is not mounted on `api.robosystems.ai`. Introspection over POST works on the hosted API, with or without credentials — see [Discovering the Schema](#discovering-the-schema).
- The endpoint is mounted only on deployments where `ROBOLEDGER_ENABLED` or `ROBOINVESTOR_ENABLED` is set, and a deployment can disable it entirely with `EXTENSIONS_GRAPHQL_ENABLED=false`. On robosystems.ai it is enabled.

This page shows usage examples. Every query the schema serves is documented field by field at [robosystems.ai/docs/extensions/graphql](https://robosystems.ai/docs/extensions/graphql); the endpoint's own request and response contract is in the extensions API reference at [robosystems.ai/docs/extensions](https://robosystems.ai/docs/extensions). How the surface is built is in the codebase ([graphql/README.md](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md)).

## Authentication

GraphQL reads use the same authentication as the rest of the API: the `X-API-Key` header.

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ entity { id name } }"}'
```

Rules and behaviors:

- Create the key in the app under **Settings → API keys**; it is shown once, and can be scoped to all your graphs or to one graph. Send it in the header — never put it in the URL.
- `Authorization: Bearer` (JWT) is a frontend concern only — don't use it for backend `curl` testing.
- **Introspection works without credentials; data does not.** You can fetch the schema (an introspection query over POST) without an API key. A query for real data with no valid credentials returns HTTP 200 with a GraphQL error whose `extensions.code` is `UNAUTHENTICATED` — not a transport-level 401. Invalid or expired credentials *do* produce an HTTP 401.
- Access is checked per graph before any resolver runs. A valid key that lacks access to the requested `graph_id` is refused with HTTP 403 before the query executes, so there is no GraphQL error body to branch on.

## Schema-per-Flag Composition

The schema is not static — it is composed at startup from the extensions enabled on the deployment. This means the set of available fields differs between a ledger-only deployment and a full ledger + investor deployment.

| Field group | Gated by | Examples |
| --- | --- | --- |
| Ledger fields | `ROBOLEDGER_ENABLED` | `entity`, `agents`, `transactions`, `fiscalCalendar`, `periodCloseStatus`, `trialBalance`, `reports` |
| Investor fields | `ROBOINVESTOR_ENABLED` | `portfolios`, `securities`, `positions`, `holdings`, `portfolioBlock` |
| Always-on fields | not flag-gated | `informationBlock` / `informationBlocks`, taxonomy blocks, the `library*` family, `hello` (an auth probe) |

The consequence: on a ledger-only deployment, a field like `portfolios` is **absent from the schema entirely**. Introspection won't list it, and querying it is a schema validation error rather than a runtime "not initialized" error. Clients should branch on the schema shape they discover through introspection rather than trial-and-error against fields that may not exist. This is why the recommended first step from any client — human or agent — is to read the schema (an introspection query or `get-graphql-schema`) and query only the fields the deployment actually exposes.

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
curl -X POST "https://api.robosystems.ai/extensions/library/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ libraryTaxonomies { name } }"}'
```

## A Worked Query: Fiscal Calendar and Entity

The `fiscalCalendar` and `entity` reads are good first queries because they return a small, stable shape and exist on any RoboLedger-enabled graph.

### Fiscal calendar

```bash
export GRAPH_ID=kg...   # from GET /v1/graphs or the app's graph selector

curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
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

The wire field names are the camelCase form of the underlying Pydantic fields: `closed_through` becomes `closedThrough`, `close_target` becomes `closeTarget`, `gap_periods` becomes `gapPeriods`, and so on. Querying the snake_case names fails. The full `fiscalCalendar` shape carries more than the five fields above — `fiscalYearStartMonth`, `catchUpSequence`, a count and a sample for each blocker that has detail (`pendingObligationCount`, `strandedObligationCount`, `reconcilingItemCount`, `unpostedSourceEventCount`, `unreconciledAccountCount`, each with its `…Sample`), `earliestPendingPeriod`, `syncStaleDays`, `lastCloseAt`, `lastSyncAt`, and a `periods` list — discover them through introspection. What each blocker means is on [Period Close](Period-Close.md#blockers).

### Parent entity

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
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
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ agents(agentType: \"customer\", limit: 10, offset: 0) { id name } }"}'
```

The `agents` resolver accepts `agentType`, `source`, `isActive`, `limit`, and `offset`. Pagination is bounded: `limit` must be between 1 and 1000 and `offset` must be 0 or greater, otherwise the query returns an `INVALID_PAGINATION` error.

### What is available, and where the list lives

**The full field list is published at [robosystems.ai/docs/extensions/graphql](https://robosystems.ai/docs/extensions/graphql)** — one page per query, with its arguments, its return type and every field of that type, and an example call. It is generated from the schema the API is actually serving, so it is deployment-accurate and cannot drift from the code. Every query field carries a description, which means introspection answers the same question offline: `{ __schema { queryType { fields { name description } } } }`.

This page does not repeat that list. What follows is the orientation the generated reference cannot give you: which read to reach for, given a question.

| You want to know | Start with |
| --- | --- |
| What is blocking the month-end close | `fiscalCalendar` — `closedThrough`, `blockers`, the per-blocker counts (`reconcilingItemCount`, `unpostedSourceEventCount`, `unreconciledAccountCount`, …) and `syncStaleDays` in one read |
| Whether the books tie to their independent sources for a month | `reconciliations(period: "YYYY-MM")` — each reconciliation's `status`, `unreconciledDifference`, balances and sign-off. See [Period Close](Period-Close.md#reconciliations) |
| Which schedules are drafted, posted or pending for a period | `periodCloseStatus`, then `periodDrafts` for the entries themselves |
| Who owes us, and how concentrated it is | `agents(agentType: "customer")` with `openReceivable` on each — names and balances together. `openReceivablesByAgent` gives the same balances keyed by id only |
| What a counterparty has done | `agent`, then the event and transaction history behind the balance |
| The chart of accounts, and what is still unmapped | `accounts` / `accountTree`, then `unmappedElements` and `mappingCoverage` |
| Balances rolled up to reporting concepts | `mappedTrialBalance`, or `accountRollups` for the CoA-level view |
| A published report, whole | `reportPackage` — metadata plus every rendered block in one round trip |
| A single statement out of a report | `statement(reportId:, blockType:)` |
| A rendered schedule, rollforward or statement block | `informationBlock`; pass `scenarioId` for a forecast slice and `series` for the whole period series |
| The books' scale and sync freshness | `summary` |
| What the taxonomy says about a concept | `libraryElement` / `searchLibraryElements`, then `libraryElementTree` and `libraryElementClassifications` |

**Balances are in minor currency units.** `openBalanceCents` is cents. Convert once, on the way in.

`reportDownloadUrl(reportId, format, expiresIn)` is worth calling out on its own: it is how a client downloads a published report's serialization bundle. (The MCP tool `get-report-bundle` returns the same link to an AI agent.) A download is a read of stored state, so it lives here rather than as a REST resource. `format` is `HOLON_JSONLD` (the default), `TAVI` or `XBRL_2_1`. Every format resolves to a short-lived presigned S3 URL — the holon is stamped at publish time; Tavi and XBRL 2.1 are built and cached on first request — and the resolver returns that URL, never the bytes. The holon is the one format that carries the whole report, and the result's `omittedContent` names what the chosen format leaves out: Tavi has no place for four kinds of content, and XBRL 2.1 omits the disclosure notes. `expiresIn` is bounded between 60 and 3600 seconds; out of range returns `INVALID_EXPIRES_IN`. A report with no published bundle raises `REPORT_BUNDLE_NOT_AVAILABLE`; an unknown id returns `null`.

On an investor-enabled deployment the investor query root adds `portfolios`, `securities`, `security`, `positions`, `position`, `holdings` and `portfolioBlock`. On any deployment, the taxonomy library reads (`library*`, `searchLibraryElements`) are always present — browse them with the `library` graph id.

## Discovering the Schema

On the hosted API you discover what fields a deployment exposes with an introspection query over POST (an AI agent uses `get-graphql-schema`, below). The in-browser GraphiQL explorer is development-only — it is not mounted on `api.robosystems.ai`; see [Self-hosted deployments](#self-hosted-deployments).

### Introspection over HTTP

Introspection is a normal GraphQL query and works without credentials:

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ __schema { queryType { fields { name } } } }"}'
```

This returns every top-level query field for that deployment — the authoritative answer to "what can I read here?". Ask for `description` alongside `name` and you get each field's documentation with it, which is the same text the [published reference](https://robosystems.ai/docs/extensions/graphql) renders:

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ __schema { queryType { fields { name description } } } }"}'
```

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

Direct HTTP queries are bounded too, just more loosely, and on a different third axis. The endpoint applies Strawberry limiters for maximum depth 15, maximum 30 aliases, and maximum 2000 **tokens** — deployment settings `EXTENSIONS_GRAPHQL_MAX_DEPTH`, `EXTENSIONS_GRAPHQL_MAX_ALIASES`, and `EXTENSIONS_GRAPHQL_MAX_TOKENS`, which a deployment's operator can tune at runtime through SSM without a redeploy. They exist to bound query cost against the small extensions OLTP connection pool, where each resolved field can open a session. Introspection is exempt from the depth limiter, so SDK codegen is unaffected.

Note the easy-to-confuse pairing: `get-graphql-schema` returns the **GraphQL SDL** for this OLTP plane, while `get-graph-schema` (no `ql`) returns the **Cypher/graph** schema for the analytical LadybugDB plane. They are different tools for different planes. See [AI Operators and MCP](AI-Operators-and-MCP.md) for the full MCP tool surface.

## Error Surface

GraphQL data errors return HTTP 200 with a typed code in `extensions.code`, so clients branch on the code rather than the HTTP status.

| Code | Meaning |
| --- | --- |
| `UNAUTHENTICATED` | No credentials were sent (introspection still works) |
| `INVALID_PAGINATION` | `limit` or `offset` out of range (`limit` 1–1000, `offset` ≥ 0) |
| `EXTENSION_NOT_PROVISIONED` | Graph isn't provisioned for the extension you queried |
| `LEDGER_NOT_INITIALIZED` | Graph has no ledger schema yet — connect a data source or run a sync first |
| `INVESTOR_NOT_INITIALIZED` | Same, for the investor surface |
| `INVALID_PERIOD` | A `period` argument isn't a valid `YYYY-MM` month (`reconciliations`) |
| `INVALID_ARGUMENT` | An argument failed a resolver's own check, such as a library tree's `maxDepth` outside 1–10 |
| `INVALID_EXPIRES_IN` | `reportDownloadUrl`'s `expiresIn` is outside 60–3600 seconds |
| `REPORT_BUNDLE_NOT_AVAILABLE` | The report exists but has no published bundle yet |
| `REPORT_BUNDLE_SIGNING_FAILED` | The bundle couldn't be built or its link signed, a server-side fault rather than a missing bundle |

Three failures never reach the GraphQL layer, so they carry no `extensions.code`: invalid or expired credentials are HTTP 401, and a graph you have no access to, or a subgraph id, is HTTP 403.

## Troubleshooting

### `extensions.code: "UNAUTHENTICATED"` even though the schema loads

Introspection works without credentials, but data resolvers require an API key. If schema browsing succeeds but every data query returns `UNAUTHENTICATED`, you're missing the `X-API-Key` header. Add it:

```bash
-H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

If you *are* sending a key and get a transport-level HTTP 401 (not a GraphQL `UNAUTHENTICATED` error), the credential itself is invalid or expired — create a new key under **Settings → API keys** in the app.

### `Cannot query field "..."` — schema validation error

The field you asked for isn't in this deployment's schema. Two common causes:

- **Wrong casing.** Wire field names are camelCase. `close_target` is wrong; `closeTarget` is correct.
- **Flag-gated field on a deployment that doesn't enable it.** Investor fields like `portfolios` simply don't exist on a ledger-only deployment. Run an introspection query and query only fields the deployment actually exposes.

### `graph_id` unknown / can't build the URL

List the graphs your key can reach and take the `graph_id` of the one you want, or copy it from the app's graph selector:

```bash
curl "https://api.robosystems.ai/v1/graphs" -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

### `LEDGER_NOT_INITIALIZED` on a fresh graph

The graph exists but has no ledger schema yet. Connect a data source and run a sync — for example, connect QuickBooks from roboledger.ai. Once data is loaded, the ledger fields resolve.

### `EXTENSION_NOT_PROVISIONED` when querying ledger fields

You queried ledger fields against a graph that doesn't have the roboledger extension provisioned. Provision the extension on that graph, or query a graph that has it. (Shared-repository graphs such as the SEC repo deliberately declare the roboledger extension, so ledger-shaped reads work against shared data.)

### `INVALID_PAGINATION` on a list query

`limit` must be between 1 and 1000 and `offset` must be 0 or greater. A `limit` of 0 or a negative `offset` trips this. Set them within range.

### I passed `graphId` as an argument and it failed

The graph is scoped by the URL path, never by a query argument. The query body is just `{ entity { ... } }` — there is no `graphId` field on any resolver. Put the graph in the URL: `POST /extensions/$GRAPH_ID/graphql`.

## Self-hosted deployments

**GraphiQL.** On a development deployment, open the endpoint URL in a browser — `http://localhost:8000/extensions/<your graph id>/graphql`. GraphiQL renders with introspection enabled; use the **Docs** / **Schema** panel to browse types and fields and the editor to compose and run queries interactively.

**A demo graph.** `just demo-roboledger` provisions a graph with synthetic books (see [RoboLedger Demo Walkthrough](RoboLedger-Demo-Walkthrough.md)); on a fresh graph it is an alternative to connecting a data source. Demo scripts write the `graph_id` to `.local/config.json` under `graphs.<slot>.graph_id` — not a top-level key — and slot names vary by script (`cascade_demo`, `cascade_demo_<entity_type>`, `roboledger_skeleton`, `roboinvestor_demo`, `saas_startup`, `custom_graph_demo`, and others). Picking a slot positionally (`to_entries[0]`) often grabs another demo's graph, so list the slots and name yours:

```bash
jq -r '.graphs | keys[]' .local/config.json
GRAPH_ID=$(jq -r '.graphs.cascade_demo.graph_id' .local/config.json)   # substitute your slot
```

For real synced data on your own stack, see [Connecting QuickBooks Locally](Connecting-QuickBooks-Locally.md).

## Related Documentation

**Guides:**

- **[Extensions Surface Overview](Extensions-Surface-Overview.md)** - The three sub-surfaces (GraphQL typed reads, command writes, analytical view operations), the shared `OperationEnvelope`, and the feature-flag story
- **[RoboLedger Operations](RoboLedger-Operations.md)** - The write counterpart at `/extensions/roboledger/{graph_id}/operations/*`, making the read/write split concrete
- **[Querying the Analytical Graph](Querying-the-Analytical-Graph.md)** - The Cypher/OLAP plane this page contrasts against, with `read-graph-cypher` and `get-graph-schema`
- **[AI Operators and MCP](AI-Operators-and-MCP.md)** - The MCP tool surface, including `get-graphql-schema` and `query-graphql`
- **[Connecting QuickBooks Locally](Connecting-QuickBooks-Locally.md)** - Shows the OLTP → materialize → Cypher flow this page's analytical plane contrasts against
- **[RoboLedger Demo Walkthrough](RoboLedger-Demo-Walkthrough.md)** - Provisions a graph with synthetic books to run these queries against

**Codebase Documentation:**

- **[GraphQL Surface](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md)** - Strawberry GraphQL surface, Pydantic auto-derivation, resolver patterns
- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business logic kernel the resolvers delegate to
- **[GraphQL reference](https://robosystems.ai/docs/extensions/graphql)** - Every query field, with its arguments, return type and an example call, generated from the live schema
- **[Extensions API reference](https://robosystems.ai/docs/extensions)** - The extensions surface, generated from the live OpenAPI spec

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Technical docs](Home.md)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
