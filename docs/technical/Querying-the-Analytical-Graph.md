# Querying the Analytical Graph

Query the analytical graph — the LadybugDB projection of a graph's data — with Cypher, with SQL over its DuckDB staging tables, or through MCP tools. It is the OLAP plane of RoboSystems, and the MCP tools wrap the same queries for AI agents.

**Quick Start:** Post `{"query": "MATCH (e:Entity) RETURN e.name LIMIT 10"}` to `https://api.robosystems.ai/v1/graphs/sec/query/cypher` with your API key in `X-API-Key` to run your first Cypher query against the SEC analytical graph.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development).

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [The Public Query Endpoint](#the-public-query-endpoint)
- [Querying the Staging Tables](#querying-the-staging-tables)
- [From an AI Agent — MCP Tools](#from-an-ai-agent--mcp-tools)
- [Read Patterns and Example Cypher](#read-patterns-and-example-cypher)
- [Gotchas and Limits](#gotchas-and-limits)
- [Self-hosted deployments](#self-hosted-deployments)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Overview

RoboSystems exposes two distinct query planes, and it is worth being explicit about which one you are using:

- **Analytical / OLAP** — Cypher (and supporting SQL) over the LadybugDB graph. This is where you traverse relationships and aggregate across an entire dataset. **This page covers the analytical plane.**
- **Operational / OLTP** — typed GraphQL reads over the extensions PostgreSQL database (live transactional state). That plane is covered in [GraphQL Reads](GraphQL-Reads).

The analytical graph is a materialized projection. Data flows into DuckDB staging tables — from the extensions PostgreSQL database on an entity graph, from uploaded files in S3 on a custom graph — and is materialized from there into LadybugDB; a sensor-driven blue/green rebuild keeps the graph in sync with its source. Because it is a projection, the analytical graph can lag the operational source by the length of a rebuild cycle.

There are three windows onto the same analytical data:

1. **The graph itself** — Cypher via `POST /v1/graphs/{graph_id}/query/cypher`.
2. **The DuckDB columnar tables** — SQL via `POST /v1/graphs/{graph_id}/query/sql`, a relational lens on the same graph-centric data, often ahead of the materialized graph.
3. **MCP tools** — `read-graph-cypher` and `get-graph-schema`, the agent-facing wrappers over the first window.

```text
Extensions PostgreSQL (entity graph)  ·  S3 uploads (custom graph)
   │
   ▼
DuckDB staging tables  ──  SQL  ──▶  POST /v1/graphs/{g}/query/sql
   │
   ▼  (materialize)
LadybugDB analytical graph  ──  Cypher  ──▶  POST /v1/graphs/{g}/query/cypher
   │                                          read-graph-cypher (MCP)
   └──────────────────────────────────────▶  get-graph-schema (MCP)
```

## Prerequisites

Before querying, ensure you have:

- A RoboSystems account and an API key — see [Quick Start](Quick-Start)
- A graph to query — the examples use the shared `sec` repository, which needs a subscription from **Repositories** in the app (see [Shared Repositories](Shared-Repositories)); the staging-SQL examples use a graph you own

## Quick Start

Every query goes through the public HTTP API at `https://api.robosystems.ai`, authenticated with `X-API-Key`:

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai

# List companies in the SEC analytical graph
curl -X POST "https://api.robosystems.ai/v1/graphs/sec/query/cypher" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (e:Entity) RETURN e.name, e.identifier LIMIT 10"}'
```

The rest of this guide walks through each surface.

## The Public Query Endpoint

The user-facing way to run Cypher is `POST /v1/graphs/{graph_id}/query/cypher`. The public API adds authentication, rate limiting, billing and lifecycle enforcement, circuit breakers, and queueing on top of the lower-level graph engine.

### Running Ad-Hoc Cypher

Send your API key as `X-API-Key` and post a Cypher query with named parameters. This one returns a company's consolidated annual revenue, written to the [four SEC query rules](SEC-Data-Model-and-Query-Rules#the-four-rules):

```bash
curl -X POST "https://api.robosystems.ai/v1/graphs/sec/query/cypher" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (ent:Entity {ticker: $ticker})<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})-[:FACT_HAS_ELEMENT]->(e:Element), (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: '\''annual'\''}) WHERE e.canonical_concept = $concept AND f.numeric_value IS NOT NULL RETURN DISTINCT ent.ticker AS ticker, e.qname AS qname, p.end_date AS end_date, f.numeric_value AS value ORDER BY end_date DESC LIMIT $limit",
    "parameters": {"ticker": "NVDA", "concept": "revenue", "limit": 3},
    "timeout": 60
  }'
```

The response is a `CypherStatementResponse`, where each row is a dictionary keyed by the returned columns:

```json
{
  "success": true,
  "data": [{"ticker": "NVDA", "qname": "us-gaap:Revenues", "end_date": "2026-01-25", "value": 215938000000.0}],
  "columns": ["ticker", "qname", "end_date", "value"],
  "row_count": 1,
  "execution_time_ms": 18.4,
  "graph_id": "sec",
  "timestamp": "2026-06-11T00:00:00Z",
  "error": null
}
```

### Request Fields

The request body is a `CypherStatementRequest`:

| Field | Type | Notes |
| --- | --- | --- |
| **`query`** | string | The Cypher query; 1–50,000 characters. |
| **`parameters`** | object | Named parameters referenced as `$name` in the query. |
| **`timeout`** | int | 1–300 seconds; defaults to 60. |

The model forbids extra fields. **Always parameterize** with `$name` placeholders rather than interpolating literals into the query string.

### Read-Only on Main Graphs

Read-only is not a property of this endpoint — it is a property of the platform. Every statement, whichever transport carried it, runs through a single **`StatementKernel`** (`middleware/graph/statement_kernel.py`): a transport-independent authorization gauntlet shared by `/query/cypher`, `/query/sql`, the MCP `read-graph-cypher` tool, and the Operator path, delegating keyword analysis to `security/cypher_analyzer.py`. One implementation means those surfaces cannot drift apart from one another.

The kernel is read-only for main graphs. Attempting a `CREATE`, `MERGE`, `SET`, `DELETE`, or other mutation against a main graph returns **HTTP 403**. Data reaches a main graph only by materialization, and where it comes from depends on the graph: an **entity graph** (RoboLedger, RoboInvestor) is written through the extensions command surface and materialized from the extensions database — file uploads are refused on it — while a **custom graph** loads files through the staging pipeline in [File Uploads](File-Uploads). See [Graph Operations](Graph-Operations#materialize) for `materialize`, and [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy#where-writes-go) for the full write map.

Subgraphs are the exception and do allow writes, subject to two carve-outs:

- **Shared repositories and their subgraphs never accept writes.** The exception is for *user* subgraphs only; a write against `sec` — or against any `sec` subgraph — is refused unconditionally.
- **Writing to a user subgraph requires the `member` or `admin` role.** A viewer gets 403: read-only access stays read-only on subgraphs too.

Schema DDL and bulk operations are blocked everywhere: `COPY`/`LOAD`/`IMPORT` return 400; `EXPORT`/`INSTALL`/`ATTACH` and `CREATE`/`DROP`/`ALTER TABLE` return 403. A main graph's schema is fixed at creation; a subgraph's is extended through the MCP tools `add-node-table` and `add-relationship-table`, not through this endpoint.

`CALL` is held to an allowlist. A call to any procedure outside the read-only set below counts as a write — so it is refused on a main graph and a shared repository with 403 — and a procedure that runs a statement passed as a string (`CALL GQL(...)`) is refused on every graph. Neo4j-style introspection such as `CALL db.labels()` or `CALL db.schema()` is not on the list; use `show_tables` and `table_info`, or the schema endpoints below.

| Read-only procedure | Returns |
| --- | --- |
| `CALL show_tables() RETURN *` | Every node and relationship table |
| `CALL table_info('Entity') RETURN *` | One table's properties and types |
| `show_indexes`, `show_functions` | Indexes and available functions |
| `query_fts_index`, `query_vector_index` | Full-text and vector index lookups |
| `db_version`, `current_setting`, `show_connection`, `show_warnings` | Engine version, settings, connection and warnings |

### Response Modes and Queueing

The endpoint accepts a `mode` query parameter (`auto`, `sync`, `async`, `stream`); `auto` is the default and picks a strategy based on query weight and current load. A heavy query may return **HTTP 202** with an `operation_id` instead of inline results. Monitor it over SSE:

```bash
curl -N "https://api.robosystems.ai/v1/operations/{operation_id}/stream" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

Streaming result delivery is available by setting `Accept: application/x-ndjson` or `Accept: text/event-stream`, and `chunk_size` (10–10,000) tunes the streamed batch size.

For the full request/response schema and every query parameter, see the API reference at [https://robosystems.ai/docs/api](https://robosystems.ai/docs/api) (operation `executeCypher`; its SQL peer below is `executeSql`).

## Querying the Staging Tables

The same graph-centric data is also addressable relationally, through the graph's DuckDB columnar tables. Rows land there on their way to the graph, so the tables are often ahead of the materialized graph — which makes SQL over them the way to validate data quality, debug ingestion, or use familiar SQL joins and aggregations on the same dataset.

### SQL Over Staging

`POST /v1/graphs/{graph_id}/query/sql` runs read-only SQL over the columnar tables. `parameters` takes either form: a **list** for positional placeholders (`?`, or numbered `$1`/`$2`), or an **object** for named `$param_name` placeholders, as on the Cypher endpoint. One statement cannot mix the two.

```bash
curl -X POST "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5e6f7a8b/query/sql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT name, industry FROM Company WHERE industry = ? LIMIT ?",
    "parameters": ["Energy", 10]
  }'
```

Use a graph you own — shared repositories such as `sec` reject SQL outright (see [below](#shared-repositories-reject-public-staging-sql)).

The response is a `SqlStatementResponse`, where rows are **arrays**, not dictionaries:

```json
{
  "columns": ["name", "industry"],
  "rows": [["Energy Innovators 1", "Energy"]],
  "row_count": 1,
  "execution_time_ms": 4.1
}
```

Staging SQL is `SELECT`-only, capped at a 30-second timeout and 10,000 rows.

### Listing Staging Tables

`GET /v1/graphs/{graph_id}/tables` lists the staging tables and their metadata. Each `TableInfo` carries `table_name`, `row_count`, `file_count`, `total_size_bytes`, and `s3_location`. Tables with `file_count = 0` are skipped during ingestion.

### Staging Joins Mirror Cypher Hops

In staging, every relationship is a table with `src` and `dst` columns holding node `identifier` values. A Cypher hop becomes a SQL join:

```sql
-- Count facts by element name in staging (the SQL analogue of a FACT_HAS_ELEMENT hop)
SELECT el.name, COUNT(*) AS fact_count
FROM Fact f
JOIN FACT_HAS_ELEMENT fhe ON f.identifier = fhe.src
JOIN Element el ON fhe.dst = el.identifier
GROUP BY el.name
ORDER BY fact_count DESC
LIMIT 20;
```

### Shared Repositories Reject Public Staging SQL

The public `POST /v1/graphs/{graph_id}/query/sql` endpoint returns **HTTP 403** for shared repositories and their subgraphs (for example, `sec`), with a message directing you to `POST /query/cypher` instead — a shared repository has no user columnar tables to query, so SQL is refused there entirely while Cypher continues to serve its reads from the graph. For shared repos, query the graph with Cypher rather than the staging tables.

## From an AI Agent — MCP Tools

The same analytical surface is exposed to AI agents through Model Context Protocol tools. Two of them cover the analytical plane:

- **`read-graph-cypher`** — runs read-only Cypher. The same write keywords blocked at the query endpoint (`CREATE`, `SET`, `DELETE`, `REMOVE`, `MERGE`, `DROP`, `DETACH DELETE`, plus any `CALL` outside the [read-only procedures](#read-only-on-main-graphs)) are rejected here. Input is `{ query: string, parameters?: object }`.
- **`get-graph-schema`** — returns the full graph schema (node types with their properties and data types, plus relationships). No input arguments; the result is cached briefly.

These map directly onto the surfaces above: `read-graph-cypher` is the query endpoint, and `get-graph-schema` is schema discovery. Database reads through MCP — Cypher, schema, and staging — **consume no credits**; only AI/LLM calls cost credits.

AI clients reach these tools over the MCP transport at `POST /v1/graphs/{graph_id}/mcp`, which speaks JSON-RPC. You can call it directly with an API key:

```bash
# List available MCP tools for a graph
curl -s -X POST "https://api.robosystems.ai/v1/graphs/sec/mcp" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }'

# Discover the schema first
curl -s -X POST "https://api.robosystems.ai/v1/graphs/sec/mcp" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{ "jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": { "name": "get-graph-schema", "arguments": {} } }'

# Then run a read-only Cypher query through MCP
curl -s -X POST "https://api.robosystems.ai/v1/graphs/sec/mcp" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{ "jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": { "name": "read-graph-cypher", "arguments": { "query": "MATCH (e:Entity) RETURN e.name LIMIT 5" } } }'
```

The older REST tool endpoints (`GET /mcp/tools`, `POST /mcp/call-tool`) no longer exist.

Alongside these two, `get-graph-info` is available on every graph, and graphs carrying the `roboledger` extension (including `sec`) also expose `get-example-queries`. `resolve-element`, which maps a concept such as "revenue" to the element qnames filers use, is served only on `sec`: it is gated on the shared-repository manifest's semantic enrichment, so a tenant RoboLedger graph does not list it. There is a separate operational MCP plane (`query-graphql`, `get-graphql-schema`) that reads the OLTP database, and an unstructured plane (`search-documents`, `get-document-section`) over the document index. For the full agent surface, see [AI Operators and MCP](AI-Operators-and-MCP).

**Note:** `read-graph-cypher` strips string literals before scanning for write keywords, but the scan matches on whole words. A variable or property named exactly like a blocked keyword — for example a property access `n.set` or a variable `delete` — trips the word-boundary match and is rejected. Names that merely contain a keyword as a substring (such as `created_at` or `deleted`) are fine; avoid naming a read-only variable exactly `set`, `create`, `delete`, `merge`, `remove`, or `drop`.

## Read Patterns and Example Cypher

The examples below use the `sec` repository as an illustrative dataset. Its data model has node types including `Entity`, `Report`, `Fact`, `Element`, `Period`, `Unit`, `Dimension`, `FactSet`, and `Structure`; [SEC Data Model & Query Rules](SEC-Data-Model-and-Query-Rules) covers the whole model and the four rules that decide whether a number read from it is right. Other graphs have their own schemas — always discover the schema before querying.

Before writing Cypher against `sec`, check whether a ready-made view answers the question: `financial-statement-analysis`, `build-fact-grid`, `disclosures`, and `information-block` (MCP tools, and operations under `POST /extensions/roboledger/sec/operations/{name}`) already apply the rules.

### Discover the Schema First

Property and label names are case-sensitive, so start by discovering what exists rather than guessing. From an agent, `get-graph-schema` returns node types, properties with their data types — `STRING`, `INT64`, `DOUBLE`, `BOOLEAN`, `TIMESTAMP`, `JSON` — and relationships in one call.

Over HTTP the same discovery is two plain `GET`s, and they answer different questions:

```bash
# Runtime schema — what is actually in the database right now:
# node labels, relationship types, and sample properties per node type
curl -s "https://api.robosystems.ai/v1/graphs/sec/schema" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"

# Declared schema — the definition the graph was created with.
# Add ?include_data_stats=true for live node/relationship counts.
curl -s "https://api.robosystems.ai/v1/graphs/sec/schema/export" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

On a small graph of your own, a label scan is a quick check too. Do not run it against `sec`: an unanchored `MATCH (n)` visits every node in the repository.

```cypher
-- Count nodes by label (small graphs only)
MATCH (n) WITH labels(n) AS label, count(n) AS c RETURN label, c ORDER BY c DESC
```

### Traversal Patterns

Start from one company. `Entity` is the anchor: lead with it (by `ticker` or `cik`) or with one `Report`, and reach `Element`, `Period` and `Structure` last.

```cypher
-- A company's most recent annual filings
MATCH (ent:Entity {ticker: $ticker})-[:ENTITY_HAS_REPORT]->(r:Report)
WHERE r.form IN ['10-K', '20-F', '40-F']
RETURN r.identifier, r.form, r.filing_date, r.fiscal_year_focus
ORDER BY r.filing_date DESC
LIMIT 5
```

Read a figure by its normalized concept, consolidated, with the period shape pinned:

```cypher
-- Consolidated annual net income for one company
MATCH (ent:Entity {ticker: $ticker})<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})
      -[:FACT_HAS_ELEMENT]->(e:Element),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'annual'})
WHERE e.canonical_concept = 'net_income' AND f.numeric_value IS NOT NULL
RETURN DISTINCT e.qname AS qname, p.start_date AS start_date, p.end_date AS end_date,
       f.numeric_value AS value
ORDER BY end_date DESC
LIMIT 5
```

Resolve a fact's full context — element, period, and unit — in a single `MATCH` of comma-separated patterns (separate `MATCH` clauses on the same node can time out):

```cypher
-- Balance-sheet facts at one date, with their units
MATCH (ent:Entity {ticker: $ticker})<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})
      -[:FACT_HAS_ELEMENT]->(e:Element),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {period_type: 'instant'}),
      (f)-[:FACT_HAS_UNIT]->(u:Unit)
WHERE p.end_date = $as_of
  AND e.canonical_concept IN ['total_assets', 'total_liabilities', 'stockholders_equity']
RETURN DISTINCT e.canonical_concept AS concept, e.qname AS qname,
       f.numeric_value AS value, u.measure AS unit
LIMIT 25
```

Pull a whole statement through the `Structure → FactSet → Fact` path. Anchor on one `Report` and reach `Structure` last — every filing has its own income-statement structure, so leading with `Structure {canonical_type: ...}` scans all of them:

```cypher
-- The income statement of one filing
MATCH (r:Report {identifier: $report_id})-[:REPORT_HAS_FACT]->(f:Fact {has_dimensions: false})
      -[:FACT_HAS_ELEMENT]->(e:Element),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'annual'}),
      (fs:FactSet)-[:FACT_SET_CONTAINS_FACT]->(f),
      (s:Structure {canonical_type: 'income_statement'})-[:STRUCTURE_HAS_FACT_SET]->(fs)
WHERE f.numeric_value IS NOT NULL
RETURN DISTINCT e.canonical_concept AS concept, e.qname AS qname,
       p.end_date AS end_date, f.numeric_value AS value
ORDER BY end_date DESC, qname
LIMIT 60
```

### Aggregate Across the Dataset

The analytical plane is built for aggregation across filers. On `sec`, keep the cohort explicit — a list of tickers, a form, a date window — so the query anchors on `Entity` and never scans every fact:

```cypher
-- Consolidated annual revenue and gross profit across a peer group
MATCH (ent:Entity)<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})
      -[:FACT_HAS_ELEMENT]->(e:Element),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'annual'})
WHERE ent.ticker IN $tickers
  AND e.canonical_concept IN ['revenue', 'gross_profit']
  AND p.end_date >= '2025-01-01'
  AND f.numeric_value IS NOT NULL
RETURN DISTINCT ent.ticker AS ticker, e.canonical_concept AS concept, e.qname AS qname,
       p.end_date AS end_date, f.numeric_value AS value
ORDER BY ticker, end_date DESC
LIMIT 50
```

Filing metadata aggregates cheaply without touching facts:

```cypher
-- Annual and quarterly filings by fiscal year
MATCH (r:Report)
WHERE r.form IN ['10-K', '10-Q']
RETURN r.fiscal_year_focus, r.form, count(r) AS filings
ORDER BY r.fiscal_year_focus DESC, r.form
LIMIT 20
```

More worked queries — a statement for a ticker's latest 10-K, a metric across eight quarters, a balance sheet at a date — are on [SEC Data Model & Query Rules](SEC-Data-Model-and-Query-Rules#worked-queries).

## Gotchas and Limits

- **SEC numbers have four rules.** Consolidated facts (`has_dimensions: false`), `Element.canonical_concept` over one qname, a pinned `Period` shape, and `RETURN DISTINCT` anchored on the `Entity`. A query that skips one returns a plausible wrong number rather than an error — see [SEC Data Model & Query Rules](SEC-Data-Model-and-Query-Rules).
- **Two parameter conventions.** Cypher takes named `$param` with a `parameters` object. Staging SQL takes either a `parameters` list for positional `?` (or `$1`, `$2`), or an object for named `$param` — never both in one statement.
- **Write attempts return 403 on main graphs.** Data enters by materialization — from the extensions database on an entity graph, from staged uploads on a custom graph. Only *user* subgraphs accept writes through the query path — and only for callers with the `member` or `admin` role. Shared repositories and their subgraphs never do.
- **Shared-repo staging SQL is blocked on the public API.** `POST /v1/graphs/{shared}/query/sql` returns 403 for shared repositories and their subgraphs — query the graph with Cypher instead.
- **Cypher query length is capped at 50,000 characters** and `timeout` is 1–300 seconds (default 60).
- **Queries are scoped to one graph.** There are no cross-database or cross-graph queries; each request targets a single `graph_id`.
- **Ingestion is sequential and single-writer.** Each database allows one writer at a time and a limited number of concurrent connections, so favor read-only traversals when querying live graphs.
- **`CALL db.*` introspection is refused on main graphs and shared repositories.** Only the [read-only procedures](#read-only-on-main-graphs) pass; use `CALL show_tables() RETURN *`, `CALL table_info('Label') RETURN *`, or `get-graph-schema`.
- **On `sec`, text is not a Cypher question.** String matching (`CONTAINS`, `STARTS WITH`, `ENDS WITH`, `=~`) on `Fact.value` or `Fact.uri` is refused with 400 — see [SEC Data Model & Query Rules](SEC-Data-Model-and-Query-Rules#other-limits) for the tools that search filing text instead.

## Self-hosted deployments

On a stack you run, the `just` recipes are development shortcuts. Two of them go through the lower-level graph engine and two open the embedded database files directly, bypassing the API entirely:

| Recipe | What it queries | Path |
| --- | --- | --- |
| **`just graph-query GRAPH_ID "CYPHER"`** | Cypher via the graph engine | through the engine |
| **`just lbug-query GRAPH_ID "CYPHER"`** | Direct LadybugDB embedded query | `./data/lbug-dbs/{graph_id}.lbug` |
| **`just tables-query GRAPH_ID "SQL"`** | SQL over DuckDB staging via the engine | through the engine |
| **`just duckdb-query GRAPH_ID "SQL"`** | Direct DuckDB staging query | `./data/staging/{graph_id}.duckdb` |
| **`just graph-health`** | Graph engine health | through the engine |
| **`just graph-info GRAPH_ID`** | Database info | through the engine |

None of them exercise the public API's authentication, rate limiting, or shared-repo guards, so the public API remains the contract for application code. Two consequences:

- `just tables-query sec "..."` works locally because the engine has no shared-repo guard, while the public `/v1/graphs/sec/query/sql` path returns 403.
- `just graph-query` rewrites single quotes to double quotes in Cypher, and takes a trailing `json` argument for JSON output instead of the default table format.

## Related Documentation

**Wiki Guides:**
- **[GraphQL Reads](GraphQL-Reads)** - The operational/OLTP query plane (typed GraphQL over PostgreSQL)
- **[AI Operators and MCP](AI-Operators-and-MCP)** - The full MCP tool surface for AI agents
- **[Graph Operations](Graph-Operations)** - Graph lifecycle, subgraphs, and the staging/materialize write path
- **[SEC Data Model & Query Rules](SEC-Data-Model-and-Query-Rules)** - The `sec` graph's model, the four query rules, and worked queries
- **[SEC XBRL Pipeline](SEC-XBRL-Pipeline)** - Loading the SEC dataset on a local stack, and its staging tables
- **[Custom Graph Schema](Custom-Graph-Schema)** - Define your own node and relationship types

**Codebase Documentation:**
- **[Graph API](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graph_api/README.md)** - The graph engine beneath the public query API
- **[Middleware / Graph](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/graph/README.md)** - Routing, multi-tenancy, and execution strategies

**API Reference:**
- **[API reference](https://robosystems.ai/docs/api)** - API reference with machine-readable OpenAPI spec
- **Cypher Manual**: [Cypher Manual](https://neo4j.com/docs/cypher-manual/current/introduction/)
- **MCP Protocol**: [Model Context Protocol](https://modelcontextprotocol.io/)

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
