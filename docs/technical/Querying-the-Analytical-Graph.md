# Querying the Analytical Graph

This guide shows you how to query the analytical graph — the OLAP plane of RoboSystems, backed by the LadybugDB columnar graph database — using ad-hoc Cypher, SQL over the DuckDB staging tables, and the MCP tools that wrap both for AI agents.

**Quick Start:** Run `just graph-query sec "MATCH (e:Entity) RETURN e.name LIMIT 10"` to run your first Cypher query against the SEC analytical graph.

## Overview

RoboSystems exposes two distinct query planes, and it is worth being explicit about which one you are using:

- **Analytical / OLAP** — Cypher (and supporting SQL) over the LadybugDB graph. This is where you traverse relationships and aggregate across an entire dataset. **This page covers the analytical plane.**
- **Operational / OLTP** — typed GraphQL reads over the extensions PostgreSQL database (live transactional state). That plane is covered in [GraphQL Reads](GraphQL-Reads).

The analytical graph is a materialized projection. Data flows `S3 → DuckDB staging → LadybugDB`, and a sensor-driven blue/green rebuild keeps the graph in sync with its source. Because it is a projection, the analytical graph can lag the operational source by the length of a rebuild cycle.

There are three windows onto the same analytical data:

1. **The graph itself** — Cypher via `POST /v1/graphs/{graph_id}/query/cypher`.
2. **The DuckDB columnar tables** — SQL via `POST /v1/graphs/{graph_id}/query/sql`, a relational lens on the same graph-centric data, often ahead of the materialized graph.
3. **MCP tools** — `read-graph-cypher` and `get-graph-schema`, the agent-facing wrappers over the first window.

```text
S3 (Parquet)
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

- Docker running locally with services started via `just start`
- A graph to query — the examples use the shared `sec` repository; run `just demo-sec` to load it
- An API key in `.local/config.json` (created by `just demo-user` or `just demo-sec`) for any `curl` against the public API

## Quick Start

The fastest path for local development is the `just graph-query` recipe, which runs Cypher against the analytical graph without requiring an API key:

```bash
# List companies in the SEC analytical graph
just graph-query sec "MATCH (e:Entity) RETURN e.name, e.identifier LIMIT 10"

# Count nodes by label
just lbug-query sec "MATCH (n) WITH labels(n) AS label, count(n) AS c RETURN label, c ORDER BY c DESC"
```

For application and integration code, use the public HTTP API at `http://localhost:8000`, authenticated with `X-API-Key`. The rest of this guide walks through each surface.

## The Public Query Endpoint

The user-facing way to run Cypher is `POST /v1/graphs/{graph_id}/query/cypher`. The public API at port 8000 adds authentication, rate limiting, billing and lifecycle enforcement, circuit breakers, and queueing on top of the lower-level graph engine.

### Running Ad-Hoc Cypher

Read the API key from `.local/config.json` and post a Cypher query with named parameters:

```bash
curl -X POST "http://localhost:8000/v1/graphs/sec/query/cypher" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (e:Entity)-[:ENTITY_HAS_REPORT]->(r:Report)-[:REPORT_HAS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element) WHERE e.name CONTAINS $name AND el.name = $element RETURN e.name, r.form, f.numeric_value LIMIT $limit",
    "parameters": {"name": "NVIDIA", "element": "Revenues", "limit": 10},
    "timeout": 60
  }'
```

The response is a `CypherStatementResponse`, where each row is a dictionary keyed by the returned columns:

```json
{
  "success": true,
  "data": [{"e.name": "NVIDIA CORP", "r.form": "10-K", "f.numeric_value": 26974000000}],
  "columns": ["e.name", "r.form", "f.numeric_value"],
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

The kernel is read-only for main graphs. Attempting a `CREATE`, `MERGE`, `SET`, `DELETE`, or other mutation against a main graph returns **HTTP 403**, with a message steering you to the staging pipeline — data enters a main graph only through `S3 → DuckDB → materialize`. See [Graph Operations](Graph-Operations) for the write/materialize path.

Subgraphs are the exception and do allow writes, subject to two carve-outs:

- **Shared repositories and their subgraphs never accept writes.** The exception is for *user* subgraphs only; a write against `sec` — or against any `sec` subgraph — is refused unconditionally.
- **Writing to a user subgraph requires the `member` or `admin` role.** A viewer gets 403: read-only access stays read-only on subgraphs too.

Schema DDL and bulk operations are blocked everywhere: `COPY`/`LOAD`/`IMPORT` return 400; `EXPORT`/`INSTALL`/`ATTACH` and `CREATE`/`DROP`/`ALTER TABLE` return 403. The graph schema is immutable after graph creation.

### Response Modes and Queueing

The endpoint accepts a `mode` query parameter (`auto`, `sync`, `async`, `stream`); `auto` is the default and picks a strategy based on query weight and current load. A heavy query may return **HTTP 202** with an `operation_id` instead of inline results. Monitor it over SSE:

```bash
curl -N "http://localhost:8000/v1/operations/{operation_id}/stream" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

Streaming result delivery is available by setting `Accept: application/x-ndjson` or `Accept: text/event-stream`, and `chunk_size` (10–10,000) tunes the streamed batch size.

For the full request/response schema and every query parameter, see the live OpenAPI spec at [https://api.robosystems.ai/docs](https://api.robosystems.ai/docs) (operation `executeCypher`; its SQL peer below is `executeSql`), or `http://localhost:8000/docs` when running locally.

## Querying the Staging Tables

The same graph-centric data is also addressable relationally, through the graph's DuckDB columnar tables. Rows land there on their way to the graph, so the tables are often ahead of the materialized graph — which makes SQL over them the way to validate data quality, debug ingestion, or use familiar SQL joins and aggregations on the same dataset.

### SQL Over Staging

`POST /v1/graphs/{graph_id}/query/sql` runs read-only SQL over the columnar tables. Parameters here are a **positional array** with `?` placeholders — a different convention from the Cypher endpoint's named `$param` — and `$1`/`$2`-style numbered placeholders bind against the same array:

```bash
curl -X POST "http://localhost:8000/v1/graphs/kg1a2b3c4d5/query/sql" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
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

The public `POST /v1/graphs/{graph_id}/query/sql` endpoint returns **HTTP 403** for shared repositories and their subgraphs (for example, `sec`), with a message directing you to `POST /query/cypher` instead — a shared repository has no user columnar tables to query, so SQL is refused there entirely while Cypher continues to serve its reads from the graph. This is a real trap: `just tables-query sec "..."` works because that recipe talks straight to the lower-level graph engine (which has no shared-repo guard), but the **public** `/v1/graphs/sec/query/sql` path is blocked. For shared repos, query the graph with Cypher rather than the staging tables.

## Command-Line Recipes

The `just` recipes are development shortcuts. Two of them go through the lower-level graph engine and two open the embedded database files directly, bypassing the API entirely:

| Recipe | What it queries | Path |
| --- | --- | --- |
| **`just graph-query GRAPH_ID "CYPHER"`** | Cypher via the graph engine | through the engine |
| **`just lbug-query GRAPH_ID "CYPHER"`** | Direct LadybugDB embedded query | `./data/lbug-dbs/{graph_id}.lbug` |
| **`just tables-query GRAPH_ID "SQL"`** | SQL over DuckDB staging via the engine | through the engine |
| **`just duckdb-query GRAPH_ID "SQL"`** | Direct DuckDB staging query | `./data/staging/{graph_id}.duckdb` |
| **`just graph-health`** | Graph engine health | through the engine |
| **`just graph-info GRAPH_ID`** | Database info | through the engine |

```bash
# Cypher against the SEC analytical graph
just graph-query sec "MATCH (e:Entity) RETURN e.name, e.identifier LIMIT 10"

# Direct LadybugDB query (bypasses the API entirely)
just lbug-query sec "MATCH (n) WITH labels(n) AS label, count(n) AS c RETURN label, c ORDER BY c DESC"

# Inspect staging directly
just duckdb-query sec "SELECT name, cik FROM Entity LIMIT 10"

# JSON output instead of the default table format
just graph-query sec "MATCH (e:Entity) RETURN e.name LIMIT 5" json
```

**Note:** `just graph-query` and `just tables-query` target the lower-level graph engine, **not** the public `/v1` path — so they do not exercise the same authentication, rate limiting, or shared-repo guards as the `curl` examples above. They are the fastest way to inspect data locally, but the public API is the contract for application code.

**Note:** `just graph-query` rewrites single quotes to double quotes in Cypher. Keep that in mind when a literal genuinely needs single quotes.

## From an AI Agent — MCP Tools

The same analytical surface is exposed to AI agents through Model Context Protocol tools. Two of them cover the analytical plane:

- **`read-graph-cypher`** — runs read-only Cypher. The same write keywords blocked at the query endpoint (`CREATE`, `SET`, `DELETE`, `REMOVE`, `MERGE`, `DROP`, `DETACH DELETE`, plus `CALL db.` and `CALL apoc.`) are rejected here. Input is `{ query: string, parameters?: object }`.
- **`get-graph-schema`** — returns the full graph schema (node types with their properties and data types, plus relationships). No input arguments; the result is cached briefly.

These map directly onto the surfaces above: `read-graph-cypher` is the query endpoint, and `get-graph-schema` is schema discovery. Database reads through MCP — Cypher, schema, and staging — **consume no credits**; only AI/LLM calls cost credits.

You can list and call tools over the public API:

```bash
# List available MCP tools for a graph
curl "http://localhost:8000/v1/graphs/sec/mcp/tools" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"

# Discover the schema first
curl -X POST "http://localhost:8000/v1/graphs/sec/mcp/call-tool" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"name": "get-graph-schema", "arguments": {}}'

# Then run a read-only Cypher query through MCP
curl -X POST "http://localhost:8000/v1/graphs/sec/mcp/call-tool" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"name": "read-graph-cypher", "arguments": {"query": "MATCH (e:Entity) RETURN e.name LIMIT 5"}}'
```

Alongside these two, `get-graph-info` is available on every graph, and on graphs carrying the `roboledger` extension (including `sec`) the plane also exposes `get-example-queries` and `resolve-element`. There is a separate operational MCP plane (`query-graphql`, `get-graphql-schema`) that reads the OLTP database, and an unstructured plane (`search-documents`, `get-document-section`) over the document index. For the full agent surface, see [AI Operators and MCP](AI-Operators-and-MCP).

**Note:** `read-graph-cypher` strips string literals before scanning for write keywords, but the scan matches on whole words. A variable or property named exactly like a blocked keyword — for example a property access `n.set` or a variable `delete` — trips the word-boundary match and is rejected. Names that merely contain a keyword as a substring (such as `created_at` or `deleted`) are fine; avoid naming a read-only variable exactly `set`, `create`, `delete`, `merge`, `remove`, or `drop`.

## Read Patterns and Example Cypher

The examples below use the `sec` repository as an illustrative dataset. Its data model has node types including `Entity`, `Report`, `Fact`, `Element`, `Period`, `Unit`, `Dimension`, and `Structure`; see [SEC XBRL Pipeline](SEC-XBRL-Pipeline) for the complete model. Other graphs have their own schemas — always discover the schema before querying.

### Discover the Schema First

Property and label names are case-sensitive, so start by discovering what exists rather than guessing:

```cypher
-- List the labels present in the graph
MATCH (n) RETURN DISTINCT labels(n)

-- Count nodes by label
MATCH (n) WITH labels(n) AS label, count(n) AS c RETURN label, c ORDER BY c DESC
```

From an agent, `get-graph-schema` returns the same information in one call (node types, properties with their data types — `STRING`, `INT64`, `DOUBLE`, `BOOLEAN`, `TIMESTAMP`, `JSON` — and relationships).

Over HTTP the same discovery is two plain `GET`s, and they answer different questions:

```bash
# Runtime schema — what is actually in the database right now:
# node labels, relationship types, and sample properties per node type
curl -s "http://localhost:8000/v1/graphs/sec/schema" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"

# Declared schema — the definition the graph was created with.
# Add ?include_data_stats=true for live node/relationship counts.
curl -s "http://localhost:8000/v1/graphs/sec/schema/export" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

### Traversal Patterns

Walk from an entity down to individual facts:

```cypher
-- Facts for a specific company by SEC CIK
MATCH (e:Entity)-[:ENTITY_HAS_REPORT]->(r:Report)-[:REPORT_HAS_FACT]->(f:Fact)
WHERE e.cik = $cik
RETURN r.form, f.numeric_value
LIMIT 25
```

Filter facts by the XBRL element they report:

```cypher
-- Revenue facts across all loaded companies
MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element)
WHERE el.name = $element
RETURN el.name, f.numeric_value, f.value
LIMIT 25
```

Resolve a fact's full context — element, period, and unit:

```cypher
-- A fact with all of its aspects
MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element)
MATCH (f)-[:FACT_HAS_PERIOD]->(p:Period)
MATCH (f)-[:FACT_HAS_UNIT]->(u:Unit)
WHERE el.name = $element
RETURN el.name, f.numeric_value, p.period_type, p.start_date, p.end_date, u
LIMIT 25
```

Traverse the taxonomy structure to see how elements roll up:

```cypher
-- Parent-child element relationships within a structure
MATCH (s:Structure)-[:STRUCTURE_HAS_ASSOCIATION]->(a:Association)
MATCH (a)-[:ASSOCIATION_HAS_FROM_ELEMENT]->(parent:Element)
MATCH (a)-[:ASSOCIATION_HAS_TO_ELEMENT]->(child:Element)
RETURN parent.name, child.name
LIMIT 50
```

### Aggregate Across the Dataset

The analytical plane shines on aggregation that spans the whole graph:

```cypher
-- The most frequently reported elements
MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element)
RETURN el.name, count(f) AS fact_count
ORDER BY fact_count DESC
LIMIT 20
```

## Gotchas and Limits

- **Two parameter conventions.** Cypher uses named `$param` with a `parameters` object; staging SQL uses positional `?` (or `$1`, `$2`) with a `parameters` array. They are not interchangeable.
- **Write attempts return 403 on main graphs.** Data enters via the staging pipeline. Only *user* subgraphs accept writes through the query path — and only for callers with the `member` or `admin` role. Shared repositories and their subgraphs never do.
- **Shared-repo staging SQL is blocked on the public API.** `POST /v1/graphs/{shared}/query/sql` returns 403 for shared repositories and their subgraphs — query the graph with Cypher instead.
- **Cypher query length is capped at 50,000 characters** and `timeout` is 1–300 seconds (default 60).
- **Queries are scoped to one graph.** There are no cross-database or cross-graph queries; each request targets a single `graph_id`.
- **Ingestion is sequential and single-writer.** Each database allows one writer at a time and a limited number of concurrent connections, so favor read-only traversals when querying live graphs.
- **`CALL db.*` introspection is auto-translated** to the LadybugDB equivalent, but prefer `get-graph-schema` for reliable schema discovery.
- **`just` shortcuts bypass the API.** `lbug-query` and `duckdb-query` open the embedded files directly with no authentication; `graph-query` and `tables-query` hit the engine, not the public `/v1` guards.

## Related Documentation

**Wiki Guides:**
- **[GraphQL Reads](GraphQL-Reads)** - The operational/OLTP query plane (typed GraphQL over PostgreSQL)
- **[AI Operators and MCP](AI-Operators-and-MCP)** - The full MCP tool surface for AI agents
- **[Graph Operations](Graph-Operations)** - Graph lifecycle, subgraphs, and the staging/materialize write path
- **[SEC XBRL Pipeline](SEC-XBRL-Pipeline)** - The example dataset, its data model, and staging-table reference
- **[Custom Graph Schema](Custom-Graph-Schema)** - Define your own node and relationship types

**Codebase Documentation:**
- **[Graph API](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graph_api/README.md)** - The graph engine beneath the public query API
- **[Middleware / Graph](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/graph/README.md)** - Routing, multi-tenancy, and execution strategies

**API Reference:**
- **[API Documentation](https://api.robosystems.ai/docs)** - API reference with machine-readable OpenAPI spec
- **Cypher Manual**: [Cypher Manual](https://neo4j.com/docs/cypher-manual/current/introduction/)
- **MCP Protocol**: [Model Context Protocol](https://modelcontextprotocol.io/)

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
