# AI Operators & MCP

RoboSystems exposes its data and reasoning surface to AI through the [Model Context Protocol](https://modelcontextprotocol.io/). An **AI Operator** is a Claude-driven executor that answers a natural-language question by composing reads across three retrieval planes — operational, analytical, and unstructured — using the same MCP tool layer that any external agent (Claude Desktop, a custom orchestrator) can call directly.

---

## Two Kinds of "Agent"

RoboSystems uses the word "agent" for two unrelated concepts. Keep them separate.

| Term | What it is | Where it lives |
| --- | --- | --- |
| **Operator** | The AI executor layer — Claude reasoning over MCP tools (`CypherOperator`, `MappingOperator`, etc.) | `operations/operators/`, `routers/graphs/operator/`, endpoint `/v1/graphs/{g}/operator` |
| **REA Agent** | A ledger counterparty — a customer, vendor, or employee party in the accounting ontology | `models/extensions/roboledger/agent.py` |

"REA Agent" is the canonical ontology term (Resources-Events-Agents accounting). The AI side is always called an **Operator** in the codebase to avoid the "agent operates on agent" collision. Marketing copy may still say "AI agent" — that is decoupled from the internal naming used in this page.

---

## The MCP Tool Surface

MCP is the shared tool layer. Every agent path — the in-product Operator endpoint, Claude Desktop, and in-process operators — calls the *same* tool classes in `middleware/mcp/tools/`. There is one implementation per tool; only the transport differs.

Two HTTP entry points expose the surface on every graph:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/v1/graphs/{graph_id}/mcp/tools` | List the MCP tools available on this graph |
| `POST` | `/v1/graphs/{graph_id}/mcp/call-tool` | Execute one MCP tool |

A `call-tool` request body is a `MCPToolCall`: a tool `name` plus an `arguments` object.

```json
{
  "name": "read-graph-cypher",
  "arguments": { "query": "MATCH (n) RETURN count(n)", "parameters": {} }
}
```

**Database reads are free.** Cypher, GraphQL, schema, and search tool calls consume no credits — only AI (LLM) token usage does. You can drive the entire MCP surface programmatically without billing impact; credits enter the picture only when an Operator calls Claude.

**Tool availability is per-deployment and per-graph.** The catalog is gated by feature flags and by each graph's `schema_extensions`. A ledger-only deployment exposes no investor fields; a read-only shared-repository graph hides write and live-OLTP tools. Always call `/mcp/tools` (or a schema-discovery tool) first rather than assuming a tool exists.

### Tool Catalog

The following tool names are the exact strings accepted by `call-tool`. They are grouped by the plane or capability they serve.

| Tool | Plane / role | Gated by |
| --- | --- | --- |
| `read-graph-cypher` | Analytical (read-only Cypher) | Always available |
| `get-graph-schema` | Analytical schema (Cypher/graph) | Always available |
| `query-graphql` | Operational (typed OLTP read) | `EXTENSIONS_GRAPHQL_ENABLED` + `MCP_GRAPHQL_ENABLED` |
| `get-graphql-schema` | Operational schema (GraphQL SDL) | `EXTENSIONS_GRAPHQL_ENABLED` + `MCP_GRAPHQL_ENABLED` |
| `search-documents` | Unstructured (hybrid BM25 + KNN) | `SEMANTIC_SEARCH_ENABLED` |
| `get-document-section` | Unstructured (full section text) | `SEMANTIC_SEARCH_ENABLED` |
| `financial-statement-analysis` | Curated analytical (LadybugDB) | `roboledger` schema extension |
| `live-financial-statement` | Curated operational (OLTP) | `roboledger` schema extension |
| `build-fact-grid` | Curated analytical (XBRL hypercube) | `FACT_GRID_ENABLED` |
| `resolve-element` | Cross-plane bridge (qname → graph) | `roboledger` schema extension |
| `get-example-queries` | Discovery helper | `roboledger` schema extension |
| `write-graph-cypher` | Memory write (subgraphs only) | `MCP_MEMORY_ENABLED` |
| `add-node-table` / `add-relationship-table` | Memory schema (subgraphs only) | `MCP_MEMORY_ENABLED` |

In addition, **write tools are auto-generated** from the operation specs of each enabled extension (mapping, schedule, and event writes, for example). These are derived from the same `OperationSpec`s that back the REST command surface, so the MCP write tools and the `/extensions/*/operations/*` endpoints stay in lockstep. The exact set depends on which extensions are enabled; enumerate it for a given graph by calling `/mcp/tools`.

For the full request/response schema of every tool, see the live OpenAPI spec at [api.robosystems.ai/docs](https://api.robosystems.ai/docs) (or `http://localhost:8000/docs` locally).

---

## The Three Retrieval Planes

An Operator answers a question by reading across three planes. Each plane has a **discover-schema-first** tool and an **execute** tool. The pattern mirrors the platform's storage split: live transactional state (OLTP), the materialized analytical graph (OLAP), and the unstructured document index.

```text
                          ┌─────────────────────────────────┐
                          │           AI Operator           │
                          │   (Claude reasoning over MCP)   │
                          └────────────────┬────────────────┘
                                           │
        ┌──────────────────────────────────┼──────────────────────────────────┐
        ▼                                  ▼                                  ▼
┌───────────────┐                 ┌───────────────┐                 ┌───────────────┐
│  OPERATIONAL  │                 │  ANALYTICAL   │                 │ UNSTRUCTURED  │
│  query-graphql│                 │read-graph-    │                 │search-        │
│  get-graphql- │                 │  cypher       │                 │  documents    │
│  schema       │                 │get-graph-     │                 │get-document-  │
│               │                 │  schema       │                 │  section      │
├───────────────┤                 ├───────────────┤                 ├───────────────┤
│ Extensions    │                 │ LadybugDB     │                 │ Document /    │
│ OLTP (live    │                 │ OLAP (XBRL    │                 │ SEC-filing    │
│ ledger state) │                 │ hypercube)    │                 │ index         │
└───────────────┘                 └───────────────┘                 └───────────────┘
```

### Plane A — Operational (Typed OLTP Reads)

**Tools:** `query-graphql`, `get-graphql-schema`
**Backs onto:** the extensions GraphQL surface (Strawberry) over the live OLTP ledger state.

This plane reads the current books — fiscal calendar, entities, accounts, the latest transactional state — through the same typed GraphQL surface the frontends use. Run `get-graphql-schema` first to retrieve the SDL, then `query-graphql` to execute.

The `graph_id` is supplied by the URL/context, never as a query argument. A query that passes `graphId` is wrong:

```text
Wrong:  { entity(graphId: "kg_x") { name } }
Right:  { entity { name } }
```

`query-graphql` is read-only: mutations and subscriptions are rejected before execution, and a depth/field/alias complexity gate limits query cost. Writes go through the registrar-generated command tools or the REST `/extensions/*/operations/*` surface.

### Plane B — Analytical (Read-Only Cypher)

**Tools:** `read-graph-cypher`, `get-graph-schema`
**Backs onto:** the OLAP LadybugDB graph — the materialized XBRL hypercube containing SEC data and post-materialization tenant facts.

This plane runs graph traversals over historical and cross-period data: facts, elements, periods, dimensions, and the relationships between them. Run `get-graph-schema` first to learn the node and relationship types, then `read-graph-cypher` to query.

`read-graph-cypher` is strictly read-only. `CREATE`, `SET`, `DELETE`, `REMOVE`, `MERGE`, `DROP` (including `DETACH DELETE`), and procedure calls (`CALL db.`, `CALL apoc.`) are blocked — a write attempt raises "Only read-only queries are allowed". Bulk ingestion (loading data into the graph) is not part of the read tool surface at all; use the file-upload ingestion path instead.

Two curated tools sit on top of this plane for ledger work:
- `financial-statement-analysis` — graph-backed statement analysis over LadybugDB (SEC + materialized tenants); historical and cross-period.
- `build-fact-grid` — pivot tables over the XBRL hypercube.

Note the distinct curated companion `live-financial-statement`, which reads the **OLTP** books of a tenant graph (current period, before materialization). Do not conflate the two: `financial-statement-analysis` is analytical/historical, `live-financial-statement` is operational/current.

### Plane C — Unstructured (Hybrid Document Search)

**Tools:** `search-documents`, `get-document-section`
**Backs onto:** the document / SEC-filing index.

This plane retrieves narrative context — disclosures, footnotes, policy documents. `search-documents` runs a BM25 keyword search by default; hybrid semantic ranking (BM25 + KNN vector similarity) is opt-in via `semantic: true`. Each hit returns a `document_id`; pass it to `get-document-section` for the full section text.

For shared repositories, search resolves to the parent `graph_id` (`sec`, not a subgraph) — subgraphs are not a search boundary.

### The Cross-Plane Bridge

The planes are most powerful in combination. iXBRL disclosures returned by `search-documents` carry `xbrl_elements` (for example `us-gaap:Goodwill`). The `resolve-element` tool looks those qnames up in the graph, and `read-graph-cypher` then pulls the structured fact. This bridges narrative context to structured numbers:

```text
search-documents  →  resolve-element  →  read-graph-cypher
(find the disclosure) (qname → graph) (fetch the fact value)
```

An Operator typically discovers a concept in prose, resolves it to a graph element, and reads the precise figure — composing all three planes to answer one question.

---

## The Operator Endpoint

The Operator endpoint runs a Claude-driven executor against a graph. It is mounted at `/v1/graphs/{graph_id}/operator`.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/v1/graphs/{graph_id}/operator` | List operators available on this graph |
| `POST` | `/v1/graphs/{graph_id}/operator` | Auto-select an operator and execute |
| `GET` | `/v1/graphs/{graph_id}/operator/{operator_type}` | Metadata for a specific operator |
| `POST` | `/v1/graphs/{graph_id}/operator/{operator_type}` | Execute a specific operator |
| `POST` | `/v1/graphs/{graph_id}/operator/batch` | Batch (up to 10 queries) |
| `POST` | `/v1/graphs/{graph_id}/operator/recommend` | Rank operators by confidence for a query |

**Auto-select vs specific.** `POST /operator` lets the platform pick the best operator for the message (use `/operator/recommend` to preview that ranking). `POST /operator/{operator_type}` targets one operator directly.

**Modes** (`mode` field on the request): `quick`, `standard`, `extended`, `streaming`. Each maps to an execution profile with a tool-call budget and timeout:

| Mode | Tool-call budget | Timeout |
| --- | --- | --- |
| `quick` | ≤ 2 | 30 s |
| `standard` | ≤ 5 | 60 s |
| `extended` | ≤ 12 | 300 s |
| `streaming` | ≤ 8 | 120 s |

**Strategies.** A `?mode=sync|async|stream|auto` query parameter overrides the transport strategy on an operator `POST`. With `auto`, the endpoint chooses sync, Server-Sent Events, or async based on the tool type, client capabilities, and current load. On `call-tool`, a `?format=json|sse|ndjson` parameter plays the same role.

The Operator `POST` path can be disabled by deployment via `OPERATOR_POST_ENABLED=false` (returns 403). The GraphQL MCP tools have a separate kill switch, `MCP_GRAPHQL_ENABLED`.

An `OperatorRequest` carries `message` (required), `mode`, optional `history` (a list of `{role, content}` turns), `context`, `enable_rag`, `force_extended_analysis`, `operator_type`, and `selection_criteria`. The `OperatorResponse` returns `content`, `operator_used`, `mode_used`, `metadata`, `tokens_used`, `confidence_score`, `execution_time`, and `error_details`. See [api.robosystems.ai/docs](https://api.robosystems.ai/docs) for the full field reference.

---

## Operators as Tools

Operators are not just behind the endpoint — they are themselves MCP-tool consumers, and they can be exposed as tools to any orchestrator.

Each operator runs via an `OperatorContext` injected into its `run()` method. The context carries `graph_id`, `user_id`, `query`, `mode`, and `history`, plus three services: `ai` (a tracked Claude client), `progress` (a reporter for streaming), and `tools` (a `ToolAccess` handle). Every operator reaches every MCP tool through a single method:

```python
await context.tools.call_tool(tool_name, arguments, return_raw)
```

Because operators call the same `middleware/mcp/tools/` classes as the external entry points, the tool that Claude Desktop invokes over MCP is the identical implementation an in-process operator invokes. There is one tool surface, used three ways.

Two operators ship today:

- **CypherOperator** — translates natural-language questions into Cypher over the analytical graph, calling `get-graph-schema` and `read-graph-cypher`. It answers questions like "What are my five largest expense accounts this year?".
- **MappingOperator** — assists with chart-of-accounts mapping; scoped to graphs carrying the `roboledger` schema extension (it will not run on a graph without it).

An operator declares its capabilities through an `OperatorSpec` (`name`, `description`, `capabilities`, `supported_modes`, `requires_credits`, `execution_profile`, and a `graph_scope`). The `graph_scope` restricts which graphs the operator runs on — by `shared_repo` or by `schema_extension` — which is how MappingOperator is confined to ledger graphs.

For the operator framework patterns, see the [Operations README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md) in codebase.

---

## Worked Examples

All examples target the local stack at `http://localhost:8000` and read the API key from `.local/config.json` (written by `just demo-user`). Use the `X-API-Key` header. Set `GRAPH_ID` to your graph (for example a roboledger graph `kg1a2b3c4d5e`).

### List the Tools Available on a Graph

This shows exactly which planes a given deployment and graph expose. Always start here.

```bash
GRAPH_ID=<your graph id>

curl -s "http://localhost:8000/v1/graphs/$GRAPH_ID/mcp/tools" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

### Analytical Plane — Cypher over LadybugDB

No credits are consumed. Run `get-graph-schema` first in real use to learn the node and relationship types.

```bash
curl -s -X POST "http://localhost:8000/v1/graphs/$GRAPH_ID/mcp/call-tool" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "read-graph-cypher",
    "arguments": {
      "query": "MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element) RETURN el.qname, f.value LIMIT 10",
      "parameters": {}
    }
  }'
```

### Operational Plane — Typed GraphQL over OLTP

The `graph_id` comes from the URL, never the query body.

```bash
curl -s -X POST "http://localhost:8000/v1/graphs/$GRAPH_ID/mcp/call-tool" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "query-graphql",
    "arguments": { "query": "{ fiscalCalendar { closedThrough closeTarget } }" }
  }'
```

### Unstructured Plane — Hybrid Document Search

`semantic: true` opts into KNN vector ranking on top of keyword search.

```bash
curl -s -X POST "http://localhost:8000/v1/graphs/$GRAPH_ID/mcp/call-tool" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "search-documents",
    "arguments": { "query": "month end close procedures", "semantic": true, "size": 5 }
  }'
```

### Ask an Operator a Natural-Language Question

Auto-select picks the right operator; `?mode=sync` returns a single JSON response rather than streaming.

```bash
curl -s -X POST "http://localhost:8000/v1/graphs/$GRAPH_ID/operator?mode=sync" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{ "message": "What are my five largest expense accounts this year?", "mode": "standard" }'
```

For direct, API-bypassing graph access during development, `just lbug-query GRAPH_ID "CYPHER"` queries LadybugDB directly and `just graph-query GRAPH_ID "CYPHER"` goes through the API.

---

## Configuration and Gotchas

| Concern | Detail |
| --- | --- |
| `read-graph-cypher` is read-only | Write Cypher (`CREATE`/`SET`/`DELETE`/`REMOVE`/`MERGE`/`DROP`) raises "Only read-only queries are allowed" |
| Write Cypher is a different tool | `write-graph-cypher` works on **subgraphs only** (memory), never the parent graph or shared repos; gated by `MCP_MEMORY_ENABLED` |
| `query-graphql` is read-only | Mutations and subscriptions are rejected; depth/field/alias complexity is gated |
| Discover schema first | `get-graph-schema` returns the **Cypher** schema; `get-graphql-schema` returns the **GraphQL SDL** — distinct tools, easy to confuse |
| Tool catalog varies | Gated by feature flags and per-graph `schema_extensions`; call `/mcp/tools` before assuming a tool exists |
| `live-` vs `analysis` | `live-financial-statement` = OLTP, tenant graphs, current books; `financial-statement-analysis` = LadybugDB, historical/cross-period |
| Operator POST kill switch | `OPERATOR_POST_ENABLED=false` returns 403 |
| GraphQL MCP kill switch | `MCP_GRAPHQL_ENABLED=false` removes `query-graphql` / `get-graphql-schema` |
| Bulk ingestion is not a read tool | Loading data into the graph is not part of the `read-graph-cypher` surface; use the file-upload ingestion path instead |
| Search scope | Shared-repo search resolves to the parent `graph_id`; subgraphs are not a search boundary |
| Credits | Only AI (LLM) calls consume credits; every database/search tool call is free |

Relevant feature flags (`ROBOLEDGER_ENABLED`, `ROBOINVESTOR_ENABLED`, `EXTENSIONS_GRAPHQL_ENABLED`, `SEMANTIC_SEARCH_ENABLED`, `MCP_GRAPHQL_ENABLED`, `MCP_MEMORY_ENABLED`, `FACT_GRID_ENABLED`) determine the catalog at runtime. See the [Configuration README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/config/README.md) in codebase.

---

## Related Documentation

**Wiki Guides:**
- **[Architecture Overview](Architecture-Overview)** - The OLTP/OLAP split that the three planes sit on top of
- **[Querying the Analytical Graph](Querying-the-Analytical-Graph)** - Cypher over LadybugDB in depth
- **[GraphQL Reads](GraphQL-Reads)** - The typed operational read surface in depth
- **[Search and AI Retrieval](Search-and-AI-Retrieval)** - The document index and hybrid search in depth

**Codebase Documentation:**
- **[GraphQL Extensions](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md)** - Strawberry GraphQL surface, resolver patterns
- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Operator framework and business-logic kernel
- **[Authentication](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/auth/README.md)** - API key and access control
- **[API Documentation](https://api.robosystems.ai/docs)** - Full endpoint and tool reference with machine-readable OpenAPI spec

---

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
