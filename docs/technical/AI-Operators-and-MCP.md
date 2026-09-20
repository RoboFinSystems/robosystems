# AI Operators & MCP

RoboSystems exposes its data and reasoning surface to AI through the [Model Context Protocol](https://modelcontextprotocol.io/). An **AI Operator** is a model-driven executor that answers a natural-language question by composing reads across three retrieval planes — operational, analytical, and unstructured — using the same MCP tool layer that any MCP client (Claude, ChatGPT, Claude Code, Cursor, a custom orchestrator) can call directly.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development).

---

## Connecting from MCP Clients

Every graph is an MCP server, served over MCP's Streamable HTTP transport directly by the API. There are two ways in:

> **OAuth (recommended):** `https://api.robosystems.ai/v1/mcp` — one URL for everyone. The client discovers the authorization server, you sign in to RoboSystems, and the consent screen asks **which graph** to connect: your own graphs, their subgraphs, and the shared repositories you subscribe to. One authorization is one graph; to work on another graph, reconnect and pick it.
>
> **API key:** `https://api.robosystems.ai/v1/graphs/{graph_id}/mcp` — the graph is in the URL (`sec` for the public SEC repository, your graph id for a tenant graph; a subgraph id such as `kg…_dev` is just another URL). The key goes in the same `X-API-Key` header the REST API uses. Keys can be account-wide or **graph-scoped** — valid only for one graph and its subgraphs.

The OAuth path is what a directory listing uses — the [RoboSystems plugin](https://chatgpt.com/plugins/plugin_asdk_app_6a8f6d7d50d081918787990d4cab45ca) in ChatGPT's plugin directory connects this way — and it works the same as a custom connector or from any OAuth-capable MCP client. Registration is automatic: clients with a hosted client-metadata document (Claude, Claude Code) are recognised from it; others register dynamically (RFC 7591). Access tokens are bound to the MCP URL, refresh tokens rotate, and every authorization is revocable — a password change or account deactivation revokes them all.

**Claude Code** — one command; `/mcp` then opens the browser for consent:

```bash
claude mcp add --transport http robosystems https://api.robosystems.ai/v1/mcp
```

With an API key instead (no consent step; the URL fixes the graph):

```bash
claude mcp add --transport http robosystems-sec \
  https://api.robosystems.ai/v1/graphs/sec/mcp \
  --header "X-API-Key: <your key>"
```

**Claude (claude.ai / Desktop)** — Settings → Connectors → Add custom connector with `https://api.robosystems.ai/v1/mcp`; the dialog detects OAuth and the hosted client metadata on its own. Header-only clients (scripts, CI, editors without OAuth) put an API key in `X-API-Key` on a per-graph URL — the **MCP page** in the app (`/connect`) mints keys scoped to one graph for exactly that, and Claude's connector dialog accepts custom request headers too. Credentials never travel in the URL: the `?token=` connector URL was the bridge to OAuth and was retired once OAuth covered those clients. Claude Desktop can alternatively run the [stdio bridge](https://github.com/RoboFinSystems/robosystems-mcp-client) in proxy mode via `claude_desktop_config.json` (which accepts only stdio-shaped `command` entries).

**ChatGPT** — install the [RoboSystems plugin](https://chatgpt.com/plugins/plugin_asdk_app_6a8f6d7d50d081918787990d4cab45ca) from the ChatGPT plugin directory and pick a graph at sign-in. The directory version carries the SEC EDGAR read surface — statements, fact grids, read-only Cypher, filing text search. A RoboLedger graph connects through it too, with those same read tools. For the full RoboLedger toolset in ChatGPT — close, mapping, journal entries — add the server as a custom connector in developer mode (Settings → Connectors → Create) with `https://api.robosystems.ai/v1/mcp`; a custom connector serves every tool of the graph you pick at consent.

**Cursor / VS Code** (`mcp.json`) — the OAuth URL (the editor runs the consent flow), or the per-graph URL with the header:

```json
"robosystems": { "url": "https://api.robosystems.ai/v1/mcp" }
```

```json
"robosystems-sec": {
  "url": "https://api.robosystems.ai/v1/graphs/sec/mcp",
  "headers": { "X-API-Key": "<your key>" }
}
```

Both routes speak JSON-RPC 2.0 and negotiate MCP revision **2025-06-18** — the only revision offered, because it is exactly the one this dispatch implements. Clients on other revisions negotiate to it at `initialize`. The transport is stateless (no session id), rejects JSON-RPC batching unconditionally, streams long-running tool calls over SSE with progress notifications when the client accepts `text/event-stream`, and runs the identical authorization chain as the rest of the API. A self-hosted stack serves the same routes — see [Self-hosted deployments](#self-hosted-deployments).

---

## The MCP Tool Surface

MCP is the shared tool layer. Every entry path — the in-product Operator endpoint, external MCP clients, and in-process operators — calls the *same* tool classes in `middleware/mcp/tools/`. There is one implementation per tool; only the transport differs.

Three transport routes expose the surface:

| Method | Path | Auth | Purpose |
| --- | --- | --- | --- |
| `POST` | `/v1/mcp` | OAuth bearer | Graph chosen at consent ([see above](#connecting-from-mcp-clients)) |
| `POST` | `/v1/graphs/{graph_id}/mcp` | `X-API-Key`, or an OAuth bearer bound to this URL | Graph fixed by the URL |
| `POST` | `/v1/mcp/roboledger` | OAuth bearer | A RoboLedger tenant graph, chosen at consent, with the platform's write tools withheld. It exists because a directory listing freezes one tool list per URL, and a plugin gets one URL |

Both take MCP JSON-RPC: `initialize`, `tools/list`, and `tools/call` with a tool `name` plus an `arguments` object.

```json
{
  "jsonrpc": "2.0", "id": 1, "method": "tools/call",
  "params": {
    "name": "read-graph-cypher",
    "arguments": { "query": "MATCH (n) RETURN count(n)", "parameters": {} }
  }
}
```

The former REST pair (`GET …/mcp/tools`, `POST …/mcp/call-tool`) was removed in v1.10.2; the transport is the only MCP surface.

**Database reads are free.** Cypher, GraphQL, schema, and search tool calls consume no credits — only AI (LLM) token usage does. You can drive the entire MCP surface programmatically without billing impact; credits enter the picture only when an Operator calls Claude.

**Tool availability is per-deployment and per-graph.** The catalog is gated by feature flags and by each graph's `schema_extensions`. A ledger-only deployment exposes no investor fields; a read-only shared-repository graph hides write and live-OLTP tools. Always call `tools/list` (or a schema-discovery tool) first rather than assuming a tool exists.

### Tool Catalog

The following tool names are the exact `name` strings for `tools/call`. Every tool also carries a `title` and MCP annotations (`readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`) in `tools/list`; the Cypher read tools are hinted read-only, matching what the server enforces, and `write-graph-cypher` is hinted destructive; every other write is hinted destructive too, conservatively, because a directory scan reads the hint rather than the statement. The [three planes](#the-three-retrieval-planes) below describe *retrieval*; the tools that manage memory, documents, and the graph workspace sit alongside them rather than inside one.

**Retrieval — the three planes:**

| Tool | Plane / role | Gated by |
| --- | --- | --- |
| `read-graph-cypher` | Analytical (read-only Cypher) | Always available |
| `get-graph-schema` | Analytical schema (Cypher/graph) | Always available |
| `get-graph-info` | Basic information about the graph itself | Always available |
| `query-graphql` | Operational (typed OLTP read) | `EXTENSIONS_GRAPHQL_ENABLED` + `MCP_GRAPHQL_ENABLED` |
| `get-graphql-schema` | Operational schema (GraphQL SDL) | `EXTENSIONS_GRAPHQL_ENABLED` + `MCP_GRAPHQL_ENABLED` |
| `search-documents` | Unstructured (hybrid BM25 + KNN) | `SEMANTIC_SEARCH_ENABLED` |
| `get-document-section` | Unstructured (full section text) | `SEMANTIC_SEARCH_ENABLED` |
| `financial-statement-analysis` | Curated analytical (LadybugDB) | `roboledger` schema extension |
| `live-financial-statement` | Curated operational (OLTP) | `roboledger` extension; writable, non-shared graphs |
| `build-fact-grid` | Curated analytical (XBRL hypercube) | `FACT_GRID_ENABLED` |
| `resolve-element` | Cross-plane bridge (qname → graph) | `roboledger` extension **and** the repository manifest's `has_semantic_enrichment` flag |
| `get-example-queries` | Discovery helper | `roboledger` schema extension |

**Semantic memory** — a per-graph vector store that gives an Operator state across turns:

| Tool | Role | Gated by |
| --- | --- | --- |
| `recall` | Read stored memories | `SEMANTIC_MEMORY_ENABLED` + `MCP_SEMANTIC_MEMORY_ENABLED`; not on shared repos |
| `remember` / `update-memory` | Store and revise | Same, plus a writable non-subgraph (a subgraph gets no store of its own) |
| `forget` | Delete a memory | Same, plus a writable graph |

**Documents** — CRUD over the platform's document rows, distinct from searching them:

| Tool | Role | Gated by |
| --- | --- | --- |
| `get-document` / `list-documents` | Read | `SEMANTIC_SEARCH_ENABLED`; user graphs only (shared repos have no document rows) |
| `create-document` / `update-document` / `delete-document` | Write | Same, plus a writable graph |

**Workspace** — the lifecycle subset mirroring `/v1/graphs/{g}/operations/*`:

| Tool | Role | Gated by |
| --- | --- | --- |
| `list-subgraphs` | Navigation — each row carries the subgraph's connector URL | `MCP_WORKSPACE_ENABLED` |
| `create-subgraph` / `delete-subgraph` / `create-backup` | Lifecycle writes | `MCP_WORKSPACE_ENABLED`; writable graphs only |
| `write-graph-cypher` | Write Cypher | `MCP_SUBGRAPH_OPS_ENABLED`; writable subgraphs only |
| `add-node-table` / `add-relationship-table` | Subgraph schema DDL | `MCP_SUBGRAPH_OPS_ENABLED`; writable subgraphs only |
| `set-write-policy` | Set a connection's outbound write-back policy | Writable, non-shared graphs |
| `sync-connection` | Trigger an on-demand connector resync | Writable, non-shared parent graphs |

`change-tier` is deliberately absent: it is a multi-minute destructive volume migration with billing consequences, so it stays on REST for humans. Restore is absent because it has no customer-facing surface at all — see [Graph Operations](Graph-Operations).

In addition, **write tools are auto-generated** from the operation specs of each enabled extension (mapping, schedule, and event writes, for example). These are derived from the same `OperationSpec`s that back the REST command surface, so the MCP write tools and the `/extensions/*/operations/*` endpoints stay in lockstep. The exact set depends on which extensions are enabled; enumerate it for a given graph with `tools/list`.

Each tool's input schema comes back with `tools/list`. The MCP surface is deliberately absent from the OpenAPI spec — and therefore from the generated SDKs: the REST API at [robosystems.ai/docs/api](https://robosystems.ai/docs/api) is the human-and-SDK surface, the transport is the MCP-client surface.

---

## The Three Retrieval Planes

An Operator answers a question by reading across three planes. Each plane has a **discover-schema-first** tool and an **execute** tool. The pattern mirrors the platform's storage split: live transactional state (OLTP), the materialized analytical graph (OLAP), and the unstructured document index.

```text
                          ┌─────────────────────────────────┐
                          │           AI Operator           │
                          │   (model reasoning over MCP)    │
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

The Operator endpoint runs a model-driven executor against a graph. It is mounted at `/v1/graphs/{graph_id}/operator`.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/v1/graphs/{graph_id}/operator` | List operators available on this graph; `?capability=` filters (for example `financial_analysis`, `rag_search`) |
| `POST` | `/v1/graphs/{graph_id}/operator` | Auto-select an operator and execute |
| `GET` | `/v1/graphs/{graph_id}/operator/{operator_type}` | Metadata for a specific operator |
| `POST` | `/v1/graphs/{graph_id}/operator/{operator_type}` | Execute a specific operator |

**Auto-select vs specific.** `POST /operator` ranks the available operators internally and dispatches to the best match for the message — there is no endpoint to preview that ranking. `POST /operator/{operator_type}` targets one operator directly. Note that `{operator_type}` is a path parameter, so a misspelled or invented operator name returns **404 "Operator type '…' not found"** rather than a routing error.

**Modes** (`mode` field on the request): `quick`, `standard`, `extended`, `streaming`. Each maps to an execution profile with a tool-call budget and timeout:

| Mode | Tool-call budget | Timeout |
| --- | --- | --- |
| `quick` | ≤ 2 | 30 s |
| `standard` | ≤ 5 | 60 s |
| `extended` | ≤ 12 | 300 s |
| `streaming` | ≤ 8 | 120 s |

**Strategies.** A `?mode=sync|async|stream|auto` query parameter overrides the transport strategy on an operator `POST`. With `auto`, the endpoint chooses sync, Server-Sent Events, or async based on the tool type, client capabilities, and current load.

The Operator `POST` path can be disabled by deployment via `OPERATOR_POST_ENABLED=false` (returns 403). The GraphQL MCP tools have a separate kill switch, `MCP_GRAPHQL_ENABLED`.

An `OperatorRequest` carries `message` (required), `mode`, optional `history` (a list of `{role, content}` turns), `context`, `enable_rag`, `force_extended_analysis`, `operator_type`, and `selection_criteria`. The `OperatorResponse` returns `content`, `operator_used`, `mode_used`, `metadata`, `tokens_used`, `confidence_score`, `execution_time`, and `error_details`. See [robosystems.ai/docs/api](https://robosystems.ai/docs/api) for the full field reference.

---

## Operators as Tools

Operators are not just behind the endpoint — they are themselves MCP-tool consumers, and they can be exposed as tools to any orchestrator.

Each operator runs via an `OperatorContext` injected into its `run()` method. The context carries `graph_id`, `user_id`, `query`, `mode`, and `history`, plus three services: `ai` (a tracked model client), `progress` (a reporter for streaming), and `tools` (a `ToolAccess` handle). Every operator reaches every MCP tool through a single method:

```python
await context.tools.call_tool(tool_name, arguments, return_raw)
```

Because operators call the same `middleware/mcp/tools/` classes as the external entry points, the tool an MCP client such as Claude or ChatGPT invokes is the identical implementation an in-process operator invokes. There is one tool surface, used three ways.

Two operators ship today:

- **AnalystOperator** (`analyst`; `cypher` is accepted as its former name) — answers natural-language questions over the graph from its read-only tool surface: curated financial reads (`live-financial-statement`, `build-fact-grid`, close and mapping status), `search-documents`, `recall`, GraphQL, and read-only Cypher as the general fallback. It answers questions like "What are my five largest expense accounts this year?".
- **MappingOperator** — assists with chart-of-accounts mapping; scoped to graphs carrying the `roboledger` schema extension (it will not run on a graph without it).

An operator declares its capabilities through an `OperatorSpec` (`name`, `description`, `capabilities`, `supported_modes`, `requires_credits`, `execution_profile`, and a `graph_scope`). The `graph_scope` restricts which graphs the operator runs on — by `shared_repo` or by `schema_extension` — which is how MappingOperator is confined to ledger graphs.

For the operator framework patterns, see the [Operations README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md) in codebase.

---

## Worked Examples

All examples target `https://api.robosystems.ai` with your API key in the `X-API-Key` header on the per-graph route. Set `GRAPH_ID` to your graph (for example a roboledger graph `kg1a2b3c4d5e`). Without an `Accept: text/event-stream` header the transport answers plain JSON, which is what you want from curl.

### List the Tools Available on a Graph

This shows exactly which planes a given deployment and graph expose. Always start here.

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai
export GRAPH_ID=kg...               # from GET /v1/graphs or the app's graph selector

curl -s -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/mcp" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }'
```

### Analytical Plane — Cypher over LadybugDB

No credits are consumed. Run `get-graph-schema` first in real use to learn the node and relationship types.

```bash
curl -s -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/mcp" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0", "id": 1, "method": "tools/call",
    "params": {
      "name": "read-graph-cypher",
      "arguments": {
        "query": "MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element) RETURN el.qname, f.value LIMIT 10",
        "parameters": {}
      }
    }
  }'
```

### Operational Plane — Typed GraphQL over OLTP

The `graph_id` comes from the URL, never the query body.

```bash
curl -s -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/mcp" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0", "id": 1, "method": "tools/call",
    "params": {
      "name": "query-graphql",
      "arguments": { "query": "{ fiscalCalendar { closedThrough closeTarget } }" }
    }
  }'
```

### Unstructured Plane — Hybrid Document Search

`semantic: true` opts into KNN vector ranking on top of keyword search.

```bash
curl -s -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/mcp" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0", "id": 1, "method": "tools/call",
    "params": {
      "name": "search-documents",
      "arguments": { "query": "month end close procedures", "semantic": true, "size": 5 }
    }
  }'
```

### Ask an Operator a Natural-Language Question

Auto-select picks the right operator; `?mode=sync` returns a single JSON response rather than streaming.

```bash
curl -s -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/operator?mode=sync" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{ "message": "What are my five largest expense accounts this year?", "mode": "standard" }'
```

---

## Configuration and Gotchas

| Concern | Detail |
| --- | --- |
| `read-graph-cypher` is read-only | Write Cypher (`CREATE`/`SET`/`DELETE`/`REMOVE`/`MERGE`/`DROP`) raises "Only read-only queries are allowed" |
| Write Cypher is a different tool | `write-graph-cypher` works on **subgraphs only** (memory), never the parent graph or shared repos; gated by `MCP_SUBGRAPH_OPS_ENABLED` |
| `query-graphql` is read-only | Mutations and subscriptions are rejected; depth/field/alias complexity is gated |
| Discover schema first | `get-graph-schema` returns the **Cypher** schema; `get-graphql-schema` returns the **GraphQL SDL** — distinct tools, easy to confuse |
| Tool catalog varies | Gated by feature flags and per-graph `schema_extensions`; call `tools/list` before assuming a tool exists |
| `live-` vs `analysis` | `live-financial-statement` = OLTP, tenant graphs, current books; `financial-statement-analysis` = LadybugDB, historical/cross-period |
| Operator POST kill switch | `OPERATOR_POST_ENABLED=false` returns 403 |
| GraphQL MCP kill switch | `MCP_GRAPHQL_ENABLED=false` removes `query-graphql` / `get-graphql-schema` |
| Bulk ingestion is not a read tool | Loading data into the graph is not part of the `read-graph-cypher` surface; use the file-upload ingestion path instead |
| Search scope | Shared-repo search resolves to the parent `graph_id`; subgraphs are not a search boundary |
| `agent` tools are not AI | On ledger graphs, `create-agent` / `list-agents` / `update-agent` manage **REA counterparties** — customers, vendors, employees (the accounting-ontology term). The AI executor layer is always called an Operator |
| Credits | Only AI (LLM) calls consume credits; every database/search tool call is free |

Relevant feature flags (`ROBOLEDGER_ENABLED`, `ROBOINVESTOR_ENABLED`, `EXTENSIONS_GRAPHQL_ENABLED`, `SEMANTIC_SEARCH_ENABLED`, `MCP_GRAPHQL_ENABLED`, `MCP_SUBGRAPH_OPS_ENABLED`, `MCP_WORKSPACE_ENABLED`, `SEMANTIC_MEMORY_ENABLED`, `MCP_SEMANTIC_MEMORY_ENABLED`, `FACT_GRID_ENABLED`) determine the catalog at runtime. See the [Configuration README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/config/README.md) in codebase.

---

## Self-hosted deployments

A stack you run serves both MCP routes at `http://localhost:8000/v1/mcp` and `http://localhost:8000/v1/graphs/{graph_id}/mcp`. The OAuth route requires `MCP_OAUTH_ENABLED=true`; the per-graph route takes the key from `just demo-user` in `X-API-Key`. See [Local Development](Local-Development).

Operators on a stack you run can use an open-weight model in place of the Bedrock models, through any OpenAI-compatible server. Which tiers it backs, which endpoints work, and which models we tested are in [Self-Hosted AI Models](Self-Hosted-AI-Models).

---

## Related Documentation

**Wiki Guides:**
- **[Architecture Overview](Architecture-Overview)** - The OLTP/OLAP split that the three planes sit on top of
- **[Querying the Analytical Graph](Querying-the-Analytical-Graph)** - Cypher over LadybugDB in depth
- **[GraphQL Reads](GraphQL-Reads)** - The typed operational read surface in depth
- **[Search and AI Retrieval](Search-and-AI-Retrieval)** - The document index and hybrid search in depth
- **[Self-Hosted AI Models](Self-Hosted-AI-Models)** - Running the operators on a model you choose

**Codebase Documentation:**
- **[GraphQL Extensions](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md)** - Strawberry GraphQL surface, resolver patterns
- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Operator framework and business-logic kernel
- **[Authentication](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/auth/README.md)** - API keys, OAuth 2.1 for MCP clients, and access control
- **[API reference](https://robosystems.ai/docs/api)** - Full endpoint and tool reference with machine-readable OpenAPI spec

---

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
