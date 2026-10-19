# AI Operators & MCP

Every RoboSystems graph is an MCP server; this page covers connecting a client, the full tool catalog, and the Operator endpoint that runs a model. RoboSystems exposes its data and reasoning surface to AI through the [Model Context Protocol](https://modelcontextprotocol.io/). An **AI Operator** is a model-driven executor that answers a natural-language question by composing reads across three retrieval planes — operational, analytical, and unstructured — using the same MCP tool layer that any MCP client (Claude, ChatGPT, Claude Code, Cursor, a custom orchestrator) can call directly.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development.md).

---

## Connecting from MCP Clients

Every graph is an MCP server, served over MCP's Streamable HTTP transport directly by the API. There are two ways in:

> **OAuth (recommended):** `https://api.robosystems.ai/v1/mcp` — one URL for everyone. The client discovers the authorization server, you sign in to RoboSystems, and the consent screen asks **which graph** to connect: your own graphs, their subgraphs, and the shared repositories you subscribe to. One authorization is one graph; to work on another graph, reconnect and pick it. On the RoboLedger route (`/v1/mcp/roboledger`, [below](#the-mcp-tool-surface)) the screen offers only RoboLedger tenant graphs — no subgraphs, no shared repositories.
>
> **API key:** `https://api.robosystems.ai/v1/graphs/{graph_id}/mcp` — the graph is in the URL (`sec` for the public SEC repository, your graph id for a tenant graph; a subgraph id such as `kg…_dev` is just another URL). The key goes in the same `X-API-Key` header the REST API uses. Keys can be account-wide or **graph-scoped** — valid only for one graph and its subgraphs.

The OAuth path is what a directory listing uses — [RoboSystems SEC](https://claude.ai/directory/robosystems-sec) in Claude's connector directory and the [RoboSystems plugin](https://chatgpt.com/plugins/plugin_asdk_app_6a8f6d7d50d081918787990d4cab45ca) in ChatGPT's plugin directory both connect this way — and it works the same as a custom connector or from any OAuth-capable MCP client. Registration is automatic: clients with a hosted client-metadata document (Claude, Claude Code) are recognised from it; others register dynamically (RFC 7591). Access tokens are bound to the MCP URL, refresh tokens rotate, and every authorization is revocable — a password change or account deactivation revokes them all.

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

**Claude (claude.ai / Desktop)** — Customize → Connectors → Add custom connector with `https://api.robosystems.ai/v1/mcp`; the dialog detects OAuth and the hosted client metadata on its own. Header-only clients (scripts, CI, editors without OAuth) put an API key in `X-API-Key` on a per-graph URL — the **MCP page** in the app (`/connect`) mints keys scoped to one graph for exactly that, and Claude's connector dialog accepts custom request headers too. Credentials never travel in the URL: the `?token=` connector URL was the bridge to OAuth and was retired once OAuth covered those clients. Claude Desktop can alternatively run the [stdio bridge](https://github.com/RoboFinSystems/robosystems-mcp-client) in proxy mode via `claude_desktop_config.json` (which accepts only stdio-shaped `command` entries). For SEC filings alone, add [RoboSystems SEC](https://claude.ai/directory/robosystems-sec) from Claude's connector directory instead: no address to paste, and the connection is pinned to the `sec` repository (`/v1/graphs/sec/mcp`, read-only tools).

**ChatGPT** — install the [RoboSystems plugin](https://chatgpt.com/plugins/plugin_asdk_app_6a8f6d7d50d081918787990d4cab45ca) from the ChatGPT plugin directory and sign in. Since v1.1.0 the directory version is pinned to the SEC EDGAR repository (`/v1/graphs/sec/mcp`) — statements, fact grids, disclosures and information blocks, read-only Cypher, filing text search — so it no longer reaches a tenant graph. For a RoboLedger graph in ChatGPT — close, mapping, journal entries — add the server as a custom connector in developer mode (Settings → Connectors → Create) with `https://api.robosystems.ai/v1/mcp`; a custom connector serves every tool of the graph you pick at consent.

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

Every MCP route (the three are listed [below](#the-mcp-tool-surface)) speaks JSON-RPC 2.0 and negotiates MCP revision **2025-06-18** — the only revision offered, because it is exactly the one this dispatch implements. Clients on other revisions negotiate to it at `initialize`. The transport is stateless (no session id), rejects JSON-RPC batching unconditionally, streams long-running tool calls over SSE with progress notifications when the client accepts `text/event-stream`, and runs the identical authorization chain as the rest of the API. A self-hosted stack serves the same routes — see [Self-hosted deployments](#self-hosted-deployments).

---

## The MCP Tool Surface

MCP is the shared tool layer. Every entry path — the in-product Operator endpoint, external MCP clients, and in-process operators — calls the *same* tool classes in `middleware/mcp/tools/`. There is one implementation per tool; only the transport differs.

The surface is served on three routes of the same transport:

| Method | Path | Auth | Purpose |
| --- | --- | --- | --- |
| `POST` | `/v1/mcp` | OAuth bearer | Graph chosen at consent ([see above](#connecting-from-mcp-clients)) |
| `POST` | `/v1/graphs/{graph_id}/mcp` | `X-API-Key`, or an OAuth bearer bound to this URL | Graph fixed by the URL |
| `POST` | `/v1/mcp/roboledger` | OAuth bearer | A RoboLedger tenant graph, chosen at consent, with the [route's exclusions](#the-roboledger-route) withheld. It exists because a directory listing freezes one tool list per URL, and a plugin gets one URL |

All three take MCP JSON-RPC: `initialize`, `tools/list`, and `tools/call` with a tool `name` plus an `arguments` object.

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

**Database reads are free.** Cypher, GraphQL, schema, and search tool calls consume no credits — only AI (LLM) token usage does. You can drive the entire MCP surface programmatically without billing impact; credits enter the picture only when an Operator calls a model.

**Tool availability is per-deployment and per-graph.** The catalog is gated by feature flags, by each graph's `schema_extensions`, and by the kind of graph: a RoboLedger or RoboInvestor tenant graph, the read-only `sec` repository, a subgraph, or a custom graph with no extension. A read-only shared repository drops every write and every tool that reads a tenant's OLTP ledger. Always call `tools/list` (or a schema-discovery tool) first rather than assuming a tool exists.

### Tool Catalog

The tool names below are the exact `name` strings for `tools/call`, grouped by family, with where each is served. Every tool also carries a `title` and the four MCP annotations (`readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`) in `tools/list`, declared per tool: the Cypher read tools are hinted read-only, matching what the server enforces; a write that only adds is hinted non-destructive; a write that reaches QuickBooks is hinted open-world; and a write not yet classified gets the most cautious hint on all four. The [three planes](#the-three-retrieval-planes) below describe *retrieval*; most families here sit beside them rather than inside one.

"RoboLedger tenant" means a graph you own with the `roboledger` extension, on a deployment with `ROBOLEDGER_ENABLED`; "every graph" includes `sec` unless a row says otherwise.

**Graph, schema and query**

| Tool | Purpose | Served on |
| --- | --- | --- |
| `get-graph-info` | Graph id, approximate node count, node labels, relationship types, and whether the graph is read-only; takes no arguments | Every graph |
| `get-graph-schema` | The Cypher schema: node tables with their properties, relationship tables with their endpoints | Every graph |
| `read-graph-cypher` | Read-only Cypher (Plane B) | Every graph |
| `get-example-queries` | Working Cypher patterns for the graph's schema | Graphs with the `roboledger` extension, `sec` included |
| `get-graphql-schema` / `query-graphql` | The GraphQL SDL, and a read-only GraphQL query over the extensions OLTP (Plane A) | Tenant graphs, not shared repositories; `EXTENSIONS_GRAPHQL_ENABLED` + `MCP_GRAPHQL_ENABLED` |

**Statements and report views** — read the analytical graph, or the report held whole:

| Tool | Purpose | Served on |
| --- | --- | --- |
| `financial-statement-analysis` | One statement from the XBRL hypercube — a `sec` filer by ticker, or a materialized tenant report | Graphs with the `roboledger` extension |
| `build-fact-grid` | Facts across elements, periods, entities and dimensions, deduplicated | Graphs with the `roboledger` extension; `FACT_GRID_ENABLED` |
| `disclosures` | The map of a report's sections — one row per note or statement; with `topic`, one family's blocks | Graphs with the `roboledger` extension |
| `information-block` | One section of a report read whole: rows, breakdowns by its own axes, calculation footing, text blocks | Graphs with the `roboledger` extension |
| `live-financial-statement` | A statement straight from the OLTP books, before materialization | RoboLedger tenant |
| `consolidation-worksheet` | The group's statement with each entity as a column: members, combined, adjustments, consolidated, and equity-method members beside the sum | RoboLedger tenant |
| `preview-consolidation-entries` | The consolidating entries a group's month needs, computed from the books: each kind and member brought to its month-end balance, less what the consolidation entity holds; residuals and disagreements returned, never plugged | RoboLedger tenant |

**SEC filings** — read-only, on the shared repository:

| Tool | Purpose | Served on |
| --- | --- | --- |
| `describe-filing` | One filing's layout: periods, statements and disclosures by role, and its Items with the offsets `read-text` pages from | Shared repositories (`sec`) |
| `search-text` | Words found inside one filing's whole text, in document order — the filing's own document from any processed year, or an 8-K earnings release with its exhibits | Shared repositories (`sec`) |
| `read-text` | One filing's whole text, a window at a time, from an offset | Shared repositories (`sec`) |
| `resolve-element` | A concept in plain words → the canonical concept and the element qnames filers use for it | A shared repository whose manifest declares `has_semantic_enrichment` — today `sec` only |

**Search and documents** — the document index (Plane C) and the platform's document rows:

| Tool | Purpose | Served on |
| --- | --- | --- |
| `search-documents` | Ranked hits over filing narratives and your documents — BM25, plus KNN with `semantic: true` | Every graph; `SEMANTIC_SEARCH_ENABLED` |
| `get-document-section` | One hit's section text, a window at a time | Every graph; `SEMANTIC_SEARCH_ENABLED` |
| `get-document` / `list-documents` | Read your document rows | Your own graphs (shared repositories have no document rows); `SEMANTIC_SEARCH_ENABLED` |
| `create-document` / `update-document` / `delete-document` | Write them; the search index follows each write | Same, plus a writable graph |
| `bind-text-block` | Bind a document, or one of its sections, to a disclosure element as a text-block fact | Writable RoboLedger tenant |

**Memory** — a per-graph vector store that gives an Operator state across turns; `SEMANTIC_MEMORY_ENABLED` + `MCP_SEMANTIC_MEMORY_ENABLED`, never on shared repositories:

| Tool | Purpose | Served on |
| --- | --- | --- |
| `recall` | Stored memories ranked by meaning | Your own graphs and their subgraphs |
| `remember` / `update-memory` | Store a memory, or revise one in place | Writable graphs that are not subgraphs (a subgraph gets no store of its own) |
| `forget` | Delete a memory by id | Writable graphs, subgraphs included |

**Workspace** — the lifecycle subset of `/v1/graphs/{g}/operations/*`, plus the sync edges:

| Tool | Purpose | Served on |
| --- | --- | --- |
| `list-subgraphs` | The parent and its subgraphs, each row with its connector URL | Every graph; `MCP_WORKSPACE_ENABLED` |
| `create-subgraph` / `delete-subgraph` / `create-backup` | Lifecycle writes | Writable graphs; `MCP_WORKSPACE_ENABLED` |
| `write-graph-cypher` | Write Cypher | Works on a subgraph only (a parent answers `subgraph_required`); `MCP_SUBGRAPH_OPS_ENABLED` |
| `add-node-table` / `add-relationship-table` | Subgraph schema DDL | Same |
| `get-graph-sync-status` | Freshness on both edges: source system → ledger, and ledger → graph | RoboLedger tenant parent graphs |
| `materialize` | Rebuild the graph from its source data | Writable RoboLedger tenant parent graphs |
| `sync-connection` | An on-demand resync from a connected source such as QuickBooks | Writable tenant parent graphs |
| `set-write-policy` | Whether a connection writes back to its source system | Writable tenant graphs |

`change-tier` is deliberately absent: it is a multi-minute destructive volume migration with billing consequences, so it stays on REST for humans. Restore is absent because it has no customer-facing surface at all — see [Graph Operations](Graph-Operations.md).

**Close and fiscal calendar** — RoboLedger tenant:

| Tool | Purpose |
| --- | --- |
| `get-close-playbook` | The close's tool sequence, setup decisions and gotchas — call it before setting up or running a close |
| `get-fiscal-calendar` | What period is next to close and what blocks it |
| `get-period-close-status` | What schedule-derived work is done for a period |
| `list-period-drafts` | The period's draft closing entries, line by line |
| `close-period` / `reopen-period` | Post the drafts and lock the period; reopen a closed one for adjustments |
| `promote-obligations` | Move matured schedule obligations forward and draft their closing entries |
| `rebuild-schedule` / `terminate-schedule` | Regenerate a schedule in place; end one early at a month-end cutoff |
| `update-journal-entry` / `delete-journal-entry` | Edit or delete a draft entry (posted entries are reversed, never edited) |
| `propose-consolidation-entries` | Draft a group's consolidating entries for a month in the consolidation entity, replacing the engine's earlier drafts; its close posts them |
| `set-allocation-override` | Set the outside owners' share of a member's result for a month in place of the pro-rata share, with its basis |
| `backfill-plan-history` | Compile monthly statement history behind the close boundary |

**Reconciliations** — RoboLedger tenant; how they gate the close is in [Period Close](Period-Close.md#reconciliations):

| Tool | Purpose |
| --- | --- |
| `preview-reconciliations` | Compare the ledger at a period end with something outside it, recording nothing |
| `refresh-reconciliations` | Run every reconciliation that applies at a period end and record each result |
| `record-statement-balance` | Record a bank, card or loan statement's ending balance for one account and reconcile to it |
| `set-reconciliation-policy` | Whether the close waits on a reconciliation, its materiality, and whether it needs a separate reviewer |
| `sign-off-reconciliation` | Record a reviewer's sign-off on a period that reconciles |
| `preview-reconciling-item` / `resolve-reconciling-item` | Read what changed on a posted event whose source payload changed, and dispose of it |

**Mapping and taxonomy** — RoboLedger tenant:

| Tool | Purpose |
| --- | --- |
| `list-mapping-structures` / `get-mapping-summary` | The chart-of-accounts mapping structures, and their coverage |
| `get-unmapped-elements` / `suggest-mapping` | Accounts not yet mapped, and heuristic reporting-concept candidates for one (no AI, no writes) |
| `create-mapping-association` / `delete-mapping-association` | Add or remove one account → reporting-concept mapping |
| `initialize-chart-of-accounts` | Create the chart from a shipped template, for a graph that has none |
| `create-taxonomy-block` / `update-taxonomy-block` / `delete-taxonomy-block` | Author a taxonomy — a chart of accounts, a reporting extension, a custom ontology — as one envelope |
| `link-entity-taxonomy` | Point the entity at a different primary chart |

**Information blocks, metrics and forecasts** — RoboLedger tenant:

| Tool | Purpose |
| --- | --- |
| `list-information-blocks` / `get-information-block` | Find blocks by type, and read one whole |
| `create-information-block` / `update-information-block` / `delete-information-block` | Block writes, dispatched by `block_type` |
| `evaluate-rules` | Run the rules on a structure against its facts and record the results |
| `compute-metrics` / `assert-metrics` | Derive a metric block's values from report facts; write externally observed values |
| `compute-forecast` | Walk a forecast block's drivers forward month by month |

**Events** — RoboLedger tenant:

| Tool | Purpose |
| --- | --- |
| `list-event-blocks` / `get-event-block` | Find and read business events |
| `create-event-block` / `update-event-block` | Capture an event (optionally applying its handler); move it through `captured` → `classified` → `committed` or `voided` |
| `preview-event-block` | Dry-run a handler against an event, writing nothing |
| `execute-event-block` | Publish an event's entries to QuickBooks, for a connection whose write policy sends them |
| `list-event-handlers` / `get-event-handler` / `create-event-handler` / `update-event-handler` | The rules that turn events into entries |

**Counterparties** — RoboLedger tenant. These manage REA agents (customers, vendors, employees), not AI:

| Tool | Purpose |
| --- | --- |
| `list-agents` / `get-agent` | Find and read counterparties |
| `agent-activity` | A counterparty's recent events and transactions |
| `create-agent` / `update-agent` | Add or patch one (agents are deactivated, never deleted) |

**Reports and entity** — RoboLedger tenant:

| Tool | Purpose |
| --- | --- |
| `create-report` / `regenerate-report` | Generate a report's facts from the ledger; re-run them against the latest ledger |
| `delete-report` | Delete a report that has not been filed, with its facts and published files |
| `get-report-bundle` | A short-lived download link for a published report, for a tool that opens a file by URL |
| `update-entity` / `change-reporting-style` | Edit the graph's entity; switch how its statements are laid out |

**RoboInvestor** — RoboInvestor tenant (the `roboinvestor` extension, `ROBOINVESTOR_ENABLED`); reads go through `query-graphql`:

| Tool | Purpose |
| --- | --- |
| `create-portfolio-block` / `update-portfolio-block` / `delete-portfolio-block` | A portfolio and its positions, as one envelope |
| `create-security` / `update-security` / `delete-security` | A security held by the graph's entity (delete is a soft delete) |

Most extension writes are generated from the `OperationSpec`s that back `/extensions/{domain}/{graph_id}/operations/*`, so an MCP write and its REST operation share one validation, one extension gate and one error map; a few that need more than that runner gives (the close and reopen, `bind-text-block`, `delete-report`) are written by hand. Enumerate what a given graph serves with `tools/list`.

#### Subgraphs

A tenant subgraph has no extensions OLTP schema, no connections and no materialization, so it serves a fixed profile — schema and Cypher in both directions, plus memory and the way back out: `get-graph-info`, `get-graph-schema`, `get-example-queries`, `read-graph-cypher`, `write-graph-cypher`, `add-node-table`, `add-relationship-table`, `recall`, `forget`, and `list-subgraphs`. Any other tool is withheld from `tools/list` and refused on call. (`remember` and `update-memory` are in the profile but not served, since a subgraph has no memory store of its own.) A subgraph of a shared repository follows the repository's catalog instead.

#### The RoboLedger route

`/v1/mcp/roboledger` serves a RoboLedger tenant graph's full catalog minus what a chat client cannot use or should not drive. It withholds the subgraph tools (`write-graph-cypher`, `add-node-table`, `add-relationship-table`), the workspace administration tools (`create-subgraph`, `delete-subgraph`, `list-subgraphs`, `create-backup`, `materialize`), `get-report-bundle` (its credentialed link has nowhere to go but the transcript), `set-write-policy` (QuickBooks write-back is an owner decision made in the app), `rebuild-schedule` and `backfill-plan-history` (maintenance, not the close), and `delete-taxonomy-block`. It is an exclusion list, so a new ledger tool appears on the route by default.

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

Two curated tools sit on top of this plane:
- `financial-statement-analysis` — graph-backed statement analysis over LadybugDB (SEC + materialized tenants); historical and cross-period.
- `build-fact-grid` — pivot tables over the XBRL hypercube.

Beside them, `disclosures` and `information-block` read one report section by section. They run xbrlkit over the report held whole — the published filing on `sec`, the ledger's own report on a tenant — so the graph is not in their path. On `sec`, raw Cypher has four rules that decide whether a number is right; see [SEC Data Model & Query Rules](SEC-Data-Model-and-Query-Rules.md).

`describe-filing`, `search-text` and `read-text` read the same filing as text: its own document from its public folder — any processed year, back to 2015, and an 8-K earnings release with its EX-99 exhibits. They register on shared repositories only; a ledger files no document, and its sections are the pair above. A `ticker` picks the filing: the latest annual report, or `fiscal_year` / `period_type` for another, `accession` for one named, `form: "8-K"` for the latest earnings release. Neither the search index nor EDGAR is in the path. `search-text` matches words, never a regular expression, and answers *where in this filing*; `search-documents` (Plane C) answers *which filings*, across the corpus. On `sec`, every tool that resolves a filing — these three, `financial-statement-analysis`, `disclosures` and `information-block` — returns `resolved_report.links`, with the viewer URL to hand a person; see [Where a Filing Is Served](SEC-Data-Model-and-Query-Rules.md#where-a-filing-is-served).

Note the distinct curated companion `live-financial-statement`, which reads the **OLTP** books of a tenant graph (current period, before materialization). Do not conflate the two: `financial-statement-analysis` is analytical/historical, `live-financial-statement` is operational/current.

### Plane C — Unstructured (Hybrid Document Search)

**Tools:** `search-documents`, `get-document-section`
**Backs onto:** the document / SEC-filing index.

This plane retrieves narrative context — disclosures, footnotes, policy documents. `search-documents` runs a BM25 keyword search by default; hybrid semantic ranking (BM25 + KNN vector similarity) is opt-in via `semantic: true`. Each hit returns a `document_id`; pass it to `get-document-section` to read the section a window at a time (`offset` / `length`, default 4,000 and at most 8,000 characters, with `next_offset` while more follows). A long section is indexed in parts of about 25,000 characters; a hit carries `part` / `part_count`, and a section read returns `next_document_id` for the next part. Once a hit has named the filing, `search-text` (Plane B) reads inside that filing whole — the cover page, a footnote to a table, an exhibit — which no indexed section carries.

On a shared repository's subgraph, the search tools read the parent repository's index — subgraphs are not a search boundary.

### The Cross-Plane Bridge

The planes are most powerful in combination. An iXBRL disclosure read with `get-document-section` carries `xbrl_elements`, the fact tags in that section (for example `us-gaap:Goodwill`); the MCP `search-documents` hits leave them out to stay small, while the REST search keeps them. On `sec`, `resolve-element` maps a concept named in the prose to the qnames and canonical concept the graph uses, and `read-graph-cypher` then pulls the structured fact. This bridges narrative context to structured numbers:

```text
search-documents  →  get-document-section  →  resolve-element  →  read-graph-cypher
(find the disclosure) (read it; xbrl_elements) (concept → qnames)  (fetch the fact value)
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

**Modes** (`mode` field on the request): `quick`, `standard`, `extended`, `streaming`. Each maps to an execution profile with a tool-call budget, a timeout, and a reasoning effort:

| Mode | Tool-call budget | Timeout | Effort |
| --- | --- | --- | --- |
| `quick` | ≤ 2 | 30 s | `low` |
| `standard` | ≤ 5 | 60 s | `medium` |
| `extended` | ≤ 12 | 300 s | `high` |
| `streaming` | ≤ 8 | 120 s | `medium` |

Effort controls how hard the model thinks and how many tool calls it chooses to make. It is sent only to models that support it, which today are Claude Sonnet 5.5 (the `balanced` tier, the platform default) and Claude Opus 5.5 (the `quality` tier); on other models the request is unchanged. The author operator (below) replaces the tool-call budget with a fixed 25-step backstop, since a write plan cut off partway would leave some changes made and some not.

**Where a run executes.** Every operator run executes on the background worker. By default an operator `POST` queues the run and answers **202** with the operation's `_links` (`stream`, `status`, `cancel`); follow `stream` for progress and the result. `?mode=sync` waits up to 50 seconds and answers **200** with the `OperatorResponse` when the run finishes in time, or 202 with the links when it is still going. `async`, `stream` and `auto` are accepted and behave like the default.

The Operator `POST` path can be disabled by deployment via `OPERATOR_POST_ENABLED=false` (returns 403). The GraphQL MCP tools have a separate kill switch, `MCP_GRAPHQL_ENABLED`.

An `OperatorRequest` carries `message` (required), `mode`, optional `history` (a list of `{role, content}` turns), `context`, `max_credits`, `enable_rag`, `force_extended_analysis`, `operator_type`, and `selection_criteria`. `max_credits` is a soft ceiling on the run's spend: it is checked before each model call (the first call always runs), and once reached the operator stops calling tools and answers from what it has, so a run can end slightly above it. The `OperatorResponse` — the 200 body under `?mode=sync`, and the result a finished operation carries — returns `content`, `operator_used`, `mode_used`, `metadata`, `tokens_used`, `confidence_score`, `execution_time`, and `error_details`. See [robosystems.ai/docs/api](https://robosystems.ai/docs/api) for the full field reference.

---

## Operators as Tools

Operators are not just behind the endpoint — they are themselves MCP-tool consumers, and they can be exposed as tools to any orchestrator.

Each operator runs via an `OperatorContext` injected into its `run()` method. The context carries `graph_id`, `user_id`, `query`, `mode`, and `history`, plus three services: `ai` (a tracked model client), `progress` (a reporter for streaming), and `tools` (a `ToolAccess` handle). Every operator reaches every MCP tool through a single method:

```python
await context.tools.call_tool(tool_name, arguments, return_raw)
```

Because operators call the same `middleware/mcp/tools/` classes as the external entry points, the tool an MCP client such as Claude or ChatGPT invokes is the identical implementation an in-process operator invokes. There is one tool surface, used three ways.

Three operators ship today:

- **AnalystOperator** (`analyst`; `cypher` is accepted as its former name) — answers natural-language questions over the graph from its read-only tool surface: curated financial reads (`live-financial-statement`, `build-fact-grid`, close and mapping status), `search-documents`, `recall`, GraphQL, and read-only Cypher as the general fallback. It answers questions like "What are my five largest expense accounts this year?".
- **AuthorOperator** (`author`) — makes the change a request asks for, on top of the analyst's read tools. It writes only through an allowlist of tools whose writes are additive and reversible, or wait on a step only a person takes: `create-taxonomy-block` (metric structures; never a chart of accounts, which would replace the entity's primary chart), `create-information-block`, `update-information-block`, `assert-metrics`, `compute-metrics`, `compute-forecast`, `create-agent`, `update-agent`, `remember`, and three ledger-bound ones, each narrowed to the arguments it may use: `create-report` (it generates the report; filing and sharing it stay with a person), `update-event-block` (only to classify a captured bank-feed line; committing, voiding or editing an event stays with a person), and `promote-obligations` (always drafting the closing entries it promotes; close-period posts them). Deletes, journal-entry edits, period close and reopen, connection sync, taxonomy updates, and Cypher writes are never offered to it, so a request that needs one gets an explanation instead of a change. (Bank-feed lines exist only on a deployment that turns on `CONNECTION_MERCURY_ENABLED` or `CONNECTION_PLAID_ENABLED`, both off by default.) It asks when a request is ambiguous, reads before it writes, and reports each change it made. It is selected by name only (the auto-select path never picks it), requires the graph's write role, runs on the `quality` tier, and has a default `max_credits` of 750 when the request sets none. Its response lists each successful write in `metadata.writes` (`operation`, `id`, `name`), including when the run stops early. In the RoboLedger console, `/do <change>` runs it and shows the writes as a receipt.
- **MappingOperator** (`mapping`) — maps a chart of accounts to rs-gaap reporting concepts, in `extended` mode only; scoped to graphs carrying the `roboledger` schema extension (it will not run on a graph without it).

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

Auto-select picks the right operator; `?mode=sync` waits up to 50 seconds and returns the answer as one JSON response. A run that takes longer answers 202 with the operation's `_links` to follow.

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
| Write Cypher is a different tool | `write-graph-cypher` works on **subgraphs only**, never the parent graph (which answers `subgraph_required`) or shared repos; gated by `MCP_SUBGRAPH_OPS_ENABLED` |
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

A stack you run serves the same MCP routes at `http://localhost:8000/v1/mcp`, `http://localhost:8000/v1/mcp/roboledger` and `http://localhost:8000/v1/graphs/{graph_id}/mcp`. The OAuth routes require `MCP_OAUTH_ENABLED=true`; the per-graph route takes the key from `just demo-user` in `X-API-Key`. See [Local Development](Local-Development.md).

Operators on a stack you run can use an open-weight model in place of the Bedrock models, through any OpenAI-compatible server. Which tiers it backs, which endpoints work, and which models we tested are in [Self-Hosted AI Models](Self-Hosted-AI-Models.md).

---

## Related Documentation

**Guides:**
- **[Architecture Overview](Architecture-Overview.md)** - The OLTP/OLAP split that the three planes sit on top of
- **[Querying the Analytical Graph](Querying-the-Analytical-Graph.md)** - Cypher over LadybugDB in depth
- **[SEC Data Model & Query Rules](SEC-Data-Model-and-Query-Rules.md)** - The `sec` graph's model and the four rules for reading its numbers
- **[GraphQL Reads](GraphQL-Reads.md)** - The typed operational read surface in depth
- **[Search and AI Retrieval](Search-and-AI-Retrieval.md)** - The document index and hybrid search in depth
- **[Self-Hosted AI Models](Self-Hosted-AI-Models.md)** - Running the operators on a model you choose

**Codebase Documentation:**
- **[GraphQL Extensions](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md)** - Strawberry GraphQL surface, resolver patterns
- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Operator framework and business-logic kernel
- **[Authentication](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/auth/README.md)** - API keys, OAuth 2.1 for MCP clients, and access control
- **[API reference](https://robosystems.ai/docs/api)** - Full endpoint and tool reference with machine-readable OpenAPI spec

---

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Technical docs](Home.md)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
