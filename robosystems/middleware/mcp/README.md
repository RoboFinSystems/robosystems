# MCP Middleware

Model Context Protocol support: a pooled client for talking to a graph
(`client.py`, `factory.py`, `pool.py`), query validation
(`query_validator.py`), and the tool set that AI Operators and external MCP
clients call (`tools/`). A tool set is assembled per graph, so what an agent
sees depends on that graph's schema extensions, its read-only status, whether
it's a shared repository, and the runtime feature flags.

**MCP tools are a transport, not a domain.** Every tool that touches extension
OLTP data delegates to the same `operations/roboledger/{reads,commands}/*`
functions the GraphQL resolvers and the REST command routers call. An
information block created through `create-information-block` goes through the
identical operations function as one created through
`POST /extensions/roboledger/{g}/operations/create-information-block` — same
validation, same gates, same idempotency. Never put business logic in a tool
file; wire it into `operations/` and have the tool delegate.

## Client

```python
from robosystems.middleware.mcp.factory import acquire_graph_mcp_client

async with acquire_graph_mcp_client(graph_id="kg1a2b3c") as client:
    result = await client.execute_query("MATCH (n) RETURN count(n)")
```

`acquire_graph_mcp_client` is the pooled context manager and lives on
`factory`, not the package root. The package root exports
`create_graph_mcp_client` (one-off, unpooled), `GraphMCPClient`,
`GraphMCPTools`, and the exception hierarchy.

The pool (`pool.py`) is per-`graph_id`, defaulting to 10 connections per graph.
Idle timeout and max lifetime come from `TuningConfig` and are SSM-tunable:
`mcp/POOL_IDLE_TIMEOUT` (300 s) and `mcp/POOL_MAX_LIFETIME` (3600 s). A pooled
client never outlives the endpoint it was built against, which is what keeps a
recycled Graph API instance from being held by a stale connection.

Exceptions all derive from `GraphAPIError`: `GraphQueryTimeoutError`,
`GraphQueryComplexityError`, `GraphValidationError`, `GraphAuthenticationError`,
`GraphAuthorizationError`, `GraphConnectionError`, `GraphResourceNotFoundError`,
`GraphRateLimitError`, `GraphSchemaError`.

`query_validator.py` checks Cypher before it is sent and scores complexity from
query shape — unbounded `LIMIT`, multiple `MATCH` clauses, `ORDER BY` counts,
and similar. A score above 50 produces a warning; the validator's hard rejects
are for syntax and write-operation violations, not for the score. Result size
is bounded separately by `MCPDefaults` (1000 rows, 5 MB) to protect the model's
context window.

## Tools

`GraphMCPTools.__init__` (`tools/manager.py`) builds the tool set from
`schema_extensions`, `read_only`, whether the graph is a shared repository, and
feature flags. A tool excluded by a gate returns a structured error via
`_tool_unavailable_reason` rather than silently vanishing, so clients always get
a typed reason.

```python
from robosystems.middleware.mcp import GraphMCPTools
from robosystems.middleware.mcp.factory import acquire_graph_mcp_client

async with acquire_graph_mcp_client(graph_id="sec") as client:
    tools = GraphMCPTools(
        graph_client=client,
        schema_extensions=("roboledger",),
        read_only=False,
    )
    definitions = tools.get_tool_definitions()
```

`schema_extensions` and `read_only` normally come from the `Graph` row in the
platform database.

Graph-query tools go through `self.client.execute_query()` so routing, auth, and
error handling stay consistent. OLTP tools open a session with
`extensions_session(graph_id)` and call the ops layer directly. All tools use
`$param` placeholders — never string interpolation.

`tools/instructions.py` generates the per-graph `instructions` string returned
in the MCP handshake. Shared repositories use an authored string from their
manifest (`agent_instructions`); tenant graphs get a generated one. It names
tool *families* rather than restating schemas, because it occupies the agent's
context for the whole session.

### Layer 1 — core, always available

| Tool | Description |
|------|-------------|
| `read-graph-cypher` | Read-only Cypher with validation |
| `get-graph-schema` | Full database schema (cached 60 s) |
| `get-graphql-schema` | Extensions GraphQL SDL or JSON introspection (process-lifetime cache) |
| `query-graphql` | Read-only GraphQL against the extensions surface; mutations and subscriptions rejected before execution; depth 10, 200 fields, 20 aliases |

The GraphQL pair requires `EXTENSIONS_GRAPHQL_ENABLED=true` **and**
`MCP_GRAPHQL_ENABLED=true`. The latter is an SSM kill switch that disables both
tools without a redeploy.

The intended pattern is one schema call per conversation, then typed queries
against it:

```
get-graphql-schema   → SDL (once, cached for the process lifetime)
query-graphql(query) → data (one call, nested typed response)
```

`graph_id` always comes from the MCP context (the URL scopes the connector). It
is never a query argument.

### Layer 2a — analytical graph reads

Require `roboledger` in `schema_extensions`. These read LadybugDB (OLAP).

| Tool | Description | Extra gate |
|------|-------------|------------|
| `get-example-queries` | Query patterns tailored to this graph's schema | — |
| `resolve-element` | Map a concept ("revenue") to XBRL element qnames | manifest `has_semantic_enrichment=True` |
| `financial-statement-analysis` | Graph-backed statement read with auto-resolve and dedup | — |
| `live-financial-statement` | Statement from the tenant's live OLTP ledger via CoA→GAAP mapping | tenant graphs only |
| `build-fact-grid` | Cross-company comparison over canonical concepts | `FACT_GRID_ENABLED` |

`financial-statement-analysis` resolves the latest relevant SEC filing when no
`report_id` is given (ticker and form-code resolution live in
`adapters/sec/mcp/report_resolver.py`), and deduplicates facts that appear in
multiple filings as comparative periods.

### Layer 2b — roboledger OLTP

Require `roboledger` in `schema_extensions`, `ROBOLEDGER_ENABLED=true`, a graph
that is not a shared repository, and a graph that is not read-only. Each tool
delegates to `robosystems.operations.roboledger.{reads,commands}.*`.

| Tool | Delegates to |
|------|--------------|
| `get-fiscal-calendar` | `reads/fiscal_calendar.get_fiscal_calendar` |
| `close-period` | `commands/fiscal_calendar.close_period` |
| `reopen-period` | `commands/fiscal_calendar.reopen_period` |
| `get-period-close-status` | `reads/fiscal_calendar.get_period_close_status` |
| `list-period-drafts` | `reads/period_drafts.list_period_drafts` |
| `get-information-block` | `operations/information_block/reads.get_information_block` |
| `list-information-blocks` | `operations/information_block/reads.list_information_blocks` |
| `create-information-block` | `operations/information_block/commands.create_information_block` |
| `update-information-block` | `operations/information_block/commands.update_information_block` |
| `delete-information-block` | `operations/information_block/commands.delete_information_block` |
| `get-unmapped-elements` | `reads/taxonomies.get_unmapped_elements` |
| `suggest-mapping` | `commands/taxonomies.suggest_mapping` |
| `create-mapping-association` | `commands/taxonomies.create_mapping_association` |
| `get-mapping-summary` | `reads/taxonomies.get_mapping_summary` |

Plus REA reads and event writes: `get-event-block` / `list-event-blocks` /
`create-event-block`, `get-event-handler` / `list-event-handlers` (tenant
event-handler DSL rows), and `get-agent` / `list-agents` / `agent-activity`
(REA Agent counterparty reads). `get-close-playbook` returns close-workflow
guidance. `bind-text-block` binds a Document to a disclosure element — it is
hand-written rather than registrar-generated because it needs both the platform
session (Documents live there) and the tenant extensions session.

`list-period-drafts` surfaces the QuickBooks outbox disposition per draft
(`will_publish_to_qb`, with `qb_publish_count` / `local_only_count` summary), so
the user can see what closing the period will push to QuickBooks.

**Schedules are not a tool family.** A schedule is an Information Block
`block_type`. Read them with `list-information-blocks(block_type="schedule")`
and `get-information-block`; write them with the generic
`create-information-block` / `update-information-block` /
`delete-information-block`. There is no `create-schedule` tool and no such
OperationSpec is registered. Closing-entry drafting (schedule-derived and
manual) and schedule termination go through `create-event-block`, which the
Python handler registry routes to `schedule_entry_due`,
`journal_entry_recorded`, and `asset_disposed`.

### Layer 3 — documents and search

Gated by `SEMANTIC_SEARCH_ENABLED`.

| Tool | Read-only graphs |
|------|------------------|
| `search-documents` | yes |
| `get-document-section` | yes |
| `get-document` / `list-documents` | yes |
| `create-document` / `update-document` | no |

Document management additionally skips shared repositories — SEC searches
OpenSearch directly and has no Postgres document rows. `search-documents`
results on iXBRL disclosures carry `xbrl_elements`, the XBRL fact tags in that
section, which cross-reference into the graph via `resolve-element` or
`read-graph-cypher`.

### Layer 4 — graph lifecycle, subgraph writes, memory

Graph lifecycle (`graph_tools.py`, `MCP_WORKSPACE_ENABLED`): `create-subgraph`,
`delete-subgraph`, `list-subgraphs`, `create-backup`, `materialize`,
`get-graph-sync-status`, `set-write-policy`.

Subgraph writes (`MCP_SUBGRAPH_OPS_ENABLED`, writable subgraphs only):
`write-graph-cypher`, `add-node-table`, `add-relationship-table`. The main graph
is read-only from MCP; these apply to subgraphs.

Semantic memory (`SEMANTIC_MEMORY_ENABLED` plus the sub-gate
`MCP_SEMANTIC_MEMORY_ENABLED`): `remember`, `recall`, `update-memory`, `forget`
— a per-graph LanceDB vector store giving an Operator a stateful memory layer,
adapting over the `MemoryService` kernel. This is distinct from
`subgraph_write_tools.py`, which builds structural knowledge graphs.

**Navigating between graphs.** `list-subgraphs` is the entire navigation
surface: each row carries the graph's `connector_url`, so enumerating and
addressing are one call, and `create-subgraph` returns the same URL directly.
No tool changes the active graph, because a connector is anchored to one graph
by its URL — reaching a subgraph means adding its endpoint as its own
connector. A key scoped to a parent covers that parent's subgraphs, so a parent
connector reuses its key; going subgraph → parent or sibling it does not, and a
key for the target comes from the app's MCP page.

### Gating summary

| Condition | Controls |
|-----------|----------|
| `roboledger` in `schema_extensions` | all Layer 2a and 2b tools |
| `ROBOLEDGER_ENABLED=true` | additional gate on Layer 2b |
| graph is not a shared repository | Layer 2b, document management, materialization |
| graph is not read-only | every write tool in every layer |
| manifest `has_semantic_enrichment=True` | `resolve-element` |
| `FACT_GRID_ENABLED` | `build-fact-grid` |
| `SEMANTIC_SEARCH_ENABLED` | Layer 3 |
| `EXTENSIONS_GRAPHQL_ENABLED` + `MCP_GRAPHQL_ENABLED` | GraphQL tools |
| `MCP_WORKSPACE_ENABLED` | graph-lifecycle tools |
| `MCP_SUBGRAPH_OPS_ENABLED` | subgraph write/DDL tools |
| `SEMANTIC_MEMORY_ENABLED` + `MCP_SEMANTIC_MEMORY_ENABLED` | memory tools |

**`schema_extensions` and the env flag are both required.** Availability is a
two-factor check: the graph must declare `roboledger` in its `schema_extensions`
(a per-graph property on the `Graph` model) *and* the runtime flag must be true.
Shared repositories like SEC declare `roboledger` so Layer 2a analytical tools
work against them, but `_is_shared_repository()` skips Layer 2b — SEC has no
extensions database schema to write to.

## Configuration

```bash
GRAPH_API_URL=http://localhost:8001   # auto-discovered in prod
GRAPH_HTTP_TIMEOUT=60
GRAPH_QUERY_TIMEOUT=30

ROBOLEDGER_ENABLED=true
ROBOINVESTOR_ENABLED=false
EXTENSIONS_GRAPHQL_ENABLED=true
MCP_GRAPHQL_ENABLED=true
FACT_GRID_ENABLED=false
SEMANTIC_SEARCH_ENABLED=false
SEMANTIC_MEMORY_ENABLED=false
MCP_SEMANTIC_MEMORY_ENABLED=false
MCP_WORKSPACE_ENABLED=false
MCP_SUBGRAPH_OPS_ENABLED=false
```

The feature flags are SSM parameters, so they take effect at runtime without a
redeploy:

```bash
just ssm-list prod features
just ssm-set prod features/MCP_SUBGRAPH_OPS_ENABLED true
```

Pool and result limits are not env vars — they resolve through `TuningConfig`
(`mcp/POOL_IDLE_TIMEOUT`, `mcp/POOL_MAX_LIFETIME`) and `MCPDefaults` in
`config/defaults.py`.

## Debugging

```python
import logging
logging.getLogger("robosystems.middleware.mcp").setLevel(logging.DEBUG)
```

A tool that "doesn't exist" is almost always a gate: check the graph's
`schema_extensions`, its read-only flag, whether it's a shared repository, and
the relevant SSM flag, in that order. Pool exhaustion is usually a missing
`async with` — the context manager is what returns the client.

## Related

- [`../../operations/README.md`](../../operations/README.md) — the kernel OLTP tools delegate to
- [`../../graphql/README.md`](../../graphql/README.md) — the GraphQL read surface over the same kernel
- [`../../models/extensions/README.md`](../../models/extensions/README.md) — OLTP models
- [`../../schemas/README.md`](../../schemas/README.md) — `schema_extensions` conventions
- [`../../graph_api/README.md`](../../graph_api/README.md) — the Graph API this client calls
- [`../graph/README.md`](../graph/README.md) — graph routing
- [`../auth/README.md`](../auth/README.md) — the MCP `?token=` door and key scoping
