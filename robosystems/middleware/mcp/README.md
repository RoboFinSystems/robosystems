# MCP Middleware

This middleware provides Model Context Protocol (MCP) integration for AI-powered graph database interactions, enabling natural language queries and intelligent data access through the RoboSystems platform.

## Overview

The MCP middleware:

- Provides MCP client implementation for graph database access
- Manages connection pooling for efficient resource usage
- Implements specialized MCP tools for graph operations
- Handles query validation and complexity management
- Integrates with the Graph API for backend communication
- Supports both shared repositories (SEC) and user graphs
- Enables subgraph navigation for isolated development environments
- Provides data operation tools for staging, querying, and graph materialization

**Operations kernel integration.** MCP tools are a **transport layer**, not a domain. Tools that touch extension OLTP data (period close, fiscal calendar, CoA→GAAP mapping, information blocks, events) delegate directly to the same `operations/roboledger/{reads,commands}/*` functions that the GraphQL resolvers and the named command operation routers call. An information block created via the `create-information-block` MCP tool goes through the exact same operations function as one created via `POST /extensions/roboledger/{g}/operations/create-information-block`. This is the single-source-of-truth contract that makes agent-driven workflows byte-identical to UI-driven workflows. Never add business logic to an MCP tool file — wire it into `operations/` and have the tool delegate.

## Architecture

```
mcp/
├── __init__.py              # Module exports and public API
├── client.py                # GraphMCPClient implementation
├── factory.py               # Client factory and pooling
├── pool.py                  # Connection pool management
├── query_validator.py       # Query validation and complexity checks
├── exceptions.py            # MCP-specific exception classes
└── tools/                   # MCP tool implementations
    ├── base_tool.py         # Base tool interface
    ├── _gate.py             # Tool gating helpers
    ├── manager.py           # GraphMCPTools — layered tool registry and dispatcher
    ├── registrar.py         # Registry-driven tool generation (infra)
    ├── constants.py         # Shared tool guidance constants
    │
    │ # Layer 1: Core tools (always available)
    ├── cypher_tool.py       # read-graph-cypher
    ├── schema_tool.py       # get-graph-schema
    ├── graphql_tool.py      # get-graphql-schema, query-graphql (EXTENSIONS_GRAPHQL_ENABLED + MCP_GRAPHQL_ENABLED)
    │
    │ # Layer 2a: Schema-extension analytical tools (roboledger gated)
    ├── example_queries_tool.py     # get-example-queries
    ├── resolve_element_tool.py     # resolve-element (canonical matching, manifest-gated)
    ├── financial_statement_tools.py # financial-statement-analysis (graph) + live-financial-statement (OLTP)
    ├── fact_grid_tool.py           # build-fact-grid (cross-company comparisons)
    │
    │ # Layer 2b: Roboledger OLTP tools (delegate to operations/roboledger/*)
    ├── fiscal_calendar_tools.py    # get-fiscal-calendar, close-period, reopen-period
    ├── schedule_tools.py           # get-period-close-status, list-period-drafts
    │                               # (schedule envelopes surface via the generic
    │                               # information_block_tools; there are no
    │                               # create/update/delete-schedule tools)
    ├── information_block_tools.py  # get-information-block, list-information-blocks
    ├── event_block_tools.py        # get/list/create-event-block (REA business events)
    ├── event_handler_tools.py      # get/list-event-handler (DSL rule rows)
    ├── agent_tools.py              # get-agent, list-agents, agent-activity (REA Agent reads)
    ├── taxonomy_tools.py           # get-unmapped-elements, suggest-mapping,
    │                               # create-mapping-association, get-mapping-summary
    ├── playbook_tools.py           # get-close-playbook (close-workflow guidance)
    ├── graph_tools.py              # create-subgraph, delete-subgraph, list-subgraphs,
    │                               # materialize, get-graph-sync-status, create-backup,
    │                               # set-write-policy, list-subgraphs
    │
    │ # Layer 3: Document search + management (SEMANTIC_SEARCH_ENABLED)
    ├── search_tools.py      # search-documents, get-document-section
    ├── document_tools.py    # create-document, update-document, get-document, list-documents
    │
    │ # Layer 4: Infrastructure (feature-flag gated)
    └── subgraph_write_tools.py  # write-graph-cypher, add-node-table, add-relationship-table (MCP_SUBGRAPH_OPS_ENABLED)
```

## Tool Layers

Tools are organized into four availability layers with conditional gating. `GraphMCPTools.__init__` (in `tools/manager.py`) assembles the tool set at construction time based on `schema_extensions`, `read_only`, the graph type (user vs shared repository), and runtime feature flags.

### Layer 1: Core (always available)

| Tool | Description |
|------|-------------|
| `read-graph-cypher` | Execute read-only Cypher queries with validation |
| `get-graph-schema` | Get complete database schema (cached 60s) |
| `get-graphql-schema` | Return extensions GraphQL schema SDL or JSON introspection (process-lifetime cache). Requires `EXTENSIONS_GRAPHQL_ENABLED=true` and `MCP_GRAPHQL_ENABLED=true`. |
| `query-graphql` | Execute a read-only GraphQL query against the extensions surface. Mutations and subscriptions rejected before execution. Complexity limits: depth 10, fields 200, aliases 20. Same gate as above. |

**GraphQL tool pattern** — call `get-graphql-schema` once per conversation to discover available types, then call `query-graphql` with parameterized queries. The graph_id always comes from the MCP context (URL-scoped), never from query arguments.

```
1. get-graphql-schema      → SDL   (once; process-lifetime cache)
2. [agent reasons]         → write a typed query against the SDL
3. query-graphql(query)    → data  (one call, nested typed response)
```

`MCP_GRAPHQL_ENABLED` is an SSM-toggleable kill switch (default: `true` when `EXTENSIONS_GRAPHQL_ENABLED=true`). Flip to `false` to disable both tools without a redeploy.

### Layer 2a: Schema-extension analytical tools

Require `roboledger` in `schema_extensions`. These tools query the LadybugDB graph (read-only, OLAP).

| Tool | Description | Additional gating |
|------|-------------|-------------------|
| `get-example-queries` | Working query patterns tailored to the graph schema | — |
| `resolve-element` | Map concepts ("revenue") to XBRL element qnames via canonical matching | Manifest flag `has_semantic_enrichment=True` |
| `financial-statement-analysis` | Graph-backed statement read (SEC + materialized tenants), auto-resolve + dedup | — |
| `live-financial-statement` | OLTP-backed ad-hoc statement from tenant's live ledger via CoA→GAAP mapping | Tenant entity graphs only (not shared repos) |
| `build-fact-grid` | Cross-company comparisons via canonical concepts | `FACT_GRID_ENABLED` |

`search-documents` results on iXBRL disclosures include `xbrl_elements` — the XBRL fact tags in that section — enabling graph cross-reference via `resolve-element` or `read-graph-cypher`.

### Layer 2b: Roboledger OLTP tools

Require `roboledger` in `schema_extensions`, `ROBOLEDGER_ENABLED=true`, the graph is not a shared repository, and the graph is not read-only. These tools delegate to `robosystems.operations.roboledger.{reads,commands}.*` — the same functions called by the `/extensions/roboledger/{g}/operations/*` REST endpoints and the GraphQL ledger resolvers.

**Fiscal calendar and period close:**

| Tool | Description | Delegates to |
|------|-------------|--------------|
| `get-fiscal-calendar` | Current close pointer, target, period state | `reads/fiscal_calendar.get_fiscal_calendar` |
| `close-period` | Close a fiscal period with balance + draft gates | `commands/fiscal_calendar.close_period` |
| `reopen-period` | Reopen a closed period with audit reason | `commands/fiscal_calendar.reopen_period` |
| `get-period-close-status` | Per-period close state and open drafts | `reads/fiscal_calendar.get_period_close_status` |

**Schedules (depreciation, amortization, accruals):**

Schedules are an Information Block `block_type`, not a separate tool
family. There are **no** `create-schedule` / `update-schedule` /
`delete-schedule` tools (no such OperationSpec is registered).

- **Reads**: use `list-information-blocks` with `block_type="schedule"` and `get-information-block`.
- **Writes**: use `create-information-block` (and `update-information-block` / `delete-information-block`) with `block_type="schedule"`.

| Tool | Description | Delegates to |
|------|-------------|--------------|
| `list-period-drafts` | List pending draft entries for a period — surfaces the QB-outbox disposition (`will_publish_to_qb` per draft + `qb_publish_count` / `local_only_count` summary) so the user sees what close will write to QuickBooks | `reads/period_drafts.list_period_drafts` |

Closing-entry drafting (schedule-derived + manual) and schedule
termination go through `create-event-block` — see the event block
section. The Python handler registry routes them to `schedule_entry_due`,
`journal_entry_recorded`, and `asset_disposed`.

**Information Block (cross-type molecular reads + writes):**

| Tool | Description | Delegates to |
|------|-------------|--------------|
| `get-information-block` | Fetch one block envelope by id | `operations/information_block/reads.get_information_block` |
| `list-information-blocks` | List envelopes, filter by block_type + category | `operations/information_block/reads.list_information_blocks` |
| `create-information-block` | Generic create (registrar-generated) | `operations/information_block/commands.create_information_block` |
| `update-information-block` | Generic update (registrar-generated) | `operations/information_block/commands.update_information_block` |
| `delete-information-block` | Generic delete (registrar-generated) | `operations/information_block/commands.delete_information_block` |

**Taxonomy and CoA→GAAP mapping:**

| Tool | Description | Delegates to |
|------|-------------|--------------|
| `get-unmapped-elements` | Find CoA elements without a GAAP mapping | `reads/taxonomies.get_unmapped_elements` |
| `suggest-mapping` | Get AI-suggested GAAP mapping for a CoA element | `commands/taxonomies.suggest_mapping` |
| `create-mapping-association` | Persist a CoA → GAAP rollup association | `commands/taxonomies.create_mapping_association` |
| `get-mapping-summary` | Coverage stats for the mapping taxonomy | `reads/taxonomies.get_mapping_summary` |

**Materialization (user graphs only):**

| Tool | Description |
|------|-------------|
| `get-graph-sync-status` | Check if the graph needs rebuild after OLTP writes |
| `materialize` | Trigger an OLTP→OLAP rebuild (replaces the legacy `materialize-graph` name) |

**Event blocks and REA agents (`event_block_tools.py`, `event_handler_tools.py`, `agent_tools.py`):**

| Tool | Description |
|------|-------------|
| `get-event-block` / `list-event-blocks` | Read REA business-event blocks |
| `create-event-block` | Record a business event (closing-entry drafting, schedule termination, etc.) |
| `get-event-handler` / `list-event-handlers` | Read tenant-configurable event-handler DSL rule rows |
| `get-agent` / `list-agents` / `agent-activity` | REA Agent (counterparty) reads — entity, list, and per-agent activity |

**Graph lifecycle (`graph_tools.py`):**

| Tool | Description |
|------|-------------|
| `create-subgraph` / `delete-subgraph` / `list-subgraphs` | Subgraph lifecycle |
| `materialize` | Trigger an OLTP→OLAP rebuild |
| `create-backup` | Create a graph backup |
| `set-write-policy` | Opt a graph into QB write-back (platform-DB connection scope) |

### Layer 3: Document search and management

Gated by `SEMANTIC_SEARCH_ENABLED`. Document management tools additionally skip shared repositories (SEC uses OpenSearch directly, no Postgres document rows).

**Search (available on all graphs including read-only):**

| Tool | Description |
|------|-------------|
| `search-documents` | Full-text keyword + semantic search across filing narratives, disclosures, and text blocks |
| `get-document-section` | Retrieve full text of a document section by ID |

**Document management (user graphs only):**

| Tool | Description | Read-only OK |
|------|-------------|--------------|
| `get-document` | Fetch document metadata + body | Yes |
| `list-documents` | List documents for a graph | Yes |
| `create-document` | Upload a new document | No |
| `update-document` | Update an existing document | No |

### Layer 4: Infrastructure (feature-flag gated)

**Subgraph navigation:**

`list-subgraphs` is the whole navigation surface. Each row carries the
graph's `connector_url`, so enumerating and addressing are one call.
`create-subgraph` returns the same URL directly, which covers the other
path — you never have to construct one.

There is no tool that changes the active graph, because there is nothing to
change: a connector is anchored to one graph by its URL. Reaching a
subgraph means adding its endpoint as its own connector. A key scoped to a
parent covers that parent's subgraphs, so a parent connector reuses its own
key; going the other way (subgraph → parent or sibling) it does not, and a
key for the target comes from the app's MCP page.

Two earlier tools tried to fill this gap and neither earned its place.
`switch-workspace` named a switch the transport never performed.
`resolve-subgraph` renamed that honestly but still only formatted a URL out
of an id `list-subgraphs` already returned — a tool whose output was a
format string over another tool's output. Both are gone; the URL moved to
where callers were already looking.

| Tool | Description | Read-only OK |
|------|-------------|--------------|
| `list-subgraphs` | Enumerate this family's graphs, each with its `connector_url` | Yes |

**Subgraph writes** (`MCP_SUBGRAPH_OPS_ENABLED`, writable subgraphs only):

| Tool | Description |
|------|-------------|
| `write-graph-cypher` | Execute write Cypher (subgraphs only — main graph is read-only) |
| `add-node-table` | Add a node table to the subgraph schema |
| `add-relationship-table` | Add a relationship table to the subgraph schema |

### Gating rules summary

| Condition | Controls |
|-----------|----------|
| `schema_extensions` contains `roboledger` | Enables all Layer 2a and 2b tools |
| `ROBOLEDGER_ENABLED=true` | Additional gate for all Layer 2b OLTP tools |
| Graph is **not** a shared repository | Materialization tools, document management write tools |
| Graph is **not** read-only | All write tools across every layer |
| Manifest `has_semantic_enrichment=True` | `resolve-element` |
| `FACT_GRID_ENABLED=true` | `build-fact-grid` |
| `SEMANTIC_SEARCH_ENABLED=true` | Layer 3 (search + document management) |
| `MCP_WORKSPACE_ENABLED=true` | Graph-lifecycle tools (`create-subgraph`, `delete-subgraph`, `create-backup`, `list-subgraphs`) |
| `MCP_SUBGRAPH_OPS_ENABLED=true` | Layer 4 subgraph write/DDL tools |

A tool that would otherwise be unavailable due to any of these gates returns a structured error via `_tool_unavailable_reason` instead of silently no-oping — clients always see a typed reason when a tool isn't mounted in a given context.

## Key Components

### 1. GraphMCPClient (`client.py`)

Main MCP client for interacting with graph databases through the RoboSystems API.

**Features:**

- HTTP-based communication with Graph API
- Automatic timeout and retry handling
- Query complexity validation
- Streaming support for large results
- Schema caching for performance

**Usage:**

```python
from robosystems.middleware.mcp import create_graph_mcp_client

# Create client with automatic endpoint discovery
client = await create_graph_mcp_client(graph_id="sec")

# Execute Cypher query
result = await client.execute_query(
    "MATCH (e:Entity) WHERE e.ticker = 'AAPL' RETURN e"
)

# Get schema information
schema = await client.get_schema()
```

### 2. Connection Pooling (`pool.py`, `factory.py`)

Efficient connection pooling to reduce initialization overhead and improve performance.

**Features:**

- Per-graph connection pools
- Configurable pool sizes and lifetimes
- Automatic cleanup of idle connections
- Connection recycling based on age
- Thread-safe pool management

**Configuration:**

```python
# Default pool settings
max_connections_per_graph: 10
max_idle_time: 300 seconds (5 minutes)
max_lifetime: 3600 seconds (1 hour)
```

**Usage:**

```python
# acquire_graph_mcp_client (the pooled context manager) is exposed from
# the factory module, not the package root. The package root exports
# create_graph_mcp_client, GraphMCPClient, and GraphMCPTools.
from robosystems.middleware.mcp.factory import acquire_graph_mcp_client

# Acquire client from pool (preferred)
async with acquire_graph_mcp_client(graph_id="kg1a2b3c") as client:
    result = await client.execute_query("MATCH (n) RETURN count(n)")
    # Client automatically returned to pool
```

For a one-off (non-pooled) client, use the package-level export:

```python
from robosystems.middleware.mcp import create_graph_mcp_client

client = await create_graph_mcp_client(graph_id="kg1a2b3c")
```

### 3. MCP Tools (`tools/`)

Graph-query tools (Layer 1, Layer 2a, Layer 4 subgraph writes) use `self.client.execute_query()` to talk to the Graph API for consistent routing, auth, and error handling. OLTP tools (Layer 2b, Layer 3 document management) open an extensions database session via `extensions_session(graph_id)` and delegate to the ops layer directly.

**Key patterns:**

- **Operations-kernel delegation**: Every roboledger OLTP tool imports from `robosystems.operations.roboledger.{reads,commands}.*` and calls those functions with an open session. Tool files contain no business logic — they're transport shims that: (1) parse MCP arguments into the Pydantic request model, (2) open the session, (3) call the ops function, (4) translate domain exceptions (`PeriodNotFoundError`, `MappingStructureNotFoundError`, etc.) into structured MCP tool errors, (5) return the Pydantic response as an MCP result. The same functions back `/extensions/roboledger/{g}/operations/*` REST endpoints and the GraphQL resolvers — three transports, one domain kernel.
- **Auto-resolve**: `financial-statement-analysis` automatically finds the latest relevant SEC filing when no `report_id` is provided (ticker + form-code resolution lives in `adapters/sec/mcp/report_resolver.py`)
- **Canonical matching**: `resolve-element` uses in-memory embedding match to map concepts to XBRL elements
- **Deduplication**: Financial tools deduplicate facts that appear in multiple filings (comparative periods)
- **Parameterized queries**: All tools use `$param` syntax to prevent injection

**Why this matters:** An agent calling `close-period` via MCP and a user closing a period via the UI both go through `operations/roboledger/commands/fiscal_calendar.close_period` — same close gate checks, same draft-entry validation, same idempotency behavior. Business rules cannot drift between transports because there is only one implementation.

### 4. Query Validation (`query_validator.py`)

Validates query complexity and enforces limits to prevent resource exhaustion.

**Features:**

- Query length validation (max 50KB)
- Complexity scoring based on query patterns
- Timeout enforcement (30 seconds default)
- Result size limits
- Protection against expensive operations

### 5. Exception Handling (`exceptions.py`)

Comprehensive exception hierarchy for MCP operations.

**Exception Classes:**

- `GraphAPIError` - Base exception for all MCP errors
- `GraphQueryTimeoutError` - Query exceeded timeout
- `GraphQueryComplexityError` - Query too complex
- `GraphValidationError` - Invalid query or parameters
- `GraphAuthenticationError` - Authentication failed
- `GraphAuthorizationError` - Insufficient permissions
- `GraphConnectionError` - Connection to Graph API failed
- `GraphResourceNotFoundError` - Resource not found
- `GraphRateLimitError` - Rate limit exceeded
- `GraphSchemaError` - Schema validation failed

## Configuration

### Environment Variables

```bash
# Graph API Connectivity
GRAPH_API_URL=http://localhost:8001    # Base URL (auto-discovered in prod)
GRAPH_HTTP_TIMEOUT=60                  # HTTP request timeout
GRAPH_QUERY_TIMEOUT=30                 # Query execution timeout

# Query Limits
GRAPH_MAX_QUERY_LENGTH=50000           # Max query size (bytes)
MCP_MAX_COMPLEXITY_SCORE=100           # Max complexity score

# Connection Pooling
MCP_POOL_MAX_CONNECTIONS=10            # Connections per graph
MCP_POOL_IDLE_TIMEOUT=300              # Idle timeout (seconds)
MCP_POOL_LIFETIME=3600                 # Connection lifetime (seconds)

# Feature Flags — MCP infrastructure
MCP_ENABLE_POOLING=true                # Enable connection pooling
MCP_ENABLE_CACHING=true                # Enable schema caching
MCP_ENABLE_VALIDATION=true             # Enable query validation

# Feature Flags — tool availability (see Gating rules summary above)
ROBOLEDGER_ENABLED=true                # Enables all Layer 2b roboledger OLTP tools (schedules, period close, taxonomy mapping, materialization). Requires `roboledger` also in the graph's schema_extensions.
ROBOINVESTOR_ENABLED=false             # Parallel flag for future roboinvestor OLTP MCP tools (no such tools exist today)
FACT_GRID_ENABLED=false                # Gates build-fact-grid tool
SEMANTIC_SEARCH_ENABLED=false          # Gates search-documents, get-document-section, and Layer 3 document management tools
MCP_WORKSPACE_ENABLED=false            # Gates Layer 4 subgraph navigation + lifecycle tools
MCP_SUBGRAPH_OPS_ENABLED=false         # Gates Layer 4 subgraph write/DDL tools (write-cypher, add-node/rel-table)
```

**Note on `schema_extensions` vs env flags:** Layer 2 tool availability is a two-factor check — the graph must declare `roboledger` in its `schema_extensions` (a per-graph property stored on the `Graph` model) **and** the runtime flag (`ROBOLEDGER_ENABLED` for Layer 2b, always-on for Layer 2a) must be true. Shared repositories like SEC have `roboledger` in their schema_extensions so Layer 2a analytical tools work against them, but they're detected as shared repos by `_is_shared_repository()` so Layer 2b OLTP tools are skipped automatically — SEC has no extensions database schema to write to.

## Integration Patterns

### With FastAPI Routes

```python
from fastapi import APIRouter
from robosystems.middleware.mcp.factory import acquire_graph_mcp_client

router = APIRouter()

@router.post("/query")
async def execute_mcp_query(
    graph_id: str,
    query: str
):
    async with acquire_graph_mcp_client(graph_id) as client:
        result = await client.execute_query(query)
        return {"results": result}
```

### With an Operator

```python
from robosystems.middleware.mcp import GraphMCPTools, create_graph_mcp_client
from robosystems.middleware.mcp.factory import acquire_graph_mcp_client

# Acquire a pooled client for the target graph
async with acquire_graph_mcp_client(graph_id="sec") as client:
    # Initialize tools — schema_extensions drives Layer 2 gating,
    # read_only drives write-tool gating. Typically sourced from
    # the Graph model in the platform database.
    tools = GraphMCPTools(
        graph_client=client,
        schema_extensions=("roboledger",),
        read_only=False,
    )

    # The Operator uses tools for natural language queries
    response = await operator.execute({
        "prompt": "What were Apple's total assets in 2023?",
        "tools": tools.get_tool_definitions(),
    })
```

An Operator that needs both graph queries and OLTP writes (e.g. a
close-workflow Operator that calls `close-period` and drafts
closing entries via `create-event-block`) initializes `GraphMCPTools`
with a writable client and `schema_extensions=("roboledger",)` so both
Layer 2a (analytical reads) and Layer 2b (OLTP commands) are available.
There is no `create-closing-entry` tool — schedule-derived and manual
drafts both come through `create-event-block`.

## Troubleshooting

### Common Issues

#### 1. Query Timeouts

- Increase `GRAPH_QUERY_TIMEOUT` for complex queries
- Optimize query patterns (use LIMIT clauses)
- Check Graph API instance health

#### 2. Connection Pool Exhausted

- Increase `MCP_POOL_MAX_CONNECTIONS`
- Check for connection leaks (missing context manager exits)
- Review pool lifetime settings

#### 3. Validation Errors

- Check query syntax (must be valid Cypher)
- Verify query length is within limits
- Review complexity score (simplify query if needed)

### Debug Mode

```python
import logging
logging.getLogger("robosystems.middleware.mcp").setLevel(logging.DEBUG)
```

## Related Documentation

- **[Operations Layer](/robosystems/operations/README.md)** - Business logic kernel that OLTP MCP tools delegate to
- **[Extensions OLTP Models](/robosystems/models/extensions/README.md)** - SQLAlchemy models backing the roboledger/roboinvestor schemas
- **[GraphQL Extensions](/robosystems/graphql/README.md)** - GraphQL read surface built on the same ops kernel MCP tools use
- **[Schemas](/robosystems/schemas/README.md)** - Graph schema definitions and `schema_extensions` naming conventions
- **[Graph API](/robosystems/graph_api/README.md)** - Underlying Graph API system
- **[Graph Middleware](/robosystems/middleware/graph/README.md)** - Graph routing layer
- **[Authentication](/robosystems/middleware/auth/README.md)** - Auth integration
- **[Configuration](/robosystems/config/README.md)** - Configuration system
