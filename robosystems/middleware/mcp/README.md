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
- Enables workspace management for isolated development environments
- Provides data operation tools for staging, querying, and graph materialization

**Operations kernel integration.** MCP tools are a **transport layer**, not a domain. Tools that touch extension OLTP data (ledger schedules, period close, fiscal calendar, CoA→GAAP mapping, reports) delegate directly to the same `operations/roboledger/{reads,commands}/*` functions that the GraphQL resolvers and the named command operation routers call. A schedule created via `create-schedule` MCP tool goes through the exact same `commands/schedules.py` function as one created via `POST /extensions/roboledger/{g}/operations/create-schedule`. This is the single-source-of-truth contract that makes agent-driven workflows byte-identical to UI-driven workflows. Never add business logic to an MCP tool file — wire it into `operations/` and have the tool delegate.

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
    ├── manager.py           # GraphMCPTools — layered tool registry and dispatcher
    ├── constants.py         # Tool name constants
    │
    │ # Layer 1: Core tools (always available)
    ├── cypher_tool.py       # read-graph-cypher
    ├── schema_tool.py       # get-graph-schema
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
    │                               # (create-schedule + siblings are registrar-
    │                               # generated; schedule envelopes now surface via
    │                               # information_block_tools)
    ├── information_block_tools.py  # get-information-block, list-information-blocks
    ├── taxonomy_tools.py           # get-unmapped-elements, suggest-mapping,
    │                               # create-mapping-association, get-mapping-summary
    ├── materialization_tools.py    # get-graph-sync-status, materialize-graph
    │
    │ # Layer 3: Document search + management (SEMANTIC_SEARCH_ENABLED)
    ├── search_tools.py      # search-documents, get-document-section
    ├── document_tools.py    # create-document, update-document, get-document, list-documents
    │
    │ # Layer 4: Infrastructure (feature-flag gated)
    ├── workspace.py         # create/delete/list/switch-workspace (MCP_WORKSPACE_ENABLED)
    └── memory_tools.py      # write-graph-cypher, add-node-table, add-relationship-table (MCP_MEMORY_ENABLED)
```

## Tool Layers

Tools are organized into four availability layers with conditional gating. `GraphMCPTools.__init__` (in `tools/manager.py`) assembles the tool set at construction time based on `schema_extensions`, `read_only`, the graph type (user vs shared repository), and runtime feature flags.

### Layer 1: Core (always available)

| Tool | Description |
|------|-------------|
| `read-graph-cypher` | Execute read-only Cypher queries with validation |
| `get-graph-schema` | Get complete database schema (cached 60s) |

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

Schedule reads (list, get facts) moved to the generic Information Block
surface — use `list-information-blocks` with `block_type="schedule"`
and `get-information-block` instead. Schedule-specific writes stay
registered for wire compatibility and operate through the unified
envelope machine under the hood.

| Tool | Description | Delegates to |
|------|-------------|--------------|
| `create-schedule` | Create a schedule structure with pre-generated facts | `commands/schedules.create_schedule` |
| `update-schedule` | Rename or edit schedule mechanics | `commands/schedules.update_schedule` |
| `delete-schedule` | Remove a schedule | `commands/schedules.delete_schedule` |
| `list-period-drafts` | List pending draft entries for a period | `reads/period_drafts.list_period_drafts` |

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
| `materialize-graph` | Trigger the `mark_graph_stale` sensor to rebuild |

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

**Workspaces** (`MCP_WORKSPACE_ENABLED`):

| Tool | Description | Read-only OK |
|------|-------------|--------------|
| `list-workspaces` | List available workspaces | Yes |
| `switch-workspace` | Switch active workspace context | Yes |
| `create-workspace` | Create subgraph workspace | No |
| `delete-workspace` | Delete workspace | No |

**Memory / write Cypher** (`MCP_MEMORY_ENABLED`, writable graphs only):

| Tool | Description |
|------|-------------|
| `write-graph-cypher` | Execute write Cypher (subgraphs / memory graphs) |
| `add-node-table` | Create staging table for nodes |
| `add-relationship-table` | Create staging table for relationships |

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
| `MCP_WORKSPACE_ENABLED=true` | Layer 4 workspace tools |
| `MCP_MEMORY_ENABLED=true` | Layer 4 memory / write-cypher tools |

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
from robosystems.middleware.mcp import acquire_graph_mcp_client

# Acquire client from pool (recommended)
async with acquire_graph_mcp_client(graph_id="kg1a2b3c") as client:
    result = await client.execute_query("MATCH (n) RETURN count(n)")
    # Client automatically returned to pool
```

### 3. MCP Tools (`tools/`)

Graph-query tools (Layer 1, Layer 2a, Layer 4 memory) use `self.client.execute_query()` to talk to the Graph API for consistent routing, auth, and error handling. OLTP tools (Layer 2b, Layer 3 document management) open an extensions database session via `extensions_session(graph_id)` and delegate to the ops layer directly.

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
- `LadybugDBSchemaError` - Schema validation failed

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
MCP_WORKSPACE_ENABLED=false            # Gates Layer 4 workspace tools
MCP_MEMORY_ENABLED=false               # Gates Layer 4 memory / write-cypher tools
```

**Note on `schema_extensions` vs env flags:** Layer 2 tool availability is a two-factor check — the graph must declare `roboledger` in its `schema_extensions` (a per-graph property stored on the `Graph` model) **and** the runtime flag (`ROBOLEDGER_ENABLED` for Layer 2b, always-on for Layer 2a) must be true. Shared repositories like SEC have `roboledger` in their schema_extensions so Layer 2a analytical tools work against them, but they're detected as shared repos by `_is_shared_repository()` so Layer 2b OLTP tools are skipped automatically — SEC has no extensions database schema to write to.

## Integration Patterns

### With FastAPI Routes

```python
from fastapi import APIRouter, Depends
from robosystems.middleware.mcp import acquire_graph_mcp_client

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

### With Agent System

```python
from robosystems.middleware.mcp import acquire_graph_mcp_client, GraphMCPTools

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

    # Agent uses tools for natural language queries
    response = await agent.execute({
        "prompt": "What were Apple's total assets in 2023?",
        "tools": tools.get_tool_definitions(),
    })
```

Agents that need both graph queries and OLTP writes (like `CloseAgent`, which calls `create-closing-entry` and `close-period`) initialize `GraphMCPTools` with a writable client and `schema_extensions=("roboledger",)` so both Layer 2a (analytical reads) and Layer 2b (OLTP commands) are available.

## Troubleshooting

### Common Issues

**1. Query Timeouts**

- Increase `GRAPH_QUERY_TIMEOUT` for complex queries
- Optimize query patterns (use LIMIT clauses)
- Check Graph API instance health

**2. Connection Pool Exhausted**

- Increase `MCP_POOL_MAX_CONNECTIONS`
- Check for connection leaks (missing context manager exits)
- Review pool lifetime settings

**3. Validation Errors**

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
