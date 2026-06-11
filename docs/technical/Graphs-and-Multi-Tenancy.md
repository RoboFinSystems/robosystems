# Graphs & Multi-Tenancy

RoboSystems is multi-tenant at the graph layer: every customer dataset lives in its own isolated graph database keyed by a `graph_id`. This page explains the `graph_id` model, the available graph tiers, subgraphs, and the day-to-day tasks of creating, listing, and querying graphs.

## Overview

A **graph** in RoboSystems is a single, isolated LadybugDB database. Each graph has a unique identifier, `graph_id`, that scopes every request to exactly one tenant's data. There is no shared table space across tenants and no cross-tenant data access at the storage layer.

Two stores work together:

- **Platform metadata** (users, organizations, billing, the graph registry) lives in PostgreSQL.
- **Business data** (entities, transactions, facts, custom nodes and relationships) lives in the LadybugDB graph itself.

The multi-tenancy model has a few load-bearing properties:

1. **One graph = one isolated database.** Each graph is its own embedded LadybugDB database. No shared tables, no cross-tenant leakage at the data layer.
2. **The `graph_id` is the tenant boundary.** Authentication and per-graph access are validated by FastAPI dependencies before any handler runs. The `graph_id` is always a URL path parameter, never a query argument.
3. **Two kinds of graphs.** Customer/entity graphs hold your own data; **shared repositories** (such as `sec`) are platform-managed and read-only for everyone.
4. **Tiers are dedicated instances.** A graph runs on a dedicated EC2 instance sized by tier. Subgraphs share that same instance and its resource budget.
5. **Only AI operations consume credits.** Database operations (queries, ingestion, backups) are free. Subgraphs draw from their parent graph's credit pool.

## Prerequisites

Before working with graphs locally, ensure you have:

- Docker running locally
- RoboSystems development environment set up
- Services started with `just start`
- Demo credentials created with `just demo-user` (writes your API key and a `graph_id` to `.local/config.json`)

All authenticated examples below read the API key from `.local/config.json` and target `http://localhost:8000`. Use the `X-API-Key` header for backend testing; `Authorization: Bearer` is a frontend concern only.

## The `graph_id` Model and Per-Graph Isolation

The `graph_id` is the primary multi-tenant identifier. It appears as a URL path parameter on every graph-scoped route, and the platform resolves it to a specific LadybugDB database before the handler executes.

### ID Formats

| Kind | Format | Example |
| --- | --- | --- |
| **Parent graph** | `kg` + 16 or more hex characters | `kg1234567890abcdef` |
| **Subgraph** | `{parent_id}_{subgraph_name}` | `kg1234567890abcdef_dev` |
| **Shared repository** | Fixed reserved name | `sec` |

The parent graph identifier follows the regex `kg[a-f0-9]{16,}`. A subgraph appends an underscore and an alphanumeric name (`[a-zA-Z0-9]{1,20}`) to its parent's ID, so the parent is always recoverable from the subgraph ID.

### Why URL-Scoped Tenancy

Because the `graph_id` lives in the URL path, the tenant scope is unambiguous and is checked by middleware before your request reaches business logic. This applies uniformly across REST, GraphQL, and MCP:

- **REST**: `POST /v1/graphs/{graph_id}/query`
- **GraphQL**: `POST /extensions/{graph_id}/graphql` — the `graph_id` comes from the URL, so queries do **not** take a `graphId` argument. Write `{ entity { … } }`, not `{ entity(graphId: "kg_x") { … } }`.
- **MCP**: tools read the `graph_id` from connection context rather than as a tool argument.

## Graph Tiers

A tier determines the instance size, storage budget, subgraph capacity, backup limits, and API rate multiplier for a graph. RoboSystems uses **instance-based naming** so the tier name reflects exactly what infrastructure you get.

| Tier (technical) | Display | Instance | Max subgraphs | Storage | API rate multiplier | Backup retention |
| --- | --- | --- | --- | --- | --- | --- |
| **`ladybug-standard`** | Standard | m7g.large (8 GB, 2 vCPU) | 3 | 20 GB | 1.0x | 7 days |
| **`ladybug-large`** | Large | r7g.large (16 GB, 2 vCPU) | 10 | 50 GB | 1.5x | 30 days |
| **`ladybug-xlarge`** | XLarge | r7g.xlarge (32 GB, 4 vCPU) | 25 | 100 GB | 2.5x | 90 days |
| **`ladybug-shared`** | Shared Repository | r7g.2xlarge (64 GB) | Platform-managed | — | — | 90 days |

Notes:

- **A graph and its subgraphs share one instance.** On `ladybug-standard`, that is 1 parent plus up to 3 subgraphs (4 databases total) on a single m7g.large. The subgraph cap, storage budget, and RAM are shared across the whole instance.
- **`ladybug-shared` is platform-managed and not user-creatable.** It backs read-only public repositories such as the `sec` corpus. You consume it through queries; you do not provision it.
- **Only Standard, Large, and XLarge are creatable tiers.** The `instance_tier` field on graph creation accepts `ladybug-standard`, `ladybug-large`, or `ladybug-xlarge`.

Do not use marketing names such as Professional, Enterprise, or Premium — the canonical names are the technical tier identifiers above.

## Creating a Graph

Graphs are created with `POST /v1/graphs`. Creation is **asynchronous**: the call returns `202 Accepted` with an `OperationEnvelope` carrying an `operation_id`, and the graph is provisioned in the background.

There are two flavors of graph:

- **Entity graphs** — pre-wired for a financial entity (the default for RoboLedger). Supply an `initial_entity` and any `schema_extensions` (such as `roboledger`).
- **Custom graphs** — your own node and relationship model. Supply a `custom_schema`. See [Custom Graph Schema](Custom-Graph-Schema) for the full how-to.

### Quick Start

The fastest path to a working custom graph is the demo command, which creates a user, provisions a graph, ingests sample data, and runs verification queries:

```bash
# Ensure RoboSystems is running
just start

# Create user, graph, sample data, and run queries
just demo-custom-graph
```

### Create an Entity Graph

This creates a RoboLedger-enabled entity graph on the Standard tier. The `Idempotency-Key` header makes retries safe.

```bash
API_KEY=$(jq -r .api_key .local/config.json)

curl -X POST "http://localhost:8000/v1/graphs" \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: $(date +%s)" \
  -H "Content-Type: application/json" \
  -d '{
    "metadata": {
      "graph_name": "Acme Consulting LLC",
      "description": "Professional consulting services",
      "schema_extensions": ["roboledger"]
    },
    "instance_tier": "ladybug-standard",
    "initial_entity": {
      "name": "Acme Consulting LLC",
      "uri": "https://acmeconsulting.com",
      "ein": "12-3456789",
      "state_of_incorporation": "Delaware",
      "entity_type": "llc"
    },
    "create_entity": true,
    "tags": ["consulting"]
  }'
```

**Note:** The response is an `OperationEnvelope` with an `operation_id`, not a finished graph. Track progress over Server-Sent Events at `GET /v1/operations/{operation_id}/stream`. The `entity_type` (such as `llc`, `corporation`, or `partnership`) drives the default reporting style for entity graphs.

For the full request and response schema — every field, type, and validation rule — see the live OpenAPI docs rather than re-deriving it here: [api.robosystems.ai/docs](https://api.robosystems.ai/docs) (or `http://localhost:8000/docs` locally).

## Listing and Inspecting Graphs

### List Your Graphs

`GET /v1/graphs` returns the graphs you can access, along with available shared repositories.

```bash
curl "http://localhost:8000/v1/graphs" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

### Inspect a Single Graph

`GET /v1/graphs/{graph_id}/info` returns database details for one graph. Locally you can use the `just` shortcuts:

```bash
# Database info (node/edge counts, size, status)
just graph-info kg1234567890abcdef

# Graph API health check
just graph-health
```

## Subgraphs

A **subgraph** is an isolated child database that lives on the same instance as its parent. Subgraphs are useful for separating environments (dev / staging / prod) or for forking a copy of a parent's data.

### What a Subgraph Shares (and Doesn't)

| Property | Behavior |
| --- | --- |
| **Data** | Fully isolated from the parent and from sibling subgraphs |
| **Credit pool** | Shared with the parent graph |
| **Permissions** | Inherited from the parent graph |
| **Instance / RAM / storage** | Shared with the parent (same EC2 instance) |
| **Schema** | Inherits the parent's `schema_extensions` by default |

### Naming and ID Rules

- **Name**: alphanumeric only, 1–20 characters, no hyphens or underscores. The name is normalized to lowercase. `dev`, `staging`, and `prod1` are valid; `dev-1` and `my_env` are rejected.
- **ID format**: `{parent_id}_{subgraph_name}`. A subgraph named `dev` under `kg1234567890abcdef` becomes `kg1234567890abcdef_dev`.
- **Single-level only**: you cannot create a subgraph of a subgraph, and shared repositories cannot have subgraphs.
- **Capacity**: enforced per tier (3 / 10 / 25 for Standard / Large / XLarge). Exceeding the cap returns `403`.

### Create a Subgraph

Subgraph creation is a graph operation and returns an `OperationEnvelope`. By default it creates an empty subgraph; set `fork_parent: true` to clone the parent's data.

```bash
curl -X POST "http://localhost:8000/v1/graphs/kg1234567890abcdef/operations/create-subgraph" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "dev",
    "display_name": "Development Environment",
    "description": "Sandbox for testing",
    "fork_parent": false
  }'
```

The resulting subgraph ID is `kg1234567890abcdef_dev`.

### List Subgraphs

`GET /v1/graphs/{graph_id}/subgraphs` returns the parent's subgraphs along with the tier's `max_subgraphs`, the current `subgraph_count`, and total size.

```bash
curl "http://localhost:8000/v1/graphs/kg1234567890abcdef/subgraphs" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

Subgraph lifecycle writes (create, delete) follow the same operation pattern as other graph operations. See [Graph Operations](Graph-Operations) for the complete operations surface.

## Querying a Graph

Graphs are queried with Cypher over `POST /v1/graphs/{graph_id}/query`.

```bash
curl -X POST "http://localhost:8000/v1/graphs/kg1234567890abcdef/query" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n) RETURN labels(n) AS label, count(*) AS count"}'
```

Locally, the `just` shortcuts wrap the same path:

```bash
# Query through the Graph API
just graph-query kg1234567890abcdef "MATCH (n) RETURN count(n)"

# Query LadybugDB directly (bypasses the API — local debugging only)
just lbug-query kg1234567890abcdef "MATCH (n) RETURN count(n)"
```

**Important:** The **main graph is read-only over Cypher** — its data arrives through the staging and materialization pipeline, not through write queries. **Subgraphs support full writes.**

### Querying with MCP

Any MCP-compatible AI tool (Claude Desktop, Claude Code, Cursor, Cline, and others) can query a graph through the RoboSystems MCP server. The `graph_id` comes from the connection's context, so it is never passed as a tool argument. The graph-facing tools are:

- `get-graph-schema` — view available node and relationship types (run this first)
- `read-graph-cypher` — run read-only Cypher queries
- `get-example-queries` — get sample queries for the graph

`read-graph-cypher` is read-only: `CREATE`, `SET`, `DELETE`, `MERGE`, and `DROP`, along with `CALL db.` and `CALL apoc.`, are blocked. For the GraphQL plane (typed extensions reads), use `get-graphql-schema` and `query-graphql`. See [Custom Graph Schema](Custom-Graph-Schema) for a worked MCP example.

## Troubleshooting

### Subgraph Creation Fails: "Maximum subgraphs limit reached"

You have hit the tier's subgraph cap (3 / 10 / 25 for Standard / Large / XLarge).

**Solution:** Delete an unused subgraph, or change the parent graph to a higher tier with a `change-tier` operation. See [Graph Operations](Graph-Operations).

### Subgraph Creation Fails: "Subgraphs are not available"

The graph's tier reports no subgraph capacity, or you are attempting to add a subgraph to a shared repository or to another subgraph.

**Solution:** Subgraphs are single-level and live only under creatable tiers. Confirm you are operating on a parent graph (`kg…`, no underscore) on a tier that allows subgraphs.

### Create Returns `202` But the Graph Isn't Ready

Graph creation is asynchronous.

**Solution:** Read the `operation_id` from the returned `OperationEnvelope` and stream progress at `GET /v1/operations/{operation_id}/stream`. The graph is usable once the operation completes.

### Invalid Subgraph Name

Names must be alphanumeric, 1–20 characters, with no hyphens or underscores.

**Solution:** Replace separators. Use `dev1` instead of `dev-1`, or `myenv` instead of `my_env`.

### Cypher Write Rejected on the Main Graph

The main graph is read-only over Cypher.

**Solution:** Write to a subgraph (which supports full writes), or load data into the main graph through the staging and materialization pipeline.

## Related Documentation

**Wiki Guides:**

- **[Graph Operations](Graph-Operations)** - Lifecycle operations (create-subgraph, delete-subgraph, change-tier, backups, materialize) and the CQRS operation envelope
- **[Authentication and API Keys](Authentication-and-API-Keys)** - API key creation, the `X-API-Key` header, and per-graph access control
- **[Custom Graph Schema](Custom-Graph-Schema)** - Designing node and relationship schemas and querying a custom graph

**Codebase Documentation:**

- **[Graph Routing](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/graph/README.md)** - Graph ID parsing, subgraph resolution, and multi-tenant routing
- **[Authentication](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/auth/README.md)** - Authentication system internals
- **[Graph API](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graph_api/README.md)** - LadybugDB backend and query execution

**API Reference:**

- **[API Documentation](https://api.robosystems.ai/docs)** - Full endpoint and schema reference with machine-readable OpenAPI spec

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
