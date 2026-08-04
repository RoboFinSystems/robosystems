# Quick Start

This guide takes you from an empty checkout to a running RoboSystems stack with a demo user, an API key, a populated graph, and your first authenticated query — in about ten minutes. Every command below is real and runnable against a local stack.

**Quick Start:** Run `just start`, then `just demo-user`, then `just demo-custom-graph`, and you have a live graph to query at `http://localhost:8000`.

## Overview

The happy path is four moves:

1. **Start the stack** — `just start` brings up the API, Graph API, databases, and orchestration with one command.
2. **Create a demo user and API key** — `just demo-user` writes your credentials to `.local/config.json`.
3. **Provision a graph** — `just demo-custom-graph` creates a graph database, generates sample data, and ingests it.
4. **Query it** — make your first authenticated request with the `X-API-Key` header, then run Cypher against your graph.

You interact with two surfaces. The **platform API** at `http://localhost:8000` handles authentication, graph lifecycle, billing, and authenticated Cypher queries — this is the happy path. A lower-level **Graph API** at `http://localhost:8001` runs Cypher directly without platform auth; the `just graph-query` recipe uses it for quick convenience checks.

## Prerequisites

Before starting, ensure you have:

- **Docker** running locally (Docker Desktop or equivalent), with at least 8 GB of memory allocated.
- **[uv](https://docs.astral.sh/uv/)** for Python package management (the demo scripts run under `uv`).
- **[just](https://github.com/casey/just)** for the task recipes used throughout this guide.
- **[jq](https://jqlang.github.io/jq/)** to read your API key out of `.local/config.json`. On macOS: `brew install jq`.
- A clone of the [robosystems repository](https://github.com/RoboFinSystems/robosystems) and a terminal at its root.

**On Windows?** Set up WSL2 first and clone the repo inside the Linux filesystem — see [Windows Setup (WSL2)](Windows-Setup-with-WSL2). Every command below then works verbatim.

## Quick Start

The fastest path from a clean checkout to a queryable graph:

```bash
# 1. Start the full stack (API, Graph API, databases, orchestration)
just start

# 2. Create a demo user and API key (writes .local/config.json)
just demo-user

# 3. Provision a graph, generate data, and ingest it
just demo-custom-graph
```

**First run**: `just start` pulls and builds images and runs database migrations, so it takes a few minutes. The demos take 1-2 minutes the first time.

**Subsequent runs**: The demos reuse the existing user and graph (~20 seconds).

The rest of this guide walks through each step, explains what is happening, and shows the underlying HTTP calls so you can integrate against the API directly.

## Step 1: Start the Stack

`just start` auto-creates `.env` and `.env.local` from their `.example` templates, then brings the `robosystems` Docker Compose profile up in the background:

```bash
just start
```

Once the containers are healthy, three local URLs are available:

| Service | URL | Purpose |
| --- | --- | --- |
| **Platform API** | `http://localhost:8000` | Auth, graph lifecycle, authenticated Cypher |
| **Graph API** | `http://localhost:8001` | Lower-level Cypher (no platform auth) |
| **Dagster UI** | `http://localhost:8002` | Orchestration dashboard |

Verify the API is up with the health check. This endpoint is **unauthenticated**, so no key is needed:

```bash
curl http://localhost:8000/v1/status
```

```json
{
  "status": "healthy",
  "timestamp": "2026-06-11T00:00:00Z",
  "details": {"service": "robosystems-api", "version": "..."}
}
```

**Important:** `GET /v1/status` is the only health check. `GET /health`, `GET /v1/health`, and the root `/` do **not** return health JSON — the root path serves the Swagger UI. The live OpenAPI spec is at `http://localhost:8000/openapi.json`, and the interactive docs are at `http://localhost:8000/docs`.

## Step 2: Create a Demo User and API Key

`just demo-user` creates a user in the platform database, generates an API key, and stores everything in `.local/config.json`:

```bash
just demo-user
```

This prints your credentials and a ready-to-run test command, including the API key (a string beginning with `rfs`):

```text
API Key: rfs...

Test API Call:
  curl -H "X-API-Key: rfs..." http://localhost:8000/v1/user
```

**Note:** `.local/config.json` does **not** exist until you run `just demo-user` — there is no committed template to copy from. Any command that reads the key out of that file will fail until this step completes.

Useful flags:

```bash
just demo-user --force          # Force a brand-new user and key
just demo-user --name "QA User" --email qa@example.com
just demo-user --json           # Machine-readable output
```

For everything about API keys — creation, rotation, the `X-API-Key` vs JWT distinction — see [Authentication and API Keys](Authentication-and-API-Keys).

## Step 3: Read Your API Key from `.local/config.json`

The credentials file holds your key at the top-level `api_key` field. Its shape:

```json
{
  "user": {"id": "...", "name": "Demo User", "email": "..."},
  "user_id": "...",
  "email": "...",
  "password": "...",
  "api_key": "rfs...",
  "base_url": "http://localhost:8000",
  "created_at": "YYYY-MM-DD HH:MM:SS",
  "graphs": {}
}
```

Read it inline with `jq` whenever you make a request:

```bash
jq -r .api_key .local/config.json
```

**Important:** Always read the key from `.local/config.json` with `jq` rather than pasting a literal key or stashing it in a `TOKEN="..."` shell variable — shell variables do not persist reliably across `curl` invocations. The `graphs` object starts as `{}` and is populated once a demo provisions a graph (Step 5).

## Step 4: Your First Authenticated Request

Every authenticated local request uses the `X-API-Key` header against `http://localhost:8000`. Confirm your key works by fetching the current user:

```bash
curl -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
     http://localhost:8000/v1/user
```

```json
{
  "id": "...",
  "name": "Demo User",
  "email": "...",
  "email_verified": false,
  "accounts": []
}
```

**Important:** Local API testing always uses the `X-API-Key` header — never `Authorization: Bearer`. Bearer/JWT auth is a frontend concern (it rides in an HTTP-only cookie); for backend `curl` testing, `X-API-Key` is the credential.

## Step 5: Provision a Graph

Graphs are the multi-tenant unit of data in RoboSystems. Each is identified by a `graph_id` (for example `kg1a2b3c4d5e...`) that appears in the URL path of every graph-scoped request.

The honest happy path is to let a demo recipe provision a graph for you. Creating a graph directly is asynchronous — `POST /v1/graphs` returns `202 Accepted` with an `OperationEnvelope` and an `operation_id` you have to follow over Server-Sent Events — so for a ten-minute start, use a demo that handles provisioning and data ingestion end to end:

```bash
# Creates a graph, generates sample data, ingests it, and writes
# the graph_id into .local/config.json under "graphs"
just demo-custom-graph
```

This builds the People / Companies / Projects graph described in [Custom Graph Schema](Custom-Graph-Schema). After it runs, the new graph appears in your credentials file and in the API.

To list your graphs through the API, call `GET /v1/graphs`:

```bash
curl -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
     http://localhost:8000/v1/graphs
```

The response is a `UserGraphsResponse` whose `graphs[]` entries each carry a `graphId`, `graphName`, `role`, and more, alongside a `selectedGraphId`. Copy the `graphId` you want to query:

```json
{
  "graphs": [
    {
      "graphId": "kg1a2b3c4d5e...",
      "graphName": "custom_graph_demo",
      "role": "admin",
      "isSelected": true,
      "graphType": "generic",
      "createdAt": "2026-06-11T00:00:00Z"
    }
  ],
  "selectedGraphId": "kg1a2b3c4d5e..."
}
```

**Note:** Reading the `graphId` from `GET /v1/graphs` is the most reliable way to pick one up, because it does not depend on the exact nesting inside `.local/config.json`. Set it as a shell variable for the next step:

```bash
GRAPH_ID=kg1a2b3c4d5e...
```

To learn how graphs, subgraphs, repositories, and tenancy fit together, see [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy).

## Step 6: Your First Query

With a `graph_id` in hand, run authenticated Cypher through the platform API at `POST /v1/graphs/{graph_id}/query`. The body is a `CypherQueryRequest`: a required `query` string plus an optional `parameters` object (use `$param` placeholders in the query for safe parameter binding).

Count nodes by label in your graph:

```bash
curl -X POST "http://localhost:8000/v1/graphs/$GRAPH_ID/query" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n) RETURN labels(n) AS label, count(*) AS count"}'
```

A parameterized query, passing values through `parameters` rather than string-interpolating them:

```bash
curl -X POST "http://localhost:8000/v1/graphs/$GRAPH_ID/query" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name, p.age", "parameters": {"min_age": 40}}'
```

**Note:** Main graphs are **read-only** through `/query` — you load data through the ingestion pipeline (which is exactly what `just demo-custom-graph` does). Only subgraphs accept writes. See [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy) for the read-vs-write model.

For quick, throwaway checks you can bypass platform auth and hit the Graph API directly with the `just graph-query` recipe:

```bash
just graph-query "$GRAPH_ID" "MATCH (n) RETURN count(n)"
```

This is a convenience path for local debugging only — it does not run through platform authentication, billing, or access control. Real integrations use the authenticated `POST /v1/graphs/{graph_id}/query` endpoint.

The full endpoint surface, request and response schemas, and parameter details live in the OpenAPI docs at `http://localhost:8000/docs` (or the hosted reference at [api.robosystems.ai/docs](https://api.robosystems.ai/docs)).

## Restarting and Iterating

As you make changes, choose the right restart command:

| Command | Use when |
| --- | --- |
| **`just restart`** | You changed Python code. Source is volume-mounted, so a container restart picks it up. |
| **`just rebuild`** | You changed dependencies, environment variables, or a Dockerfile. A plain restart will **not** pick these up. |

```bash
just restart   # Python code changes
just rebuild   # dependency / env / Dockerfile changes
```

## Troubleshooting

### `jq: error: Could not open file .local/config.json`

**Solution:** The credentials file does not exist until you create a demo user. Run:

```bash
just demo-user
```

### Connection Refused on `localhost:8000`

**Solution:** The stack is not up, or is still starting. Bring it up and confirm the containers are running:

```bash
just start
docker ps   # Verify the robosystems containers are healthy
```

The first `just start` builds images and runs migrations, so allow a few minutes before the health check responds.

### Health Check Returns 404

**Solution:** You are hitting the wrong path. The only health endpoint is `GET /v1/status`. `GET /health`, `GET /v1/health`, and `/` will not return health JSON.

```bash
curl http://localhost:8000/v1/status
```

### `401 Unauthorized` on an Authenticated Request

**Solution:** Confirm you are sending the `X-API-Key` header (not `Authorization: Bearer`) and that the key matches `.local/config.json`:

```bash
curl -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
     http://localhost:8000/v1/user
```

### Write Query Rejected on a Main Graph

**Solution:** Main graphs are read-only through `/query`. Load data through the ingestion pipeline (the demos do this), or run writes against a subgraph. See [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy).

### Code or Environment Changes Are Not Taking Effect

**Solution:** Use `just restart` for Python code changes and `just rebuild` for dependency, environment, or Dockerfile changes — a plain restart does not pick up env changes.

## Related Documentation

**Wiki Guides:**

- **[Authentication and API Keys](Authentication-and-API-Keys)** - Create, rotate, and use API keys; `X-API-Key` vs JWT
- **[Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy)** - `graph_id`, main graphs vs subgraphs, read-vs-write semantics, repositories
- **[Custom Graph Schema](Custom-Graph-Schema)** - The full version of the graph provisioned in Step 5
- **[Architecture Overview](Architecture-Overview)** - Platform API vs Graph API, the two-database split, and the components behind `just start`

**Codebase Documentation:**

- **[Examples](https://github.com/RoboFinSystems/robosystems/blob/main/examples/README.md)** - The demo scripts behind `just demo-user` and `just demo-custom-graph` in codebase
- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business workflow orchestration in codebase
- **[API Documentation](https://api.robosystems.ai/docs)** - API reference with machine-readable OpenAPI spec

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
