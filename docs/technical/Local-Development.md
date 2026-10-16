# Local Development

Run the open-source RoboSystems platform on your own machine to develop against it, contribute to it, or evaluate it before deploying your own. This guide takes you from an empty checkout to a running stack with a demo user, an API key, a populated graph, and your first authenticated query in about ten minutes.

**Quick Start:** Run `just start`, then `just demo-user`, then `just demo-custom-graph`, and you have a live graph to query at `http://localhost:8000`.

Every API guide in these docs is written against the hosted platform at `https://api.robosystems.ai`, and every example in them works against a local stack too: swap the base URL for `http://localhost:8000` and use the key from `just demo-user`. The table below lists the other differences.

## Local vs Hosted

| | Hosted ([robosystems.ai](https://robosystems.ai)) | Local stack |
| --- | --- | --- |
| **API base URL** | `https://api.robosystems.ai` | `http://localhost:8000` |
| **API reference** | [robosystems.ai/docs/api](https://robosystems.ai/docs/api) — a page per operation | `http://localhost:8000/docs` redirects to `/docs/api` on the local robosystems-app (`ROBOSYSTEMS_URL`, `http://localhost:3000`), which renders your stack's own spec while the app runs |
| **Try a call in the browser** | [api.robosystems.ai](https://api.robosystems.ai) — Swagger UI | `http://localhost:8000/` — Swagger UI, against your own stack |
| **Account and API key** | Sign up, then **Settings → API keys** | `just demo-user` writes both to `.local/config.json` |
| **Graphs** | **Create Graph** in the app (needs a payment method) | `just demo-*` recipes provision and populate them |
| **Billing and credits** | On | Off by default (`BILLING_ENABLED=false` in `.env`) |
| **SEC repository** | Paid subscription, from **Repositories** in the app | Free: load the filings you want with `just sec-load` |
| **GraphQL** | Queries and introspection; no browser playground | The same, plus the GraphiQL playground when you open the endpoint in a browser |
| **Graph API** (Cypher without platform auth) | Not exposed | `http://localhost:8001`, used by `just graph-query` |
| **Dagster UI** | Not exposed | `http://localhost:8002` |
| **Object storage** | Amazon S3 | LocalStack at `http://localhost:4566` |
| **OpenSearch** | Not exposed | `http://localhost:9200` |
| **Admin CLI** | Platform operators only | `just admin dev ...` |

## Prerequisites

Before starting, ensure you have:

- **Docker** running locally (Docker Desktop or equivalent), with at least 8 GB of memory allocated.
- **[uv](https://docs.astral.sh/uv/)** for Python package management (the demo scripts run under `uv`).
- **[just](https://github.com/casey/just)** for the task recipes used throughout this guide.
- **[jq](https://jqlang.github.io/jq/)** to read your API key out of `.local/config.json`. On macOS: `brew install jq`.
- A clone of the [robosystems repository](https://github.com/RoboFinSystems/robosystems) and a terminal at its root.

**On Windows?** Set up WSL2 first and clone the repo inside the Linux filesystem — see [Windows Setup (WSL2)](Windows-Setup-with-WSL2.md). Every command below then works verbatim.

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

**First run**: `just start` pulls the published images and runs database migrations, so it takes a few minutes. The demos take 1-2 minutes the first time.

**Subsequent runs**: The demos reuse the existing user and graph (~20 seconds).

The rest of this guide walks through each step, explains what is happening, and shows the underlying HTTP calls.

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

The same profile also runs PostgreSQL (`5432`), Valkey (`6379`), LocalStack (`4566`), and OpenSearch (`9200`).

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

**Important:** `GET /v1/status` is the only health check. `GET /health`, `GET /v1/health`, and the root `/` do **not** return health JSON — the root path serves the Swagger UI, where you can run a call against your own stack. The live OpenAPI spec is at `http://localhost:8000/openapi.json`, and `/docs` redirects to `ROBOSYSTEMS_URL/docs/api` — the local robosystems-app at `http://localhost:3000/docs/api`, which answers once you run the app (see [Web Apps](#web-apps)).

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
just demo-user --name QA --email qa@example.com
just demo-user --json           # Machine-readable output
```

For everything about API keys — creation, rotation, the `X-API-Key` vs JWT distinction — see [Authentication and API Keys](Authentication-and-API-Keys.md).

## Step 3: Export Your API Key

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

The API guides in these docs send the key as `$ROBOSYSTEMS_API_KEY`. Export it from the credentials file so their examples run unchanged apart from the base URL:

```bash
export ROBOSYSTEMS_API_KEY=$(jq -r .api_key .local/config.json)
```

Run this again in each new terminal. The `graphs` object starts as `{}` and is populated once a demo provisions a graph (Step 5).

## Step 4: Your First Authenticated Request

Every authenticated request uses the `X-API-Key` header. Confirm your key works by fetching the current user:

```bash
curl -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
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

**Important:** Backend testing with `curl` uses the `X-API-Key` header, not `Authorization: Bearer`. Bearer/JWT auth is a frontend concern (it rides in an HTTP-only cookie).

## Step 5: Provision a Graph

Graphs are the multi-tenant unit of data in RoboSystems. Each is identified by a `graph_id` (for example `kg1a2b3c4d5e...`) that appears in the URL path of every graph-scoped request.

The quickest route is to let a demo recipe provision a graph for you. Creating a graph directly is asynchronous — `POST /v1/graphs` returns `202 Accepted` with an `OperationEnvelope` and an `operation_id` you have to follow over Server-Sent Events — so for a ten-minute start, use a demo that handles provisioning and data ingestion end to end:

```bash
# Creates a graph, generates sample data, ingests it, and writes
# the graph_id into .local/config.json under "graphs"
just demo-custom-graph
```

This builds the People / Companies / Projects graph described in [Custom Graph Schema](Custom-Graph-Schema.md). After it runs, the new graph appears in your credentials file and in the API.

To list your graphs through the API, call `GET /v1/graphs`:

```bash
curl -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
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

**Note:** Reading the `graphId` from `GET /v1/graphs` is the most reliable way to pick one up, because it does not depend on the exact nesting inside `.local/config.json`. Export it for the next step:

```bash
export GRAPH_ID=kg1a2b3c4d5e...
```

Other demos provision other kinds of graph: `just demo-roboledger` builds a RoboLedger graph from synthetic data (see [RoboLedger Demos](RoboLedger-Demo-Walkthrough.md)), and `just demo-roboinvestor` a RoboInvestor one. To learn how graphs, subgraphs, repositories, and tenancy fit together, see [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy.md).

## Step 6: Your First Query

With a `graph_id` in hand, run authenticated Cypher through the platform API at `POST /v1/graphs/{graph_id}/query/cypher`. The body is a `CypherStatementRequest`: a required `query` string plus an optional `parameters` object (use `$param` placeholders in the query for safe parameter binding) and an optional `timeout` in seconds (1–300, default 60). Its sibling, `POST /v1/graphs/{graph_id}/query/sql`, runs read-only SQL over the same graph's columnar (DuckDB) tables — a relational lens on the same data.

Count nodes by label in your graph:

```bash
curl -X POST "http://localhost:8000/v1/graphs/$GRAPH_ID/query/cypher" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n) RETURN labels(n) AS label, count(*) AS count"}'
```

A parameterized query, passing values through `parameters` rather than string-interpolating them:

```bash
curl -X POST "http://localhost:8000/v1/graphs/$GRAPH_ID/query/cypher" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name, p.age", "parameters": {"min_age": 40}}'
```

**Note:** Main graphs are **read-only** through `/query/cypher` — you load data through the ingestion pipeline (which is exactly what `just demo-custom-graph` does). Only subgraphs accept writes. See [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy.md) for the read-vs-write model.

## Local-Only Tools

These work only against a stack you run. None of them go through platform authentication, billing, or access control, so use them for debugging, not for integration code.

**Cypher and SQL without platform auth.** The `graph-query` and `tables-query` recipes call the Graph API on port 8001 directly; `lbug-query` opens the LadybugDB database file and bypasses the API entirely:

```bash
just graph-query "$GRAPH_ID" "MATCH (n) RETURN count(n)"
just lbug-query "$GRAPH_ID" "MATCH (n) RETURN count(n)"
```

**GraphiQL.** Open `http://localhost:8000/extensions/{graph_id}/graphql` in a browser to explore the extensions schema interactively. The playground is mounted only in development; hosted, the endpoint serves queries and introspection but no playground. See [GraphQL Reads](GraphQL-Reads.md).

**Dagster.** The orchestration UI at [localhost:8002](http://localhost:8002) shows every pipeline run, including QuickBooks syncs, SEC processing, and materialization, with step logs for failures.

**Admin CLI.** `just admin dev --help` lists the operator command groups, such as subscriptions, credits, graphs, users, orgs, and cache. On a hosted or self-deployed environment these are run by the platform operator, not by API users.

**Logs.**

```bash
just logs api               # API logs
just logs worker            # Background worker logs
just logs dagster-daemon    # Dagster daemon logs
```

## SEC Filings

The hosted SEC repository is a paid subscription. Locally it is free: load the filings you want and query them through the same `sec` graph id the hosted examples use. The demo user still needs a subscription record to reach `sec`, so create one the first time.

```bash
just sec-load <TICKER> 2025   # Load one company's filings for a year
just demo-sec-subscribe       # Give the demo user access to sec (no data load)
just sec-health               # Check the local SEC database
just sec-reset                # Start over
```

`just demo-sec` does all of it in one step: loads filings, subscribes the demo user, and runs example queries.

`just sec-dump` pulls the published SEC database dump from Hugging Face instead of processing filings yourself. It is large: tens of GiB to download and more than 100 GiB on disk. The pipeline behind `sec-load` is described in [SEC XBRL Pipeline](SEC-XBRL-Pipeline.md).

## Web Apps

The RoboSystems, RoboLedger, and RoboInvestor web apps are open source too, each in its own repository ([robosystems-app](https://github.com/RoboFinSystems/robosystems-app), [roboledger-app](https://github.com/RoboFinSystems/roboledger-app), [roboinvestor-app](https://github.com/RoboFinSystems/roboinvestor-app)). Clone one next to this repository, copy its `.env.example` to `.env` (it already points at `http://localhost:8000`), and run `npm install` then `npm run dev`. They serve on `http://localhost:3000`, `http://localhost:3001`, and `http://localhost:3002`. Sign in with the demo user's email and password, which `just demo-user` prints and saves in `.local/config.json`.

**Run the web apps in Docker.** To skip the Node toolchain, `just clone-apps` clones all three repositories next to this one, and `just start apps build` builds their images and starts them on the same three ports, pointed at your local API. In this profile the landing page at `/` shows a maintenance screen; sign in at `/login`.

To connect a real QuickBooks company to a local stack, see [Connecting QuickBooks Locally](Connecting-QuickBooks-Locally.md).

## AI Models

The stack reaches a language model in two ways, and you can run both on open-weight models you host, so no part of the platform depends on a proprietary AI service. MCP clients bring their own model — the client that connects to your stack's MCP routes decides what drives the tools. The operators, the server-side AI, call Claude through the `AWS_BEDROCK_*` credentials in `.env` by default, and can be pointed at any server that speaks the OpenAI Chat Completions API with tool calling:

```bash
OPENAI_COMPAT_ENABLED=true
OPENAI_COMPAT_BASE_URL=https://integrate.api.nvidia.com/v1   # or vLLM, Ollama, LM Studio, another catalog
OPENAI_COMPAT_MODEL=z-ai/glm-5.3-flash
OPENAI_COMPAT_API_KEY=...                                    # blank if the server needs none
OPERATOR_PROFILE_BALANCED=openai-compat                      # a tier you leave unmapped stays on Bedrock
```

Then run `just rebuild`, because a plain restart does not pick up environment changes. For the rest of the settings, which endpoints work, which models we tested, and what to check before pointing it at real books, see **[Self-Hosted AI Models](Self-Hosted-AI-Models.md)**.

## Restarting and Iterating

As you make changes, choose the right restart command:

| Command | Use when |
| --- | --- |
| **`just restart`** | You changed Python code. Source is volume-mounted, so a container restart picks it up. |
| **`just rebuild`** | You changed dependencies, environment variables, or a Dockerfile. A plain restart will **not** pick these up. |
| **`just upgrade`** | You pulled new commits and want the latest published images, recreating only what changed. |
| **`just reset-local`** | You edited the taxonomy framework source (`frameworks/**/taxonomy.jsonld`). It tears the stack down, wipes local data, rebuilds, and reseeds the library; validate the edit first with `just framework-validate`. |

```bash
just restart   # Python code changes
just rebuild   # dependency / env / Dockerfile changes
```

Before opening a pull request, run the same checks CI runs:

```bash
just test       # Unit tests
just test-all   # Unit tests plus linting, formatting, and type checks
```

## Deploying Your Own

Running locally is the first half of self-hosting. The repository also ships the GitHub Actions workflows and CloudFormation templates that deploy the full platform into your own AWS account — see the [Bootstrap Guide](Bootstrap-Guide.md).

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

The first `just start` pulls images and runs migrations, so allow a few minutes before the health check responds.

### Health Check Returns 404

**Solution:** You are hitting the wrong path. The only health endpoint is `GET /v1/status`. `GET /health`, `GET /v1/health`, and `/` will not return health JSON.

```bash
curl http://localhost:8000/v1/status
```

### `401 Unauthorized` on an Authenticated Request

**Solution:** Confirm you are sending the `X-API-Key` header (not `Authorization: Bearer`) and that `$ROBOSYSTEMS_API_KEY` is set in this terminal:

```bash
export ROBOSYSTEMS_API_KEY=$(jq -r .api_key .local/config.json)
curl -H "X-API-Key: $ROBOSYSTEMS_API_KEY" http://localhost:8000/v1/user
```

### An Example From Another Page Fails Locally

**Solution:** Check the [Local vs Hosted](#local-vs-hosted) table. Examples that use the `sec` graph need filings loaded with `just sec-load`, and examples that depend on an existing graph need one provisioned by a demo recipe.

### Write Query Rejected on a Main Graph

**Solution:** Main graphs are read-only through `/query/cypher`. Load data through the ingestion pipeline (the demos do this), or run writes against a subgraph. See [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy.md).

### Code or Environment Changes Are Not Taking Effect

**Solution:** Use `just restart` for Python code changes and `just rebuild` for dependency, environment, or Dockerfile changes — a plain restart does not pick up env changes.

## Related Documentation

**Guides:**

- **[Quick Start](Quick-Start.md)** - The same first steps against the hosted platform
- **[Windows Setup (WSL2)](Windows-Setup-with-WSL2.md)** - Run this guide on Windows
- **[Self-Hosted AI Models](Self-Hosted-AI-Models.md)** - Run the operators on an open-weight model instead of Bedrock
- **[Custom Graph Schema](Custom-Graph-Schema.md)** - The full version of the graph provisioned in Step 5
- **[RoboLedger Demos](RoboLedger-Demo-Walkthrough.md)** - The synthetic Cascade demo and the Driftline and Cadence showcase scenarios
- **[Architecture Overview](Architecture-Overview.md)** - Platform API vs Graph API, the two-database split, and the components behind `just start`
- **[Bootstrap Guide](Bootstrap-Guide.md)** - Deploy the platform into your own AWS account

**Codebase Documentation:**

- **[Examples](https://github.com/RoboFinSystems/robosystems/blob/main/examples/README.md)** - The demo scripts behind `just demo-user` and `just demo-custom-graph`
- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business workflow orchestration in the codebase

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Technical docs](Home.md)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
