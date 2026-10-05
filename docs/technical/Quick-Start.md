# Quick Start

Create a RoboSystems account and API key, get a graph, and run your first authenticated query against the hosted API at `https://api.robosystems.ai`. The last step connects an AI client to the same graph over MCP, and every command below is real.

**Quick Start:** Create an account at [robosystems.ai](https://robosystems.ai/register), create an API key under **Settings → API keys**, and send it as the `X-API-Key` header to `https://api.robosystems.ai/v1/graphs`.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development).

## Overview

The path is five moves:

1. **Create an account** at robosystems.ai.
2. **Create an API key** in the app's settings.
3. **Get a graph** — create one, connect your books through RoboLedger, or subscribe to the SEC repository.
4. **Query it** with authenticated Cypher.
5. **Connect an AI client** over MCP, so Claude, ChatGPT, or any MCP client can work with the same graph.

There are two ways in. **Scripts, SDKs, and integrations** call the REST and GraphQL API with an API key in the `X-API-Key` header; that is most of this guide. **AI clients** connect over MCP and usually sign in with OAuth instead of a key (Step 5).

## Prerequisites

- A browser, to create your account and key.
- **curl** in a terminal, to follow the examples. **[jq](https://jqlang.github.io/jq/)** is optional, for reading JSON responses.
- To create a graph: an organization owner or admin role and a payment method. Creating a graph starts a subscription; see [pricing](https://robosystems.ai/pricing). To query SEC filings instead, a subscription to the SEC repository.

## Step 1: Create an Account

Sign up at [robosystems.ai/register](https://robosystems.ai/register). Your account comes with an organization, which owns your graphs, billing, and members.

The API is up whenever this health check answers. It is **unauthenticated**, so no key is needed:

```bash
curl https://api.robosystems.ai/v1/status
```

```json
{
  "status": "healthy",
  "timestamp": "2026-09-17T00:00:00Z",
  "details": {"service": "robosystems-api", "version": "..."}
}
```

**Important:** `GET /v1/status` is the only health check. `GET /health`, `GET /v1/health`, and the root `/` do **not** return health JSON — the root path serves the Swagger UI. The live OpenAPI spec is at [api.robosystems.ai/openapi.json](https://api.robosystems.ai/openapi.json), and the interactive reference is at [robosystems.ai/docs/api](https://robosystems.ai/docs/api).

## Step 2: Create an API Key

In the app, open **Settings** and find **API keys**. Choose **Create API key**, name the key, and pick its **Scope**:

- **Account-wide (all graphs)** — the key can reach every graph your account can, plus account-level routes such as `/v1/user` and `/v1/graphs`.
- One graph, picked by its name and id — the key works only on that graph and its subgraphs. Prefer this for a script or integration that needs one graph, and for an AI client that connects to a graph's MCP address with a key rather than OAuth. A graph-scoped key is refused on account-level routes, so the `/v1/user` and `/v1/graphs` calls below need an account-wide key.

The key is a string beginning with `rfs`, and it is **shown once**. Copy it into your terminal's environment:

```bash
export ROBOSYSTEMS_API_KEY=rfs...
```

Every example in this wiki reads the key from `$ROBOSYSTEMS_API_KEY`, the same variable the [RoboSystems MCP client](https://github.com/RoboFinSystems/robosystems-mcp-client) uses. Revoke a key at any time from the same page.

Confirm the key works by fetching your user:

```bash
curl -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
     https://api.robosystems.ai/v1/user
```

```json
{
  "id": "...",
  "name": "...",
  "email": "...",
  "email_verified": true,
  "accounts": []
}
```

Without a key the API answers `401` with `{"detail": "Authentication required"}`, and with a key it does not accept, `401` with `{"detail": "Invalid API key"}`. For rotation, key scopes, and the `X-API-Key` vs JWT boundary, see [Authentication and API Keys](Authentication-and-API-Keys).

## Step 3: Get a Graph

Graphs are the multi-tenant unit of data in RoboSystems. Each is a dedicated graph database identified by a `graph_id` (for example `kg1a2b3c4d5e...`) that appears in the URL path of every graph-scoped request. There are three ways to have one:

| Route | Where | What you get |
| --- | --- | --- |
| **Create a graph** | **Create Graph** on the app's Home page or in the graph selector | An empty graph you load with your own data, on the tier you choose. |
| **Connect your books** | [roboledger.ai](https://roboledger.ai) | A RoboLedger graph populated from QuickBooks. See [Connect your books](https://roboledger.ai/docs/connect-your-books). |
| **Subscribe to SEC filings** | **Repositories** in the app | Read access to the shared `sec` repository of public-company XBRL filings. See [Shared Repositories](Shared-Repositories). |

`POST /v1/graphs` creates a graph over the API too. It is asynchronous — it returns `202 Accepted` with an `OperationEnvelope` whose `operation_id` you follow to completion — and it answers `402 payment_required` if the organization has no payment method, or `403 graph_creation_requires_admin` if you are not an owner or admin. See [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy).

List your graphs with `GET /v1/graphs`:

```bash
curl -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
     https://api.robosystems.ai/v1/graphs
```

The response is a `UserGraphsResponse` whose `graphs[]` entries each carry a `graphId`, `graphName`, `role`, and more, alongside a `selectedGraphId`:

```json
{
  "graphs": [
    {
      "graphId": "kg1a2b3c4d5e...",
      "graphName": "acme_books",
      "role": "admin",
      "isSelected": true,
      "graphType": "generic",
      "createdAt": "2026-09-17T00:00:00Z"
    }
  ],
  "selectedGraphId": "kg1a2b3c4d5e..."
}
```

Export the `graphId` you want to query:

```bash
export GRAPH_ID=kg1a2b3c4d5e...
```

## Step 4: Your First Query

With a `graph_id` in hand, run authenticated Cypher at `POST /v1/graphs/{graph_id}/query/cypher`. The body is a `CypherStatementRequest`: a required `query` string plus an optional `parameters` object (use `$param` placeholders in the query for safe parameter binding) and an optional `timeout` in seconds (1–300, default 60). Its sibling, `POST /v1/graphs/{graph_id}/query/sql`, runs SQL over the same graph's columnar (DuckDB) tables — a relational lens on the same data. It takes `SELECT` statements only, stops a query at 30 seconds, returns at most 10,000 rows, and is not available on shared repositories such as `sec`.

Count nodes by label in your graph:

```bash
curl -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/query/cypher" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n) RETURN labels(n) AS label, count(*) AS count"}'
```

A newly created graph returns no rows until you load data into it. With an SEC subscription, the same call against the `sec` repository counts real nodes:

```bash
curl -X POST "https://api.robosystems.ai/v1/graphs/sec/query/cypher" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (e:Entity) RETURN count(e) AS entities"}'
```

A parameterized query passes values through `parameters` rather than string-interpolating them:

```bash
curl -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/query/cypher" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n) WHERE n.name = $name RETURN n LIMIT 5", "parameters": {"name": "Operating Expenses"}}'
```

**Note:** Main graphs are **read-only** through `/query/cypher` — you load data through ingestion (file uploads, connectors, or integrations), and only subgraphs accept writes. See [Querying the Analytical Graph](Querying-the-Analytical-Graph) for the query surface and [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy) for the read-vs-write model.

## Step 5: Connect an AI Client

AI clients reach the same graphs through RoboSystems' MCP server. The simplest connection uses OAuth: add one URL, sign in to RoboSystems, and pick the graph on the consent screen. No API key is involved.

```text
https://api.robosystems.ai/v1/mcp
```

In Claude Code:

```bash
claude mcp add --transport http robosystems https://api.robosystems.ai/v1/mcp
```

The app's **MCP** page builds a ready-made connector for your graph, and [AI Operators and MCP](AI-Operators-and-MCP) covers every client — Claude, ChatGPT, Cursor, VS Code — along with the per-graph address, `https://api.robosystems.ai/v1/graphs/{graph_id}/mcp`, for clients that cannot run OAuth: they send an API key in the `X-API-Key` header, never in the URL, and a key scoped to that one graph is the one to give them.

## Load Your Data

An empty graph becomes useful once data flows into it. Pick the route that matches your source:

- **Files** — upload Parquet, CSV, or JSON node and relationship data into a custom graph: [File Uploads](File-Uploads).
- **Documents** — add policies, procedures, and notes as markdown for search and AI retrieval: [Document Management](Document-Management).
- **Accounting data** — connect QuickBooks through [RoboLedger](https://roboledger.ai/docs), or write ledger entries through [RoboLedger Operations](RoboLedger-Operations).
- **Your own systems** — the SDKs, [`robosystems-client`](https://pypi.org/project/robosystems-client/) for Python and [`@robosystems/client`](https://www.npmjs.com/package/@robosystems/client) for TypeScript, and the patterns in [Building Custom Integrations](Building-Custom-Integrations).
- **Next:** post events from your own system into a ledger with [Build a Ledger Integration](Build-a-Ledger-Integration), and read [Operations Contract](Operations-Contract) for the envelope and retry rules every write shares.

## Troubleshooting

### `401 Unauthorized`

**Solution:** Confirm you are sending the `X-API-Key` header (not `Authorization: Bearer`) and that `$ROBOSYSTEMS_API_KEY` is set in this terminal. A key revoked in **Settings → API keys** no longer works, and a key scoped to one graph answers `401` on account-level routes such as `/v1/user`.

```bash
curl -H "X-API-Key: $ROBOSYSTEMS_API_KEY" https://api.robosystems.ai/v1/user
```

### `403 Forbidden` on a Graph

**Solution:** Your account has no access to that graph, or the key was created for a different single graph. Check `GET /v1/graphs` for the graphs your account can reach, and the key's scope in **Settings → API keys**.

### `402 Payment Required` When Creating a Graph

**Solution:** The organization has no payment method on file. An owner adds one in the app, then retries.

### Health Check Returns 404

**Solution:** You are hitting the wrong path. The only health endpoint is `GET /v1/status`. `GET /health`, `GET /v1/health`, and `/` will not return health JSON.

### Write Query Rejected on a Main Graph

**Solution:** Main graphs are read-only through `/query/cypher`. Load data through ingestion, or run writes against a subgraph. See [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy).

## Related Documentation

**Wiki Guides:**

- **[Core Concepts](Core-Concepts)** - The vocabulary: graphs, tiers, blocks, operators, operational vs analytical
- **[Authentication and API Keys](Authentication-and-API-Keys)** - Key scopes, rotation, and the `X-API-Key` vs JWT boundary
- **[Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy)** - `graph_id`, main graphs vs subgraphs, read-vs-write semantics, repositories
- **[AI Operators and MCP](AI-Operators-and-MCP)** - Connecting AI clients and the retrieval planes behind them
- **[Local Development](Local-Development)** - Run the whole platform on your own machine

**Reference:**

- **[API reference](https://robosystems.ai/docs/api)** - API reference with machine-readable OpenAPI spec
- **[RoboLedger Docs](https://roboledger.ai/docs)** - Product guides for running your books with RoboLedger

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
