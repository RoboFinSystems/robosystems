The REST and GraphQL API for [RoboSystems](https://robosystems.ai) — an open-source, AI-native platform for accounting, financial reporting and investment management. It powers [RoboLedger](https://roboledger.ai) and [RoboInvestor](https://roboinvestor.ai).

## Two surfaces

**Platform** — `/v1/…` — graphs, queries, schema, documents, search, memory, connections, billing and access. Reads are REST `GET`s; every write is a named operation at `/operations/{name}` returning an `OperationEnvelope`, accepting an `Idempotency-Key`, and streaming progress at `/v1/operations/{id}/stream`.
→ [API reference](https://robosystems.ai/docs/api)

**Extensions** — `/extensions/…` — RoboLedger and RoboInvestor, scoped by `graph_id` in the URL. Reads are GraphQL at `POST /extensions/{graph_id}/graphql` and change nothing; writes are named operations at `/extensions/{domain}/{graph_id}/operations/{name}`; analytical views read the materialized graph.
→ [Extensions reference](https://robosystems.ai/docs/extensions) · [every GraphQL query](https://robosystems.ai/docs/extensions/graphql)

## Authenticating

`X-API-Key: rfs…` on REST, GraphQL and per-graph MCP. Browser sessions use JWTs from `POST /v1/auth/login`. The graph-agnostic MCP endpoint `/v1/mcp` takes OAuth 2.1 bearer tokens only.

## Connecting an AI client

Every graph is an MCP server at `POST /v1/graphs/{graph_id}/mcp`, and `POST /v1/mcp` serves OAuth clients that pick their graph at consent.
→ [MCP guide](https://robosystems.ai/docs/technical/ai-operators-and-mcp)

## Clients

[`robosystems-client`](https://pypi.org/project/robosystems-client/) for Python, [`@robosystems/client`](https://www.npmjs.com/package/@robosystems/client) for TypeScript.
→ [Guides](https://robosystems.ai/docs/guides) · [Technical docs](https://robosystems.ai/docs/technical)
