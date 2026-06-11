# Authentication & API Keys

This guide shows you how to authenticate against a local RoboSystems stack from scripts, `curl`, MCP clients, and SDKs. The one rule to internalize up front: backend and programmatic access uses the `X-API-Key` header, while JWT/Bearer tokens are a frontend concern.

## Two Authentication Methods at a Glance

RoboSystems has two doors into the same building. Both resolve to the same `User`; they differ in who carries the credential and how it is issued.

| Method | Who uses it | Header | Where it comes from | Lifecycle |
| --- | --- | --- | --- | --- |
| **API key** | Scripts, `curl`, MCP clients, SDKs, CI | `X-API-Key` | `just demo-user` writes one to `.local/config.json`; create more via `POST /v1/user/api-keys` | Long-lived; you revoke or expire it explicitly |
| **JWT / Bearer** | The browser frontends (robosystems-app, roboledger-app, roboinvestor-app) | `Authorization: Bearer …` | `POST /v1/auth/login` issues a token | Short-lived, auto-refreshed by the frontend session layer |

For everything in this guide — `curl`, MCP, SDK smoke tests — use the API key. You never need to call `login` for backend testing.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Getting Your Local API Key](#getting-your-local-api-key)
- [The X-API-Key Path](#the-x-api-key-path)
- [Worked Examples](#worked-examples)
- [API Key Lifecycle](#api-key-lifecycle)
- [Graph-Scoped Access](#graph-scoped-access)
- [The JWT / Bearer Boundary](#the-jwt--bearer-boundary)
- [Single Sign-On (SSO)](#single-sign-on-sso)
- [Troubleshooting](#troubleshooting)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Overview

Authentication in RoboSystems is credential-in-header, validated by a FastAPI dependency before any handler runs. There is exactly one route you can hit anonymously — `GET /v1/status`, the health probe — and everything else requires a valid credential. A request carries either an `X-API-Key` header (programmatic) or an `Authorization: Bearer` header (frontend session); both resolve to the same authenticated `User`.

Access is layered. A valid credential authenticates *you*; access to a specific `graph_id` is checked separately by the graph-scoped dependency. That distinction drives the most important status-code rule on the platform: **401 means "no/invalid credential," 403 means "valid user, no access to that graph."** Keep them straight and most auth debugging resolves itself.

The full endpoint surface — request/response schemas, every field, every status code — lives in the live OpenAPI docs at [http://localhost:8000/docs](http://localhost:8000/docs) (or the hosted [API Documentation](https://api.robosystems.ai/docs)). This page covers the concepts and the day-to-day tasks; it does not re-tabulate the endpoint surface.

## Prerequisites

- A running local stack. See [Quick Start](Quick-Start) for `just start` and the full setup. The API listens on `http://localhost:8000`.
- `jq` installed (used to read the key out of `.local/config.json` inline).
- A demo user with an API key — created by `just demo-user` (next section).

## Getting Your Local API Key

Run the demo-user recipe from the `robosystems/` directory. It creates (or reuses) a demo user, issues an API key, and writes everything to `.local/config.json`:

```bash
just demo-user
```

The resulting `.local/config.json` looks like this:

```json
{
  "user": { "id": "...", "name": "...", "email": "..." },
  "user_id": "...",
  "email": "demo_user_...@example.com",
  "password": "...",
  "api_key": "rfs<64 hex chars>",
  "base_url": "http://localhost:8000",
  "created_at": "YYYY-MM-DD HH:MM:SS",
  "graphs": { "<slot>": { "graph_id": "kg..." } }
}
```

The two fields you read most often:

- `api_key` — your `X-API-Key` credential. Read it inline with `jq -r .api_key .local/config.json`.
- `graphs.<slot>.graph_id` — a graph identifier, for graph-scoped requests. Read it with `jq -r '.graphs.<slot>.graph_id' .local/config.json`.

**Important:** Read the key inline from the config file rather than copy-pasting a literal. Do not stash it in a shell variable like `TOKEN="..."` — that does not persist reliably into `curl` calls. The inline `$(jq -r .api_key .local/config.json)` form is the canonical pattern across all RoboSystems docs.

## The X-API-Key Path

For all programmatic access, send your key in the `X-API-Key` header:

```bash
-H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

The header name is exactly `X-API-Key` (case-insensitive per HTTP, but use this spelling). On each request the auth dependency resolves the key to a `User`, updates the key's `last_used_at`, and attaches the user to the request context. The same header works for REST endpoints, the graph-scoped GraphQL endpoint, and MCP clients.

**Precedence — Bearer wins.** If a request carries *both* an `Authorization: Bearer` header and an `X-API-Key` header, the Bearer token is tried first and takes precedence. If a Bearer header is present it must validate or the request is rejected with 401 — the server does **not** fall back to the API key. The API key path is only taken when no Bearer header is present. This matters when debugging an SDK or browser tool that sets both: a stale Bearer token will 401 even though your API key is good. Strip the `Authorization` header when testing with an API key.

## Worked Examples

### Health Check (Unauthenticated)

`GET /v1/status` is the only routinely-anonymous endpoint — it is the load-balancer health probe. No credential needed:

```bash
curl http://localhost:8000/v1/status
```

**Output:**

```text
{"status":"healthy","timestamp":"...","details":{"service":"robosystems-api","version":"..."}}
```

Note that the root `/` serves the Swagger UI (HTML), and `/openapi.json` serves the live OpenAPI spec — neither is a health check. Use `/v1/status`.

### Authenticated REST GET (Whoami)

Read your current user with the key pulled inline from config:

```bash
curl -X GET "http://localhost:8000/v1/user" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json"
```

A 200 with your user record confirms the key is valid. A 401 means the key is missing, malformed, expired, or revoked.

### List Your Graphs

```bash
curl -X GET "http://localhost:8000/v1/graphs" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json"
```

### Graph-Scoped GraphQL Read

The extensions GraphQL endpoint is graph-scoped at the URL level: the `graph_id` is a path parameter, never a query argument. The same `X-API-Key` authenticates you, and the endpoint validates your access to that specific graph before the resolver runs:

```bash
GRAPH_ID=$(jq -r '.graphs.roboledger.graph_id // .graph_id' .local/config.json)

curl -X POST "http://localhost:8000/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ fiscalCalendar { closedThrough closeTarget } }"}'
```

The `fiscalCalendar` query above is illustrative — substitute any field your deployment exposes. The thing to remember is the URL is the scope:

> Right: `{ entity { … } }` — `graph_id` comes from the URL path.
> Wrong: `{ entity(graphId: "kg_x") { … } }` — GraphQL queries do not take a `graphId` argument.

## API Key Lifecycle

API keys are managed under `/v1/user/api-keys`. The full request/response schemas are in the [OpenAPI docs](http://localhost:8000/docs); the concepts you need to operate them are below.

### Key Format

A RoboSystems API key is the literal prefix `rfs` followed by 64 hex characters (67 characters total), e.g. `rfs9ce10...`. Keys are stored **hashed** (bcrypt) — never in plaintext. The server cannot show you the raw key after it is created; it only ever exposes the 8-character `prefix` (e.g. `rfs9ce10`) used to identify the key in lists.

### Create a Key

```bash
curl -X POST "http://localhost:8000/v1/user/api-keys" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"name": "CI pipeline key", "description": "for nightly smoke tests"}'
```

The request fields are `name` (required), `description` (optional), and `expires_at` (optional, ISO-8601, must be in the future):

```bash
curl -X POST "http://localhost:8000/v1/user/api-keys" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "CI pipeline key",
    "description": "for nightly smoke tests",
    "expires_at": "2026-12-31T23:59:59Z"
  }'
```

The response contains the key metadata (`api_key`) plus the raw `key`:

```text
{
  "api_key": {"id": "uak_...", "name": "CI pipeline key", "prefix": "rfs9ce10", ...},
  "key": "rfs<64 hex chars>"
}
```

**Important:** The raw `key` is returned exactly once, at create time. Capture it immediately. After this response you can never retrieve it again — only the `prefix` is visible going forward. If you lose a key, revoke it and create a new one.

### List Keys

```bash
curl -X GET "http://localhost:8000/v1/user/api-keys" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json"
```

Each entry exposes `id`, `name`, `description`, `prefix`, `is_active`, `last_used_at`, `expires_at`, and `created_at` — but **never** the raw key.

### Update a Key

`PUT /v1/user/api-keys/{api_key_id}` updates mutable metadata (`name` and `description`). Use the `id` (a ULID prefixed `uak`) from the list response:

```bash
curl -X PUT "http://localhost:8000/v1/user/api-keys/uak_..." \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"description": "rotated 2026-06-11"}'
```

### Revoke a Key

```bash
curl -X DELETE "http://localhost:8000/v1/user/api-keys/uak_..." \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json"
```

Revocation deactivates the key and invalidates its cached lookup. The effect is immediate but cache-bounded — the API-key validation cache has a short TTL, so a revoked key stops working within that window rather than on the next millisecond. Plan key rotation accordingly: create the replacement, cut traffic over, then revoke the old key.

## Graph-Scoped Access

Authentication and authorization are separate steps. Your API key authenticates *you* against the platform. Access to a specific graph is a second check performed by the graph-scoped dependency layer: it validates that your user has access to the `graph_id` in the URL before the handler or GraphQL resolver runs.

The practical consequence is the status-code split:

| Status | Meaning | Typical fix |
| --- | --- | --- |
| **401** | No credential, or the credential is invalid/expired/revoked | Check the `X-API-Key` header; confirm the key still exists and is active |
| **403** | Valid user, but no access to that `graph_id` | Confirm the graph belongs to (or is shared with) your user; check the graph id is correct |

This applies uniformly across the REST graph endpoints (`/v1/graphs/{graph_id}/...`), the extensions command and view operations (`/extensions/{domain}/{graph_id}/operations/...`), and the GraphQL endpoint (`/extensions/{graph_id}/graphql`). The same key, validated against per-graph access. See [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy) for the multi-tenant model and how graph access is granted.

## The JWT / Bearer Boundary

The browser frontends authenticate with JWT Bearer tokens, not API keys. This is the boundary you do not cross when testing the backend.

The flow, at a glance:

| Endpoint | Purpose |
| --- | --- |
| `POST /v1/auth/login` | Exchange credentials for a Bearer token |
| `GET /v1/auth/me` | Identify the current user from a Bearer token |
| `POST /v1/auth/refresh` | Refresh the session (within a grace window) |
| `POST /v1/auth/logout` | End the session |

`login` returns a short-lived Bearer JWT that the frontend session layer carries on subsequent requests and refreshes automatically before it expires. The token is signed with HS256.

**For backend testing you never need this path.** There is no reason to call `login` from `curl` when you have an API key — the API key authenticates the same `User` against the same endpoints. If you find yourself wrangling Bearer tokens in a shell script, you are on the wrong door; switch to `X-API-Key`. The Bearer path exists for browser sessions across the three frontend domains, where token-based session handling and refresh are the right model.

## Single Sign-On (SSO)

RoboSystems supports SSO so a user authenticated in one frontend app (for example robosystems-app) can hand off to a sibling app (roboledger-app, roboinvestor-app) without re-entering credentials. It is a three-step token handoff:

| Step | Endpoint | What it does |
| --- | --- | --- |
| 1. Generate | `POST /v1/auth/sso-token` | The originating app (authenticated) mints a short-lived SSO token |
| 2. Exchange | `POST /v1/auth/sso-exchange` | The target app exchanges that token for a session |
| 3. Complete | `POST /v1/auth/sso-complete` | The session is finalized in the target app |

SSO is a frontend-to-frontend concern built on the Bearer/session layer. You do not invoke it from `curl` when testing the backend — like the JWT path, it sits on the frontend door. It is documented here so the full authentication picture is complete; for programmatic access, stay on the `X-API-Key` path.

## Troubleshooting

### 401 Unauthorized on a Request You Expect to Work

The credential is missing, malformed, expired, or revoked. Check, in order:

- The `X-API-Key` header is present and spelled correctly.
- You are reading a live key: `jq -r .api_key .local/config.json` returns an `rfs...` value, not `null`.
- The key has not been revoked or expired (list your keys; confirm `is_active` is true and `expires_at` is in the future).
- No stale `Authorization: Bearer` header is also being sent (see the next item).

### 401 Even Though the API Key Is Correct

Bearer-wins precedence is biting you. If the request carries an `Authorization: Bearer` header alongside `X-API-Key`, the Bearer token is validated first and the request does **not** fall back to the API key. A stale or expired Bearer header will 401 regardless of how good your API key is.

**Solution:** Remove the `Authorization` header from the request and send only `X-API-Key`. This is most common with SDKs or browser tooling that set both.

### 403 Forbidden (Not 401)

You are authenticated, but your user does not have access to the `graph_id` in the URL. This is an authorization failure, not a credential failure.

**Solution:** Confirm the graph id is correct and that the graph belongs to (or is shared with) your user. See [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy).

### "I Lost My API Key"

The raw key is shown exactly once, at create time, and is stored hashed thereafter — there is no way to recover it.

**Solution:** Revoke the lost key (`DELETE /v1/user/api-keys/{id}`) and create a new one.

### GraphQL Returns Errors About `graphId`

The graph-scoped GraphQL endpoint takes `graph_id` from the URL path, not as a query argument. Passing `graphId` inside the query body is wrong.

**Solution:** Put the graph id in the URL (`/extensions/{graph_id}/graphql`) and remove any `graphId` argument from the query — `{ entity { … } }`, not `{ entity(graphId: "...") { … } }`.

### `/v1/status` Works but Everything Else 401s

`/v1/status` is the only unauthenticated endpoint. If it returns 200 but authenticated calls 401, the stack is up and the problem is your credential, not the server. Re-run `just demo-user` to refresh `.local/config.json`, then re-read the key inline.

### Health Check Hits the Wrong Path

`GET /health` and `GET /v1/health` do not exist. The root `/` is the Swagger UI (HTML), not a probe.

**Solution:** Use `GET /v1/status` for health checks and `GET /openapi.json` for the live spec.

## Related Documentation

**Wiki Guides:**

- **[Quick Start](Quick-Start)** - Start the local stack and run `just demo-user` to get your first API key
- **[Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy)** - The per-graph access model and 401-vs-403 semantics
- **[Graph Operations](Graph-Operations)** - Where authenticated requests go: operation envelopes and `X-API-Key` in practice

**API Reference:**

- **[API Documentation](https://api.robosystems.ai/docs)** - Live OpenAPI reference with request/response schemas for every endpoint
- **Local OpenAPI spec**: [http://localhost:8000/docs](http://localhost:8000/docs) (Swagger UI) and [http://localhost:8000/openapi.json](http://localhost:8000/openapi.json) (machine-readable)

**Codebase Documentation:**

- **[Authentication Middleware](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/auth/README.md)** - The authentication system internals
- **[Graph Routing](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/graph/README.md)** - Multi-tenant graph routing and per-graph access
- **[GraphQL Extensions](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md)** - The graph-scoped GraphQL surface and its auth

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
