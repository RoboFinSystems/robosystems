# Extensions Surface Overview

The extensions surface is the read/write API layer for RoboSystems product extensions (RoboLedger and RoboInvestor). It sits beside the core `/v1` platform API under a single `/extensions/*` prefix, and everything on it is graph-scoped at the URL.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development).

## Table of Contents

- [What the Extensions Surface Is](#what-the-extensions-surface-is)
- [The Three Sub-Surfaces](#the-three-sub-surfaces)
- [Graph-Scoping at the URL](#graph-scoping-at-the-url)
- [Auth Runs Before the Handler](#auth-runs-before-the-handler)
- [Feature Flags and the Dynamically Built Schema](#feature-flags-and-the-dynamically-built-schema)
- [The OperationEnvelope Contract](#the-operationenvelope-contract)
- [One Invariant: The Ops Layer Is the Single Source of Truth](#one-invariant-the-ops-layer-is-the-single-source-of-truth)
- [Worked Examples](#worked-examples)
- [Gotchas](#gotchas)
- [Related Documentation](#related-documentation)
- [Support](#support)

## What the Extensions Surface Is

The core platform API lives under `/v1` and covers graphs, billing, auth, and shared repositories. Product extensions — RoboLedger (accounting) and RoboInvestor (investment management) — are served separately under `/extensions/*`. The split is deliberate: the platform API manages identity, infrastructure, and metadata, while the extensions surface is where you read and write the domain data for a specific graph.

The extensions surface has a single organizing rule: **the graph (tenant) is always in the URL path**. Every route below is scoped to a `graph_id`, and FastAPI validates the user's access to that graph before any handler runs.

## The Three Sub-Surfaces

Everything on the extensions surface falls into one of three sub-surfaces, split by transport rather than by HTTP verb. Typed reads go through GraphQL, writes go through named command operations, and graph-backed analytics go through named view operations.

| Sub-surface | Method + Path | Read/Write | Backs onto |
| --- | --- | --- | --- |
| **GraphQL typed reads** | `POST /extensions/{graph_id}/graphql` | Read | PostgreSQL extensions OLTP database |
| **Command writes** | `POST /extensions/{roboledger\|roboinvestor}/{graph_id}/operations/{op_name}` | Write | PostgreSQL extensions OLTP database |
| **Analytical view operations** | `POST /extensions/roboledger/{graph_id}/operations/{view_name}` | Read | Materialized LadybugDB XBRL hypercube |

A few details worth pinning down:

- **GraphQL typed reads** are served by a single Strawberry endpoint per graph. The schema is composed dynamically per deployment (see [Feature Flags](#feature-flags-and-the-dynamically-built-schema)). Introspection over POST works on the hosted API, with or without credentials; the in-browser GraphiQL explorer is development-only and is not mounted on `api.robosystems.ai`.
- **Command writes** are explicit, named operations (`close-period`, `share-report`, `create-security`) rather than resource CRUD. They match the multi-step domain operations they actually perform. Each returns an [`OperationEnvelope`](#the-operationenvelope-contract).
- **Analytical view operations** share the same envelope contract as command writes but are read-only and routed to the materialized graph rather than the OLTP database. There are two: `build-fact-grid` (multi-dimensional pivot tables over the XBRL hypercube) and `financial-statement-analysis`. These are gated independently of the OLTP domains, so a deployment without RoboLedger tenants can still use them.
- **One more shape rides the command-operation transport.** A few RoboLedger operations are POST `…/operations/{name}` and return an `OperationEnvelope` like a command write, but read the OLTP database instead of writing to it. `live-financial-statement` generates an ad-hoc statement straight from the tenant's ledger using the active CoA→GAAP mapping, with no materialization step. Because it reads the same PostgreSQL database the ledger commands write, it is gated by `ROBOLEDGER_ENABLED` rather than `FACT_GRID_ENABLED`.

For the full, machine-readable endpoint surface — every operation name, request schema, and response shape — see the live OpenAPI spec at [`https://api.robosystems.ai/docs`](https://api.robosystems.ai/docs). This page covers the concepts and tasks; the OpenAPI spec is the authoritative endpoint reference.

## Graph-Scoping at the URL

The `graph_id` is always a URL path parameter, never a query argument or request-body field. The path pattern accepts parent graph IDs (`kg` + 16 or more hex chars, e.g. `kg1a2b3c4d5e6f7890`), shared-repository IDs (`sec`, `sec_historical`), the reserved `library` sentinel (which browses the shared public taxonomy library), and subgraph IDs (`{parent_id}_{subgraph_name}`, e.g. `kg1a2b3c4d5e6f7890_dev`).

This matters most for GraphQL. The graph is determined by the URL, and resolvers read it from request context — so GraphQL queries do **not** take a `graphId` argument:

```text
Wrong:  { entity(graphId: "kg1a2b3c4d5e6f7890") { name } }
Right:  { entity { name } }
```

Putting the tenant in the URL rather than the query body eliminates a whole class of "I queried the wrong tenant" bugs: the scope is fixed before the resolver runs, and there is no way to override it from inside the query.

Subgraph IDs are the one case where matching the pattern is not enough. On the **GraphQL** sub-surface a subgraph is rejected with **HTTP 403** — a subgraph is a modality container with no extensions schema of its own, so the request is refused rather than quietly resolved to its parent. The check runs after authentication and before the access check, so the denial stays attributable. Target the parent graph instead. The command and view operations carry no such guard.

## Auth Runs Before the Handler

Authentication and per-graph access are enforced by FastAPI dependencies that run **before** any resolver, command runner, or view handler executes. A request that lacks access to the graph never reaches business logic.

- GraphQL requests resolve the user and graph through the context dependency, then check graph access before the query is executed.
- Command and view operations resolve the user and validate per-graph access via the operation dependency before the runner is invoked.

Authenticate API calls with the **`X-API-Key`** header. Create the key in the app under **Settings → API keys** — it is shown once, and can be scoped to all your graphs or to one graph. The key has the format `rfs` followed by 64 hex characters:

```bash
-H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

`Authorization: Bearer` (JWT) is a frontend concern — the apps use an HTTP-only cookie. Do not use Bearer tokens for backend `curl` testing.

## Feature Flags and the Dynamically Built Schema

Each sub-surface is gated by a feature flag, and the gating happens at **schema-construction time, not request time**. Disabled domains are simply absent from the API rather than mounted and throwing runtime errors.

These flags are deployment configuration, not something an API caller sets. On robosystems.ai all four are on; a self-hosted deployment controls them through its environment.

| Flag | Gates | Default |
| --- | --- | --- |
| `ROBOLEDGER_ENABLED` | RoboLedger command operations + reads + GraphQL ledger fields | `true` |
| `ROBOINVESTOR_ENABLED` | RoboInvestor command operations + GraphQL investor fields | `true` |
| `EXTENSIONS_GRAPHQL_ENABLED` | Kill switch for the `/extensions/{graph_id}/graphql` endpoint | `true` |
| `FACT_GRID_ENABLED` | Graph-backed analytical view operations (`build-fact-grid`, `financial-statement-analysis`) | `true` |

`EXTENSIONS_ENABLED` is a **derived** property — it is `ROBOLEDGER_ENABLED OR ROBOINVESTOR_ENABLED`, not a standalone environment variable. It controls whether the extensions database engine is active at all. (The legacy `LEDGER_ENABLED` / `INVESTOR_ENABLED` names have been retired; use the `ROBO*` names.)

The database that engine opens is configured with **`EXTENSIONS_DATABASE_URL`** — a database separate from the platform's `DATABASE_URL`, with its own independent migration history (both live on the same RDS instance in deployed environments). Tenancy inside it is **schema-per-graph**: each request sets `search_path` to the graph's own schema with `public` as the fallback, so a graph's rows are never visible from another graph's session.

**The GraphQL schema is composed per deployment.** A baseline set of query fields is always present (information blocks, taxonomy blocks, and the library), and the domain-specific field sets are added only when their flag is on:

- `ROBOLEDGER_ENABLED` adds the ledger query fields (roughly three dozen — `entity`, `trialBalance`, `fiscalCalendar`, `reports`, `mappings`, and more).
- `ROBOINVESTOR_ENABLED` adds the investor query fields (`portfolios`, `securities`, `positions`, `holdings`, and related).

Because the schema is built at construction time, a ledger-only deployment literally has no `portfolios` field — introspection never reveals it, and clients should branch on the introspected schema rather than discovering disabled domains through trial-and-error runtime errors. Note that the *graph-backed* analytical view operations are gated by `FACT_GRID_ENABLED` independently of `ROBOLEDGER_ENABLED`: they are mounted under the `/extensions/roboledger/...` path but stay available on deployments (such as SEC-only setups) that have no RoboLedger tenants. The OLTP-backed read operation `live-financial-statement` sits on the same path but follows `ROBOLEDGER_ENABLED`.

## The OperationEnvelope Contract

Both command writes and analytical view operations return an `OperationEnvelope`. This is the same envelope/idempotency/SSE infrastructure used by graph lifecycle writes under `/v1/graphs/{graph_id}/operations/{op_name}`. On the wire, fields use camelCase aliases:

| Field | Meaning |
| --- | --- |
| `operation` | The kebab-case operation name (e.g. `close-period`) |
| `operationId` | A ULID prefixed with `op_`; the bridge to SSE progress streaming |
| `status` | `"completed"` (HTTP 200, ran synchronously), `"pending"` (HTTP 202, async), or `"failed"` |
| `result` | The operation's response payload, or `null` |
| `at` | ISO-8601 UTC timestamp |
| `createdBy` | The user ID that issued the operation |
| `idempotentReplay` | `true` when the response was served from the idempotency cache without re-executing |

Two behaviors fall out of this contract:

- **Idempotency.** Every operation accepts an `Idempotency-Key` header for safe retries. Replaying an identical request returns the cached result with `idempotentReplay: true` and does not re-execute. Reusing the same key with a different request body returns **HTTP 409** (a fingerprint conflict).
- **Async operations stream progress.** Long-running commands return `status: "pending"` with HTTP 202; monitor them via Server-Sent Events at `/v1/operations/{operationId}/stream` rather than expecting the result inline.

## One Invariant: The Ops Layer Is the Single Source of Truth

All three sub-surfaces — plus the MCP tool layer — delegate to the same domain functions in `operations/{domain}/{reads,commands,views}`. Those functions are the only place domain logic lives:

- **GraphQL resolvers** delegate reads to `operations/{domain}/reads/*`.
- **Command operation routers** delegate writes to `operations/{domain}/commands/*`.
- **Analytical view handlers** delegate to `operations/{domain}/views/*`.
- **MCP tools** (the operational and analytical retrieval planes) call the same ops-layer modules directly.

This invariant is load-bearing: whether a request arrives through GraphQL, a named operation, or an MCP tool, the same function runs and the same business rules are enforced. Adding logic anywhere else — in a router, a resolver, or an MCP handler — is a mistake.

## Worked Examples

All examples assume an API key and a RoboLedger graph — see [Quick Start](Quick-Start) for getting both. Set them once:

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai
export GRAPH_ID=kg...               # from GET /v1/graphs or the app's graph selector
```

### GraphQL Liveness and Auth Probe

The `hello` field is an auth probe that returns no business data — it works on any extensions-enabled graph and is the quickest way to confirm credentials and reachability:

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ hello }"}'
```

### GraphQL Typed Read

The graph is in the URL, so the query carries no `graphId` argument. Field names are camelCase on the wire:

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ fiscalCalendar { closedThrough closeTarget } entity { name } }"}'
```

GraphQL list resolvers take `limit` (per-resolver default, must be 1–1000) and `offset` (default 0, must be ≥ 0). A success returns `{"data": {...}}`; a typed error returns `{"errors": [{"extensions": {"code": "..."}}]}`. Clients should branch on `extensions.code` (for example `UNAUTHENTICATED`, `FORBIDDEN`, `INVALID_PAGINATION`, `LEDGER_NOT_INITIALIZED`, `INVESTOR_NOT_INITIALIZED`), not on the human-readable message.

### Command Write — Close a Period

A RoboLedger command write returns an `OperationEnvelope` and supports `Idempotency-Key`:

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/close-period" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Idempotency-Key: $(date +%s)" \
  -H "Content-Type: application/json" \
  -d '{"period": "2026-03", "allow_stale_sync": false}'
```

### Analytical View Operation — build-fact-grid

A graph-backed analytical view that pivots over the XBRL hypercube. It hits the materialized LadybugDB graph rather than the OLTP database:

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/build-fact-grid" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
        "canonical_concepts": ["revenue"],
        "entities": ["NVDA", "AMD"],
        "form": "10-K",
        "fiscal_year": 2024,
        "fiscal_period": "FY",
        "view_config": {"rows": [{"type": "entity"}]}
      }'
```

**Important:** `build-fact-grid` requires a scoping filter. You must supply `elements` and/or `canonical_concepts`, **and** at least one of `periods` / `period_type` / `fiscal_year`. Omitting both classes of filter returns HTTP 400.

### RoboInvestor Command Write — Create a Security

RoboInvestor writes go through the same operation surface under the `roboinvestor` path segment, and require a deployment with `ROBOINVESTOR_ENABLED` (on robosystems.ai it is) and a graph that carries the `roboinvestor` extension:

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboinvestor/$GRAPH_ID/operations/create-security" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "Acme Common Stock", "security_type": "common_stock"}'
```

For the exact request-body fields of any operation, consult the OpenAPI spec at [`https://api.robosystems.ai/docs`](https://api.robosystems.ai/docs) rather than guessing.

## Gotchas

- **GraphQL takes no `graphId` argument.** Use `{ entity { … } }`, never `{ entity(graphId: "kg_x") }`. The graph is the URL.
- **Use `X-API-Key` for API calls**, not `Authorization: Bearer`. Send the key as a header, never in the URL.
- **Graph-backed analytical views (`build-fact-grid`, `financial-statement-analysis`) are gated by `FACT_GRID_ENABLED`, not `ROBOLEDGER_ENABLED`.** They live under the `/extensions/roboledger/...` path but are flagged independently, so they remain available on SEC-only deployments. `live-financial-statement`, which reads the OLTP database rather than the graph, follows `ROBOLEDGER_ENABLED` like the command writes.
- **Subgraph IDs are rejected on the GraphQL endpoint with HTTP 403.** They match the path pattern, so the failure surfaces at runtime rather than as a validation error. Target the parent graph. The operation endpoints do not apply this restriction.
- **`build-fact-grid` requires a scoping filter** — `elements` and/or `canonical_concepts`, plus at least one of `periods` / `period_type` / `fiscal_year`, or you get HTTP 400.
- **A GraphQL "error" can still return HTTP 200.** Anonymous introspection is allowed, so an unauthenticated request can return a `200` with an `UNAUTHENTICATED` error code in `extensions.code`; HTTP 401 means credentials were presented but invalid.
- **Idempotency conflicts return HTTP 409** when the same key is reused with a different request body. Identical replays return `idempotentReplay: true` without re-executing.
- **Async commands return HTTP 202 with `status: "pending"`.** Monitor them over SSE at `/v1/operations/{operationId}/stream`; do not expect the result inline.
- **Disabled-domain fields are simply absent.** Branch on the introspected schema, not on trial-and-error runtime errors.
- **Pagination bounds are enforced.** `limit` must be 1–1000 or the resolver returns `INVALID_PAGINATION`.

## Related Documentation

**Wiki Guides:**
- **[Architecture Overview](Architecture-Overview)** - Platform architecture; its "Extensions API" subsection is the parent context for this page
- **[GraphQL Reads](GraphQL-Reads)** - Deep dive on the typed read surface and field catalog
- **[RoboLedger Operations](RoboLedger-Operations)** - The RoboLedger command operation catalog
- **[RoboInvestor Operations](RoboInvestor-Operations)** - The RoboInvestor command operation catalog
- **[RoboLedger Demo Walkthrough](RoboLedger-Demo-Walkthrough)** - End-to-end close and reporting workflow against synthetic data

**Codebase Documentation:**
- **[GraphQL Extensions](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md)** - Strawberry GraphQL surface, Pydantic auto-derivation, and resolver patterns
- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - The reads/commands/views domain kernel that all sub-surfaces delegate to
- **[Authentication](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/auth/README.md)** - API key and JWT authentication
- **[API Models](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/api/README.md)** - Pydantic request/response models for the operation surface
- **[API Documentation](https://api.robosystems.ai/docs)** - Live OpenAPI spec with the full endpoint surface

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
