# Errors and Rate Limits

Every RoboSystems endpoint reports failure the same way: a non-2xx status and a small JSON body with a `detail` and a `request_id`. This page covers that body, what each status code means for an integrator and what to do about it, the rate-limit headers and 429 response, and the response an AI operation returns when a graph runs out of credits.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development).

## Table of Contents

- [The Error Body](#the-error-body)
- [Status Codes and What to Do](#status-codes-and-what-to-do)
- [Typed Error Codes](#typed-error-codes)
- [GraphQL Errors](#graphql-errors)
- [Quoting a Request ID](#quoting-a-request-id)
- [Rate Limits](#rate-limits)
- [Insufficient Credits](#insufficient-credits)
- [Retry Policy](#retry-policy)
- [Self-hosted deployments](#self-hosted-deployments)
- [Related Documentation](#related-documentation)
- [Support](#support)

## The Error Body

An error response carries two keys:

```json
{
  "detail": "Event already ingested for this source and external_id",
  "request_id": "7f3c9a1e-2b4d-4c6e-8f0a-1b2c3d4e5f60"
}
```

- **`detail`** is usually a human-readable string. Some errors carry an object instead, in one of two shapes, each with a machine-readable `code` (see [Typed Error Codes](#typed-error-codes)). Always read it as `body["detail"]` and check its type.
- **`request_id`** identifies this request in the platform's logs. The same value is sent as the `X-Request-ID` response header on every response, success or failure.

**Request validation (422)** adds a `code`. When a body or parameter fails schema validation — a missing required field, a wrong type, a value outside its allowed pattern — the response is:

```json
{
  "detail": "external_config.source_name: String should match pattern '^[a-z][a-z0-9_-]{1,63}$'",
  "code": "VALIDATION_ERROR",
  "request_id": "0c1d2e3f-4a5b-4c6d-8e7f-9a0b1c2d3e4f"
}
```

`detail` joins every failing field as `field.path: message`, separated by `; `. A 422 without `code: "VALIDATION_ERROR"` is a business-rule refusal from the operation itself (a closed period, an unbalanced entry, an invalid status transition) — the request was well-formed, and `detail` says what to change.

**Unexpected server errors (500).** An exception no handler anticipated answers with a generic body:

```json
{ "detail": "Internal server error", "request_id": "…" }
```

A route that catches its own failure may instead return a short message of its own, such as `"Failed to cancel operation"`, still with the `request_id`. Either way, the `request_id` is how that error is found — see [Quoting a Request ID](#quoting-a-request-id).

The SCIM provisioning surface (`/scim/v2`) is the one exception to this shape: it answers in the RFC 7644 error format that identity providers expect. See [Enterprise SSO and SCIM](Enterprise-SSO-and-SCIM).

## Status Codes and What to Do

| Status | Meaning | What to do |
| --- | --- | --- |
| **400** | The request is well-formed but not acceptable as asked — a required pairing missing, a confirmation that doesn't match, an option the resource doesn't support | Fix the request per `detail`. Do not retry unchanged |
| **401** | No valid credential — the `X-API-Key` header is missing, malformed, revoked, or expired, or a graph-scoped key was sent to a route without a graph id | Check the header name, the key, and its scope. Do not retry until fixed |
| **402** | Payment is required for this action — no payment method on the organization, or the graph has no credits for an AI operation | See [Insufficient Credits](#insufficient-credits). Not retryable until an owner acts |
| **403** | Not allowed — no access to this graph, a role too low for a write, a key scoped to a different graph, a feature not provisioned on this graph, or a write to a read-only shared repository. On a REST or MCP route with a `graph_id`, an API key that fails validation also answers 403, not 401 | Check the key is live, then the graph id, the key's scope, and your role. A graph that does not exist also answers 403, so a typo in `graph_id` looks like this |
| **404** | The named resource doesn't exist in this graph — an event, a period, an operation whose records have expired | Check the id. For extension operations, a 404 can also mean the graph's RoboLedger or RoboInvestor data has not been initialized |
| **409** | Conflict with current state. Three kinds: an `Idempotency-Key` in use or reused with a different body; a duplicate (an event with the same `source` + `external_id`, a connection that already exists); a row locked by another writer, such as a running sync ("Retry in a moment") | Read `detail`. Retry the lock case after a short delay; treat a duplicate as already done; fix the key for a key conflict |
| **413** | The work is too large for the graph's tier, e.g. a materialization over its limits | Reduce the input or change tier |
| **422** | Validation (`code: "VALIDATION_ERROR"`) or a domain rule refusal (no `code`) | Fix the request per `detail`. A retry of the same body fails the same way |
| **429** | Rate limit exceeded | Wait for `Retry-After` seconds, then retry. See [Rate Limits](#rate-limits) |
| **500** | Unexpected server error | Retry once with the same `Idempotency-Key`; if it persists, report the `request_id` |
| **503** | A dependency is temporarily unavailable, or a lock needed for the operation could not be taken | Retry with backoff |
| **504** | The request exceeded a time limit — for extension operations, the per-statement ceiling | Narrow the request (filters, a smaller page) or retry after `Retry-After` |

Two patterns cover most integration code:

- **Retry**: 409 lock conflicts, 429, 500, 503, 504 — with backoff, and with the same `Idempotency-Key` so a retry that races a success cannot double-write (see [Operations Contract](Operations-Contract#idempotency-key)).
- **Stop and fix**: 400, 401, 403, 404, 413, 422 — the same request will fail the same way.

## Typed Error Codes

Where a client needs to branch programmatically, `detail` is an object with a `code`. Two object shapes occur.

**The platform error object** — `detail` nests a message of its own, the code, and a timestamp:

```json
{
  "detail": {
    "detail": "API key not found or access denied",
    "code": "NOT_FOUND",
    "timestamp": "2026-10-05T14:05:11.204871+00:00"
  },
  "request_id": "…"
}
```

Its `code` is one of a small fixed set: `NOT_FOUND`, `ALREADY_EXISTS`, `INVALID_INPUT`, `UNAUTHORIZED`, `INVALID_CREDENTIALS`, `FORBIDDEN`, `PROVIDER_ERROR`, `EXTERNAL_SERVICE_ERROR`, `OPERATION_FAILED`, `INTERNAL_ERROR`. Read the human-readable text at `detail.detail`.

**Operation-specific objects** — a `code`, a `message`, and fields particular to the failure:

```json
{
  "detail": {
    "code": "INSUFFICIENT_CREDITS",
    "message": "Not enough credits to perform AI analysis",
    "required_credits": 42,
    "available_credits": 10
  },
  "request_id": "…"
}
```

The codes an integrator is most likely to meet:

| Code | Where | Meaning |
| --- | --- | --- |
| `VALIDATION_ERROR` | Any endpoint, 422 | Schema validation failed (top-level `code`, string `detail`) |
| `INSUFFICIENT_CREDITS` | AI Operator endpoints, 402 | The graph's credit pool cannot cover the call |
| `WRITE_BACK_FAILED`, `STATEMENT_STAMP_FAILED` | `close-period`, 422 | The close could not complete a publishing step; `detail.message` explains which |

`close-period` also returns its blocking conditions as structured detail (`message` plus `blockers`) so a client can show what stands in the way of a close. Most other refusals are plain strings — branch on the status code first, then on `detail.code` when `detail` is an object.

`OPERATOR_FAILED` and `OPERATOR_CANCELLED` are not error responses. When an AI Operator call waits for its run and the run fails or is cancelled, the call still answers **200**, and the code arrives in the body's `error_details` — the request succeeded; the run did not.

## GraphQL Errors

GraphQL at `/extensions/{graph_id}/graphql` follows GraphQL conventions rather than the REST body above: a query that reaches the resolver returns **HTTP 200** with an `errors` array, and each error carries a machine-readable code in `extensions.code`:

```json
{
  "data": { "trialBalance": null },
  "errors": [
    { "message": "…", "path": ["trialBalance"], "extensions": { "code": "LEDGER_NOT_INITIALIZED" } }
  ]
}
```

Branch on `extensions.code`, not on `message`:

| Code | Meaning |
| --- | --- |
| `UNAUTHENTICATED` | The query asked for data without a credential (anonymous requests may only introspect) |
| `EXTENSION_NOT_PROVISIONED` | The graph does not have the extension the field belongs to |
| `LEDGER_NOT_INITIALIZED`, `INVESTOR_NOT_INITIALIZED` | The graph's RoboLedger or RoboInvestor data has not been set up yet |
| `INVALID_PAGINATION` | `limit` or `offset` out of range |
| `INVALID_ARGUMENT`, `INVALID_PERIOD` | An argument failed a resolver-level check |
| `INVALID_EXPIRES_IN`, `REPORT_BUNDLE_NOT_AVAILABLE`, `REPORT_BUNDLE_SIGNING_FAILED` | A report download link could not be issued |
| `STATEMENT_TIMEOUT` | The read exceeded the per-statement time limit; narrow it |
| `INTERNAL_ERROR` | An unexpected failure; the error's `extensions.requestId` is the value to quote |

Access is not a GraphQL error code. An HTTP 401 (bad credential) or 403 (no access to the graph, or a subgraph id in the URL) from the GraphQL endpoint means the request was refused before any query ran. See [GraphQL Reads](GraphQL-Reads).

## Quoting a Request ID

Every response — success or error — carries an `X-Request-ID` header, and every error body repeats it as `request_id`. It is the one value that lets the platform find your exact request in its logs, alongside the operation, graph, and timing.

When you report a problem, include:

- the `request_id` (or `X-Request-ID` header),
- the method and path (with your `graph_id`),
- the approximate time (UTC) and the status code you received,
- for an async operation, the `operationId` from the envelope.

Never include your API key. Log `request_id` on every non-2xx response in your integration so it is on hand when you need it. Report issues through [GitHub Issues](https://github.com/RoboFinSystems/robosystems/issues) or [Discussions](https://github.com/orgs/RoboFinSystems/discussions); anything security-sensitive goes through the repository's security policy instead of a public issue.

## Rate Limits

Rate limits protect shared infrastructure from bursts. They are separate from credits: a rate limit never consumes credits, and an exhausted credit pool never changes a rate limit.

**How they are shaped.**

- Limits are **per endpoint category** — graph reads, graph writes, Cypher queries, MCP, AI Operators, search, backups, extension GraphQL reads, extension operations, and so on — so a burst of one kind of traffic does not starve another.
- Windows are **short** — the configured limits are per minute. They absorb normal bursts rather than cap monthly volume; volume is what credits and tier limits are for.
- Categories served by the graph's own dedicated instance (queries, reads, writes, MCP, operators, analytics) **scale with the graph's tier** and are counted per graph. Categories that land on shared infrastructure (search, backups, imports, management, sync) are the same on every tier and are counted per user.
- Shared repositories such as `sec` carry their own per-plan limits — see [Shared Repositories](Shared-Repositories).

**Where the numbers are.** The limits are configuration, served live rather than restated here:

- `GET /v1/graphs/{graph_id}/limits` — the graph's `rate_limits` block, alongside its storage, backup, and credit limits. That block reports one category, Cypher queries, for the graph's tier: `requests_per_minute` is the enforced budget, and `requests_per_hour` and `burst_capacity` restate it rather than add separate ceilings. Every other category's budget is in the `X-RateLimit-Limit` header of its own responses.
- `GET /v1/offering` and `GET /v1/graphs/tiers` — each tier's `api_rate_multiplier`, and for shared repositories each plan's `rate_limits`.

**Headers on a successful response.** Rate-limited endpoints report where you stand:

| Header | Meaning |
| --- | --- |
| `X-RateLimit-Limit` | Requests allowed in the current window for this category |
| `X-RateLimit-Remaining` | Requests left in the current window |
| `X-RateLimit-Tier` | The tier the limit was taken from (tier-aware categories) |
| `X-RateLimit-Category` | The endpoint category that was counted (tier-aware categories) |

**The 429 response.**

```text
HTTP/1.1 429 Too Many Requests
Retry-After: 60
X-RateLimit-Limit: …
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1790000000
X-RateLimit-Tier: ladybug-standard
X-RateLimit-Category: extensions_write
```

```json
{
  "detail": "Rate limit exceeded for extensions write operations.",
  "request_id": "…"
}
```

`Retry-After` is in seconds; `X-RateLimit-Reset` is a Unix timestamp in seconds. Limits outside the tier-aware categories (authentication, SSE connections, billing) send `X-RateLimit-Type` in place of the tier and category headers. Wait at least `Retry-After` before retrying; spreading a backfill evenly across windows is faster overall than bursting and backing off.

Server-Sent Event streams have their own limit on concurrent connections per user — see [Operations Contract](Operations-Contract#following-an-async-operation).

## Insufficient Credits

Only AI operations consume credits (see [Credits and Billing](Credits-and-Billing)). When a graph's pool cannot cover an AI Operator call, the operator endpoints (`POST /v1/graphs/{graph_id}/operator` and `POST /v1/graphs/{graph_id}/operator/{operator_type}`) refuse it up front with **402**:

```json
{
  "detail": {
    "code": "INSUFFICIENT_CREDITS",
    "message": "Not enough credits to perform AI analysis",
    "required_credits": 42,
    "available_credits": 10
  },
  "request_id": "…"
}
```

`required_credits` is an estimate made before the call; the actual charge is the measured token cost afterwards. An AI run that is already queued re-checks the balance when it starts, and reports the same code as an operation failure if the pool was spent in the meantime.

Nothing else is affected: queries, operations, syncs, imports, and backups do not use credits and keep working with an empty pool. Check the balance with `GET /v1/graphs/{graph_id}/credits`.

A different 402 appears when creating a graph for an organization with no payment method on file — `detail` says so, and an owner resolves it in the app.

## Retry Policy

A sound default for an integration:

1. Send an `Idempotency-Key` on every write.
2. On 429, sleep `Retry-After` seconds, then retry.
3. On 409 lock conflicts, 500, 503, and 504, retry with exponential backoff and jitter (for example 1s, 2s, 4s, 8s, capped), reusing the same `Idempotency-Key`.
4. On anything else in the 4xx range, stop and surface `detail` and `request_id`.
5. Cap total attempts; a request that keeps failing is a bug to report, not a load to push harder.

## Self-hosted deployments

Rate limiting is controlled by `RATE_LIMIT_ENABLED`, and the limit table lives in `robosystems/config/rate_limits.py`. A local stack runs with billing off (`BILLING_ENABLED=false`), so credit checks do not gate AI calls there. Locally, the `request_id` of an error is searchable in the API logs (`just logs api`).

## Related Documentation

**Wiki Guides:**

- **[Operations Contract](Operations-Contract)** - The envelope, `Idempotency-Key`, and progress streaming shared by every write
- **[Credits and Billing](Credits-and-Billing)** - What consumes credits, allocations, and balances
- **[Authentication and API Keys](Authentication-and-API-Keys)** - Creating, scoping, and rotating keys
- **[Shared Repositories](Shared-Repositories)** - Per-plan limits on `sec` and other repositories
- **[GraphQL Reads](GraphQL-Reads)** - The GraphQL error codes in context

**API Reference:**

- **[API reference](https://robosystems.ai/docs/api)** - Every endpoint's documented error responses

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
