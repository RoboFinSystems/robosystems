# Authentication Middleware

Authentication and per-graph authorization for the platform. This package
exposes FastAPI **dependencies**, not ASGI middleware classes — you protect an
endpoint by declaring the dependency in the route signature. It handles two
credential types (JWT and API key), caches validation results in Valkey, and
enforces graph access on every authenticated graph-scoped route.

Rate limiting is a separate concern and lives in
[`../rate_limits/`](../rate_limits/). This package's `__init__.py` re-exports
`graph_scoped_rate_limit_dependency` and
`subscription_aware_rate_limit_dependency` for convenience but implements
neither.

## The two credential surfaces

**JWT (`Authorization: Bearer …`) is for frontends.** Short-lived HS256 access
tokens, 30-minute expiry (`JWT_EXPIRY_HOURS = 0.5` in `config/constants.py` — a
constant, not an env var), with a refresh flow. Claims: `user_id`, `jti`,
`session_version`, `iss`, `aud`, and an optional `device_hash`. Revocation is
per-`jti` in Valkey (`revoked_jwt:{jti}`) with a TTL equal to the token's
remaining lifetime and a short grace window for `session_refresh` revocations.
Bumping a user's `session_version` invalidates every outstanding token for that
user, because `verify_jwt_claims` compares the claim against the stored value.

**API keys (`X-API-Key`) are for programmatic clients.** Format is
`^rfsc?[0-9a-f]{64}$` (`utils.py:_API_KEY_FORMAT_RE`) — 64 lowercase hex
characters from `secrets.token_hex(32)`, prefixed `rfs` for account-wide keys
and `rfsc` for graph-scoped keys. Keys are bcrypt-hashed in the database; the
first 8 characters are stored in an indexed `prefix` column so verification
only bcrypt-checks the handful of candidate rows. SHA-256 of the raw key is
used *only* as the Valkey cache lookup key, never as credential storage.

When testing locally, always use `X-API-Key` — Bearer tokens are a frontend
concern:

```bash
curl -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
     http://localhost:8000/v1/graphs/$GRAPH_ID/...
```

### Key scoping

A key minted with a `graph_id` (`user_api_keys.graph_id`, prefix `rfsc`) is
valid only for that graph and its subgraphs, **on every carriage path**, and is
rejected outright on endpoints with no graph context — `validate_api_key`
refuses scoped keys. A NULL `graph_id` means account-wide. The authoritative
check is always the row's `graph_id`; the `rfsc` prefix is legibility for
humans and incident response only.

### Credentials in query parameters — the two deliberate doors

Header carriage is the rule. Exactly two routes accept a credential via a
`?token=` query parameter, both because their client cannot send custom
headers. Both are covered by the redaction list in `middleware/logging.py` and
by the OTel span redaction in `middleware/otel/setup.py`.

| Route                                  | Dependency                              | Accepts                          | Why                                       |
| -------------------------------------- | --------------------------------------- | -------------------------------- | ----------------------------------------- |
| `GET /v1/operations/{id}/stream` (SSE) | `get_current_user_sse`                  | **JWT only** (30-min TTL)        | browser `EventSource` cannot set headers  |
| `POST /v1/graphs/{graph_id}/mcp`       | `get_current_user_with_graph_or_url_token` | **graph-scoped API key only** | MCP connector clients cannot set headers  |

The asymmetry is deliberate: the SSE door carries a short-lived session token,
so no extra restriction is needed; the MCP door carries a durable key, so only
graph-scoped keys are honored and account-wide keys are hard-rejected — the
account credential must never be the one that travels in a URL. Header and JWT
auth take precedence on both; the query parameter is consulted only when
neither is present. Do not add a third door without matching this table, the
redaction lists, and a scope story.

### SSO

`jwt.py:create_sso_token` mints a single-use handoff JWT with a 300-second TTL,
carrying `{"sso": true}` and a `token_id` used to enforce single use. Reuse of
a spent token is treated as a possible replay of a leaked credential and logged
as such. Used for handoff between app.robosystems.ai, roboledger.ai, and
roboinvestor.ai.

## Dependencies

All in `dependencies.py`. Every one of them accepts either credential type —
JWT is tried first when an `Authorization: Bearer` header is present, otherwise
the `X-API-Key` header is used.

| Dependency                                      | Returns | Notes                                                                   |
| ----------------------------------------------- | ------- | ----------------------------------------------------------------------- |
| `get_current_user`                              | `User`  | 401 if unauthenticated.                                                 |
| `get_optional_user`                             | `User \| None` | Never raises for missing credentials.                             |
| `get_current_user_with_graph`                   | `User`  | Reads `graph_id` from the path and validates access.                    |
| `get_current_user_with_graph_or_url_token`      | `User`  | As above, plus the `?token=` door. MCP only.                            |
| `get_current_user_sse`                          | `User`  | As `get_current_user`, plus the `?token=` JWT door.                     |
| `get_current_user_with_repository_access`       | `User`  | Shared repositories (SEC, etc.); resolves subgraphs to the parent.      |
| `get_repository_user_dependency(repo, op_type)` | factory | Builds the above bound to a specific repository and operation type.     |

```python
from robosystems.middleware.auth.dependencies import get_current_user_with_graph

@router.post("/v1/graphs/{graph_id}/expensive-operation")
async def expensive_operation(
    graph_id: str,
    user: User = Depends(get_current_user_with_graph),
):
    return {"status": "success"}
```

`get_current_user_with_graph` returns the `User` directly — it does not return a
tuple. The validated `graph_id` is the path parameter you already declared.

### Membership is not write access

`get_current_user_with_graph` proves *membership*. It does not prove the user
may mutate the graph: the `viewer` role is read-only. `require_graph_write_role(user_id, graph_id)`
is the single shared write gate — REST command operations, the extensions
registrar, content operations, and the hand-written lifecycle operations all
call it, and the MCP surface enforces the same through
`validate_mcp_access(..., "write")`. It raises 403 and emits an
`AuthorizationDenied` audit event. Any new write path must go through it.

Failed API-key validation and failed graph access return the *same* 403 with
the same message. That conflation is intentional — it denies an attacker an
oracle for whether a key is valid.

## Caching

`cache.py` caches both positive and negative results in Valkey, keyed by the
SHA-256 of the API key (never the raw key):

- API key validation → user data and `is_active`
- Graph access for an API key → `(hashed_key, graph_id)` → bool
- JWT user data → keyed alongside `session_version`, so a session bump
  invalidates it
- JWT graph access → `(user_id, graph_id)` → bool, capped at 10 minutes even
  when the JWT TTL is longer

TTLs come from `TuningConfig` (`get_cache_api_key_ttl()`,
`get_cache_jwt_ttl()`), so they are SSM-tunable at runtime without a redeploy.
Valkey database numbers come from `config/valkey_registry.py` — never hardcode
one.

`cache_validator.py` re-checks cached entries against the database and refreshes
stale ones. `distributed_lock.py` provides a Valkey-backed lock used to prevent
cache stampedes when many requests miss the same key at once.

The cache is the reason a permission change may not take effect instantly.
When revoking access, invalidate explicitly rather than waiting out the TTL:

```bash
just admin dev cache info auth
just admin dev cache keys auth --pattern "apikey:*"
just admin dev cache keys auth --pattern "revoked_jwt:*"
```

## Admin authentication

`admin.py` is a separate path from the user-facing flow. `AdminAuthMiddleware`
compares a bearer token in constant time against the admin key in AWS Secrets
Manager, exposed via the `admin_auth` singleton and the `require_admin`
decorator. It guards the `/admin/v1/*` surface, which the ALB additionally
rejects from the public listener.

## Password policy

Enforced in `robosystems/security/password.py` (`PasswordSecurity`), not in this
package. Minimum 12 characters, maximum 128, upper/lower/digit/special all
required, at least 8 unique characters, a minimum strength score of 60, and a
weak-pattern blocklist. Hashing is bcrypt at 14 rounds; passwords are truncated
to bcrypt's 72-byte input explicitly so long passwords keep working under
bcrypt 5.x.

## Maintenance

`maintenance.py` holds cleanup helpers, not request middleware:
`cleanup_expired_api_keys` deactivates keys past their `expires_at`, and
`cleanup_jwt_cache_expired` reports JWT cache statistics (JWT cache entries
expire on their own via Valkey TTL).

## Configuration

```bash
JWT_SECRET_KEY=...   # minimum 32 bytes
JWT_ISSUER=...
JWT_AUDIENCE=...
VALKEY_URL=redis://localhost:6379
RATE_LIMIT_ENABLED=true   # consumed by middleware/rate_limits/
```

Read these through `robosystems.config.env`, never `os.getenv()`.

## Related

- [`../rate_limits/`](../rate_limits/) — burst limiting, tier-aware
- [`../graph/README.md`](../graph/README.md) — graph routing and `graph_id` resolution
- `robosystems/security/` — password policy, audit logging, auth protection
- Platform health is `GET /v1/status`; there is no per-subsystem health route.
