# Authentication Middleware

This middleware provides authentication and authorization for the RoboSystems platform.

## Overview

The authentication middleware:

- Handles JWT token and API key authentication
- Implements caching (Valkey) for performance
- Manages multi-tenant graph access control
- Supports Single Sign-On (SSO) across RoboSystems applications
- Provides admin authentication via AWS Secrets Manager

Rate limiting is a separate concern and lives in
[`middleware/rate_limits/`](/robosystems/middleware/rate_limits/) — this
package re-exports two rate-limit dependencies (`graph_scoped_rate_limit_dependency`,
`subscription_aware_rate_limit_dependency`) for convenience but does not
implement them.

## Architecture

```
auth/
├── __init__.py                  # Module exports
├── dependencies.py              # FastAPI dependency injection (get_current_user, etc.)
├── jwt.py                       # JWT create/verify/revoke + SSO token helpers
├── utils.py                     # API key validation utilities
├── admin.py                     # AdminAuthMiddleware (AWS Secrets Manager admin key)
├── cache.py                     # Valkey caching layer (API key + JWT user data)
├── cache_validator.py           # Cache validation and refresh
├── distributed_lock.py          # Distributed locking for cache
└── maintenance.py               # API key expiry cleanup functions
```

## Authentication Methods

### 1. JWT Token Authentication

Used by frontend applications for user sessions.

**Features:**

- **Algorithm**: HS256
- **Expiration**: 30 minutes (`JWT_EXPIRY_HOURS = 0.5` in `config/constants.py`); short-lived access tokens with a refresh flow
- **Claims**: `user_id`, `jti` (for revocation), `session_version`, `iss`, `aud`, optional `device_hash`
- **Revocation**: Tokens can be revoked by `jti` (Valkey-backed revocation list with a short refresh grace period)

The token is created in `jwt.py:create_jwt_token()`:

```python
payload = {
    "user_id": user_id,
    "jti": jti,                  # JWT ID for revocation tracking
    "session_version": ...,
    "exp": datetime.now(UTC) + timedelta(hours=JWT_EXPIRY_HOURS),  # 30 minutes
    "iat": datetime.now(UTC),
    "iss": env.JWT_ISSUER,
    "aud": env.JWT_AUDIENCE,
}
```

Auth routers return the token in the response body (e.g. `expires_in = int(JWT_EXPIRY_HOURS * 3600)`); cookie-based delivery (cookie name `auth-token`) is a frontend concern and is read back by the rate-limit and SSO layers.

### 2. API Key Authentication

Used for programmatic access and integrations.

**Features:**

- **Header**: `X-API-Key`
- **Format**: `rfs` (account-wide) or `rfsc` (graph-scoped) prefix + 64 lowercase hex characters (regex `^rfsc?[0-9a-f]{64}$`) — see `utils.py:_API_KEY_FORMAT_RE`
- **Storage**: bcrypt-hashed in database; SHA-256 of the raw key is used as the cache lookup key
- **Graph Scoping**: Access is validated per graph via `validate_api_key_with_graph`
- **Activity Tracking**: Last used timestamp

**Example:**

```bash
curl -H "X-API-Key: rfs..." \
     https://api.robosystems.ai/v1/graphs/kg1a2b3c/...
```

**Graph-scoped keys** (`user_api_keys.graph_id`): a key minted with a graph
scope is valid only for that graph and its subgraphs — on *every* carriage
path — and is rejected on endpoints with no graph context (`validate_api_key`
refuses scoped keys). NULL scope = account-wide, the historical behavior. The
authoritative check is the row's `graph_id`; the `rfsc` prefix is legibility
only.

### Credentials in query parameters (the two deliberate doors)

Header carriage is the rule; exactly two routes accept a credential via a
`?token=` query parameter, both because their client cannot send custom
headers, and both already covered by the sensitive-query-param redaction in
`middleware/logging.py` and the OTel span redaction in `middleware/otel/setup.py`:

| Route | Dependency | Credential accepted | Why |
| --- | --- | --- | --- |
| `GET /v1/operations/{id}/stream` (SSE) | `get_current_user_sse` | **JWT only** (30-min TTL) | browser `EventSource` cannot set headers |
| `POST /v1/graphs/{graph_id}/mcp` | `get_current_user_with_graph_or_url_token` | **graph-scoped API key only** | MCP connector clients (claude.ai / Claude Desktop) cannot set headers |

The asymmetry is deliberate: the SSE door carries a short-lived session token,
so no extra restriction is needed; the MCP door carries a durable key, so only
graph-scoped keys are honored there and account-wide keys are hard-rejected —
the account credential must never be the one that travels in a URL. Do not add
a third query-credential door without matching this table, the redaction
lists, and a scope story.

### 3. Single Sign-On (SSO)

Seamless authentication across RoboSystems applications.

**Flow:**

1. Generate SSO token (300-second / 5-minute TTL — `jwt.py:create_sso_token`)
2. Exchange token for session (single-use, tracked by `token_id`)
3. Complete handoff

**Supported Applications:**

- RoboLedger (roboledger.ai)
- RoboInvestor (roboinvestor.ai)
- RoboSystems (app.robosystems.ai)

## Key Components

### 1. Dependencies (`dependencies.py`)

FastAPI dependency injection for authentication.

**Core Dependencies:**

#### `get_current_user`

Requires authenticated user:

```python
@router.get("/protected")
async def protected_route(
    user: User = Depends(get_current_user)
):
    return {"user_id": user.id}
```

#### `get_optional_user`

Optional authentication:

```python
@router.get("/public")
async def public_route(
    user: Optional[User] = Depends(get_optional_user)
):
    if user:
        return {"message": f"Hello {user.name}"}
    return {"message": "Hello anonymous"}
```

#### `get_current_user_with_graph`

Validates graph access:

```python
@router.get("/v1/graphs/{graph_id}/data")
async def get_graph_data(
    graph_id: str,
    auth: Tuple[User, str] = Depends(get_current_user_with_graph)
):
    user, validated_graph_id = auth
    # User has access to this graph
    return {"data": "..."}
```

### 2. Authentication Cache (`cache.py`)

Caching using Valkey. The raw API key is never used as a cache key — its
SHA-256 hash is. Caches both positive and negative results.

**Cache Types:**

- **API key validation**: hashed-key → user data + `is_active` flag
- **Graph access**: (hashed-key, graph_id) → boolean access result
- **JWT user data**: user_id → user data, keyed alongside `session_version` so a
  session bump invalidates cached JWT users
- **JWT graph access**: (user_id, graph_id) → boolean access result

JWT revocation is tracked separately in Valkey by `jti`
(`revoked_jwt:{jti}`) with a TTL equal to the token's remaining lifetime
(see `jwt.py:revoke_jwt_token`).

### 3. Rate Limiting

Rate limiting is **not** implemented in this package. It lives in
[`middleware/rate_limits/`](/robosystems/middleware/rate_limits/) —
burst-focused, subscription-tier-aware limiting with per-tier buckets
(`ladybug-standard`, `ladybug-large`, `ladybug-xlarge`). The auth
`__init__.py` re-exports `graph_scoped_rate_limit_dependency` and
`subscription_aware_rate_limit_dependency` for convenience. See that
package's source and `config/rate_limits.py` for the actual limits and
the credit model (only AI operations consume credits; storage is a
separate credit line; database operations are free).

### 4. Cache Validator (`cache_validator.py`)

Ensures cache consistency with database.

**Features:**

- **Periodic Validation**: Checks cache accuracy
- **Lazy Refresh**: Updates cache on access
- **Batch Operations**: Validates multiple entries

**Validation Flow:**

1. Check cache staleness
2. Compare with database
3. Update if different
4. Track validation metrics

### 5. Distributed Lock (`distributed_lock.py`)

Prevents cache stampedes and race conditions using a Valkey-backed lock.

```python
async with lock.acquire("user_update:123", timeout=5):
    # Critical section
    await update_user_cache()
    await update_database()
```

### 6. Admin Authentication (`admin.py`)

`AdminAuthMiddleware` authenticates admin requests using a bearer token
compared (constant-time) against the admin key stored in AWS Secrets
Manager. Exposed via the `admin_auth` singleton and the `require_admin`
decorator. Distinct from the user-facing API key / JWT flow.

### 7. API Key Maintenance (`maintenance.py`)

Cleanup helpers (not request middleware): `cleanup_expired_api_keys`
deactivates API keys past their `expires_at`, and `cleanup_jwt_cache_expired`
reports JWT cache stats (JWT cache expiry is automatic via Valkey TTL).

## Security Features

### Password Security

- **Algorithm**: Bcrypt with cost factor 12
- **Validation**: Minimum 8 characters
- **History**: Prevents reuse of last 5 passwords

### Token Security

- **JWT Secret**: Strong random key (minimum 32 bytes)
- **Short-lived access tokens**: 30-minute expiry with a refresh flow
- **Revocation**: Per-`jti` revocation list in Valkey, applied immediately on logout (with a short refresh grace window)

### API Key Security

- **Generation**: Cryptographically secure (`secrets.token_hex`, 64 hex chars)
- **Prefix**: `rfs` prefix identifies the key type
- **Hashing**: bcrypt-hashed in the database (SHA-256 used only as the cache lookup key)
- **Rotation**: Support for key rotation

### Multi-Tenant Security

- **Graph Isolation**: Complete data isolation
- **Role-Based Access**: Admin, Member, Viewer
- **Permission Caching**: 5-minute TTL
- **Audit Logging**: All access logged

## Configuration

### Required Environment Variables

```bash
# JWT Configuration
JWT_SECRET_KEY=your-secret-key-minimum-32-bytes
JWT_ISSUER=...
JWT_AUDIENCE=...
# Token lifetime is JWT_EXPIRY_HOURS in config/constants.py (0.5 = 30 min);
# it is a constant, not an env var.

# Valkey Cache
VALKEY_URL=redis://localhost:6379

# Rate limiting (configured in middleware/rate_limits/ + config/rate_limits.py)
RATE_LIMIT_ENABLED=true
```

Cache TTLs and Valkey database allocations are managed centrally
(`config/valkey_registry.py`) rather than via standalone env vars.

## Integration Examples

This package exposes FastAPI **dependencies**, not ASGI middleware classes.
Protect endpoints by declaring the dependency in the route signature.

### Protected Endpoint

```python
from robosystems.middleware.auth.dependencies import get_current_user_with_graph

@router.post("/v1/graphs/{graph_id}/expensive-operation")
async def expensive_operation(
    graph_id: str,
    user: User = Depends(get_current_user_with_graph),
):
    # get_current_user_with_graph authenticates AND validates that the
    # user has access to {graph_id} (the graph_id path param is read by
    # the dependency). Returns the authenticated User.
    return {"status": "success"}
```

For shared repositories (SEC, etc.) use `get_current_user_with_repository_access`
or the `get_repository_user_dependency(repository_id, operation_type)` factory.

### Rate Limiting

Rate limiting is applied via dependencies from `middleware/rate_limits/`
(e.g. `subscription_aware_rate_limit_dependency`,
`graph_scoped_rate_limit_dependency`), not via decorators in this package.

## Monitoring

### Key Metrics

1. **Authentication Metrics**

   - Login success/failure rates
   - Token refresh frequency
   - API key usage by key
   - SSO token exchanges

2. **Cache Metrics**

   - Hit/miss ratios
   - Eviction rates
   - Validation frequency
   - Lock contention

3. **Security Metrics**
   - Failed authentication attempts
   - Suspicious activity patterns
   - JWT revocation list size
   - Permission changes

(Rate-limit metrics are emitted by `middleware/rate_limits/`.)

### Health Checks

Platform health is exposed at `GET /v1/status` (not a per-subsystem
`/health/auth` endpoint).

## Troubleshooting

### Common Issues

1. **"Invalid token" Errors**

   ```bash
   # Check if a token's jti has been revoked (key: revoked_jwt:{jti})
   just admin dev cache keys auth --pattern "revoked_jwt:*"
   ```

2. **Rate Limit Issues**

   See `middleware/rate_limits/` and `config/rate_limits.py`; inspect
   rate-limit keys via `just admin dev cache info`.

3. **Cache Inconsistency**

   ```python
   # Force cache refresh
   await cache.invalidate_user_cache(user_id)

   # Validate all user caches
   await validator.validate_all_users()
   ```

4. **SSO Failures**

   SSO tokens are single-use JWTs (5-minute TTL) tracked by `token_id`.
   Inspect single-use tracking keys via `just admin dev cache info auth`.

## Best Practices

1. **Security**

   - Rotate JWT secrets regularly
   - Monitor failed authentication attempts
   - Use secure cookie settings in the frontend

2. **Performance**

   - Rely on the Valkey cache for authentication checks
   - Monitor cache hit rates

3. **Reliability**

   - Have fallback authentication paths
   - Test failover scenarios

4. **Maintenance**
   - Run periodic API key cleanup (`maintenance.py`)
   - Monitor the JWT revocation list size
   - Audit authentication logs
