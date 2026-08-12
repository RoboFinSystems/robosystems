# Authentication Middleware

Authentication and per-graph authorization for the platform. This package
exposes FastAPI **dependencies**, not ASGI middleware classes — you protect an
endpoint by declaring the dependency in the route signature. It handles three
credential types (JWT, API key, and the SCIM bearer token), caches validation
results in Valkey, and enforces graph access on every authenticated
graph-scoped route.

JWT and API key are the two general-purpose credentials and are
interchangeable across the dependencies below. The SCIM token is not: it is
accepted **only** at `/scim/v2` and nowhere else, and the general dependencies
never accept it. Enterprise **OIDC login** is a different kind of thing again —
a sign-in *method*, not a credential the API accepts; it terminates in an
ordinary platform JWT.

Rate limiting is a separate concern and lives in
[`../rate_limits/`](../rate_limits/). This package's `__init__.py` re-exports
`graph_scoped_rate_limit_dependency` and
`subscription_aware_rate_limit_dependency` for convenience but implements
neither.

## The credential surfaces

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

**SCIM bearer tokens (`Authorization: Bearer …` at `/scim/v2`) are for an
IdP.** `scim.py:require_scim_org` is their entire auth path: it resolves the
presented bearer to an active, unexpired `scim_tokens` row, stamps
`last_used_at`, and returns the token's **org** — SCIM is org-scoped, not
user-scoped, and publishes `request.state.scim_org_id` for the handlers.
Storage mirrors `UserAPIKey` (bcrypt `token_hash`, indexed fingerprint, stored
`prefix`), but the plaintext carries its own `rfss` prefix, so a SCIM token can
never satisfy the `^rfsc?[0-9a-f]{64}$` API-key format and vice versa. Failures
answer in the SCIM error envelope (`{"schemas": [...], "detail": ..., "status":
"401"}`), not FastAPI's `{"detail": ...}`, and never distinguish unknown from
revoked from expired.

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

### SSO — one word, two mechanisms

**Cross-app handoff** is the original one and involves no IdP at all.
`jwt.py:create_sso_token` mints a single-use handoff JWT with a 300-second TTL,
carrying `{"sso": true}` and a `token_id` used to enforce single use. These
tokens have no `jti`, cannot be revoked, and `verify_jwt_claims` refuses them as
session bearers. Reuse of a spent token is treated as a possible replay of a
leaked credential and logged as such. Used for handoff between
app.robosystems.ai, roboledger.ai, and roboinvestor.ai.

**Enterprise SSO (OIDC login)** is the newer one: a real IdP sign-in for
dedicated deployments. The kernel is `operations/oidc.py`; the browser-facing
endpoints are `routers/auth/oidc.py`, mounted **only** when
`SSO_OIDC_ENABLED=true` (`routers/auth/__init__.py`) and both
`include_in_schema=False`, because they speak in redirects rather than JSON —
deliberately not SDK surface. The managed platform never mounts them.

```
GET /v1/auth/oidc/login     → 302 to the IdP (also the IdP-initiated login URI)
GET /v1/auth/oidc/callback  → validates the ID token, resolves the user,
                              302s to the login home's ?session_id= bridge
```

Things worth knowing before touching this path:

- **The IdP authenticates; the platform mints.** The callback does not issue a
  JWT. It writes an `sso_session:{id}` payload and hands the browser to the same
  `sso-complete` consumption path the cross-app bridge uses — which is why the
  two "SSO" concepts meet at exactly one point.
- **Resolution is link-only — login never creates a user.**
  `resolve_oidc_user` looks up the `user_identities` link on `(issuer, sub)`. On
  a miss there is exactly one fallback: an active user whose email matches, who
  carries a SCIM `external_id`, and who has no identity for this issuer yet. The
  `external_id` predicate is load-bearing — without it anyone controlling a
  matching mailbox in the customer's IdP could bind onto a locally-created
  account. An explicit `email_verified: false` is refused (a missing claim is
  tolerated; Entra omits it).
- **`is_active` is re-checked even on a valid link**, because IdP assignment and
  SCIM assignment drift apart (Okta runs them as two apps).
- **Failures redirect, they do not return JSON** — `?reason=` on the login home,
  with the real distinction in the audit log. A top-level browser navigation
  that dead-ends on an error body is unrecoverable UX.
- **CSRF/replay defenses**: PKCE, a nonce, GETDEL flow state (a replayed state
  reads as invalid), a path-scoped `oidc_flow` browser-binding cookie compared
  with `compare_digest`, and a third-party-initiated `iss` that is validated
  against the configured issuer rather than followed.

### SCIM provisioning

`SCIM_ENABLED=true` mounts `routers/scim` at `/scim/v2` (`main.py`) — gated
independently of OIDC, since a deployment may run either alone. The surface is
Users CRUD (`GET`/`POST /Users`, `GET`/`PUT`/`PATCH`/`DELETE /Users/{id}`) plus
the `ServiceProviderConfig` / `ResourceTypes` / `Schemas` conformance probes,
all `include_in_schema=False`. Every route sits behind two rate buckets and then
`require_scim_org`: an IP bucket first, because each presented bearer keys its
own token bucket and a caller inventing a new bearer per request would otherwise
never be metered, then the per-token bucket for legitimate IdP traffic.
SCIM-provisioned users join the org with `SSO_DEFAULT_ROLE`.

### Deployment posture

`GET /v1/auth/providers` (`routers/auth/providers.py`) reports which methods a
deployment offers — `password_auth`, `oidc` (with `provider_label`),
`registration`, `passkeys` — so the login surface renders posture from runtime
config instead of hardcoding it.

Posture is enforced, not merely advertised: `require_password_auth` in
`routers/auth/utils.py` guards *every* password-credential endpoint (login,
registration, reset, change) with a 403 when `PASSWORD_AUTH_ENABLED=false`. A
login the UI hides but the API still accepts would let a password bypass the
IdP's MFA and conditional-access policies. `config/validation.py` refuses to
boot a deployment that disables password auth without enabling OIDC, that
enables OIDC without a complete connection, that sets a privileged
`SSO_DEFAULT_ROLE`, or that runs OIDC/SCIM with `RATE_LIMIT_ENABLED=false` —
the OIDC and SCIM rate buckets are no-ops without it.

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

# Auth posture — reported by GET /v1/auth/providers
PASSWORD_AUTH_ENABLED=true    # false requires SSO_OIDC_ENABLED=true

# Enterprise SSO (OIDC). Off by default; the managed platform never sets these.
# The flags live in SSM Parameter Store (/features/); the whole connection
# block below is read via get_secret_value — env vars locally, the
# robosystems/{env} base secret when deployed (JWT_ISSUER precedent; nothing
# SSO-related flows through CloudFormation).
SSO_OIDC_ENABLED=false
SSO_OIDC_ISSUER=            # IdP org authorization server, https:// when deployed;
                            # no query/fragment. For Okta this is https://<org>.okta.com,
                            # NOT /oauth2/default (that mints API tokens, not sign-in ID tokens)
SSO_OIDC_CLIENT_ID=
SSO_OIDC_CLIENT_SECRET=
SSO_OIDC_PROVIDER_LABEL=SSO # button label on the login home

# SCIM 2.0 provisioning. Gated independently of OIDC.
SCIM_ENABLED=false
SSO_DEFAULT_ROLE=member     # validation rejects anything but member|admin

# One-org boundary. Set AFTER the first `scim bootstrap` mints the enterprise
# org (validation warns while unset + deployed). Once pinned: SCIM bearers for
# other orgs 401, bootstrap only targets this org, and OIDC first-login
# linking requires membership in it.
ENTERPRISE_ORG_ID=

# ID-token claim compared against the SCIM-stamped external_id at first-login
# linking (equality required — presence is not provenance). Okta sends its
# user id as both SCIM externalId and OIDC sub, so the default fits; Entra
# pairs externalId (objectId) with `oid`.
SSO_OIDC_BINDING_CLAIM=sub
```

The OIDC redirect URI is derived, not configured:
`{ROBOSYSTEMS_API_URL}/v1/auth/oidc/callback`. Register that exact value with
the IdP.

Read these through `robosystems.config.env`, never `os.getenv()`.

## Related

- [`../rate_limits/`](../rate_limits/) — burst limiting, tier-aware
- [`../graph/README.md`](../graph/README.md) — graph routing and `graph_id` resolution
- `robosystems/security/` — password policy, audit logging, auth protection
- Platform health is `GET /v1/status`; there is no per-subsystem health route.
