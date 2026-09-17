# Authentication & API Keys

This guide shows you how to authenticate against the RoboSystems API from scripts, `curl`, MCP clients, and SDKs. The one rule to internalize up front: backend and programmatic access uses the `X-API-Key` header, JWT/Bearer tokens are a frontend concern, and OAuth 2.1 is how MCP clients sign in (covered on [AI Operators & MCP](AI-Operators-and-MCP#connecting-from-mcp-clients)).

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development).

## Three Authentication Methods at a Glance

RoboSystems has three doors into the same building. All resolve to the same `User`; they differ in who carries the credential, how it is issued, and — for OAuth — what it can reach.

| Method | Who uses it | Header | Where it comes from | Lifecycle |
| --- | --- | --- | --- | --- |
| **API key** | Scripts, `curl`, MCP clients, SDKs, CI | `X-API-Key` | **Settings → API keys** at robosystems.ai; or `POST /v1/user/api-keys` | Long-lived; you revoke or expire it explicitly |
| **JWT / Bearer** | The browser frontends (robosystems-app, roboledger-app, roboinvestor-app) | `Authorization: Bearer …` | `POST /v1/auth/login` issues a token | Short-lived, auto-refreshed by the frontend session layer |
| **OAuth 2.1 bearer** | MCP clients that run a consent flow — claude.ai, Claude Code, ChatGPT, Cursor, VS Code, the Docker MCP Toolkit | `Authorization: Bearer rfso…` (opaque; accepted by the MCP routes only) | The client discovers the authorization server from `https://api.robosystems.ai/v1/mcp`, you sign in, and the consent screen picks the graph — see [AI Operators & MCP](AI-Operators-and-MCP#connecting-from-mcp-clients) | 1-hour access token, 90-day refresh token rotated on use; scoped to the one graph chosen at consent; revoke with `POST /v1/oauth/revoke` |

For everything in this guide — `curl`, MCP, SDK smoke tests — use the API key. You never need to call `login` for backend testing, and OAuth is the browser-driven sign-in an MCP client runs for you.

Those three are the *credentials*. How a person proves who they are to get a JWT in the first place is a separate, configurable question: password, passkey, or an enterprise identity provider. A deployment publishes its own answer at `GET /v1/auth/providers` — see [Login Methods](#login-methods) below.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Getting an API Key](#getting-an-api-key)
- [The X-API-Key Path](#the-x-api-key-path)
- [Worked Examples](#worked-examples)
- [API Key Lifecycle](#api-key-lifecycle)
- [Graph-Scoped Access](#graph-scoped-access)
- [Organizations and Roles](#organizations-and-roles)
- [Graph Membership](#graph-membership)
- [The JWT / Bearer Boundary](#the-jwt--bearer-boundary)
- [Accounts, Passwords, and Registration](#accounts-passwords-and-registration)
- [Login Methods](#login-methods)
- [Passkeys and MFA](#passkeys-and-mfa)
- [Enterprise SSO (OIDC)](#enterprise-sso-oidc)
- [The Internal Cross-App SSO Bridge](#the-internal-cross-app-sso-bridge)
- [Troubleshooting](#troubleshooting)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Overview

Authentication in RoboSystems is credential-in-header, validated by a FastAPI dependency before any handler runs. Anonymous access is limited to the health probe (`GET /v1/status`), the public offering (`GET /v1/offering`), and the pre-login auth flow — registration, login, password reset, email verification, invitation preview, the password policy, the auth-provider posture, and the CAPTCHA config. Everything else requires a valid credential. A request carries either an `X-API-Key` header (programmatic) or an `Authorization: Bearer` header (frontend session); both resolve to the same authenticated `User`.

Access is layered. A valid credential authenticates *you*; access to a specific `graph_id` is checked separately by the graph-scoped dependency. That distinction drives the most important status-code rule on the platform: **401 means "no/invalid credential," 403 means "valid user, no access to that graph."** Keep them straight and most auth debugging resolves itself.

The full endpoint surface — request/response schemas, every field, every status code — lives in the live [API Documentation](https://api.robosystems.ai/docs). This page covers the concepts and the day-to-day tasks; it does not re-tabulate the endpoint surface.

## Prerequisites

- A RoboSystems account — sign up at [robosystems.ai/register](https://robosystems.ai/register).
- An API key (next section).
- For the graph-scoped examples, a graph. See [Quick Start](Quick-Start) for getting all three.

## Getting an API Key

In the app at robosystems.ai, open **Settings → API keys** and choose **Create**. A key can be scoped to all your graphs or to one graph. The key starts with `rfs` and is shown once — copy it before you leave the page. Keys can also be created over the API; see [API Key Lifecycle](#api-key-lifecycle).

The examples on this page read the key and a graph id from the environment:

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai
export GRAPH_ID=kg...               # from GET /v1/graphs or the app's graph selector
```

**Important:** `ROBOSYSTEMS_API_KEY` is the same variable name the MCP client uses, and it is the pattern across the RoboSystems docs. Keep the key in the environment rather than pasting the literal into commands.

## The X-API-Key Path

For all programmatic access, send your key in the `X-API-Key` header:

```bash
-H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

The header name is exactly `X-API-Key` (case-insensitive per HTTP, but use this spelling). On each request the auth dependency resolves the key to a `User`, updates the key's `last_used_at`, and attaches the user to the request context. The same header works for REST endpoints, the graph-scoped GraphQL endpoint, and MCP clients.

**Precedence — Bearer wins.** If a request carries *both* an `Authorization: Bearer` header and an `X-API-Key` header, the Bearer token is tried first and takes precedence. If a Bearer header is present it must validate or the request is rejected with 401 — the server does **not** fall back to the API key. The API key path is only taken when no Bearer header is present. This matters when debugging an SDK or browser tool that sets both: a stale Bearer token will 401 even though your API key is good. Strip the `Authorization` header when testing with an API key.

## Worked Examples

### Health Check (Unauthenticated)

`GET /v1/status` is the load-balancer health probe and needs no credential:

```bash
curl https://api.robosystems.ai/v1/status
```

**Output:**

```text
{"status":"healthy","timestamp":"...","details":{"service":"robosystems-api","version":"..."}}
```

Note that the root `/` serves the Swagger UI (HTML), and `/openapi.json` serves the live OpenAPI spec — neither is a health check. Use `/v1/status`.

### Authenticated REST GET (Whoami)

Read your current user with the key from the environment:

```bash
curl -X GET "https://api.robosystems.ai/v1/user" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json"
```

A 200 with your user record confirms the key is valid. A 401 means the key is missing, malformed, expired, or revoked.

### List Your Graphs

```bash
curl -X GET "https://api.robosystems.ai/v1/graphs" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json"
```

### Graph-Scoped GraphQL Read

The extensions GraphQL endpoint is graph-scoped at the URL level: the `graph_id` is a path parameter, never a query argument. The same `X-API-Key` authenticates you, and the endpoint validates your access to that specific graph before the resolver runs:

```bash
export GRAPH_ID=kg...               # from GET /v1/graphs or the app's graph selector

curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ fiscalCalendar { closedThrough closeTarget } }"}'
```

The `fiscalCalendar` query above is illustrative — substitute any field your deployment exposes. The thing to remember is the URL is the scope:

> Right: `{ entity { … } }` — `graph_id` comes from the URL path.
> Wrong: `{ entity(graphId: "kg_x") { … } }` — GraphQL queries do not take a `graphId` argument.

## API Key Lifecycle

API keys are managed under `/v1/user/api-keys`. The full request/response schemas are in the [OpenAPI docs](https://api.robosystems.ai/docs); the concepts you need to operate them are below.

### Key Format

A RoboSystems API key is a short literal prefix followed by 64 hex characters. There are two variants:

| Variant | Prefix | Length | Scope |
| --- | --- | --- | --- |
| **Account-wide** | `rfs` | 67 characters | Every graph your user can reach |
| **Graph-scoped** | `rfsc` | 68 characters | One graph (and its subgraphs) |

The prefix difference exists for human and incident legibility only — **the authoritative scope check is the key row's `graph_id`, never the spelling of the prefix.** Keys are stored **hashed** (bcrypt) — never in plaintext. The server cannot show you the raw key after it is created; it only ever exposes the 8-character `prefix` (the key's first eight characters, e.g. `rfs9ce10` or `rfsc9ce1`) used to identify the key in lists.

### Create a Key

```bash
curl -X POST "https://api.robosystems.ai/v1/user/api-keys" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "CI pipeline key", "description": "for nightly smoke tests"}'
```

The request fields are `name` (required), `description` (optional), `expires_at` (optional, ISO-8601, must be in the future), and `graph_id` (optional). Supplying a `graph_id` mints a **graph-scoped** key: it works only for that graph and its subgraphs, and it is the only kind of key an MCP connector URL accepts. Omit it for an account-wide key.

```bash
curl -X POST "https://api.robosystems.ai/v1/user/api-keys" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
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
curl -X GET "https://api.robosystems.ai/v1/user/api-keys" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json"
```

Each entry exposes `id`, `name`, `description`, `prefix`, `is_active`, `last_used_at`, `expires_at`, `created_at`, and `graph_id` (the key's graph scope; `null` means account-wide) — but **never** the raw key.

### Update a Key

`PUT /v1/user/api-keys/{api_key_id}` updates mutable metadata (`name` and `description`). Use the `id` (a ULID prefixed `uak`) from the list response:

```bash
curl -X PUT "https://api.robosystems.ai/v1/user/api-keys/uak_..." \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"description": "rotated 2026-06-11"}'
```

### Revoke a Key

```bash
curl -X DELETE "https://api.robosystems.ai/v1/user/api-keys/uak_..." \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
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

## Organizations and Roles

Every user belongs to an **organization**. The org is the account that owns graphs and carries billing, and it is where "who can do what" is ultimately decided. Registering normally creates a personal org with you as its sole owner; registering through an invitation link joins you to the inviting org instead (see [Registration and invitations](#registration-and-invitations) below).

Organizations are multi-user. A member holds exactly one of three roles:

| Role | What it carries |
| --- | --- |
| **`owner`** | Everything an admin can do, plus checkout, payment methods, and the Stripe customer portal. An org always has at least one owner |
| **`admin`** | Manage members and invitations, view invoices, change org settings, and manage another member's subscriptions |
| **`member`** | Belongs to the org and works in the graphs they have been granted; manages only their own subscriptions |

Two guard rails are worth knowing before you write tooling against this. **Owner promotion and demotion are not available through the member endpoints** — `PUT /v1/orgs/{org_id}/members/{user_id}` rejects both directions with a 400, because transferring ownership needs a dedicated handoff workflow rather than a role flip that could strand an org with no owner. And **no one changes their own role here**: an admin targeting themselves is rejected outright, and an owner targeting themselves runs into the owner guard.

The org surface lives under `/v1/orgs`:

| Endpoint | Purpose |
| --- | --- |
| `GET /v1/orgs` | The organizations you belong to |
| `GET \| PUT /v1/orgs/{org_id}` | Read or update the org (update requires admin or owner; only an owner may change the org type) |
| `GET /v1/orgs/{org_id}/graphs` | The graphs the org owns |
| `GET /v1/orgs/{org_id}/members` | Roster, with each member's role and join date |
| `PUT \| DELETE /v1/orgs/{org_id}/members/{user_id}` | Change a member's role, or remove them (members may remove themselves) |
| `GET /v1/orgs/{org_id}/limits` | Account-level limits, including the graph cap |
| `GET /v1/orgs/{org_id}/usage` | Usage aggregated across every graph in the org (`days` controls the lookback, default 30) |

### The invitation flow

Adding someone to an org is an invitation, not a direct write — there is no endpoint that attaches an existing account to an org you administer. An admin or owner invites an **email address that has no account yet**, and the invited person becomes a member by registering through the emailed link:

1. **Invite** — `POST /v1/orgs/{org_id}/invitations` with `{"email": "...", "role": "member"}`. The role defaults to `member`.
2. **Preview** — the registration page calls `GET /v1/auth/invitations/{token}` (anonymous) to show what is being joined: org name, invited email, role, and expiry. Unknown, expired, revoked, and already-accepted tokens all return 404.
3. **Accept** — the invitee registers with `POST /v1/auth/register` carrying `invite_token`. They join the inviting org at the invited role instead of receiving a personal org, and the token itself proves control of the mailbox, so the account skips separate email verification. The email in the registration must match the one that was invited.

Admins and owners can also list pending invitations (`GET /v1/orgs/{org_id}/invitations`), revoke a pending one (`DELETE .../invitations/{invitation_id}`), and resend (`POST .../invitations/{invitation_id}/resend` — this rotates the token, extends the expiry, and sends a fresh email).

Invitations are gated by the `ORG_MEMBER_INVITATIONS_ENABLED` feature flag. A deployment with the flag off returns `501 Not Implemented` from every invitation endpoint.

## Graph Membership

Org roles decide who can administer the account; **graph membership decides who can reach a given graph.** Grants are managed per graph under `/v1/graphs/{graph_id}/members`, and every one of the four routes requires graph admin:

| Endpoint | Purpose |
| --- | --- |
| `GET /v1/graphs/{graph_id}/members` | Everyone with access — explicit grants plus implicit org admins |
| `POST /v1/graphs/{graph_id}/members` | Grant access to an org member at a chosen role |
| `PUT /v1/graphs/{graph_id}/members/{user_id}` | Change an explicit member's graph role |
| `DELETE /v1/graphs/{graph_id}/members/{user_id}` | Revoke an explicit grant |

Graph roles are their own three-level ladder, distinct from org roles: **`viewer`** (read-only), **`member`** (read/write), and **`admin`** (full control, including managing other members).

Four rules govern the surface, and each one is a distinct error if you trip it:

- **You can only grant access to someone already in the graph's owning organization.** There is no cross-org sharing here; the invitation flow above is how a person enters the org in the first place.
- **Org owners and admins hold implicit graph admin** on every graph their org owns. They appear in the listing with `source: "org_role"` and a null `granted_at`, as opposed to `source: "explicit"` for a direct grant. Their access is managed by changing their org role — you cannot revoke it from the graph.
- **Subgraphs inherit the parent's permissions.** Calling the member endpoints on a subgraph returns 400 and points you at the parent graph.
- **Shared repositories have no member management.** They are subscription-based, so these endpoints return 403 for `sec` and its peers. See [Shared Repositories](Shared-Repositories).

```bash
curl "https://api.robosystems.ai/v1/graphs/kg1234567890abcdef/members" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

```text
{
  "members": [
    {"user_id": "user_...", "name": "...", "email": "...",
     "role": "admin", "source": "explicit", "granted_at": "..."}
  ],
  "total": 1,
  "graph_id": "kg1234567890abcdef"
}
```

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

The four endpoints above are the session flow. The rest of the account lifecycle — how a user comes into existence, and how they change their credentials — is below.

## Accounts, Passwords, and Registration

### Registration and invitations

`POST /v1/auth/register` creates a user. Without an `invite_token` it also creates a personal organization with the new user as owner; with one, the user joins the inviting org at the invited role instead (see [The invitation flow](#the-invitation-flow)). The request carries `name`, `email`, `password`, an optional `captcha_token`, and the optional `invite_token`.

Two anonymous endpoints exist so a registration form can be built correctly before anyone has a credential:

| Endpoint | Purpose |
| --- | --- |
| `GET /v1/auth/captcha/config` | Whether CAPTCHA is required and the site key to use (`null` when disabled) |
| `GET /v1/auth/password/policy` | The password requirements the form should enforce |

`POST /v1/auth/password/check` scores a candidate password against that policy, so a form can give live feedback without attempting a registration.

Email verification, where a deployment enables it, is a two-endpoint pair: `POST /v1/auth/email/verify` consumes the token from the email link (and returns a JWT, so the user lands logged in), and `POST /v1/auth/email/resend` re-sends it for the authenticated user.

### Resetting a forgotten password

The reset flow is anonymous end to end, because by definition the user cannot authenticate:

1. `POST /v1/auth/password/forgot` sends the reset email. It **always** reports success, whether or not the address exists — that is deliberate, so the endpoint cannot be used to enumerate accounts.
2. `GET /v1/auth/password/reset/validate` checks a token without consuming it, so the reset page can fail fast on a dead link. It returns a masked email on success.
3. `POST /v1/auth/password/reset` sets the new password and returns a JWT for auto-login.

### Changing a known password

An authenticated user changes their own password with `PUT /v1/user/password`, supplying `current_password`, `new_password`, and `confirm_password`. The current password is re-verified; the endpoint is unavailable to SSO-only accounts.

**A password change invalidates every JWT that user holds, but leaves their API keys working.** Changing the password bumps the user's `session_version`, and any token minted before the bump stops authenticating — so every open browser session is signed out, including the one that made the change. API keys are a separate credential family and are untouched: a script or CI job running on `X-API-Key` keeps working straight through a password change. The same applies to a password *reset*, which invalidates sessions the same way.

The practical consequence: **rotating a password is not a way to cut off programmatic access.** If a key may be compromised, revoke the key (`DELETE /v1/user/api-keys/{id}`) — changing the password will not do it for you.

### Updating your profile

`PUT /v1/user` updates the authenticated user's own profile — `name` and `email`. It is the write counterpart to the `GET /v1/user` whoami call used in the [worked examples](#authenticated-rest-get-whoami) above.

## Login Methods

Everything above concerns *credentials in a header*. This section covers the other half — how a human proves identity to obtain a session in the first place. It is deployment-configurable, and a deployment publishes its own posture:

```bash
curl https://api.robosystems.ai/v1/auth/providers
```

```json
{
  "password_auth": true,
  "oidc": { "enabled": false, "provider_label": null },
  "registration": true,
  "passkeys": false
}
```

That endpoint is public and carries no secrets. It exists so a frontend renders the login page from runtime configuration rather than hardcoding a set of methods — the same frontend build serves a password-primary deployment, a passkey deployment, and an SSO-only deployment. It is a **rendering hint, never a security boundary**: the backend flags are the enforcement, and a client that ignores this endpoint gains nothing.

| Method | Flag | Notes |
| --- | --- | --- |
| Password | `PASSWORD_AUTH_ENABLED` | Bcrypt at cost 14, score-based strength policy |
| Passkey (WebAuthn) | `PASSKEYS_ENABLED` | Second factor *and* passwordless first factor |
| MFA enforcement | `MFA_ENFORCEMENT_ENABLED` | Requires org owners and admins to enroll a passkey |
| Enterprise SSO (OIDC) | `SSO_OIDC_ENABLED` | Off on the managed platform; for dedicated and self-hosted deployments |

Disabling password auth is only permitted when passkeys or OIDC are enabled — boot validation refuses a configuration in which nobody could log in.

## Passkeys and MFA

Passkeys are WebAuthn credentials, and they serve two distinct roles behind one flag.

**As a second factor**, after a password login succeeds:

| Endpoint | Purpose |
| --- | --- |
| `POST /v1/auth/mfa/options` | Get the assertion challenge for a pending login |
| `POST /v1/auth/mfa/verify` | Complete the challenge and receive the session |
| `GET /v1/auth/mfa/status` | Whether this user has a factor enrolled |
| `POST /v1/auth/mfa/recovery-codes/regenerate` | Issue a fresh set of recovery codes |

**As a passwordless first factor**, replacing the password entirely:

| Endpoint | Purpose |
| --- | --- |
| `POST /v1/auth/passkeys/login/options` | Begin a passwordless login |
| `POST /v1/auth/passkeys/login/verify` | Complete it and receive the session |

**Enrollment and management** live behind an authenticated session:

| Endpoint | Purpose |
| --- | --- |
| `POST /v1/auth/passkeys/register/options` · `/verify` | Enroll a credential |
| `GET /v1/auth/passkeys` | List enrolled credentials |
| `DELETE /v1/auth/passkeys/{id}` | Remove one |
| `POST /v1/auth/passkeys/reauth/options` | The fresh re-authentication proof enrollment requires |

Things worth knowing before you wire a frontend against this:

- **Enrollment needs more than a session.** It requires a fresh re-authentication proof on top of the session, and **API keys are refused** for it — programmatic credentials cannot enroll a second factor.
- **MFA challenge tokens are not session tokens.** They're short-lived (5 minutes), scoped to a single purpose (`login` versus `enroll`), and explicitly refused if presented as a session bearer.
- **Recovery codes**: ten, single-use, stored hashed, regenerable. They are shown once.
- **Stored credentials hold no secrets** — the public key, a signature counter, transports, and backup flags. There is nothing in the record to steal and replay.
- **The relying-party identity is per-deployment**, derived from the deployment's root domain (overridable with `PASSKEY_RP_ID` / `PASSKEY_ORIGIN`). Each deployment is its own credential zone; a passkey enrolled on one does not exist on another.
- **Enforcement keys on the password door.** When `MFA_ENFORCEMENT_ENABLED` is on, org owners and admins must enroll a passkey before a password login yields a session. Enrolled users are always challenged once passkeys are on, whether or not enforcement is.

## Enterprise SSO (OIDC)

A real identity-provider sign-in — Okta and other OIDC providers — paired with SCIM 2.0 provisioning, so the customer's IdP is the authoritative roster and unassigning a user ends their access.

This is **off by default and not enabled on the managed platform**. It exists for deployments serving a single organization: a dedicated deployment or a self-hosted fork.

```
GET /v1/auth/oidc/login     → 302 to the identity provider
GET /v1/auth/oidc/callback  → validates the ID token, resolves the user,
                              hands off to the session bridge
```

The design rule that surprises people: **login never creates an account.** SCIM provisions users; OIDC only resolves already-provisioned ones. An authenticated IdP subject with no SCIM-provisioned local user is refused, by design.

Both endpoints are redirect-based and deliberately absent from the OpenAPI spec — they are browser surface, not SDK surface, and you do not exercise them with `curl`.

**Full setup, IdP configuration, and the control story: [Enterprise SSO & SCIM](Enterprise-SSO-and-SCIM).**

## The Internal Cross-App SSO Bridge

> **"SSO" means two different things in this platform.** The section above is identity-provider federation. This one is an internal handoff between the platform's own frontends. They are unrelated mechanisms that happen to share a word — and they meet at exactly one point, which is that the OIDC callback finishes by handing the browser to the bridge's completion path, so there is a single place where a session token is issued.

The bridge lets a user authenticated in one frontend app (say robosystems-app) move to a sibling app (roboledger-app, roboinvestor-app) without re-entering credentials. It is a three-step token handoff:

| Step | Endpoint | What it does |
| --- | --- | --- |
| 1. Generate | `POST /v1/auth/sso-token` | The originating app (authenticated) mints a short-lived, single-use token |
| 2. Exchange | `POST /v1/auth/sso-exchange` | The target app exchanges that token for a session |
| 3. Complete | `POST /v1/auth/sso-complete` | The session is finalized in the target app |

The handoff token is not a session token: it is single-use, expires in five minutes, and is refused if presented as a bearer. The session version is captured at exchange and re-checked at completion, so a password change between the two steps kills the handoff rather than completing it.

This is a frontend-to-frontend concern built on the Bearer/session layer. You do not invoke it from `curl` when testing the backend — like the JWT path, it sits on the frontend door. It is documented here so the full authentication picture is complete; for programmatic access, stay on the `X-API-Key` path.

## Troubleshooting

### 401 Unauthorized on a Request You Expect to Work

The credential is missing, malformed, expired, or revoked. Check, in order:

- The `X-API-Key` header is present and spelled correctly.
- You are sending a live key: `$ROBOSYSTEMS_API_KEY` expands to an `rfs...` value, not an empty string.
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

`/v1/status` is the load-balancer health probe and needs no credential. If it returns 200 but authenticated calls 401, the API is up and the problem is your credential, not the server. Check the key under **Settings → API keys** at robosystems.ai, or create a new one and export it again.

### Every Browser Session Signed Out After a Password Change

Expected. Changing or resetting a password bumps the user's `session_version`, which invalidates every JWT issued before that moment — including the session that made the change. Log in again to get a fresh token. API keys are unaffected and keep working. See [Changing a known password](#changing-a-known-password).

### Health Check Hits the Wrong Path

`GET /health` and `GET /v1/health` do not exist. The root `/` is the Swagger UI (HTML), not a probe.

**Solution:** Use `GET /v1/status` for health checks and `GET /openapi.json` for the live spec.

## Related Documentation

**Wiki Guides:**

- **[Quick Start](Quick-Start)** - Create an account, your first API key, and a graph on robosystems.ai
- **[Enterprise SSO & SCIM](Enterprise-SSO-and-SCIM)** - Identity-provider federation and SCIM 2.0 provisioning for dedicated and self-hosted deployments
- **[Security & Compliance](Security-and-Compliance)** - The platform's built-in controls and the optional compliance stacks
- **[Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy)** - The per-graph access model and 401-vs-403 semantics
- **[Graph Operations](Graph-Operations)** - Where authenticated requests go: operation envelopes and `X-API-Key` in practice
- **[Credits and Billing](Credits-and-Billing)** - What the organization carries: credit pools, account limits, and the role-gated billing endpoints

**API Reference:**

- **[API Documentation](https://api.robosystems.ai/docs)** - Live OpenAPI reference with request/response schemas for every endpoint
- **OpenAPI spec**: [https://api.robosystems.ai/openapi.json](https://api.robosystems.ai/openapi.json) (machine-readable)

**Codebase Documentation:**

- **[Authentication Middleware](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/auth/README.md)** - The authentication system internals
- **[Graph Routing](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/graph/README.md)** - Multi-tenant graph routing and per-graph access
- **[GraphQL Extensions](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md)** - The graph-scoped GraphQL surface and its auth

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
