# Enterprise SSO & SCIM

This guide covers the enterprise identity plane: **OIDC single sign-on** for login and **SCIM 2.0** for user provisioning and deprovisioning. It's written for operators standing up a deployment for an organization that runs its own identity provider — and for the security reviewer on the other side of that conversation, who needs to know what the controls actually are.

Both surfaces are **flag-gated and off by default**. They exist for deployments that serve a single organization — a dedicated deployment or a self-hosted fork — and they are deliberately **not enabled on the managed multi-tenant platform**, where accounts are self-registered and belong to many unrelated organizations. Turning them on is a deployment decision, not a plan upgrade: the code ships in the repository under Apache-2.0, so a fork gets the whole capability with no license gate.

## The one rule to internalize

**SCIM provisions. OIDC authenticates. Login never creates an account.**

This is the load-bearing design decision, and everything else follows from it:

- Your IdP pushes users to the platform over SCIM. That's the only way an account comes into existence on an SSO deployment.
- OIDC login then *resolves* an already-provisioned account. If the identity provider authenticates someone SCIM never created, the login is refused — there is no just-in-time provisioning, and there is no path where a valid token at a trusted IdP mints a local account.
- Consequently, **SCIM must be configured and assignments must have flowed before the first login attempt.** An OIDC login against an unprovisioned user is *supposed* to fail, and it will.

The payoff is that your IdP's assignment list is the authoritative roster, in both directions. Unassign someone and their access ends — which is the joiner/leaver control an auditor will ask you to demonstrate.

## Table of Contents

- [What's Supported](#whats-supported)
- [Prerequisites](#prerequisites)
- [Enabling It](#enabling-it)
- [Configuring the Identity Provider](#configuring-the-identity-provider)
- [Verifying the Joiner/Leaver Path](#verifying-the-joinerleaver-path)
- [How the Login Flow Works](#how-the-login-flow-works)
- [Security Properties](#security-properties)
- [Rotating and Revoking the SCIM Token](#rotating-and-revoking-the-scim-token)
- [Troubleshooting](#troubleshooting)
- [Related Documentation](#related-documentation)
- [Support](#support)

## What's Supported

Deliberately narrow. The surface implements what a real IdP integration needs and stops there.

| Capability | Status |
| --- | --- |
| OIDC authorization-code login with PKCE | ✅ Supported |
| One IdP connection per deployment | ✅ By design — not a multi-connection broker |
| IdP-initiated login (dashboard app tile) | ✅ Supported |
| SCIM 2.0 `/Users` — create, read, update, deactivate, delete | ✅ Supported |
| SCIM service discovery (`ServiceProviderConfig`, `ResourceTypes`, `Schemas`) | ✅ Supported |
| SCIM filtering | ⚠️ `userName eq "…"` only — the dedupe probe IdPs send before creating |
| SCIM `/Groups` | ❌ Not implemented — users only |
| SCIM bulk operations, sort, ETag, password change | ❌ Not supported (declared as such in `ServiceProviderConfig`) |
| Just-in-time user creation at login | ❌ Deliberately not supported — see [the one rule](#the-one-rule-to-internalize) |
| SAML | ❌ Not implemented — OIDC only |
| Role/group mapping from IdP claims | ❌ Provisioned users join at one configured role |

**Validated against Okta**, end to end, including the console sequence below. Microsoft Entra sends some non-RFC payload shapes; the deactivation path tolerates them, but Entra is **not a validated integration** — treat it as untested and budget for iteration. Other SCIM-conformant providers should work and have not been exercised.

## Prerequisites

- A deployment you control — dedicated or self-hosted. See the [Bootstrap Guide](Bootstrap-Guide) for standing one up.
- Admin access to the customer's identity provider.
- `RATE_LIMIT_ENABLED=true`. The OIDC and SCIM surfaces carry their own rate buckets, which are inert without it; boot validation refuses to start otherwise.
- Access to the deployment's admin CLI (`just admin <env> …`), which reaches the admin plane over an SSM tunnel. The admin surface is not internet-reachable.

## Enabling It

**The sequence matters and is not atomic.** The org id you need to pin doesn't exist until after the first bootstrap, so this is four steps with two restarts — not one config change.

Flags live in SSM Parameter Store; the connection block lives in the environment's Secrets Manager secret. Both are read once at process start, so a restart is required after each. See [Bootstrap Guide → SSM Parameters & Secrets Manager](Bootstrap-Guide#ssm-parameters--secrets-manager) for the mechanics.

### 1. Turn on the flags and configure the connection

```bash
just ssm-set prod features/SSO_OIDC_ENABLED true
just ssm-set prod features/SCIM_ENABLED true
```

The two are gated independently — a deployment can run SCIM provisioning without OIDC login, or the reverse.

Then merge the connection block into the `robosystems/{env}` secret:

| Key | Value |
| --- | --- |
| `SSO_OIDC_ISSUER` | The IdP's issuer URL. For Okta this is the **org authorization server** (`https://<org>.okta.com`), *not* `/oauth2/default` — the wrong one surfaces as an `iss` mismatch at login |
| `SSO_OIDC_CLIENT_ID` | From the IdP's OIDC app |
| `SSO_OIDC_CLIENT_SECRET` | From the IdP's OIDC app |
| `SSO_OIDC_PROVIDER_LABEL` | What the login button says (e.g. `Okta`); defaults to `SSO` |
| `SSO_OIDC_BINDING_CLAIM` | The ID-token claim compared against the SCIM-stamped `externalId`. Default `sub` (correct for Okta); Entra deployments use `oid` |
| `SSO_DEFAULT_ROLE` | Org role provisioned users join at — `member` or `admin` only. Boot validation refuses `owner` |

Restart the API.

### 2. Bootstrap the org and mint the SCIM token

```bash
just admin prod scim bootstrap --org-name "Acme Inc"
```

This creates the enterprise organization and prints a SCIM bearer token **once**. It is never recoverable — paste it into the IdP's SCIM connector immediately. Default lifetime is 365 days.

Note the org id it prints.

### 3. Pin the org boundary

Write that org id into `ENTERPRISE_ORG_ID` in the same secret, and restart again.

Until you do, the deployment logs a boot **warning** rather than an error — deliberately, because a hard failure here would deadlock the sequence (you cannot pin an id that does not yet exist). Don't leave it unpinned: an unpinned deployment accepts a valid SCIM bearer for *any* org.

Once pinned:

- SCIM bearers scoped to any other org are refused with the same generic 401 as an invalid token — no reason leak.
- `scim bootstrap` refuses to create a second org (re-mint against the pinned one with `--org-id`).
- OIDC resolution requires membership in the pinned org on **every** login, not just at first link.

### 4. Turn off password login (optional)

```bash
just ssm-set prod features/PASSWORD_AUTH_ENABLED false
```

On a deployment where the IdP is the only intended door, this closes the others. Boot validation refuses the combination if it would lock everyone out — disabling password auth requires OIDC or passkeys to be enabled.

Clients read the resulting posture from `GET /v1/auth/providers`, which returns which methods this deployment offers, so the login page renders from runtime config rather than a hardcoded assumption.

## Configuring the Identity Provider

The proven path is Okta, and it needs **two plain app integrations** — SCIM cannot ride on an OIDC app.

> Do not use Okta's "Your Apps" / OIN wizard. That's the catalog *submission* path: it creates no app instance in the org and dead-ends in a review queue.

### The OIDC app (login)

Applications → Create App Integration → **OIDC** → **Web Application**.

- **Sign-in redirect URI**: `https://api.<your-domain>/v1/auth/oidc/callback`
- **Grant type**: Authorization Code only
- **Federation Broker Mode**: OFF (leaving it on hides the dashboard tile)
- Optional, for tile launch: **Initiate login URI** `https://api.<your-domain>/v1/auth/oidc/login`, with "Login initiated by: Either Okta or App"
- Assign the staff who should have access

Capture the client ID and secret for step 1 above.

### The SCIM app (provisioning)

Browse App Catalog → **"SCIM 2.0 Test App (OAuth Bearer Token)"**. It's available on the free Integrator plan, so there's no support-ticket dependency.

- **Sign-On tab**: leave the SAML shell unconfigured. Set **Application username format = Email** — the SCIM surface treats `userName` as the email address.
- **Provisioning tab → Configure API Integration**:
  - Base URL: `https://api.<your-domain>/scim/v2`
  - Bearer token: the token from step 2
  - **Import Groups: OFF** — this is a users-only surface
  - Click **Test API Credentials** (fires an authenticated filtered `GET /Users`)
- **Provisioning → To App → Edit**: enable **Create Users**, **Update User Attributes**, **Deactivate Users**. **Never enable Sync Password** — SCIM-provisioned users are passwordless by design, and the password-reset flow refuses them.

Then assign staff to the SCIM app. Each assignment fires `POST /Users` and creates a passwordless account carrying `externalId` — which is the value OIDC will later present as `sub`, and the value the link predicate compares.

**Assign SCIM before anyone tries to log in.** Link-only resolution means the callback correctly refuses anyone SCIM hasn't created yet.

### Console gotchas

- The Provisioning panel lazy-renders **blank** on hash navigation. Load the app page fresh, then click the tab.
- Deactivation arrives as `PATCH active:false` — never `DELETE`. Reactivation arrives as a PATCH plus a full-profile PUT.
- **Unassignment from the *SCIM* app is what ends access.** The OIDC app keeps its own separate assignment list, which is exactly why the login callback re-checks `is_active` on every resolution rather than trusting the token.

## Verifying the Joiner/Leaver Path

Run this end to end before the deployment carries real users. It's also the evidence artifact for a SOC 2 user-access-management walkthrough, so record it.

| Step | Expected |
| --- | --- |
| Assign a pilot user in the IdP | Appears locally as a passwordless member of the pinned org, carrying `externalId` |
| That user clicks the app tile (or hits `/v1/auth/oidc/login`) | Lands authenticated — the strict link predicate passing is itself the evidence |
| Present a deliberately wrong SCIM bearer | `401` in the SCIM error envelope |
| Unassign the user in the IdP | `PATCH active:false` arrives within seconds; their live session dies on the next request |
| That user attempts to log in again | Refused |

The last two rows are the control that matters: deactivation is the offboarding kill switch. It bumps the user's session version **and** revokes their API keys and OAuth tokens, so it terminates the browser session, any programmatic credential, and any MCP client connection the user held.

## How the Login Flow Works

Two browser endpoints, both redirect-based. They're intentionally excluded from the OpenAPI spec — they speak in 302s, not JSON, and are not SDK surface.

```
GET /v1/auth/oidc/login     → 302 to the identity provider
                              (also serves as the IdP-initiated login URI)
GET /v1/auth/oidc/callback  → validates the ID token, resolves the user,
                              → 302 to the login home's session bridge
```

The callback does not mint a session token itself. It hands the browser to the same handoff path the platform's internal cross-app bridge already uses, which is the single point where a platform JWT is issued. One issuance point, one set of session rules — see [Authentication & API Keys](Authentication-and-API-Keys) for the credential model that results.

Failures redirect back to the login page with a coarse reason code rather than returning an error body: a top-level browser navigation that dead-ends on JSON is unrecoverable for the user. The distinction between failure kinds is recorded in the audit log, not handed to the browser.

## Security Properties

The control story, for the security reviewer reading this during diligence.

**Identity resolution is link-only and provenance-gated.** Primary lookup is a stored `(issuer, subject)` link. On a miss, exactly one fallback exists — and it requires *all* of: an active user, an email match, an `externalId` that **equals** the ID-token binding claim, an `email_verified` claim that isn't explicitly false, and no existing link for that issuer. Equality is required rather than mere presence, because a matching mailbox is not provenance on its own. Once the link is written it is the lookup forever after.

**The org boundary is re-checked on every resolution, not just at link time.** An identity linked before the org was pinned, or a user removed from the pinned org without an explicit SCIM deactivation, stops resolving. The fallback path additionally checks membership *before* writing a link, so a refused resolution never leaves one behind.

**`is_active` is re-checked on every login even for a valid link.** IdP assignment and SCIM assignment are separate lists that can drift, so a SCIM-deactivated user can still arrive at the callback holding a perfectly valid ID token. They're refused there.

**The SCIM bearer is its own credential class.** Per-organization, bcrypt-hashed at rest, shown once at mint, expiring. It is accepted **only** at `/scim/v2` — user JWTs and API keys are never accepted on the SCIM surface, and a SCIM token is never accepted by any normal authentication path. Provisioning credentials cannot be used to read tenant data.

**Provisioned users cannot be provisioned into privilege.** `SSO_DEFAULT_ROLE` accepts only `member` or `admin`; both boot validation and a runtime check refuse anything else, so an IdP integration can never mint organization owners.

**The flow itself is hardened against login CSRF.** Flow state is single-use, held out-of-process with a 10-minute lifetime and consumed atomically, so a replayed or expired state reads as invalid. It carries a PKCE verifier and a nonce, and it's bound to a path-scoped HttpOnly cookie — so the browser that *completes* a login must be the one that *started* it. On IdP-initiated login the asserted issuer is validated against the configured one and a mismatch is refused rather than followed, since honoring an attacker-supplied issuer would start a code flow against a hostile authorization server.

**ID tokens are fully validated**: signature against the provider's published keys (with a single forced refetch on an unknown key id, so provider key rotation doesn't cause an outage), plus issuer, audience, expiry, nonce, and authorized-party checks.

**Everything is audited.** OIDC login denials, SCIM authentication failures, provisioning events, identity linking, and org and graph membership changes all emit security audit records. See [`SECURITY.md`](https://github.com/RoboFinSystems/robosystems/blob/main/SECURITY.md) for the audit-logging pipeline and the optional long-retention stack.

## Rotating and Revoking the SCIM Token

**Rotate by overlap.** Mint a second token against the pinned org, install it in the IdP, confirm provisioning still works, then revoke the old one:

```bash
just admin prod scim bootstrap --org-id <pinned-org-id> --token-name scim-rotation-2027
# install the new token in the IdP, verify, then:
just admin prod scim revoke-token <old-token-id>
```

Revocation takes effect immediately — the next provisioning call with that bearer is refused.

The default lifetime is 365 days and there is no expiry notification. **Calendar the rotation.** An expired token doesn't announce itself; it shows up as provisioning silently ceasing to flow, which means joiners and — more importantly — *leavers* stop syncing.

## Troubleshooting

### Login fails immediately with an `iss` mismatch

The configured issuer doesn't match what the provider asserts. On Okta this is almost always the org authorization server versus `/oauth2/default`. Use `https://<org>.okta.com`.

### Every login is refused as not provisioned

Expected if SCIM hasn't run. Confirm the user exists locally and carries an `externalId`, then confirm that value equals what the ID token presents in the claim named by `SSO_OIDC_BINDING_CLAIM` (`sub` on Okta, `oid` on Entra). A user created some other way — self-registration, an invitation — has no `externalId` and will never link.

### A user provisioned fine but login is still refused

Check org membership against the pinned `ENTERPRISE_ORG_ID`, and check `is_active`. Both are re-evaluated on every login, so a user removed from the org in the platform — or deactivated through SCIM while still assigned to the OIDC app — is refused despite holding a valid token.

### SCIM returns 401 with a token you believe is correct

Three possibilities, all answered with the same generic 401 by design: the token is unknown, revoked, or expired; or it's scoped to a different org than the pinned one. Check the token's status through the admin CLI rather than inferring from the response.

### The IdP's "Test API Credentials" fails

Confirm the base URL is `https://api.<your-domain>/scim/v2` (not the app URL, and not `/v1`), and that `SCIM_ENABLED` is actually true in the running process — the surface is not mounted at all when the flag is off, so the probe gets a 404 rather than an auth error.

### Boot warns that `ENTERPRISE_ORG_ID` is unset

You're between steps 2 and 3 of [Enabling It](#enabling-it). Finish the sequence. Leaving it unset means the deployment isn't scoped to a single org.

### The login page doesn't show an SSO button

The frontend renders from `GET /v1/auth/providers`. Call it and confirm `oidc.enabled` is true. If it isn't, the flag didn't take — flags freeze at process start, so verify the restart happened after the SSM change.

## Related Documentation

**Wiki Guides:**

- **[Authentication & API Keys](Authentication-and-API-Keys)** — the credential model SSO resolves into, plus passkeys and MFA
- **[Security & Compliance](Security-and-Compliance)** — built-in controls, optional compliance stacks, SOC 2 posture
- **[Bootstrap Guide](Bootstrap-Guide)** — standing up a deployment, and the SSM/Secrets Manager mechanics this guide's steps rely on
- **[Graphs & Multi-Tenancy](Graphs-and-Multi-Tenancy)** — what an organization owns, and the per-graph access model

**Codebase Documentation:**

- **[SECURITY.md](https://github.com/RoboFinSystems/robosystems/blob/main/SECURITY.md)** — the full control catalog with implementation references
- **[Authentication Middleware](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/auth/README.md)** — how the three credential types are validated, and why "SSO" means two different things in this codebase
- **[Admin Tools](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/admin/README.md)** — the `scim` command group

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
