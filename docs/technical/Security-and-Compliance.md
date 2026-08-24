# Security & Compliance

This guide covers RoboSystems' security posture and the optional, off-by-default compliance infrastructure you can enable when you deploy a fork. It's written for operators standing up their own deployment — if you want the exhaustive control catalog with file-level implementation references, see [`SECURITY.md`](https://github.com/RoboFinSystems/robosystems/blob/main/SECURITY.md) in the repo.

## The shared-responsibility line

RoboSystems runs on AWS, so security splits three ways — and knowing which layer you own matters if you ever pursue an audit:

| Layer | Owned by | Examples |
| --- | --- | --- |
| Physical / infrastructure | **AWS** | data centers, hypervisor, hardware — inherited via AWS's own SOC reports |
| Platform controls (in the code) | **RoboSystems** (you inherit by deploying) | auth, encryption, tenant isolation, rate limiting, audit logging |
| Your deployment | **You** | AWS account IAM, who holds keys, which optional stacks you enable, your data governance |

## Table of Contents

- [Built-in Controls](#built-in-controls)
- [Optional Compliance Stacks](#optional-compliance-stacks)
- [Enabling the Optional Stacks](#enabling-the-optional-stacks)
- [SOC 2 Posture for Forks](#soc-2-posture-for-forks)
- [Reporting a Vulnerability](#reporting-a-vulnerability)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Built-in Controls

These are always on in any deployment — you inherit them by running the platform:

- **Authentication** — JWT (30-min TTL, JTI revocation, fails closed) and API keys (`rfs` prefix, `rfsc` for graph-scoped keys, bcrypt cost-12 at rest, per-key expiry). Login methods are deployment-configurable — password, WebAuthn passkeys (second factor *and* passwordless first factor, with optional enforcement for org owners and admins), or an enterprise identity provider — and a deployment publishes its own posture at `GET /v1/auth/providers`. See [Authentication & API Keys](Authentication-and-API-Keys).
- **Identity lifecycle** — optional OIDC single sign-on and SCIM 2.0 provisioning bind account lifecycle to the customer's identity provider, so assignment and unassignment there are the joiner/leaver control. Deactivation is a genuine kill switch: it invalidates live sessions *and* revokes the user's API keys. Off by default and not enabled on the managed platform. See [Enterprise SSO & SCIM](Enterprise-SSO-and-SCIM).
- **Access control** — role-based, with a distinct vocabulary per scope: org (owner / admin / member) and graph (admin / member / viewer), where org owners and admins hold implicit graph admin over graphs their org owns. Shared repositories use subscription-based access instead of membership, and are **read-only for every subscriber** — writes against a shared repository or its subgraphs are refused unconditionally at the query surface.
- **Tenant isolation** — every graph is a separate database; extensions OLTP uses schema-per-graph PostgreSQL isolation, with `search_path` re-stamped at the top of each session, schema names validated against a strict pattern before use, and the binding cleared before a connection returns to the pool. Enforced at the data layer, not just in app code. A CI structural test additionally guards the migration path, failing any extensions migration whose tenant-table DDL is not fanned out to every existing tenant schema.
- **Encryption** — TLS 1.2+ in transit; AES-256 at rest (RDS, EBS, S3); Fernet field-encryption for OAuth/connection credentials.
- **Abuse protection** — sliding-window rate limiting, progressive auth-failure backoff with IP threat levels, CAPTCHA, Cypher query analysis, and load-shedding admission control.
- **Admin isolation** — the shared admin surface is not internet-reachable (the ALB returns 403 for `/admin/v1/*`); admin access is SSO/SSM-tunnel only.
- **Secrets** — sourced from AWS Secrets Manager (fails closed in prod), never committed.

## Optional Compliance Stacks

Everything below is **off by default** to keep costs down, and gated by a GitHub Actions variable. Each deploys via its own CloudFormation stack through the normal pipeline — there's no separate deployment flow.

| Capability | Toggle (GitHub variable) | What it does |
| --- | --- | --- |
| **WAF** | `WAF_ENABLED_{PROD,STAGING}` | Rate limiting, SQLi/XSS managed rules, payload caps, optional geo-blocking |
| **CloudTrail** | `CLOUDTRAIL_ENABLED` | Multi-region API audit trail with log-file validation → S3; also enables **SSM Session Manager command logging** for bastion access |
| **VPC Flow Logs** | `VPC_FLOW_LOGS_ENABLED` | Network traffic capture → S3 |
| **Security Baseline** | `SECURITY_ENABLED` (+ `SECURITY_CONFIG_ENABLED`) | Account-global detective controls: GuardDuty, Security Hub (FSBP), IAM Access Analyzer, Amazon Inspector, and — gated separately — AWS Config |
| **Audit Retention** | `AUDIT_ENABLED_{PROD,STAGING}` | Forwards security-audit records to a long-retention (~13-month) S3 bucket via a CloudWatch Logs → Firehose pipeline |
| **Secrets Rotation** | `SECRETS_ROTATION_ENABLED_{PROD,STAGING}` | Monthly Lambda rotation of the Postgres password, Valkey token, and API keys |

A few of these are **account-global singletons** (CloudTrail, the security baseline) — deployed once and shared across your prod and staging environments, so their variables are single-valued rather than per-environment.

## Enabling the Optional Stacks

Set the variable and deploy:

```bash
# List / set compliance variables
just gha-list SECURITY
just gha-set SECURITY_ENABLED true
```

Then run a deploy (via the `staging.yml` / `prod.yml` workflows). The gated job creates or updates the stack; setting the variable back to `false` skips future deploys but does **not** tear down an already-created stack (delete it explicitly if you want it gone).

**One prerequisite for the Security Baseline:** it needs additional deploy-role IAM permissions (for GuardDuty, Security Hub, Config, Inspector, Access Analyzer). Re-run `just bootstrap` once before the first enable so the OIDC deploy role picks up those grants — otherwise the stack fails to create. See the [Bootstrap Guide](Bootstrap-Guide).

**Cost note:** the free/near-free detective services (GuardDuty, Security Hub, Inspector, Access Analyzer) have free trials and are cheap to leave on; **AWS Config** is the cost outlier (it scales with resource count), which is why it's behind its own `SECURITY_CONFIG_ENABLED` switch — leave it off until you actually need it.

## SOC 2 Posture for Forks

RoboSystems is **aligning its controls to SOC 2 in preparation for a future audit** — the platform is not SOC 2 attested today, and this page describes the groundwork rather than a completed report. A SOC 2 report attests to *an organization operating a system over time* — it is not a property of the source code, and nothing here is a SOC 2 "certification" by itself. What that means for a fork:

- **You inherit the control _design_.** A large share of the technical controls a SOC 2 expects map to controls implemented in the platform and its optional stacks — a foundation you build on, not a finished attestation. That's a head start toward a **Type I** report (controls designed and in place at a point in time).
- **You do not inherit operating evidence.** A **Type II** report attests that controls operated effectively across an observation window (commonly 3–12 months). That evidence can only be produced by *your* deployment running over time — you can't inherit someone else's.
- **AWS covers the infrastructure layer** via its own SOC reports, which you rely on as a subservice organization (the same way any AWS customer does).
- **The report itself is your engagement.** Only a licensed CPA firm can issue a SOC 2 report. Turn the technical controls on *before* engaging one, because the observation window only counts time during which the controls were actually running.

In short: RoboSystems aligns its control **design** to what SOC 2 expects and ships the optional **evidence infrastructure**; achieving a report is your organization's engagement with an auditor.

> This page is operator guidance, not legal or attestation advice. Confirm scope and requirements with the CPA firm you engage.

## Reporting a Vulnerability

Report security issues privately — please don't open a public issue:

- **Email** — [security@robosystems.ai](mailto:security@robosystems.ai)
- **GitHub Security Advisory** — [open a private advisory](https://github.com/RoboFinSystems/robosystems/security/advisories/new)

The machine-readable contact record is served at [`/.well-known/security.txt`](https://api.robosystems.ai/.well-known/security.txt) (RFC 9116), and the disclosure policy is [`SECURITY.md`](https://github.com/RoboFinSystems/robosystems/blob/main/SECURITY.md). Every report gets reviewed. There is no paid bug bounty program and no monetary rewards are offered.

## Related Documentation

**Wiki Guides:**
- **[Bootstrap Guide](Bootstrap-Guide)** — set up a fork, OIDC, and the deployment variables
- **[Authentication & API Keys](Authentication-and-API-Keys)** — credentials and access
- **[Enterprise SSO & SCIM](Enterprise-SSO-and-SCIM)** — identity-provider federation, SCIM provisioning, and the joiner/leaver control
- **[Graphs & Multi-Tenancy](Graphs-and-Multi-Tenancy)** — tenant isolation model
- **[Architecture Overview](Architecture-Overview)** — infrastructure and deployment

**Codebase Documentation:**
- **[SECURITY.md](https://github.com/RoboFinSystems/robosystems/blob/main/SECURITY.md)** — the full control catalog with implementation references

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
