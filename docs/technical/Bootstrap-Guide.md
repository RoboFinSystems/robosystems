# Bootstrap Guide

This guide walks you through bootstrapping a fresh fork of RoboSystems.

## Table of Contents

- [How It Works](#how-it-works)
- [Prerequisites](#prerequisites)
- [Fresh AWS Account Setup](#fresh-aws-account-setup)
- [Bootstrap](#bootstrap)
- [SSM Parameters & Secrets Manager](#ssm-parameters--secrets-manager)
- [Deploy](#deploy)
- [Multi-Repository Setup](#multi-repository-setup)
- [API Access Modes](#api-access-modes)
- [Frontend App Deployment](#frontend-app-deployment)
- [Troubleshooting](#troubleshooting)
- [Quick Reference](#quick-reference)

## How It Works

RoboSystems uses GitHub OIDC federation for AWS authentication. No AWS credentials are stored in GitHub.

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  GitHub Action  │─────▶│  OIDC Token     │─────▶│     AWS STS     │
│  Workflow       │      │  (I am repo X)  │      │  (temp creds)   │
└─────────────────┘      └─────────────────┘      └─────────────────┘
                                                          │
                                                          ▼
                                                  ┌─────────────────┐
                                                  │  Deploy to AWS  │
                                                  │  (1hr session)  │
                                                  └─────────────────┘
```

**Security Benefits:**
- No long-term credentials stored anywhere
- Credentials scoped to specific repo/branch
- 1-hour max session (can't be abused if compromised)

## Prerequisites

- AWS IAM Identity Center (SSO) enabled with admin permissions
- `aws` CLI v2 (`brew install awscli`)
- `gh` CLI authenticated (`brew install gh && gh auth login`)
- `jq` (`brew install jq`)
- `direnv` (optional - `brew install direnv`)

**GitHub token scopes:** `repo`, `admin:org`, `workflow`

Verify you have access to the correct repo:
```bash
gh repo view
# Should show your fork, e.g.: HarbingerFinLab/robosystems
```

## Fresh AWS Account Setup

Skip this section if your AWS account already has IAM Identity Center configured.

1. Log in as root user and enable MFA
2. Enable IAM Identity Center (SSO)
3. Create Permission Set:
   - IAM Identity Center → Permission sets → Create
   - Select "Predefined permission set" → "AdministratorAccess"
4. Create SSO admin user:
   - IAM Identity Center → Users → Add user
   - Complete details and set up MFA
5. Assign permissions:
   - Users → [your user] → Assign AWS accounts
   - Select account(s) → AdministratorAccess permission set

## Bootstrap

```bash
# 1. Configure AWS CLI to use SSO
aws configure sso --profile robosystems-sso

# 2. Run the bootstrap
just bootstrap                              # Default profile/region
just bootstrap robosystems-sso              # Custom profile
just bootstrap robosystems-sso eu-west-1    # Custom profile AND region
```

Bootstrap handles everything:
1. Deploys OIDC federation CloudFormation stack
2. Sets GitHub variables (`AWS_ROLE_ARN`, `AWS_ACCOUNT_ID`, `AWS_REGION`)
3. Prompts for the CloudWatch alert email (`AWS_SNS_ALERT_EMAIL`)
4. Creates ECR repository for Docker images
5. Creates `.envrc` for automatic profile/region selection
6. Prompts to run `setup-aws` (application secrets) and `setup-gha` (deployment variables)

**The alert email is required.** Bootstrap auto-detects it from your SSO user where it can, but exits with an error if you leave it blank — CloudWatch alarms need a destination before you deploy. AWS sends a confirmation email you must accept to activate the subscription.

**The ECR repository name is fixed at `robosystems`**, whatever you named your fork — the deploy role's ECR scope and the workflow defaults both assume that name, so a fork-derived name would create a repository the deploy role can't reach. Bootstrap then offers three lifecycle policies: Robust (the full operational policy, recommended), Basic (keep the last 20 untagged images), or Skip (leave an existing policy untouched). Set `ECR_LIFECYCLE_POLICY=robust|basic|skip` to answer non-interactively, or re-apply later with `just bootstrap-ecr-lifecycle`.

**What gets created in AWS:**
- OIDC Identity Provider for GitHub Actions
- IAM Role: `RoboSystemsGitHubActionsRole` (backend deployments)
- IAM Role: `RoboSystemsGitHubActionsFrontendRole` (frontend app deployments)
- IAM Role: `RoboSystemsAppSuperAdminRole` (application admin via SSO)
- ECR Repository with lifecycle policy
- SES email identity for your domain (DKIM verified, production access requested)

### Optional GitHub Secrets

These secrets are **not required** for deployment but enhance workflow functionality:

| Secret | Purpose | Required For |
|--------|---------|--------------|
| `ACTIONS_TOKEN` | GitHub PAT with `repo` scope | Push to protected branches, create tags/releases, PRs that auto-trigger CI |
| `ANTHROPIC_API_KEY` | Claude API key | AI-generated release notes (`tag-release.yml`); without it the changelog falls back to a plain commit list |
| `CLAUDE_CODE_OAUTH_TOKEN` | Claude Code OAuth token | The `@claude` PR/issue assistant workflow (`claude.yml`) |
| `DOCKERHUB_USERNAME` / `DOCKERHUB_TOKEN` | Docker Hub credentials | Publishing release images to Docker Hub, gated by the `DOCKERHUB_PUBLISHING_ENABLED` variable |

**ACTIONS_TOKEN details:**
- Enables pushing to protected `main` branch, creating tags/releases, and PRs that auto-trigger CI workflows
- Without it, workflows fall back to `github.token` which may fail on protected branches and won't trigger `on:pull_request` workflows
- To create: [github.com/settings/tokens](https://github.com/settings/tokens) → new token with `repo` scope → `gh secret set ACTIONS_TOKEN`

### GitHub Variables (`just setup-gha`)

While bootstrap sets the core OIDC variables (`AWS_ROLE_ARN`, `AWS_ACCOUNT_ID`, `AWS_REGION`), the `setup-gha` script provides full control over the deployment variable set. Run `gh variable list` to see everything currently configured; the categories are:

| Category | Examples | Purpose |
|----------|----------|---------|
| API | `API_MIN_CAPACITY_*`, `API_CPU_*`, `API_FARGATE_SPOT_WEIGHT_*` | Scaling, Fargate sizing, Spot/On-Demand mix |
| Database | `DATABASE_INSTANCE_SIZE_*`, `DATABASE_POSTGRES_VERSION_*` | RDS sizing and configuration |
| RDS Proxy | `RDS_PROXY_ENABLED_*`, `RDS_PROXY_MAX_CONNECTIONS_PERCENT_*` | Connection pooling in front of RDS (optional) |
| Dagster | `DAGSTER_DAEMON_CPU_*`, `DAGSTER_MAX_CONCURRENT_RUNS_*` | Orchestration resources and Spot config |
| Worker | `WORKER_ENABLED_*`, `WORKER_CPU_*`, `WORKER_MEMORY_*`, `WORKER_MIN_COUNT_*`, `WORKER_MAX_COUNT_*`, `WORKER_AUTOSCALING_ENABLED_*` | Background worker service (off by default) |
| LadybugDB | `LBUG_*_ENABLED_*`, `LBUG_*_MIN_INSTANCES_*` | Graph database tier configuration |
| Shared Replicas | `SHARED_REPLICAS_*`, `SHARED_REPOSITORIES_*` | Read-only replica fleet for shared repos |
| Valkey | `VALKEY_NODE_TYPE_*`, `VALKEY_ENCRYPTION_ENABLED_*` | Cache configuration |
| OpenSearch | `OPENSEARCH_ENABLED_*`, `OPENSEARCH_INSTANCE_TYPE_*`, `OPENSEARCH_EBS_SIZE_*` | Document search (disabled by default) |
| Observability | `OBSERVABILITY_ENABLED_*` | Managed Prometheus / Grafana stacks (optional) |
| Security | `WAF_ENABLED_*`, `CLOUDTRAIL_ENABLED`, `VPC_FLOW_LOGS_ENABLED`, `SECURITY_ENABLED`, `AUDIT_ENABLED_*`, `SECRETS_ROTATION_ENABLED_*` | WAF, CloudTrail, flow logs, detective baseline, audit retention, key rotation (all off by default) |
| Infrastructure | `VPC_ENDPOINT_MODE`, `VPC_SECOND_OCTET`, `VPC_MAX_AVAILABILITY_ZONES` | VPC and networking |

**When to run:**
- During bootstrap (prompted) - the recommended path since you opt-in to each step
- Standalone via `just setup-gha` if you skipped during bootstrap

**Note:** Workflows have sensible defaults. You can skip this during bootstrap for basic deployments and run it later when you need custom infrastructure sizing, staging environment, or production features (WAF, Multi-AZ, etc.).

For the security and compliance toggles (WAF, CloudTrail, the detective baseline, audit retention), see [Security & Compliance](Security-and-Compliance). The Security Baseline needs a `just bootstrap` re-run before its first enable so the deploy role picks up the required IAM grants.

### Application Secrets (`just setup-aws`)

Prompted during bootstrap (or run standalone later). Creates both Secrets Manager credentials and SSM parameters. See [SSM Parameters & Secrets Manager](#ssm-parameters--secrets-manager) for full details on what gets created and how to manage it.

Safe to re-run — existing resources are never overwritten.

**Fork-specific:** GitHub Actions workflows automatically pass your AWS account ID as a namespace to CloudFormation, creating unique bucket names like `robosystems-{account-id}-shared-raw-{env}`.

### SSM Parameters & Secrets Manager

Both are seeded by `just setup-aws` — some flags fall back to their code defaults until you set them explicitly. Secrets Manager holds sensitive credentials; SSM Parameter Store holds feature flags and runtime tuning.

#### Secrets Manager

Stores encryption keys, API credentials, and integration secrets. One secret per environment.

**Secret hierarchy:**
```
robosystems/{env}                      # Single JSON secret per environment
  ├── JWT_SECRET_KEY                   # JWT signing key (auto-generated)
  ├── JWT_ISSUER                       # JWT issuer (set in internal mode)
  ├── JWT_AUDIENCE                     # JWT audience (set in internal mode)
  ├── CONNECTION_CREDENTIALS_KEY       # Fernet key for OAuth tokens (auto-generated)
  ├── INTUIT_CLIENT_ID                 # QuickBooks OAuth
  ├── INTUIT_CLIENT_SECRET
  ├── INTUIT_ENVIRONMENT               # "production" or "sandbox"
  ├── INTUIT_REDIRECT_URI
  ├── SEC_GOV_USER_AGENT               # SEC EDGAR API identification
  ├── OPENFIGI_API_KEY                 # OpenFIGI security resolution
  ├── STRIPE_SECRET_KEY                # Stripe billing
  ├── STRIPE_PUBLISHABLE_KEY
  ├── STRIPE_WEBHOOK_SECRET
  ├── TURNSTILE_SECRET_KEY             # Cloudflare Turnstile captcha
  └── TURNSTILE_SITE_KEY
```

**Enterprise SSO keys are deliberately not seeded.** A deployment that enables OIDC login and SCIM provisioning adds these to the same secret by hand:

```
robosystems/{env}
  ├── SSO_OIDC_ISSUER                  # The IdP's issuer URL
  ├── SSO_OIDC_CLIENT_ID
  ├── SSO_OIDC_CLIENT_SECRET
  ├── SSO_OIDC_PROVIDER_LABEL          # Login button text; defaults to "SSO"
  ├── SSO_OIDC_BINDING_CLAIM           # ID-token claim matched to SCIM externalId (default "sub")
  ├── SSO_DEFAULT_ROLE                 # Org role provisioned users join at — "member" or "admin"
  └── ENTERPRISE_ORG_ID                # The single org this deployment's SSO/SCIM surface serves
```

They are absent rather than placeheld on purpose: boot validation refuses to start with `SSO_OIDC_ENABLED=true` and an unconfigured connection, and a placeholder string would satisfy that check with garbage instead of failing loudly. The full enablement sequence is in [Enterprise SSO & SCIM](Enterprise-SSO-and-SCIM).

**Managing secrets:**
```bash
# View current values
aws secretsmanager get-secret-value --secret-id robosystems/prod --query SecretString --output text | jq .

# Update values (merge into existing JSON)
aws secretsmanager put-secret-value --secret-id robosystems/prod --secret-string "$(cat updated.json)"
```

Safe to re-run `just setup-aws` — existing secrets are never overwritten. Two keys (`JWT_SECRET_KEY`, `CONNECTION_CREDENTIALS_KEY`) are auto-generated; all others are placeholders to configure later. Graph backups carry no application-layer key of their own — they rely on S3 server-side encryption (SSE-AES256).

#### SSM Parameter Store

Feature flags and tuning parameters. Uses SSM for cost efficiency (FREE tier vs $0.40/secret/month).

**Parameter hierarchy:**
```
/robosystems/{env}/
  features/           # Boolean feature flags — `just ssm-list <env> features` for the full set
    RATE_LIMIT_ENABLED
    BILLING_ENABLED
    CONNECTIONS_ENABLED
    SEMANTIC_SEARCH_ENABLED
    MCP_SUBGRAPH_OPS_ENABLED
    TAXONOMY_AUTHORING_ENABLED
    PASSWORD_AUTH_ENABLED       # Identity: password login
    PASSKEYS_ENABLED            # Identity: WebAuthn passkeys (MFA + passwordless)
    MFA_ENFORCEMENT_ENABLED     # Identity: require a passkey for org owners/admins
    SSO_OIDC_ENABLED            # Identity: enterprise OIDC login
    SCIM_ENABLED                # Identity: SCIM 2.0 provisioning
    ROBOLEDGER_ENABLED          # RoboLedger extensions surface (ops + GraphQL)
    ROBOINVESTOR_ENABLED        # RoboInvestor extensions surface
    EXTENSIONS_GRAPHQL_ENABLED  # GraphQL reads endpoint kill switch
    ...
  tuning/             # Runtime tunables
    cache/            # Cache TTLs (BALANCE_TTL, JWT_TTL, etc.)
    admission/        # Main API thresholds (MEMORY_THRESHOLD, CPU_THRESHOLD, QUEUE_THRESHOLD)
    lbug_admission/   # LadybugDB/Graph API thresholds (MEMORY_THRESHOLD, CPU_THRESHOLD)
    database/         # Connection pool (POOL_SIZE, MAX_OVERFLOW, POOL_TIMEOUT, POOL_RECYCLE)
    queues/           # Queue config (MAX_SIZE, MAX_CONCURRENT, MAX_PER_USER, TIMEOUT)
    circuits/         # Circuit breakers (THRESHOLD, TIMEOUT)
    load_shedding/    # Load shedding (START_PRESSURE, STOP_PRESSURE)
    mcp/              # MCP limits (MAX_RESULT_ROWS, MAX_RESULT_SIZE_MB, POOL_*)
    workers/          # Worker pool (MAX_WORKERS)
    timeouts/         # HTTP/query timeouts (GRAPH_HTTP, GRAPH_QUERY)
    sse/              # Server-sent events (MAX_CONNECTIONS_PER_USER, QUEUE_SIZE)
    limits/           # Org limits (ORG_GRAPHS_DEFAULT)
```

**Managing parameters:**
```bash
# List all feature flags or tuning parameters
just ssm-list prod features
just ssm-list prod tuning

# Get/set individual parameters
just ssm-get prod features/RATE_LIMIT_ENABLED
just ssm-set prod features/RATE_LIMIT_ENABLED false

just ssm-get prod tuning/cache/BALANCE_TTL
just ssm-set prod tuning/cache/BALANCE_TTL 600
```

**Override priority:** Environment Variable > SSM Parameter > Default Value

Tuning parameters can be adjusted at runtime without redeployment. Changes take effect within the application's cache TTL (typically 5 minutes).

## Deploy

```bash
just deploy prod    # ~20-30 min for initial setup
```

Initial deployment creates all infrastructure (VPC, databases, ECS services). Subsequent deploys only update changed resources.

**Verify deployment:**
```bash
just tunnel prod all    # Connect to all services
# API at http://localhost:8000
# Dagster at http://localhost:8002
```

## Multi-Repository Setup

The OIDC stack creates three roles:

| Role | Repositories / Users | Permissions |
|------|----------------------|-------------|
| `RoboSystemsGitHubActionsRole` | `robosystems` | Full infrastructure |
| `RoboSystemsGitHubActionsFrontendRole` | `robosystems-app`, `roboledger-app`, `roboinvestor-app`, `xbrlkit-viewer` | Limited frontend |
| `RoboSystemsAppSuperAdminRole` | SSO users (via IAM Identity Center) | Application admin only (no infra) |

The frontend repos hold no AWS access of their own. `just bootstrap` in the
backend sets each one's `AWS_ROLE_ARN`, `AWS_ACCOUNT_ID` and `AWS_REGION`
directly — the repository names come from the OIDC stack's own parameters, so a
fork that renamed its apps stays consistent with the trust policy. Only the
deployment variables remain to set in each app repo:

```bash
# In robosystems-app, roboledger-app, or roboinvestor-app
npm run setup:gha
```

**Allowed deployment branches:** `main`, `release/*`, `v*` tags

## API Access Modes

RoboSystems supports two API access modes, configured via `API_ACCESS_MODE_PROD` / `API_ACCESS_MODE_STAGING`:

| Mode | ALB Scheme | TLS | Domain Required | Use Case |
|------|------------|-----|-----------------|----------|
| `internal` | Internal | No | No | VPC-only access, no public endpoints |
| `public` | Internet-facing | Yes | Yes | Production with custom domain |

### Internal Mode (Default)

The ALB is internal (not internet-accessible) and all access is through the VPC via SSM tunnel:

```bash
just tunnel prod all
# API at http://localhost:8000
```

This is the default - no GitHub variables needed.

### Public Mode (Custom Domain)

Full production setup with HTTPS and custom domain:

1. Domain must be hosted in Route53
2. Configure variables:
   ```bash
   gh variable set API_ACCESS_MODE_PROD --body "public"
   gh variable set API_DOMAIN_NAME_ROOT --body "yourdomain.com"
   gh variable set API_DOMAIN_NAME_PROD --body "api.yourdomain.com"
   ```
3. Deploy - ACM certificates and DNS records created automatically

### Switching Modes

You can switch modes at any time:

```bash
# Switch from internal to public with domain
gh variable set API_ACCESS_MODE_PROD --body "public"
gh variable set API_DOMAIN_NAME_ROOT --body "yourdomain.com"
gh variable set API_DOMAIN_NAME_PROD --body "api.yourdomain.com"
just deploy prod
```

**Note:** Switching between internal and internet-facing modes will replace the ALB (causes brief downtime).

## Frontend App Deployment

Frontend apps (robosystems-app, roboledger-app, roboinvestor-app) are 99% client-side Next.js applications deployed on **AWS App Runner behind CloudFront**.

**Note:** Unlike the API, frontend apps don't have an `internal` mode - App Runner is always internet-facing. Use the API's internal mode if you need VPC-only access for microservice/extension use cases.

### Access Modes

Configured via `APP_ACCESS_MODE_PROD` / `APP_ACCESS_MODE_STAGING`:

| Mode | Domain Required | Use Case |
|------|-----------------|----------|
| `public` (default) | Yes | CloudFront with a custom domain and HTTPS — needs `DOMAIN_NAME_ROOT` / `DOMAIN_NAME_PROD` hosted in Route53 |
| `public-http` | No | CloudFront on the AWS-issued distribution domain — the path for a fork with no Route53 domain |

### Bootstrap

Run from the **backend** repo. It deploys the OIDC stack and pushes the identity
variables to every frontend repo named in that stack's parameters:

```bash
just bootstrap         # full
just bootstrap-oidc    # re-apply the roles and identity variables only
```

Then, from the frontend app repository:

```bash
npm run setup:gha      # Deployment variables: domains, access mode, scaling
```

### Custom Domain Setup

1. Domain must be hosted in Route53
2. Configure variables via `npm run setup:gha` or manually:
   ```bash
   gh variable set DOMAIN_NAME_ROOT --body "yourdomain.com"
   gh variable set DOMAIN_NAME_PROD --body "yourdomain.com"
   ```
3. Deploy - ACM certificates and DNS records created automatically

## SES Email Identity

Bootstrap automatically sets up Amazon SES for transactional emails (account verification, password reset, welcome). You'll be prompted for your email domain (default: `robosystems.ai`), which is saved as the `AWS_SES_DOMAIN` GitHub variable.

This involves three steps:

1. **Domain identity** — Creates an SES email identity for your domain
2. **DKIM verification** — Adds CNAME records to Route53 (or prints them for manual DNS setup if the hosted zone isn't in the same account)
3. **Production access** — Requests sandbox removal so emails can be sent to any address

**New accounts start in SES sandbox mode**, which only allows sending to verified email addresses. Production access is required for real user registration. AWS typically approves the request within 24 hours (often instantly for transactional-only use cases).

**Verify SES status:**
```bash
# Check domain verification (replace with your domain)
aws sesv2 get-email-identity --email-identity yourdomain.com --region us-east-1 \
  --query '{DkimStatus: DkimAttributes.Status, SendingEnabled: VerifiedForSendingStatus}'

# Check production access
aws sesv2 get-account --region us-east-1 \
  --query '{ProductionAccess: ProductionAccessEnabled, SendingEnabled: SendingEnabled}'
```

**If DKIM is still pending**, verify the DNS records exist:
```bash
DOMAIN=yourdomain.com
aws route53 list-resource-record-sets \
  --hosted-zone-id $(aws route53 list-hosted-zones --query "HostedZones[?Name=='${DOMAIN}.'].Id" --output text | sed 's|/hostedzone/||') \
  --query "ResourceRecordSets[?contains(Name, '_domainkey')].[Name,ResourceRecords[0].Value]" \
  --output table
```

## Troubleshooting

### SSO: "No accounts found"
Ensure your SSO user has permission sets assigned to accounts.

### SSO: "Profile not found"
```bash
aws configure sso --profile robosystems-sso
```

### AWS CLI: "You must specify a region"
```bash
echo 'export AWS_REGION=us-east-1' >> .envrc && direnv allow
```

### OIDC: "Not authorized to perform sts:AssumeRoleWithWebIdentity"
1. Verify OIDC stack deployed successfully
2. Check branch matches allowed conditions (main, release/*, v* tags)
3. Ensure workflow has `permissions: id-token: write`

### SES: Verification emails not sending
1. Check domain is verified: `aws sesv2 get-email-identity --email-identity robosystems.ai --region us-east-1`
2. Check production access: `aws sesv2 get-account --region us-east-1 --query 'ProductionAccessEnabled'`
3. If in sandbox, either request production access or verify recipient email: `aws ses verify-email-identity --email-address user@example.com --region us-east-1`

## Quick Reference

```bash
# Bootstrap
just bootstrap                    # Default profile/region
just bootstrap my-sso eu-west-1   # Custom profile AND region

# Deploy
just deploy prod                  # Production (~20-30 min initial)
just deploy staging               # Staging

# Connect
just tunnel prod all              # All tunnels (postgres, valkey, dagster, api)

# Secrets Manager
aws secretsmanager get-secret-value --secret-id robosystems/prod --query SecretString --output text | jq .

# SSM Parameters
just ssm-list prod features       # List feature flags
just ssm-list prod tuning         # List tuning parameters
just ssm-set prod features/BILLING_ENABLED true
just ssm-set prod tuning/cache/BALANCE_TTL 600

# Optional / Re-run individually
just setup-aws                    # Secrets, feature flags, tuning params
just setup-gha                    # Full GitHub variable control
just setup-bedrock                # Local AI/Bedrock development
just bootstrap-ecr-lifecycle      # Re-apply the ECR image lifecycle policy

# Optional secrets (enhance workflow functionality)
gh secret set ACTIONS_TOKEN        # For protected branches, releases, PRs
gh secret set ANTHROPIC_API_KEY    # For AI-powered release notes

# Verify
gh variable list
gh secret list
aws sts get-caller-identity
```

## CloudFormation Templates

| Template | Deployed By | Purpose |
|----------|-------------|---------|
| `bootstrap-oidc.yaml` | `just bootstrap` (local) | GitHub OIDC federation |
| `vpc.yaml` | GitHub Actions | VPC and networking |
| `postgres.yaml` | GitHub Actions | RDS PostgreSQL |
| `valkey.yaml` | GitHub Actions | ElastiCache Redis |
| `s3.yaml` | GitHub Actions | S3 buckets |
| `api.yaml` | GitHub Actions | ECS API service |
| `waf.yaml` | GitHub Actions | Web Application Firewall (optional) |
| `dagster.yaml` | GitHub Actions | ECS Dagster service |
| `worker.yaml` | GitHub Actions | ECS background worker service (optional, `WORKER_ENABLED_*`) |
| `opensearch.yaml` | GitHub Actions | OpenSearch document search (optional) |
| `graph-*.yaml` | GitHub Actions | LadybugDB infrastructure |
| `prometheus.yaml` | GitHub Actions | Managed Prometheus (optional) |
| `grafana.yaml` | GitHub Actions | Managed Grafana (optional) |
| `cloudtrail.yaml` | GitHub Actions | AWS audit logging (optional) |
| `security.yaml` | GitHub Actions | Detective security baseline — GuardDuty, Security Hub, Inspector, Access Analyzer, Config (optional, `SECURITY_ENABLED`) |
| `audit.yaml` | GitHub Actions | Long-retention security-audit log pipeline (optional, `AUDIT_ENABLED_*`) |
| `bastion.yaml` | GitHub Actions | SSM bastion host |

## Related Documentation

- [Architecture Overview](Architecture-Overview) - System architecture
- [CloudFormation Templates](https://github.com/RoboFinSystems/robosystems/tree/main/cloudformation) - Infrastructure as code
- [Setup Scripts](https://github.com/RoboFinSystems/robosystems/tree/main/bin/setup) - Bootstrap scripts
