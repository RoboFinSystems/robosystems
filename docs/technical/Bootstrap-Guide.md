# Bootstrap Guide

This guide walks you through bootstrapping a fresh fork of RoboSystems.

## Table of Contents

- [How It Works](#how-it-works)
- [Prerequisites](#prerequisites)
- [Fresh AWS Account Setup](#fresh-aws-account-setup)
- [Bootstrap](#bootstrap)
- [SSM Parameters](#ssm-parameters)
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
3. Creates ECR repository for Docker images
4. Creates `.envrc` for automatic profile/region selection
5. Prompts to run `setup-aws` (application secrets) and `setup-gha` (deployment variables)

**What gets created in AWS:**
- OIDC Identity Provider for GitHub Actions
- IAM Role: `RoboSystemsGitHubActionsRole` (backend)
- IAM Role: `RoboSystemsGitHubActionsFrontendRole` (frontend apps)
- ECR Repository with lifecycle policy

### Optional GitHub Secrets

These secrets are **not required** for deployment but enhance workflow functionality:

| Secret | Purpose | Required For |
|--------|---------|--------------|
| `ACTIONS_TOKEN` | GitHub PAT with `repo` scope | Push to protected branches, create tags/releases, PRs that auto-trigger CI |
| `ANTHROPIC_API_KEY` | Claude API key | AI-generated PR summaries and release notes |

**ACTIONS_TOKEN details:**
- Enables pushing to protected `main` branch, creating tags/releases, and PRs that auto-trigger CI workflows
- Without it, workflows fall back to `github.token` which may fail on protected branches and won't trigger `on:pull_request` workflows
- To create: [github.com/settings/tokens](https://github.com/settings/tokens) → new token with `repo` scope → `gh secret set ACTIONS_TOKEN`

### GitHub Variables (`just setup-gha`)

While bootstrap sets the core OIDC variables (`AWS_ROLE_ARN`, `AWS_ACCOUNT_ID`, `AWS_REGION`), the `setup-gha` script provides full control over ~80 deployment variables:

| Category | Examples | Purpose |
|----------|----------|---------|
| API | `API_MIN_CAPACITY_*`, `API_ACCESS_MODE_*` | Scaling, domain, Spot/On-Demand mix |
| Database | `DATABASE_INSTANCE_SIZE_*`, `DATABASE_POSTGRES_VERSION_*` | RDS sizing and configuration |
| Dagster | `DAGSTER_DAEMON_CPU_*`, `DAGSTER_MAX_CONCURRENT_RUNS_*` | Orchestration resources |
| LadybugDB | `LBUG_*_ENABLED_*`, `LBUG_*_MIN_INSTANCES_*` | Graph database tier configuration |
| Valkey | `VALKEY_NODE_TYPE_*`, `VALKEY_ENCRYPTION_ENABLED_*` | Cache configuration |
| Security | `WAF_ENABLED_*`, `VPC_FLOW_LOGS_ENABLED` | WAF, audit logging |
| Infrastructure | `VPC_ENDPOINT_MODE`, `MAX_AVAILABILITY_ZONES` | VPC and networking |

**When to run:**
- During bootstrap (prompted) - the recommended path since you opt-in to each step
- Standalone via `just setup-gha` if you skipped during bootstrap

**Note:** Workflows have sensible defaults. You can skip this during bootstrap for basic deployments and run it later when you need custom infrastructure sizing, staging environment, or production features (WAF, Multi-AZ, etc.).

### Application Secrets (`just setup-aws`)

Prompted during bootstrap (or run standalone later). Creates:

**Secrets Manager** (`robosystems/prod`, `robosystems/staging`):
- Auto-generated encryption keys (JWT, connection credentials, backup)
- Integration placeholders: QuickBooks, Plaid, Stripe, Turnstile (configure later)

**SSM Parameter Store**:
- Feature flags (`/robosystems/{env}/features/*`)
- Tuning parameters (`/robosystems/{env}/tuning/*`)

Safe to re-run - existing secrets are never overwritten. To customize later:

```bash
# View current values
aws secretsmanager get-secret-value --secret-id robosystems/prod --query SecretString --output text | jq .

# Update values
aws secretsmanager put-secret-value --secret-id robosystems/prod --secret-string "$(cat updated.json)"
```

**Fork-specific:** GitHub Actions workflows automatically pass your AWS account ID as a namespace to CloudFormation, creating unique bucket names like `robosystems-{account-id}-shared-raw-{env}`.

### SSM Parameters

SSM Parameter Store holds feature flags and tuning parameters. Created by `just setup-aws`.

**Parameter hierarchy:**
```
/robosystems/{env}/
  features/           # Boolean feature flags
    RATE_LIMIT_ENABLED
    BILLING_ENABLED
    ...
  tuning/             # Runtime tunables
    cache/            # Cache TTLs (BALANCE_TTL, JWT_TTL, etc.)
    admission/        # Main API thresholds (MEMORY_THRESHOLD, CPU_THRESHOLD, QUEUE_THRESHOLD)
    lbug_admission/   # LadybugDB/Graph API thresholds (MEMORY_THRESHOLD, CPU_THRESHOLD)
    queues/           # Queue config (MAX_SIZE, MAX_CONCURRENT)
    circuits/         # Circuit breakers (THRESHOLD, TIMEOUT)
    load_shedding/    # Load shedding (START_PRESSURE, STOP_PRESSURE)
    mcp/              # MCP limits (MAX_RESULT_ROWS, MAX_RESULT_SIZE_MB)
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

The OIDC stack creates two roles:

| Role | Repositories | Permissions |
|------|--------------|-------------|
| `RoboSystemsGitHubActionsRole` | `robosystems` | Full infrastructure |
| `RoboSystemsGitHubActionsFrontendRole` | `robosystems-app`, `roboledger-app`, `roboinvestor-app` | Limited frontend |

After bootstrapping the backend, set up frontend apps:

```bash
# In robosystems-app, roboledger-app, or roboinvestor-app
npm run setup:bootstrap
```

**Allowed deployment branches:** `main`, `release/*`, `v*` tags

## API Access Modes

RoboSystems supports two API access modes, configured via `API_ACCESS_MODE_PROD` / `API_ACCESS_MODE_STAGING`:

| Mode | ALB Scheme | TLS | Domain Required | Use Case |
|------|------------|-----|-----------------|----------|
| `internal` | Internal | No | No | Zero public attack surface, VPC-only access |
| `public` | Internet-facing | Yes | Yes | Production with custom domain |

### Internal Mode (Default)

No public attack surface. The ALB is internal (not internet-accessible) and all access is through the VPC via SSM tunnel:

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

### Bootstrap

From the frontend app repository:

```bash
npm run setup:bootstrap    # Sets AWS_ROLE_ARN, prompts for gha-setup
```

This checks for the OIDC infrastructure (deployed by the main robosystems repo) and configures GitHub variables.

### Custom Domain Setup

1. Domain must be hosted in Route53
2. Configure variables via `npm run setup:gha` or manually:
   ```bash
   gh variable set DOMAIN_NAME_ROOT --body "yourdomain.com"
   gh variable set DOMAIN_NAME_PROD --body "yourdomain.com"
   ```
3. Deploy - ACM certificates and DNS records created automatically

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

# SSM Parameters
just ssm-list prod features       # List feature flags
just ssm-list prod tuning         # List tuning parameters
just ssm-set prod features/BILLING_ENABLED true
just ssm-set prod tuning/cache/BALANCE_TTL 600

# Optional / Re-run individually
just setup-aws                    # Secrets, feature flags, tuning params
just setup-gha                    # Full GitHub variable control (~80 vars)
just setup-bedrock                # Local AI/Bedrock development

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
| `dagster.yaml` | GitHub Actions | ECS Dagster service |
| `graph-*.yaml` | GitHub Actions | LadybugDB infrastructure |
| `bastion.yaml` | GitHub Actions | SSM bastion host |

## Related Documentation

- [Architecture Overview](Architecture-Overview) - System architecture
- [CloudFormation Templates](https://github.com/RoboFinSystems/robosystems/tree/main/cloudformation) - Infrastructure as code
- [Setup Scripts](https://github.com/RoboFinSystems/robosystems/tree/main/bin/setup) - Bootstrap scripts
