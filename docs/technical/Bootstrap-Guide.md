# Bootstrap Guide

This guide walks you through bootstrapping a fresh fork of RoboSystems.

## Table of Contents

- [How It Works](#how-it-works)
- [Prerequisites](#prerequisites)
- [Bootstrap Steps](#bootstrap-steps)
- [What Gets Created](#what-gets-created)
- [Multi-Repository Setup](#multi-repository-setup)
- [Allowed Branches](#allowed-branches)
- [CLI Access](#cli-access)
- [GitHub Secrets](#github-secrets)
- [AWS Secrets Manager](#aws-secrets-manager)
- [Fresh AWS Account Checklist](#fresh-aws-account-checklist)
- [Domain Configuration](#domain-configuration)
- [Troubleshooting](#troubleshooting)
- [Local Development (Optional)](#local-development-optional)
- [Quick Reference](#quick-reference)
- [CloudFormation Templates](#cloudformation-templates)

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

- AWS IAM Identity Center (SSO) enabled
- SSO account with admin permissions
- AWS CLI v2 installed
- GitHub CLI installed and authenticated (see below)
- `jq` installed (JSON parser - `brew install jq` or `apt install jq`)
- `direnv` installed (optional but recommended - `brew install direnv`)

### GitHub CLI Authentication

The bootstrap process uses `gh` to set repository variables and secrets. You need:

```bash
# Install GitHub CLI
brew install gh

# Authenticate with a PAT that has repo admin access
gh auth login
```

**Required scopes for your GitHub token:**
- `repo` - Full repository access
- `admin:org` - For org-level secrets (if using)
- `workflow` - To trigger workflows

**For forks:** Ensure you're authenticated to an account with admin access to the forked repository. The `gh` CLI will set variables on whatever repo the current directory points to.

```bash
# Verify you have access to the correct repo
gh repo view

# Should show your fork, e.g.: HarbingerFinLab/robosystems
```

## Bootstrap Steps

```bash
# 1. Configure AWS CLI to use SSO (if not already done)
aws configure sso --profile robosystems-sso

# 2. Run the bootstrap (does everything)
just bootstrap
```

**For forks with a different AWS account:**

```bash
# 1. Configure SSO with a custom profile name
aws configure sso --profile my-fork-sso

# 2. Run bootstrap with your profile (and optionally region)
just bootstrap my-fork-sso              # Custom profile, default region (us-east-1)
just bootstrap my-fork-sso eu-west-1    # Custom profile AND region

# The profile and region are auto-exported for the session
# and saved to .envrc for future use
```

This will:
1. Create/update `.envrc` with your AWS profile and region
2. Verify SSO authentication
3. Deploy `bootstrap-oidc.yaml` CloudFormation stack
4. Set GitHub variables (including `AWS_ROLE_ARN`)
5. Run `setup-gha` to configure essential variables (auto-detected)

## What Gets Created

**In AWS:**
- OIDC Identity Provider for GitHub Actions
- IAM Role: `RoboSystemsGitHubActionsRole` (backend - full permissions)
- IAM Role: `RoboSystemsGitHubActionsFrontendRole` (frontend apps - limited permissions)

**In GitHub (robosystems repo):**
- Variable: `AWS_ROLE_ARN` (backend role ARN)
- Variable: `AWS_ACCOUNT_ID`
- Variable: `AWS_REGION`
- Plus ~80 configuration variables (via `setup-gha`)

**In GitHub (frontend repos):**
- Variable: `AWS_ROLE_ARN` (frontend role ARN)
- Variable: `AWS_ACCOUNT_ID`
- Variable: `AWS_REGION`

## Variable Configuration

Bootstrap sets only the essential variables needed for OIDC authentication:
- `AWS_ROLE_ARN` - IAM role for GitHub Actions
- `AWS_ACCOUNT_ID` - Your AWS account
- `AWS_REGION` - Deployment region

**All other variables have sensible defaults** in workflows and `.github/configs/graph.yml`. Basic deployments work immediately after bootstrap.

### Optional: Full Variable Control

For production customization (custom domains, specific instance sizes, scaling policies):

```bash
just setup-gha    # Sets ~80 variables for explicit control
```

This is optional - only run it if you need to override workflow defaults.

## Multi-Repository Setup

The OIDC stack creates two roles with hardcoded repository names for security:

| Role | Repositories | Permissions |
|------|--------------|-------------|
| `RoboSystemsGitHubActionsRole` | `robosystems` (backend) | Full infrastructure access |
| `RoboSystemsGitHubActionsFrontendRole` | `robosystems-app`, `roboledger-app`, `roboinvestor-app` | Limited frontend deployment access |

Only the GitHub organization is parameterized - repository names are hardcoded in the CloudFormation template.

### Setting Up Frontend Apps

After bootstrapping the main repo, run the lightweight bootstrap in each frontend repo:

```bash
# In robosystems-app, roboledger-app, or roboinvestor-app
npm run setup:bootstrap
```

This will:
1. Auto-detect the repository from git remote
2. Verify the OIDC infrastructure exists (created by main bootstrap)
3. Set `AWS_ROLE_ARN` to the **frontend role** (limited permissions)
4. Set `AWS_ACCOUNT_ID` and `AWS_REGION`
5. Check for required secrets (repo and org level)
6. Optionally run the full variable setup (`npm run setup:gha`)

## Allowed Branches

The OIDC role only allows deployments from:

| Source | Pattern | Use Case |
|--------|---------|----------|
| `main` branch | `refs/heads/main` | Production deploys |
| `release/*` branches | `refs/heads/release/*` | Hotfix deploys |
| `v*` tags | `refs/tags/v*` | Release tag deploys |

## CLI Access

```bash
# Login via SSO (opens browser)
aws sso login --profile robosystems-sso

# Use the profile
export AWS_PROFILE=robosystems-sso
aws sts get-caller-identity
```

### Automatic Profile Selection with direnv

The bootstrap creates a `.envrc` file that automatically sets `AWS_PROFILE` and `AWS_REGION` when you enter the directory:

```bash
# After bootstrap, allow direnv to load the .envrc
direnv allow

# Now AWS_PROFILE and AWS_REGION are automatically set when you cd into the repo
cd robosystems
echo $AWS_PROFILE  # robosystems-sso
echo $AWS_REGION   # us-east-1
```

This prevents accidentally running commands against the wrong AWS account when working across multiple repositories or forks. Each fork can have its own `.envrc` with the appropriate profile and region.

## GitHub Secrets

Secrets can be set at **repository level** or **organization level**. Org-level secrets are inherited by all repos.

| Secret | Required | Scope | Description |
|--------|----------|-------|-------------|
| `ACTIONS_TOKEN` | No | Org or Repo | GitHub PAT for workflow automations |
| `ANTHROPIC_API_KEY` | No | Org or Repo | For Claude-powered PR/release workflows |
| `DOCKERHUB_TOKEN` | No | Repo | For publishing to Docker Hub |

**Notes:**
- With OIDC, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are **not needed**
- `ACTIONS_TOKEN` enables additional automations (deploy after release, gh CLI in workflows, cross-workflow triggers) - not required for core CI/CD
- `ANTHROPIC_API_KEY` enables `create-pr.yml` and `tag-release.yml` workflows in all repos
- Backend AI inference uses AWS Bedrock (separate from `ANTHROPIC_API_KEY`)
- The bootstrap scripts check both repo and org-level secrets

**Graceful Degradation:**

All workflows work without optional secrets - they degrade gracefully instead of failing:

| Missing Secret | Behavior |
|----------------|----------|
| `ACTIONS_TOKEN` | Core CI/CD works. Auto-deploy after release disabled. Workflow summary shows manual deploy commands. |
| `ANTHROPIC_API_KEY` | PR and release workflows use basic descriptions (commit stats) instead of AI-generated summaries. |

This design allows forks to deploy immediately without configuring optional integrations.

## AWS Secrets Manager

Application secrets are stored in AWS Secrets Manager:

```bash
just setup-aws
```

Creates:
- `robosystems/prod` - Production secrets
- `robosystems/staging` - Staging secrets

**Edit `bin/setup/aws.sh` first** to replace placeholders:
- `JWT_SECRET_KEY` - Generate with `just generate-key`
- `CONNECTION_CREDENTIALS_KEY` - Generate with `just generate-key`
- Integration credentials (QuickBooks, Plaid, etc.)

## Fresh AWS Account Checklist

### Phase 0: Account Setup (AWS Console)

1. [ ] Log in as root user
2. [ ] Enable MFA on root
3. [ ] Enable IAM Identity Center (SSO)
4. [ ] Create Permission Set:
   - Go to IAM Identity Center → Permission sets → Create permission set
   - Select "Predefined permission set" → "AdministratorAccess"
   - Click Next → Create
5. [ ] Create SSO admin user:
   - Go to IAM Identity Center → Users → Add user
   - Complete user details and set up MFA
6. [ ] Assign permissions to user:
   - Go to Users → [your user] → Assign AWS accounts
   - Select your AWS account(s)
   - Click Next, select the AdministratorAccess permission set
   - Submit
7. [ ] Configure AWS CLI locally

### Phase 1: Bootstrap

```bash
# Configure SSO profile
aws configure sso --profile robosystems-sso

# Run bootstrap (interactive - prompts for secrets creation)
just bootstrap
```

Bootstrap handles:
1. OIDC federation setup
2. GitHub variables (AWS_ROLE_ARN, AWS_ACCOUNT_ID, AWS_REGION)
3. Application secrets & feature flags - prompts `(Y/n)`, required for deploy
4. Full variable setup - prompts `(y/N)`, optional for customization

The secrets step creates `robosystems/prod` and `robosystems/staging` with:
- JWT signing keys (auto-generated)
- Encryption keys (auto-generated)
- Feature flags (sensible defaults)
- Integration placeholders (QuickBooks, Plaid, etc.)

**Note:** Existing secrets are never overwritten. Safe to re-run bootstrap.

### Phase 2: Deploy

```bash
# Deploy to production (~20-30 min for initial setup)
just deploy prod
```

Initial deployment creates all infrastructure (VPC, databases, ECS services, etc.). Subsequent deployments are faster as they only update changed resources.

## Domain Configuration

RoboSystems supports two deployment modes. During `just bootstrap`, you can simply press Enter to skip domain configuration for VPC-only deployments.

### VPC-Only Mode (Default)

No custom domain required. The API is accessible via:
- **Bastion tunnel**: SSH tunnel through the bastion host for secure access
- **Internal load balancer**: Direct access within the VPC

```bash
# Access via bastion tunnel
just bastion-tunnel

# API is now accessible at http://localhost:8000
```

This is the default when `API_DOMAIN_NAME_ROOT` is empty. Perfect for:
- Internal microservice architectures
- Development and testing environments
- Private deployments behind corporate networks

### Custom Domain Mode

For production deployments with a custom domain (e.g., `api.robosystems.ai`):

1. **Set the domain variable** before deploying:
   ```bash
   gh variable set API_DOMAIN_NAME_ROOT --body "robosystems.ai"
   ```

2. **Prerequisites:**
   - Domain registered and DNS hosted in Route53 (or external DNS)
   - ACM certificate for `*.robosystems.ai` in the deployment region
   - The deployment will create Route53 records automatically if using Route53

3. **Result:**
   - API accessible at `https://api.robosystems.ai`
   - Automatic HTTPS redirect
   - SSL/TLS termination at the load balancer

**Note:** You can switch between modes by setting or clearing `API_DOMAIN_NAME_ROOT` and redeploying.

## Troubleshooting

### SSO: "No accounts found"

Ensure your SSO user has permission sets assigned to accounts.

### SSO: "Profile not found"

```bash
aws configure sso --profile robosystems-sso
```

### AWS CLI: "You must specify a region"

If you see this error, ensure `AWS_REGION` is set. The bootstrap script should have added this to your `.envrc`, but if not:

```bash
# Add to .envrc
echo 'export AWS_REGION=us-east-1' >> .envrc
direnv allow
```

Or add `region = us-east-1` to your profile in `~/.aws/config`.

### OIDC: "Not authorized to perform sts:AssumeRoleWithWebIdentity"

Check that:
1. The OIDC stack was deployed successfully
2. The repository/branch matches the allowed conditions (main, release/*, v* tags)
3. The workflow has `permissions: id-token: write`

### OIDC: "Could not assume role"

Verify the role ARN is correct:
```bash
gh variable get AWS_ROLE_ARN
aws iam get-role --role-name RoboSystemsGitHubActionsRole --profile robosystems-sso
```

## Local Development (Optional)

### Bedrock Access for Docker

If you need to test AI/Bedrock features locally in Docker, run:

```bash
just setup-bedrock
```

This creates an IAM user with Bedrock permissions and updates `.env` with credentials. Docker containers can't access host SSO sessions, so this provides a workaround.

**Note:** Production uses IAM role credentials via ECS task roles - no stored credentials needed.

## Quick Reference

```bash
# Bootstrap (interactive - handles everything)
just bootstrap                    # Default profile/region
just bootstrap my-sso             # Custom SSO profile
just bootstrap my-sso eu-west-1   # Custom profile AND region

# Then deploy
just deploy prod                  # Deploy to production (~20-30 min initial)

# Optional customization
just setup-gha          # Full variable control (~80 vars) - not required for basic deploys
just setup-aws          # Application secrets & feature flags (if skipped)
just setup-bedrock      # Local Bedrock dev (optional)

# SSO login
aws sso login --profile robosystems-sso
export AWS_PROFILE=robosystems-sso  # or use direnv (automatic)

# Generate keys
just generate-key       # Single key
just generate-keys      # All required keys

# Verify setup
gh variable list
gh secret list
aws sts get-caller-identity

# Deploy
just deploy prod        # Deploy to production
just deploy staging     # Deploy to staging
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

Note: `bootstrap-oidc.yaml` is deployed locally because GitHub Actions needs it to exist before it can authenticate to AWS.

## Related Documentation

**Wiki Guides:**
- [Architecture Overview](Architecture-Overview) - System architecture and components
- [Getting Started](Home) - Initial setup and development

**Codebase:**
- [CloudFormation Templates](https://github.com/RoboFinSystems/robosystems/tree/main/cloudformation) - Infrastructure as code
- [Setup Scripts](https://github.com/RoboFinSystems/robosystems/tree/main/bin/setup) - Bootstrap and setup scripts

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Discussions](https://github.com/RoboFinSystems/robosystems/discussions)
