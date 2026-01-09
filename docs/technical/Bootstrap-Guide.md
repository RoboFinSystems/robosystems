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
- GitHub CLI installed and authenticated (`gh auth login`)
- `jq` installed (JSON parser - `brew install jq` or `apt install jq`)

## Bootstrap Steps

```bash
# 1. Configure AWS CLI to use SSO (if not already done)
aws configure sso --profile robosystems-sso

# 2. Run the bootstrap (does everything)
just bootstrap
```

This will:
1. Verify SSO authentication
2. Deploy `bootstrap-oidc.yaml` CloudFormation stack
3. Set GitHub variables (including `AWS_ROLE_ARN`)
4. Optionally create AWS Secrets Manager secrets

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
4. [ ] Create SSO admin user
5. [ ] Configure AWS CLI locally

### Phase 1: Bootstrap

```bash
# Configure SSO profile
aws configure sso --profile robosystems-sso

# Run bootstrap
just bootstrap
```

### Phase 2: Deploy

```bash
# Deploy to production
just deploy prod
```

## Troubleshooting

### SSO: "No accounts found"

Ensure your SSO user has permission sets assigned to accounts.

### SSO: "Profile not found"

```bash
aws configure sso --profile robosystems-sso
```

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
# Bootstrap
just bootstrap          # Sets up OIDC federation
just setup-gha          # GitHub variables only
just setup-aws          # AWS Secrets Manager
just setup-bedrock      # Local Bedrock dev (optional)

# SSO login
aws sso login --profile robosystems-sso
export AWS_PROFILE=robosystems-sso

# Generate keys
just generate-key       # Single key
just generate-keys      # All required keys

# Verify setup
gh variable list
gh secret list
aws sts get-caller-identity

# Deploy
just deploy prod        # Deploy to production
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
