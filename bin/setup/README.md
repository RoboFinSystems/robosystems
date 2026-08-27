# Setup Scripts

Scripts for bootstrapping and configuring RoboSystems deployments. These handle AWS infrastructure setup, GitHub Actions configuration, and local development environment initialization.

## Quick Reference

| Script               | Purpose                        | Prerequisites       | Est. Time |
| -------------------- | ------------------------------ | ------------------- | --------- |
| `bootstrap.sh`       | Complete first-time setup      | AWS SSO, GitHub CLI | 5-10 min  |
| `aws.sh`             | Secrets + SSM parameters       | AWS credentials     | 1-2 min   |
| `gha.sh`             | Configure GitHub variables     | GitHub CLI          | 2-3 min   |
| `bedrock.sh`         | Local AI development setup     | AWS credentials     | 1 min     |
| `localstack-init.sh` | Local AWS emulation            | Docker (automatic)  | N/A       |
| `postgres-init.sh`   | PostgreSQL databases           | Docker (automatic)  | N/A       |

## Bootstrap Flow

The complete bootstrap process for a fresh deployment:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          just bootstrap                                     │
│                     (or: bin/setup/bootstrap.sh)                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 1: DIRENV SETUP                                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│  Creates/updates .envrc with:                                               │
│    export AWS_PROFILE=robosystems-sso                                       │
│    export AWS_REGION=us-east-1                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 2: SSO CONFIGURATION                                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  If SSO profile doesn't exist:                                              │
│    - Prompts for SSO Start URL (e.g., https://d-xxx.awsapps.com/start)      │
│    - Creates ~/.aws/config profile                                          │
│    - Opens browser for SSO login                                            │
│    - Lists available accounts and roles                                     │
│  If SSO profile exists:                                                     │
│    - Verifies credentials or triggers login                                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 3: DEPLOY GITHUB OIDC                                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│  Deploys cloudformation/bootstrap-oidc.yaml:                                │
│    - Creates IAM OIDC Provider for GitHub                                   │
│    - Backend role: trusts {GitHubOrg}/{backend repo} only                   │
│    - Frontend role: trusts the three *-app repos + holon-viewer             │
│    - Allowed refs (both roles): main, release/*, v* tags                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 4: SET CORE GITHUB VARIABLES                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│  Sets essential GitHub repository variables:                                │
│    - AWS_ROLE_ARN (from OIDC stack output)                                  │
│    - AWS_ACCOUNT_ID                                                         │
│    - AWS_REGION                                                             │
│    - AWS_SNS_ALERT_EMAIL (prompts or auto-detects from SSO)                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 5: CREATE ECR REPOSITORY                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│  Creates ECR repository:                                                    │
│    - Repository name is always "robosystems" (fleet-uniform, even on        │
│      a renamed fork — the deploy role's ECR scope assumes it)               │
│    - Image scanning on push, AES256 encryption                              │
│    - First run: prompts for a lifecycle policy (robust / basic / skip);    │
│      later runs: silent when the live policy matches the bundled file      │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 6: SES EMAIL IDENTITY                                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│  Verifies a sending domain for transactional email:                         │
│    - Prompts for the email domain, stores it as AWS_SES_DOMAIN              │
│    - Creates the SESv2 domain identity if missing                           │
│    - Publishes DKIM CNAMEs to Route53 automatically when a hosted           │
│      zone exists; otherwise prints them for manual DNS entry                │
│    - Requests SES production access (out of the sandbox)                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 7: CHECK GITHUB SECRETS                                               │
├─────────────────────────────────────────────────────────────────────────────┤
│  Checks for optional secrets (repo and org level):                          │
│    - ACTIONS_TOKEN (enables cross-workflow triggers)                        │
│    - ANTHROPIC_API_KEY (enables AI-powered PR/release notes)                │
│  Note: AWS credentials NOT needed with OIDC                                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 8: OPTIONAL CONFIGURATION (Interactive Prompts)                       │
├─────────────────────────────────────────────────────────────────────────────┤
│  Prompt: "Setup AWS Secrets Manager?" (Y/n)                                 │
│    └─► Runs aws.sh if yes (creates secrets + SSM parameters)                │
│                                                                             │
│  Prompt: "Setup GitHub Variables?" (y/N)                                    │
│    └─► Runs gha.sh if yes                                                   │
│                                                                             │
│  If either is selected, prompts for environment choice:                     │
│    1) Production only (recommended)                                         │
│    2) Production + Staging                                                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Script Details

### `bootstrap.sh`

**Purpose**: Complete first-time setup for a fresh deployment. This is the main entry point.

**Usage**:

```bash
# Using justfile (recommended)
just bootstrap                           # Default: robosystems-sso profile, us-east-1
just bootstrap my-fork-sso               # Custom SSO profile
just bootstrap my-fork-sso eu-west-1     # Custom profile and region
just bootstrap-oidc                      # Re-apply the deploy roles only (see below)

# Direct execution
./bin/setup/bootstrap.sh [--oidc] [--with-app-config] [profile] [region]
```

**Arguments**:
| Argument | Default | Description |
|----------|---------|-------------|
| `profile` | `robosystems-sso` | AWS SSO profile name |
| `region` | `us-east-1` | AWS region |
| `--oidc` | off | Only the OIDC stack and the three identity variables; nothing else is touched |
| `--with-app-config` | off | On a re-run, also offer the Secrets Manager / GitHub-variables setup |

**Re-running**: the script is built to be re-run against a live account without
risk. The OIDC stack is applied through a **change set** — you see the resource
changes and confirm, and a stack that already matches the template is reported
rather than updated; on an existing stack the trusted org/repo come from the
stack's own parameters, not a prompt. The ECR lifecycle step is silent when the
live policy matches `ecr-lifecycle-policy.json` (reconcile deliberately with
`just bootstrap-ecr-lifecycle` if it differs). The application-config step —
Secrets Manager and GitHub variables — runs on the **first** bootstrap only;
afterwards it needs `--with-app-config`, and the GitHub-variables part asks for a
typed `yes`, because `gha.sh` re-asserts every repository variable and would
reset a live account's sizing and toggles to defaults. After editing
`cloudformation/bootstrap-oidc.yaml`, `just bootstrap-oidc` is the whole
procedure.

**Prerequisites**:

- AWS CLI v2 installed
- AWS IAM Identity Center (SSO) enabled with admin access
- GitHub CLI installed and authenticated (`gh auth login`)
- `jq` installed

**What It Creates**:

| Resource             | Description                                                                             |
| -------------------- | --------------------------------------------------------------------------------------- |
| `.envrc`             | Local direnv config with AWS_PROFILE and AWS_REGION                                     |
| `~/.aws/config`      | SSO profile (if not exists)                                                             |
| CloudFormation Stack | `RoboSystemsGitHubOIDC`                                                                 |
| ECR Repository       | `robosystems` (fixed name, not derived from the repo)                                   |
| SES Identity         | Domain identity + DKIM records for transactional email                                  |
| GitHub Variables     | `AWS_ROLE_ARN`, `AWS_ACCOUNT_ID`, `AWS_REGION`, `AWS_SNS_ALERT_EMAIL`, `AWS_SES_DOMAIN` |
| Secrets Manager      | `robosystems/prod` (credentials)                                                        |
| SSM Parameters       | Feature flags + tuning parameters                                                       |

**Environment Variables Used**:
| Variable | Source | Description |
|----------|--------|-------------|
| `AWS_PROFILE` | Argument or env | SSO profile to use |
| `AWS_REGION` | Argument or env | AWS region |
| `ECR_LIFECYCLE_POLICY` | Env | Skips the lifecycle-policy prompt: `robust` (the bundled `ecr-lifecycle-policy.json`), `basic`, or `skip`/`none` |

---

### `aws.sh`

**Purpose**: Create secrets in AWS Secrets Manager and parameters in SSM Parameter Store.

**Usage**:

```bash
just setup-aws
# or
./bin/setup/aws.sh
```

**Prerequisites**:

- AWS CLI installed
- Valid AWS credentials (via SSO or otherwise)

**Safe to Re-run**: Yes. Existing resources are NEVER overwritten.

**Resources Created**:

| Resource   | Path                            | Description                    |
| ---------- | ------------------------------- | ------------------------------ |
| Secret     | `robosystems/prod`              | Production credentials         |
| Secret     | `robosystems/staging`           | Staging credentials (optional) |
| SSM Params | `/robosystems/{env}/features/*` | Feature flags                  |
| SSM Params | `/robosystems/{env}/tuning/*`   | Tuning parameters              |

The parameter sets live in the `params` arrays in `aws.sh` — read those for the
current names and seeded values, or `just ssm-list {env} {features,tuning}` for
what an environment actually has.

**Secrets Manager** (credentials only — placeholders you fill in, except the two
generated keys). `JWT_ISSUER` / `JWT_AUDIENCE` are only written in `internal`
access mode:

```json
{
  "JWT_ISSUER": "localhost",
  "JWT_AUDIENCE": "localhost",
  "JWT_SECRET_KEY": "[generated]",
  "CONNECTION_CREDENTIALS_KEY": "[generated]",
  "TURNSTILE_SECRET_KEY": "...",
  "TURNSTILE_SITE_KEY": "...",
  "INTUIT_CLIENT_ID": "...",
  "INTUIT_CLIENT_SECRET": "...",
  "INTUIT_ENVIRONMENT": "production",
  "INTUIT_REDIRECT_URI": "...",
  "SEC_GOV_USER_AGENT": "...",
  "OPENFIGI_API_KEY": "...",
  "STRIPE_SECRET_KEY": "...",
  "STRIPE_PUBLISHABLE_KEY": "...",
  "STRIPE_WEBHOOK_SECRET": "..."
}
```

**SSM Parameter Store** (feature flags + tuning):

```
/robosystems/{env}/features/
  RATE_LIMIT_ENABLED, BILLING_ENABLED, LOAD_SHEDDING_ENABLED, ...

/robosystems/{env}/tuning/
  cache/BALANCE_TTL, cache/JWT_TTL, ...
  admission/MEMORY_THRESHOLD, admission/CPU_THRESHOLD, admission/QUEUE_THRESHOLD, ...
  lbug_admission/MEMORY_THRESHOLD, lbug_admission/CPU_THRESHOLD, ...
  queues/MAX_SIZE, queues/MAX_CONCURRENT, ...
  circuits/THRESHOLD, circuits/TIMEOUT, ...
  load_shedding/START_PRESSURE, load_shedding/STOP_PRESSURE, ...
  mcp/MAX_RESULT_ROWS, mcp/MAX_RESULT_SIZE_MB, ...
  workers/MAX_WORKERS, timeouts/GRAPH_HTTP, timeouts/GRAPH_QUERY, ...
  sse/MAX_CONNECTIONS_PER_USER, sse/QUEUE_SIZE, limits/ORG_GRAPHS_DEFAULT, ...
  database/POOL_SIZE, database/MAX_OVERFLOW, ...
```

**Managing SSM parameters**:

```bash
just ssm-list prod features     # List feature flags
just ssm-list prod tuning       # List tuning parameters
just ssm-set prod features/BILLING_ENABLED true
just ssm-set prod tuning/cache/BALANCE_TTL 600
```

**Environment Variables Used**:
| Variable | Source | Description |
|----------|--------|-------------|
| `API_ACCESS_MODE` | Bootstrap | API access mode (internal or public) |
| `SETUP_STAGING` | Bootstrap | Whether to create staging resources |

---

### `gha.sh`

**Purpose**: Configure all GitHub Actions variables for explicit infrastructure control.

**Usage**:

```bash
just setup-gha
# or
./bin/setup/gha.sh
```

**Prerequisites**:

- GitHub CLI installed and authenticated
- In a git repository

**Optional**: Basic deployments work without this. All workflows have sensible defaults.

**Interactive Prompts**:

1. Environment choice (Production only vs Production + Staging) — skipped when
   bootstrap already passed `SETUP_STAGING`
2. Root domain (optional - leave empty for VPC-only deployment)
3. GitHub organization name
4. Repository name
5. AWS account ID
6. AWS region (defaults to the region bootstrap exported, else `us-east-1`)
7. Alert email (if not already set)
8. ECR repository name
9. Optional: RoboLedger/RoboInvestor app URLs — only offered when a root domain
   was given

**Variables Set** — `gha.sh` is the authoritative list of names and seeded
defaults; run `just gha-list` for what is live in this repo. The groups it
configures, in script order:

| Group                      | Covers                                                                                                                                 |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| Core & access              | `AWS_ECR_REPOSITORY`, `AWS_ACCOUNT_ID`, `AWS_REGION`, `ENVIRONMENT_*`, `API_ACCESS_MODE_*`                                             |
| Domains & app URLs         | `API_DOMAIN_NAME_*`, `ROBOSYSTEMS_{API,APP}_URL_*`, `PUBLIC_DOMAIN_NAME_*`, optional `ROBOLEDGER_APP_URL_*` / `ROBOINVESTOR_APP_URL_*` |
| API sizing & scaling       | `API_{MIN,MAX}_CAPACITY_*`, `API_CPU_*`, `API_MEMORY_*`, `API_{CPU,MEMORY}_TARGET_*`                                                   |
| Dagster                    | daemon/webserver CPU + memory, `DAGSTER_WEBSERVER_DESIRED_COUNT_*`, `DAGSTER_MAX_CONCURRENT_RUNS_*`, `DAGSTER_CONTAINER_INSIGHTS_*`    |
| Background worker          | `WORKER_*` (off by default)                                                                                                            |
| Fargate capacity providers | `API_FARGATE_SPOT_WEIGHT_*`, `DAGSTER_{DAEMON,WEBSERVER}_FARGATE_SPOT_WEIGHT_*`, the matching `*_FARGATE_BASE_*`                       |
| Database                   | `DATABASE_*` (instance size, storage, Multi-AZ, Postgres version, Performance Insights), `RDS_PROXY_*`                                 |
| Valkey                     | `VALKEY_*` (node type, node count, version, encryption, snapshot retention)                                                            |
| OpenSearch                 | `OPENSEARCH_*` (off by default)                                                                                                        |
| LadybugDB writers          | `LBUG_{STANDARD,LARGE,XLARGE,SHARED}_*`                                                                                                |
| Shared replicas            | `SHARED_REPLICAS_*`, `SHARED_REPOSITORIES_*`                                                                                           |
| Graph AMI                  | `GRAPH_AMI_AUTO_UPDATE`, `GRAPH_AMI_AUTO_DEPLOY`                                                                                       |
| Compliance & security      | `VPC_FLOW_LOGS_*`, `CLOUDTRAIL_*`, `SECURITY_ENABLED`, `SECURITY_CONFIG_ENABLED`, `AUDIT_*`, `SECRETS_ROTATION_ENABLED_*`              |
| WAF                        | `WAF_*`                                                                                                                                |
| Networking                 | `VPC_MAX_AVAILABILITY_ZONES`, `VPC_ENDPOINT_MODE`, `VPC_SECOND_OCTET`                                                                  |
| Runners & alerting         | `RUNNER_LABELS`, `RUNNER_SCOPE`, `AWS_SNS_ALERT_EMAIL`, `API_TARGET_ERROR_THRESHOLD`                                                   |
| Observability & publishing | `OBSERVABILITY_ENABLED_*`, `DOCKERHUB_PUBLISHING_ENABLED`                                                                              |

**Defaults worth knowing before the first deploy** (everything else is safe as
shipped):

- **Fargate Spot is off in production.** `API_FARGATE_SPOT_WEIGHT_PROD` and both
  Dagster spot weights seed to `0`; staging seeds them on (`90` for the API,
  `80` for Dagster). The on-demand weight is not a variable — the deploy
  workflow derives it as `100 - spot_weight`.
- **Compliance and security stacks ship off** — VPC flow logs, CloudTrail, the
  security baseline (GuardDuty, Security Hub, Access Analyzer, Inspector), the
  AWS Config recorder, audit-log retention, secrets rotation, and WAF. The
  baseline also needs the deploy-role grants from `bootstrap-oidc`, so re-run
  bootstrap after enabling it.
- **Only the standard LadybugDB writer tier deploys by default.** Large, xlarge,
  shared, the shared-replica fleet, and OpenSearch are all opt-in. Shared-replica
  Spot is off as well (`SHARED_REPLICAS_SPOT_ENABLED_*=false`, with
  `SHARED_REPLICAS_OD_BASE_*` and `SHARED_REPLICAS_SPOT_WEIGHT_*` at `0`).
- **`VPC_ENDPOINT_MODE=minimal`** — `gateway` is free (S3 + DynamoDB only),
  `minimal` adds the ECR endpoints that keep deployment image pulls off the NAT
  gateway, `full` costs roughly double `minimal`.
- **`RUNNER_LABELS=github-hosted`** — set it to e.g. `self-hosted,Linux,X64` to
  run workflows on your own runners; `RUNNER_SCOPE` (`repo` / `org` / `both`)
  controls where the workflows look for them.
- **Staging variables are only written when you pick "Production + Staging".**
  Choosing production only also *deletes* `ENVIRONMENT_STAGING`, which is what
  keeps staging deployments from firing accidentally.

---

### `bedrock.sh`

**Purpose**: Enable local Docker development with AWS Bedrock AI.

**Usage**:

```bash
just setup-bedrock
# or
./bin/setup/bedrock.sh
```

**Prerequisites**:

- Bootstrap completed (`just bootstrap`)
- SSO session active (`aws sso login --profile robosystems-sso`)

**What It Creates**:

| Resource   | Name                       | Description                               |
| ---------- | -------------------------- | ----------------------------------------- |
| IAM User   | `RoboSystemsBedrockDev`    | Development user for local Bedrock access |
| IAM Policy | `RoboSystemsBedrockAccess` | Permissions to invoke Claude models       |
| Access Key | (generated)                | Credentials for local Docker              |

**Policy Permissions**:

```json
{
  "Statement": [
    {
      "Action": [
        "bedrock:InvokeModel",
        "bedrock:InvokeModelWithResponseStream"
      ],
      "Resource": [
        "arn:aws:bedrock:*::foundation-model/anthropic.claude-*",
        "arn:aws:bedrock:*:*:inference-profile/*"
      ]
    },
    {
      "Action": ["bedrock:GetFoundationModel", "bedrock:ListFoundationModels"],
      "Resource": "*"
    }
  ]
}
```

**Updates to `.env`**:

```bash
AWS_BEDROCK_ACCESS_KEY_ID=AKIA...
AWS_BEDROCK_SECRET_ACCESS_KEY=...
```

**Note**: Only needed for local AI development. Production uses IAM role credentials via ECS task roles.

---

### `localstack-init.sh`

**Purpose**: Initialize LocalStack for local AWS service emulation.

**Execution**: Automatic - runs when LocalStack container starts via Docker Compose.

**S3 Buckets Created**:

| Bucket Name                    | Purpose                                    |
| ------------------------------ | ------------------------------------------ |
| `robosystems-shared-raw`       | External source downloads (SEC, FRED, BLS) |
| `robosystems-shared-processed` | Parquet files for graph ingestion          |
| `robosystems-user`             | User uploads, staging tables, exports      |
| `robosystems-public-data`      | Public data with CORS enabled              |
| `robosystems-local`            | Local test bucket for pytest               |

**DynamoDB Tables Created**:

| Table Name                                | Partition Key | Description                     |
| ----------------------------------------- | ------------- | ------------------------------- |
| `robosystems-graph-dev-graph-registry`    | `graph_id`    | Graph database metadata         |
| `robosystems-graph-dev-instance-registry` | `instance_id` | EC2/container instance tracking |

**Graph Registry GSIs**:

- `entity-index` - Query by entity_id
- `instance-index` - Query by instance_id
- `region-status-index` - Query by region + status
- `entity-region-index` - Query by entity + region
- `replication-status-index` - Query by replication status

**Instance Registry GSIs**:

- `region-cluster-index` - Query by region + cluster_type
- `cluster-capacity-index` - Query by cluster_group + capacity
- `health-region-index` - Query by status + region

**Local Instance Registration**:
Registers a local LadybugDB writer instance:

```json
{
  "instance_id": "local-lbug-writer",
  "private_ip": "graph-api",
  "status": "healthy",
  "api_endpoint": "http://graph-api:8001",
  "cluster_tier": "ladybug-standard",
  "node_type": "writer"
}
```

---

### `postgres-init.sh`

**Purpose**: Initialize PostgreSQL databases for local development.

**Execution**: Automatic - runs when PostgreSQL container starts via Docker Compose.

**Databases Created**:

| Database           | Purpose                                             |
| ------------------ | --------------------------------------------------- |
| `robosystems`      | Platform database (IAM, billing, graph metadata)    |
| `robosystems_test` | Platform test database for pytest                   |
| `dagster`          | Dagster metadata database                           |
| `extensions`       | Extensions OLTP database (roboledger, roboinvestor) |
| `extensions_test`  | Extensions test database for pytest                 |

`robosystems` itself is created by the Postgres container from `POSTGRES_DB`;
the script adds the other four. The platform and extensions databases have
independent Alembic histories — see the migration commands in the root
[CLAUDE.md](/CLAUDE.md).

---

## Environment Files

| File         | Purpose                                          | Created by                 | Git Ignored |
| ------------ | ------------------------------------------------ | -------------------------- | ----------- |
| `.envrc`     | Direnv config (AWS_PROFILE, AWS_REGION)          | `bootstrap.sh`             | Yes         |
| `.env`       | Docker Compose environment (container hostnames) | `just start` / `just init` | Yes         |
| `.env.local` | Local development (localhost URLs)               | `just start` / `just init` | Yes         |

`bedrock.sh` writes the Bedrock development credentials into `.env`.

---

## Commands Quick Reference

```bash
# Complete bootstrap (interactive)
just bootstrap

# Bootstrap with custom SSO profile
just bootstrap my-fork-sso

# Bootstrap with custom profile AND region
just bootstrap my-fork-sso eu-west-1

# Individual setup scripts
just setup-aws          # Secrets + SSM parameters
just setup-gha          # GitHub Actions variables
just setup-bedrock      # Local Bedrock development

# SSM Parameter Management
just ssm-list prod features     # List feature flags
just ssm-list prod tuning       # List tuning parameters
just ssm-set prod features/BILLING_ENABLED true
just ssm-set prod tuning/cache/BALANCE_TTL 600

# Generate cryptographic keys
just generate-key       # Single 32-byte base64 key
just generate-keys      # All required keys with descriptions

# Verify setup
gh variable list        # GitHub variables
gh secret list          # GitHub secrets
aws sts get-caller-identity  # AWS authentication
aws secretsmanager list-secrets  # AWS secrets
```

---

## Troubleshooting

### SSO Login Issues

| Issue               | Solution                                      |
| ------------------- | --------------------------------------------- |
| "Profile not found" | Run `just bootstrap` to create profile        |
| "Token expired"     | Run `aws sso login --profile robosystems-sso` |
| "Access denied"     | Verify SSO permissions in IAM Identity Center |

### GitHub CLI Issues

| Issue                  | Solution                                                |
| ---------------------- | ------------------------------------------------------- |
| "Not authenticated"    | Run `gh auth login`                                     |
| "Repository not found" | Ensure you're in the correct git repository             |
| "Permission denied"    | Check GitHub token scopes (need `repo` and `admin:org`) |

### Secret Conflicts

```bash
# Check if secret exists
aws secretsmanager describe-secret --secret-id robosystems/prod

# View secret value
aws secretsmanager get-secret-value --secret-id robosystems/prod \
  --query SecretString --output text | jq .

# Update specific value
aws secretsmanager put-secret-value --secret-id robosystems/prod \
  --secret-string '$(cat updated-secrets.json)'
```

### Variable Conflicts

```bash
# List all variables
gh variable list

# Delete a variable
gh variable delete VARIABLE_NAME

# Update a variable
gh variable set VARIABLE_NAME --body "new_value"
```

---

## Fork Considerations

When forking to a different AWS account:

1. **Run bootstrap with your profile**:

   ```bash
   just bootstrap my-fork-sso
   ```

2. **S3 Buckets**: GitHub Actions workflows automatically pass your AWS account ID as a namespace to CloudFormation, creating unique bucket names like `robosystems-{account-id}-shared-raw-{env}`.

3. **OIDC Trust**: Bootstrap updates the CloudFormation with your GitHub organization.

4. **Secrets**: New secrets are created with auto-generated keys.

5. **API Access Mode**: Choose your access mode during bootstrap:
   - `internal` (default): Access via SSM tunnel, JWT uses localhost
   - `public`: Internet-facing with custom domain and HTTPS

See the [Bootstrap Guide](https://github.com/RoboFinSystems/robosystems/wiki/Bootstrap-Guide) for complete fork deployment instructions.

---

## Security Notes

- **No long-term AWS credentials for deploy or runtime** - Uses SSO and OIDC
  (`bedrock.sh` mints a scoped IAM key for local AI development only)
- **Secrets auto-generated** - JWT keys, encryption keys created automatically
- **Resources never overwritten** - Re-running scripts is safe
- **Production/staging isolation** - Separate secrets and SSM params per environment
- **SSM uses FREE tier** - Feature flags and tuning at no cost
- **Bedrock credentials scoped** - Only Bedrock invoke permissions

---

## Related Documentation

- [Bootstrap Guide](https://github.com/RoboFinSystems/robosystems/wiki/Bootstrap-Guide) - Complete deployment walkthrough
- [CloudFormation Templates](/cloudformation/README.md) - Infrastructure templates
- [Configuration Module](/robosystems/config/README.md) - Application configuration
- [Graph Config](/.github/configs/graph.yml) - Graph database tier configurations
