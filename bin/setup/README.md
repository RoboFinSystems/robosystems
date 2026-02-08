# Setup Scripts

Scripts for bootstrapping and configuring RoboSystems deployments. These handle AWS infrastructure setup, GitHub Actions configuration, and local development environment initialization.

## Quick Reference

| Script               | Purpose                        | Prerequisites       | Est. Time |
| -------------------- | ------------------------------ | ------------------- | --------- |
| `bootstrap.sh`       | Complete first-time setup      | AWS SSO, GitHub CLI | 5-10 min  |
| `aws.sh`             | Secrets + SSM parameters       | AWS credentials     | 1-2 min   |
| `gha.sh`             | Configure ~80 GitHub variables | GitHub CLI          | 2-3 min   |
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
│    - Creates IAM Role for GitHub Actions                                    │
│    - Trusts: {GitHubOrg}/robosystems (main, release/*, v* tags)             │
│    - Trusts: {GitHubOrg}/*-app repos (frontend apps)                        │
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
│    - Repository name derived from GitHub repo (e.g., robosystems)           │
│    - Image scanning on push                                                 │
│    - Lifecycle policy: keep last 20 untagged images                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 6: CHECK GITHUB SECRETS                                               │
├─────────────────────────────────────────────────────────────────────────────┤
│  Checks for optional secrets (not required with OIDC):                      │
│    - ACTIONS_TOKEN (enables cross-workflow triggers)                        │
│    - ANTHROPIC_API_KEY (enables AI-powered PR/release notes)                │
│  Note: AWS credentials NOT needed with OIDC                                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 7: OPTIONAL CONFIGURATION (Interactive Prompts)                       │
├─────────────────────────────────────────────────────────────────────────────┤
│  Prompt: "Setup AWS Secrets Manager?" (Y/n)                                 │
│    └─► Runs aws.sh if yes (creates secrets + SSM parameters)                │
│                                                                             │
│  Prompt: "Setup GitHub Variables?" (y/N)                                    │
│    └─► Runs gha.sh if yes                                                   │
│                                                                             │
│  Both prompt for environment choice:                                        │
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

# Direct execution
./bin/setup/bootstrap.sh [profile] [region]
```

**Arguments**:
| Argument | Default | Description |
|----------|---------|-------------|
| `profile` | `robosystems-sso` | AWS SSO profile name |
| `region` | `us-east-1` | AWS region |

**Prerequisites**:

- AWS CLI v2 installed
- AWS IAM Identity Center (SSO) enabled with admin access
- GitHub CLI installed and authenticated (`gh auth login`)
- `jq` installed

**What It Creates**:

| Resource             | Description                                                           |
| -------------------- | --------------------------------------------------------------------- |
| `.envrc`             | Local direnv config with AWS_PROFILE and AWS_REGION                   |
| `~/.aws/config`      | SSO profile (if not exists)                                           |
| CloudFormation Stack | `RoboSystemsGitHubOIDC`                                               |
| ECR Repository       | `robosystems` (or repo name)                                          |
| GitHub Variables     | `AWS_ROLE_ARN`, `AWS_ACCOUNT_ID`, `AWS_REGION`, `AWS_SNS_ALERT_EMAIL` |
| Secrets Manager      | `robosystems/prod` (credentials)                                      |
| SSM Parameters       | Feature flags + tuning parameters                                     |

**Environment Variables Used**:
| Variable | Source | Description |
|----------|--------|-------------|
| `AWS_PROFILE` | Argument or env | SSO profile to use |
| `AWS_REGION` | Argument or env | AWS region |

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
| SSM Params | `/robosystems/{env}/features/*` | Feature flags (22 params)      |
| SSM Params | `/robosystems/{env}/tuning/*`   | Tuning parameters (19 params)  |

**Secrets Manager** (credentials only):

```json
{
  "JWT_SECRET_KEY": "[generated]",
  "JWT_ISSUER": "localhost",
  "JWT_AUDIENCE": "localhost",
  "CONNECTION_CREDENTIALS_KEY": "[generated]",
  "GRAPH_BACKUP_ENCRYPTION_KEY": "[generated]",
  "INTUIT_CLIENT_ID": "...",
  "INTUIT_CLIENT_SECRET": "...",
  "PLAID_CLIENT_ID": "...",
  "STRIPE_SECRET_KEY": "...",
  "SEC_GOV_USER_AGENT": "...",
  "TURNSTILE_SECRET_KEY": "..."
}
```

**SSM Parameter Store** (feature flags + tuning):

```
/robosystems/{env}/features/
  RATE_LIMIT_ENABLED, BILLING_ENABLED, SSE_ENABLED, ...

/robosystems/{env}/tuning/
  cache/BALANCE_TTL, cache/JWT_TTL, ...
  admission/MEMORY_THRESHOLD, admission/CPU_THRESHOLD, admission/QUEUE_THRESHOLD, ...
  lbug_admission/MEMORY_THRESHOLD, lbug_admission/CPU_THRESHOLD, ...
  queues/MAX_SIZE, queues/MAX_CONCURRENT, ...
  circuits/THRESHOLD, circuits/TIMEOUT, ...
  load_shedding/START_PRESSURE, load_shedding/STOP_PRESSURE, ...
  mcp/MAX_RESULT_ROWS, mcp/MAX_RESULT_SIZE_MB, ...
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
| `API_ACCESS_MODE` | Bootstrap | API access mode (internal, public, public-http) |
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

1. Environment choice (Production only vs Production + Staging)
2. Root domain (optional - leave empty for VPC-only deployment)
3. GitHub organization name
4. Repository name
5. AWS account ID
6. Alert email (if not already set)
7. ECR repository name
8. Optional: RoboLedger/RoboInvestor app URLs

**Variables Set** (~80 total):

#### Core Configuration

| Variable              | Default       | Description                           |
| --------------------- | ------------- | ------------------------------------- |
| `REPOSITORY_NAME`     | User input    | Full repo path (org/repo)             |
| `ECR_REPOSITORY`      | `robosystems` | ECR repository name                   |
| `AWS_ACCOUNT_ID`      | User input    | AWS account ID                        |
| `AWS_REGION`          | `us-east-1`   | AWS region                            |
| `ENVIRONMENT_PROD`    | `prod`        | Production environment name           |
| `ENVIRONMENT_STAGING` | `staging`     | Staging environment name (if enabled) |

#### Domain Configuration (optional - skip for VPC-only)

| Variable                   | Example                      | Description              |
| -------------------------- | ---------------------------- | ------------------------ |
| `API_DOMAIN_NAME_ROOT`     | `robosystems.ai`             | Root domain              |
| `API_DOMAIN_NAME_PROD`     | `api.robosystems.ai`         | Production API subdomain |
| `API_DOMAIN_NAME_STAGING`  | `staging.api.robosystems.ai` | Staging API subdomain    |
| `ROBOSYSTEMS_API_URL_PROD` | `https://api.robosystems.ai` | Production API URL       |
| `ROBOSYSTEMS_APP_URL_PROD` | `https://robosystems.ai`     | Production app URL       |

#### API Scaling

| Variable             | Prod Default | Staging Default |
| -------------------- | ------------ | --------------- |
| `API_MIN_CAPACITY_*` | `1`          | `1`             |
| `API_MAX_CAPACITY_*` | `10`         | `2`             |
| `API_ASG_REFRESH_*`  | `true`       | `true`          |

#### Dagster Configuration

| Variable                        | Prod Default | Description           |
| ------------------------------- | ------------ | --------------------- |
| `DAGSTER_DAEMON_CPU_*`          | `1024`       | Daemon CPU units      |
| `DAGSTER_DAEMON_MEMORY_*`       | `2048`       | Daemon memory (MB)    |
| `DAGSTER_WEBSERVER_CPU_*`       | `512`        | Webserver CPU units   |
| `DAGSTER_WEBSERVER_MEMORY_*`    | `1024`       | Webserver memory (MB) |
| `DAGSTER_RUN_JOB_CPU_*`         | `1024`       | Run job CPU units     |
| `DAGSTER_RUN_JOB_MEMORY_*`      | `4096`       | Run job memory (MB)   |
| `DAGSTER_MAX_CONCURRENT_RUNS_*` | `20`         | Max concurrent runs   |
| `DAGSTER_REFRESH_ECS_*`         | `true`       | Refresh ECS on deploy |
| `DAGSTER_CONTAINER_INSIGHTS_*`  | `disabled`   | Container insights    |
| `RUN_MIGRATIONS_*`              | `true`       | Run DB migrations     |

#### Database Configuration

| Variable                           | Prod Default   | Description          |
| ---------------------------------- | -------------- | -------------------- |
| `DATABASE_ENGINE_*`                | `postgres`     | Database engine      |
| `DATABASE_INSTANCE_SIZE_*`         | `db.t4g.small` | RDS instance type    |
| `DATABASE_ALLOCATED_STORAGE_*`     | `20`           | Initial storage (GB) |
| `DATABASE_MAX_ALLOCATED_STORAGE_*` | `100`          | Max storage (GB)     |
| `DATABASE_MULTI_AZ_ENABLED_*`      | `false`        | Multi-AZ deployment  |
| `DATABASE_POSTGRES_VERSION_*`      | `16.11`        | PostgreSQL version   |

#### Valkey (Redis) Configuration

| Variable                           | Prod Default              | Description        |
| ---------------------------------- | ------------------------- | ------------------ |
| `VALKEY_NODE_TYPE_*`               | `cache.t4g.micro`         | Cache node type    |
| `VALKEY_NUM_NODES_*`               | `1`                       | Number of nodes    |
| `VALKEY_ENCRYPTION_ENABLED_*`      | `true`                    | Enable encryption  |
| `VALKEY_SNAPSHOT_RETENTION_DAYS_*` | `7` (prod), `0` (staging) | Snapshot retention |
| `VALKEY_VERSION_*`                 | `8.1`                     | Valkey version     |

#### LadybugDB Writer Configuration

| Variable                        | Prod Default | Description                              |
| ------------------------------- | ------------ | ---------------------------------------- |
| `LBUG_STANDARD_MIN_INSTANCES_*` | `1`          | Min standard instances (always deployed) |
| `LBUG_STANDARD_MAX_INSTANCES_*` | `10`         | Max standard instances                   |
| `LBUG_LARGE_ENABLED_*`          | `false`      | Enable large tier                        |
| `LBUG_LARGE_MIN_INSTANCES_*`    | `0`          | Min large instances                      |
| `LBUG_LARGE_MAX_INSTANCES_*`    | `20`         | Max large instances                      |
| `LBUG_XLARGE_ENABLED_*`         | `false`      | Enable xlarge tier                       |
| `LBUG_XLARGE_MIN_INSTANCES_*`   | `0`          | Min xlarge instances                     |
| `LBUG_XLARGE_MAX_INSTANCES_*`   | `10`         | Max xlarge instances                     |
| `LBUG_SHARED_ENABLED_*`         | `false`      | Enable shared tier                       |

#### Graph Settings

| Variable         | Default       | Description                 |
| ---------------- | ------------- | --------------------------- |
| `GRAPH_AMI_ID_*` | Auto-detected | Amazon Linux 2023 ARM64 AMI |

#### Compliance & Security

| Variable                         | Default  | Description          |
| -------------------------------- | -------- | -------------------- |
| `VPC_FLOW_LOGS_ENABLED`          | `true`   | Enable VPC flow logs |
| `VPC_FLOW_LOGS_RETENTION_DAYS`   | `90`     | Flow log retention   |
| `VPC_FLOW_LOGS_TRAFFIC_TYPE`     | `REJECT` | Traffic type to log  |
| `CLOUDTRAIL_ENABLED`             | `true`   | Enable CloudTrail    |
| `CLOUDTRAIL_LOG_RETENTION_DAYS`  | `90`     | CloudTrail retention |
| `CLOUDTRAIL_DATA_EVENTS_ENABLED` | `false`  | Log S3 data events   |

#### WAF Configuration

| Variable                        | Default | Description               |
| ------------------------------- | ------- | ------------------------- |
| `WAF_ENABLED_*`                 | `true`  | Enable WAF                |
| `WAF_RATE_LIMIT_PER_IP`         | `3000`  | Requests per 5 min per IP |
| `WAF_GEO_BLOCKING_ENABLED`      | `false` | Block non-US/CA traffic   |
| `WAF_AWS_MANAGED_RULES_ENABLED` | `true`  | Use AWS managed rules     |

#### Runner Configuration

| Variable        | Default         | Description                    |
| --------------- | --------------- | ------------------------------ |
| `RUNNER_LABELS` | `github-hosted` | Runner labels (or self-hosted) |
| `RUNNER_SCOPE`  | `both`          | Where to check for runners     |

#### Other

| Variable                       | Default    | Description            |
| ------------------------------ | ---------- | ---------------------- |
| `AWS_SNS_ALERT_EMAIL`          | User input | CloudWatch alert email |
| `MAX_AVAILABILITY_ZONES`       | `5`        | Max AZs to use         |
| `OBSERVABILITY_ENABLED_*`      | `true`     | Enable observability   |
| `DOCKERHUB_PUBLISHING_ENABLED` | `false`    | Publish to Docker Hub  |

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

| Database           | Purpose                                            |
| ------------------ | -------------------------------------------------- |
| `robosystems`      | Main application database (IAM, billing, metadata) |
| `robosystems_test` | Test database for pytest                           |
| `dagster`          | Dagster metadata database                          |

---

## Environment Files

Bootstrap creates/updates these files:

| File         | Purpose                                          | Git Ignored |
| ------------ | ------------------------------------------------ | ----------- |
| `.envrc`     | Direnv config (AWS_PROFILE, AWS_REGION)          | No          |
| `.env`       | Docker Compose environment (container hostnames) | Yes         |
| `.env.local` | Local development (localhost URLs)               | Yes         |

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
   - `internal` (default): Access via bastion tunnel, JWT uses localhost
   - `public`: Internet-facing with custom domain and HTTPS
   - `public-http`: Internet-facing via ALB DNS, HTTP only

See the [Bootstrap Guide](https://github.com/RoboFinSystems/robosystems/wiki/Bootstrap-Guide) for complete fork deployment instructions.

---

## Security Notes

- **No long-term AWS credentials stored** - Uses SSO and OIDC
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
