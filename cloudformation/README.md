# CloudFormation Templates

Infrastructure as Code for deploying RoboSystems to AWS. All templates are deployed via GitHub Actions workflows, except `bootstrap-oidc.yaml` which is deployed locally to enable CI/CD authentication.

**This README describes what each template is for and how the stacks fit together. It deliberately does not restate parameter tables, defaults, or instance sizes.** Those live in each template's own `Parameters:` block, in `.github/configs/graph.yml` (graph tiers), and in the GitHub Actions variables the deploy workflows pass in (`just gha-list`). Template defaults are frequently *not* what an environment runs — sizing is overridden per environment by workflow inputs — so a restated default here is worse than no value at all.

## Quick Reference

| Template | Purpose | Deployed by | Dependencies |
|----------|---------|-------------|--------------|
| `bootstrap-oidc.yaml` | GitHub OIDC for CI/CD | `just bootstrap` (local) | None |
| `vpc.yaml` | Network foundation | `deploy-vpc.yml` | None |
| `cloudtrail.yaml` | Audit logging | `deploy-vpc.yml` (`cloudtrail` job) | None |
| `security.yaml` | Account-global detective controls | `deploy-vpc.yml` (`security` job) | None |
| `s3.yaml` | Object storage | `deploy-s3.yml` | None |
| `postgres.yaml` | RDS PostgreSQL | `deploy-postgres.yml` | VPC |
| `valkey.yaml` | ElastiCache cache | `deploy-valkey.yml` | VPC |
| `opensearch.yaml` | Document search | `deploy-opensearch.yml` | VPC, Bastion |
| `bastion.yaml` | Secure SSM access | `deploy-bastion.yml` | VPC, S3, Valkey |
| `graph-infra.yaml` | Graph DB registries | `deploy-graph-infra.yml` | None |
| `graph-volumes.yaml` | EBS management | `deploy-graph-volumes.yml` | VPC, graph-infra |
| `graph-ladybug.yaml` | LadybugDB writers | `deploy-graph.yml` → `deploy-graph-ladybug.yml` | VPC, S3, Valkey, Postgres, graph-volumes |
| `graph-ladybug-replicas.yaml` | LadybugDB readers | `deploy-graph.yml` → `deploy-graph-replicas.yml` | VPC, S3, Valkey, Postgres, graph-ladybug (shared) |
| `api.yaml` | ECS API service | `deploy-api.yml` | VPC, S3, Postgres, Valkey, Bastion |
| `waf.yaml` | Web firewall | `deploy-api.yml` (`waf` job) | API (for ALB ARN) |
| `audit.yaml` | Long-retention audit log forwarding | `deploy-api.yml` (`audit` job) | API (log group) |
| `dagster.yaml` | Workflow orchestration | `deploy-dagster.yml` | VPC, S3, Postgres, Valkey |
| `worker.yaml` | Background task worker | `deploy-dagster.yml` (same job) | Dagster (cluster + SG), Postgres, Valkey, S3 |
| `prometheus.yaml` | Metrics collection | `deploy-prometheus.yml` | None |
| `grafana.yaml` | Dashboards | `deploy-grafana.yml` | None |

Five templates have no workflow of their own and ride a sibling's: `waf.yaml` and `audit.yaml` deploy from `deploy-api.yml`, `cloudtrail.yaml` and `security.yaml` from `deploy-vpc.yml`, and `worker.yaml` from `deploy-dagster.yml`.

**Cost**: templates that can state a meaningful figure publish an `EstimatedMonthlyCost` output (`waf.yaml`, `audit.yaml`, `security.yaml`); everything else is dominated by instance sizing, which is set per environment by GitHub variables. Use `/cost-review` for actual spend rather than an estimate table here.

### Feature-gated stacks

Several stacks are off unless a GitHub variable turns them on:

| Stack | Variable |
|-------|----------|
| `opensearch.yaml` | `OPENSEARCH_ENABLED_{PROD,STAGING}` |
| `waf.yaml` | `WAF_ENABLED_{PROD,STAGING}` |
| `audit.yaml` | `AUDIT_ENABLED_{PROD,STAGING}` |
| `worker.yaml` | `WORKER_ENABLED_{PROD,STAGING}` |
| `prometheus.yaml`, `grafana.yaml` | `OBSERVABILITY_ENABLED_{PROD,STAGING}` |
| `cloudtrail.yaml` | `CLOUDTRAIL_ENABLED` (account-global, no env suffix) |
| `security.yaml` | `SECURITY_ENABLED` (account-global, no env suffix) |

`cloudtrail.yaml` and `security.yaml` create per-account singletons, so each is **one shared stack** (`RoboSystemsCloudTrail`, `RoboSystemsSecurity`) across both environments — never per-environment, or the second deploy collides. `vpc.yaml` and `grafana.yaml` are likewise shared.

## Deployment Order

`prod.yml` / `staging.yml` encode the real dependency graph as job `needs:`. The shape:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PHASE 1: BOOTSTRAP                                │
├─────────────────────────────────────────────────────────────────────────────┤
│  bootstrap-oidc.yaml                                                        │
│  └── Enables GitHub Actions → AWS authentication (deploy locally first)     │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        PHASE 2: FOUNDATION                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  vpc.yaml ◄───────── Core networking (VPC, subnets, NAT, IGW)               │
│  ├── cloudtrail.yaml, security.yaml (same workflow, flag-gated)             │
│  s3.yaml  ◄───────── Six buckets + CloudFront for public data               │
│  └── package-scripts (uploads userdata/Lambda artifacts to the bucket)      │
│  graph-infra.yaml ◄─ DynamoDB registries (instance, graph, volume)          │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        PHASE 3: DATA LAYER                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  postgres.yaml ◄──── RDS PostgreSQL (depends on VPC)                        │
│  valkey.yaml ◄────── ElastiCache Valkey (depends on VPC)                    │
│  bastion.yaml ◄───── SSM bastion host (depends on VPC, S3, Valkey)          │
│  opensearch.yaml ◄── Managed OpenSearch domain (depends on VPC, Bastion)    │
│  graph-volumes.yaml ◄ EBS lifecycle Lambdas (depends on VPC, graph-infra)   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      PHASE 4: GRAPH TIER                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│  graph-ladybug.yaml ◄──────── One writer stack per enabled tier, fanned out │
│                               by a matrix built from graph.yml              │
│  graph-ladybug-replicas.yaml ◄ Read replica fleet with ALB (shared repos)   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      PHASE 5: APPLICATION SERVICES                          │
├─────────────────────────────────────────────────────────────────────────────┤
│  api.yaml ◄─────────── ECS Fargate API + ALB                                │
│  ├── waf.yaml, audit.yaml (same workflow, flag-gated)                       │
│  dagster.yaml ◄──────── Dagster daemon + webserver (Cloud Map, no ALB)      │
│  └── worker.yaml (same job, flag-gated, reuses the Dagster cluster)         │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      PHASE 6: OBSERVABILITY (Optional)                      │
├─────────────────────────────────────────────────────────────────────────────┤
│  prometheus.yaml ◄─── Amazon Managed Prometheus (standalone)                │
│  grafana.yaml ◄────── Amazon Managed Grafana (uses Prometheus, CloudWatch)  │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Template Details

Parameters are documented in each template — read the `Parameters:` block for names, types, defaults, and allowed values. What follows is the purpose, the resources worth knowing about, and the **export names** other stacks import.

### Bootstrap

#### `bootstrap-oidc.yaml`
**Purpose**: Enables passwordless GitHub Actions → AWS authentication using OIDC federation, for this repo and the three frontend app repos plus the holon viewer.

**Deploy**: Locally with `just bootstrap` before any CI/CD workflows can run.

**Key Resources**:
- `AWS::IAM::OIDCProvider` - GitHub OIDC identity provider
- `AWS::IAM::Role` - Deploy role for this repo, a separate frontend role, and an app super-admin role

**Exports**:
- `{StackName}-OIDCProviderArn`
- `{StackName}-GitHubActionsRoleArn`, `-GitHubActionsRoleName`
- `{StackName}-GitHubActionsFrontendRoleArn`, `-GitHubActionsFrontendRoleName`
- `{StackName}-AppSuperAdminRoleArn`, `-AppSuperAdminRoleName`

---

### Core Infrastructure

#### `vpc.yaml`
**Purpose**: Foundation networking - VPC with public/private subnets across up to 5 AZs, NAT gateway, Internet gateway, and optional VPC endpoints. Shared between prod and staging.

**Key Resources**:
- VPC with configurable CIDR `10.X.0.0/16` (where X = `VpcSecondOctet`)
- Up to 5 public subnets (`10.X.10.0/24` through `10.X.14.0/24`)
- Up to 5 private subnets (`10.X.1.0/24` through `10.X.5.0/24`)
- NAT Gateway (single, shared across all AZs)
- Internet Gateway, route tables, security groups
- VPC endpoints (`DeployVpcEndpoints`: `gateway` = free S3+DynamoDB, `minimal` ≈ $22/mo adding interface endpoints in 1 AZ, `full` ≈ $45/mo across 2 AZs)
- Optional VPC Flow Logs to S3 for SOC 2 compliance

**VPC Peering Note**: Forks can set `VpcSecondOctet` to a non-zero value (e.g. `2` for `10.2.0.0/16`) to enable VPC peering with another `10.0.0.0/16` VPC. Configured via the `VPC_SECOND_OCTET` GitHub variable.

**Exports** (critical for other stacks):
- `{StackName}-{Env}-VpcId` - VPC ID
- `{StackName}-{Env}-VpcCidr` - VPC CIDR block (for peering/security groups)
- `{StackName}-{Env}-PublicSubnetIds` - Comma-separated public subnet IDs
- `{StackName}-{Env}-PrivateSubnetIds` - Comma-separated private subnet IDs
- `{StackName}-{Env}-NatGatewayIP` - NAT Gateway Elastic IP (first AZ)
- `{StackName}-VPCFlowLogsEnabled`, `{StackName}-VPCFlowLogsBucket`

---

#### `postgres.yaml`
**Purpose**: RDS PostgreSQL for IAM, billing, subscriptions, and metadata. Hosts both the `robosystems` platform database and the `extensions` OLTP database.

**Dependencies**: VPC stack

**Key Resources**:
- `AWS::RDS::DBInstance` - PostgreSQL database
- `AWS::RDS::DBSubnetGroup` - Multi-AZ subnet group
- `AWS::EC2::SecurityGroup` - Database access security group
- `AWS::SecretsManager::Secret` + rotation Lambda - master credential rotation
- `AWS::RDS::DBProxy` - optional, behind `EnableRDSProxy`
- CloudWatch alarms for CPU, memory, disk queue, free storage

**Exports** (all carry an `{Env}` segment):
- `{StackName}-{Env}-DatabaseEndpoint` / `-RDSEndpoint`
- `{StackName}-{Env}-DatabasePort` / `-RDSPort`
- `{StackName}-{Env}-RDSSecurityGroupId`
- `{StackName}-{Env}-RDSPasswordSecretArn`, `-PostgresSecretArnPattern`
- `{StackName}-{Env}-DatabaseProxyEndpoint`, `-DatabaseProxyArn`, `-RDSProxyEnabled`
- plus engine/identifier/storage metadata exports

---

#### `valkey.yaml`
**Purpose**: ElastiCache Valkey (Redis-compatible) for caching, rate limiting, SSE messaging, and distributed locks.

**Dependencies**: VPC stack

**Key Resources**:
- `AWS::ElastiCache::ReplicationGroup` - Valkey cluster (`Engine: valkey`)
- `AWS::EC2::SecurityGroup` - cluster SG plus a **client** SG that other stacks attach to
- `AWS::SecretsManager::Secret` + rotation Lambda - AUTH token rotation

**Exports** (all carry an `{Env}` segment):
- `{StackName}-{Env}-ValkeyEndpoint`, `-ValkeyPort`, `-ValkeyConfigEndpoint`
- `{StackName}-{Env}-ValkeyUrl`, `-ValkeyUrlWithAuth`
- `{StackName}-{Env}-ValkeyClientSecurityGroupId` - attach this to clients
- `{StackName}-{Env}-ValkeyAuthSecretArn`, `-ValkeyAuthSecretName`
- plus engine/encryption/replication-group metadata exports

---

#### `opensearch.yaml`
**Purpose**: Amazon OpenSearch Service managed domain for full-text document search (BM25) across SEC filing narratives, iXBRL disclosures, and text blocks.

**Dependencies**: VPC stack, Bastion (its security group is granted access for debugging)

**Key Resources**:
- `AWS::OpenSearchService::Domain` - managed domain (single-node/single-AZ by default; zone awareness and dedicated masters are parameterized)
- `AWS::EC2::SecurityGroup` - domain SG + client SG
- `AWS::CloudWatch::Alarm` - cluster red, low free storage, high JVM memory, writes blocked
- `AWS::SNS::Topic` - alert notifications
- `AWS::Logs::LogGroup` - application and slow query logs

**Exports** (all carry an `{Env}` segment):
- `{StackName}-{Env}-OpenSearchEndpoint`, `-OpenSearchDomainArn`, `-OpenSearchDomainName`
- `{StackName}-{Env}-OpenSearchClientSecurityGroupId`
- `{StackName}-{Env}-OpenSearchNotificationTopicArn`

**Feature-flag gated**: deployed via `deploy-opensearch.yml`, controlled by `OPENSEARCH_ENABLED_{ENV}`. Instance type, count, and version come from `OPENSEARCH_INSTANCE_TYPE_{ENV}` / `OPENSEARCH_INSTANCE_COUNT_{ENV}` / `OPENSEARCH_VERSION_{ENV}` — prod and staging deliberately differ. Application-side access is gated separately by the `SEMANTIC_SEARCH_ENABLED` SSM parameter read at runtime.

---

#### `s3.yaml`
**Purpose**: S3 buckets for deployment artifacts, raw and processed shared data, public data, user uploads, and compute logs.

**Dependencies**: None (standalone)

**Key Resources** — six buckets, named `robosystems-{namespace}-{role}-{env}` where the workflow passes the AWS account ID as `Namespace` (falling back to `robosystems-{role}-{env}` when unset):
- `…-deployment-{env}` - userdata scripts, Lambda packages, and the `api.yaml` template body
- `…-shared-raw-{env}` - external source downloads (SEC, FRED, BLS)
- `…-shared-processed-{env}` - parquet files for graph ingestion
- `…-public-data-{env}` - publicly readable data, fronted by CloudFront
- `…-user-{env}` - user uploads, staging tables, exports
- `…-logs-{env}` - Dagster compute logs from ECS runs

All six are `DeletionPolicy: Retain`. The stack also creates the CloudFront distribution/OAC for public data and two managed IAM policies (graph-writer and shared-data-writer).

**Exports** (all carry an `{Env}` segment): `-DeploymentBucketName`/`-Arn`, `-SharedRawBucketName`/`-Arn`, `-SharedProcessedBucketName`/`-Arn`, `-PublicDataBucketName`/`-Arn`, `-UserDataBucketName`/`-Arn`, `-LogsBucketName`/`-Arn`, plus `-PublicDataCDNURL`, `-CloudFrontDistributionId`, `-GraphWriterS3PolicyArn`, `-SharedDataWriterPolicyArn`.

---

### Application Services

#### `api.yaml`
**Purpose**: ECS Fargate API service with Application Load Balancer.

**Dependencies**: VPC, S3, Postgres, Valkey, Bastion. Endpoints and security group IDs are passed in as stack parameters by `deploy-api.yml`, which reads them from the upstream stacks' outputs.

**Deployed from S3**: at ~57 KB the template exceeds CloudFormation's 51,200-byte `--template-body` limit, so `deploy-api.yml` uploads it to the deployment bucket and deploys with `--template-url`.

**Key Resources**:
- `AWS::ECS::Cluster`, `AWS::ECS::Service`, `AWS::ECS::TaskDefinition`
- `AWS::ElasticLoadBalancingV2::LoadBalancer` / `TargetGroup` / `Listener` / `ListenerRule`
- `AWS::ServiceDiscovery::PrivateDnsNamespace` - `{env}.robosystems.local`
- `AWS::SecretsManager::Secret` + rotation Lambda - admin API key
- `AWS::Logs::LogGroup` and a set of security alarms (failed admin auth, auth-failure spike, injection attempt, privilege escalation, authorization-denied spike) plus ALB health/latency/5xx alarms

**Access mode**: `ApiAccessMode` selects `public` (ALB with ACM certificate and Route53 record) or `internal` (private ALB reachable only through the SSM tunnel).

**Exports**:
- `{StackName}-ApiALBArn` - ALB ARN (consumed by `waf.yaml`)
- `{StackName}-LoadBalancerDNS` - ALB DNS name
- `{StackName}-ApiSecurityGroupId`
- `{StackName}-AdminKeySecretArn`
- `{StackName}-ServiceDiscoveryEndpoint`, `-ServiceDiscoveryNamespaceId`, `-ServiceDiscoveryNamespaceArn`
- `{StackName}-{Env}-ECRRepositoryUsed`, `{StackName}-{Env}-CertificateArn`

---

#### `dagster.yaml`
**Purpose**: Dagster orchestration platform (daemon + webserver) on ECS Fargate.

**Dependencies**: VPC, S3, Postgres, Valkey

**Key Resources**:
- `AWS::ECS::Cluster`, two `AWS::ECS::Service` (daemon, webserver), task definitions for both plus a run-job task definition that jobs override via `ecs/cpu`, `ecs/memory`, `ecs/ephemeral_storage` tags
- `AWS::ServiceDiscovery::PrivateDnsNamespace` / `Service` - `dagster.{env}.robosystems.local`
- `AWS::Logs::LogGroup`, SNS alert topic, backup-coverage-gap alarm

There is **no load balancer**: the webserver is reached over Cloud Map private DNS through the SSM tunnel (`just tunnel {env} dagster`). The webserver's desired count defaults to 0 — it is scaled up on demand.

**Exports**:
- `{StackName}-ClusterName`, `-ClusterArn`
- `{StackName}-SecurityGroupId` (output key `DagsterSecurityGroupId`)
- `{StackName}-DaemonServiceArn`, `-WebserverServiceArn`
- `{StackName}-RunJobTaskDefinition`
- `{StackName}-InternalEndpoint`, `-DagsterUrl`

---

#### `worker.yaml`
**Purpose**: Background task worker service on ECS Fargate. Runs the queue consumer for asynchronous work, with optional queue-depth-based autoscaling (`WorkerAutoscalingEnabled`).

**Dependencies**: Dagster (reuses its ECS cluster and security group — both are read from the Dagster stack's outputs and passed in), Postgres, Valkey, S3

**Key Resources**:
- `AWS::ECS::Service` - Fargate worker service on the Dagster cluster
- `AWS::ECS::TaskDefinition` - worker container definition
- Queue-depth-high / queue-depth-zero / DLQ-depth alarms and an SNS alert topic
- `AWS::Logs::LogGroup`

**Exports**:
- `{StackName}-WorkerServiceArn`
- `{StackName}-WorkerTaskDefinitionArn`

---

#### `bastion.yaml`
**Purpose**: EC2 bastion host for secure SSM-based access to private resources. No SSH keys or open ports required — the security group has no ingress rules at all.

**Dependencies**: VPC (public subnet), S3 (userdata script), Valkey (SG for tunnelling)

**Key Resources**:
- `AWS::EC2::Instance` - ARM64 Amazon Linux 2023 instance in a public subnet
- `AWS::IAM::Role` / `InstanceProfile` - SSM-enabled, with read access to `/robosystems/*/features` and `/robosystems/*/tuning` SSM parameters for debugging
- `AWS::Logs::LogGroup`, CPU-high alarm

**Exports**:
- `{StackName}-BastionHostId` - instance ID for SSM sessions
- `{StackName}-BastionSecurityGroupId` - granted access by Postgres, Valkey, OpenSearch

**Usage**:
```bash
# Preferred: the wrapper resolves the instance and ports for you
# (services: postgres, valkey, dagster, api, api-internal, all)
just tunnel prod postgres

# Raw equivalents
aws ssm start-session --target i-0123456789abcdef0

aws ssm start-session --target i-0123456789abcdef0 \
  --document-name AWS-StartPortForwardingSessionToRemoteHost \
  --parameters '{"host":["mydb.xxx.rds.amazonaws.com"],"portNumber":["5432"],"localPortNumber":["5432"]}'
```

---

### Graph Database Infrastructure

The graph database system uses a modular architecture with separate templates for infrastructure, storage, writers, and readers.

#### `graph-infra.yaml`
**Purpose**: Core graph database infrastructure - DynamoDB registries for tracking instances, graphs, and volumes, plus the Graph API credential and the container-refresh control plane.

**Dependencies**: None (standalone)

**Key Resources**:
- `AWS::DynamoDB::Table` × 3 - `robosystems-graph-{env}-instance-registry`, `-graph-registry`, `-volume-registry`
- `AWS::SecretsManager::Secret` + `RotationSchedule` - Graph API authentication, rotated by Lambda
- `AWS::SSM::Document` + `AWS::Lambda::Function` - fleet container refresh (`{StackName}-graph-container-refresh`)
- `AWS::Logs::LogGroup` - unified `/robosystems/{env}/graph-api` log group
- 15 CloudWatch alarms covering registry read/write throttles, consumed capacity, database capacity, and Lambda errors

**DynamoDB Table Schemas**:
| Table | Partition Key | GSIs | Purpose |
|-------|--------------|------|---------|
| instance-registry | `instance_id` | status/database_count, cluster_tier/status, backend_type/status | Track EC2 writer/reader instances |
| graph-registry | `graph_id` | instance_id, entity_id | Track graph databases and their metadata |
| volume-registry | `volume_id` | instance_id, database_id, tier | Track EBS volumes and attachments |

**Exports**:
- `{StackName}-InstanceRegistryTable`, `-InstanceRegistryTableArn`
- `{StackName}-GraphRegistryTable`, `-GraphRegistryTableArn`
- `{StackName}-VolumeRegistryTable`, `-VolumeRegistryTableArn`
- `{StackName}-SecretArn` - Graph API authentication secret
- `{StackName}-AlertTopicArn`, `-GraphContainerRefreshFunctionName`, `-unified-log-group`

---

#### `graph-volumes.yaml`
**Purpose**: Lambda-based EBS volume lifecycle management - creation, attachment, monitoring, detachment, and snapshot cleanup.

**Dependencies**: VPC, graph-infra (the volume registry table name is passed in as a parameter)

**Key Resources**:
- `AWS::Lambda::Function` × 3 - volume manager, volume monitor, volume detachment
- `AWS::Events::Rule` × 5 - scheduled monitoring, daily cleanup, instance termination, volume detachment, snapshot cleanup
- `AWS::SNS::Topic` × 2 - alerts and detachment notifications
- `AWS::IAM::Role` × 3, `AWS::EC2::SecurityGroup` × 2
- 11 CloudWatch alarms including tiered disk-usage warning/critical/emergency, which invoke the monitor Lambda directly

**Exports** (consumed by `graph-ladybug.yaml`, passed through by the deploy workflow):
- `{StackName}-volume-manager-arn`
- `{StackName}-detachment-topic`
- `{StackName}-alert-topic`, `-volume-registry`, `-role-arn`

---

#### `graph-ladybug.yaml`
**Purpose**: LadybugDB writer instances - one EC2 Auto Scaling Group per tier, with a Launch Template. `deploy-graph.yml` builds a matrix from `.github/configs/graph.yml` and deploys one stack per enabled tier.

**Dependencies**: VPC, S3, Valkey, Postgres, graph-volumes. `VolumeManagerFunctionArn` and `VolumeDetachmentTopicArn` are read from the graph-volumes stack outputs by the workflow and passed in as parameters.

**Key Resources**:
- `AWS::AutoScaling::AutoScalingGroup` + `LifecycleHook` - writer ASG
- `AWS::EC2::LaunchTemplate` - instance configuration
- `AWS::IAM::Role` / `InstanceProfile` - S3/DynamoDB/EBS access
- `AWS::CloudWatch::Alarm` × 3 - high CPU, high memory, allocation failures
- `AWS::SNS::Topic` - per-tier alerts

> `DesiredCapacity` is intentionally omitted from the ASG; it is managed by the application at runtime.

**Instance Tiers**: `WriterTier` selects the tier and `InstanceType` / `DatabasesPerInstance` are passed in from **`.github/configs/graph.yml`**, which is the authoritative source for instance type, RAM, vCPUs, subgraph limits, memory budgets, and per-tier copy/backup/graph limits — and which differs between prod and staging. Read its per-tier `instance:` block rather than relying on a table here. The tiers are `ladybug-standard`, `ladybug-large`, `ladybug-xlarge`, and `ladybug-shared` (platform-managed public repositories such as SEC — deployed by the same template with `SharedRepositories` set).

**Exports**:
- `{StackName}-asg-name` - Auto Scaling Group name
- `{StackName}-sg-id` - writer security group ID

---

#### `graph-ladybug-replicas.yaml`
**Purpose**: Read-only LadybugDB replica fleet with Application Load Balancer for horizontal read scaling of the shared repositories. Serves `SharedRepositories` only — the shared master keeps everything else.

**Dependencies**: VPC, S3, Valkey, Postgres, and the shared writer stack (`depends_on: ladybug-shared` in `graph.yml`)

**Key Resources**:
- `AWS::AutoScaling::AutoScalingGroup` + 3 `ScalingPolicy` (CPU, memory, optional response time)
- `AWS::EC2::LaunchTemplate` - replica configuration, optional Spot via `SpotEnabled`
- `AWS::ElasticLoadBalancingV2::LoadBalancer` / `TargetGroup` / `Listener` - internal ALB
- `AWS::Logs::MetricFilter` + 7 alarms - insufficient/unhealthy hosts, high latency, 5xx spike, container start, query-abuse signal, query-engine disruption

Instance sizing and scaling for replicas come from `graph.yml`'s `shared-replicas` entry and the `SHARED_REPLICAS_*` GitHub variables; the replica memory block deliberately overrides the writer's settings because replica hardware differs.

**Exports**:
- `{StackName}-ALBDNSName`, `-ALBEndpoint` - read query entry point
- `{StackName}-ASGName`, `-LaunchTemplateName`
- `{StackName}-SecurityGroupId`, `-TargetGroupArn`

---

### Observability

#### `prometheus.yaml`
**Purpose**: Amazon Managed Prometheus workspace for metrics collection.

**Dependencies**: None (standalone)

**Key Resources**:
- `AWS::APS::Workspace` - Prometheus workspace

**Exports**:
- `{StackName}-PrometheusWorkspaceArn`, `-PrometheusWorkspaceId`
- `{StackName}-PrometheusWorkspaceEndpoint` - query endpoint
- `{StackName}-PrometheusRemoteWriteEndpoint` - remote write endpoint for ADOT
- `{StackName}-PrometheusEnvironment`

---

#### `grafana.yaml`
**Purpose**: Amazon Managed Grafana workspace for dashboards and visualization.

**Dependencies**: None (uses Prometheus/CloudWatch as data sources)

**Key Resources**:
- `AWS::Grafana::Workspace` - Grafana workspace (shared across environments)
- `AWS::IAM::Role` - service role with Prometheus, CloudWatch, Athena access

**Manual Setup Required**:
1. Configure AWS SSO user for Grafana access
2. Associate license (Enterprise Free Trial)
3. Configure data sources (Prometheus, CloudWatch)

**Exports**:
- `{StackName}-shared-GrafanaWorkspaceId`, `-shared-GrafanaWorkspaceEndpoint`
- `{StackName}-shared-GrafanaServiceRoleArn`
- `{StackName}-shared-ManualSetupRequired`, `-shared-DataSourceConfiguration`

---

### Security & Compliance

#### `waf.yaml`
**Purpose**: Web Application Firewall for ALB protection with rate limiting and attack prevention.

**Dependencies**: API stack (needs `ApiAlbArn` to associate)

**Key Resources**:
- `AWS::WAFv2::WebACL` - Web ACL with rules
- `AWS::WAFv2::IPSet` - allowlisted IPs
- `AWS::WAFv2::WebACLAssociation` - ALB association

Those three are the whole stack — per-rule CloudWatch metrics are emitted by the Web ACL's `VisibilityConfig`, but the template creates no dashboard and no alarms.

**WAF Rules** (in priority order):
1. **AllowWhitelistedIPs** - bypass all rules for trusted IPs
2. **RateLimitPerIP** - block IPs exceeding the request limit (429 response)
3. **BlockCommonAttacks** - SQL injection, XSS detection (403 response)
4. **SizeRestrictions** - block oversized payloads (413 response)
5. **GeoBlocking** - optional: block non-US/CA traffic (403 response)
6. **AWSManagedRulesCommonRuleSet** - optional: AWS managed rules

**Exports**:
- `{StackName}-WebACLId`, `-WebACLArn`
- `{StackName}-AllowedIPSetArn`

---

#### `audit.yaml`
**Purpose**: Forwards only the compliance-relevant audit records out of the API log group (`/robosystems/{env}/api`) to a dedicated long-retention S3 bucket, so security evidence survives the short operational-log retention. Entirely log-side — no application code, nothing on the request path.

**Dependencies**: API stack (subscribes to its log group)

**Key Resources**:
- `AWS::Logs::SubscriptionFilter` → `AWS::KinesisFirehose::DeliveryStream` → `AWS::S3::Bucket`
- `AWS::IAM::Role` × 2 - Firehose delivery and subscription-filter roles
- `AWS::Logs::LogGroup` / `LogStream` - Firehose error logging

**Exports**:
- `{StackName}-AuditBucket`
- `{StackName}-AuditFirehoseArn`

---

#### `security.yaml`
**Purpose**: Account- and region-global detective controls for SOC 2 (CC4/CC6/CC7). Every resource is a per-account singleton, so this is **one shared stack** (`RoboSystemsSecurity`), never per-environment.

**Dependencies**: None

**Key Resources**:
- `AWS::GuardDuty::Detector` - threat detection
- `AWS::SecurityHub::Hub` + `Standard` × 2 - FSBP always, CIS behind `EnableCISStandard`
- `AWS::AccessAnalyzer::Analyzer` - external-access findings
- `AWS::Config::ConfigurationRecorder` + `DeliveryChannel` + bucket - behind `EnableConfig`, recording frequency parameterized
- `AWS::Events::Rule` × 4 - GuardDuty findings, root-account activity, Access Analyzer findings, detective-control tampering

Each control has its own `Enable*` toggle so they can be turned on independently.

**Exports**:
- `{StackName}-GuardDutyDetectorId`
- `{StackName}-ConfigBucket`

---

#### `cloudtrail.yaml`
**Purpose**: CloudTrail audit logging for SOC 2 compliance. Like `security.yaml`, an account-global singleton deployed as one shared stack (`RoboSystemsCloudTrail`).

**Dependencies**: None (creates its own S3 bucket)

**Key Resources**:
- `AWS::S3::Bucket` - CloudTrail log bucket (with Intelligent-Tiering)
- `AWS::CloudTrail::Trail` - multi-region trail; S3 data events are opt-in via `DataEventsEnabled` because they are costly

**Exports**:
- `{StackName}-CloudTrailArn`
- `{StackName}-CloudTrailBucket`
- `{StackName}-CloudTrailEnabled`

---

## Cross-Stack References

Templates share values two ways, and it is worth keeping them straight.

### Export/Import (rare)
Direct `Fn::ImportValue` between templates is used in exactly one place — `api.yaml` reaching into the Prometheus stack, whose name it takes as a parameter:

```yaml
# prometheus.yaml exports
Outputs:
  PrometheusWorkspaceArn:
    Value: !GetAtt PrometheusWorkspace.Arn
    Export:
      Name: !Sub "${AWS::StackName}-PrometheusWorkspaceArn"

# api.yaml imports
Resource: !ImportValue
  Fn::Sub: "${PrometheusStackName}-PrometheusWorkspaceArn"
```

Every other stack still publishes exports — they are the documented contract and are readable with `aws cloudformation list-exports` — but consumers reach them through the workflow rather than importing them, which is what keeps stacks independently deletable.

### Workflow-mediated parameters (the dominant pattern here)
Most cross-stack wiring is done by the **deploy workflow**, not by CloudFormation: the workflow reads an upstream stack's outputs with `describe-stacks` and passes them to the downstream stack as ordinary parameters. This is how `api.yaml` gets `DatabaseEndpoint` / `ValkeyUrl`, how `worker.yaml` gets the Dagster cluster ARN and security group, and how `graph-ladybug.yaml` gets `VolumeManagerFunctionArn` and `VolumeDetachmentTopicArn`:

```yaml
# deploy-dagster.yml
CLUSTER_ARN=$(aws cloudformation describe-stacks \
  --stack-name ${{ inputs.stack_name }} \
  --query "Stacks[0].Outputs[?OutputKey=='ClusterArn'].OutputValue" \
  --output text)
```

Because this reads by **OutputKey**, not by export name, note that the two often differ — `dagster.yaml`'s `DagsterSecurityGroupId` output is exported as `{StackName}-SecurityGroupId`.

### SSM Parameter Store
No template creates or reads an SSM parameter. SSM Parameter Store is used **at runtime by the application**, not at deploy time by CloudFormation: feature flags and tuning values live at `/robosystems/{env}/{features,tuning}/{NAME}` and are read by the running services (see `just ssm-list`, `just ssm-get`). The templates' only involvement is granting IAM read access to those paths (`api.yaml`, `bastion.yaml`).

---

## Usage

### Deploying via GitHub Actions

All 15 `deploy-*.yml` workflows are `workflow_call`-only — they cannot be dispatched directly. Deployment is driven through `prod.yml` / `staging.yml`, which are `workflow_dispatch` (and called by `create-release.yml`):

```bash
# Deploy everything to production
just deploy prod

# Deploy everything to staging
just deploy staging

# Same thing, by hand
gh workflow run prod.yml --ref <branch-or-tag>
```

There is no per-stack entry point. To redeploy a single stack, run the environment workflow — jobs whose inputs are unchanged converge to no-ops — or, for a container-only refresh, use `service-refresh.yml` / `graph-asg-refresh.yml`.

### Local Validation

```bash
# Lint + validate a single template (pass the name, no path or extension)
just cf-lint api

# Lint all templates
just cf-lint-all
```

`just cf-lint <name>` runs `cfn-lint` and then `aws cloudformation validate-template` against `cloudformation/<name>.yaml`. `validate-template` accepts a template body of at most 51,200 bytes, so for any template over that size the validate step is **skipped** with a notice rather than failing — currently that is `api.yaml`, which deploys from S3 via `--template-url` for the same reason. `cfn-lint` has no size limit and does the static analysis that matters.

### Viewing Stack Status

Stack names are defined in `.github/configs/stacks.yml` (mirroring the workflows, which are the source of truth). They follow `RoboSystems{Component}{Variant}{Environment}`, with the shared stacks — `RoboSystemsVPC`, `RoboSystemsGrafana`, `RoboSystemsCloudTrail`, `RoboSystemsSecurity` — carrying no environment suffix.

```bash
# List all stacks
aws cloudformation list-stacks --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE

# Describe a specific stack
aws cloudformation describe-stacks --stack-name RoboSystemsAPIProd

# View stack events (useful for debugging)
aws cloudformation describe-stack-events --stack-name RoboSystemsAPIProd

# View stack exports
aws cloudformation list-exports
```

### Troubleshooting

Common issues:

| Issue | Solution |
|-------|----------|
| Export not found | Ensure the dependency stack is deployed first, and check the export name with `aws cloudformation list-exports` — several carry an `{Env}` segment |
| IAM permission denied | Check the OIDC deploy role has the required permissions (`bootstrap-oidc.yaml`) |
| Resource limit exceeded | Request a limit increase or delete unused resources |
| Circular dependency | Pass the value through the deploy workflow as a parameter instead of importing it |
| `validate-template` size error | Expected for templates over 51,200 bytes; `just cf-lint` skips validate for those |

---

## Fork Considerations

When forking RoboSystems to a different AWS account:

1. **Bootstrap First**: Deploy `bootstrap-oidc.yaml` with your GitHub organization (`GitHubOrg` parameter) via `just bootstrap`
2. **S3 Bucket Names**: Namespaced by the `Namespace` parameter, which the workflows set to `AWS_ACCOUNT_ID`, so they stay globally unique
3. **Configuration**: Run `just setup-gha` to populate the GitHub variables the deploy workflows read — the templates' own defaults are not the deployed sizing
4. **Secrets**: Create new secrets in Secrets Manager (`just setup-aws`)
5. **Domain Names**: Optional - works without custom domains via `ApiAccessMode=internal` and the SSM tunnel

See the [Bootstrap Guide](https://github.com/RoboFinSystems/robosystems/wiki/Bootstrap-Guide) for complete fork deployment instructions.

---

## Related Documentation

- [Bootstrap Guide](https://github.com/RoboFinSystems/robosystems/wiki/Bootstrap-Guide) - Complete deployment walkthrough
- [Architecture Overview](https://github.com/RoboFinSystems/robosystems/wiki/Architecture-Overview) - System architecture
- [Setup Scripts](/bin/setup/README.md) - Bootstrap and configuration scripts
- [Graph Config](/.github/configs/graph.yml) - Graph database tier configurations (authoritative for instance sizing)
- [Stack Names](/.github/configs/stacks.yml) - CloudFormation stack names per environment
