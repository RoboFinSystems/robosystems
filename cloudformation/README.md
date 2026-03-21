# CloudFormation Templates

Infrastructure as Code for deploying RoboSystems to AWS. All templates are deployed via GitHub Actions workflows, except `bootstrap-oidc.yaml` which is deployed locally to enable CI/CD authentication.

## Quick Reference

| Template | Purpose | Dependencies | Est. Monthly Cost |
|----------|---------|--------------|-------------------|
| `bootstrap-oidc.yaml` | GitHub OIDC for CI/CD | None | Free |
| `vpc.yaml` | Network foundation | None | ~$35 (NAT Gateway) |
| `postgres.yaml` | RDS PostgreSQL | VPC | ~$15 (db.t4g.micro) |
| `valkey.yaml` | ElastiCache cache | VPC | ~$13 (cache.t4g.micro) |
| `s3.yaml` | Object storage | None | Variable |
| `api.yaml` | ECS API service | VPC, others | ~$20-50 (Fargate Spot) |
| `dagster.yaml` | Workflow orchestration | VPC, others | ~$15-30 (Fargate) |
| `bastion.yaml` | Secure SSH access | VPC | Free (SSM only) |
| `graph-infra.yaml` | Graph DB registries | None | ~$3 (DynamoDB) |
| `graph-volumes.yaml` | EBS management | VPC, graph-infra | ~$1 (Lambda) |
| `graph-ladybug.yaml` | LadybugDB writers | VPC, S3, Valkey, graph-infra | ~$50+ (EC2) |
| `graph-ladybug-replicas.yaml` | LadybugDB readers | VPC, S3 | ~$30+ (EC2) |
| `graph-neo4j.yaml` | Neo4j (alternative) | VPC, graph-infra | ~$50+ (EC2) |
| `opensearch.yaml` | Document search | VPC | ~$30 (t3.medium.search) |
| `prometheus.yaml` | Metrics collection | None | ~$0.03/metric-series |
| `grafana.yaml` | Dashboards | None | ~$9/user/month |
| `cloudtrail.yaml` | Audit logging | None | ~$2+ |
| `waf.yaml` | Web firewall | API (for ALB ARN) | ~$6-7 |

## Deployment Order

Templates have cross-stack dependencies that determine deployment order:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PHASE 1: BOOTSTRAP                                │
├─────────────────────────────────────────────────────────────────────────────┤
│  bootstrap-oidc.yaml                                                        │
│  └── Enables GitHub Actions → AWS authentication (deploy locally first)    │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        PHASE 2: FOUNDATION                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  vpc.yaml ◄───────── Core networking (VPC, subnets, NAT, IGW)               │
│  s3.yaml  ◄───────── S3 buckets (deployment, shared-processed, user-data)  │
│  graph-infra.yaml ◄─ DynamoDB registries (instance, graph, volume)         │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        PHASE 3: DATA LAYER                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  postgres.yaml ◄──── RDS PostgreSQL (depends on VPC)                       │
│  valkey.yaml ◄────── ElastiCache Valkey (depends on VPC)                   │
│  graph-volumes.yaml ◄ EBS volume management Lambda (depends on VPC,        │
│                       graph-infra)                                          │
│  opensearch.yaml ◄─── OpenSearch managed domain for document search        │
│                       (depends on VPC)                                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      PHASE 4: APPLICATION SERVICES                          │
├─────────────────────────────────────────────────────────────────────────────┤
│  api.yaml ◄─────────── ECS Fargate API (depends on VPC, uses SSM params)   │
│  dagster.yaml ◄──────── Dagster orchestration (depends on VPC)             │
│  bastion.yaml ◄──────── SSM bastion host (depends on VPC)                  │
│  graph-ladybug.yaml ◄── LadybugDB writers (depends on VPC, S3, Valkey,     │
│                         graph-infra, uses SSM params from graph-volumes)   │
│  graph-neo4j.yaml ◄──── Neo4j Community (depends on VPC, graph-infra,      │
│                         graph-volumes) - Alternative to LadybugDB          │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      PHASE 5: SCALING & SECURITY                            │
├─────────────────────────────────────────────────────────────────────────────┤
│  graph-ladybug-replicas.yaml ◄── Read replica fleet with ALB               │
│  waf.yaml ◄───────────────────── WAF for ALB protection (needs API ALB)    │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      PHASE 6: OBSERVABILITY (Optional)                      │
├─────────────────────────────────────────────────────────────────────────────┤
│  prometheus.yaml ◄─── Amazon Managed Prometheus (standalone)               │
│  grafana.yaml ◄────── Amazon Managed Grafana (uses Prometheus, CloudWatch) │
│  cloudtrail.yaml ◄─── Audit logging (standalone, creates own S3 bucket)    │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Template Details

### Bootstrap

#### `bootstrap-oidc.yaml`
**Purpose**: Enables passwordless GitHub Actions → AWS authentication using OIDC federation.

**Deploy**: Locally with `just bootstrap` before any CI/CD workflows can run.

**Key Resources**:
- `AWS::IAM::OIDCProvider` - GitHub OIDC identity provider
- `AWS::IAM::Role` - Assumable role for GitHub Actions

**Parameters**:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `GitHubOrganization` | `RoboFinSystems` | GitHub org allowed to assume role |
| `RepositoryFilter` | `*` | Which repos can assume the role |

**Exports**:
- `{StackName}-OidcRoleArn` - Role ARN for GitHub Actions to assume

---

### Core Infrastructure

#### `vpc.yaml`
**Purpose**: Foundation networking - VPC with public/private subnets across up to 5 AZs, NAT gateways, Internet gateway, and optional VPC endpoints.

**Key Resources**:
- VPC with configurable CIDR `10.X.0.0/16` (where X = `VpcSecondOctet`)
- Up to 5 Public subnets (`10.X.10.0/24` through `10.X.14.0/24`)
- Up to 5 Private subnets (`10.X.1.0/24` through `10.X.5.0/24`)
- NAT Gateway (single, shared across all AZs)
- Internet Gateway
- Route tables and security groups
- VPC endpoints (configurable: S3, DynamoDB always free; ECR, Secrets Manager in minimal/full modes)
- Optional VPC Flow Logs for SOC 2 compliance

**Parameters**:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `VpcSecondOctet` | `0` | Second octet for VPC CIDR (`10.X.0.0/16`). Change to avoid overlap with other VPCs for peering (0-255) |
| `MaxAvailabilityZones` | `5` | Maximum AZs to use (2-6, uses fewer if region has limited AZs) |
| `DeployVpcEndpoints` | `minimal` | VPC endpoint mode: `gateway` (free S3+DynamoDB), `minimal` (~$22/mo, +ECR in 1 AZ), `full` (~$45/mo, +ECR in 2 AZs) |
| `EnableVPCFlowLogs` | `false` | Enable VPC Flow Logs for network traffic monitoring |
| `FlowLogsRetentionDays` | `30` | Days to retain VPC Flow Logs (30 minimum for SOC 2) |
| `FlowLogsTrafficType` | `REJECT` | Traffic type to capture: `ALL`, `ACCEPT`, or `REJECT` |

**VPC Peering Note**: Forks can set `VpcSecondOctet` to a non-zero value (e.g., `2` for `10.2.0.0/16`) to enable VPC peering with another `10.0.0.0/16` VPC. This is configured via the `VPC_SECOND_OCTET` GitHub variable.

**Exports** (critical for other stacks):
- `{StackName}-{Env}-VpcId` - VPC ID
- `{StackName}-{Env}-VpcCidr` - VPC CIDR block (for peering/security groups)
- `{StackName}-{Env}-PublicSubnetIds` - Comma-separated public subnet IDs
- `{StackName}-{Env}-PrivateSubnetIds` - Comma-separated private subnet IDs
- `{StackName}-{Env}-NatGatewayId` - NAT Gateway ID (first AZ)

---

#### `postgres.yaml`
**Purpose**: RDS PostgreSQL database for IAM, billing, subscriptions, and metadata.

**Dependencies**: VPC stack

**Key Resources**:
- `AWS::RDS::DBInstance` - PostgreSQL database
- `AWS::RDS::DBSubnetGroup` - Multi-AZ subnet group
- `AWS::EC2::SecurityGroup` - Database access security group

**Parameters**:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `Environment` | `prod` | Environment name |
| `DatabaseInstanceClass` | `db.t4g.micro` | RDS instance type |
| `AllocatedStorage` | `20` | Storage in GB |
| `MultiAZ` | `false` | Enable Multi-AZ |
| `MasterUsername` | `postgres` | Master username |
| `VpcStackName` | Required | VPC stack name |

**Exports**:
- `{StackName}-DatabaseEndpoint` - RDS endpoint
- `{StackName}-DatabasePort` - RDS port (5432)
- `{StackName}-DatabaseSecurityGroup` - Security group ID

---

#### `valkey.yaml`
**Purpose**: ElastiCache Valkey (Redis-compatible) for caching, rate limiting, SSE messaging, and distributed locks.

**Dependencies**: VPC stack

**Key Resources**:
- `AWS::ElastiCache::ServerlessCache` - Valkey serverless cluster
- `AWS::EC2::SecurityGroup` - Cache access security group

**Parameters**:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `Environment` | `prod` | Environment name |
| `MaximumDataStorage` | `1` | Max storage in GB |
| `MaximumECPU` | `1000` | Max ECPUs per second |
| `VpcStackName` | Required | VPC stack name |

**Exports**:
- `{StackName}-ValkeyEndpoint` - Valkey endpoint
- `{StackName}-ValkeyPort` - Valkey port (6379)
- `{StackName}-ValkeySecurityGroup` - Security group ID

---

#### `opensearch.yaml`
**Purpose**: Amazon OpenSearch Service managed domain for full-text document search (BM25) across SEC filing narratives, iXBRL disclosures, and text blocks.

**Dependencies**: VPC stack

**Key Resources**:
- `AWS::OpenSearchService::Domain` - Managed OpenSearch domain (single-node, single-AZ)
- `AWS::EC2::SecurityGroup` - Domain access security group + client security group
- `AWS::CloudWatch::Alarm` - Cluster red status and free storage alarms
- `AWS::SNS::Topic` - Alert notifications
- `AWS::Logs::LogGroup` - Application and slow query logs

**Parameters**:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `Environment` | `prod` | Environment name |
| `InstanceType` | `t3.medium.search` | OpenSearch instance type |
| `EBSVolumeSize` | `100` | EBS volume size in GB |
| `EngineVersion` | `OpenSearch_2.17` | OpenSearch engine version |
| `VpcId` | Required | VPC ID |
| `SubnetId` | Required | Private subnet ID (single-AZ) |
| `SNSAlertEmail` | Required | Email for alert notifications |

**Exports**:
- `{StackName}-OpenSearchEndpoint` - Domain endpoint URL
- `{StackName}-OpenSearchDomainArn` - Domain ARN

**Feature-flag gated**: Deployed via `deploy-opensearch.yml` workflow, controlled by `OPENSEARCH_ENABLED_{ENV}` GitHub variable. Application access gated by `TEXT_SEARCH_ENABLED` SSM parameter.

---

#### `s3.yaml`
**Purpose**: S3 buckets for deployment artifacts, processed data, and user data.

**Dependencies**: None (standalone)

**Key Resources**:
- `robosystems-{account}-deployment-{env}` - Deployment artifacts (userdata scripts, Lambda packages)
- `robosystems-{account}-shared-processed-{env}` - SEC/shared data processing
- `robosystems-{account}-user-{env}` - User-uploaded files

**Parameters**:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `Environment` | `prod` | Environment name |
| `EnableVersioning` | `true` | Enable S3 versioning |

**Exports**:
- `{StackName}-DeploymentBucketName`, `-DeploymentBucketArn`
- `{StackName}-SharedProcessedBucketName`, `-SharedProcessedBucketArn`
- `{StackName}-UserDataBucketName`, `-UserDataBucketArn`

---

### Application Services

#### `api.yaml`
**Purpose**: ECS Fargate API service with Application Load Balancer.

**Dependencies**: VPC (required), Postgres/Valkey (via SSM parameters)

**Key Resources**:
- `AWS::ECS::Cluster` - ECS cluster
- `AWS::ECS::Service` - Fargate service
- `AWS::ECS::TaskDefinition` - Container definitions
- `AWS::ElasticLoadBalancingV2::LoadBalancer` - Application Load Balancer
- `AWS::ElasticLoadBalancingV2::TargetGroup` - Target group
- `AWS::Logs::LogGroup` - CloudWatch logs

**Parameters**:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `Environment` | `prod` | Environment name |
| `ImageTag` | `latest` | Container image tag |
| `DesiredCount` | `1` | Number of tasks |
| `CPU` | `256` | Task CPU units |
| `Memory` | `512` | Task memory (MB) |
| `VpcStackName` | Required | VPC stack name |

**Exports**:
- `{StackName}-ClusterArn` - ECS cluster ARN
- `{StackName}-ServiceArn` - ECS service ARN
- `{StackName}-AlbArn` - ALB ARN (used by WAF)
- `{StackName}-AlbDnsName` - ALB DNS name

---

#### `dagster.yaml`
**Purpose**: Dagster orchestration platform (webserver + daemon) on ECS Fargate.

**Dependencies**: VPC (required)

**Key Resources**:
- `AWS::ECS::Cluster` - Dagster ECS cluster
- `AWS::ECS::Service` - Webserver and daemon services
- `AWS::ECS::TaskDefinition` - Container definitions
- `AWS::ElasticLoadBalancingV2::LoadBalancer` - Internal ALB
- `AWS::Logs::LogGroup` - CloudWatch logs

**Parameters**:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `Environment` | `prod` | Environment name |
| `WebserverCpu` | `256` | Webserver CPU units |
| `WebserverMemory` | `512` | Webserver memory (MB) |
| `DaemonCpu` | `512` | Daemon CPU units |
| `DaemonMemory` | `1024` | Daemon memory (MB) |

**Exports**:
- `{StackName}-ClusterArn` - ECS cluster ARN
- `{StackName}-WebserverServiceArn` - Webserver service ARN
- `{StackName}-DaemonServiceArn` - Daemon service ARN

---

#### `bastion.yaml`
**Purpose**: EC2 bastion host for secure SSM-based access to private resources. No SSH keys or open ports required.

**Dependencies**: VPC (required)

**Key Resources**:
- `AWS::EC2::Instance` - t4g.nano bastion instance
- `AWS::IAM::Role` - SSM-enabled instance role
- `AWS::IAM::InstanceProfile` - Instance profile

**Parameters**:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `Environment` | `prod` | Environment name |
| `InstanceType` | `t4g.nano` | EC2 instance type |
| `VpcStackName` | Required | VPC stack name |

**Exports**:
- `{StackName}-BastionInstanceId` - Instance ID for SSM session

**Usage**:
```bash
# Start SSM session
aws ssm start-session --target i-0123456789abcdef0

# Port forward to RDS
aws ssm start-session --target i-0123456789abcdef0 \
  --document-name AWS-StartPortForwardingSessionToRemoteHost \
  --parameters '{"host":["mydb.xxx.rds.amazonaws.com"],"portNumber":["5432"],"localPortNumber":["5432"]}'
```

---

### Graph Database Infrastructure

The graph database system uses a modular architecture with separate templates for infrastructure, compute, and storage.

#### `graph-infra.yaml`
**Purpose**: Core graph database infrastructure - DynamoDB registries for tracking instances, graphs, and volumes.

**Dependencies**: None (standalone)

**Key Resources**:
- `AWS::DynamoDB::Table` - `robosystems-instance-registry-{env}` (tracks EC2 instances)
- `AWS::DynamoDB::Table` - `robosystems-graph-registry-{env}` (tracks graph databases)
- `AWS::DynamoDB::Table` - `robosystems-volume-registry-{env}` (tracks EBS volumes)
- `AWS::SecretsManager::Secret` - Graph database authentication

**Parameters**:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `Environment` | `prod` | Environment name |
| `GraphSecretArn` | `` | Optional existing secret ARN |

**Exports**:
- `{StackName}-InstanceRegistryTableName`, `-InstanceRegistryTableArn`
- `{StackName}-GraphRegistryTableName`, `-GraphRegistryTableArn`
- `{StackName}-VolumeRegistryTableName`, `-VolumeRegistryTableArn`
- `{StackName}-GraphSecretArn` - Authentication secret ARN

**DynamoDB Table Schemas**:
| Table | Partition Key | Sort Key | Purpose |
|-------|--------------|----------|---------|
| instance-registry | `instance_id` | - | Track EC2 writer/reader instances |
| graph-registry | `graph_id` | - | Track graph databases and their metadata |
| volume-registry | `volume_id` | - | Track EBS volumes and attachments |

---

#### `graph-volumes.yaml`
**Purpose**: Lambda-based EBS volume lifecycle management - creation, attachment, and cleanup.

**Dependencies**: VPC, graph-infra

**Key Resources**:
- `AWS::Lambda::Function` - Volume management Lambda
- `AWS::IAM::Role` - Lambda execution role with EC2/EBS permissions
- `AWS::Lambda::Permission` - API Gateway integration
- `AWS::SSM::Parameter` - Stores Lambda ARN for other stacks

**Parameters**:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `Environment` | `prod` | Environment name |
| `VpcStackName` | Required | VPC stack name |
| `GraphInfraStackName` | Required | Graph infra stack name |
| `VolumeType` | `gp3` | EBS volume type |
| `VolumeSizeGB` | `100` | Default volume size |
| `VolumeIops` | `3000` | IOPS for gp3 volumes |
| `VolumeThroughput` | `125` | Throughput (MB/s) for gp3 |

**SSM Parameters Created**:
- `/robosystems/{env}/graph/volume-manager-lambda-arn` - Lambda ARN for invocation

---

#### `graph-ladybug.yaml`
**Purpose**: LadybugDB writer instances - EC2 Auto Scaling Group with Launch Template.

**Dependencies**: VPC, S3, Valkey, graph-infra (reads SSM parameters from graph-volumes)

**Key Resources**:
- `AWS::AutoScaling::AutoScalingGroup` - Writer instance ASG
- `AWS::EC2::LaunchTemplate` - Instance configuration
- `AWS::IAM::Role` - Instance role with S3/DynamoDB/EBS access
- `AWS::CloudWatch::Alarm` - CPU and memory alarms

**Parameters**:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `Environment` | `prod` | Environment name |
| `InstanceType` | `r7g.medium` | EC2 instance type |
| `MinCapacity` | `0` | Minimum instances |
| `MaxCapacity` | `2` | Maximum instances |
| `DesiredCapacity` | `1` | Desired instances |
| `VpcStackName` | Required | VPC stack name |
| `S3StackName` | Required | S3 stack name |
| `ValkeyStackName` | Required | Valkey stack name |
| `GraphInfraStackName` | Required | Graph infra stack name |

**Instance Tiers** (configured in `.github/configs/graph.yml`):
| Tier | Instance Type | Memory | Use Case |
|------|---------------|--------|----------|
| ladybug-standard | r7g.medium | 16 GB | Shared/free tier |
| ladybug-large | r7g.large | 32 GB | Standard subscriptions |
| ladybug-xlarge | r7g.xlarge | 64 GB | Large subscriptions |

**Exports**:
- `{StackName}-AutoScalingGroupArn` - ASG ARN
- `{StackName}-LaunchTemplateId` - Launch template ID
- `{StackName}-InstanceSecurityGroup` - Security group ID

---

#### `graph-ladybug-replicas.yaml`
**Purpose**: Read-only LadybugDB replica fleet with Application Load Balancer for horizontal read scaling.

**Dependencies**: VPC, S3 (reads SSM parameters)

**Key Resources**:
- `AWS::AutoScaling::AutoScalingGroup` - Reader instance ASG
- `AWS::EC2::LaunchTemplate` - Replica configuration
- `AWS::ElasticLoadBalancingV2::LoadBalancer` - Internal ALB
- `AWS::ElasticLoadBalancingV2::TargetGroup` - Health-checked target group
- `AWS::CloudWatch::Alarm` - Scaling and health alarms

**Parameters**:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `Environment` | `prod` | Environment name |
| `InstanceType` | `r7g.medium` | EC2 instance type |
| `MinCapacity` | `0` | Minimum replicas |
| `MaxCapacity` | `3` | Maximum replicas |
| `DesiredCapacity` | `0` | Desired replicas |
| `VpcStackName` | Required | VPC stack name |

**Exports**:
- `{StackName}-ReplicaAlbDnsName` - ALB DNS for read queries
- `{StackName}-ReplicaAlbArn` - ALB ARN
- `{StackName}-AutoScalingGroupArn` - Reader ASG ARN

---

#### `graph-neo4j.yaml`
**Purpose**: Neo4j Community Edition as alternative graph backend to LadybugDB.

**Dependencies**: VPC, graph-infra, graph-volumes

**Key Resources**:
- `AWS::AutoScaling::AutoScalingGroup` - Neo4j instance ASG
- `AWS::EC2::LaunchTemplate` - Neo4j configuration
- `AWS::IAM::Role` - Instance role

**Parameters**:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `Environment` | `prod` | Environment name |
| `InstanceType` | `r7g.medium` | EC2 instance type |
| `Neo4jVersion` | `5.15.0` | Neo4j version |
| `VpcStackName` | Required | VPC stack name |

**Exports**:
- `{StackName}-AutoScalingGroupArn` - ASG ARN
- `{StackName}-InstanceSecurityGroup` - Security group ID

---

### Observability

#### `prometheus.yaml`
**Purpose**: Amazon Managed Prometheus workspace for metrics collection.

**Dependencies**: None (standalone)

**Key Resources**:
- `AWS::APS::Workspace` - Prometheus workspace

**Parameters**:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `Environment` | `prod` | Environment name |
| `ProjectName` | `robosystems` | Project name |

**Exports**:
- `{StackName}-PrometheusWorkspaceArn` - Workspace ARN
- `{StackName}-PrometheusWorkspaceId` - Workspace ID
- `{StackName}-PrometheusWorkspaceEndpoint` - Query endpoint
- `{StackName}-PrometheusRemoteWriteEndpoint` - Remote write endpoint for ADOT

---

#### `grafana.yaml`
**Purpose**: Amazon Managed Grafana workspace for dashboards and visualization.

**Dependencies**: None (uses Prometheus/CloudWatch as data sources)

**Key Resources**:
- `AWS::Grafana::Workspace` - Grafana workspace (shared across environments)
- `AWS::IAM::Role` - Service role with Prometheus, CloudWatch, Athena access

**Parameters**:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `ProjectName` | `robosystems` | Project name |

**Manual Setup Required**:
1. Configure AWS SSO user for Grafana access
2. Associate license (Enterprise Free Trial)
3. Configure data sources (Prometheus, CloudWatch)

**Exports**:
- `{StackName}-shared-GrafanaWorkspaceId` - Workspace ID
- `{StackName}-shared-GrafanaWorkspaceEndpoint` - Grafana URL
- `{StackName}-shared-GrafanaServiceRoleArn` - Service role ARN

---

### Security & Compliance

#### `waf.yaml`
**Purpose**: Web Application Firewall for ALB protection with rate limiting and attack prevention.

**Dependencies**: API stack (needs ALB ARN to associate)

**Key Resources**:
- `AWS::WAFv2::WebACL` - Web ACL with rules
- `AWS::WAFv2::IPSet` - Allowlisted IPs
- `AWS::WAFv2::WebACLAssociation` - ALB association
- `AWS::CloudWatch::Dashboard` - WAF metrics dashboard
- `AWS::CloudWatch::Alarm` - High block rate alarm

**WAF Rules** (in priority order):
1. **AllowWhitelistedIPs** - Bypass all rules for trusted IPs
2. **RateLimitPerIP** - Block IPs exceeding request limit (429 response)
3. **BlockCommonAttacks** - SQL injection, XSS detection (403 response)
4. **SizeRestrictions** - Block oversized payloads (413 response)
5. **GeoBlocking** - Optional: Block non-US/CA traffic (403 response)
6. **AWSManagedRulesCommonRuleSet** - Optional: AWS managed rules

**Parameters**:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `Environment` | `prod` | Environment name |
| `ApiAlbArn` | `` | API ALB ARN to protect |
| `RateLimitPerIP` | `3000` | Requests per 5 min per IP |
| `EnableGeoBlocking` | `false` | Block non-US/CA traffic |
| `EnableAwsManagedRules` | `true` | Use AWS managed rules |
| `AllowedIPs` | `0.0.0.0/32` | IPs to allowlist |

**Exports**:
- `{StackName}-WebACLId` - Web ACL ID
- `{StackName}-WebACLArn` - Web ACL ARN
- `{StackName}-AllowedIPSetArn` - IP allowlist ARN

---

#### `cloudtrail.yaml`
**Purpose**: CloudTrail audit logging for SOC 2 compliance.

**Dependencies**: None (creates its own S3 bucket)

**Key Resources**:
- `AWS::S3::Bucket` - CloudTrail log bucket (with Intelligent-Tiering)
- `AWS::CloudTrail::Trail` - Multi-region trail

**Parameters**:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `Environment` | `prod` | Environment name |
| `EnableCloudTrail` | `false` | Enable CloudTrail |
| `LogRetentionDays` | `90` | Log retention period |
| `DataEventsEnabled` | `false` | Log S3 data events (costly) |
| `UserDataBucketArn` | `` | Optional: S3 bucket to monitor |

**Exports**:
- `{StackName}-CloudTrailArn` - Trail ARN
- `{StackName}-CloudTrailBucket` - Log bucket name
- `{StackName}-CloudTrailEnabled` - Whether enabled

---

## Cross-Stack References

Templates communicate via CloudFormation exports and SSM Parameters:

### Export/Import Pattern
```yaml
# Exporting stack (vpc.yaml)
Outputs:
  VpcId:
    Value: !Ref VPC
    Export:
      Name: !Sub "${AWS::StackName}-VpcId"

# Importing stack (postgres.yaml)
Resources:
  DBSubnetGroup:
    Properties:
      VpcSecurityGroups:
        - Fn::ImportValue: !Sub "${VpcStackName}-VpcId"
```

### SSM Parameter Pattern
Used for dynamic values that change (Lambda ARNs, instance IDs):
```yaml
# graph-volumes.yaml creates parameter
Resources:
  VolumeLambdaArnParameter:
    Type: AWS::SSM::Parameter
    Properties:
      Name: !Sub "/robosystems/${Environment}/graph/volume-manager-lambda-arn"
      Value: !GetAtt VolumeManagerLambda.Arn

# graph-ladybug.yaml reads parameter
Parameters:
  VolumeLambdaArn:
    Type: AWS::SSM::Parameter::Value<String>
    Default: /robosystems/prod/graph/volume-manager-lambda-arn
```

---

## Usage

### Deploying via GitHub Actions

Most templates are deployed automatically by GitHub Actions workflows:

```bash
# Deploy everything to production
just deploy prod

# Deploy everything to staging
just deploy staging

# Deploy specific stack
gh workflow run deploy-<stack>.yml -f environment=prod
```

### Local Validation

```bash
# Validate a template
just cf-validate cloudformation/api.yaml

# Lint all templates
just cf-lint

# Validate all templates
just cf-validate-all
```

### Viewing Stack Status

```bash
# List all stacks
aws cloudformation list-stacks --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE

# Describe a specific stack
aws cloudformation describe-stacks --stack-name RoboSystemsApiProd

# View stack events (useful for debugging)
aws cloudformation describe-stack-events --stack-name RoboSystemsApiProd

# View stack exports
aws cloudformation list-exports
```

### Troubleshooting

Common issues:

| Issue | Solution |
|-------|----------|
| Export not found | Ensure dependency stack is deployed first |
| IAM permission denied | Check role has required permissions |
| Resource limit exceeded | Request limit increase or delete unused resources |
| Circular dependency | Use SSM parameters instead of direct exports |

---

## Fork Considerations

When forking RoboSystems to a different AWS account:

1. **Bootstrap First**: Deploy `bootstrap-oidc.yaml` with your GitHub organization
2. **S3 Bucket Names**: Automatically namespaced with account ID to avoid collisions
3. **OIDC Trust**: Update `GitHubOrganization` parameter in bootstrap
4. **Secrets**: Create new secrets in Secrets Manager
5. **Domain Names**: Optional - works without custom domains via SSM tunnel

See the [Bootstrap Guide](https://github.com/RoboFinSystems/robosystems/wiki/Bootstrap-Guide) for complete fork deployment instructions.

---

## Related Documentation

- [Bootstrap Guide](https://github.com/RoboFinSystems/robosystems/wiki/Bootstrap-Guide) - Complete deployment walkthrough
- [Architecture Overview](https://github.com/RoboFinSystems/robosystems/wiki/Architecture-Overview) - System architecture
- [Setup Scripts](/bin/setup/README.md) - Bootstrap and configuration scripts
- [Graph Config](/.github/configs/graph.yml) - Graph database tier configurations
