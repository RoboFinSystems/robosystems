---
description: Read-only AWS reliability & resilience review; doubles as SOC 2 Availability evidence.
---

Run a read-only AWS reliability & resilience review (the Well-Architected Reliability pillar), then produce a report and a prioritized plan. Doubles as **SOC 2 Availability (A1)** evidence. Pairs with the `reliability-review` runbook (`local/RoboSystems/runbooks/`) for the account-specific detail — read it alongside this file, since it holds the live record of what's been accepted, deferred, or already remediated.

## Scope & guardrails

- **Read-only.** `describe-*` / `list-*` / `get-*` only; never `get-secret-value`. Any change (enabling Multi-AZ, standing up a backup plan, adjusting scaling) needs explicit user confirmation; CloudFormation/destructive actions are the user's to run.
- **Output is sensitive — never commit it.** A reliability report names your single points of failure, backup gaps, and unrecoverable paths — a roadmap for both attackers and adversarial due-diligence. Vault or private Artifact only.
- **Feeds SOC 2 Availability.** Tag findings that map to CC7.5 / A1 (backups, recovery, capacity) so this doubles as readiness evidence for `/soc2-review`.

## Phase 1 — Redundancy & single points of failure

```bash
aws rds describe-db-instances --region us-east-1 --query 'DBInstances[].{id:DBInstanceIdentifier,multiAZ:MultiAZ,az:AvailabilityZone}'
aws elasticache describe-replication-groups --region us-east-1 --query 'ReplicationGroups[].{id:ReplicationGroupId,multiAZ:MultiAZ,automaticFailover:AutomaticFailover}'
aws autoscaling describe-auto-scaling-groups --region us-east-1 --query 'AutoScalingGroups[].{name:AutoScalingGroupName,min:MinSize,desired:DesiredCapacity,azs:AvailabilityZones}'
```

Then the surfaces the three calls above miss — each is its own SPOF class:

```bash
# Container services: desired vs running, per cluster
aws ecs list-clusters --region us-east-1
aws ecs describe-services --cluster <c> --services <s> --region us-east-1 \
  --query 'services[].{name:serviceName,desired:desiredCount,running:runningCount}'

# Managed search: node count, zone awareness, dedicated master, standby
aws opensearch list-domain-names --region us-east-1
aws opensearch describe-domain --domain-name <d> --region us-east-1 --query 'DomainStatus.ClusterConfig'

# Registries backing graph routing
aws dynamodb list-tables --region us-east-1

# Edge: distributions fronting the customer-facing apps
aws cloudfront list-distributions --query 'DistributionList.Items[].{id:Id,enabled:Enabled,aliases:Aliases.Items}'

# Functions on critical paths (secret rotation, volume management)
aws lambda list-functions --region us-east-1 --query 'Functions[].FunctionName'
```

Flag: single-AZ RDS, single-node caches, single-instance/single-AZ services, ASGs with `min=0`/`min=1` on critical paths, search domains without zone awareness or a dedicated master, and any ECS service whose running count trails desired.

## Phase 2 — Backups & recoverability

- **RDS**: `BackupRetentionPeriod` (>0?), automated-snapshot presence, point-in-time recovery, `DeletionProtection`, backup encryption.
- **AWS Backup**: `list-backup-plans` / `list-backup-vaults` — is there a **centralized plan with vault-lock**, or only service-native snapshots? An empty result here is a finding, not a clean pass.
- **EBS**: the graph tier's data lives on EBS volumes. With no centralized backup plan, snapshots are the only recovery path — check that they exist and are recent: `aws ec2 describe-snapshots --owner-ids self --region us-east-1 --query 'reverse(sort_by(Snapshots,&StartTime))[:20].[SnapshotId,VolumeId,StartTime]' --output text`.
- **DynamoDB**: the registries that back graph routing — `aws dynamodb describe-continuous-backups --table-name <t>` for PITR status.
- **S3**: versioning on data buckets (tamper/rollback), cross-region replication where warranted.
- Cross-region / cross-account backup for the truly-critical data.

## Phase 3 — Disaster recovery

Confirm documented **RTO/RPO** targets and — the usual gap — **evidence of an actual restore/DR test**. A backup you've never restored is a hope, not a control. Check the BCDR policy vs. reality.

## Phase 4 — Health, scaling & self-healing

- ALB/target-group health checks and current target health; ECS service desired-vs-running; ASG health-check type + grace.
- Auto-scaling floors on user-facing services (can it survive an AZ loss and a traffic spike?).
- Deployment safety: rollback path, blue/green, and cleanup of failed stacks (`ROLLBACK_COMPLETE`) and retained/orphaned resources.

## Phase 5 — State & registry consistency

Stateful reconciliation is a frequent reliability gap: registries/metadata that can **drift from reality** on instance cycling (e.g. DynamoDB instance/graph/volume registries vs. live EC2 after ASG replacement) — is there an on-disk-truth reconciliation, or does drift accumulate silently? Check idempotency + fail-closed behavior on the critical write paths.

## Phase 6 — Quotas & dependency resilience

- Service Quotas headroom on the limits you'd hit under scale/failover (Fargate tasks, EIPs, RDS connections).
- Circuit breakers / retries / timeouts on external dependencies; fail-closed vs fail-open posture on auth and data paths.

## Output — report + plan

1. **Reliability report** — per-area posture (redundant / single-point / gap), with the blast radius of each SPOF and the recovery story (can we restore, and have we proven it?).
2. **Prioritized plan** — ranked by (impact of failure × likelihood) ÷ effort. Tag the items that are also **SOC 2 Availability** evidence. Separate quick wins (enable a backup plan, add a health check) from cost-bearing changes (Multi-AZ, cross-region) so the availability-vs-cost tradeoff is explicit.
