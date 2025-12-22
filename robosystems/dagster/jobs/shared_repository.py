"""Dagster jobs for shared repository management.

These jobs handle EBS snapshot and replica management:
- Create EBS snapshots of shared master's data volume
- Update replica ASG launch template with new snapshot
- Trigger rolling instance refresh of replica fleet

These jobs are typically triggered after SEC materialization completes,
or can be run manually for ad-hoc snapshot/refresh operations.
"""

from datetime import UTC, datetime
from typing import Any

import boto3
from dagster import (
  Config,
  DefaultScheduleStatus,
  OpExecutionContext,
  ScheduleDefinition,
  job,
  op,
)

from robosystems.config import env

# ============================================================================
# Configuration
# ============================================================================


class SnapshotConfig(Config):
  """Configuration for snapshot operations."""

  graph_id: str = "sec"
  wait_for_completion: bool = True
  description_prefix: str = "Shared repository"


class ReplicaConfig(Config):
  """Configuration for replica operations."""

  min_healthy_percentage: int = 50
  instance_warmup_seconds: int = 300


# ============================================================================
# Environment-based Schedule Status
# ============================================================================

# Shared repository schedules are STOPPED by default everywhere.
# Enable via SHARED_REPO_SCHEDULE_ENABLED=true after verifying jobs work manually.
SHARED_REPO_SCHEDULE_STATUS = (
  DefaultScheduleStatus.RUNNING
  if env.SHARED_REPO_SCHEDULE_ENABLED
  else DefaultScheduleStatus.STOPPED
)


# ============================================================================
# Snapshot Operations
# ============================================================================


@op
def get_shared_master_volume(
  context: OpExecutionContext, config: SnapshotConfig
) -> str:
  """Discover shared master's data volume from DynamoDB instance registry.

  Queries the instance registry for the healthy shared_master instance
  and returns its data volume ID.
  """
  dynamodb = boto3.client("dynamodb", region_name=env.AWS_REGION)

  # Query instance registry for shared master
  table_name = f"robosystems-graph-{env.ENVIRONMENT}-instances"
  context.log.info(f"Querying {table_name} for shared_master instance")

  response = dynamodb.scan(
    TableName=table_name,
    FilterExpression="node_type = :nt AND #status = :s",
    ExpressionAttributeNames={"#status": "status"},
    ExpressionAttributeValues={
      ":nt": {"S": "shared_master"},
      ":s": {"S": "healthy"},
    },
  )

  items = response.get("Items", [])
  if not items:
    raise Exception(
      f"No healthy shared_master found in {table_name}. "
      "Ensure the shared master is deployed and healthy."
    )

  # Get first healthy shared master
  master = items[0]
  instance_id = master["instance_id"]["S"]
  context.log.info(f"Found shared master instance: {instance_id}")

  # Get volume ID from instance or registry
  if "volume_id" in master:
    volume_id = master["volume_id"]["S"]
    context.log.info(f"Found volume ID from registry: {volume_id}")
    return volume_id

  # Fall back to querying EC2 for attached volumes
  ec2 = boto3.client("ec2", region_name=env.AWS_REGION)
  response = ec2.describe_instances(InstanceIds=[instance_id])

  if not response["Reservations"]:
    raise Exception(f"Instance {instance_id} not found in EC2")

  instance = response["Reservations"][0]["Instances"][0]

  # Find data volume (typically /dev/xvdf)
  for bdm in instance.get("BlockDeviceMappings", []):
    if bdm["DeviceName"] in ("/dev/xvdf", "/dev/sdf"):
      volume_id = bdm["Ebs"]["VolumeId"]
      context.log.info(f"Found data volume from EC2: {volume_id}")
      return volume_id

  raise Exception(
    f"No data volume found for instance {instance_id}. Expected /dev/xvdf or /dev/sdf."
  )


@op
def create_snapshot(
  context: OpExecutionContext, volume_id: str, config: SnapshotConfig
) -> str:
  """Create EBS snapshot of shared master's data volume.

  Creates a snapshot with appropriate tags for tracking and
  optionally waits for completion.
  """
  ec2 = boto3.client("ec2", region_name=env.AWS_REGION)

  timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
  description = (
    f"{config.description_prefix} {config.graph_id} - Dagster run {context.run_id[:8]}"
  )

  context.log.info(f"Creating snapshot of volume {volume_id}")

  response = ec2.create_snapshot(
    VolumeId=volume_id,
    Description=description,
    TagSpecifications=[
      {
        "ResourceType": "snapshot",
        "Tags": [
          {
            "Key": "Name",
            "Value": f"robosystems-shared-{config.graph_id}-{timestamp}",
          },
          {"Key": "Environment", "Value": env.ENVIRONMENT},
          {"Key": "AllRepositories", "Value": "true"},
          {"Key": "DagsterRunId", "Value": context.run_id},
          {"Key": "GraphId", "Value": config.graph_id},
          {"Key": "CreatedBy", "Value": "Dagster"},
          {"Key": "Service", "Value": "RoboSystems"},
          {"Key": "Component", "Value": "SharedRepository"},
        ],
      }
    ],
  )

  snapshot_id = response["SnapshotId"]
  context.log.info(f"Created snapshot: {snapshot_id}")

  if config.wait_for_completion:
    context.log.info(
      "Waiting for snapshot completion (this may take several minutes)..."
    )
    waiter = ec2.get_waiter("snapshot_completed")
    waiter.wait(
      SnapshotIds=[snapshot_id],
      WaiterConfig={
        "Delay": 30,  # Check every 30 seconds
        "MaxAttempts": 120,  # Wait up to 60 minutes
      },
    )
    context.log.info(f"Snapshot {snapshot_id} completed successfully")

  return snapshot_id


@op
def update_replica_launch_template(
  context: OpExecutionContext, snapshot_id: str
) -> dict[str, Any]:
  """Update replica ASG launch template with new snapshot ID.

  Creates a new launch template version with the updated snapshot
  and sets it as the default.
  """
  ec2 = boto3.client("ec2", region_name=env.AWS_REGION)

  lt_name = f"robosystems-shared-replicas-{env.ENVIRONMENT}"
  context.log.info(f"Updating launch template: {lt_name}")

  # Get current launch template version
  response = ec2.describe_launch_template_versions(
    LaunchTemplateName=lt_name, Versions=["$Latest"]
  )

  if not response["LaunchTemplateVersions"]:
    raise Exception(f"Launch template {lt_name} not found")

  current = response["LaunchTemplateVersions"][0]
  lt_id = current["LaunchTemplateId"]
  current_version = current["VersionNumber"]
  lt_data = current["LaunchTemplateData"]

  context.log.info(f"Current launch template version: {current_version}")

  # Update snapshot ID in block device mappings
  updated = False
  for bdm in lt_data.get("BlockDeviceMappings", []):
    if bdm["DeviceName"] in ("/dev/xvdf", "/dev/sdf"):
      old_snapshot = bdm.get("Ebs", {}).get("SnapshotId", "none")
      bdm["Ebs"]["SnapshotId"] = snapshot_id
      # Remove VolumeSize if present - size comes from snapshot
      bdm["Ebs"].pop("VolumeSize", None)
      updated = True
      context.log.info(f"Updated snapshot: {old_snapshot} -> {snapshot_id}")
      break

  if not updated:
    raise Exception("No data volume found in launch template block device mappings")

  # Create new launch template version
  new_version_response = ec2.create_launch_template_version(
    LaunchTemplateId=lt_id,
    SourceVersion=str(current_version),
    LaunchTemplateData=lt_data,
    VersionDescription=f"Snapshot {snapshot_id} - Dagster",
  )

  new_version = new_version_response["LaunchTemplateVersion"]["VersionNumber"]
  context.log.info(f"Created new launch template version: {new_version}")

  # Set as default version
  ec2.modify_launch_template(LaunchTemplateId=lt_id, DefaultVersion=str(new_version))
  context.log.info(f"Set version {new_version} as default")

  return {
    "launch_template_id": lt_id,
    "launch_template_name": lt_name,
    "previous_version": current_version,
    "new_version": new_version,
    "snapshot_id": snapshot_id,
  }


@op
def refresh_replica_instances(
  context: OpExecutionContext, lt_update: dict[str, Any], config: ReplicaConfig
) -> dict[str, Any]:
  """Trigger rolling refresh of replica ASG.

  Starts an instance refresh that gradually replaces instances
  with new ones using the updated launch template.
  """
  autoscaling = boto3.client("autoscaling", region_name=env.AWS_REGION)

  asg_name = f"robosystems-shared-replicas-{env.ENVIRONMENT}-asg"
  context.log.info(f"Starting instance refresh for ASG: {asg_name}")

  # Check if ASG exists and has instances
  response = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])

  if not response["AutoScalingGroups"]:
    context.log.warning(f"ASG {asg_name} not found - skipping refresh")
    return {
      "status": "skipped",
      "reason": "ASG not found",
      "asg_name": asg_name,
    }

  asg = response["AutoScalingGroups"][0]
  desired_capacity = asg["DesiredCapacity"]

  if desired_capacity == 0:
    context.log.info("No replica instances to refresh (ASG at 0 capacity)")
    return {
      "status": "skipped",
      "reason": "No instances to refresh",
      "asg_name": asg_name,
      "desired_capacity": 0,
    }

  context.log.info(f"ASG has {desired_capacity} instances - starting refresh")

  # Trigger rolling refresh
  refresh_response = autoscaling.start_instance_refresh(
    AutoScalingGroupName=asg_name,
    Strategy="Rolling",
    Preferences={
      "MinHealthyPercentage": config.min_healthy_percentage,
      "InstanceWarmup": config.instance_warmup_seconds,
    },
  )

  refresh_id = refresh_response["InstanceRefreshId"]
  context.log.info(f"Started instance refresh: {refresh_id}")

  return {
    "status": "started",
    "refresh_id": refresh_id,
    "asg_name": asg_name,
    "desired_capacity": desired_capacity,
    "min_healthy_percentage": config.min_healthy_percentage,
    "instance_warmup_seconds": config.instance_warmup_seconds,
    "launch_template_version": lt_update.get("new_version"),
  }


# ============================================================================
# Jobs
# ============================================================================


@job
def shared_repository_snapshot_job():
  """Create snapshot of shared master and update replicas.

  Full pipeline:
  1. Discover shared master's data volume from DynamoDB
  2. Create EBS snapshot of the volume
  3. Update replica ASG launch template with new snapshot
  4. Trigger rolling instance refresh

  This job is typically run after SEC materialization completes.
  """
  volume_id = get_shared_master_volume()
  snapshot_id = create_snapshot(volume_id)
  lt_update = update_replica_launch_template(snapshot_id)
  refresh_replica_instances(lt_update)


@job
def shared_repository_snapshot_only_job():
  """Create snapshot without updating replicas.

  Useful for:
  - Creating a backup snapshot
  - Testing snapshot creation
  - Manual control over when replicas are updated
  """
  volume_id = get_shared_master_volume()
  create_snapshot(volume_id)


@op
def get_current_launch_template_info(context: OpExecutionContext) -> dict[str, Any]:
  """Get current launch template info for refresh-only operations."""
  return {
    "launch_template_name": f"robosystems-shared-replicas-{env.ENVIRONMENT}",
    "new_version": "current",
  }


@job
def shared_repository_refresh_replicas_job():
  """Refresh replicas with current launch template.

  Useful for:
  - Forcing a refresh without creating a new snapshot
  - Recovering from failed refresh
  - Rolling out non-snapshot changes (e.g., new AMI)

  Note: This uses the existing launch template - run snapshot_job
  first if you need to update the snapshot.
  """
  lt_info = get_current_launch_template_info()
  refresh_replica_instances(lt_info)


# ============================================================================
# Schedules
# ============================================================================

# Weekly snapshot schedule - Sundays at 6 AM UTC (after SEC materialization)
# This gives time for any weekend SEC processing to complete
# Auto-enabled in prod/staging only
weekly_shared_repository_snapshot_schedule = ScheduleDefinition(
  job=shared_repository_snapshot_job,
  cron_schedule="0 6 * * 0",  # Sundays at 6 AM UTC
  default_status=SHARED_REPO_SCHEDULE_STATUS,
)
