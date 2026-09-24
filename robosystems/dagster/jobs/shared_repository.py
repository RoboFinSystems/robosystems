"""Platform-generic lifecycle jobs for the shared master and its replica fleet.

Adapter pipelines (SEC today) drive these from their sensor chains:
wake/sleep bookend the master-dependent part of a run, and
shared_replicas_refresh runs in asset lineage after publish.
shared_repository_refresh_replicas_job is the ad-hoc, fire-and-forget refresh.
"""

from typing import Any

import boto3
from dagster import (
  AssetSelection,
  Config,
  OpExecutionContext,
  define_asset_job,
  job,
  op,
)

from robosystems.config import env

# Light on-demand profile: a few AWS API calls plus a health poll. The
# triggering sensor adds its own pipeline/mode/phase tags per run.
_MASTER_PARKING_TAGS = {
  "pipeline": "shared",
  "ecs/cpu": "512",
  "ecs/memory": "2048",
  "ecs/ephemeral_storage": "21",
  "ecs/run_task_kwargs": {
    "capacityProviderStrategy": [
      {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
    ],
  },
}

shared_master_wake_job = define_asset_job(
  name="shared_master_wake",
  description="Scale the shared master to 1 and wait until healthy.",
  selection=AssetSelection.keys("shared_master_awake"),
  tags=_MASTER_PARKING_TAGS,
)

shared_master_sleep_job = define_asset_job(
  name="shared_master_sleep",
  description="Clear scale-in protection and scale the shared master to 0.",
  selection=AssetSelection.keys("shared_master_asleep"),
  tags=_MASTER_PARKING_TAGS,
)


# Selected by key: the asset is built in definitions.py with adapter deps.
shared_replicas_refresh_job = define_asset_job(
  name="shared_replicas_refresh",
  description="Refresh shared replica fleet (materializes shared_replicas_refreshed asset).",
  selection=AssetSelection.keys("shared_replicas_refreshed"),
  tags={
    "pipeline": "shared",
    "phase": "replica_refresh",
    "ecs/cpu": "512",
    "ecs/memory": "2048",
    "ecs/ephemeral_storage": "21",
    # On-demand: Spot interruptions would kill the long poll.
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)


class ReplicaConfig(Config):
  """Configuration for replica operations."""

  # 100 = never terminate old instance until replacement is healthy
  min_healthy_percentage: int = 100
  # 200 = allow temporarily doubling fleet during refresh
  max_healthy_percentage: int = 200
  # Matches the CloudFormation HealthCheckGracePeriod; S3 downloads are slow.
  instance_warmup_seconds: int = 900


@op
def refresh_replica_instances(
  context: OpExecutionContext, config: ReplicaConfig
) -> dict[str, Any]:
  """Start a rolling replica ASG refresh so instances pick up the new S3 database.

  Skips if a refresh is already active.
  """
  autoscaling = boto3.client("autoscaling", region_name=env.AWS_REGION)

  asg_name = f"robosystems-shared-replicas-{env.ENVIRONMENT}-asg"
  context.log.info(f"Checking ASG: {asg_name}")

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

  context.log.info("Checking for existing instance refresh...")
  refresh_response = autoscaling.describe_instance_refreshes(
    AutoScalingGroupName=asg_name,
    MaxRecords=1,
  )

  existing_refreshes = refresh_response.get("InstanceRefreshes", [])
  if existing_refreshes:
    latest_refresh = existing_refreshes[0]
    refresh_status = latest_refresh["Status"]

    if refresh_status in ("Pending", "InProgress", "Cancelling"):
      existing_id = latest_refresh["InstanceRefreshId"]
      context.log.warning(
        f"Instance refresh already in progress: {existing_id} "
        f"(status: {refresh_status}). Skipping to avoid conflict."
      )
      return {
        "status": "skipped",
        "reason": "refresh_already_in_progress",
        "existing_refresh_id": existing_id,
        "existing_refresh_status": refresh_status,
        "asg_name": asg_name,
        "desired_capacity": desired_capacity,
      }

  context.log.info(f"ASG has {desired_capacity} instances - starting refresh")

  refresh_response = autoscaling.start_instance_refresh(
    AutoScalingGroupName=asg_name,
    Strategy="Rolling",
    Preferences={
      "MinHealthyPercentage": config.min_healthy_percentage,
      "MaxHealthyPercentage": config.max_healthy_percentage,
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
    "max_healthy_percentage": config.max_healthy_percentage,
    "instance_warmup_seconds": config.instance_warmup_seconds,
  }


@job(
  tags={
    "dagster/priority": "-1",
    "dagster/max_retries": 3,
    # On-demand: critical infrastructure.
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  }
)
def shared_repository_refresh_replicas_job():
  """Refresh replicas from the current S3 database, outside the publish lineage.

  For forcing a refresh, recovering from a failed one, or rolling out AMI/code changes.
  """
  refresh_replica_instances()
