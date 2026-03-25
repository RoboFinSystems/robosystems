"""Dagster jobs for shared repository replica management.

Two jobs for refreshing the shared replica fleet:

1. shared_replicas_refresh_job (asset job):
   Materializes the shared_replicas_refreshed asset. Used by the automated
   pipeline sensor chain (materialize → publish → refresh) so Dagster
   tracks the materialization in asset lineage.

2. shared_repository_refresh_replicas_job (standalone op job):
   Fire-and-forget refresh for ad-hoc operations:
   - Forcing a refresh after a failed previous refresh
   - Rolling out non-database changes (e.g., new AMI, code updates)
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

# Asset job: materializes shared_replicas_refreshed asset.
# Uses AssetSelection.key() because the asset is built dynamically by
# build_shared_replicas_refreshed() in definitions.py with adapter-specific deps.
shared_replicas_refresh_job = define_asset_job(
  name="shared_replicas_refresh",
  description="Refresh shared replica fleet (materializes shared_replicas_refreshed asset).",
  selection=AssetSelection.keys("shared_replicas_refreshed"),
  tags={
    "pipeline": "shared",
    "phase": "replica_refresh",
    # Light profile: just AWS API calls to start ASG refresh + polling
    "ecs/cpu": "512",
    "ecs/memory": "2048",
    "ecs/ephemeral_storage": "21",
    # On-demand to avoid Spot interruptions during long polling
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
  # Set to 900s to match CloudFormation HealthCheckGracePeriod (15 min)
  # Large database downloads from S3 need significant warmup time
  instance_warmup_seconds: int = 900


@op
def refresh_replica_instances(
  context: OpExecutionContext, config: ReplicaConfig
) -> dict[str, Any]:
  """Trigger rolling refresh of replica ASG.

  Starts an instance refresh that gradually replaces instances
  so they pick up the new S3 database via ATTACH.

  Checks for existing in-progress refresh and skips if one is active.
  """
  autoscaling = boto3.client("autoscaling", region_name=env.AWS_REGION)

  asg_name = f"robosystems-shared-replicas-{env.ENVIRONMENT}-asg"
  context.log.info(f"Checking ASG: {asg_name}")

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

  # Check for existing in-progress instance refresh
  context.log.info("Checking for existing instance refresh...")
  refresh_response = autoscaling.describe_instance_refreshes(
    AutoScalingGroupName=asg_name,
    MaxRecords=1,
  )

  existing_refreshes = refresh_response.get("InstanceRefreshes", [])
  if existing_refreshes:
    latest_refresh = existing_refreshes[0]
    refresh_status = latest_refresh["Status"]

    # Active statuses that block new refresh
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

  # Trigger rolling refresh
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
    # Critical infrastructure - use on-demand to avoid Spot interruptions
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  }
)
def shared_repository_refresh_replicas_job():
  """Refresh replicas with current S3 database.

  Useful for:
  - Forcing a refresh without publishing a new database
  - Recovering from failed refresh
  - Rolling out non-database changes (e.g., new AMI, code updates)

  The normal publish + refresh flow is handled by asset lineage:
    sec_graph_materialized -> sec_lbug_s3_published -> shared_replicas_refreshed
  """
  refresh_replica_instances()
