"""Dagster jobs for shared repository management.

These jobs handle S3-based replica data distribution:
- Upload shared master's database to S3 for replicas
- Trigger rolling instance refresh of replica fleet

Replicas use LadybugDB S3 ATTACH to connect directly to S3-hosted
.lbug files using the httpfs extension.

These jobs are typically triggered after SEC materialization completes,
or can be run manually for ad-hoc sync/refresh operations.

NOTE: Previous implementation used raw httpx (no auth) and SSM for S3 upload.
Now uses Graph Client Factory which handles auth, routing, and circuit breakers.
The backup runs entirely on-instance via the Graph API backup endpoint.
"""

import asyncio
from typing import Any

import boto3
from dagster import (
  Config,
  OpExecutionContext,
  job,
  op,
)

from robosystems.config import env

# ============================================================================
# Configuration
# ============================================================================


class S3SyncConfig(Config):
  """Configuration for S3 sync operations."""

  graph_id: str = "sec"
  s3_prefix: str = "shared-repos"


class ReplicaConfig(Config):
  """Configuration for replica operations."""

  min_healthy_percentage: int = 50
  # Set to 900s to match CloudFormation HealthCheckGracePeriod (15 min)
  # Large S3 ATTACH databases need significant warmup time for httpfs caching
  instance_warmup_seconds: int = 900


# ============================================================================
# S3 Sync Operations (via Graph Client Factory)
# ============================================================================


@op
def upload_database_to_s3(
  context: OpExecutionContext,
  config: S3SyncConfig,
) -> dict[str, Any]:
  """Upload shared database to S3 for replicas via Graph API.

  Uses Graph Client Factory to call the backup endpoint on the shared master.
  The Graph API handles:
  1. CHECKPOINT to flush WAL
  2. Raw .lbug multipart upload to S3

  This replaces the previous SSM-based approach which bypassed Graph API auth.
  """
  from robosystems.graph_api.client.factory import get_graph_client_for_sec_ingestion

  graph_id = config.graph_id
  s3_prefix = config.s3_prefix

  # Get AWS account ID for bucket name
  sts = boto3.client("sts", region_name=env.AWS_REGION)
  account_id = sts.get_caller_identity()["Account"]

  # Build S3 path
  bucket = f"robosystems-{account_id}-user-{env.ENVIRONMENT}"
  s3_key = f"{s3_prefix}/{graph_id}.lbug"
  s3_uri = f"s3://{bucket}/{s3_key}"

  context.log.info(f"Uploading database to: {s3_uri}")

  # Use Graph Client Factory (handles auth, routing, circuit breakers)
  loop = asyncio.new_event_loop()
  try:
    client = loop.run_until_complete(get_graph_client_for_sec_ingestion())

    context.log.info("Calling backup endpoint on shared master...")
    result = loop.run_until_complete(
      client.backup_with_sse(
        graph_id=graph_id,
        backup_type="replica",
        s3_destination={"bucket": bucket, "key": s3_key},
        compression=False,
        checkpoint=True,
        timeout=7200,  # 2 hours for large databases
      )
    )
  finally:
    loop.close()

  if result.get("status") != "completed":
    error = result.get("error", "Unknown error")
    raise RuntimeError(f"S3 upload failed: {error}")

  # Verify upload by checking S3
  s3 = boto3.client("s3", region_name=env.AWS_REGION)
  head = s3.head_object(Bucket=bucket, Key=s3_key)
  file_size = head["ContentLength"]
  last_modified = head["LastModified"]

  context.log.info(
    f"Database uploaded to S3: {s3_uri} "
    f"(size: {file_size / (1024**3):.2f}GB, modified: {last_modified})"
  )

  return {
    "status": "success",
    "s3_uri": s3_uri,
    "s3_bucket": bucket,
    "s3_key": s3_key,
    "file_size_bytes": file_size,
    "file_size_gb": round(file_size / (1024**3), 2),
    "last_modified": last_modified.isoformat(),
    "graph_id": graph_id,
  }


@op
def refresh_replica_instances(
  context: OpExecutionContext, s3_upload: dict[str, Any], config: ReplicaConfig
) -> dict[str, Any]:
  """Trigger rolling refresh of replica ASG.

  Starts an instance refresh that gradually replaces instances
  so they pick up the new S3 database via ATTACH.

  Checks for existing in-progress refresh and skips if one is active.
  """
  # Skip if S3 upload failed
  if s3_upload.get("status") != "success":
    context.log.info(
      f"Skipping instance refresh: S3 upload status was {s3_upload.get('status')}"
    )
    return {
      "status": "skipped",
      "reason": "s3_upload_not_successful",
    }

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
    "s3_uri": s3_upload.get("s3_uri"),
  }


# ============================================================================
# Jobs
# ============================================================================


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
def shared_repository_s3_sync_job():
  """Upload shared database to S3 and refresh replicas.

  Pipeline:
  1. CHECKPOINT + upload database to S3 via Graph API backup endpoint
  2. Trigger rolling instance refresh (skips if refresh already in progress)

  Replicas use S3 ATTACH to connect directly to the S3-hosted database
  via LadybugDB's httpfs extension.

  This job is typically run after SEC materialization completes.
  """
  s3_upload = upload_database_to_s3()
  refresh_replica_instances(s3_upload)


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
def shared_repository_s3_upload_only_job():
  """Upload database to S3 without refreshing replicas.

  Useful for:
  - Initial S3 upload before replicas are deployed
  - Manual control over when replicas are refreshed
  - Testing S3 upload without affecting replicas
  """
  upload_database_to_s3()


@op
def get_current_s3_database_info(context: OpExecutionContext) -> dict[str, Any]:
  """Get current S3 database info for refresh-only operations."""
  import boto3

  sts = boto3.client("sts", region_name=env.AWS_REGION)
  account_id = sts.get_caller_identity()["Account"]

  bucket = f"robosystems-{account_id}-user-{env.ENVIRONMENT}"
  s3_key = "shared-repos/sec.lbug"
  s3_uri = f"s3://{bucket}/{s3_key}"

  return {
    "status": "success",
    "s3_uri": s3_uri,
    "s3_bucket": bucket,
    "s3_key": s3_key,
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
  - Forcing a refresh without uploading a new database
  - Recovering from failed refresh
  - Rolling out non-database changes (e.g., new AMI, code updates)

  Note: This uses the existing S3 database - run s3_sync_job
  first if you need to upload a new database version.
  """
  s3_info = get_current_s3_database_info()
  refresh_replica_instances(s3_info)
