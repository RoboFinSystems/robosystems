"""Shared Replica Fleet Refresh Asset.

This asset triggers and monitors the rolling refresh of the shared replica fleet
after a new database has been published to S3. Replicas use S3 ATTACH to connect
to the published database, so a refresh cycles instances to pick up the new version.

The replica fleet is a single shared ASG that serves all shared repositories
(SEC, industry, economic, etc.). Refreshing it cycles all instances regardless
of which repository was published.

At scale (100+ replicas), this can take hours. The asset monitors progress and
logs each stage of the refresh for visibility.
"""

import time
from datetime import UTC, datetime

from dagster import (
  AssetExecutionContext,
  Config,
  MaterializeResult,
  asset,
)

from robosystems.config import env


class SharedReplicaRefreshConfig(Config):
  """Configuration for replica fleet refresh."""

  # Minimum percentage of healthy instances during refresh
  # Higher = slower but safer, Lower = faster but riskier
  min_healthy_percentage: int = 50

  # Seconds to wait for new instance to become healthy
  # S3 ATTACH download + warmup takes ~10-15 min for 85GB database
  instance_warmup_seconds: int = 900

  # How often to poll for refresh status (seconds)
  poll_interval_seconds: int = 30

  # Maximum time to wait for refresh completion (seconds)
  # With 100 replicas at 40 min each and 50% parallel, expect ~80 min
  max_wait_seconds: int = 7200  # 2 hours

  # Whether to wait for completion or just start the refresh
  wait_for_completion: bool = True


@asset(
  group_name="shared_repositories",
  description="Refresh shared replica fleet to pick up new S3 database",
  kinds={"aws", "autoscaling"},
  deps=["sec_s3_published"],  # Add future repos here: "industry_s3_published", etc.
  metadata={
    "stage": "replica_refresh",
  },
)
def shared_replicas_refreshed(
  context: AssetExecutionContext,
  config: SharedReplicaRefreshConfig,
) -> MaterializeResult:
  """Trigger and monitor rolling refresh of shared replica fleet.

  After a shared repository database is published to S3, replicas need to be
  cycled to pick up the new version via S3 ATTACH. This asset:

  1. Checks for existing in-progress refresh
  2. Starts a new rolling instance refresh
  3. Monitors progress, logging each stage
  4. Completes when all instances are refreshed

  At scale, this can take hours. The asset provides visibility into:
  - Instances pending, in progress, and completed
  - Success/failure counts
  - Estimated time remaining

  Returns:
      MaterializeResult with refresh statistics and timing
  """
  import boto3

  # Skip in dev environment
  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping replica refresh in dev environment")
    return MaterializeResult(
      metadata={
        "status": "skipped",
        "reason": "dev_environment",
      }
    )

  autoscaling = boto3.client("autoscaling", region_name=env.AWS_REGION)
  asg_name = f"robosystems-shared-replicas-{env.ENVIRONMENT}-asg"

  start_time = datetime.now(UTC)
  context.log.info(f"Starting replica fleet refresh for {asg_name}")

  # =========================================================================
  # Step 1: Check ASG exists and has instances
  # =========================================================================
  try:
    asg_response = autoscaling.describe_auto_scaling_groups(
      AutoScalingGroupNames=[asg_name]
    )
  except Exception as e:
    raise RuntimeError(f"Failed to describe ASG {asg_name}: {e}")

  if not asg_response["AutoScalingGroups"]:
    context.log.warning(f"ASG {asg_name} not found")
    return MaterializeResult(
      metadata={
        "status": "skipped",
        "reason": "asg_not_found",
        "asg_name": asg_name,
      }
    )

  asg = asg_response["AutoScalingGroups"][0]
  desired_capacity = asg["DesiredCapacity"]
  current_instances = len(asg.get("Instances", []))

  context.log.info(
    f"ASG {asg_name}: desired={desired_capacity}, current={current_instances}"
  )

  if desired_capacity == 0:
    context.log.info("No replicas to refresh (ASG at 0 capacity)")
    return MaterializeResult(
      metadata={
        "status": "skipped",
        "reason": "no_instances",
        "asg_name": asg_name,
      }
    )

  # =========================================================================
  # Step 2: Check for existing in-progress refresh
  # =========================================================================
  context.log.info("Checking for existing instance refresh...")

  refresh_check = autoscaling.describe_instance_refreshes(
    AutoScalingGroupName=asg_name,
    MaxRecords=1,
  )

  existing = refresh_check.get("InstanceRefreshes", [])
  if existing and existing[0]["Status"] in ("Pending", "InProgress"):
    existing_refresh = existing[0]
    refresh_id = existing_refresh["InstanceRefreshId"]
    status = existing_refresh["Status"]
    progress = existing_refresh.get("PercentageComplete", 0)

    context.log.warning(
      f"Refresh already in progress: {refresh_id} ({status}, {progress}% complete)"
    )

    if config.wait_for_completion:
      context.log.info("Will monitor existing refresh instead of starting new one")
      # Fall through to monitoring loop
    else:
      return MaterializeResult(
        metadata={
          "status": "existing_refresh",
          "refresh_id": refresh_id,
          "existing_status": status,
          "existing_progress": progress,
        }
      )
  else:
    # =========================================================================
    # Step 3: Start new instance refresh
    # =========================================================================
    context.log.info(
      f"Starting rolling instance refresh with {config.min_healthy_percentage}% "
      f"minimum healthy, {config.instance_warmup_seconds}s warmup"
    )

    try:
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
    except Exception as e:
      raise RuntimeError(f"Failed to start instance refresh: {e}")

  # =========================================================================
  # Step 4: Monitor refresh progress
  # =========================================================================
  if not config.wait_for_completion:
    return MaterializeResult(
      metadata={
        "status": "started",
        "refresh_id": refresh_id,
        "asg_name": asg_name,
        "desired_capacity": desired_capacity,
        "min_healthy_percentage": config.min_healthy_percentage,
        "instance_warmup_seconds": config.instance_warmup_seconds,
        "wait_for_completion": False,
      }
    )

  context.log.info(
    f"Monitoring refresh progress (polling every {config.poll_interval_seconds}s, "
    f"max wait {config.max_wait_seconds}s)..."
  )

  last_progress = -1
  last_status = None
  iterations = 0
  max_iterations = config.max_wait_seconds // config.poll_interval_seconds

  while iterations < max_iterations:
    iterations += 1
    time.sleep(config.poll_interval_seconds)

    try:
      status_response = autoscaling.describe_instance_refreshes(
        AutoScalingGroupName=asg_name,
        InstanceRefreshIds=[refresh_id],
      )
    except Exception as e:
      context.log.warning(f"Failed to get refresh status: {e}")
      continue

    refreshes = status_response.get("InstanceRefreshes", [])
    if not refreshes:
      context.log.warning(f"Refresh {refresh_id} not found")
      continue

    refresh = refreshes[0]
    status = refresh["Status"]
    progress = refresh.get("PercentageComplete", 0)
    instances_to_update = refresh.get("InstancesToUpdate", 0)

    # Log progress changes
    if progress != last_progress or status != last_status:
      elapsed = (datetime.now(UTC) - start_time).total_seconds()
      elapsed_min = int(elapsed // 60)

      if progress > 0:
        estimated_total = elapsed / (progress / 100)
        remaining = estimated_total - elapsed
        remaining_min = int(remaining // 60)
        eta_str = f", ~{remaining_min}m remaining"
      else:
        eta_str = ""

      context.log.info(
        f"[{elapsed_min}m] Refresh {status}: {progress}% complete, "
        f"{instances_to_update} instances pending{eta_str}"
      )

      last_progress = progress
      last_status = status

    # Check terminal states
    if status == "Successful":
      end_time = datetime.now(UTC)
      duration = (end_time - start_time).total_seconds()

      context.log.info(
        f"Refresh completed successfully in {int(duration // 60)}m {int(duration % 60)}s"
      )

      return MaterializeResult(
        metadata={
          "status": "completed",
          "refresh_id": refresh_id,
          "asg_name": asg_name,
          "instances_refreshed": desired_capacity,
          "duration_seconds": int(duration),
          "duration_minutes": round(duration / 60, 1),
          "started_at": start_time.isoformat(),
          "completed_at": end_time.isoformat(),
        }
      )

    if status in ("Cancelled", "Failed", "RollbackSuccessful", "RollbackFailed"):
      status_reason = refresh.get("StatusReason", "Unknown")
      raise RuntimeError(f"Instance refresh {status}: {status_reason}")

  # Timeout
  elapsed = (datetime.now(UTC) - start_time).total_seconds()
  raise RuntimeError(
    f"Instance refresh timed out after {int(elapsed // 60)}m. "
    f"Last status: {last_status}, {last_progress}% complete. "
    f"Refresh ID: {refresh_id}"
  )
