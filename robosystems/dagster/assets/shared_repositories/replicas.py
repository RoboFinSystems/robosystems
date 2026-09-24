"""Rolling refresh of the shared replica fleet after a database is published to S3.

Replicas pull their databases from S3 on boot, so an ASG instance refresh picks
up the new version. One ASG serves every shared repository, so every refresh
cycles all instances.
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

  # 100: never terminate an old instance before its replacement is healthy, so
  # even a single-instance fleet has no downtime.
  min_healthy_percentage: int = 100

  # 200: the fleet may double so replacements launch alongside old instances.
  max_healthy_percentage: int = 200

  # S3 download + warmup of an ~85 GB database takes ~10-15 min.
  instance_warmup_seconds: int = 900

  poll_interval_seconds: int = 30

  max_wait_seconds: int = 7200  # 2 hours

  wait_for_completion: bool = True


def build_shared_replicas_refreshed(deps: list[str] | None = None):
  """Build the shared_replicas_refreshed asset.

  ``deps`` are the publish assets adapters declare under "shared_replica_deps";
  with none, the asset runs only when triggered via its job.
  """

  @asset(
    name="shared_replicas_refreshed",
    group_name="shared_repositories",
    description="Refresh shared replica fleet to pick up new S3 database",
    kinds={"aws", "autoscaling"},
    deps=deps or [],
    metadata={
      "stage": "replica_refresh",
    },
  )
  def shared_replicas_refreshed(
    context: AssetExecutionContext,
    config: SharedReplicaRefreshConfig,
  ) -> MaterializeResult:
    """Trigger and monitor rolling refresh of shared replica fleet."""
    import boto3

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
      context.log.info(
        f"Starting rolling instance refresh with {config.min_healthy_percentage}% "
        f"min healthy, {config.max_healthy_percentage}% max healthy, "
        f"{config.instance_warmup_seconds}s warmup"
      )

      try:
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
      except Exception as e:
        raise RuntimeError(f"Failed to start instance refresh: {e}")

    if not config.wait_for_completion:
      return MaterializeResult(
        metadata={
          "status": "started",
          "refresh_id": refresh_id,
          "asg_name": asg_name,
          "desired_capacity": desired_capacity,
          "min_healthy_percentage": config.min_healthy_percentage,
          "max_healthy_percentage": config.max_healthy_percentage,
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

    elapsed = (datetime.now(UTC) - start_time).total_seconds()
    raise RuntimeError(
      f"Instance refresh timed out after {int(elapsed // 60)}m. "
      f"Last status: {last_status}, {last_progress}% complete. "
      f"Refresh ID: {refresh_id}"
    )

  return shared_replicas_refreshed
