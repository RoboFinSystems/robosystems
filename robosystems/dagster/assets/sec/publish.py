"""SEC S3 Publish Asset.

This asset publishes the SEC database to S3 for replica cluster consumption.
Replicas use LadybugDB S3 ATTACH to connect directly to the published database
via the httpfs extension.

The asset:
1. Uses Graph Client Factory to call the backup endpoint on the shared master
2. The Graph API runs CHECKPOINT and uploads raw .lbug to S3 on-instance
3. Optionally triggers replica fleet refresh

This is distinct from sec_backup which creates compressed, downloadable backups
for users. This asset creates the source-of-truth for the replica cluster.

NOTE: Previous implementation used raw httpx (no auth - caused 401 errors) and
SSM for S3 upload. Now uses Graph Client Factory which handles auth, routing,
and circuit breakers automatically.
"""

import asyncio
from datetime import UTC, datetime

from dagster import (
  AssetExecutionContext,
  Config,
  MaterializeResult,
  asset,
)

from robosystems.config import env


class SECS3PublishConfig(Config):
  """Configuration for S3 publish operations."""

  graph_id: str = "sec"
  # Replica refresh is now a separate asset (sec_replicas_refreshed)
  # Set to True only for backwards compatibility or one-off runs
  refresh_replicas: bool = False
  min_healthy_percentage: int = 50
  instance_warmup_seconds: int = 900


@asset(
  group_name="sec_pipeline",
  description="Publish SEC database to S3 for replica cluster (S3 ATTACH source)",
  kinds={"s3", "ladybug"},
  deps=["sec_graph_materialized"],
  metadata={
    "pipeline": "sec",
    "stage": "publish",
    "replica_source": True,
  },
)
def sec_s3_published(
  context: AssetExecutionContext,
  config: SECS3PublishConfig,
) -> MaterializeResult:
  """Publish SEC database to S3 for replica consumption via ATTACH.

  Uses Graph Client Factory to call the backup endpoint on the shared master.
  The backup runs entirely on-instance (CHECKPOINT + S3 multipart upload),
  so this Dagster task only orchestrates and monitors via SSE.

  Returns:
      MaterializeResult with S3 URI and upload statistics
  """
  graph_id = config.graph_id
  context.log.info(f"Publishing {graph_id} database to S3 for replica cluster")

  # Skip in dev environment
  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping S3 publish in dev environment")
    return MaterializeResult(
      metadata={
        "status": "skipped",
        "reason": "dev_environment",
      }
    )

  import boto3

  from robosystems.graph_api.client.factory import get_graph_client_for_sec_ingestion
  from robosystems.middleware.graph.utils import MultiTenantUtils

  # Validate graph_id is a shared repository
  if not MultiTenantUtils.is_shared_repository(graph_id):
    raise ValueError(f"{graph_id} is not a shared repository")

  # Build S3 destination
  sts = boto3.client("sts", region_name=env.AWS_REGION)
  account_id = sts.get_caller_identity()["Account"]
  bucket = f"robosystems-{account_id}-user-{env.ENVIRONMENT}"
  s3_key = f"shared-repos/{graph_id}.lbug"
  s3_uri = f"s3://{bucket}/{s3_key}"

  context.log.info(f"Target: {s3_uri}")

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
    raise RuntimeError(f"S3 publish failed: {error}")

  # Verify upload
  s3 = boto3.client("s3", region_name=env.AWS_REGION)
  head = s3.head_object(Bucket=bucket, Key=s3_key)
  file_size = head["ContentLength"]
  last_modified = head["LastModified"]

  context.log.info(
    f"Database published to S3: {s3_uri} "
    f"(size: {file_size / (1024**3):.2f}GB, modified: {last_modified})"
  )

  # Optionally trigger replica refresh
  refresh_result = {"status": "skipped", "reason": "disabled"}

  if config.refresh_replicas:
    context.log.info("Triggering replica fleet refresh...")

    autoscaling = boto3.client("autoscaling", region_name=env.AWS_REGION)
    asg_name = f"robosystems-shared-replicas-{env.ENVIRONMENT}-asg"

    try:
      asg_response = autoscaling.describe_auto_scaling_groups(
        AutoScalingGroupNames=[asg_name]
      )

      if not asg_response["AutoScalingGroups"]:
        context.log.warning(f"ASG {asg_name} not found - skipping refresh")
        refresh_result = {"status": "skipped", "reason": "asg_not_found"}
      else:
        asg = asg_response["AutoScalingGroups"][0]
        desired_capacity = asg["DesiredCapacity"]

        if desired_capacity == 0:
          context.log.info("No replica instances to refresh (ASG at 0 capacity)")
          refresh_result = {"status": "skipped", "reason": "no_instances"}
        else:
          # Check for existing in-progress refresh
          refresh_check = autoscaling.describe_instance_refreshes(
            AutoScalingGroupName=asg_name,
            MaxRecords=1,
          )

          existing = refresh_check.get("InstanceRefreshes", [])
          if existing and existing[0]["Status"] in (
            "Pending",
            "InProgress",
            "Cancelling",
          ):
            context.log.warning(
              f"Refresh already in progress: {existing[0]['InstanceRefreshId']}"
            )
            refresh_result = {
              "status": "skipped",
              "reason": "refresh_in_progress",
              "existing_refresh_id": existing[0]["InstanceRefreshId"],
            }
          else:
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
            refresh_result = {
              "status": "started",
              "refresh_id": refresh_id,
              "asg_name": asg_name,
              "desired_capacity": desired_capacity,
            }

    except Exception as e:
      context.log.warning(f"Failed to trigger replica refresh: {e}")
      refresh_result = {"status": "error", "error": str(e)}

  return MaterializeResult(
    metadata={
      "s3_uri": s3_uri,
      "s3_bucket": bucket,
      "s3_key": s3_key,
      "file_size_bytes": file_size,
      "file_size_gb": round(file_size / (1024**3), 2),
      "last_modified": last_modified.isoformat(),
      "graph_id": graph_id,
      "published_at": datetime.now(UTC).isoformat(),
      "replica_refresh": refresh_result,
    }
  )
