"""SEC S3 Publish Asset.

This asset publishes the SEC database to S3 for replica cluster consumption.
Replicas use LadybugDB S3 ATTACH to connect directly to the published database
via the httpfs extension.

The asset:
1. Runs CHECKPOINT to flush WAL and ensure database consistency
2. Uploads the raw .lbug file to S3 (no compression - replicas need raw format)
3. Optionally triggers replica fleet refresh

This is distinct from sec_backup which creates compressed, downloadable backups
for users. This asset creates the source-of-truth for the replica cluster.
"""

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

  This asset is the source-of-truth for the shared replica cluster.
  Replicas connect to this S3-hosted database using LadybugDB's httpfs
  extension and S3 ATTACH command.

  Steps:
  1. Verify shared master is healthy
  2. Execute CHECKPOINT to flush WAL
  3. Upload raw .lbug file to S3 via SSM
  4. Optionally trigger rolling replica refresh

  The uploaded file is NOT compressed - replicas need the raw .lbug format
  for S3 ATTACH to work correctly.

  Returns:
      MaterializeResult with S3 URI and upload statistics
  """
  import boto3

  from robosystems.middleware.graph.utils import MultiTenantUtils

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

  # Validate graph_id is a shared repository
  if not MultiTenantUtils.is_shared_repository(graph_id):
    raise ValueError(f"{graph_id} is not a shared repository")

  dynamodb = boto3.client("dynamodb", region_name=env.AWS_REGION)
  ssm = boto3.client("ssm", region_name=env.AWS_REGION)
  s3 = boto3.client("s3", region_name=env.AWS_REGION)
  sts = boto3.client("sts", region_name=env.AWS_REGION)

  # Get AWS account ID for bucket name
  account_id = sts.get_caller_identity()["Account"]
  bucket = f"robosystems-{account_id}-user-{env.ENVIRONMENT}"
  s3_key = f"shared-repos/{graph_id}.lbug"
  s3_uri = f"s3://{bucket}/{s3_key}"

  # =========================================================================
  # Step 1: Verify shared master is healthy
  # =========================================================================
  context.log.info("Verifying shared master health...")

  table_name = env.INSTANCE_REGISTRY_TABLE
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
    raise RuntimeError(
      f"No healthy shared_master found in {table_name}. "
      "Ensure the shared master is deployed and healthy."
    )

  master = items[0]
  instance_id = master["instance_id"]["S"]
  private_ip = master.get("private_ip", {}).get("S")

  if not private_ip:
    raise RuntimeError(f"Shared master {instance_id} has no private_ip in registry.")

  context.log.info(f"Found shared master: {instance_id} at {private_ip}")

  # =========================================================================
  # Step 2: Execute CHECKPOINT to flush WAL
  # =========================================================================
  context.log.info(f"Executing CHECKPOINT on {graph_id}...")

  import httpx

  checkpoint_url = f"http://{private_ip}:8001/databases/{graph_id}/query"
  try:
    with httpx.Client(timeout=120.0) as client:
      response = client.post(checkpoint_url, json={"query": "CHECKPOINT"})
      response.raise_for_status()
      context.log.info("CHECKPOINT completed successfully")
  except httpx.TimeoutException:
    raise RuntimeError(f"CHECKPOINT timed out for {graph_id}")
  except httpx.HTTPStatusError as e:
    raise RuntimeError(f"CHECKPOINT failed: {e.response.status_code}")

  # =========================================================================
  # Step 3: Upload raw .lbug file to S3 via SSM
  # =========================================================================
  context.log.info(f"Uploading database to {s3_uri}...")

  upload_command = f"""#!/bin/bash
set -e

DB_PATH="/mnt/ladybug-data/databases/lbug-dbs/{graph_id}.lbug"
S3_URI="{s3_uri}"

echo "Uploading database to S3..."
echo "  Source: $DB_PATH"
echo "  Destination: $S3_URI"

# Verify database exists
if [ ! -e "$DB_PATH" ]; then
  echo "ERROR: Database not found at $DB_PATH"
  exit 1
fi

# Get size for logging
DB_SIZE=$(du -sh "$DB_PATH" | cut -f1)
echo "  Size: $DB_SIZE"

# Upload to S3 (no compression - replicas need raw .lbug format)
aws s3 cp "$DB_PATH" "$S3_URI" --region {env.AWS_REGION}

echo "Upload complete"
"""

  command_response = ssm.send_command(
    InstanceIds=[instance_id],
    DocumentName="AWS-RunShellScript",
    Parameters={"commands": [upload_command]},
    TimeoutSeconds=3600,  # 1 hour timeout for large uploads
    Comment=f"Publish {graph_id} to S3 - Dagster run {context.run_id[:8]}",
  )

  command_id = command_response["Command"]["CommandId"]
  context.log.info(f"SSM command started: {command_id}")

  # Wait for command to complete
  waiter = ssm.get_waiter("command_executed")
  try:
    waiter.wait(
      CommandId=command_id,
      InstanceId=instance_id,
      WaiterConfig={
        "Delay": 30,
        "MaxAttempts": 120,  # Wait up to 60 minutes
      },
    )
  except Exception as e:
    # Get command output for debugging
    try:
      output = ssm.get_command_invocation(
        CommandId=command_id,
        InstanceId=instance_id,
      )
      stderr = output.get("StandardErrorContent", "")
      context.log.error(f"SSM command failed. stderr: {stderr}")
    except Exception:
      pass
    raise RuntimeError(f"S3 upload command failed: {e}")

  # Verify upload
  head = s3.head_object(Bucket=bucket, Key=s3_key)
  file_size = head["ContentLength"]
  last_modified = head["LastModified"]

  context.log.info(
    f"Database published to S3: {s3_uri} "
    f"(size: {file_size / (1024**3):.2f}GB, modified: {last_modified})"
  )

  # =========================================================================
  # Step 4: Optionally trigger replica refresh
  # =========================================================================
  refresh_result = {"status": "skipped", "reason": "disabled"}

  if config.refresh_replicas:
    context.log.info("Triggering replica fleet refresh...")

    autoscaling = boto3.client("autoscaling", region_name=env.AWS_REGION)
    asg_name = f"robosystems-shared-replicas-{env.ENVIRONMENT}-asg"

    try:
      # Check if ASG exists and has instances
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
            # Start rolling refresh
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
      "master_instance_id": instance_id,
      "published_at": datetime.now(UTC).isoformat(),
      "replica_refresh": refresh_result,
    }
  )
