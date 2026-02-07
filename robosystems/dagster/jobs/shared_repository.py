"""Dagster jobs for shared repository management.

These jobs handle S3-based replica data distribution:
- Upload shared master's database to S3 for replicas
- Trigger rolling instance refresh of replica fleet

Replicas use LadybugDB S3 ATTACH to connect directly to S3-hosted
.lbug files using the httpfs extension.

These jobs are typically triggered after SEC materialization completes,
or can be run manually for ad-hoc sync/refresh operations.
"""

from typing import Any

import boto3
import httpx
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
# Health Check Operations
# ============================================================================


@op
def verify_shared_master_health(
  context: OpExecutionContext, config: S3SyncConfig
) -> dict[str, Any]:
  """Verify shared master is healthy before snapshotting.

  Performs both DynamoDB registry check and actual HTTP health check
  to ensure the instance is truly healthy and serving requests.
  """
  dynamodb = boto3.client("dynamodb", region_name=env.AWS_REGION)

  # Query instance registry for shared master
  table_name = env.INSTANCE_REGISTRY_TABLE
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
  private_ip = master.get("private_ip", {}).get("S")

  if not private_ip:
    raise Exception(
      f"Shared master {instance_id} has no private_ip in registry. "
      "Instance may not be fully initialized."
    )

  context.log.info(f"Found shared master: {instance_id} at {private_ip}")

  # Perform actual HTTP health check
  health_url = f"http://{private_ip}:8001/health"
  context.log.info(f"Performing health check: {health_url}")

  try:
    with httpx.Client(timeout=30.0) as client:
      response = client.get(health_url)
      response.raise_for_status()
      health_data = response.json()

      context.log.info(f"Health check passed: {health_data}")

      return {
        "instance_id": instance_id,
        "private_ip": private_ip,
        "health_status": "healthy",
        "health_response": health_data,
      }

  except httpx.TimeoutException:
    raise Exception(
      f"Health check timed out for shared master {instance_id} at {health_url}. "
      "Instance may be overloaded or unresponsive."
    )
  except httpx.HTTPStatusError as e:
    raise Exception(
      f"Health check failed for shared master {instance_id}: {e.response.status_code}. "
      f"Response: {e.response.text[:500]}"
    )
  except Exception as e:
    raise Exception(
      f"Health check failed for shared master {instance_id} at {health_url}: {e}"
    )


# ============================================================================
# S3 Sync Operations
# ============================================================================


@op
def checkpoint_shared_database(
  context: OpExecutionContext,
  health_check: dict[str, Any],
  config: S3SyncConfig,
) -> dict[str, Any]:
  """Execute CHECKPOINT on the shared master to flush WAL.

  This ensures the database file is consistent before uploading to S3.
  Replicas will read this file via S3 ATTACH.
  """
  private_ip = health_check["private_ip"]
  graph_id = config.graph_id

  context.log.info(f"Executing CHECKPOINT on shared master {private_ip} for {graph_id}")

  # Execute CHECKPOINT via Graph API
  checkpoint_url = f"http://{private_ip}:8001/databases/{graph_id}/query"

  try:
    with httpx.Client(timeout=120.0) as client:
      response = client.post(
        checkpoint_url,
        json={"query": "CHECKPOINT"},
      )
      response.raise_for_status()
      result = response.json()

      context.log.info(f"CHECKPOINT completed: {result}")

      return {
        "graph_id": graph_id,
        "private_ip": private_ip,
        "checkpoint_status": "success",
      }

  except httpx.TimeoutException:
    raise Exception(
      f"CHECKPOINT timed out for {graph_id} on {private_ip}. "
      "Database may be under heavy load."
    )
  except httpx.HTTPStatusError as e:
    raise Exception(
      f"CHECKPOINT failed for {graph_id}: {e.response.status_code}. "
      f"Response: {e.response.text[:500]}"
    )


@op
def upload_database_to_s3(
  context: OpExecutionContext,
  checkpoint_result: dict[str, Any],
  config: S3SyncConfig,
) -> dict[str, Any]:
  """Upload shared database to S3 for replicas.

  Uses SSM to run an upload command on the shared master instance.
  The database file is uploaded directly to S3 where replicas can
  ATTACH to it via httpfs.
  """

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

  # Get shared master instance ID from DynamoDB
  dynamodb = boto3.client("dynamodb", region_name=env.AWS_REGION)
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
    raise Exception("No healthy shared_master found for S3 upload")

  instance_id = items[0]["instance_id"]["S"]
  context.log.info(f"Using shared master instance: {instance_id}")

  # Use SSM to run the upload command on the shared master
  ssm = boto3.client("ssm", region_name=env.AWS_REGION)

  # Build the upload command
  # The database is at /mnt/ladybug-data/databases/lbug-dbs/{graph_id}.lbug
  upload_command = f"""
#!/bin/bash
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

# Upload to S3
aws s3 cp "$DB_PATH" "$S3_URI" --region {env.AWS_REGION}

echo "✅ Upload complete"
"""

  # Send SSM command
  context.log.info(f"Sending SSM command to instance {instance_id}")

  command_response = ssm.send_command(
    InstanceIds=[instance_id],
    DocumentName="AWS-RunShellScript",
    Parameters={"commands": [upload_command]},
    TimeoutSeconds=3600,  # 1 hour timeout for large uploads
    Comment=f"Upload {graph_id} database to S3 - Dagster run {context.run_id[:8]}",
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
        "Delay": 30,  # Check every 30 seconds
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
    except Exception as fetch_err:
      context.log.warning(
        f"Failed to fetch SSM command output for debugging: {fetch_err}"
      )
    raise Exception(f"S3 upload command failed: {e}")

  # Get command output
  output = ssm.get_command_invocation(
    CommandId=command_id,
    InstanceId=instance_id,
  )

  stdout = output.get("StandardOutputContent", "")
  context.log.info(f"SSM command output:\n{stdout}")

  # Verify upload by checking S3
  s3 = boto3.client("s3", region_name=env.AWS_REGION)
  try:
    head = s3.head_object(Bucket=bucket, Key=s3_key)
    file_size = head["ContentLength"]
    last_modified = head["LastModified"]

    context.log.info(
      f"✅ Database uploaded to S3: {s3_uri} "
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

  except Exception as e:
    raise Exception(f"Failed to verify S3 upload: {e}")


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

  Full pipeline:
  1. Verify shared master is healthy (HTTP health check)
  2. Execute CHECKPOINT to flush WAL
  3. Upload database file to S3 via SSM
  4. Trigger rolling instance refresh (skips if refresh already in progress)

  Replicas use S3 ATTACH to connect directly to the S3-hosted database
  via LadybugDB's httpfs extension.

  This job is typically run after SEC materialization completes.
  """
  health_check = verify_shared_master_health()
  checkpoint_result = checkpoint_shared_database(health_check)
  s3_upload = upload_database_to_s3(checkpoint_result)
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
  health_check = verify_shared_master_health()
  checkpoint_result = checkpoint_shared_database(health_check)
  upload_database_to_s3(checkpoint_result)


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
