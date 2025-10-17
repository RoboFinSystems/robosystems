"""
Kuzu Instance Monitor Lambda Function

This Lambda handles infrastructure-level monitoring for Graph instances:
- Health checks for EC2 instances
- Registry cleanup for stale entries
- CloudWatch metrics collection
- Volume registry maintenance
"""

import boto3
import logging
import os
import re
from datetime import datetime, timezone
from typing import Dict, Any

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
ec2 = boto3.client("ec2")
dynamodb = boto3.resource("dynamodb")
cloudwatch = boto3.client("cloudwatch")

# Environment variables
ENVIRONMENT = os.environ.get("ENVIRONMENT", "staging")
INSTANCE_REGISTRY_TABLE = os.environ.get("INSTANCE_REGISTRY_TABLE")
GRAPH_REGISTRY_TABLE = os.environ.get("GRAPH_REGISTRY_TABLE")
VOLUME_REGISTRY_TABLE = os.environ.get("VOLUME_REGISTRY_TABLE")
CLOUDWATCH_NAMESPACE = f"RoboSystemsGraph/{ENVIRONMENT.title()}"

# Configuration
STALE_GRAPH_DAYS = 7  # Days before deleted graphs are removed
STALE_VOLUME_DAYS = 30  # Days before unattached volumes are removed

# Tier capacity mapping based on .github/configs/kuzu.yml
# IMPORTANT: These values must match the kuzu.yml configuration file
# Update both locations if changing capacity values
TIER_CAPACITY_MAP = {
  "standard": 10,  # 10 databases per instance
  "enterprise": 1,  # 1 database per instance (isolated)
  "premium": 1,  # 1 database per instance (max performance)
  "shared": 10,  # 10 shared repositories per instance
  "shared_repository": 10,  # Alias for shared
}

# EC2 instance ID pattern (i-xxxxxxxxxxxxxxxxx or i-xxxxxxxxx)
EC2_INSTANCE_ID_PATTERN = re.compile(r"^i-[0-9a-f]{8,17}$")


def validate_tier_capacity(tier: str) -> int:
  """
  Validate and return capacity for a given tier.

  Args:
    tier: The tier name to validate

  Returns:
    The capacity for the tier

  Raises:
    ValueError: If tier is unknown
  """
  if not tier:
    logger.warning("No tier specified, using 'standard' as default")
    return TIER_CAPACITY_MAP["standard"]

  if tier not in TIER_CAPACITY_MAP:
    logger.error(f"Unknown tier: {tier}. Valid tiers: {list(TIER_CAPACITY_MAP.keys())}")
    # Fall back to standard tier for safety
    return TIER_CAPACITY_MAP["standard"]

  return TIER_CAPACITY_MAP[tier]


def is_valid_ec2_instance_id(instance_id: str) -> bool:
  """Validate if a string is a valid EC2 instance ID format."""
  if not instance_id or not isinstance(instance_id, str):
    return False
  return EC2_INSTANCE_ID_PATTERN.match(instance_id) is not None


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """Main Lambda handler"""

  # Determine action from event
  action = event.get("action", "health_check")

  logger.info(f"Executing action: {action}")

  try:
    if action == "health_check":
      return check_instance_health()
    elif action == "cleanup_graphs":
      return cleanup_stale_graph_entries()
    elif action == "collect_metrics":
      return collect_kuzu_metrics()
    elif action == "cleanup_volumes":
      return cleanup_stale_volume_entries()
    elif action == "full_maintenance":
      # Run all maintenance tasks
      results = {
        "health_check": check_instance_health(),
        "cleanup_graphs": cleanup_stale_graph_entries(),
        "metrics": collect_kuzu_metrics(),
        "cleanup_volumes": cleanup_stale_volume_entries(),
      }
      return {
        "statusCode": 200,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "results": results,
      }
    else:
      return {
        "statusCode": 400,
        "error": f"Unknown action: {action}",
      }
  except Exception as e:
    logger.error(f"Error executing {action}: {e}", exc_info=True)
    return {
      "statusCode": 500,
      "error": str(e),
      "action": action,
    }


def check_instance_health() -> Dict[str, Any]:
  """
  Check health of Graph instances and update registry.

  This function:
  1. Queries all instances from DynamoDB registry
  2. Checks actual EC2 instance states
  3. Updates instance health status in registry
  4. Removes instances that have been terminated
  """
  logger.info("Starting instance health check")

  results = {
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "total_instances": 0,
    "healthy": 0,
    "unhealthy": 0,
    "terminated": 0,
    "removed": 0,
    "invalid_ids": 0,
    "errors": 0,
  }

  try:
    # Get instance registry table
    table = dynamodb.Table(INSTANCE_REGISTRY_TABLE)

    # Scan all instances
    # Process items in batches to avoid unbounded memory growth
    BATCH_SIZE = 100  # Process 100 items at a time
    items = []

    response = table.scan(Limit=BATCH_SIZE)
    current_batch = response.get("Items", [])
    items.extend(current_batch)

    # Handle pagination with batch processing
    while "LastEvaluatedKey" in response and len(items) < 10000:  # Safety limit
      response = table.scan(
        ExclusiveStartKey=response["LastEvaluatedKey"], Limit=BATCH_SIZE
      )
      current_batch = response.get("Items", [])
      items.extend(current_batch)

      # Log progress for large scans
      if len(items) % 500 == 0:
        logger.info(f"Scanned {len(items)} instances so far...")

    results["total_instances"] = len(items)

    if not items:
      logger.info("No instances found in registry")
      return results

    # Collect and validate instance IDs for batch EC2 query
    all_instance_ids = [
      item.get("instance_id") for item in items if item.get("instance_id")
    ]
    valid_instance_ids = []
    invalid_instance_ids = []

    for instance_id in all_instance_ids:
      if is_valid_ec2_instance_id(instance_id):
        valid_instance_ids.append(instance_id)
      else:
        invalid_instance_ids.append(instance_id)
        logger.warning(
          f"Invalid EC2 instance ID format found in registry: {instance_id}"
        )

    if invalid_instance_ids:
      results["invalid_ids"] = len(invalid_instance_ids)
      logger.error(
        f"Found {len(invalid_instance_ids)} invalid instance IDs that will be marked for cleanup: {invalid_instance_ids[:5]}..."
      )  # Log first 5

    # Query EC2 for instance states
    ec2_instances = {}

    # Mark invalid IDs as not_found for cleanup
    for invalid_id in invalid_instance_ids:
      ec2_instances[invalid_id] = "invalid_id"

    # EC2 describe-instances has a limit of 1000 instance IDs per request
    for i in range(0, len(valid_instance_ids), 1000):
      batch_ids = valid_instance_ids[i : i + 1000]
      if not batch_ids:  # Skip if no valid IDs in this batch
        continue
      try:
        response = ec2.describe_instances(InstanceIds=batch_ids)
        for reservation in response.get("Reservations", []):
          for instance in reservation.get("Instances", []):
            ec2_instances[instance["InstanceId"]] = instance["State"]["Name"]
      except ec2.exceptions.ClientError as e:
        if "InvalidInstanceID.NotFound" in str(e):
          # Some instances don't exist, check one by one
          for instance_id in batch_ids:
            try:
              resp = ec2.describe_instances(InstanceIds=[instance_id])
              if resp["Reservations"]:
                inst = resp["Reservations"][0]["Instances"][0]
                ec2_instances[instance_id] = inst["State"]["Name"]
            except Exception:
              ec2_instances[instance_id] = "not_found"
        else:
          raise

    # Update each instance in registry
    current_time = datetime.now(timezone.utc).isoformat()

    for item in items:
      instance_id = item.get("instance_id")
      if not instance_id:
        continue

      try:
        actual_state = ec2_instances.get(instance_id, "not_found")
        current_status = item.get("status", "unknown")

        # Determine new status based on EC2 state
        if actual_state == "running":
          new_status = "healthy"
          results["healthy"] += 1

          # Get tier to determine capacity with validation
          tier = item.get("tier") or item.get("cluster_tier", "standard")
          tier_capacity = validate_tier_capacity(tier)

          # Update health check timestamp while preserving critical fields using if_not_exists
          # This is more efficient than fetching the item first
          table.update_item(
            Key={"instance_id": instance_id},
            UpdateExpression="""
              SET #status = :status,
                  last_health_check = :timestamp,
                  tier = if_not_exists(tier, :tier),
                  total_capacity = if_not_exists(total_capacity, :capacity),
                  available_capacity = if_not_exists(available_capacity, :capacity),
                  max_databases = if_not_exists(max_databases, :capacity)
            """,
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
              ":status": new_status,
              ":timestamp": current_time,
              ":tier": tier,
              ":capacity": tier_capacity,
            },
          )

        elif actual_state in ["terminated", "shutting-down", "not_found", "invalid_id"]:
          results["terminated"] += 1

          # Also update volume registry for any attached volumes
          if VOLUME_REGISTRY_TABLE:
            try:
              volume_table = dynamodb.Table(VOLUME_REGISTRY_TABLE)
              # Find volumes attached to this instance
              volume_response = volume_table.scan(
                FilterExpression="instance_id = :instance_id",
                ExpressionAttributeValues={":instance_id": instance_id},
              )

              for volume in volume_response.get("Items", []):
                volume_id = volume.get("volume_id")
                databases = volume.get("databases", [])

                # Mark volume as available but preserve databases
                volume_table.update_item(
                  Key={"volume_id": volume_id},
                  UpdateExpression="SET #status = :status, instance_id = :unattached, last_detached = :timestamp, databases = :databases",
                  ExpressionAttributeNames={"#status": "status"},
                  ExpressionAttributeValues={
                    ":status": "available",
                    ":unattached": "unattached",
                    ":timestamp": current_time,
                    ":databases": databases,
                  },
                )
                logger.info(
                  f"Updated volume {volume_id} to available after instance {instance_id} termination"
                )
            except Exception as e:
              logger.warning(
                f"Failed to update volume registry for instance {instance_id}: {e}"
              )

          # Remove terminated or invalid instances from registry
          table.delete_item(Key={"instance_id": instance_id})
          results["removed"] += 1

          if actual_state == "invalid_id":
            logger.warning(
              f"Removed invalid instance ID from registry: {instance_id} "
              f"(not a valid EC2 instance ID format)"
            )
          else:
            logger.info(
              f"Removed terminated instance {instance_id} "
              f"(state: {actual_state}, was: {current_status})"
            )

        else:
          # Instance is in transitional state
          new_status = "unhealthy"
          results["unhealthy"] += 1

          # Get tier to determine capacity with validation
          tier = item.get("tier") or item.get("cluster_tier", "standard")
          tier_capacity = validate_tier_capacity(tier)

          # Update status but keep in registry, preserving all fields using if_not_exists
          # This is more efficient than fetching the item first
          table.update_item(
            Key={"instance_id": instance_id},
            UpdateExpression="""
              SET #status = :status,
                  last_health_check = :timestamp,
                  tier = if_not_exists(tier, :tier),
                  total_capacity = if_not_exists(total_capacity, :capacity),
                  available_capacity = if_not_exists(available_capacity, :capacity),
                  max_databases = if_not_exists(max_databases, :capacity)
            """,
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
              ":status": new_status,
              ":timestamp": current_time,
              ":tier": tier,
              ":capacity": tier_capacity,
            },
          )

          logger.warning(
            f"Instance {instance_id} is unhealthy "
            f"(state: {actual_state}, was: {current_status})"
          )

      except Exception as e:
        logger.error(f"Error processing instance {instance_id}: {e}")
        results["errors"] += 1

    logger.info(
      f"Instance health check completed: "
      f"{results['healthy']} healthy, "
      f"{results['unhealthy']} unhealthy, "
      f"{results['removed']} removed"
    )

  except Exception as e:
    logger.error(f"Failed to check instance health: {e}", exc_info=True)
    results["error"] = str(e)

  return results


def cleanup_stale_graph_entries() -> Dict[str, int]:
  """
  Clean up stale entries from graph registry.

  Removes:
  - Entries marked as deleted older than 7 days
  - Entries with missing instance_id references
  """
  logger.info("Starting graph registry cleanup")

  results = {
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "removed_count": 0,
    "errors": 0,
  }

  try:
    # Get registries
    graph_table = dynamodb.Table(GRAPH_REGISTRY_TABLE)
    instance_table = dynamodb.Table(INSTANCE_REGISTRY_TABLE)

    # Get all graph entries
    response = graph_table.scan()
    items = response.get("Items", [])

    # Handle pagination
    while "LastEvaluatedKey" in response:
      response = graph_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
      items.extend(response.get("Items", []))

    # Get all valid instance IDs
    instance_response = instance_table.scan(ProjectionExpression="instance_id")
    valid_instances = {
      item["instance_id"] for item in instance_response.get("Items", [])
    }

    for item in items:
      graph_id = item.get("graph_id")
      status = item.get("status")
      instance_id = item.get("instance_id")
      deleted_at = item.get("deleted_at")

      should_remove = False

      # Check if entry should be removed
      if status == "deleted" and deleted_at:
        # Remove if deleted more than 7 days ago
        try:
          deleted_time = datetime.fromisoformat(deleted_at.replace("Z", "+00:00"))
          age_days = (datetime.now(timezone.utc) - deleted_time).days
          if age_days > STALE_GRAPH_DAYS:
            should_remove = True
            logger.info(f"Removing graph {graph_id}: deleted {age_days} days ago")
        except Exception:
          pass

      # Remove if instance doesn't exist
      if instance_id and instance_id not in valid_instances:
        should_remove = True
        logger.info(f"Removing graph {graph_id}: instance {instance_id} doesn't exist")

      if should_remove:
        try:
          graph_table.delete_item(Key={"graph_id": graph_id})
          results["removed_count"] += 1
        except Exception as e:
          logger.error(f"Failed to remove graph {graph_id}: {e}")
          results["errors"] += 1

    logger.info(
      f"Graph registry cleanup completed: {results['removed_count']} entries removed"
    )

  except Exception as e:
    logger.error(f"Failed to cleanup graph registry: {e}", exc_info=True)
    results["error"] = str(e)

  return results


def collect_kuzu_metrics() -> Dict[str, int]:
  """
  Collect and publish Kuzu cluster capacity metrics to CloudWatch.

  This function:
  1. Queries instance and graph registries
  2. Calculates capacity, utilization, and health metrics
  3. Publishes metrics to CloudWatch for monitoring and auto-scaling
  """
  logger.info("Starting Kuzu metrics collection")

  results = {
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "metrics_published": 0,
    "errors": 0,
  }

  try:
    # Get registry tables
    graph_table = dynamodb.Table(GRAPH_REGISTRY_TABLE)
    instance_table = dynamodb.Table(INSTANCE_REGISTRY_TABLE)

    # Get all healthy instances
    instances_response = instance_table.scan(
      FilterExpression="#s = :status",
      ExpressionAttributeNames={"#s": "status"},
      ExpressionAttributeValues={":status": "healthy"},
    )
    instances = instances_response.get("Items", [])

    # Handle pagination
    while "LastEvaluatedKey" in instances_response:
      instances_response = instance_table.scan(
        FilterExpression="#s = :status",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":status": "healthy"},
        ExclusiveStartKey=instances_response["LastEvaluatedKey"],
      )
      instances.extend(instances_response.get("Items", []))

    # Calculate instance metrics
    total_capacity = 0
    total_used = 0
    total_available = 0
    instance_age_buckets = {"new": 0, "stabilizing": 0, "stable": 0}
    utilization_buckets = {}
    tier_counts = {"standard": 0, "enterprise": 0, "premium": 0, "shared_repository": 0}
    metrics = []

    # Default max databases if not specified
    default_max_dbs = 50  # Standard tier default

    for instance in instances:
      instance_id = instance.get("instance_id")
      # Use 'tier' field, fallback to 'cluster_tier' for compatibility
      tier = instance.get("tier") or instance.get("cluster_tier", "standard")
      # Use 'total_capacity' field, fallback to 'max_databases' for compatibility
      max_dbs = int(
        instance.get("total_capacity") or instance.get("max_databases", default_max_dbs)
      )
      used_dbs = int(instance.get("database_count", 0))
      # Use 'available_capacity' if present, otherwise calculate
      available_dbs = int(instance.get("available_capacity", max_dbs - used_dbs))
      created_at = instance.get("created_at", "")

      # Track tier distribution
      if tier in tier_counts:
        tier_counts[tier] += 1

      # Calculate instance age
      age_hours = 0
      if created_at:
        try:
          created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
          age = datetime.now(timezone.utc) - created_dt
          age_hours = age.total_seconds() / 3600
        except Exception:
          pass

      # Categorize by age
      if age_hours < 0.25:  # 15 minutes
        instance_age_buckets["new"] += 1
      elif age_hours < 1:  # 1 hour
        instance_age_buckets["stabilizing"] += 1
      else:
        instance_age_buckets["stable"] += 1

      # Calculate utilization
      utilization = (used_dbs / max_dbs * 100) if max_dbs > 0 else 0
      utilization_bracket = f"{int(utilization / 10) * 10}%"
      utilization_buckets[utilization_bracket] = (
        utilization_buckets.get(utilization_bracket, 0) + 1
      )

      # Accumulate totals
      total_capacity += max_dbs
      total_used += used_dbs
      total_available += available_dbs

      # Per-instance metrics
      metrics.extend(
        [
          {
            "MetricName": "InstanceDatabaseCount",
            "Value": used_dbs,
            "Unit": "Count",
            "Dimensions": [
              {"Name": "InstanceId", "Value": instance_id},
              {"Name": "Environment", "Value": ENVIRONMENT},
              {"Name": "ClusterTier", "Value": tier},
            ],
          },
          {
            "MetricName": "InstanceUtilization",
            "Value": utilization,
            "Unit": "Percent",
            "Dimensions": [
              {"Name": "InstanceId", "Value": instance_id},
              {"Name": "Environment", "Value": ENVIRONMENT},
              {"Name": "ClusterTier", "Value": tier},
            ],
          },
          {
            "MetricName": "InstanceAvailableSlots",
            "Value": available_dbs,
            "Unit": "Count",
            "Dimensions": [
              {"Name": "InstanceId", "Value": instance_id},
              {"Name": "Environment", "Value": ENVIRONMENT},
              {"Name": "ClusterTier", "Value": tier},
            ],
          },
        ]
      )

    # Count active databases (not deleted)
    try:
      active_db_response = graph_table.scan(
        FilterExpression="#s <> :deleted AND #s <> :pending_deletion",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
          ":deleted": "deleted",
          ":pending_deletion": "pending_deletion",
        },
        Select="COUNT",
      )
      total_active = active_db_response.get("Count", total_used)
    except Exception:
      total_active = total_used

    # Overall cluster metrics
    if total_capacity > 0:
      available_percent = (total_available / total_capacity) * 100
      used_percent = (total_used / total_capacity) * 100
      # active_percent = (total_active / total_capacity) * 100  # Unused

      metrics.extend(
        [
          {
            "MetricName": "ClusterTotalCapacity",
            "Value": total_capacity,
            "Unit": "Count",
            "Dimensions": [{"Name": "Environment", "Value": ENVIRONMENT}],
          },
          {
            "MetricName": "ClusterTotalUsed",
            "Value": total_used,
            "Unit": "Count",
            "Dimensions": [{"Name": "Environment", "Value": ENVIRONMENT}],
          },
          {
            "MetricName": "ClusterTotalActive",
            "Value": total_active,
            "Unit": "Count",
            "Dimensions": [{"Name": "Environment", "Value": ENVIRONMENT}],
          },
          {
            "MetricName": "ClusterAvailableCapacityPercent",
            "Value": available_percent,
            "Unit": "Percent",
            "Dimensions": [{"Name": "Environment", "Value": ENVIRONMENT}],
          },
          {
            "MetricName": "ClusterUsedCapacityPercent",
            "Value": used_percent,
            "Unit": "Percent",
            "Dimensions": [{"Name": "Environment", "Value": ENVIRONMENT}],
          },
          {
            "MetricName": "ClusterInstanceCount",
            "Value": len(instances),
            "Unit": "Count",
            "Dimensions": [{"Name": "Environment", "Value": ENVIRONMENT}],
          },
        ]
      )

      # Instance age distribution
      for age_type, count in instance_age_buckets.items():
        metrics.append(
          {
            "MetricName": "InstancesByAge",
            "Value": count,
            "Unit": "Count",
            "Dimensions": [
              {"Name": "Environment", "Value": ENVIRONMENT},
              {"Name": "AgeCategory", "Value": age_type},
            ],
          }
        )

      # Tier distribution
      for tier, count in tier_counts.items():
        if count > 0:
          metrics.append(
            {
              "MetricName": "InstancesByTier",
              "Value": count,
              "Unit": "Count",
              "Dimensions": [
                {"Name": "Environment", "Value": ENVIRONMENT},
                {"Name": "ClusterTier", "Value": tier},
              ],
            }
          )

    # Publish metrics in batches (CloudWatch limit is 20 metrics per call)
    for i in range(0, len(metrics), 20):
      batch = metrics[i : i + 20]
      cloudwatch.put_metric_data(
        Namespace=CLOUDWATCH_NAMESPACE,
        MetricData=batch,
      )
      results["metrics_published"] += len(batch)

    logger.info(
      f"Published {results['metrics_published']} metrics to CloudWatch namespace {CLOUDWATCH_NAMESPACE}"
    )

  except Exception as e:
    logger.error(f"Failed to collect Kuzu metrics: {e}", exc_info=True)
    results["error"] = str(e)

  return results


def cleanup_stale_volume_entries() -> Dict[str, int]:
  """
  Clean up stale entries from volume registry.

  Removes or updates:
  - Volumes stuck in 'attaching' state to non-existent instances
  - Volumes with missing instance references
  - Old unattached volumes (older than 30 days)
  """
  logger.info("Starting volume registry cleanup")

  results = {
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "updated_count": 0,
    "removed_count": 0,
    "errors": 0,
  }

  try:
    # Get registries
    volume_table = dynamodb.Table(VOLUME_REGISTRY_TABLE)
    instance_table = dynamodb.Table(INSTANCE_REGISTRY_TABLE)

    # Get all volume entries
    response = volume_table.scan()
    items = response.get("Items", [])

    # Handle pagination
    while "LastEvaluatedKey" in response:
      response = volume_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
      items.extend(response.get("Items", []))

    # Get all valid instance IDs
    instance_response = instance_table.scan(ProjectionExpression="instance_id")
    valid_instances = {
      item["instance_id"] for item in instance_response.get("Items", [])
    }

    for item in items:
      volume_id = item.get("volume_id")
      status = item.get("status")
      instance_id = item.get("instance_id")
      created_at = item.get("created_at")

      should_update = False
      should_remove = False
      new_status = None

      # Check volumes stuck in 'attaching' state to non-existent instances
      if status == "attaching" and instance_id and instance_id != "unattached":
        if instance_id not in valid_instances:
          should_update = True
          new_status = "failed"
          logger.info(
            f"Volume {volume_id} stuck attaching to non-existent instance {instance_id}"
          )

      # Check old unattached volumes
      if instance_id == "unattached" and status == "available" and created_at:
        try:
          created_time = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
          age_days = (datetime.now(timezone.utc) - created_time).days
          if age_days > STALE_VOLUME_DAYS:
            should_remove = True
            logger.info(
              f"Removing old unattached volume {volume_id}: {age_days} days old"
            )
        except Exception:
          pass

      # Update volume status
      if should_update and new_status:
        try:
          volume_table.update_item(
            Key={"volume_id": volume_id},
            UpdateExpression="SET #status = :status, #instance = :instance, updated_at = :timestamp",
            ExpressionAttributeNames={
              "#status": "status",
              "#instance": "instance_id",
            },
            ExpressionAttributeValues={
              ":status": new_status,
              ":instance": "unattached",
              ":timestamp": datetime.now(timezone.utc).isoformat(),
            },
          )
          results["updated_count"] += 1
          logger.info(f"Updated volume {volume_id} status to {new_status}")
        except Exception as e:
          logger.error(f"Failed to update volume {volume_id}: {e}")
          results["errors"] += 1

      # Remove volume entry
      if should_remove:
        try:
          volume_table.delete_item(Key={"volume_id": volume_id})
          results["removed_count"] += 1
        except Exception as e:
          logger.error(f"Failed to remove volume {volume_id}: {e}")
          results["errors"] += 1

    logger.info(
      f"Volume registry cleanup completed: "
      f"{results['updated_count']} updated, {results['removed_count']} removed"
    )

  except Exception as e:
    logger.error(f"Failed to cleanup volume registry: {e}", exc_info=True)
    results["error"] = str(e)

  return results
