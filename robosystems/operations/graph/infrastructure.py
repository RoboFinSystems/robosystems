"""Graph infrastructure operations.

This module provides infrastructure-level operations for Graph instances
(LadybugDB and Neo4j), migrated from Lambda functions:
- Instance health checks and registry maintenance
- CloudWatch metrics collection
- Volume registry cleanup

These operations interact with:
- DynamoDB registries (instances, graphs, volumes)
- EC2 instances and EBS volumes
- CloudWatch metrics

Usage:
    from robosystems.operations.graph.infrastructure import InstanceMonitor

    # Check instance health
    monitor = InstanceMonitor()
    results = await monitor.check_instance_health()
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import boto3
from botocore.exceptions import ClientError

from robosystems.config import env
from robosystems.logger import logger

if TYPE_CHECKING:
  from mypy_boto3_cloudwatch import CloudWatchClient  # type: ignore[import-not-found]
  from mypy_boto3_dynamodb import (
    DynamoDBServiceResource,  # type: ignore[import-not-found]
  )
  from mypy_boto3_ec2 import EC2Client  # type: ignore[import-not-found]


# Configuration
# Note: CLOUDWATCH_NAMESPACE is environment-specific: RoboSystems/Graph/{environment}
# The namespace includes the environment (prod/staging) instead of using an Environment dimension
STALE_GRAPH_DAYS = 7
STALE_VOLUME_DAYS = 30

# Tier capacity mapping (matches .github/configs/graph.yml)
TIER_CAPACITY_MAP = {
  "ladybug-standard": 10,
  "ladybug-large": 1,
  "ladybug-xlarge": 1,
  "ladybug-shared": 10,
  "neo4j-community-large": 1,
  "neo4j-enterprise-xlarge": 1,
}

# EC2 instance ID pattern
EC2_INSTANCE_ID_PATTERN = re.compile(r"^i-[0-9a-f]{8,17}$")


@dataclass
class HealthCheckResult:
  """Result of an instance health check."""

  timestamp: str
  total_instances: int = 0
  healthy: int = 0
  unhealthy: int = 0
  terminated: int = 0
  removed: int = 0
  invalid_ids: int = 0
  errors: int = 0
  error_message: str | None = None


@dataclass
class CleanupResult:
  """Result of a cleanup operation."""

  timestamp: str
  removed_count: int = 0
  updated_count: int = 0
  errors: int = 0
  error_message: str | None = None


@dataclass
class MetricsResult:
  """Result of metrics collection."""

  timestamp: str
  metrics_published: int = 0
  errors: int = 0
  error_message: str | None = None


def _get_tier_capacity(tier: str) -> int:
  """Get capacity for a given tier with validation."""
  if not tier:
    logger.warning("No tier specified, using 'ladybug-standard' as default")
    return TIER_CAPACITY_MAP["ladybug-standard"]

  if tier not in TIER_CAPACITY_MAP:
    logger.error(f"Unknown tier: {tier}. Valid tiers: {list(TIER_CAPACITY_MAP.keys())}")
    return TIER_CAPACITY_MAP["ladybug-standard"]

  return TIER_CAPACITY_MAP[tier]


def _is_valid_ec2_instance_id(instance_id: str) -> bool:
  """Validate if a string is a valid EC2 instance ID format."""
  if not instance_id or not isinstance(instance_id, str):
    return False
  return EC2_INSTANCE_ID_PATTERN.match(instance_id) is not None


class InstanceMonitor:
  """Monitor and maintain Graph instance infrastructure.

  This class provides operations for:
  - Health checking EC2 instances
  - Updating DynamoDB registry
  - Collecting CloudWatch metrics
  - Cleaning up stale registry entries
  """

  def __init__(
    self,
    instance_registry_table: str | None = None,
    graph_registry_table: str | None = None,
    volume_registry_table: str | None = None,
    environment: str | None = None,
  ):
    """Initialize the instance monitor.

    Args:
        instance_registry_table: DynamoDB table for instance registry
        graph_registry_table: DynamoDB table for graph registry
        volume_registry_table: DynamoDB table for volume registry
        environment: Environment name (prod, staging)
    """
    self.environment = environment or env.ENVIRONMENT

    # Table names from environment or explicit config
    self.instance_registry_table = (
      instance_registry_table
      or f"robosystems-graph-{self.environment}-instance-registry"
    )
    self.graph_registry_table = (
      graph_registry_table or f"robosystems-graph-{self.environment}-graph-registry"
    )
    self.volume_registry_table = (
      volume_registry_table or f"robosystems-graph-{self.environment}-volume-registry"
    )

    # Lazy-loaded AWS clients
    self._ec2: EC2Client | None = None
    self._dynamodb: DynamoDBServiceResource | None = None
    self._cloudwatch: CloudWatchClient | None = None

  @property
  def ec2(self) -> EC2Client:
    """Get EC2 client (lazy-loaded)."""
    if self._ec2 is None:
      self._ec2 = boto3.client("ec2")
    return self._ec2

  @property
  def dynamodb(self) -> DynamoDBServiceResource:
    """Get DynamoDB resource (lazy-loaded)."""
    if self._dynamodb is None:
      self._dynamodb = boto3.resource("dynamodb")
    return self._dynamodb

  @property
  def cloudwatch(self) -> CloudWatchClient:
    """Get CloudWatch client (lazy-loaded)."""
    if self._cloudwatch is None:
      self._cloudwatch = boto3.client("cloudwatch")
    return self._cloudwatch

  def check_instance_health(self) -> HealthCheckResult:
    """Check health of Graph instances and update registry.

    This method:
    1. Queries all instances from DynamoDB registry
    2. Checks actual EC2 instance states
    3. Updates instance health status in registry
    4. Removes instances that have been terminated

    Returns:
        HealthCheckResult with counts of healthy, unhealthy, and removed instances
    """
    logger.info("Starting instance health check")

    result = HealthCheckResult(
      timestamp=datetime.now(UTC).isoformat(),
    )

    try:
      table = self.dynamodb.Table(self.instance_registry_table)

      # Scan all instances with pagination
      items: list[dict[str, Any]] = []
      response = table.scan(Limit=100)
      items.extend(response.get("Items", []))

      while "LastEvaluatedKey" in response and len(items) < 10000:
        response = table.scan(
          ExclusiveStartKey=response["LastEvaluatedKey"],
          Limit=100,
        )
        items.extend(response.get("Items", []))

        if len(items) % 500 == 0:
          logger.info(f"Scanned {len(items)} instances so far...")

      result.total_instances = len(items)

      if not items:
        logger.info("No instances found in registry")
        return result

      # Validate instance IDs
      valid_instance_ids = []
      invalid_instance_ids = []

      for item in items:
        instance_id = item.get("instance_id")
        if instance_id:
          if _is_valid_ec2_instance_id(instance_id):
            valid_instance_ids.append(instance_id)
          else:
            invalid_instance_ids.append(instance_id)
            logger.warning(f"Invalid EC2 instance ID format in registry: {instance_id}")

      result.invalid_ids = len(invalid_instance_ids)

      # Query EC2 for instance states
      ec2_instances: dict[str, str] = {}

      # Mark invalid IDs for cleanup
      for invalid_id in invalid_instance_ids:
        ec2_instances[invalid_id] = "invalid_id"

      # Query EC2 in batches of 1000
      for i in range(0, len(valid_instance_ids), 1000):
        batch_ids = valid_instance_ids[i : i + 1000]
        if not batch_ids:
          continue

        try:
          response = self.ec2.describe_instances(InstanceIds=batch_ids)
          for reservation in response.get("Reservations", []):
            for instance in reservation.get("Instances", []):
              ec2_instances[instance["InstanceId"]] = instance["State"]["Name"]
        except ClientError as e:
          if "InvalidInstanceID.NotFound" in str(e):
            # Check instances one by one
            for instance_id in batch_ids:
              try:
                resp = self.ec2.describe_instances(InstanceIds=[instance_id])
                if resp["Reservations"]:
                  inst = resp["Reservations"][0]["Instances"][0]
                  ec2_instances[instance_id] = inst["State"]["Name"]
              except Exception:
                ec2_instances[instance_id] = "not_found"
          else:
            raise

      # Update each instance in registry
      current_time = datetime.now(UTC).isoformat()

      for item in items:
        instance_id = item.get("instance_id")
        if not instance_id:
          continue

        try:
          actual_state = ec2_instances.get(instance_id, "not_found")
          current_status = item.get("status", "unknown")
          tier = item.get("tier") or item.get("cluster_tier", "ladybug-standard")
          tier_capacity = _get_tier_capacity(tier)

          if actual_state == "running":
            result.healthy += 1
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
                ":status": "healthy",
                ":timestamp": current_time,
                ":tier": tier,
                ":capacity": tier_capacity,
              },
            )

          elif actual_state in [
            "terminated",
            "shutting-down",
            "not_found",
            "invalid_id",
          ]:
            result.terminated += 1

            # Update volume registry for attached volumes
            self._update_volumes_for_terminated_instance(instance_id, current_time)

            # Remove from registry
            table.delete_item(Key={"instance_id": instance_id})
            result.removed += 1

            if actual_state == "invalid_id":
              logger.warning(f"Removed invalid instance ID: {instance_id}")
            else:
              logger.info(
                f"Removed terminated instance {instance_id} "
                f"(state: {actual_state}, was: {current_status})"
              )

          else:
            # Transitional state
            result.unhealthy += 1
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
                ":status": "unhealthy",
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
          result.errors += 1

      logger.info(
        f"Instance health check completed: "
        f"{result.healthy} healthy, "
        f"{result.unhealthy} unhealthy, "
        f"{result.removed} removed"
      )

    except Exception as e:
      logger.error(f"Failed to check instance health: {e}", exc_info=True)
      result.error_message = str(e)

    return result

  def _update_volumes_for_terminated_instance(
    self, instance_id: str, current_time: str
  ) -> None:
    """Update volume registry when an instance is terminated."""
    try:
      volume_table = self.dynamodb.Table(self.volume_registry_table)
      response = volume_table.scan(
        FilterExpression="instance_id = :instance_id",
        ExpressionAttributeValues={":instance_id": instance_id},
      )

      for volume in response.get("Items", []):
        volume_id = volume.get("volume_id")
        databases = volume.get("databases", [])

        volume_table.update_item(
          Key={"volume_id": volume_id},
          UpdateExpression="""
                        SET #status = :status,
                            instance_id = :unattached,
                            last_detached = :timestamp,
                            databases = :databases
                    """,
          ExpressionAttributeNames={"#status": "status"},
          ExpressionAttributeValues={
            ":status": "available",
            ":unattached": "unattached",
            ":timestamp": current_time,
            ":databases": databases,
          },
        )
        logger.info(
          f"Updated volume {volume_id} to available after instance termination"
        )

    except Exception as e:
      logger.warning(f"Failed to update volumes for instance {instance_id}: {e}")

  def cleanup_stale_graphs(self) -> CleanupResult:
    """Clean up stale entries from graph registry.

    Removes:
    - Entries marked as deleted older than 7 days
    - Entries with missing instance_id references

    Returns:
        CleanupResult with count of removed entries
    """
    logger.info("Starting graph registry cleanup")

    result = CleanupResult(
      timestamp=datetime.now(UTC).isoformat(),
    )

    try:
      graph_table = self.dynamodb.Table(self.graph_registry_table)
      instance_table = self.dynamodb.Table(self.instance_registry_table)

      # Get all graph entries
      response = graph_table.scan()
      items = response.get("Items", [])

      while "LastEvaluatedKey" in response:
        response = graph_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))

      # Get valid instance IDs
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

        # Remove if deleted more than 7 days ago
        if status == "deleted" and deleted_at:
          try:
            deleted_time = datetime.fromisoformat(deleted_at.replace("Z", "+00:00"))
            age_days = (datetime.now(UTC) - deleted_time).days
            if age_days > STALE_GRAPH_DAYS:
              should_remove = True
              logger.info(f"Removing graph {graph_id}: deleted {age_days} days ago")
          except Exception:
            pass

        # Remove if instance doesn't exist
        if instance_id and instance_id not in valid_instances:
          should_remove = True
          logger.info(
            f"Removing graph {graph_id}: instance {instance_id} doesn't exist"
          )

        if should_remove:
          try:
            graph_table.delete_item(Key={"graph_id": graph_id})
            result.removed_count += 1
          except Exception as e:
            logger.error(f"Failed to remove graph {graph_id}: {e}")
            result.errors += 1

      logger.info(
        f"Graph registry cleanup completed: {result.removed_count} entries removed"
      )

    except Exception as e:
      logger.error(f"Failed to cleanup graph registry: {e}", exc_info=True)
      result.error_message = str(e)

    return result

  def cleanup_stale_volumes(self) -> CleanupResult:
    """Clean up stale entries from volume registry.

    Removes or updates:
    - Volumes stuck in 'attaching' state to non-existent instances
    - Volumes with missing instance references
    - Old unattached volumes (older than 30 days)

    Returns:
        CleanupResult with counts of updated and removed entries
    """
    logger.info("Starting volume registry cleanup")

    result = CleanupResult(
      timestamp=datetime.now(UTC).isoformat(),
    )

    try:
      volume_table = self.dynamodb.Table(self.volume_registry_table)
      instance_table = self.dynamodb.Table(self.instance_registry_table)

      # Get all volume entries
      response = volume_table.scan()
      items = response.get("Items", [])

      while "LastEvaluatedKey" in response:
        response = volume_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))

      # Get valid instance IDs
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

        # Check volumes stuck attaching to non-existent instances
        if status == "attaching" and instance_id and instance_id != "unattached":
          if instance_id not in valid_instances:
            should_update = True
            new_status = "failed"
            logger.info(
              f"Volume {volume_id} stuck attaching to "
              f"non-existent instance {instance_id}"
            )

        # Check old unattached volumes
        if instance_id == "unattached" and status == "available" and created_at:
          try:
            created_time = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            age_days = (datetime.now(UTC) - created_time).days
            if age_days > STALE_VOLUME_DAYS:
              should_remove = True
              logger.info(
                f"Removing old unattached volume {volume_id}: {age_days} days old"
              )
          except Exception:
            pass

        if should_update and new_status:
          try:
            volume_table.update_item(
              Key={"volume_id": volume_id},
              UpdateExpression="""
                                SET #status = :status,
                                    #instance = :instance,
                                    updated_at = :timestamp
                            """,
              ExpressionAttributeNames={
                "#status": "status",
                "#instance": "instance_id",
              },
              ExpressionAttributeValues={
                ":status": new_status,
                ":instance": "unattached",
                ":timestamp": datetime.now(UTC).isoformat(),
              },
            )
            result.updated_count += 1
            logger.info(f"Updated volume {volume_id} status to {new_status}")
          except Exception as e:
            logger.error(f"Failed to update volume {volume_id}: {e}")
            result.errors += 1

        if should_remove:
          try:
            volume_table.delete_item(Key={"volume_id": volume_id})
            result.removed_count += 1
          except Exception as e:
            logger.error(f"Failed to remove volume {volume_id}: {e}")
            result.errors += 1

      logger.info(
        f"Volume registry cleanup completed: "
        f"{result.updated_count} updated, {result.removed_count} removed"
      )

    except Exception as e:
      logger.error(f"Failed to cleanup volume registry: {e}", exc_info=True)
      result.error_message = str(e)

    return result

  def collect_metrics(self) -> MetricsResult:
    """Collect and publish Graph cluster capacity metrics to CloudWatch.

    This method:
    1. Queries instance and graph registries
    2. Calculates capacity, utilization, and health metrics
    3. Publishes metrics to CloudWatch for monitoring and auto-scaling

    Returns:
        MetricsResult with count of published metrics
    """
    logger.info("Starting Graph metrics collection")

    result = MetricsResult(
      timestamp=datetime.now(UTC).isoformat(),
    )

    try:
      graph_table = self.dynamodb.Table(self.graph_registry_table)
      instance_table = self.dynamodb.Table(self.instance_registry_table)

      # Get all healthy instances
      instances_response = instance_table.scan(
        FilterExpression="#s = :status",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":status": "healthy"},
      )
      instances = instances_response.get("Items", [])

      while "LastEvaluatedKey" in instances_response:
        instances_response = instance_table.scan(
          FilterExpression="#s = :status",
          ExpressionAttributeNames={"#s": "status"},
          ExpressionAttributeValues={":status": "healthy"},
          ExclusiveStartKey=instances_response["LastEvaluatedKey"],
        )
        instances.extend(instances_response.get("Items", []))

      # Calculate metrics
      total_capacity = 0
      total_used = 0
      total_available = 0
      instance_age_buckets = {"new": 0, "stabilizing": 0, "stable": 0}
      tier_counts = dict.fromkeys(TIER_CAPACITY_MAP, 0)
      metrics: list[dict[str, Any]] = []

      default_max_dbs = 50

      for instance in instances:
        instance_id = instance.get("instance_id")
        tier = instance.get("tier") or instance.get("cluster_tier", "ladybug-standard")
        max_dbs = int(
          instance.get("total_capacity")
          or instance.get("max_databases", default_max_dbs)
        )
        used_dbs = int(instance.get("database_count", 0))
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
            age = datetime.now(UTC) - created_dt
            age_hours = age.total_seconds() / 3600
          except Exception:
            pass

        # Categorize by age
        if age_hours < 0.25:
          instance_age_buckets["new"] += 1
        elif age_hours < 1:
          instance_age_buckets["stabilizing"] += 1
        else:
          instance_age_buckets["stable"] += 1

        # Calculate utilization
        utilization = (used_dbs / max_dbs * 100) if max_dbs > 0 else 0

        # Accumulate totals
        total_capacity += max_dbs
        total_used += used_dbs
        total_available += available_dbs

        # Per-instance metrics (environment is in namespace, not dimension)
        metrics.extend(
          [
            {
              "MetricName": "InstanceDatabaseCount",
              "Value": used_dbs,
              "Unit": "Count",
              "Dimensions": [
                {"Name": "InstanceId", "Value": instance_id},
                {"Name": "ClusterTier", "Value": tier},
              ],
            },
            {
              "MetricName": "InstanceUtilization",
              "Value": utilization,
              "Unit": "Percent",
              "Dimensions": [
                {"Name": "InstanceId", "Value": instance_id},
                {"Name": "ClusterTier", "Value": tier},
              ],
            },
            {
              "MetricName": "InstanceAvailableSlots",
              "Value": available_dbs,
              "Unit": "Count",
              "Dimensions": [
                {"Name": "InstanceId", "Value": instance_id},
                {"Name": "ClusterTier", "Value": tier},
              ],
            },
          ]
        )

      # Count active databases
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

        # Cluster-wide metrics (no Environment dimension - it's in the namespace)
        metrics.extend(
          [
            {
              "MetricName": "ClusterTotalCapacity",
              "Value": total_capacity,
              "Unit": "Count",
            },
            {
              "MetricName": "ClusterTotalUsed",
              "Value": total_used,
              "Unit": "Count",
            },
            {
              "MetricName": "ClusterTotalActive",
              "Value": total_active,
              "Unit": "Count",
            },
            {
              "MetricName": "ClusterAvailableCapacityPercent",
              "Value": available_percent,
              "Unit": "Percent",
            },
            {
              "MetricName": "ClusterUsedCapacityPercent",
              "Value": used_percent,
              "Unit": "Percent",
            },
            {
              "MetricName": "ClusterInstanceCount",
              "Value": len(instances),
              "Unit": "Count",
            },
          ]
        )

        # Instance age distribution (no Environment dimension - it's in the namespace)
        for age_type, count in instance_age_buckets.items():
          metrics.append(
            {
              "MetricName": "InstancesByAge",
              "Value": count,
              "Unit": "Count",
              "Dimensions": [
                {"Name": "AgeCategory", "Value": age_type},
              ],
            }
          )

        # Tier distribution (no Environment dimension - it's in the namespace)
        for tier, count in tier_counts.items():
          if count > 0:
            metrics.append(
              {
                "MetricName": "InstancesByTier",
                "Value": count,
                "Unit": "Count",
                "Dimensions": [
                  {"Name": "ClusterTier", "Value": tier},
                ],
              }
            )

      # Use environment-specific namespace
      cloudwatch_namespace = f"RoboSystems/Graph/{self.environment}"

      # Publish metrics in batches of 20
      for i in range(0, len(metrics), 20):
        batch = metrics[i : i + 20]
        self.cloudwatch.put_metric_data(
          Namespace=cloudwatch_namespace,
          MetricData=batch,
        )
        result.metrics_published += len(batch)

      logger.info(
        f"Published {result.metrics_published} metrics to CloudWatch "
        f"namespace {cloudwatch_namespace}"
      )

    except Exception as e:
      logger.error(f"Failed to collect Graph metrics: {e}", exc_info=True)
      result.error_message = str(e)

    return result
