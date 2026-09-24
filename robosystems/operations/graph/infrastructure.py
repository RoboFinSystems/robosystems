"""Reconcile the graph fleet's DynamoDB registries (instances, graphs, volumes)
against EC2/EBS, and publish the capacity metrics the scaling policies read.

Every method is a scheduled full-table sweep; failures are collected into the
returned result rather than raised, so one bad row cannot abort a sweep.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import boto3
from botocore.exceptions import ClientError

from robosystems.config import env
from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
from robosystems.logger import logger
from robosystems.middleware.graph.allocation_manager import (
  OCCUPYING_DATABASE_STATUSES,
)

if TYPE_CHECKING:
  from mypy_boto3_cloudwatch import CloudWatchClient  # type: ignore[import-not-found]
  from mypy_boto3_dynamodb import (
    DynamoDBServiceResource,  # type: ignore[import-not-found]
  )
  from mypy_boto3_ec2 import EC2Client  # type: ignore[import-not-found]


STALE_GRAPH_DAYS = 7
STALE_VOLUME_DAYS = 30

# Databases per instance, per tier. Must match `.github/configs/graph.yml`.
TIER_CAPACITY_MAP = {
  "ladybug-standard": 1,
  "ladybug-large": 1,
  "ladybug-xlarge": 1,
  "ladybug-shared": 10,
}

# Tiers a tenant can be placed on; `ladybug-shared` hosts only shared
# repositories, and counting its slots hides a full tenant tier. TenantSlotsFree
# is reported per tier so one tier's headroom cannot mask another being full.
TENANT_TIERS = ("ladybug-standard", "ladybug-large", "ladybug-xlarge")

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
  orphaned_count: int = 0
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
  """Databases per instance for a tier; falls back to standard if unknown."""
  if not tier:
    logger.warning("No tier specified, using 'ladybug-standard' as default")
    return TIER_CAPACITY_MAP["ladybug-standard"]

  if tier not in TIER_CAPACITY_MAP:
    logger.error(f"Unknown tier: {tier}. Valid tiers: {list(TIER_CAPACITY_MAP.keys())}")
    return TIER_CAPACITY_MAP["ladybug-standard"]

  return TIER_CAPACITY_MAP[tier]


def _is_valid_ec2_instance_id(instance_id: str) -> bool:
  """True for a well-formed ``i-…`` EC2 instance ID."""
  if not instance_id or not isinstance(instance_id, str):
    return False
  return EC2_INSTANCE_ID_PATTERN.match(instance_id) is not None


class InstanceMonitor:
  """Registry maintenance and metrics for the graph EC2 fleet."""

  def __init__(
    self,
    instance_registry_table: str | None = None,
    graph_registry_table: str | None = None,
    volume_registry_table: str | None = None,
    environment: str | None = None,
  ):
    self.environment = environment or env.ENVIRONMENT

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

    self._ec2: EC2Client | None = None
    self._dynamodb: DynamoDBServiceResource | None = None
    self._cloudwatch: CloudWatchClient | None = None

  @property
  def ec2(self) -> EC2Client:
    if self._ec2 is None:
      self._ec2 = boto3.client("ec2")
    return self._ec2

  @property
  def dynamodb(self) -> DynamoDBServiceResource:
    if self._dynamodb is None:
      self._dynamodb = boto3.resource("dynamodb")
    return self._dynamodb

  @property
  def cloudwatch(self) -> CloudWatchClient:
    if self._cloudwatch is None:
      self._cloudwatch = boto3.client("cloudwatch")
    return self._cloudwatch

  def check_instance_health(self) -> HealthCheckResult:
    """Reconcile the instance registry against live EC2 state.

    A terminated instance is dropped from the registry and its volumes are
    released back to ``available``, so an ASG replacement does not inherit a
    volume the registry still thinks is attached.
    """
    logger.info("Starting instance health check")

    result = HealthCheckResult(
      timestamp=datetime.now(UTC).isoformat(),
    )

    try:
      table = self.dynamodb.Table(self.instance_registry_table)

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

      ec2_instances: dict[str, str] = {}

      for invalid_id in invalid_instance_ids:
        ec2_instances[invalid_id] = "invalid_id"

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
            # One missing id fails the whole batch; retry individually.
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

            self._update_volumes_for_terminated_instance(instance_id, current_time)
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
    """Release an instance's volumes back to ``available``/``unattached``.

    Database assignments on the volume are preserved, so the volume can be
    re-attached to a replacement instance without losing what it holds.
    """
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
    """Drop long-deleted graph-registry rows; mark, never delete, orphaned ones.

    A row whose instance is missing from the instance registry is still a live
    graph's routing, and that registry drifts during ASG cycling; user graphs
    have no boot-time re-registration, so deleting the row would be permanent.
    It is stamped ``instance_missing_since`` instead, counted in
    ``OrphanedGraphRegistrations``, and unstamped when the instance returns.
    """
    logger.info("Starting graph registry cleanup")

    result = CleanupResult(
      timestamp=datetime.now(UTC).isoformat(),
    )

    try:
      graph_table = self.dynamodb.Table(self.graph_registry_table)
      instance_table = self.dynamodb.Table(self.instance_registry_table)

      response = graph_table.scan()
      items = response.get("Items", [])

      while "LastEvaluatedKey" in response:
        response = graph_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))

      # Paginated: a truncated page would make later instances look missing
      # and stamp their graphs orphaned.
      instance_response = instance_table.scan(ProjectionExpression="instance_id")
      instance_items = instance_response.get("Items", [])
      while "LastEvaluatedKey" in instance_response:
        instance_response = instance_table.scan(
          ProjectionExpression="instance_id",
          ExclusiveStartKey=instance_response["LastEvaluatedKey"],
        )
        instance_items.extend(instance_response.get("Items", []))
      valid_instances = {item["instance_id"] for item in instance_items}

      now_iso = datetime.now(UTC).isoformat()

      for item in items:
        graph_id = item.get("graph_id")
        status = item.get("status")
        instance_id = item.get("instance_id")
        deleted_at = item.get("deleted_at")

        should_remove = False

        if status == "deleted" and deleted_at:
          try:
            deleted_time = datetime.fromisoformat(deleted_at.replace("Z", "+00:00"))
            age_days = (datetime.now(UTC) - deleted_time).days
            if age_days > STALE_GRAPH_DAYS:
              should_remove = True
              logger.info(f"Removing graph {graph_id}: deleted {age_days} days ago")
          except Exception:
            pass

        if should_remove:
          try:
            graph_table.delete_item(Key={"graph_id": graph_id})
            result.removed_count += 1
          except Exception as e:
            logger.error(f"Failed to remove graph {graph_id}: {e}")
            result.errors += 1
          continue

        # Exempt rows that are not routing: shared repositories (the router
        # resolves them without this registry, and their master is parked
        # between ingestion runs) and rows already marked deleted (their
        # instance is recycled long before the row ages out). A previously
        # stamped exempt row is unstamped below.
        instance_missing = (
          bool(instance_id)
          and instance_id not in valid_instances
          and status != "deleted"
          and not is_shared_repository_or_subgraph(graph_id)
        )
        already_marked = item.get("instance_missing_since") is not None

        if instance_missing:
          result.orphaned_count += 1
          if not already_marked:
            logger.warning(
              f"Graph {graph_id} points at instance {instance_id}, which is "
              "absent from the instance registry; marking, not removing"
            )
            try:
              graph_table.update_item(
                Key={"graph_id": graph_id},
                UpdateExpression="SET instance_missing_since = :ts",
                ConditionExpression="attribute_not_exists(instance_missing_since)",
                ExpressionAttributeValues={":ts": now_iso},
              )
              result.updated_count += 1
            except Exception as e:
              logger.error(f"Failed to mark graph {graph_id} as orphaned: {e}")
              result.errors += 1
        elif already_marked:
          logger.info(
            f"Graph {graph_id} no longer counts as orphaned "
            f"(instance {instance_id}); clearing the marker"
          )
          try:
            graph_table.update_item(
              Key={"graph_id": graph_id},
              UpdateExpression="REMOVE instance_missing_since",
            )
            result.updated_count += 1
          except Exception as e:
            logger.error(f"Failed to clear orphan marker on graph {graph_id}: {e}")
            result.errors += 1

      self._publish_orphaned_graph_metric(result.orphaned_count)

      logger.info(
        f"Graph registry cleanup completed: {result.removed_count} entries removed, "
        f"{result.orphaned_count} pointing at missing instances"
      )

    except Exception as e:
      logger.error(f"Failed to cleanup graph registry: {e}", exc_info=True)
      result.error_message = str(e)

    return result

  def _publish_orphaned_graph_metric(self, count: int) -> None:
    """Publish the orphaned-registration count so an alarm can watch it.

    Published on every sweep, zero included, so the alarm sees a real
    datapoint rather than reading OK on missing data.
    """
    try:
      self.cloudwatch.put_metric_data(
        Namespace=f"RoboSystems/Graph/{self.environment}",
        MetricData=[
          {
            "MetricName": "OrphanedGraphRegistrations",
            "Value": count,
            "Unit": "Count",
          }
        ],
      )
    except Exception as e:
      logger.warning(f"Could not publish OrphanedGraphRegistrations metric: {e}")

  def cleanup_stale_volumes(self) -> CleanupResult:
    """Correct or drop volume-registry rows that no longer reflect reality.

    A volume stuck ``attaching`` to a dead instance is marked ``failed`` and
    unattached; a volume that carries no databases and has been unattached
    for more than ``STALE_VOLUME_DAYS`` is dropped from the registry. Rows
    that still list databases are never dropped. Neither path deletes the
    EBS volume itself.
    """
    logger.info("Starting volume registry cleanup")

    result = CleanupResult(
      timestamp=datetime.now(UTC).isoformat(),
    )

    try:
      volume_table = self.dynamodb.Table(self.volume_registry_table)
      instance_table = self.dynamodb.Table(self.instance_registry_table)

      response = volume_table.scan()
      items = response.get("Items", [])

      while "LastEvaluatedKey" in response:
        response = volume_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))

      # Paginated: a truncated page would make later instances look missing
      # and mark volumes attaching to them failed.
      instance_response = instance_table.scan(ProjectionExpression="instance_id")
      instance_items = instance_response.get("Items", [])
      while "LastEvaluatedKey" in instance_response:
        instance_response = instance_table.scan(
          ProjectionExpression="instance_id",
          ExclusiveStartKey=instance_response["LastEvaluatedKey"],
        )
        instance_items.extend(instance_response.get("Items", []))
      valid_instances = {item["instance_id"] for item in instance_items}

      for item in items:
        volume_id = item.get("volume_id")
        status = item.get("status")
        instance_id = item.get("instance_id")
        created_at = item.get("created_at")

        should_update = False
        should_remove = False
        new_status = None

        if status == "attaching" and instance_id and instance_id != "unattached":
          if instance_id not in valid_instances:
            should_update = True
            new_status = "failed"
            logger.info(
              f"Volume {volume_id} stuck attaching to "
              f"non-existent instance {instance_id}"
            )

        if instance_id == "unattached" and status == "available":
          # The row is the only link between those graphs and their data;
          # dropping it makes the next launch mint an empty volume.
          if item.get("databases"):
            continue

          # Age from the last detach: a volume that parks nightly would
          # otherwise be dropped once it turned STALE_VOLUME_DAYS old.
          age_anchor = (
            item.get("last_detached") or item.get("last_attached") or created_at
          )
          if age_anchor:
            try:
              anchor_time = datetime.fromisoformat(age_anchor.replace("Z", "+00:00"))
              age_days = (datetime.now(UTC) - anchor_time).days
              if age_days > STALE_VOLUME_DAYS:
                should_remove = True
                logger.info(
                  f"Removing old unattached volume {volume_id}: "
                  f"unattached for {age_days} days"
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
    """Publish fleet capacity and utilization metrics to CloudWatch.

    The fleet's auto-scaling policies consume these, so a failed collection run
    leaves scaling decisions on stale data.
    """
    logger.info("Starting Graph metrics collection")

    result = MetricsResult(
      timestamp=datetime.now(UTC).isoformat(),
    )

    try:
      graph_table = self.dynamodb.Table(self.graph_registry_table)
      instance_table = self.dynamodb.Table(self.instance_registry_table)

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

        if tier in tier_counts:
          tier_counts[tier] += 1

        age_hours = 0
        if created_at:
          try:
            created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            age = datetime.now(UTC) - created_dt
            age_hours = age.total_seconds() / 3600
          except Exception:
            pass

        if age_hours < 0.25:
          instance_age_buckets["new"] += 1
        elif age_hours < 1:
          instance_age_buckets["stabilizing"] += 1
        else:
          instance_age_buckets["stable"] += 1

        utilization = (used_dbs / max_dbs * 100) if max_dbs > 0 else 0

        total_capacity += max_dbs
        total_used += used_dbs
        total_available += available_dbs

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

      # One paginated pass yields both the active count and the per-instance
      # occupancy TenantSlotsFree needs.
      occupied_by_instance: dict[str, int] = {}
      try:
        total_active = 0
        scan_kwargs: dict[str, Any] = {
          "FilterExpression": "#s <> :deleted AND #s <> :pending_deletion",
          "ExpressionAttributeNames": {"#s": "status", "#i": "instance_id"},
          "ExpressionAttributeValues": {
            ":deleted": "deleted",
            ":pending_deletion": "pending_deletion",
          },
          "ProjectionExpression": "#i, #s",
        }
        while True:
          graph_response = graph_table.scan(**scan_kwargs)
          for row in graph_response.get("Items", []):
            total_active += 1
            if row.get("status") in OCCUPYING_DATABASE_STATUSES:
              row_instance = row.get("instance_id")
              if row_instance:
                occupied_by_instance[row_instance] = (
                  occupied_by_instance.get(row_instance, 0) + 1
                )
          last_key = graph_response.get("LastEvaluatedKey")
          if not last_key:
            break
          scan_kwargs["ExclusiveStartKey"] = last_key
      except Exception as exc:
        # Occupancy is unknown, so publishing TenantSlotsFree would assert
        # headroom nobody verified. Leave it unpublished and let the alarm's
        # TreatMissingData: breaching speak instead.
        logger.warning(f"Graph registry scan failed, skipping TenantSlotsFree: {exc}")
        total_active = total_used
        occupied_by_instance = {}
        tenant_slots_free = None
      else:
        tenant_slots_free = dict.fromkeys(TENANT_TIERS, 0)
        for instance in instances:
          tier = instance.get("tier") or instance.get(
            "cluster_tier", "ladybug-standard"
          )
          if tier not in tenant_slots_free:
            continue
          instance_id = instance.get("instance_id")
          # The same two inputs `_find_best_instance` places against; the
          # instance registry's counts can disagree with where graphs are.
          slot_total = int(instance.get("max_databases") or _get_tier_capacity(tier))
          occupied = occupied_by_instance.get(instance_id, 0)
          tenant_slots_free[tier] += max(0, slot_total - occupied)

      if total_capacity > 0:
        available_percent = (total_available / total_capacity) * 100
        used_percent = (total_used / total_capacity) * 100

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

      # Outside the `total_capacity > 0` branch: an empty fleet's zero free
      # slots is exactly the reading worth alarming on.
      if tenant_slots_free is not None:
        metrics.extend(
          {
            "MetricName": "TenantSlotsFree",
            "Value": free,
            "Unit": "Count",
            "Dimensions": [{"Name": "ClusterTier", "Value": tier}],
          }
          for tier, free in tenant_slots_free.items()
        )

      # The environment is in the namespace, not a dimension, so staging and
      # prod never share a metric stream.
      cloudwatch_namespace = f"RoboSystems/Graph/{self.environment}"

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
