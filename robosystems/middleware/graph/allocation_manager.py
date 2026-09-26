"""Allocation of LadybugDB databases across writer instances.

Two DynamoDB tables hold the state: a graph registry mapping each graph_id to
the instance hosting it, and an instance registry tracking which writers are
healthy. Allocation writes the graph row conditionally, so two concurrent
allocations of the same graph_id cannot both succeed. Routing reads the graph
registry directly — there is no load balancer in front of the writers, since a
graph lives on exactly one instance.

Subgraphs are not allocated separately: they resolve to their parent's
instance.
"""

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, cast

import boto3
from botocore.exceptions import ClientError

from robosystems.config import env
from robosystems.logger import logger
from robosystems.middleware.graph.types import (
  GraphTier,
  GraphTypeRegistry,
  is_subgraph_id,
)
from robosystems.security import SecurityAuditLogger, SecurityEventType

from .utils import MultiTenantUtils, parse_subgraph_id


class GraphIDCollisionError(Exception):
  """Raised when a generated graph ID collides with an existing one owned by a different entity."""


class VolumeNotResolvedError(Exception):
  """An instance's data volume could not be identified from the registry."""


class AllocationRaceConditionError(Exception):
  """Raised when DynamoDB conditional write fails and the existing item cannot be resolved."""


# Entity IDs: alphanumeric, underscore, dash, max 128 chars. These feed
# DynamoDB keys and instance routing, so they are validated before use.
VALID_ENTITY_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]{1,128}$")

# Built lazily: importing GRAPH_ID_PATTERN at module load would be circular.
_VALID_GRAPH_ID_REGEX: re.Pattern[str] | None = None


def _get_valid_graph_id_regex() -> re.Pattern[str]:
  """Get compiled graph ID regex, building it lazily on first call."""
  global _VALID_GRAPH_ID_REGEX
  if _VALID_GRAPH_ID_REGEX is None:
    from robosystems.middleware.graph.types import GRAPH_ID_PATTERN

    _VALID_GRAPH_ID_REGEX = re.compile(GRAPH_ID_PATTERN)
  return _VALID_GRAPH_ID_REGEX


VALID_INSTANCE_ID_PATTERN = re.compile(r"^i-[0-9a-f]{8,17}$")  # AWS instance ID format


def get_dynamodb_resource():
  """DynamoDB resource, pointed at LocalStack in development."""
  region = env.AWS_REGION

  if env.is_development() and env.AWS_ENDPOINT_URL:
    return boto3.resource(
      "dynamodb", endpoint_url=env.AWS_ENDPOINT_URL, region_name=region
    )
  return boto3.resource("dynamodb", region_name=region)


class DatabaseStatus(Enum):
  """Database status enumeration."""

  ACTIVE = "active"
  CREATING = "creating"
  MIGRATING = "migrating"
  FAILED = "failed"
  DELETED = "deleted"


class InstanceStatus(Enum):
  """Instance status enumeration."""

  HEALTHY = "healthy"
  UNHEALTHY = "unhealthy"
  TERMINATING = "terminating"


# The single definition of "occupies a slot": placement (`_find_best_instance`)
# and the fleet metrics collector must agree on it.
OCCUPYING_DATABASE_STATUSES = frozenset(
  {
    DatabaseStatus.ACTIVE.value,
    DatabaseStatus.CREATING.value,
    DatabaseStatus.MIGRATING.value,
  }
)


@dataclass
class DatabaseLocation:
  """Database location information."""

  graph_id: str
  instance_id: str
  private_ip: str
  availability_zone: str
  created_at: datetime
  status: DatabaseStatus
  backend_type: str = "ladybug"


@dataclass
class InstanceInfo:
  """Instance information from DynamoDB."""

  instance_id: str
  private_ip: str
  availability_zone: str
  status: InstanceStatus
  database_count: int
  max_databases: int
  created_at: datetime

  @property
  def available_capacity(self) -> int:
    """Number of additional databases this instance can handle."""
    return max(0, self.max_databases - self.database_count)

  @property
  def utilization_percent(self) -> float:
    """Database utilization percentage."""
    return (
      (self.database_count / self.max_databases * 100) if self.max_databases > 0 else 0
    )


class LadybugAllocationManager:
  """DynamoDB-based allocation manager for graph databases."""

  def __init__(
    self,
    environment: str,
    max_databases_per_instance: int | None = None,
    asg_name: str | None = None,
  ):
    self.environment = environment
    self.max_databases_per_instance = (
      max_databases_per_instance
      if max_databases_per_instance is not None
      else MultiTenantUtils.get_max_databases_per_node()
    )

    # Memory and chunk sizes come from graph.yml via GraphTierConfig.
    self.tier_configs = {
      GraphTier.LADYBUG_STANDARD: {
        "backend": "ladybug",
        "backend_type": "ladybug",
        "databases_per_instance": self.max_databases_per_instance,
      },
      GraphTier.LADYBUG_LARGE: {
        "backend": "ladybug",
        "backend_type": "ladybug",
        "databases_per_instance": 1,  # parent + its subgraphs
      },
      GraphTier.LADYBUG_XLARGE: {
        "backend": "ladybug",
        "backend_type": "ladybug",
        "databases_per_instance": 1,
      },
      GraphTier.LADYBUG_SHARED: {
        "backend": "ladybug",
        "backend_type": "ladybug",
        "databases_per_instance": 1,  # One repository per instance
      },
    }
    # Fallback only; the real ASG name is derived from instance data.
    if asg_name:
      self.default_asg_name = asg_name
    else:
      # Validate the environment before interpolating it into a resource name.
      if not re.match(r"^[a-z]+$", environment.lower()):
        raise ValueError(f"Invalid environment name: {environment}")
      self.default_asg_name = (
        f"robosystems-ladybug-standard-writers-{environment.lower()}-asg"
      )

    dynamodb = cast(Any, get_dynamodb_resource())

    self.graph_table = dynamodb.Table(env.GRAPH_REGISTRY_TABLE)
    self.instance_table = dynamodb.Table(env.INSTANCE_REGISTRY_TABLE)
    self.volume_table = dynamodb.Table(env.VOLUME_REGISTRY_TABLE)

    region = env.AWS_REGION

    if env.is_development() and env.AWS_ENDPOINT_URL:
      endpoint_url = env.AWS_ENDPOINT_URL
      self.autoscaling = boto3.client(
        "autoscaling", endpoint_url=endpoint_url, region_name=region
      )
      self.cloudwatch = boto3.client(
        "cloudwatch", endpoint_url=endpoint_url, region_name=region
      )
    else:
      self.autoscaling = boto3.client("autoscaling", region_name=region)
      self.cloudwatch = boto3.client("cloudwatch", region_name=region)

    logger.info(f"Initialized LadybugAllocationManager for environment: {environment}")

  def get_tier_config(self, tier: GraphTier) -> dict[str, Any]:
    """Backend type and allocation settings for a tier.

    Memory and chunk-size settings live in `GraphTierConfig`
    (`config/graph_tier.py`).
    """
    return self.tier_configs.get(tier, self.tier_configs[GraphTier.LADYBUG_STANDARD])

  async def allocate_database(
    self,
    entity_id: str,
    graph_id: str | None = None,
    graph_type: str | None = None,
    instance_tier: GraphTier | None = None,
  ) -> DatabaseLocation:
    """Place a new database on a writer instance and return its location.

    `graph_id` is generated when not supplied. The registry row is written
    conditionally, so a collision with an existing graph owned by a
    different entity raises `GraphIDCollisionError` rather than silently
    re-pointing it.
    """
    if not entity_id or not isinstance(entity_id, str):
      SecurityAuditLogger.log_input_validation_failure(
        field_name="entity_id",
        invalid_value=str(entity_id),
        validation_error="Entity ID must be a non-empty string",
      )
      raise ValueError("Entity ID must be a non-empty string")

    if not VALID_ENTITY_ID_PATTERN.match(entity_id):
      SecurityAuditLogger.log_input_validation_failure(
        field_name="entity_id",
        invalid_value=entity_id,
        validation_error="Invalid entity ID format, must be alphanumeric with underscores/dashes",
      )
      raise ValueError(
        f"Invalid entity ID format: {entity_id}. Must contain only alphanumeric characters, underscores, and dashes."
      )

    # User graphs use a kg prefix with a ULID, so IDs sort by creation time.
    if not graph_id:
      from robosystems.utils.ulid import generate_ulid_hex

      graph_id = f"kg{generate_ulid_hex(20)}"

    if not _get_valid_graph_id_regex().match(graph_id):
      # Subgraph IDs get a specific error rather than the generic one.
      if is_subgraph_id(graph_id):
        parent_id = graph_id.split("_")[0]
        SecurityAuditLogger.log_input_validation_failure(
          field_name="graph_id",
          invalid_value=graph_id,
          validation_error="Subgraph ID used in registry lookup",
        )
        raise ValueError(
          f"Subgraph IDs are not stored in the DynamoDB registry. "
          f"Use the parent graph ID ('{parent_id}') for registry lookups. "
          f"Subgraphs share their parent's instance allocation."
        )

      SecurityAuditLogger.log_input_validation_failure(
        field_name="graph_id",
        invalid_value=graph_id,
        validation_error="Invalid graph ID format",
      )
      raise ValueError(
        f"Invalid graph ID format: {graph_id}. Must be 'kg' followed by 16+ lowercase hex characters or a shared repository name."
      )

    subgraph_info = parse_subgraph_id(graph_id)
    if subgraph_info:
      logger.info(
        f"Detected subgraph {graph_id} - routing to parent {subgraph_info.parent_graph_id}"
      )

      parent_location = await self.find_database_location(subgraph_info.parent_graph_id)
      if not parent_location:
        raise ValueError(
          f"Parent graph {subgraph_info.parent_graph_id} not found. "
          f"Cannot create subgraph without parent allocation."
        )

      # Creating the database itself happens a layer up.
      logger.info(
        f"Subgraph {graph_id} will use parent's instance {parent_location.instance_id} "
        f"({parent_location.private_ip})"
      )

      # Subgraphs are real databases on disk, so they need volume tracking.
      volume_id = self._resolve_instance_volume(parent_location.instance_id)
      await self._update_volume_registry_add_database(volume_id, graph_id)

      return DatabaseLocation(
        graph_id=graph_id,
        instance_id=parent_location.instance_id,
        private_ip=parent_location.private_ip,
        availability_zone=parent_location.availability_zone,
        created_at=datetime.now(UTC),
        status=DatabaseStatus.ACTIVE,
        backend_type=parent_location.backend_type,
      )

    logger.info(f"Allocating database {graph_id} for entity {entity_id}")

    try:
      identity = GraphTypeRegistry.identify_graph(graph_id, graph_tier=instance_tier)

      tier_config = self.get_tier_config(instance_tier or GraphTier.LADYBUG_STANDARD)
      backend_type = tier_config.get("backend_type", "ladybug")

      # Fail fast before writing anything: a writer with capacity whose
      # volume resolves, since instance replacement reattaches from the
      # volume registry.
      excluded: set[str] = set()
      picked = await self._pick_writer(instance_tier, excluded)

      if not picked:
        tier_name = (
          instance_tier.value.replace("-", " ").title()
          if instance_tier
          else "Ladybug Standard"
        )
        await self._publish_failure_metric("no_capacity", entity_id, None)
        raise Exception(
          f"No {tier_name} capacity currently available. "
          "Please contact support or try again later."
        )
      instance, volume_id = picked

      now = datetime.now(UTC)
      max_retries = 3
      retry_count = 0

      while retry_count < max_retries:
        try:
          # STEP 1: create the graph row only if the graph_id is free.
          self.graph_table.put_item(
            Item={
              "graph_id": graph_id,
              "entity_id": entity_id,
              "graph_type": identity.graph_type if identity else graph_type,
              "backend_type": backend_type,
              "instance_id": instance.instance_id,
              "private_ip": instance.private_ip,
              "availability_zone": instance.availability_zone,
              "created_at": now.isoformat(),
              "last_accessed": now.isoformat(),
              "status": DatabaseStatus.ACTIVE.value,
              "database_size_mb": Decimal(0),
              "allocation_lock": f"allocated_by_{now.timestamp()}",
            },
            ConditionExpression="attribute_not_exists(graph_id)",
          )

          # STEP 2: claim a slot on the instance, conditional on capacity.
          try:
            self.instance_table.update_item(
              Key={"instance_id": instance.instance_id},
              UpdateExpression="ADD database_count :inc SET last_allocation = :timestamp",
              ConditionExpression="database_count < max_databases",
              ExpressionAttributeValues={
                ":inc": 1,
                ":timestamp": now.isoformat(),
              },
            )

            break

          except ClientError as capacity_error:
            if (
              capacity_error.response["Error"]["Code"]
              == "ConditionalCheckFailedException"
            ):
              # Lost the race for the last slot: undo STEP 1, try another instance.
              logger.warning(
                f"Instance {instance.instance_id} reached capacity during allocation, rolling back"
              )

              try:
                self.graph_table.delete_item(
                  Key={"graph_id": graph_id},
                  ConditionExpression="allocation_lock = :lock_id",
                  ExpressionAttributeValues={
                    ":lock_id": f"allocated_by_{now.timestamp()}"
                  },
                )
              except ClientError as rollback_error:
                logger.error(
                  f"Failed to rollback database entry during capacity conflict: {rollback_error}"
                )

              excluded.add(instance.instance_id)
              picked = await self._pick_writer(instance_tier, excluded)
              if not picked:
                raise Exception("No available instances after capacity conflict")
              instance, volume_id = picked

              retry_count += 1
              if retry_count >= max_retries:
                raise Exception(
                  f"Failed to allocate database after {max_retries} attempts due to capacity conflicts"
                )

              logger.info(
                f"Retrying allocation with instance {instance.instance_id} (attempt {retry_count + 1})"
              )
              continue
            else:
              raise capacity_error

        except ClientError as e:
          if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            try:
              response = self.graph_table.get_item(Key={"graph_id": graph_id})
              if "Item" in response:
                item = response["Item"]
                existing_entity = item.get("entity_id", "unknown")
                if existing_entity == entity_id:
                  # Same entity retrying: idempotent.
                  logger.info(
                    f"Database {graph_id} already allocated to same entity {entity_id} (idempotent retry)"
                  )
                  return DatabaseLocation(
                    graph_id=graph_id,
                    instance_id=item["instance_id"],
                    private_ip=item["private_ip"],
                    availability_zone=item.get("availability_zone", "unknown"),
                    created_at=datetime.fromisoformat(item["created_at"]),
                    status=DatabaseStatus(item.get("status", "active")),
                    backend_type=item.get("backend_type", "ladybug"),
                  )
                else:
                  logger.error(
                    f"Graph ID collision: {graph_id} already belongs to entity {existing_entity}, "
                    f"requested by entity {entity_id}"
                  )
                  raise GraphIDCollisionError(
                    f"Graph ID {graph_id} already exists (owned by a different entity)."
                  )
              else:
                logger.error(
                  f"Conditional check failed but database {graph_id} not found"
                )
                raise AllocationRaceConditionError(
                  f"Conditional check failed but database {graph_id} not found"
                )
            except ClientError as lookup_error:
              logger.error(
                f"Failed to lookup existing database after conditional check failure: {lookup_error}"
              )
              raise AllocationRaceConditionError(
                f"Database allocation failed for {graph_id} and lookup failed"
              )
          else:
            raise e

      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.DATABASE_ALLOCATED,
        details={
          "action": "database_allocated",
          "entity_id": entity_id,
          "graph_category": identity.category.value,
          "graph_type": identity.graph_type,
          "graph_id": graph_id,
          "instance_id": instance.instance_id,
          "private_ip": instance.private_ip,
          "availability_zone": instance.availability_zone,
          "allocated_at": now.isoformat(),
        },
        risk_level="low",
      )

      logger.info(
        f"Allocated {graph_id} to instance {instance.instance_id} ({instance.private_ip}) "
        f"- tier: {instance_tier.value if instance_tier else 'ladybug-standard'}, "
        f"entity: {entity_id}"
      )

      await self._update_volume_registry_add_database(volume_id, graph_id)

      # Protect the instance from scale-in now that it holds a database.
      if self.environment not in ["dev", "test"]:
        try:
          asg_name = await self._get_asg_name_for_instance(instance.instance_id)
          if asg_name:
            self.autoscaling.set_instance_protection(
              InstanceIds=[instance.instance_id],
              AutoScalingGroupName=asg_name,
              ProtectedFromScaleIn=True,
            )
            logger.info(
              f"Enabled scale-in protection for instance {instance.instance_id} in ASG {asg_name}"
            )
          else:
            logger.warning(
              f"Could not determine ASG name for instance {instance.instance_id}"
            )
        except ClientError as e:
          # Protection is defense in depth; its failure must not fail allocation.
          logger.error(f"Failed to enable instance protection: {e}")

        await self._publish_allocation_metrics()

      return DatabaseLocation(
        graph_id=graph_id,
        instance_id=instance.instance_id,
        private_ip=instance.private_ip,
        availability_zone=instance.availability_zone,
        created_at=now,
        status=DatabaseStatus.ACTIVE,
        backend_type=backend_type,
      )

    except ClientError as e:
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
        details={
          "action": "database_allocation_failed",
          "entity_id": entity_id,
          "graph_id": graph_id,
          "error": str(e),
          "error_type": "client_error",
        },
        risk_level="high",
      )

      logger.error(f"Failed to allocate database: {e}")
      raise Exception(f"Database allocation failed: {e!s}")

  async def find_database_location(self, graph_id: str) -> DatabaseLocation | None:
    """Locate an existing database, or None when it is not registered.

    A subgraph resolves to its parent's instance — subgraphs have no
    allocation of their own — and is returned carrying the subgraph's own
    graph_id.
    """
    subgraph_info = parse_subgraph_id(graph_id)
    if subgraph_info:
      logger.debug(
        f"Resolving subgraph {graph_id} to parent {subgraph_info.parent_graph_id}"
      )
      parent_location = await self.find_database_location(subgraph_info.parent_graph_id)
      if not parent_location:
        return None

      return DatabaseLocation(
        graph_id=graph_id,
        instance_id=parent_location.instance_id,
        private_ip=parent_location.private_ip,
        availability_zone=parent_location.availability_zone,
        created_at=parent_location.created_at,
        status=parent_location.status,
        backend_type=parent_location.backend_type,
      )

    try:
      response = self.graph_table.get_item(Key={"graph_id": graph_id})

      if "Item" not in response:
        return None

      item = response["Item"]
      instance_id = item["instance_id"]

      # A row without a cached private_ip resolves through the instance
      # registry, which is authoritative for instance details.
      private_ip = item.get("private_ip")
      availability_zone = item.get("availability_zone", "unknown")

      if not private_ip:
        instance_response = self.instance_table.get_item(
          Key={"instance_id": instance_id}
        )
        if "Item" in instance_response:
          instance_item = instance_response["Item"]
          private_ip = instance_item.get("private_ip")
          availability_zone = instance_item.get("availability_zone", availability_zone)
          logger.info(
            f"Resolved private_ip for {graph_id} from instance-registry: {private_ip}"
          )

          # Refresh the cached copy so the next lookup is one read.
          try:
            self.graph_table.update_item(
              Key={"graph_id": graph_id},
              UpdateExpression="SET private_ip = :ip, availability_zone = :az, last_accessed = :time",
              ExpressionAttributeValues={
                ":ip": private_ip,
                ":az": availability_zone,
                ":time": datetime.now(UTC).isoformat(),
              },
            )
          except ClientError as update_error:
            logger.warning(
              f"Failed to update graph-registry with instance info: {update_error}"
            )
        else:
          logger.warning(
            f"Instance {instance_id} not found in instance-registry for graph {graph_id}"
          )

      if not private_ip:
        logger.error(
          f"Cannot resolve private_ip for graph {graph_id} - not in graph-registry or instance-registry"
        )
        return None

      self.graph_table.update_item(
        Key={"graph_id": graph_id},
        UpdateExpression="SET last_accessed = :time",
        ExpressionAttributeValues={":time": datetime.now(UTC).isoformat()},
      )

      return DatabaseLocation(
        graph_id=graph_id,
        instance_id=instance_id,
        private_ip=private_ip,
        availability_zone=availability_zone,
        created_at=datetime.fromisoformat(item["created_at"]),
        status=DatabaseStatus(item.get("status", "active")),
        backend_type=item.get("backend_type", "ladybug"),
      )

    except ClientError as e:
      logger.error(f"Error finding database location: {e}")
      return None

  async def deallocate_database(self, graph_id: str) -> bool:
    """Release a database's registry row via a conditional write.

    The condition is what makes concurrent deallocation safe: only one
    caller can transition the row, so the instance's database count is
    never decremented twice.
    """
    logger.info(f"Deallocating database {graph_id}")

    try:
      response = self.graph_table.get_item(Key={"graph_id": graph_id})
      if "Item" not in response:
        logger.warning(f"Database {graph_id} not found")
        return False

      item = response["Item"]
      instance_id = item["instance_id"]
      current_status = item.get("status", DatabaseStatus.ACTIVE.value)

      if current_status == DatabaseStatus.DELETED.value:
        logger.info(f"Database {graph_id} already deleted")
        return True

      deallocation_timestamp = datetime.now(UTC).isoformat()

      # STEP 1: mark deleted, unless another process already did.
      try:
        self.graph_table.update_item(
          Key={"graph_id": graph_id},
          UpdateExpression="SET #status = :deleted_status, deleted_at = :time, deallocation_lock = :lock_id",
          ConditionExpression="#status <> :deleted_status",
          ExpressionAttributeNames={"#status": "status"},
          ExpressionAttributeValues={
            ":deleted_status": DatabaseStatus.DELETED.value,
            ":time": deallocation_timestamp,
            ":lock_id": f"deallocated_by_{deallocation_timestamp}",
          },
        )
      except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
          logger.info(f"Database {graph_id} was already deleted by another process")
          return True
        else:
          raise e

      # STEP 2: release the slot, never below zero.
      try:
        self.instance_table.update_item(
          Key={"instance_id": instance_id},
          UpdateExpression="ADD database_count :dec SET last_deallocation = :timestamp",
          ConditionExpression="database_count > :zero",
          ExpressionAttributeValues={
            ":dec": -1,
            ":zero": 0,
            ":timestamp": deallocation_timestamp,
          },
        )
      except ClientError as capacity_error:
        if (
          capacity_error.response["Error"]["Code"] == "ConditionalCheckFailedException"
        ):
          # An integrity issue worth auditing, but not a reason to fail.
          logger.warning(
            f"Instance {instance_id} database count was already 0 during deallocation"
          )

          SecurityAuditLogger.log_security_event(
            event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
            details={
              "action": "database_count_integrity_issue",
              "graph_id": graph_id,
              "instance_id": instance_id,
              "issue": "Instance database count was 0 during deallocation",
              "timestamp": deallocation_timestamp,
            },
            risk_level="medium",
          )
        else:
          # Undo STEP 1.
          logger.error(
            f"Failed to decrement database count for {instance_id}: {capacity_error}"
          )

          try:
            self.graph_table.update_item(
              Key={"graph_id": graph_id},
              UpdateExpression="SET #status = :active_status REMOVE deleted_at, deallocation_lock",
              ConditionExpression="deallocation_lock = :lock_id",
              ExpressionAttributeNames={"#status": "status"},
              ExpressionAttributeValues={
                ":active_status": DatabaseStatus.ACTIVE.value,
                ":lock_id": f"deallocated_by_{deallocation_timestamp}",
              },
            )
            logger.info(
              f"Rolled back database {graph_id} status due to count update failure"
            )
          except ClientError as rollback_error:
            logger.error(f"Failed to rollback database status: {rollback_error}")

          return False

      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.DATABASE_DEALLOCATED,
        details={
          "action": "database_deallocated",
          "graph_id": graph_id,
          "instance_id": instance_id,
          "entity_id": item.get("entity_id"),
          "deallocated_at": datetime.now(UTC).isoformat(),
        },
        risk_level="low",
      )

      logger.info(f"Deallocated database {graph_id} from instance {instance_id}")

      # Keep the volume registry in step so replacement doesn't restore it.
      await self._update_volume_registry_remove_database(instance_id, graph_id)

      if self.environment not in ["dev", "test"]:
        try:
          response = self.instance_table.get_item(Key={"instance_id": instance_id})
          if "Item" in response:
            current_count = int(response["Item"].get("database_count", 0))
            if current_count == 0:
              # Nothing left to protect; let the ASG scale this instance in.
              try:
                asg_name = await self._get_asg_name_for_instance(instance_id)
                if asg_name:
                  self.autoscaling.set_instance_protection(
                    InstanceIds=[instance_id],
                    AutoScalingGroupName=asg_name,
                    ProtectedFromScaleIn=False,
                  )
                  logger.info(
                    f"Removed scale-in protection from empty instance {instance_id} in ASG {asg_name}"
                  )
                else:
                  logger.warning(
                    f"Could not determine ASG name for instance {instance_id}"
                  )
              except ClientError as e:
                # Never fail deallocation over a protection change.
                logger.error(f"Failed to remove instance protection: {e}")
        except ClientError as e:
          logger.error(f"Failed to check instance database count: {e}")

        await self._publish_allocation_metrics()

      return True

    except ClientError as e:
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
        details={
          "action": "database_deallocation_failed",
          "graph_id": graph_id,
          "error": str(e),
          "error_type": "client_error",
        },
        risk_level="high",
      )

      logger.error(f"Failed to deallocate database: {e}")
      return False

  async def get_instance_databases(self, instance_id: str) -> list[str]:
    """List the active graph IDs on an instance.

    Raises `ValueError` for a malformed instance ID rather than passing it
    to DynamoDB.
    """
    if not instance_id or not isinstance(instance_id, str):
      raise ValueError("Instance ID must be a non-empty string")

    if not VALID_INSTANCE_ID_PATTERN.match(instance_id):
      raise ValueError(f"Invalid instance ID format: {instance_id}")

    try:
      response = self.graph_table.query(
        IndexName="instance-index",
        KeyConditionExpression="instance_id = :iid",
        FilterExpression="#status = :status",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
          ":iid": instance_id,
          ":status": DatabaseStatus.ACTIVE.value,
        },
      )

      return [item["graph_id"] for item in response.get("Items", [])]

    except ClientError as e:
      logger.error(f"Error getting instance databases: {e}")
      return []

  async def get_all_instances(self) -> list[dict]:
    """Every healthy instance with its id, private IP, and status."""
    try:
      instance_response = self.instance_table.scan(
        FilterExpression="#status = :status",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":status": InstanceStatus.HEALTHY.value},
      )

      return instance_response.get("Items", [])

    except ClientError as e:
      logger.error(f"Error getting all instances: {e}")
      return []

  async def get_allocation_metrics(self) -> dict:
    """Get current allocation metrics."""
    try:
      instances = await self.get_all_instances()

      total_capacity = 0
      total_used = 0
      instance_metrics = []

      for instance in instances:
        max_dbs = int(instance.get("max_databases", self.max_databases_per_instance))
        used_dbs = int(instance.get("database_count", 0))

        total_capacity += max_dbs
        total_used += used_dbs

        instance_metrics.append(
          {
            "instance_id": instance["instance_id"],
            "utilization_percent": (used_dbs / max_dbs * 100) if max_dbs > 0 else 0,
            "database_count": used_dbs,
            "max_databases": max_dbs,
            "available_capacity": max_dbs - used_dbs,
          }
        )

      overall_utilization = (
        (total_used / total_capacity * 100) if total_capacity > 0 else 0
      )

      return {
        "total_instances": len(instances),
        "total_capacity": total_capacity,
        "total_databases": total_used,
        "overall_utilization_percent": overall_utilization,
        "instances": instance_metrics,
        "scale_up_needed": overall_utilization > 80,
        "timestamp": datetime.now(UTC).isoformat(),
      }

    except ClientError as e:
      logger.error(f"Error getting allocation metrics: {e}")
      return {"error": str(e), "timestamp": datetime.now(UTC).isoformat()}

  async def check_tier_capacity(self, tier: GraphTier) -> str:
    """Capacity status for a tier.

    - `ready`: open slot on a healthy instance
    - `scalable`: no slots, but the ASG can add instances
    - `at_capacity`: no slots and the ASG is at max
    """
    # Placeable means what allocation means: capacity and a resolvable volume.
    if await self._pick_writer(tier, set()):
      return "ready"

    has_headroom = await self._asg_has_headroom(tier)
    if has_headroom:
      return "scalable"

    return "at_capacity"

  async def _asg_has_headroom(self, tier: GraphTier) -> bool:
    """Check whether the ASG for a tier has room to add instances."""
    try:
      target_tier = tier.value

      if self.environment in ["prod", "staging"]:
        asg_name = f"robosystems-{target_tier}-writers-{self.environment}-asg"
      else:
        asg_name = self.default_asg_name

      response = self.autoscaling.describe_auto_scaling_groups(
        AutoScalingGroupNames=[asg_name]
      )

      if not response["AutoScalingGroups"]:
        return False

      asg = response["AutoScalingGroups"][0]
      return asg["DesiredCapacity"] < asg["MaxSize"]

    except ClientError as e:
      logger.error(f"Error checking ASG headroom for {tier.value}: {e}")
      return False

  def _count_allocated_graphs(self, instance_id: str) -> int:
    """Occupied slots per the graph registry, the authoritative record.

    Not the instance registry's `database_count`: it resets when a
    replacement instance re-registers, and a drifted zero would double-book
    an occupied writer.
    """
    from boto3.dynamodb.conditions import Key

    count = 0
    query_kwargs: dict[str, Any] = {
      "IndexName": "instance-index",
      "KeyConditionExpression": Key("instance_id").eq(instance_id),
    }
    while True:
      response = self.graph_table.query(**query_kwargs)
      count += sum(
        1
        for item in response.get("Items", [])
        if item.get("status") in OCCUPYING_DATABASE_STATUSES
      )
      last_key = response.get("LastEvaluatedKey")
      if not last_key:
        break
      query_kwargs["ExclusiveStartKey"] = last_key
    return count

  async def _find_best_instance(
    self,
    instance_tier: GraphTier | None = None,
    exclude_instance: str | None = None,
    exclude: set[str] | None = None,
  ) -> InstanceInfo | None:
    """Find the instance with most available capacity for the specified tier."""
    try:
      target_tier = (
        instance_tier.value if instance_tier else GraphTier.LADYBUG_STANDARD.value
      )

      if self.environment in ["prod", "staging"]:
        supported_tiers = [
          "ladybug-standard",
          "ladybug-large",
          "ladybug-xlarge",
          "ladybug-shared",
        ]
        if target_tier not in supported_tiers:
          logger.error(
            f"Invalid tier {target_tier}. Supported tiers: {supported_tiers}"
          )
          return None

        stack_name = self._get_stack_name_for_tier(target_tier)
        if not stack_name:
          logger.warning(
            f"No stack configured for tier {target_tier} in {self.environment}"
          )
          return None

      logger.info(
        f"Scanning instance table: {self.instance_table.table_name} for tier: {target_tier}"
      )

      response = self.instance_table.scan(
        FilterExpression="#status = :status AND #tier = :tier",
        ExpressionAttributeNames={"#status": "status", "#tier": "cluster_tier"},
        ExpressionAttributeValues={
          ":status": InstanceStatus.HEALTHY.value,
          ":tier": target_tier,
        },
      )

      instances = response.get("Items", [])

      if not instances:
        return None

      instance_infos = []
      for item in instances:
        instance_id = item["instance_id"]

        if (exclude_instance and instance_id == exclude_instance) or (
          exclude and instance_id in exclude
        ):
          continue

        # See `_count_allocated_graphs`; `database_count` remains only the
        # atomic race guard in allocation STEP 2.
        database_count = self._count_allocated_graphs(instance_id)
        max_databases = int(item.get("max_databases", self.max_databases_per_instance))

        if database_count < max_databases:
          instance_infos.append(
            InstanceInfo(
              instance_id=instance_id,
              private_ip=item["private_ip"],
              availability_zone=item.get("availability_zone", "unknown"),
              status=InstanceStatus.HEALTHY,
              database_count=database_count,
              max_databases=max_databases,
              created_at=datetime.fromisoformat(item["created_at"]),
            )
          )

      if not instance_infos:
        logger.warning(f"No {target_tier} tier instances with available capacity found")
        return None

      best_instance = max(instance_infos, key=lambda x: x.available_capacity)
      logger.info(
        f"Selected {target_tier} tier instance {best_instance.instance_id} with {best_instance.available_capacity} available capacity"
      )
      return best_instance

    except ClientError as e:
      logger.error(f"Error finding best instance: {e}")
      return None

  def _get_stack_name_for_tier(self, tier: str) -> str | None:
    """Get the CloudFormation stack name for a given tier and environment."""
    if self.environment not in ["prod", "staging"]:
      return None

    env_suffix = self.environment.capitalize()
    # Stack names match deploy-graph.yml: RoboSystemsGraph{StackSuffix}{Env}
    suffix_map = {
      "ladybug-standard": "LadybugStandard",
      "ladybug-large": "LadybugLarge",
      "ladybug-xlarge": "LadybugXlarge",
      "ladybug-shared": "LadybugShared",
    }
    suffix = suffix_map.get(tier)
    if not suffix:
      return None
    return f"RoboSystemsGraph{suffix}{env_suffix}"

  async def _get_asg_name_for_instance(self, instance_id: str) -> str | None:
    """Get the ASG name for a specific instance from DynamoDB registry."""
    try:
      response = self.instance_table.get_item(Key={"instance_id": instance_id})

      if "Item" not in response:
        logger.warning(f"Instance {instance_id} not found in registry")
        return None

      item = response["Item"]

      cluster_tier = item.get("cluster_tier", "ladybug-standard")
      if self.environment in ["prod", "staging"]:
        return f"robosystems-{cluster_tier}-writers-{self.environment}-asg"

      return self.default_asg_name

    except ClientError as e:
      logger.error(f"Error getting ASG name for instance {instance_id}: {e}")
      return None

  async def _publish_allocation_metrics(self):
    """Publish allocation metrics to CloudWatch (only in prod/staging)."""
    if self.environment in ["dev", "test"]:
      return

    try:
      metrics = await self.get_allocation_metrics()

      if "error" in metrics:
        return

      utilization_percent = metrics["overall_utilization_percent"]

      metric_data = [
        {
          "MetricName": "DatabaseUtilizationPercent",
          "Value": utilization_percent,
          "Unit": "Percent",
          "Dimensions": [
            {"Name": "NodeType", "Value": "writer"},
          ],
        },
        {
          "MetricName": "TotalDatabaseCount",
          "Value": metrics["total_databases"],
          "Unit": "Count",
          "Dimensions": [
            {"Name": "NodeType", "Value": "writer"},
          ],
        },
      ]

      # Publish to environment-specific Graph namespace
      namespace = f"RoboSystems/Graph/{self.environment}"
      self.cloudwatch.put_metric_data(Namespace=namespace, MetricData=metric_data)

    except ClientError as e:
      logger.error(f"Error publishing metrics: {e}")

  async def _publish_failure_metric(
    self, failure_reason: str, entity_id: str, user_id: str | None = None
  ):
    """Publish an allocation failure metric (prod/staging only).

    Emitted under two dimension sets on purpose: CloudWatch matches alarms on
    the exact dimension set, and ``AllocationFailureAlarm`` in
    ``graph-ladybug.yaml`` watches ``Environment``, while ``FailureReason`` is
    the one useful in triage.
    """
    if self.environment in ["dev", "test"]:
      return

    try:
      namespace = f"RoboSystems/Graph/{self.environment}"
      metric_data = [
        {
          "MetricName": "AllocationFailures",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [
            {"Name": "FailureReason", "Value": failure_reason},
          ],
        },
        {
          "MetricName": "AllocationFailures",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [
            {"Name": "Environment", "Value": self.environment},
          ],
        },
      ]
      self.cloudwatch.put_metric_data(Namespace=namespace, MetricData=metric_data)
      logger.warning(
        f"Allocation failure published: reason={failure_reason}, "
        f"entity={entity_id}, user={user_id}"
      )
    except Exception as e:
      logger.error(f"Failed to publish failure metric: {e}")

  def _resolve_instance_volume(self, instance_id: str) -> str | None:
    """The volume this instance's registry row names, or VolumeNotResolvedError.

    Only the instance's own row counts, in any of its live states. Guessing a
    volume by AZ and tier records the graph on another writer's volume. None
    outside prod/staging, where the volume registry does not exist.
    """
    if self.environment in ["dev", "test"]:
      return None

    # Page cap so a pathological table can't spin here forever.
    MAX_PAGES = 100
    items: list[dict[str, Any]] = []
    last_evaluated_key = None
    for _ in range(MAX_PAGES):
      scan_params: dict[str, Any] = {
        "FilterExpression": "instance_id = :iid AND #status IN (:a, :e, :o)",
        "ExpressionAttributeNames": {"#status": "status"},
        "ExpressionAttributeValues": {
          ":iid": instance_id,
          ":a": "attached",
          ":e": "expanding",
          ":o": "optimizing",
        },
      }
      if last_evaluated_key:
        scan_params["ExclusiveStartKey"] = last_evaluated_key
      response = self.volume_table.scan(**scan_params)
      items.extend(response.get("Items", []))
      last_evaluated_key = response.get("LastEvaluatedKey")
      if not last_evaluated_key:
        break

    if len(items) != 1:
      logger.critical(
        f"Volume for instance {instance_id} not resolved: "
        f"{len(items)} live registry rows"
      )
      raise VolumeNotResolvedError(
        f"Instance {instance_id} has {len(items)} live volume registry rows; "
        "refusing to record a database without exactly one."
      )
    return str(items[0]["volume_id"])

  async def _pick_writer(
    self, instance_tier: GraphTier | None, excluded: set[str]
  ) -> tuple[InstanceInfo, str | None] | None:
    """The best writer whose volume resolves, and that volume.

    A writer whose volume row is stuck is skipped, not fatal: on dedicated
    tiers every empty writer ties, and the same one would win every time.
    Skipped writers are added to `excluded`.
    """
    while True:
      instance = await self._find_best_instance(instance_tier, exclude=excluded)
      if not instance:
        return None
      try:
        return instance, self._resolve_instance_volume(instance.instance_id)
      except VolumeNotResolvedError:
        excluded.add(instance.instance_id)

  async def _update_volume_registry_add_database(
    self, volume_id: str | None, graph_id: str
  ) -> None:
    """Record a database against a volume. None (dev/test) is a no-op."""
    if volume_id is None:
      return

    # list_append is atomic, so concurrent writers don't lose each other's
    # entries; the ConditionExpression makes a duplicate add a no-op.
    try:
      self.volume_table.update_item(
        Key={"volume_id": volume_id},
        UpdateExpression="SET databases = list_append(if_not_exists(databases, :empty), :new_db), last_updated = :timestamp",
        ConditionExpression="attribute_not_exists(databases) OR NOT contains(databases, :gid)",
        ExpressionAttributeValues={
          ":empty": [],
          ":new_db": [graph_id],
          ":gid": graph_id,
          ":timestamp": datetime.now(UTC).isoformat(),
        },
      )
      logger.info(f"Added database {graph_id} to volume {volume_id} registry")
    except ClientError as e:
      if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
        logger.info(f"Database {graph_id} already in volume {volume_id} registry")
        return
      logger.critical(
        f"Failed to add database {graph_id} to volume {volume_id} registry: {e}. "
        "This will cause database loss on ASG refresh!"
      )

  async def _update_volume_registry_remove_database(
    self, instance_id: str, graph_id: str
  ) -> None:
    """Remove a database from an instance's volume registry."""
    if self.environment in ["dev", "test"]:
      logger.debug(
        f"Skipping volume registry removal in {self.environment} environment"
      )
      return

    try:
      MAX_PAGES = 100
      items = []
      last_evaluated_key = None
      pages_scanned = 0

      while pages_scanned < MAX_PAGES:
        scan_params = {
          "FilterExpression": "instance_id = :iid AND #status = :status",
          "ExpressionAttributeNames": {"#status": "status"},
          "ExpressionAttributeValues": {
            ":iid": instance_id,
            ":status": "attached",
          },
        }

        if last_evaluated_key:
          scan_params["ExclusiveStartKey"] = last_evaluated_key

        response = self.volume_table.scan(**scan_params)
        items.extend(response.get("Items", []))
        pages_scanned += 1

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
          break

      if pages_scanned >= MAX_PAGES:
        logger.warning(
          f"Volume registry scan hit safety limit ({MAX_PAGES} pages) for instance {instance_id}"
        )

      if not items:
        logger.warning(
          f"No attached volume found for instance {instance_id} - "
          f"cannot update volume registry for database {graph_id} removal"
        )
        return

      volume_id = items[0]["volume_id"]

      # Remove by index, conditioned on that slot still holding graph_id, so a
      # concurrent add is never overwritten; a shifted list re-reads and retries.
      for _ in range(3):
        item = self.volume_table.get_item(
          Key={"volume_id": volume_id}, ConsistentRead=True
        ).get("Item", {})
        databases = item.get("databases", [])
        if graph_id not in databases:
          logger.debug(f"Database {graph_id} was not in volume {volume_id} registry")
          return
        index = databases.index(graph_id)
        try:
          self.volume_table.update_item(
            Key={"volume_id": volume_id},
            UpdateExpression=f"REMOVE databases[{index}] SET last_updated = :timestamp",
            ConditionExpression=f"databases[{index}] = :gid",
            ExpressionAttributeValues={
              ":gid": graph_id,
              ":timestamp": datetime.now(UTC).isoformat(),
            },
          )
          logger.info(f"Removed database {graph_id} from volume {volume_id} registry")
          return
        except ClientError as e:
          if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise

      logger.warning(
        f"Gave up removing {graph_id} from volume {volume_id} registry "
        f"after repeated concurrent modification"
      )

    except ClientError as e:
      logger.error(
        f"Failed to update volume registry for database {graph_id} removal "
        f"on instance {instance_id}: {e}"
      )


def create_allocation_manager(environment: str = "prod") -> LadybugAllocationManager:
  """Build an allocation manager for an environment."""
  return LadybugAllocationManager(environment=environment)
