"""
LadybugDB Allocation Manager - DynamoDB-based

Manages database allocation across LadybugDB writer instances using DynamoDB for persistent state.
This replaces the in-memory registry with a reliable, distributed storage solution.

Key features:
- DynamoDB-based database registry
- Direct instance routing (no ALB)
- Automatic instance registration/deregistration
- Capacity-based allocation
- Health monitoring via DynamoDB
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


class AllocationRaceConditionError(Exception):
  """Raised when DynamoDB conditional write fails and the existing item cannot be resolved."""


# Valid identifier patterns for security with length limits
VALID_ENTITY_ID_PATTERN = re.compile(
  r"^[a-zA-Z0-9_-]{1,128}$"
)  # Entity IDs: alphanumeric, underscore, dash (max 128 chars)

# Graph ID regex is lazy to avoid circular imports at module load time
_VALID_GRAPH_ID_REGEX: re.Pattern[str] | None = None


def _get_valid_graph_id_regex() -> re.Pattern[str]:
  """Get compiled graph ID regex, building it lazily on first call."""
  global _VALID_GRAPH_ID_REGEX
  if _VALID_GRAPH_ID_REGEX is None:
    from robosystems.middleware.graph.types import GRAPH_ID_PATTERN

    _VALID_GRAPH_ID_REGEX = re.compile(GRAPH_ID_PATTERN)
  return _VALID_GRAPH_ID_REGEX


VALID_INSTANCE_ID_PATTERN = re.compile(r"^i-[0-9a-f]{8,17}$")  # AWS instance ID format


# Configure DynamoDB client based on environment
def get_dynamodb_resource():
  """Get DynamoDB resource with proper endpoint configuration."""
  # Get region from environment or use default
  region = env.AWS_REGION

  # Check if we should use LocalStack in dev environment
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
    # Use environment variable if max_databases_per_instance not explicitly provided
    self.max_databases_per_instance = (
      max_databases_per_instance
      if max_databases_per_instance is not None
      else MultiTenantUtils.get_max_databases_per_node()
    )

    # Tier-based configuration for database allocation.
    # Runtime-specific settings are configured in the corresponding userdata scripts.
    # Memory and chunk size settings are loaded from graph.yml via GraphTierConfig.
    self.tier_configs = {
      GraphTier.LADYBUG_STANDARD: {
        "backend": "ladybug",
        "backend_type": "ladybug",
        "databases_per_instance": self.max_databases_per_instance,  # Dedicated (1 per instance)
      },
      GraphTier.LADYBUG_LARGE: {
        "backend": "ladybug",
        "backend_type": "ladybug",
        "databases_per_instance": 1,  # Dedicated instance (parent + subgraphs)
      },
      GraphTier.LADYBUG_XLARGE: {
        "backend": "ladybug",
        "backend_type": "ladybug",
        "databases_per_instance": 1,  # Large dedicated instance (parent + subgraphs)
      },
      GraphTier.LADYBUG_SHARED: {
        "backend": "ladybug",
        "backend_type": "ladybug",
        "databases_per_instance": 1,  # One repository per instance
      },
    }
    # ASG name will be determined dynamically from instance data
    # This is a fallback for tests/local development
    # Use the standard tier ASG as default
    if asg_name:
      self.default_asg_name = asg_name
    else:
      # Construct ASG name based on environment (kebab-case convention)
      # Validate environment to prevent injection
      if not re.match(r"^[a-z]+$", environment.lower()):
        raise ValueError(f"Invalid environment name: {environment}")
      self.default_asg_name = (
        f"robosystems-ladybug-standard-writers-{environment.lower()}-asg"
      )

    # Get DynamoDB resource with proper endpoint
    dynamodb = cast(Any, get_dynamodb_resource())

    # DynamoDB tables - use centralized configuration
    self.graph_table = dynamodb.Table(env.GRAPH_REGISTRY_TABLE)
    self.instance_table = dynamodb.Table(env.INSTANCE_REGISTRY_TABLE)
    self.volume_table = dynamodb.Table(env.VOLUME_REGISTRY_TABLE)

    # AWS clients with region configuration
    region = env.AWS_REGION

    # Configure clients based on environment
    if env.is_development() and env.AWS_ENDPOINT_URL:
      # LocalStack configuration for dev
      endpoint_url = env.AWS_ENDPOINT_URL
      self.autoscaling = boto3.client(
        "autoscaling", endpoint_url=endpoint_url, region_name=region
      )
      self.cloudwatch = boto3.client(
        "cloudwatch", endpoint_url=endpoint_url, region_name=region
      )
    else:
      # Production configuration
      self.autoscaling = boto3.client("autoscaling", region_name=region)
      self.cloudwatch = boto3.client("cloudwatch", region_name=region)

    logger.info(f"Initialized LadybugAllocationManager for environment: {environment}")

  def get_tier_config(self, tier: GraphTier) -> dict[str, Any]:
    """
    Get configuration for a specific tier.

    Returns backend type and database allocation settings.
    For memory/chunk size settings, use GraphTierConfig from config/graph_tier.py.
    """
    return self.tier_configs.get(tier, self.tier_configs[GraphTier.LADYBUG_STANDARD])

  async def allocate_database(
    self,
    entity_id: str,
    graph_id: str | None = None,
    graph_type: str | None = None,
    instance_tier: GraphTier | None = None,
  ) -> DatabaseLocation:
    """
    Allocate a new database for an entity.

    Args:
        entity_id: Entity identifier (entity ID, user ID, etc.)
        graph_id: Optional custom graph ID
        graph_type: Optional graph type (defaults to auto-detection)
        instance_tier: Optional instance tier override

    Returns:
        DatabaseLocation with instance details
    """
    # Validate entity_id
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

    # Generate graph_id if not provided
    # All user graphs must use kg prefix with ULID for time-ordering
    if not graph_id:
      from robosystems.utils.ulid import generate_ulid_hex

      graph_id = f"kg{generate_ulid_hex(20)}"

    if not _get_valid_graph_id_regex().match(graph_id):
      # Check if this looks like a subgraph ID - provide helpful error
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

      # Generic validation error for other invalid formats
      SecurityAuditLogger.log_input_validation_failure(
        field_name="graph_id",
        invalid_value=graph_id,
        validation_error="Invalid graph ID format",
      )
      raise ValueError(
        f"Invalid graph ID format: {graph_id}. Must be 'kg' followed by 16+ lowercase hex characters or a shared repository name."
      )

    # Check if this is a subgraph - if so, route to parent's allocation
    subgraph_info = parse_subgraph_id(graph_id)
    if subgraph_info:
      logger.info(
        f"Detected subgraph {graph_id} - routing to parent {subgraph_info.parent_graph_id}"
      )

      # Find the parent's allocation
      parent_location = await self.find_database_location(subgraph_info.parent_graph_id)
      if not parent_location:
        raise ValueError(
          f"Parent graph {subgraph_info.parent_graph_id} not found. "
          f"Cannot create subgraph without parent allocation."
        )

      # Return parent's location but with subgraph ID
      # The actual database creation happens at a higher level
      logger.info(
        f"Subgraph {graph_id} will use parent's instance {parent_location.instance_id} "
        f"({parent_location.private_ip})"
      )

      # Track subgraph in volume registry (subgraphs are real databases on disk)
      await self._update_volume_registry_add_database(
        parent_location.instance_id, graph_id
      )

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
      # Get graph identity for routing
      identity = GraphTypeRegistry.identify_graph(graph_id, graph_tier=instance_tier)

      # Get backend type for this tier
      tier_config = self.get_tier_config(instance_tier or GraphTier.LADYBUG_STANDARD)
      backend_type = tier_config.get("backend_type", "ladybug")

      # Find instance with capacity for the specified tier (do this first to fail fast)
      instance = await self._find_best_instance(instance_tier)

      if not instance:
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

      # Atomic allocation using DynamoDB conditional writes
      now = datetime.now(UTC)
      max_retries = 3
      retry_count = 0

      while retry_count < max_retries:
        try:
          # STEP 1: Atomically create database entry with condition that it doesn't exist
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
              "allocation_lock": f"allocated_by_{now.timestamp()}",  # Allocation tracking
            },
            ConditionExpression="attribute_not_exists(graph_id)",  # ATOMIC: Only if database doesn't exist
          )

          # STEP 2: Atomically increment instance count with capacity check
          try:
            self.instance_table.update_item(
              Key={"instance_id": instance.instance_id},
              UpdateExpression="ADD database_count :inc SET last_allocation = :timestamp",
              ConditionExpression="database_count < max_databases",  # ATOMIC: Only if capacity available
              ExpressionAttributeValues={
                ":inc": 1,
                ":timestamp": now.isoformat(),
              },
            )

            # Both operations succeeded - allocation complete
            break

          except ClientError as capacity_error:
            if (
              capacity_error.response["Error"]["Code"]
              == "ConditionalCheckFailedException"
            ):
              # Instance is now at capacity - rollback database creation and retry with different instance
              logger.warning(
                f"Instance {instance.instance_id} reached capacity during allocation, rolling back"
              )

              # Rollback: Delete the database entry we just created
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

              # Find a different instance and retry
              instance = await self._find_best_instance(
                instance_tier, exclude_instance=instance.instance_id
              )
              if not instance:
                raise Exception("No available instances after capacity conflict")

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
              # Different error - re-raise
              raise capacity_error

        except ClientError as e:
          if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            # Database already exists - this is a graph_id collision
            try:
              response = self.graph_table.get_item(Key={"graph_id": graph_id})
              if "Item" in response:
                item = response["Item"]
                existing_entity = item.get("entity_id", "unknown")
                if existing_entity == entity_id:
                  # Same entity retrying - safe to return existing allocation
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
                  # Different entity - graph_id collision, must not share allocation
                  logger.error(
                    f"Graph ID collision: {graph_id} already belongs to entity {existing_entity}, "
                    f"requested by entity {entity_id}"
                  )
                  raise GraphIDCollisionError(
                    f"Graph ID {graph_id} already exists (owned by a different entity)."
                  )
              else:
                # Shouldn't happen - conditional check failed but item doesn't exist
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
            # Different DynamoDB error
            raise e

      # Log successful database allocation
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTH_SUCCESS,  # Could add DATABASE_ALLOCATED
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
        risk_level="medium",
      )

      logger.info(
        f"Allocated {graph_id} to instance {instance.instance_id} ({instance.private_ip}) "
        f"- tier: {instance_tier.value if instance_tier else 'ladybug-standard'}, "
        f"entity: {entity_id}"
      )

      # Update volume registry to track database on volume (critical for instance replacement)
      await self._update_volume_registry_add_database(instance.instance_id, graph_id)

      # Enable instance protection now that it has a database (only in prod/staging)
      if self.environment not in ["dev", "test"]:
        try:
          # Get the ASG name from the instance data
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
          # Log but don't fail allocation - protection is a safety feature
          logger.error(f"Failed to enable instance protection: {e}")

        # Publish metrics (only in prod/staging)
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
      # Log database allocation failure
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
    """
    Find the location of an existing database.

    For subgraphs, this method automatically resolves to the parent graph's location
    since subgraphs share the same physical instance as their parent.

    Args:
        graph_id: Graph/database identifier (can be parent or subgraph)

    Returns:
        DatabaseLocation if found, None otherwise
    """
    # Check if this is a subgraph - if so, look up parent instead
    subgraph_info = parse_subgraph_id(graph_id)
    if subgraph_info:
      logger.debug(
        f"Resolving subgraph {graph_id} to parent {subgraph_info.parent_graph_id}"
      )
      # Recursively call with parent ID to get parent's location
      parent_location = await self.find_database_location(subgraph_info.parent_graph_id)
      if not parent_location:
        return None

      # Return location with subgraph's graph_id but parent's instance details
      return DatabaseLocation(
        graph_id=graph_id,
        instance_id=parent_location.instance_id,
        private_ip=parent_location.private_ip,
        availability_zone=parent_location.availability_zone,
        created_at=parent_location.created_at,
        status=parent_location.status,
        backend_type=parent_location.backend_type,
      )

    # Parent graph or shared repository - look up in DynamoDB
    try:
      response = self.graph_table.get_item(Key={"graph_id": graph_id})

      if "Item" not in response:
        return None

      item = response["Item"]
      instance_id = item["instance_id"]

      # Get private_ip - first try graph-registry, then fall back to instance-registry
      # This handles cases where graph-registry entry is stale after instance replacement
      private_ip = item.get("private_ip")
      availability_zone = item.get("availability_zone", "unknown")

      if not private_ip:
        # Look up private_ip from instance-registry (source of truth for instance info)
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

          # Update graph-registry with current instance info for faster future lookups
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

      # Update last accessed time
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
    """
    Atomically deallocate a database using conditional writes.

    Args:
        graph_id: Database to deallocate

    Returns:
        True if successful
    """
    logger.info(f"Deallocating database {graph_id}")

    try:
      # Get database info first to validate it exists and get instance_id
      response = self.graph_table.get_item(Key={"graph_id": graph_id})
      if "Item" not in response:
        logger.warning(f"Database {graph_id} not found")
        return False

      item = response["Item"]
      instance_id = item["instance_id"]
      current_status = item.get("status", DatabaseStatus.ACTIVE.value)

      # Skip if already deleted
      if current_status == DatabaseStatus.DELETED.value:
        logger.info(f"Database {graph_id} already deleted")
        return True

      deallocation_timestamp = datetime.now(UTC).isoformat()

      # STEP 1: Atomically mark database as deleted (only if not already deleted)
      try:
        self.graph_table.update_item(
          Key={"graph_id": graph_id},
          UpdateExpression="SET #status = :deleted_status, deleted_at = :time, deallocation_lock = :lock_id",
          ConditionExpression="#status <> :deleted_status",  # ATOMIC: Only if not already deleted
          ExpressionAttributeNames={"#status": "status"},
          ExpressionAttributeValues={
            ":deleted_status": DatabaseStatus.DELETED.value,
            ":time": deallocation_timestamp,
            ":lock_id": f"deallocated_by_{deallocation_timestamp}",
          },
        )
      except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
          # Database already deleted by another process
          logger.info(f"Database {graph_id} was already deleted by another process")
          return True
        else:
          raise e

      # STEP 2: Atomically decrement instance count (only if count > 0)
      try:
        self.instance_table.update_item(
          Key={"instance_id": instance_id},
          UpdateExpression="ADD database_count :dec SET last_deallocation = :timestamp",
          ConditionExpression="database_count > :zero",  # ATOMIC: Only if count > 0
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
          # Instance count is already 0 - this shouldn't happen but handle gracefully
          logger.warning(
            f"Instance {instance_id} database count was already 0 during deallocation"
          )

          # Log this as a potential integrity issue but don't fail the deallocation
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
          # Unexpected error - try to rollback database status change
          logger.error(
            f"Failed to decrement database count for {instance_id}: {capacity_error}"
          )

          try:
            # Rollback: Change database status back to active
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

      # Log successful database deallocation
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTHORIZATION_DENIED,  # Could add DATABASE_DEALLOCATED
        details={
          "action": "database_deallocated",
          "graph_id": graph_id,
          "instance_id": instance_id,
          "entity_id": item.get("entity_id"),
          "deallocated_at": datetime.now(UTC).isoformat(),
        },
        risk_level="high",
      )

      logger.info(f"Deallocated database {graph_id} from instance {instance_id}")

      # Update volume registry to remove database from volume tracking
      await self._update_volume_registry_remove_database(instance_id, graph_id)

      # Check if instance now has zero databases and remove protection if so (only in prod/staging)
      if self.environment not in ["dev", "test"]:
        try:
          response = self.instance_table.get_item(Key={"instance_id": instance_id})
          if "Item" in response:
            current_count = int(response["Item"].get("database_count", 0))
            if current_count == 0:
              # Remove instance protection since it has no databases
              try:
                # Get the ASG name from the instance data
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
                # Log but don't fail deallocation
                logger.error(f"Failed to remove instance protection: {e}")
        except ClientError as e:
          logger.error(f"Failed to check instance database count: {e}")

        # Publish metrics (only in prod/staging)
        await self._publish_allocation_metrics()

      return True

    except ClientError as e:
      # Log database deallocation failure
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
    """
    Get all databases on a specific instance with input validation.

    Args:
        instance_id: EC2 instance ID

    Returns:
        List of graph IDs on the instance

    Raises:
        ValueError: If input validation fails
    """
    # Validate instance_id
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
    """
    Get all healthy instances with their metadata.

    Returns:
        List of instance dictionaries with instance_id, private_ip, status, etc.
    """
    try:
      # Scan all healthy instances
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
      # Get all healthy instances
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
    """
    Check capacity status for a tier.

    Returns 'ready', 'scalable', or 'at_capacity'.

    - ready: Open slot on a healthy instance
    - scalable: No slots but ASG has headroom to add instances
    - at_capacity: No slots and ASG is at max
    """
    instance = await self._find_best_instance(tier)
    if instance:
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
    """Count graphs allocated to an instance according to the graph registry.

    The graph registry is the authoritative allocation record — rows are
    created with conditional writes at allocation time and re-pointed by the
    volume-manager Lambda when instances cycle. The instance registry's
    denormalized database_count is reset when a replacement instance
    re-registers after ASG cycling, so capacity decisions must not trust it
    (a drifted zero makes an occupied dedicated writer look empty and gets
    it double-booked).
    """
    from boto3.dynamodb.conditions import Key

    occupying_statuses = {
      DatabaseStatus.ACTIVE.value,
      DatabaseStatus.CREATING.value,
      DatabaseStatus.MIGRATING.value,
    }
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
        if item.get("status") in occupying_statuses
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
  ) -> InstanceInfo | None:
    """Find the instance with most available capacity for the specified tier."""
    try:
      # Default to standard tier if not specified
      target_tier = (
        instance_tier.value if instance_tier else GraphTier.LADYBUG_STANDARD.value
      )

      # Validate tier is supported in this environment
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

        # Check if the tier's ASG exists (especially for optional tiers in prod)
        stack_name = self._get_stack_name_for_tier(target_tier)
        if not stack_name:
          logger.warning(
            f"No stack configured for tier {target_tier} in {self.environment}"
          )
          return None

      logger.info(
        f"Scanning instance table: {self.instance_table.table_name} for tier: {target_tier}"
      )

      # Scan for healthy instances with the specified tier
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

      # Convert to InstanceInfo objects and filter by available capacity
      instance_infos = []
      for item in instances:
        instance_id = item["instance_id"]

        # Skip excluded instance if specified
        if exclude_instance and instance_id == exclude_instance:
          continue

        # Occupancy comes from the graph registry, not the instance
        # registry's database_count — that counter resets to a stale value
        # when a replacement instance re-registers after ASG cycling, which
        # would make an occupied dedicated writer look empty. The counter is
        # still used as the atomic race guard during allocation (STEP 2).
        database_count = self._count_allocated_graphs(instance_id)
        max_databases = int(item.get("max_databases", self.max_databases_per_instance))

        # Only include instances with available capacity
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

      # Return instance with most available capacity
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

      # Construct ASG name from tier and environment (kebab-case convention)
      cluster_tier = item.get("cluster_tier", "ladybug-standard")
      if self.environment in ["prod", "staging"]:
        return f"robosystems-{cluster_tier}-writers-{self.environment}-asg"

      # Fallback for development/test environments
      return self.default_asg_name

    except ClientError as e:
      logger.error(f"Error getting ASG name for instance {instance_id}: {e}")
      return None

  async def _publish_allocation_metrics(self):
    """Publish allocation metrics to CloudWatch (only in prod/staging)."""
    # Skip metrics in dev/test environments
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
    """Publish allocation failure metric to CloudWatch (only in prod/staging)."""
    # Skip metrics in dev/test environments
    if self.environment in ["dev", "test"]:
      return

    try:
      # Use environment-specific namespace instead of Environment dimension
      namespace = f"RoboSystems/Graph/{self.environment}"
      metric_data = [
        {
          "MetricName": "AllocationFailures",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [
            {"Name": "FailureReason", "Value": failure_reason},
          ],
        }
      ]
      self.cloudwatch.put_metric_data(Namespace=namespace, MetricData=metric_data)
      logger.warning(
        f"Allocation failure published: reason={failure_reason}, "
        f"entity={entity_id}, user={user_id}"
      )
    except Exception as e:
      logger.error(f"Failed to publish failure metric: {e}")

  async def _update_volume_registry_add_database(
    self, instance_id: str, graph_id: str
  ) -> None:
    """
    Add a database to the volume registry for the given instance.

    This ensures the volume registry tracks which databases exist on each volume,
    which is CRITICAL for proper volume reattachment during instance replacement.
    Without this, ASG refreshes will lose track of databases on the volume.
    """
    # Skip in dev/test environments where volume registry may not exist
    if self.environment in ["dev", "test"]:
      logger.debug(f"Skipping volume registry update in {self.environment} environment")
      return

    logger.info(
      f"Updating volume registry: adding database {graph_id} for instance {instance_id}"
    )

    try:
      # Find the volume attached to this instance using paginated scan
      # Safety limits to prevent infinite loops
      MAX_PAGES = 100  # Volume registry should never have this many pages
      all_items = []
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
        all_items.extend(response.get("Items", []))
        pages_scanned += 1

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
          break

      if pages_scanned >= MAX_PAGES:
        logger.warning(
          f"Volume registry scan hit safety limit ({MAX_PAGES} pages) for instance {instance_id}"
        )

      if not all_items:
        # Try alternative: look up volume from instance registry
        logger.warning(
          f"No attached volume found via scan for instance {instance_id}, "
          f"trying instance registry lookup"
        )

        # Check instance registry for volume info
        try:
          instance_response = self.instance_table.get_item(
            Key={"instance_id": instance_id}
          )
          if "Item" in instance_response:
            instance_item = instance_response["Item"]
            # The instance might have volume info cached
            logger.info(
              f"Instance {instance_id} found in instance registry, "
              f"AZ: {instance_item.get('availability_zone')}, tier: {instance_item.get('tier')}"
            )

            # Try to find volume by AZ and tier
            az = instance_item.get("availability_zone")
            tier = instance_item.get("tier")
            if az and tier:
              vol_response = self.volume_table.scan(
                FilterExpression="availability_zone = :az AND tier = :tier AND #status = :status",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                  ":az": az,
                  ":tier": tier,
                  ":status": "attached",
                },
              )
              vol_items = vol_response.get("Items", [])
              if vol_items:
                all_items = vol_items
                logger.info(
                  f"Found volume {vol_items[0]['volume_id']} via AZ/tier lookup for instance {instance_id}"
                )
        except Exception as lookup_error:
          logger.warning(f"Instance registry lookup failed: {lookup_error}")

      if not all_items:
        logger.critical(
          f"No attached volume found for instance {instance_id} - "
          f"cannot update volume registry for database {graph_id}. "
          f"This will cause database loss on ASG refresh!"
        )
        return

      volume_id = all_items[0]["volume_id"]
      logger.info(f"Found volume {volume_id} for instance {instance_id}")

      # Use atomic list_append to avoid lost updates from concurrent writes
      # ConditionExpression prevents duplicates; ConditionalCheckFailedException means already exists
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
        logger.info(
          f"Successfully added database {graph_id} to volume {volume_id} registry"
        )
      except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
          logger.info(
            f"Database {graph_id} already in volume {volume_id} registry (no update needed)"
          )
        else:
          raise

    except ClientError as e:
      logger.critical(
        f"Failed to update volume registry for database {graph_id} "
        f"on instance {instance_id}: {e}. This will cause database loss on ASG refresh!"
      )
      # Note: We don't fail the allocation, but this is a critical issue that needs attention

  async def _update_volume_registry_remove_database(
    self, instance_id: str, graph_id: str
  ) -> None:
    """
    Remove a database from the volume registry for the given instance.

    This ensures the volume registry accurately reflects which databases exist
    on each volume after deletion.
    """
    # Skip in dev/test environments where volume registry may not exist
    if self.environment in ["dev", "test"]:
      logger.debug(
        f"Skipping volume registry removal in {self.environment} environment"
      )
      return

    try:
      # Find the volume attached to this instance using paginated scan
      # Safety limits to prevent infinite loops
      MAX_PAGES = 100  # Volume registry should never have this many pages
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
      current_databases = items[0].get("databases", [])

      # Remove database if in list
      # Use conditional write with expected state to detect concurrent modifications
      if graph_id in current_databases:
        updated_databases = [db for db in current_databases if db != graph_id]
        try:
          self.volume_table.update_item(
            Key={"volume_id": volume_id},
            UpdateExpression="SET databases = :dbs, last_updated = :timestamp",
            ConditionExpression="contains(databases, :gid)",
            ExpressionAttributeValues={
              ":dbs": updated_databases,
              ":gid": graph_id,
              ":timestamp": datetime.now(UTC).isoformat(),
            },
          )
          logger.info(
            f"Removed database {graph_id} from volume {volume_id} registry "
            f"(now has {len(updated_databases)} databases)"
          )
        except ClientError as e:
          if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            # Concurrent modification - database was already removed or list changed
            logger.debug(
              f"Database {graph_id} already removed from volume {volume_id} registry "
              f"(concurrent modification detected)"
            )
          else:
            raise
      else:
        logger.debug(f"Database {graph_id} was not in volume {volume_id} registry")

    except ClientError as e:
      logger.error(
        f"Failed to update volume registry for database {graph_id} removal "
        f"on instance {instance_id}: {e}"
      )
      # Don't fail the deallocation - volume registry is supplementary


# Factory function for compatibility
def create_allocation_manager(environment: str = "prod") -> LadybugAllocationManager:
  """
  Create allocation manager for the specified environment.

  Always uses DynamoDB-based allocation (with LocalStack in development).
  """
  return LadybugAllocationManager(environment=environment)
