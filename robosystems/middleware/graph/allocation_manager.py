"""
Kuzu Database Allocation Manager V2 - DynamoDB-based

Manages database allocation across Kuzu writer instances using DynamoDB for persistent state.
This replaces the in-memory registry with a reliable, distributed storage solution.

Key features:
- DynamoDB-based database registry
- Direct instance routing (no ALB)
- Automatic instance registration/deregistration
- Capacity-based allocation
- Health monitoring via DynamoDB
"""

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, cast
from dataclasses import dataclass
from enum import Enum
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

from robosystems.logger import logger
from robosystems.security import SecurityAuditLogger, SecurityEventType
from robosystems.middleware.graph.types import GraphTypeRegistry, GraphTier
from .multitenant_utils import MultiTenantUtils
from .subgraph_utils import parse_subgraph_id
from robosystems.config import env

# Valid identifier patterns for security with length limits
VALID_ENTITY_ID_PATTERN = re.compile(
  r"^[a-zA-Z0-9_-]{1,128}$"
)  # Entity IDs: alphanumeric, underscore, dash (max 128 chars)
VALID_GRAPH_ID_PATTERN = re.compile(
  r"^[a-zA-Z0-9_-]{1,128}$"
)  # Graph IDs: alphanumeric, underscore, dash (max 128 chars)
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
  backend_type: str = "kuzu"


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


class KuzuAllocationManager:
  """DynamoDB-based allocation manager for graph databases."""

  def __init__(
    self,
    environment: str,
    max_databases_per_instance: Optional[int] = None,
    asg_name: Optional[str] = None,
  ):
    self.environment = environment
    # Use environment variable if max_databases_per_instance not explicitly provided
    self.max_databases_per_instance = (
      max_databases_per_instance
      if max_databases_per_instance is not None
      else MultiTenantUtils.get_max_databases_per_node()
    )

    # Tier-based configuration for backend selection and database allocation
    # Note: Backend-specific settings (Kuzu buffer pools, Neo4j JVM heap) are
    # configured in their respective userdata scripts, not here.
    self.tier_configs = {
      GraphTier.KUZU_STANDARD: {
        "backend": "kuzu",
        "backend_type": "kuzu",
        "databases_per_instance": self.max_databases_per_instance,  # Multi-tenant (10 per instance)
        "kuzu_max_memory_mb": env.KUZU_STANDARD_MAX_MEMORY_MB,
        "kuzu_chunk_size": env.KUZU_STANDARD_CHUNK_SIZE,
      },
      GraphTier.KUZU_LARGE: {
        "backend": "kuzu",
        "backend_type": "kuzu",
        "databases_per_instance": 1,  # Dedicated instance (parent + subgraphs)
        "kuzu_max_memory_mb": env.KUZU_STANDARD_MAX_MEMORY_MB,  # Same as standard (r7g.large)
        "kuzu_chunk_size": env.KUZU_STANDARD_CHUNK_SIZE,
      },
      GraphTier.KUZU_XLARGE: {
        "backend": "kuzu",
        "backend_type": "kuzu",
        "databases_per_instance": 1,  # Large dedicated instance (parent + subgraphs)
        "kuzu_max_memory_mb": env.KUZU_STANDARD_MAX_MEMORY_MB,  # r7g.xlarge has more memory
        "kuzu_chunk_size": env.KUZU_STANDARD_CHUNK_SIZE,
      },
      GraphTier.KUZU_SHARED: {
        "backend": "kuzu",
        "backend_type": "kuzu",
        "databases_per_instance": 1,  # One repository per instance
        "kuzu_max_memory_mb": env.KUZU_STANDARD_MAX_MEMORY_MB,
        "kuzu_chunk_size": env.KUZU_STANDARD_CHUNK_SIZE,
      },
      GraphTier.NEO4J_COMMUNITY_LARGE: {
        "backend": "neo4j",
        "backend_type": "neo4j",
        "neo4j_edition": "community",
        "databases_per_instance": 1,  # Dedicated instance (no subgraphs - restricted to 1 DB)
      },
      GraphTier.NEO4J_ENTERPRISE_XLARGE: {
        "backend": "neo4j",
        "backend_type": "neo4j",
        "neo4j_edition": "enterprise",
        "databases_per_instance": 1,  # Dedicated instance (parent + subgraphs)
      },
    }
    # ASG name will be determined dynamically from instance data
    # This is a fallback for tests/local development
    # Use the standard tier ASG as default
    if asg_name:
      self.default_asg_name = asg_name
    else:
      # Construct a realistic ASG name based on environment
      # Validate environment to prevent injection
      if not re.match(r"^[a-z]+$", environment.lower()):
        raise ValueError(f"Invalid environment name: {environment}")
      env_capitalized = environment.capitalize()
      self.default_asg_name = (
        f"RoboSystemsGraphWritersStandard{env_capitalized}-writers-asg"
      )

    # Get DynamoDB resource with proper endpoint
    dynamodb = cast(Any, get_dynamodb_resource())

    # Rate limiting for scale-up triggers (one per 5 minutes per tier)
    self._scale_up_timestamps: Dict[str, datetime] = {}

    # DynamoDB tables - use centralized configuration
    self.graph_table = dynamodb.Table(env.GRAPH_REGISTRY_TABLE)
    self.instance_table = dynamodb.Table(env.INSTANCE_REGISTRY_TABLE)

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

    logger.info(f"Initialized KuzuAllocationManagerV2 for environment: {environment}")

  def get_tier_config(self, tier: GraphTier) -> Dict[str, Any]:
    """
    Get configuration for a specific tier.

    Returns memory limits and chunk sizes optimized for each tier.
    """
    return self.tier_configs.get(tier, self.tier_configs[GraphTier.KUZU_STANDARD])

  async def allocate_database(
    self,
    entity_id: str,
    graph_id: Optional[str] = None,
    graph_type: Optional[str] = None,
    instance_tier: Optional[GraphTier] = None,
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
    # All user graphs must use kg prefix with UUID for security
    if not graph_id:
      import uuid

      graph_id = f"kg{uuid.uuid4().hex[:16]}"

    if not VALID_GRAPH_ID_PATTERN.match(graph_id):
      SecurityAuditLogger.log_input_validation_failure(
        field_name="graph_id",
        invalid_value=graph_id,
        validation_error="Invalid graph ID format",
      )
      raise ValueError(
        f"Invalid graph ID format: {graph_id}. Must contain only alphanumeric characters, underscores, and dashes."
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

      return DatabaseLocation(
        graph_id=graph_id,
        instance_id=parent_location.instance_id,
        private_ip=parent_location.private_ip,
        availability_zone=parent_location.availability_zone,
        created_at=datetime.now(timezone.utc),
        status=DatabaseStatus.ACTIVE,
        backend_type=parent_location.backend_type,
      )

    logger.info(f"Allocating database {graph_id} for entity {entity_id}")

    try:
      # Get graph identity for routing
      identity = GraphTypeRegistry.identify_graph(graph_id, instance_tier)

      # Get backend type for this tier
      tier_config = self.get_tier_config(instance_tier or GraphTier.KUZU_STANDARD)
      backend_type = tier_config.get("backend_type", "kuzu")

      # Find instance with capacity for the specified tier (do this first to fail fast)
      instance = await self._find_best_instance(instance_tier)

      if not instance:
        # No capacity - trigger scale up or provide tier-specific error
        if instance_tier and instance_tier != GraphTier.KUZU_STANDARD:
          # Dedicated tiers require manual provisioning
          tier_name = instance_tier.value.replace("-", " ").title()
          await self._publish_failure_metric(
            f"no_{instance_tier.value}_capacity", entity_id, None
          )
          raise Exception(
            f"No {tier_name} tier capacity available. "
            f"{tier_name} infrastructure requires manual provisioning. "
            f"Please contact support to request "
            f"dedicated {tier_name} infrastructure for your account."
          )
        else:
          # Standard tier - attempt auto-scaling
          await self._trigger_scale_up(instance_tier)
          # Publish allocation failure metric
          await self._publish_failure_metric("no_capacity", entity_id, None)
          raise Exception(
            "No Kuzu Standard tier capacity available. Our system is automatically scaling up to meet demand. "
            "Please retry in 3-5 minutes. If this persists, contact support."
          )

      # Atomic allocation using DynamoDB conditional writes
      now = datetime.now(timezone.utc)
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
            # Database already exists - check if we can return existing allocation
            try:
              response = self.graph_table.get_item(Key={"graph_id": graph_id})
              if "Item" in response:
                item = response["Item"]
                logger.info(
                  f"Database {graph_id} already exists on instance {item['instance_id']} (concurrent allocation detected)"
                )
                return DatabaseLocation(
                  graph_id=graph_id,
                  instance_id=item["instance_id"],
                  private_ip=item["private_ip"],
                  availability_zone=item.get("availability_zone", "unknown"),
                  created_at=datetime.fromisoformat(item["created_at"]),
                  status=DatabaseStatus(item.get("status", "active")),
                  backend_type=item.get("backend_type", "kuzu"),
                )
              else:
                # Shouldn't happen - conditional check failed but item doesn't exist
                logger.error(
                  f"Conditional check failed but database {graph_id} not found"
                )
                raise Exception("Database allocation failed due to race condition")
            except ClientError as lookup_error:
              logger.error(
                f"Failed to lookup existing database after conditional check failure: {lookup_error}"
              )
              raise Exception("Database allocation failed and lookup failed")
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
        f"- tier: {instance_tier.value if instance_tier else 'kuzu-standard'}, "
        f"entity: {entity_id}"
      )

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
      raise Exception(f"Database allocation failed: {str(e)}")

  async def find_database_location(self, graph_id: str) -> Optional[DatabaseLocation]:
    """
    Find the location of an existing database.

    Args:
        graph_id: Graph/database identifier

    Returns:
        DatabaseLocation if found, None otherwise
    """
    try:
      response = self.graph_table.get_item(Key={"graph_id": graph_id})

      if "Item" not in response:
        return None

      item = response["Item"]

      # Update last accessed time
      self.graph_table.update_item(
        Key={"graph_id": graph_id},
        UpdateExpression="SET last_accessed = :time",
        ExpressionAttributeValues={":time": datetime.now(timezone.utc).isoformat()},
      )

      return DatabaseLocation(
        graph_id=graph_id,
        instance_id=item["instance_id"],
        private_ip=item["private_ip"],
        availability_zone=item.get("availability_zone", "unknown"),
        created_at=datetime.fromisoformat(item["created_at"]),
        status=DatabaseStatus(item.get("status", "active")),
        backend_type=item.get("backend_type", "kuzu"),
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

      deallocation_timestamp = datetime.now(timezone.utc).isoformat()

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
          "deallocated_at": datetime.now(timezone.utc).isoformat(),
        },
        risk_level="high",
      )

      logger.info(f"Deallocated database {graph_id} from instance {instance_id}")

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

  async def get_instance_databases(self, instance_id: str) -> List[str]:
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

  async def get_all_instances(self) -> List[Dict]:
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

  async def get_allocation_metrics(self) -> Dict:
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
        "timestamp": datetime.now(timezone.utc).isoformat(),
      }

    except ClientError as e:
      logger.error(f"Error getting allocation metrics: {e}")
      return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

  async def _find_best_instance(
    self,
    instance_tier: Optional[GraphTier] = None,
    exclude_instance: Optional[str] = None,
  ) -> Optional[InstanceInfo]:
    """Find the instance with most available capacity for the specified tier."""
    try:
      # Default to standard tier if not specified
      target_tier = (
        instance_tier.value if instance_tier else GraphTier.KUZU_STANDARD.value
      )

      # Validate tier is supported in this environment
      if self.environment in ["prod", "staging"]:
        supported_tiers = [
          "kuzu-standard",
          "kuzu-large",
          "kuzu-xlarge",
          "kuzu-shared",
          "neo4j-community-large",
          "neo4j-enterprise-xlarge",
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

        database_count = int(item.get("database_count", 0))
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

  async def _trigger_scale_up(self, instance_tier: Optional[GraphTier] = None):
    """Trigger auto scaling group to add an instance for the specified tier."""
    try:
      # Determine ASG name based on tier
      target_tier = instance_tier.value if instance_tier else "kuzu-standard"

      # Rate limiting: Check if we've recently triggered scale-up for this tier
      now = datetime.now(timezone.utc)
      last_trigger = self._scale_up_timestamps.get(target_tier)
      if last_trigger and (now - last_trigger).total_seconds() < 300:  # 5 minutes
        logger.info(
          f"Skipping scale-up for tier {target_tier} - last triggered "
          f"{(now - last_trigger).total_seconds():.0f} seconds ago"
        )
        return

      # Update timestamp
      self._scale_up_timestamps[target_tier] = now

      # Construct ASG name based on environment and tier
      stack_name = self._get_stack_name_for_tier(target_tier)
      if stack_name:
        asg_name = f"{stack_name}-writers-asg"
      else:
        # Development environment or unknown tier
        if self.environment not in ["prod", "staging"]:
          asg_name = self.default_asg_name
        else:
          logger.error(f"Unknown tier {target_tier} for environment {self.environment}")
          return

      logger.info(f"Attempting to scale up ASG {asg_name} for tier {target_tier}")

      response = self.autoscaling.describe_auto_scaling_groups(
        AutoScalingGroupNames=[asg_name]
      )

      if not response["AutoScalingGroups"]:
        logger.error(f"Auto Scaling Group {asg_name} not found")
        return

      asg = response["AutoScalingGroups"][0]
      current_capacity = asg["DesiredCapacity"]
      max_size = asg["MaxSize"]

      if current_capacity < max_size:
        new_capacity = current_capacity + 1
        self.autoscaling.set_desired_capacity(
          AutoScalingGroupName=asg_name,
          DesiredCapacity=new_capacity,
          HonorCooldown=True,
        )
        logger.info(
          f"Triggered scale up for {asg_name}: {current_capacity} -> {new_capacity}"
        )
      else:
        logger.warning(f"Already at maximum capacity: {max_size} for {asg_name}")

    except ClientError as e:
      logger.error(f"Error triggering scale up: {e}")

  def _get_stack_name_for_tier(self, tier: str) -> Optional[str]:
    """Get the CloudFormation stack name for a given tier and environment."""
    if self.environment == "prod":
      tier_map = {
        "kuzu-standard": "RoboSystemsGraphWritersKuzuStandardProd",
        "kuzu-large": "RoboSystemsGraphWritersKuzuLargeProd",
        "kuzu-xlarge": "RoboSystemsGraphWritersKuzuXlargeProd",
        "kuzu-shared": "RoboSystemsGraphWritersKuzuSharedProd",
        "neo4j-community-large": "RoboSystemsGraphWritersNeo4jCommunityLargeProd",
        "neo4j-enterprise-xlarge": "RoboSystemsGraphWritersNeo4jEnterpriseXlargeProd",
      }
    elif self.environment == "staging":
      tier_map = {
        "kuzu-standard": "RoboSystemsGraphWritersKuzuStandardStaging",
        "kuzu-large": "RoboSystemsGraphWritersKuzuLargeStaging",
        "kuzu-xlarge": "RoboSystemsGraphWritersKuzuXlargeStaging",
        "kuzu-shared": "RoboSystemsGraphWritersKuzuSharedStaging",
        "neo4j-community-large": "RoboSystemsGraphWritersNeo4jCommunityLargeStaging",
        "neo4j-enterprise-xlarge": "RoboSystemsGraphWritersNeo4jEnterpriseXlargeStaging",
      }
    else:
      # Development or other environments
      return None

    return tier_map.get(tier)

  async def _get_asg_name_for_instance(self, instance_id: str) -> Optional[str]:
    """Get the ASG name for a specific instance from DynamoDB registry."""
    try:
      response = self.instance_table.get_item(Key={"instance_id": instance_id})

      if "Item" not in response:
        logger.warning(f"Instance {instance_id} not found in registry")
        return None

      item = response["Item"]

      # First try to use the stack_name if available
      stack_name = item.get("stack_name")
      if stack_name:
        # CloudFormation stack name format: {stack_name}-writers-asg
        return f"{stack_name}-writers-asg"

      # Fallback: construct from tier and environment
      cluster_tier = item.get("cluster_tier", "kuzu-standard")
      stack_name = self._get_stack_name_for_tier(cluster_tier)
      if stack_name:
        return f"{stack_name}-writers-asg"

      # Last resort fallback
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
            {"Name": "Environment", "Value": self.environment},
            {"Name": "NodeType", "Value": "writer"},
          ],
        },
        {
          "MetricName": "TotalDatabaseCount",
          "Value": metrics["total_databases"],
          "Unit": "Count",
          "Dimensions": [
            {"Name": "Environment", "Value": self.environment},
            {"Name": "NodeType", "Value": "writer"},
          ],
        },
      ]

      # Also publish the capacity utilization metric that alarms use
      await self._publish_capacity_metric(utilization_percent)

      # Use environment-specific namespace for better separation
      namespace = f"RoboSystemsKuzu/{self.environment.capitalize()}"
      self.cloudwatch.put_metric_data(Namespace=namespace, MetricData=metric_data)

    except ClientError as e:
      logger.error(f"Error publishing metrics: {e}")

  async def _publish_failure_metric(
    self, failure_reason: str, entity_id: str, user_id: Optional[str] = None
  ):
    """Publish allocation failure metric to CloudWatch (only in prod/staging)."""
    # Skip metrics in dev/test environments
    if self.environment in ["dev", "test"]:
      return

    try:
      namespace = f"RoboSystemsKuzu/{self.environment.capitalize()}"
      metric_data = [
        {
          "MetricName": "AllocationFailures",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [
            {"Name": "Environment", "Value": self.environment},
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

  async def _publish_capacity_metric(self, utilization_percent: float):
    """Publish capacity utilization metric to CloudWatch (only in prod/staging)."""
    # Skip metrics in dev/test environments
    if self.environment in ["dev", "test"]:
      return

    try:
      namespace = f"RoboSystemsKuzu/{self.environment.capitalize()}"
      metric_data = [
        {
          "MetricName": "CapacityUtilization",
          "Value": utilization_percent,
          "Unit": "Percent",
          "Dimensions": [
            {"Name": "Environment", "Value": self.environment},
            {"Name": "NodeType", "Value": "writer"},
          ],
        }
      ]
      self.cloudwatch.put_metric_data(Namespace=namespace, MetricData=metric_data)
    except Exception as e:
      logger.error(f"Failed to publish capacity metric: {e}")


# Factory function for compatibility
def create_allocation_manager(environment: str = "prod") -> KuzuAllocationManager:
  """
  Create allocation manager for the specified environment.

  Always uses DynamoDB-based allocation (with LocalStack in development).
  """
  return KuzuAllocationManager(environment=environment)
