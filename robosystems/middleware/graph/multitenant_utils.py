"""
Multi-tenant utility functions for graph database operations.

This module provides utilities for handling multi-tenant database operations,
including database name resolution, validation, configuration management,
and cluster operations.
"""

import re
import asyncio
from typing import Optional, List, Dict, Any
from enum import Enum

import redis
from httpx import HTTPStatusError

from robosystems.logger import logger
from robosystems.security import SecurityAuditLogger, SecurityEventType
from robosystems.middleware.graph.types import (
  GraphIdentity,
  GraphTypeRegistry,
  AccessPattern as GraphAccessPattern,
)
from robosystems.config import env
from robosystems.graph_api.client import GraphClient
from robosystems.graph_api.client.exceptions import GraphClientError


class AccessPattern(str, Enum):
  """Graph database access patterns (legacy, use GraphAccessPattern)."""

  DIRECT_FILE = "direct_file"  # Legacy: Direct file access
  API_WRITER = "api_writer"  # API access to writer node
  API_READER = "api_reader"  # API access to reader node (via ALB)
  API_AUTO = "api_auto"  # API access with automatic routing


class MultiTenantUtils:
  """Utility class for multi-tenant database operations."""

  @staticmethod
  def is_multitenant_mode() -> bool:
    """
    Check if the system is running in multi-tenant mode.

    For graph databases, multi-tenant is always enabled via clusters.
    This maintains backward compatibility while supporting new cluster architecture.

    Returns:
        bool: True if multi-tenant mode is enabled, False otherwise
    """
    # For graph databases, multi-tenant is always enabled via clusters
    return True

  @staticmethod
  def validate_graph_id(graph_id: str) -> str:
    """
    Validate graph_id meets database naming requirements.

    Graph database names must:
    - Not be empty
    - Be at most 64 characters long
    - Contain only alphanumeric characters, underscores, and hyphens
    - Not be reserved names

    Args:
        graph_id: The graph identifier to validate

    Returns:
        str: The validated graph_id

    Raises:
        ValueError: If graph_id doesn't meet requirements
    """
    if not graph_id:
      raise ValueError("graph_id cannot be empty")

    # Special case: shared repositories are allowed
    if MultiTenantUtils.is_shared_repository(graph_id):
      return graph_id

    # Path traversal protection
    if ".." in graph_id or "/" in graph_id or "\\" in graph_id:
      raise ValueError("graph_id contains invalid path characters")

    # Basic validation
    if len(graph_id) > 64:
      raise ValueError(f"graph_id too long: {len(graph_id)} characters (max 64)")

    # Check for invalid characters (alphanumeric, underscore, hyphen only)
    if not re.match(r"^[a-zA-Z0-9_-]+$", graph_id):
      raise ValueError(
        "graph_id contains invalid characters (use only alphanumeric, underscore, hyphen)"
      )

    # Additional validation to prevent edge cases
    if graph_id.startswith("-") or graph_id.endswith("-"):
      raise ValueError("graph_id cannot start or end with hyphen")

    if graph_id.startswith("_") or graph_id.endswith("_"):
      raise ValueError("graph_id cannot start or end with underscore")

    # Check for reserved names
    reserved_names = {"system", "kuzu", "default", "sec"}
    if graph_id.lower() in reserved_names:
      raise ValueError(f"graph_id '{graph_id}' is a reserved name")

    return graph_id

  @staticmethod
  def get_database_name(graph_id: Optional[str] = None) -> str:
    """
    Get the appropriate database name based on multi-tenant mode and graph_id.

    Special handling for shared repositories - always routes to their specific
    database names regardless of multi-tenant mode to maintain shared data repositories.

    Args:
        graph_id: Optional graph identifier for multi-tenant mode

    Returns:
        str: Database name to use
    """
    # Check if this is a shared repository
    if graph_id and MultiTenantUtils.is_shared_repository(graph_id):
      repository_name = MultiTenantUtils.get_repository_database_name(graph_id)
      logger.debug(f"Routing to shared repository database: '{repository_name}'")
      return repository_name

    if MultiTenantUtils.is_multitenant_mode():
      if graph_id and graph_id != "default":
        # Validate the graph_id before using it (but allow "default" to pass through)
        validated_graph_id = MultiTenantUtils.validate_graph_id(graph_id)
        logger.debug(
          f"Multi-tenant mode: using database '{validated_graph_id}' for graph_id '{graph_id}'"
        )
        return validated_graph_id
      else:
        logger.debug(
          "Multi-tenant mode: no graph_id provided or 'default' used, using default database"
        )
        return "default"
    else:
      logger.debug("Single-tenant mode: using default database")
      return "default"

  @staticmethod
  def log_database_operation(
    operation: str, database_name: str, graph_id: Optional[str] = None
  ) -> None:
    """
    Log database operations for observability.

    Args:
        operation: Description of the operation being performed
        database_name: Name of the database being accessed
        graph_id: Optional graph identifier
    """
    if graph_id:
      logger.info(f"{operation} - Database: {database_name}, Graph ID: {graph_id}")
    else:
      logger.info(f"{operation} - Database: {database_name}")

  @staticmethod
  def check_database_limits() -> None:
    """
    Check if the current number of databases exceeds the configured limit.

    Currently implemented as a no-op. Database limits are enforced at the
    instance level based on KUZU_MAX_DATABASES_PER_NODE.

    Raises:
        RuntimeError: If database limit would be exceeded (future implementation)
    """
    # Database limits are enforced per node via allocation manager
    pass

  @staticmethod
  def validate_database_creation(graph_id: str) -> str:
    """
    Validate that a new database can be created for the given graph_id.

    This combines graph_id validation with database limit checking.

    Args:
        graph_id: The graph identifier for the new database

    Returns:
        str: The validated graph_id

    Raises:
        ValueError: If graph_id is invalid
        RuntimeError: If database limit would be exceeded
    """
    validated_graph_id = MultiTenantUtils.validate_graph_id(graph_id)
    MultiTenantUtils.check_database_limits()

    logger.info(f"Validated database creation for graph_id: {validated_graph_id}")
    return validated_graph_id

  @staticmethod
  def is_sec_database(graph_id: str) -> bool:
    """
    Check if the given graph_id refers to the shared SEC database.

    Args:
        graph_id: Graph identifier to check

    Returns:
        bool: True if this is the SEC database
    """
    return graph_id == "sec"

  @staticmethod
  def get_sec_database_name() -> str:
    """
    Get the SEC database name.

    Returns:
        str: Always returns 'sec' for the shared public data repository
    """
    return "sec"

  @staticmethod
  def validate_sec_access(graph_id: str) -> bool:
    """
    Validate that the requested graph_id is appropriate for SEC access.

    Args:
        graph_id: Graph identifier being requested

    Returns:
        bool: True if SEC access is valid for this graph_id
    """
    return MultiTenantUtils.is_sec_database(graph_id)

  # ============================================================================
  # GENERIC REPOSITORY METHODS (NEW)
  # ============================================================================

  # Use the centralized registry for shared repositories
  SHARED_REPOSITORIES = GraphTypeRegistry.SHARED_REPOSITORIES

  @staticmethod
  def is_shared_repository(graph_id: Optional[str]) -> bool:
    """
    Check if the given graph_id refers to a shared repository.

    Args:
        graph_id: Graph identifier to check

    Returns:
        bool: True if this is a shared repository
    """
    # Check if graph_id is in the list of shared repositories
    return graph_id in MultiTenantUtils.SHARED_REPOSITORIES

  @staticmethod
  def get_repository_database_name(repository_id: str) -> str:
    """
    Get the database name for a shared repository.

    Args:
        repository_id: Repository identifier (e.g., 'sec', 'industry')

    Returns:
        str: Database name for the repository

    Raises:
        ValueError: If repository_id is not a known shared repository
    """
    if repository_id not in MultiTenantUtils.SHARED_REPOSITORIES:
      raise ValueError(f"Unknown shared repository: {repository_id}")

    return MultiTenantUtils.SHARED_REPOSITORIES[repository_id]

  @staticmethod
  def list_shared_repositories() -> List[str]:
    """
    Get a list of all known shared repository identifiers.

    Returns:
        List[str]: List of repository IDs
    """
    return GraphTypeRegistry.list_shared_repositories()

  # ============================================================================
  # NEW TYPE-AWARE METHODS
  # ============================================================================

  @staticmethod
  def get_graph_identity(graph_id: str) -> GraphIdentity:
    """
    Get complete graph identity including category and type.

    Args:
        graph_id: Graph identifier

    Returns:
        GraphIdentity with full type information
    """
    return GraphTypeRegistry.identify_graph(graph_id)

  @staticmethod
  def get_graph_routing(graph_id: str) -> Dict[str, Any]:
    """
    Get routing information for a graph based on its type.

    Args:
        graph_id: Graph identifier

    Returns:
        Dict with routing configuration including cluster type, access mode, etc.
    """
    identity = MultiTenantUtils.get_graph_identity(graph_id)
    routing_info = identity.get_routing_info()

    # Add database name
    routing_info["database_name"] = MultiTenantUtils.get_database_name(graph_id)
    routing_info["graph_identity"] = identity

    return routing_info

  @staticmethod
  def validate_graph_access(
    graph_id: str,
    required_access: GraphAccessPattern,
    user_permissions: Optional[Dict[str, Any]] = None,
  ) -> bool:
    """
    Validate if the requested access pattern is allowed for this graph.

    Args:
        graph_id: Graph identifier
        required_access: Required access pattern
        user_permissions: Optional user permissions to check

    Returns:
        bool: True if access is allowed
    """
    identity = MultiTenantUtils.get_graph_identity(graph_id)
    _allowed_access = identity.get_access_pattern()

    # For shared repositories, never allow write access
    if (
      identity.is_shared_repository and required_access == GraphAccessPattern.READ_WRITE
    ):
      logger.warning(
        f"Write access requested for shared repository {graph_id}, denying"
      )
      return False

    # For system graphs, only allow restricted access
    if identity.is_system_graph and required_access != GraphAccessPattern.RESTRICTED:
      logger.warning(
        f"Non-restricted access requested for system graph {graph_id}, denying"
      )
      return False

    return True

  @staticmethod
  def get_graph_cluster_type(graph_id: str) -> str:
    """
    Determine which cluster type should handle this graph.

    Args:
        graph_id: Graph identifier

    Returns:
        str: Cluster type ("user_writer", "shared_writer", etc.)
    """
    identity = MultiTenantUtils.get_graph_identity(graph_id)

    if identity.is_shared_repository:
      return "shared_writer"
    elif identity.is_user_graph:
      return "user_writer"
    else:
      return "system"

  @staticmethod
  def is_user_graph(graph_id: str) -> bool:
    """
    Check if this is a user-created graph.

    Args:
        graph_id: Graph identifier

    Returns:
        bool: True if this is a user graph
    """
    identity = MultiTenantUtils.get_graph_identity(graph_id)
    return identity.is_user_graph

  @staticmethod
  def get_repository_type_from_graph_id(graph_id: str):
    """
    Get the RepositoryType enum value from a graph_id.

    Args:
        graph_id: Graph identifier

    Returns:
        RepositoryType: The repository type enum value

    Raises:
        ValueError: If graph_id is not a known repository
    """
    from ...models.iam import RepositoryType

    repository_mapping = {
      "sec": RepositoryType.SEC,
      "industry": RepositoryType.INDUSTRY,
      "economic": RepositoryType.ECONOMIC,
    }

    if graph_id not in repository_mapping:
      raise ValueError(f"Unknown repository graph_id: {graph_id}")

    return repository_mapping[graph_id]

  @staticmethod
  def validate_repository_access(
    graph_id: str, user_id: str, operation_type: str = "read"
  ) -> bool:
    """
    Validate that a user has access to a shared repository.

    Args:
        graph_id: Repository identifier
        user_id: User ID to check
        operation_type: Type of operation (read, write, admin)

    Returns:
        bool: True if user has appropriate access
    """
    if not MultiTenantUtils.is_shared_repository(graph_id):
      return False

    from ...models.iam import (
      UserRepository,
      UserRepositoryAccessLevel as RepositoryAccessLevel,
    )
    from ...database import session

    # Get user's access level for this repository
    repository_name = MultiTenantUtils.get_repository_database_name(graph_id)
    access_level = UserRepository.get_user_access_level(
      user_id, repository_name, session()
    )

    # Check if user has required access level
    has_access = False
    if operation_type == "read":
      has_access = access_level in [
        RepositoryAccessLevel.READ,
        RepositoryAccessLevel.WRITE,
        RepositoryAccessLevel.ADMIN,
      ]
    elif operation_type == "write":
      has_access = access_level in [
        RepositoryAccessLevel.WRITE,
        RepositoryAccessLevel.ADMIN,
      ]
    elif operation_type == "admin":
      has_access = access_level == RepositoryAccessLevel.ADMIN
    else:
      has_access = False

    # Log the access attempt result
    if has_access:
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTH_SUCCESS,
        user_id=str(user_id),
        details={
          "action": "repository_access_granted",
          "repository": repository_name,
          "operation_type": operation_type,
          "access_level": access_level.value if access_level else None,
        },
        risk_level="low",
      )
    else:
      SecurityAuditLogger.log_authorization_denied(
        user_id=str(user_id),
        resource=f"repository:{repository_name}",
        action=operation_type,
      )

    return has_access

  # ============================================================================
  # CLUSTER AND ACCESS PATTERN METHODS
  # ============================================================================

  @staticmethod
  def get_access_pattern() -> AccessPattern:
    """
    Get the preferred graph database access pattern.

    Returns:
        AccessPattern: The access pattern to use
    """
    pattern = env.KUZU_ACCESS_PATTERN.lower()
    try:
      return AccessPattern(pattern)
    except ValueError:
      logger.warning(f"Invalid KUZU_ACCESS_PATTERN: {pattern}, using api_auto")
      return AccessPattern.API_AUTO

  @staticmethod
  def get_max_databases_per_node() -> int:
    """
    Get the maximum number of databases per graph node.

    Returns:
        int: Maximum databases per node
    """
    return env.KUZU_MAX_DATABASES_PER_NODE

  @staticmethod
  def get_database_path_for_graph(graph_id: str) -> str:
    """
    Get the database file path for a graph (Kuzu 0.11.x single-file format).

    Args:
        graph_id: Graph identifier

    Returns:
        str: Database file path (.kuzu extension)
    """
    from ...operations.kuzu.path_utils import get_kuzu_database_path

    # Use the centralized path utility for consistent handling
    db_path = get_kuzu_database_path(graph_id)
    return str(db_path)

  @staticmethod
  def log_cluster_operation(
    operation: str, cluster_id: str, graph_id: str, **kwargs
  ) -> None:
    """
    Log cluster operation for monitoring and debugging.

    Args:
        operation: Operation description
        cluster_id: Cluster identifier
        graph_id: Graph identifier
        **kwargs: Additional context
    """
    context = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
    logger.info(
      f"Graph Cluster Operation: {operation} | "
      f"Cluster: {cluster_id} | Graph: {graph_id}"
      f"{' | ' + context if context else ''}"
    )

  @staticmethod
  def get_migration_status() -> Dict[str, Any]:
    """
    Get the current graph database migration status.

    Returns:
        Dict: Migration status information
    """
    return {
      "access_pattern": MultiTenantUtils.get_access_pattern().value,
      "max_databases_per_node": MultiTenantUtils.get_max_databases_per_node(),
      "shared_repositories": {
        "sec_engine": "kuzu",
      },
      "environment": env.ENVIRONMENT,
    }

  # ============================================================================
  # DATABASE CREATION
  # ============================================================================

  @staticmethod
  async def ensure_database_with_schema(
    kuzu_url: str,
    db_name: str,
    schema_name: str,
    api_key: Optional[str] = None,
    lock_timeout: int = 300,
  ) -> Dict[str, Any]:
    """
    Ensure database exists with proper schema.

    Args:
        kuzu_url: Graph API endpoint URL
        db_name: Database name to create
        schema_name: Schema name to apply (e.g., "sec", "entity")
        api_key: Optional API key for authentication
        lock_timeout: Unused parameter (kept for backwards compatibility)

    Returns:
        Dict with creation status and database info

    Raises:
        RuntimeError: If creation fails
    """

    # Create Graph client
    headers = {}
    if api_key:
      headers["Authorization"] = f"Bearer {api_key}"

    async with GraphClient(base_url=kuzu_url, headers=headers) as client:
      # Check if database already exists
      try:
        db_info = await client.get_database_info(db_name)
        if db_info.get("is_healthy", False):
          logger.info(f"Database {db_name} already exists and is ready")
          return {"created": False, "status": "exists", "database": db_info}
      except GraphClientError as e:
        if "not found" in str(e).lower():
          logger.info(f"Database {db_name} not found, will create")
        else:
          logger.error(f"KuzuClient error checking database: {e}")
          raise RuntimeError(f"Failed to check database existence: {e}")
      except HTTPStatusError as e:
        if e.response.status_code == 404:
          logger.info(f"Database {db_name} not found, will create")
        else:
          logger.error(
            f"HTTP error checking database: {e.response.status_code} - {e.response.text}"
          )
          raise RuntimeError(f"Failed to check database existence: {e}")
      except Exception as e:
        logger.error(f"Unexpected error checking database: {type(e).__name__}: {e}")
        raise

      # Determine schema type and repository name
      schema_type = "custom"
      repository_name = None

      # Map schema names to types - use existing logic
      if MultiTenantUtils.is_shared_repository(db_name):
        schema_type = "shared"
        repository_name = db_name
      elif schema_name in ["sec", "sec_xbrl", "sec_filings"]:
        schema_type = "shared"
        repository_name = "sec"
      elif schema_name in ["entity", "entity_financials"]:
        schema_type = "entity"
      elif schema_name in ["industry", "economic"]:
        schema_type = "shared"
        repository_name = schema_name

      # Create database
      try:
        logger.info(
          f"Creating database {db_name} with schema type: {schema_type}, "
          f"repository: {repository_name}"
        )

        create_result = await client.create_database(
          graph_id=db_name, schema_type=schema_type, repository_name=repository_name
        )

        logger.info(f"Successfully created database {db_name}")

        # Wait for database to be ready
        max_wait = 30  # seconds
        wait_interval = 2
        total_waited = 0

        while total_waited < max_wait:
          try:
            db_info = await client.get_database_info(db_name)
            if db_info.get("is_healthy", False):
              logger.info(f"Database {db_name} is ready")
              return {
                "created": True,
                "status": "created",
                "database": db_info,
                "create_result": create_result,
              }
          except Exception as e:
            logger.debug(f"Database not ready yet: {e}")

          await asyncio.sleep(wait_interval)
          total_waited += wait_interval

        raise RuntimeError(
          f"Database {db_name} creation timed out after {max_wait} seconds"
        )

      except Exception as e:
        logger.error(f"Failed to create database {db_name}: {e}")
        raise RuntimeError(f"Database creation failed: {e}")

  @staticmethod
  def ensure_database_with_schema_sync(
    kuzu_url: str,
    db_name: str,
    schema_name: str,
    api_key: Optional[str] = None,
    redis_client: Optional[redis.Redis] = None,
  ) -> Dict[str, Any]:
    """
    Synchronous wrapper for ensure_database_with_schema.

    Args:
        kuzu_url: Graph API endpoint URL
        db_name: Database name to create
        schema_name: Schema name to apply
        api_key: Optional API key for authentication
        redis_client: Optional Redis client for locking (unused, for compatibility)

    Returns:
        Dict with creation status and database info
    """
    # Run in event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
      return loop.run_until_complete(
        MultiTenantUtils.ensure_database_with_schema(
          kuzu_url=kuzu_url, db_name=db_name, schema_name=schema_name, api_key=api_key
        )
      )
    finally:
      loop.close()


# Backward compatibility functions
def is_multitenant_mode() -> bool:
  """Backward compatibility wrapper."""
  return MultiTenantUtils.is_multitenant_mode()


def get_database_name(graph_id: str) -> str:
  """
  Backward compatibility wrapper for database name resolution.

  For graph clusters, this returns the graph_id as the database name
  since database routing is handled at the cluster level.
  """
  return MultiTenantUtils.get_database_name(graph_id)


def validate_database_creation(graph_id: str) -> str:
  """Backward compatibility wrapper for database creation validation."""
  return MultiTenantUtils.validate_database_creation(graph_id)


def log_database_operation(operation: str, database_name: str, graph_id: str) -> None:
  """Backward compatibility wrapper for operation logging."""
  MultiTenantUtils.log_database_operation(operation, database_name, graph_id)
