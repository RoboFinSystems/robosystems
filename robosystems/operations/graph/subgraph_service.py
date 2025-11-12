"""
Subgraph Service for managing subgraph operations.

This service handles the creation, deletion, and management of subgraphs
for Enterprise and Premium tier graphs. Subgraphs share the parent's
infrastructure and credit pool while maintaining separate databases.

Key features:
- Subgraph creation on parent's instance
- Shared memory pool management
- Schema inheritance from parent
- Database lifecycle management
- Access control validation
"""

from typing import Dict, Any, Optional, List, TYPE_CHECKING
from datetime import datetime, timezone

if TYPE_CHECKING:
  from ...models.iam import Graph, User

from ...config import env
from ...middleware.graph.allocation_manager import KuzuAllocationManager
from ...middleware.graph.types import GraphTypeRegistry
from ...middleware.graph.subgraph_utils import (
  construct_subgraph_id,
  parse_subgraph_id,
  validate_subgraph_name,
  validate_parent_graph_id,
)
from ...graph_api.client.factory import get_graph_client_for_instance
from ...exceptions import GraphAllocationError
from ...logger import logger


class SubgraphService:
  """
  Service class for subgraph-related operations.

  This service manages subgraph lifecycle operations including creation,
  deletion, and configuration. It ensures subgraphs are properly isolated
  while sharing the parent's infrastructure resources.
  """

  def __init__(self):
    """Initialize the subgraph service."""
    self.allocation_manager = KuzuAllocationManager(environment=env.ENVIRONMENT)
    self.shared_repositories = list(GraphTypeRegistry.SHARED_REPOSITORIES.keys())

  async def create_subgraph_database(
    self,
    parent_graph_id: str,
    subgraph_name: str,
    schema_extensions: Optional[List[str]] = None,
  ) -> Dict[str, Any]:
    """
    Create a new subgraph database on the parent's instance.

    This method creates a new database on the same instance as the parent graph,
    allowing Enterprise/Premium customers to maximize their dedicated infrastructure.

    Args:
        parent_graph_id: Parent graph identifier (must be Enterprise/Premium)
        subgraph_name: Alphanumeric name for the subgraph (1-20 chars)
        schema_extensions: Optional schema extensions to apply

    Returns:
        Dictionary with creation status and details including:
        - status: "created" or "exists"
        - graph_id: Full subgraph identifier
        - database_name: Actual database name on disk
        - parent_graph_id: Parent graph ID
        - instance_id: EC2 instance ID
        - instance_ip: Private IP of the instance

    Raises:
        GraphAllocationError: If parent not found or creation fails
        ValueError: If inputs are invalid
    """
    # Validate inputs
    if not validate_parent_graph_id(parent_graph_id):
      raise ValueError(f"Invalid parent graph ID: {parent_graph_id}")

    if parent_graph_id.lower() in self.shared_repositories:
      raise ValueError(
        f"Shared repository '{parent_graph_id}' cannot have subgraphs. "
        "Subgraphs are only available for user-owned Enterprise/Premium graphs."
      )

    if not validate_subgraph_name(subgraph_name):
      raise ValueError(
        f"Invalid subgraph name: {subgraph_name}. "
        "Must be alphanumeric and 1-20 characters."
      )

    # Construct the full subgraph ID
    subgraph_id = construct_subgraph_id(parent_graph_id, subgraph_name)
    database_name = subgraph_id  # Using underscore notation

    logger.info(
      f"Creating subgraph database {subgraph_id} on parent {parent_graph_id}'s instance"
    )

    try:
      # Get the parent's instance location
      parent_location = await self.allocation_manager.find_database_location(
        parent_graph_id
      )

      if not parent_location:
        raise GraphAllocationError(
          f"Parent graph {parent_graph_id} not found. "
          "The parent graph must exist before creating subgraphs."
        )

      # Get a direct client to the parent's instance
      client = await get_graph_client_for_instance(parent_location.private_ip)

      # Check if database already exists
      existing_databases = await client.list_databases()
      if database_name in existing_databases:
        logger.warning(f"Database {database_name} already exists on instance")
        return {
          "status": "exists",
          "graph_id": subgraph_id,
          "database_name": database_name,
          "parent_graph_id": parent_graph_id,
          "instance_id": parent_location.instance_id,
          "instance_ip": parent_location.private_ip,
          "message": "Subgraph database already exists",
        }

      # Create the new database
      logger.info(
        f"Creating database {database_name} on instance {parent_location.instance_id} "
        f"({parent_location.private_ip})"
      )

      await client.create_database(
        graph_id=database_name,
        schema_type="entity",  # Subgraphs use entity schema type
        custom_schema_ddl=None,  # Will install schema separately
        is_subgraph=True,  # Bypass max_databases check for Enterprise/Premium
      )

      # Install schema with extensions if provided
      if schema_extensions:
        logger.info(f"Installing schema with extensions: {schema_extensions}")
        await self._install_schema_with_extensions(
          client, database_name, schema_extensions
        )
      else:
        logger.info("Installing base schema only (no extensions)")
        await self._install_base_schema(client, database_name)

      logger.info(f"Successfully created subgraph database {subgraph_id}")

      return {
        "status": "created",
        "graph_id": subgraph_id,
        "database_name": database_name,
        "parent_graph_id": parent_graph_id,
        "instance_id": parent_location.instance_id,
        "instance_ip": parent_location.private_ip,
        "created_at": datetime.now(timezone.utc).isoformat(),
      }

    except Exception as e:
      logger.error(f"Failed to create subgraph database {subgraph_id}: {e}")
      raise GraphAllocationError(f"Failed to create subgraph: {str(e)}")

  def create_subgraph(
    self,
    parent_graph: "Graph",
    user: "User",
    name: str,
    description: str | None = None,
    subgraph_type: str = "static",
    metadata: dict | None = None,
  ) -> Dict[str, Any]:
    """
    Create a subgraph including both the database and PostgreSQL metadata.

    This is a synchronous wrapper that creates the PostgreSQL records.
    The actual Kuzu database creation happens asynchronously.

    Args:
        parent_graph: Parent graph model
        user: User creating the subgraph
        name: Alphanumeric subgraph name
        description: Optional description
        subgraph_type: Type of subgraph (default: "static")
        metadata: Optional metadata dict

    Returns:
        Dictionary with created subgraph details
    """
    from ...models.iam.graph import Graph
    from ...models.iam.graph_user import GraphUser
    from ...database import get_db_session

    subgraph_id = construct_subgraph_id(parent_graph.graph_id, name)

    db = next(get_db_session())
    try:
      existing_subgraphs = (
        db.query(Graph)
        .filter(Graph.parent_graph_id == parent_graph.graph_id)
        .order_by(Graph.subgraph_index.desc())
        .all()
      )
      next_index = (
        (existing_subgraphs[0].subgraph_index + 1) if existing_subgraphs else 1
      )

      now = datetime.now(timezone.utc)

      subgraph = Graph(
        graph_id=subgraph_id,
        org_id=parent_graph.org_id,
        graph_name=description or name,
        graph_type=parent_graph.graph_type,
        base_schema=parent_graph.base_schema,
        schema_extensions=parent_graph.schema_extensions or [],
        graph_instance_id=parent_graph.graph_instance_id,
        graph_cluster_region=parent_graph.graph_cluster_region,
        graph_tier=parent_graph.graph_tier,
        parent_graph_id=parent_graph.graph_id,
        subgraph_index=next_index,
        subgraph_name=name,
        is_subgraph=True,
        subgraph_metadata=metadata or {},
        is_repository=False,
        repository_type=None,
        data_source_type=None,
        created_at=now,
        updated_at=now,
      )
      db.add(subgraph)

      graph_user = GraphUser(
        user_id=user.id,
        graph_id=subgraph_id,
        role="admin",
        created_at=now,
        updated_at=now,
      )
      db.add(graph_user)

      db.commit()
      db.refresh(subgraph)

      logger.info(
        f"Created subgraph {subgraph_id} (index {next_index}) for parent {parent_graph.graph_id}"
      )

      return {
        "graph_id": subgraph.graph_id,
        "subgraph_index": subgraph.subgraph_index,
        "graph_type": subgraph.graph_type,
        "status": "active",
        "created_at": subgraph.created_at,
        "updated_at": subgraph.updated_at,
      }

    except Exception as e:
      db.rollback()
      logger.error(f"Failed to create subgraph metadata: {e}")
      raise
    finally:
      db.close()

  async def delete_subgraph_database(
    self,
    subgraph_id: str,
    force: bool = False,
    create_backup: bool = False,
  ) -> Dict[str, Any]:
    """
    Delete a subgraph database from the parent's instance.

    Args:
        subgraph_id: Full subgraph identifier to delete
        force: Force deletion even if database contains data
        create_backup: Create a backup before deletion

    Returns:
        Dictionary with deletion status including:
        - status: "deleted", "not_found", or "backup_created"
        - graph_id: Deleted subgraph ID
        - backup_location: S3 location if backup was created

    Raises:
        GraphAllocationError: If deletion fails
        ValueError: If subgraph_id is invalid
    """
    # Parse the subgraph ID
    subgraph_info = parse_subgraph_id(subgraph_id)
    if not subgraph_info:
      raise ValueError(f"Invalid subgraph ID: {subgraph_id}")

    parent_graph_id = subgraph_info.parent_graph_id
    database_name = subgraph_info.database_name

    logger.info(f"Deleting subgraph database {subgraph_id}")

    try:
      # Get the parent's instance location
      parent_location = await self.allocation_manager.find_database_location(
        parent_graph_id
      )

      if not parent_location:
        raise GraphAllocationError(
          f"Parent graph {parent_graph_id} not found. Cannot delete subgraph."
        )

      # Get a direct client to the parent's instance
      client = await get_graph_client_for_instance(parent_location.private_ip)

      # Check if database exists
      existing_databases = await client.list_databases()
      if database_name not in existing_databases:
        logger.warning(f"Database {database_name} does not exist on instance")
        return {
          "status": "not_found",
          "graph_id": subgraph_id,
          "message": "Subgraph database does not exist",
        }

      # Create backup if requested
      backup_location = None
      if create_backup:
        logger.info(f"Creating backup of {database_name} before deletion")
        backup_location = await self._create_backup(
          client, database_name, parent_location.instance_id
        )

      # Check if database contains data (unless forced)
      if not force:
        has_data = await self._check_database_has_data(client, database_name)
        if has_data:
          raise GraphAllocationError(
            f"Subgraph {subgraph_id} contains data. "
            "Use force=True to delete anyway, or create a backup first."
          )

      # Delete the database
      logger.info(
        f"Deleting database {database_name} from instance {parent_location.instance_id}"
      )
      await client.delete_database(database_name)

      logger.info(f"Successfully deleted subgraph database {subgraph_id}")

      return {
        "status": "deleted",
        "graph_id": subgraph_id,
        "database_name": database_name,
        "parent_graph_id": parent_graph_id,
        "instance_id": parent_location.instance_id,
        "backup_location": backup_location,
        "deleted_at": datetime.now(timezone.utc).isoformat(),
      }

    except Exception as e:
      logger.error(f"Failed to delete subgraph database {subgraph_id}: {e}")
      raise GraphAllocationError(f"Failed to delete subgraph: {str(e)}")

  async def list_subgraph_databases(
    self,
    parent_graph_id: str,
  ) -> List[Dict[str, Any]]:
    """
    List all subgraph databases for a parent graph.

    Args:
        parent_graph_id: Parent graph identifier

    Returns:
        List of subgraph information dictionaries
    """
    logger.info(f"Listing subgraph databases for parent {parent_graph_id}")

    try:
      # Get the parent's instance location
      parent_location = await self.allocation_manager.find_database_location(
        parent_graph_id
      )

      if not parent_location:
        logger.warning(f"Parent graph {parent_graph_id} not found")
        return []

      # Get a direct client to the parent's instance
      client = await get_graph_client_for_instance(parent_location.private_ip)

      # List all databases on the instance
      all_databases = await client.list_databases()

      # Filter for subgraphs of this parent
      subgraphs = []
      parent_prefix = f"{parent_graph_id}_"

      for db_name in all_databases:
        if db_name.startswith(parent_prefix):
          # This is a subgraph of our parent
          subgraph_info = parse_subgraph_id(db_name)
          if subgraph_info:
            subgraphs.append(
              {
                "graph_id": db_name,
                "subgraph_name": subgraph_info.subgraph_name,
                "database_name": db_name,
                "parent_graph_id": parent_graph_id,
                "instance_id": parent_location.instance_id,
              }
            )

      logger.info(f"Found {len(subgraphs)} subgraphs for {parent_graph_id}")
      return subgraphs

    except Exception as e:
      logger.error(f"Failed to list subgraphs for {parent_graph_id}: {e}")
      return []

  async def get_subgraph_info(
    self,
    subgraph_id: str,
  ) -> Optional[Dict[str, Any]]:
    """
    Get detailed information about a specific subgraph.

    Args:
        subgraph_id: Full subgraph identifier

    Returns:
        Dictionary with subgraph information or None if not found
    """
    # Parse the subgraph ID
    subgraph_info = parse_subgraph_id(subgraph_id)
    if not subgraph_info:
      logger.warning(f"Invalid subgraph ID: {subgraph_id}")
      return None

    parent_graph_id = subgraph_info.parent_graph_id
    database_name = subgraph_info.database_name

    try:
      # Get the parent's instance location
      parent_location = await self.allocation_manager.find_database_location(
        parent_graph_id
      )

      if not parent_location:
        logger.warning(f"Parent graph {parent_graph_id} not found")
        return None

      # Get a direct client to the parent's instance
      client = await get_graph_client_for_instance(parent_location.private_ip)

      # Check if database exists
      existing_databases = await client.list_databases()
      if database_name not in existing_databases:
        logger.warning(f"Subgraph database {database_name} not found")
        return None

      # Get database statistics (if available)
      stats = await self._get_database_stats(client, database_name)

      return {
        "graph_id": subgraph_id,
        "subgraph_name": subgraph_info.subgraph_name,
        "database_name": database_name,
        "parent_graph_id": parent_graph_id,
        "instance_id": parent_location.instance_id,
        "instance_ip": parent_location.private_ip,
        "statistics": stats,
      }

    except Exception as e:
      logger.error(f"Failed to get info for subgraph {subgraph_id}: {e}")
      return None

  # Private helper methods

  async def _install_schema_with_extensions(
    self,
    client,
    database_name: str,
    extensions: List[str],
  ) -> None:
    """Install schema with specified extensions."""
    try:
      # First install base schema
      await self._install_base_schema(client, database_name)

      # Then install each extension
      for extension in extensions:
        logger.info(f"Installing {extension} extension for {database_name}")
        await client.install_schema(
          graph_id=database_name, base_schema="base", extensions=[extension]
        )

      logger.info(
        f"Successfully installed schema with extensions {extensions} for {database_name}"
      )
    except Exception as e:
      logger.error(f"Failed to install schema with extensions for {database_name}: {e}")
      raise

  async def _install_base_schema(
    self,
    client,
    database_name: str,
  ) -> None:
    """Install base schema only."""
    try:
      # Install the base entity schema
      await client.install_schema(
        graph_id=database_name, base_schema="base", extensions=[]
      )
      logger.info(f"Successfully installed base schema for {database_name}")
    except Exception as e:
      logger.error(f"Failed to install base schema for {database_name}: {e}")
      raise

  async def _check_database_has_data(
    self,
    client,
    database_name: str,
  ) -> bool:
    """Check if a database contains any data."""
    try:
      # Query for any nodes to check if database has data
      result = await client.execute(
        graph_id=database_name, query="MATCH (n) RETURN count(n) as node_count LIMIT 1"
      )

      if result and len(result) > 0:
        node_count = result[0].get("node_count", 0)
        has_data = node_count > 0
        logger.info(f"Database {database_name} has {node_count} nodes")
        return has_data

      return False
    except Exception as e:
      logger.warning(f"Could not check data for {database_name}: {e}")
      # If we can't check, assume no data for safety
      return False

  async def _create_backup(
    self,
    client,
    database_name: str,
    instance_id: str,
  ) -> str:
    """Create a backup of the database."""
    backup_location = ""  # Initialize to avoid unbound variable error

    try:
      timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
      backup_location = (
        f"s3://robosystems-backups/{instance_id}/{database_name}_{timestamp}.backup"
      )

      # Use the backup endpoint if available
      backup_response = await client.backup(
        graph_id=database_name, backup_location=backup_location
      )

      logger.info(f"Created backup at {backup_location}")
      return backup_response.get("location", backup_location)
    except AttributeError:
      # Backup method may not be implemented yet
      logger.warning(f"Backup not yet implemented for {database_name}")
      return backup_location
    except Exception as e:
      logger.error(f"Failed to create backup for {database_name}: {e}")
      raise

  async def _get_database_stats(
    self,
    client,
    database_name: str,
  ) -> Dict[str, Any]:
    """Get statistics for a database."""
    try:
      # Get node and edge counts
      node_result = await client.execute(
        graph_id=database_name, query="MATCH (n) RETURN count(n) as count"
      )
      node_count = node_result[0]["count"] if node_result else 0

      edge_result = await client.execute(
        graph_id=database_name, query="MATCH ()-[r]->() RETURN count(r) as count"
      )
      edge_count = edge_result[0]["count"] if edge_result else 0

      # Try to get database info for size
      try:
        db_info = await client.get_database(database_name)
        size_mb = db_info.get("size_mb", None)
        last_modified = db_info.get("last_modified", None)
      except Exception:
        size_mb = None
        last_modified = None

      return {
        "node_count": node_count,
        "edge_count": edge_count,
        "size_mb": size_mb,
        "last_modified": last_modified,
      }
    except Exception as e:
      logger.warning(f"Could not get statistics for {database_name}: {e}")
      return {
        "node_count": None,
        "edge_count": None,
        "size_mb": None,
        "last_modified": None,
      }
