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
  from ...graph_api.client.client import GraphClient

from ...config import env
from ...middleware.graph.allocation_manager import LadybugAllocationManager
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
    self.allocation_manager = LadybugAllocationManager(environment=env.ENVIRONMENT)
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

    # Check subgraph limit for parent's tier
    from ...models.iam.graph import Graph
    from ...database import get_db_session
    from ...config.graph_tier import GraphTierConfig

    db = next(get_db_session())
    try:
      parent_graph = db.query(Graph).filter(Graph.graph_id == parent_graph_id).first()
      if not parent_graph:
        raise GraphAllocationError(
          f"Parent graph {parent_graph_id} not found in database. "
          "The parent graph must exist before creating subgraphs."
        )

      max_subgraphs = GraphTierConfig.get_max_subgraphs(parent_graph.graph_tier)

      if max_subgraphs is not None and max_subgraphs == 0:
        raise GraphAllocationError(
          f"Tier '{parent_graph.graph_tier}' does not support subgraphs. "
          f"Upgrade to a tier with subgraph support to use this feature."
        )

      if max_subgraphs is not None:
        existing_subgraphs = await self.list_subgraph_databases(parent_graph_id)
        if len(existing_subgraphs) >= max_subgraphs:
          raise GraphAllocationError(
            f"Maximum subgraph limit ({max_subgraphs}) reached for tier '{parent_graph.graph_tier}'. "
            f"Currently have {len(existing_subgraphs)} subgraphs. "
            f"Upgrade to a higher tier for more subgraphs."
          )
    finally:
      db.close()

    # Construct the full subgraph ID
    subgraph_id = construct_subgraph_id(parent_graph_id, subgraph_name)
    database_name = subgraph_id  # Using underscore notation

    logger.info(
      f"Creating subgraph database {subgraph_id} on parent {parent_graph_id}'s instance"
    )

    try:
      # Get the parent's instance location
      # For local dev graphs, use PostgreSQL instance_id directly
      from robosystems.database import get_db_session
      from robosystems.models.iam.graph import Graph
      from robosystems.config import env

      session = next(get_db_session())
      parent_graph_record = (
        session.query(Graph).filter(Graph.graph_id == parent_graph_id).first()
      )
      session.close()

      if not parent_graph_record:
        logger.warning(f"Parent graph {parent_graph_id} not found")
        raise GraphAllocationError(
          f"Parent graph {parent_graph_id} not found. "
          "The parent graph must exist before creating subgraphs."
        )

      # Check if this is a local dev graph
      if (
        parent_graph_record.graph_instance_id
        and parent_graph_record.graph_instance_id.startswith("local-")
      ):
        # For local graphs, create a mock location without DynamoDB lookup
        from dataclasses import dataclass

        @dataclass
        class LocalGraphLocation:
          instance_id: str
          private_ip: str

        # For local dev, use the graph-api service name
        parent_location = LocalGraphLocation(
          instance_id=parent_graph_record.graph_instance_id,
          private_ip="graph-api" if env.ENVIRONMENT == "dev" else "localhost",
        )
        logger.info(
          f"Using local graph instance: {parent_location.instance_id} at {parent_location.private_ip}"
        )
      else:
        # For production graphs, use the allocation manager to find in DynamoDB
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
      existing_databases_response = await client.list_databases()
      existing_database_ids = [
        db["graph_id"] for db in existing_databases_response.get("databases", [])
      ]
      if database_name in existing_database_ids:
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
        schema_type="custom",  # Use custom to skip auto schema installation
        custom_schema_ddl=None,  # We'll install schema with extensions separately
        is_subgraph=True,  # Bypass max_databases check for Enterprise/Premium
      )

      # Install schema with extensions using the same pattern as entity graph creation
      logger.info(f"Installing schema with extensions: {schema_extensions}")
      ddl = await self._generate_schema_ddl(schema_extensions or [])

      result = await client.install_schema(graph_id=database_name, custom_ddl=ddl)
      logger.info(f"Schema installation completed: {result}")
      logger.info(f"Installed schema with {len(schema_extensions or [])} extensions")

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

  async def create_subgraph(
    self,
    parent_graph: "Graph",
    user: "User",
    name: str,
    description: str | None = None,
    subgraph_type: str = "static",
    metadata: dict | None = None,
    fork_parent: bool = False,
    fork_options: Optional[Dict[str, Any]] = None,
  ) -> Dict[str, Any]:
    """
    Create a subgraph including both the database and PostgreSQL metadata.

    This method:
    1. Creates the actual LadybugDB database on the parent's instance
    2. Installs schema (base + extensions from parent)
    3. Creates PostgreSQL metadata records
    4. Creates user-graph relationship
    5. Optionally forks parent data via ingestion

    Args:
        parent_graph: Parent graph model
        user: User creating the subgraph
        name: Alphanumeric subgraph name
        description: Optional description
        subgraph_type: Type of subgraph (default: "static")
        metadata: Optional metadata dict
        fork_parent: If True, copy data from parent graph (creates a "fork")
        fork_options: Options for forking (tables, filters, etc.)

    Returns:
        Dictionary with created subgraph details
    """
    from ...models.iam.graph import Graph
    from ...models.iam.graph_user import GraphUser
    from ...database import get_db_session

    subgraph_id = construct_subgraph_id(parent_graph.graph_id, name)

    # Step 1: Create the actual LadybugDB database on the parent's instance
    logger.info(
      f"Creating LadybugDB database for subgraph {subgraph_id} on parent {parent_graph.graph_id}'s instance"
    )

    try:
      # Directly await the async database creation method
      db_creation_result = await self.create_subgraph_database(
        parent_graph_id=parent_graph.graph_id,
        subgraph_name=name,
        schema_extensions=parent_graph.schema_extensions or [],
      )
      logger.info(f"LadybugDB database created: {db_creation_result}")
    except Exception as e:
      logger.error(f"Failed to create LadybugDB database for subgraph: {e}")
      raise

    # Step 2: Create PostgreSQL metadata records
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

      # Ensure subgraph_type is stored in metadata
      subgraph_metadata = (metadata or {}).copy()
      if "subgraph_type" not in subgraph_metadata:
        subgraph_metadata["subgraph_type"] = subgraph_type

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
        subgraph_metadata=subgraph_metadata,
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

      # Step 3: Fork parent data if requested
      fork_status = None
      if fork_parent:
        logger.info(
          f"Forking parent data from {parent_graph.graph_id} to {subgraph_id}"
        )
        try:
          fork_status = await self.fork_parent_data(
            parent_graph_id=parent_graph.graph_id,
            subgraph_id=subgraph_id,
            options=fork_options,
          )
          logger.info(f"Fork completed: {fork_status}")
        except Exception as fork_error:
          logger.error(f"Fork failed but subgraph created: {fork_error}")
          # Don't fail the whole operation if fork fails
          fork_status = {"status": "failed", "error": str(fork_error)}

      return {
        "graph_id": subgraph.graph_id,
        "subgraph_index": subgraph.subgraph_index,
        "graph_type": subgraph.graph_type,
        "status": "active",
        "created_at": subgraph.created_at,
        "updated_at": subgraph.updated_at,
        "database_created": db_creation_result.get("status") == "created",
        "instance_id": db_creation_result.get("instance_id"),
        "fork_status": fork_status,
      }

    except Exception as e:
      db.rollback()
      logger.error(f"Failed to create subgraph metadata: {e}")

      if db_creation_result.get("status") == "created":
        logger.warning(
          f"Cleaning up orphaned LadybugDB database {subgraph_id} due to metadata creation failure"
        )
        try:
          await self.delete_subgraph_database(
            subgraph_id=subgraph_id,
            force=True,
            create_backup=False,
          )
          logger.info(f"Successfully cleaned up orphaned database {subgraph_id}")
        except Exception as cleanup_error:
          logger.error(
            f"Failed to clean up orphaned database {subgraph_id}: {cleanup_error}. "
            f"Manual cleanup may be required."
          )

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
      # Get the Graph API client
      # In local mode, use direct URL; in production, resolve parent's instance
      from ...graph_api.client.factory import get_graph_client_for_instance
      from ...graph_api.client import GraphClient
      from ...config import env

      # Local development mode: use GRAPH_API_URL directly
      # Check for localhost or docker container hostnames (graph-api, etc.)
      is_local = env.GRAPH_API_URL and any(
        host in env.GRAPH_API_URL for host in ["localhost", "graph-api", "127.0.0.1"]
      )

      parent_location = None
      if is_local:
        logger.info(f"Using local Graph API URL for deletion: {env.GRAPH_API_URL}")
        client = GraphClient(base_url=env.GRAPH_API_URL)
      else:
        # Production mode: resolve parent graph's location from DynamoDB
        parent_location = await self.allocation_manager.find_database_location(
          parent_graph_id
        )

        if not parent_location:
          raise GraphAllocationError(
            f"Parent graph {parent_graph_id} not found. Cannot delete subgraph."
          )

        client = await get_graph_client_for_instance(parent_location.private_ip)

      # Check if database exists
      existing_databases_response = await client.list_databases()
      existing_database_ids = [
        db["graph_id"] for db in existing_databases_response.get("databases", [])
      ]
      if database_name not in existing_database_ids:
        logger.warning(f"Database {database_name} does not exist on instance")
        return {
          "status": "not_found",
          "graph_id": subgraph_id,
          "message": "Subgraph database does not exist",
        }

      # Get instance_id for logging and response
      if is_local:
        instance_id = "local-lbug-writer"
      else:
        instance_id = parent_location.instance_id if parent_location else "unknown"

      # Create backup if requested
      backup_location = None
      if create_backup:
        logger.info(f"Creating backup of {database_name} before deletion")
        backup_location = await self._create_backup(client, database_name, instance_id)

      # Check if database contains data (unless forced)
      if not force:
        has_data = await self._check_database_has_data(client, database_name)
        if has_data:
          raise GraphAllocationError(
            f"Subgraph {subgraph_id} contains data. "
            "Use force=True to delete anyway, or create a backup first."
          )

      # Delete the database
      logger.info(f"Deleting database {database_name} from instance {instance_id}")
      await client.delete_database(database_name)

      logger.info(f"Successfully deleted subgraph database {subgraph_id}")

      return {
        "status": "deleted",
        "graph_id": subgraph_id,
        "database_name": database_name,
        "parent_graph_id": parent_graph_id,
        "instance_id": instance_id,
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
      all_databases_response = await client.list_databases()
      all_database_ids = [
        db["graph_id"] for db in all_databases_response.get("databases", [])
      ]

      # Filter for subgraphs of this parent
      subgraphs = []
      parent_prefix = f"{parent_graph_id}_"

      for db_name in all_database_ids:
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
      existing_databases_response = await client.list_databases()
      existing_database_ids = [
        db["graph_id"] for db in existing_databases_response.get("databases", [])
      ]
      if database_name not in existing_database_ids:
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

  async def _generate_schema_ddl(self, extensions: List[str]) -> str:
    """
    Generate DDL from schema extensions using SchemaManager.

    This uses the same pattern as entity graph creation to generate
    DDL from base schema + extensions.

    Args:
        extensions: List of extension names (e.g., ['roboledger'])

    Returns:
        str: Generated DDL statements
    """
    from ...schemas.manager import SchemaManager

    manager = SchemaManager()
    config = manager.create_schema_configuration(
      name="SubgraphSchema",
      description="Subgraph schema with extensions",
      extensions=extensions,
    )

    schema = manager.load_and_compile_schema(config)
    ddl = schema.to_cypher()

    statement_count = len([s for s in ddl.split(";") if s.strip()])
    logger.info(
      f"Generated DDL for subgraph: {statement_count} statements, {len(ddl)} characters"
    )

    return ddl

  async def _install_schema_with_extensions(
    self,
    client: "GraphClient",
    database_name: str,
    extensions: List[str],
  ) -> None:
    """Install schema with specified extensions."""
    try:
      # Install base schema + all extensions in a single call
      logger.info(
        f"Installing entity schema with extensions {extensions} for {database_name}"
      )
      await client.install_schema(
        graph_id=database_name, base_schema="entity", extensions=extensions
      )

      logger.info(
        f"Successfully installed schema with extensions {extensions} for {database_name}"
      )
    except Exception as e:
      logger.error(f"Failed to install schema with extensions for {database_name}: {e}")
      raise

  async def _install_base_schema(
    self,
    client: "GraphClient",
    database_name: str,
  ) -> None:
    """Install base schema only."""
    try:
      # Install the base entity schema (subgraphs always use entity schema)
      await client.install_schema(
        graph_id=database_name, base_schema="entity", extensions=[]
      )
      logger.info(f"Successfully installed base entity schema for {database_name}")
    except Exception as e:
      logger.error(f"Failed to install base entity schema for {database_name}: {e}")
      raise

  async def _check_database_has_data(
    self,
    client: "GraphClient",
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
    client: "GraphClient",
    database_name: str,
    instance_id: str,
  ) -> Optional[str]:
    """Create a backup of the database.

    Returns:
        Backup location if successful, None if backup is not implemented.

    Raises:
        Exception if backup fails unexpectedly.
    """
    try:
      timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
      backup_location = f"s3://{env.GRAPH_DATABASES_BUCKET}/{instance_id}/{database_name}_{timestamp}.backup"

      # Use the backup endpoint if available
      backup_response = await client.backup(
        graph_id=database_name, backup_location=backup_location
      )

      logger.info(f"Created backup at {backup_location}")
      return backup_response.get("location", backup_location)
    except AttributeError:
      # Backup method may not be implemented yet
      logger.warning(f"Backup not yet implemented for {database_name}")
      return None
    except Exception as e:
      logger.error(f"Failed to create backup for {database_name}: {e}")
      raise

  async def _get_database_stats(
    self,
    client: "GraphClient",
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

  async def fork_parent_data(
    self,
    parent_graph_id: str,
    subgraph_id: str,
    options: Optional[Dict[str, Any]] = None,
    progress_callback: Optional[Any] = None,
  ) -> Dict[str, Any]:
    """
    Fork data from parent graph to subgraph by calling Graph API fork endpoint.

    This method calls the Graph API's fork endpoint which attaches the parent graph's
    DuckDB staging database and copies tables directly to the subgraph's LadybugDB database.
    All operations happen on the EC2 instance where both databases live.

    Args:
        parent_graph_id: Parent graph to copy data from
        subgraph_id: Subgraph to copy data to
        options: Fork options including:
          - tables: List of table names (default: all tables)
          - exclude_patterns: List of table patterns to exclude (e.g., ["Report*"])
        progress_callback: Optional async callback function(msg, pct) for progress updates

    Returns:
        Dictionary with fork status including:
        - status: "success" or "failed"
        - tables_copied: List of tables successfully copied
        - row_count: Total rows copied
        - errors: List of any errors encountered
    """
    # Default options
    options = options or {}
    tables_to_copy = options.get("tables", [])

    logger.info(
      f"Forking data from {parent_graph_id} to {subgraph_id} with options: {options}"
    )

    try:
      # Report initial progress
      if progress_callback:
        progress_callback("Initiating fork from parent DuckDB", 10)

      # Get the Graph API client
      # In local mode, use direct URL; in production, resolve parent's instance
      from ...graph_api.client.factory import get_graph_client_for_instance
      from ...graph_api.client import GraphClient
      from ...config import env

      if progress_callback:
        progress_callback("Connecting to Graph API", 20)

      # Local development mode: use GRAPH_API_URL directly
      # Check for localhost or docker container hostnames (graph-api, etc.)
      is_local = env.GRAPH_API_URL and any(
        host in env.GRAPH_API_URL for host in ["localhost", "graph-api", "127.0.0.1"]
      )

      if is_local:
        logger.info(f"Using local Graph API URL: {env.GRAPH_API_URL}")
        client = GraphClient(base_url=env.GRAPH_API_URL)
      else:
        # Production mode: resolve parent graph's location from DynamoDB
        parent_location = await self.allocation_manager.find_database_location(
          parent_graph_id
        )
        if not parent_location:
          raise GraphAllocationError(f"Parent graph {parent_graph_id} not found")

        client = await get_graph_client_for_instance(parent_location.private_ip)

      if progress_callback:
        progress_callback("Forking tables from parent DuckDB", 30)

      logger.info(f"Calling fork endpoint for {parent_graph_id} -> {subgraph_id}")

      result = await client.fork_from_parent(
        parent_graph_id=parent_graph_id,
        subgraph_id=subgraph_id,
        tables=tables_to_copy if tables_to_copy else None,
        ignore_errors=True,
      )

      tables_copied = result.get("tables_copied", [])
      row_count = result.get("total_rows", 0)
      fork_status = result.get("status", "success")

      if progress_callback:
        progress_callback(f"Fork complete: {row_count} rows copied", 100)

      logger.info(
        f"Fork completed with status {fork_status}: "
        f"{len(tables_copied)} tables, {row_count} rows copied"
      )

      return {
        "status": fork_status,
        "tables_copied": tables_copied,
        "row_count": row_count,
        "errors": [],
        "parent_graph_id": parent_graph_id,
        "subgraph_id": subgraph_id,
      }

    except Exception as e:
      logger.error(f"Fork operation failed: {e}")
      return {
        "status": "failed",
        "error": str(e),
        "tables_copied": [],
        "row_count": 0,
      }
