"""
Generic graph service for creating and managing graph databases.

This service handles the creation of generic graph databases that are not tied
to any specific entity type (entity, user, etc). It supports flexible schema
configurations including custom schemas and schema extensions.
"""

import asyncio
import json
import uuid
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timezone
from ...logger import logger
from ...config import env
from ...models.iam import UserGraph
from ...database import get_db_session
from ...middleware.graph.allocation_manager import KuzuAllocationManager
from ...middleware.graph.types import InstanceTier


class GenericGraphService:
  """Service for generic graph database operations."""

  async def create_graph(
    self,
    graph_id: Optional[str],
    schema_extensions: List[str],
    metadata: Dict[str, Any],
    tier: str,
    initial_data: Optional[Dict[str, Any]],
    user_id: str,
    custom_schema: Optional[Dict[str, Any]] = None,
    cancellation_callback: Optional[Callable] = None,
    progress_callback: Optional[Callable] = None,
  ) -> Dict[str, Any]:
    """
    Create a new graph database with flexible configuration.

    This method:
    1. Validates user permissions and limits
    2. Generates or validates graph_id
    3. Allocates database on optimal cluster
    4. Installs schema extensions
    5. Stores metadata
    6. Optionally populates initial data
    7. Creates user-graph relationship

    Args:
        graph_id: Requested graph ID (auto-generated if None)
        schema_extensions: List of schema extensions to install
        metadata: Graph metadata (name, description, type, tags)
        tier: Service tier (standard, enterprise, premium)
        initial_data: Optional initial data to populate
        user_id: ID of the user creating the graph
        cancellation_callback: Optional callback to check for cancellation
        progress_callback: Optional callback to report progress

    Returns:
        Dictionary containing graph_id and creation details
    """
    logger.info(f"Starting graph creation for user {user_id}")

    # Always generate graph ID using the standard kg pattern
    # This ensures security and consistency across all graph types
    # Using UUID directly for simplicity and uniqueness
    unique_id = str(uuid.uuid4()).replace("-", "")[:16]
    graph_id = f"kg{unique_id}"

    logger.info(f"Creating graph with ID: {graph_id}")

    # Step 1: Validate user permissions using UserLimits
    if progress_callback:
      progress_callback("Checking user limits and permissions...", 10)

    from ...models.iam import UserLimits

    db_gen = get_db_session()
    db = next(db_gen)
    try:
      # Check user's graph creation limits (subscription-based)
      user_limits = UserLimits.get_or_create_for_user(user_id, db)
      can_create, reason = user_limits.can_create_user_graph(db)
      if not can_create:
        raise ValueError(reason)
    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass

    # Check for cancellation
    if cancellation_callback:
      cancellation_callback()

    # Step 2: Allocate database on Kuzu cluster
    if progress_callback:
      progress_callback("Allocating database cluster...", 20)

    logger.info(f"Allocating database for graph {graph_id}")

    # Get environment from centralized config
    allocation_manager = KuzuAllocationManager(environment=env.ENVIRONMENT)
    # Note: tier is not used by allocation manager in current implementation
    cluster_info = await allocation_manager.allocate_database(
      entity_id=user_id,  # Use user_id as entity
      graph_id=graph_id,
      graph_type=metadata.get("type", "custom"),
      instance_tier=InstanceTier(tier) if tier else None,
    )

    if not cluster_info:
      raise RuntimeError(f"Failed to allocate database for graph {graph_id}")

    logger.info(
      f"Allocated graph {graph_id} on instance {cluster_info.instance_id} "
      f"at {cluster_info.private_ip}"
    )

    # Check for cancellation
    if cancellation_callback:
      cancellation_callback()

    # Step 3: Prepare schema for database creation
    custom_ddl = None

    if custom_schema:
      logger.info("Preparing custom custom schema")
      logger.info(f"Schema name: {custom_schema.get('name', 'custom')}")
      logger.info(f"Schema version: {custom_schema.get('version', 'unversioned')}")

      # Parse and validate custom schema
      from ...schemas.custom import (
        CustomSchemaManager,
      )

      manager = CustomSchemaManager()

      try:
        parsed_schema = manager.create_from_dict(custom_schema)

        # Check if it extends base
        if custom_schema.get("extends") == "base":
          parsed_schema = manager.merge_with_base(parsed_schema)

        # Generate DDL
        custom_ddl = parsed_schema.to_cypher()
        # Count DDL statements
        statement_count = len([s for s in custom_ddl.split(";") if s.strip()])
        logger.info(
          f"Generated custom DDL: {statement_count} statements, {len(custom_ddl)} characters"
        )
        logger.debug(f"DDL preview (first 200 chars): {custom_ddl[:200]}...")

      except Exception as e:
        logger.error(f"Failed to parse custom schema: {e}")
        raise ValueError(f"Invalid custom schema: {str(e)}")

    # Step 4: Create the database with schema
    if progress_callback:
      progress_callback("Creating graph database...", 40)

    logger.info(f"Creating database {graph_id} on Kuzu writer")

    # Use KuzuClient with proper API key
    from ...kuzu_api.client import get_kuzu_client_for_instance

    kuzu_client = await get_kuzu_client_for_instance(cluster_info.private_ip)

    try:
      schema_type = "custom" if custom_schema else "entity"
      result = await kuzu_client.create_database(
        graph_id=graph_id,
        schema_type=schema_type,
        custom_schema_ddl=custom_ddl if custom_schema else None,
      )
      logger.info(f"Database {graph_id} created successfully with schema: {result}")
    finally:
      await kuzu_client.close()

    # Step 5: Handle schema extensions (if no custom schema)
    if not custom_schema and schema_extensions:
      if progress_callback:
        progress_callback("Installing schema extensions...", 60)

      logger.info("Installing schema extensions for generic graph")
      logger.info(f"Extensions requested: {schema_extensions}")

      # Use KuzuClient for schema installation with proper API key
      kuzu_client = await get_kuzu_client_for_instance(cluster_info.private_ip)

      try:
        # Use the new install_schema method
        result = await kuzu_client.install_schema(
          graph_id=graph_id, base_schema="base", extensions=schema_extensions
        )
        logger.info("Schema installation completed successfully")
        logger.info(f"Result: {result}")
        logger.info(
          f"Generic graph schema installed with base + {len(schema_extensions)} extensions"
        )
      except Exception as e:
        logger.error(f"Failed to install schema extensions: {e}")
        raise RuntimeError(f"Schema installation failed: {e}")
      finally:
        await kuzu_client.close()

    # Check for cancellation
    if cancellation_callback:
      cancellation_callback()

    # Step 4: Store graph metadata in the graph
    if progress_callback:
      progress_callback("Storing graph metadata...", 70)

    logger.info("Storing graph metadata in the graph")

    # Use direct connection to writer instance for metadata storage
    # since the graph was just created and readers may not be available yet
    kuzu_client = await get_kuzu_client_for_instance(cluster_info.private_ip)

    try:
      # Create metadata node
      metadata_cypher = """
        CREATE (m:GraphMetadata {
            identifier: $identifier,
            graph_id: $graph_id,
            name: $name,
            description: $description,
            type: $type,
            tags: $tags,
            created_at: $created_at,
            updated_at: $updated_at,
            created_by: $user_id,
            tier: $tier,
            schema_extensions: $schema_extensions,
            schema_type: $schema_type,
            custom_schema_name: $custom_schema_name,
            custom_schema_version: $custom_schema_version,
            status: $status,
            access_level: $access_level
        })
        RETURN m
        """

      # Get current timestamp as string
      current_time = datetime.now(timezone.utc).isoformat()

      from ...utils.uuid import generate_deterministic_uuid7

      metadata_params = {
        "identifier": generate_deterministic_uuid7(
          graph_id, namespace="graph_metadata"
        ),
        "graph_id": graph_id,
        "name": metadata.get("name", graph_id),
        "description": metadata.get("description", ""),
        "type": metadata.get("type", "generic"),
        "tags": json.dumps(metadata.get("tags", [])),  # Convert to JSON string
        "created_at": current_time,
        "updated_at": current_time,
        "user_id": user_id,
        "tier": tier,
        "schema_extensions": json.dumps(
          schema_extensions if not custom_schema else []
        ),  # Convert to JSON string
        "schema_type": "custom" if custom_schema else "extensions",
        "custom_schema_name": custom_schema.get("name") if custom_schema else None,
        "custom_schema_version": custom_schema.get("version")
        if custom_schema
        else None,
        "status": "active",
        "access_level": metadata.get("access_level", "private"),
      }

      try:
        await kuzu_client.query(metadata_cypher, graph_id, metadata_params)
      except Exception as e:
        logger.error(f"Failed to store graph metadata: {e}")
        # Don't fail the entire creation if metadata storage fails
        logger.warning("Graph created but metadata storage failed - will retry later")

      # Store custom metadata if provided
      if metadata.get("custom_metadata"):
        custom_cypher = """
              MATCH (m:GraphMetadata {graph_id: $graph_id})
              SET m.custom_metadata = $custom_metadata
              """
        try:
          await kuzu_client.query(
            custom_cypher,
            graph_id,
            {
              "graph_id": graph_id,
              "custom_metadata": json.dumps(metadata["custom_metadata"]),
            },
          )
        except Exception as e:
          logger.warning(f"Failed to store custom metadata: {e}")
    finally:
      # Close the kuzu client connection
      await kuzu_client.close()

    # Step 5: Populate initial data if provided
    if initial_data:
      logger.info("Populating initial data")
      # This would be implementation-specific based on the data structure
      # For now, we'll skip this as it's highly dependent on use case
      pass

    # Check for cancellation
    if cancellation_callback:
      cancellation_callback()

    # Step 6: Create graph metadata and user-graph relationship
    if progress_callback:
      progress_callback("Setting up user access...", 80)

    logger.info("Creating graph metadata and user-graph relationship")

    db_gen = get_db_session()
    db = next(db_gen)
    try:
      # First, create Graph entry to store metadata
      from ...models.iam.graph import Graph

      graph = Graph.create(
        graph_id=graph_id,
        graph_name=metadata.get("name", graph_id),
        graph_type="generic",  # This is a generic graph, not a entity graph
        session=db,
        base_schema=None
        if custom_schema
        else "base",  # Only set base_schema if using extensions
        schema_extensions=schema_extensions if not custom_schema else [],
        graph_instance_id=cluster_info.instance_id,
        graph_cluster_region=None,  # Could be populated from cluster info if available
        graph_metadata={
          "created_by": user_id,
          "description": metadata.get("description", ""),
          "type": metadata.get("type", "generic"),
          "tags": metadata.get("tags", []),
          "custom_schema_name": custom_schema.get("name") if custom_schema else None,
          "custom_schema_version": custom_schema.get("version")
          if custom_schema
          else None,
          "access_level": metadata.get("access_level", "private"),
        },
      )

      logger.info(f"Graph metadata created: {graph}")

      # Then create UserGraph relationship
      user_graph = UserGraph(
        user_id=user_id,
        graph_id=graph_id,
        role="admin",  # Owner gets admin role
        is_selected=True,  # Set as selected graph for the user
      )
      db.add(user_graph)
      db.commit()

      # Step 7: Create credit pool for the new graph
      if progress_callback:
        progress_callback("Creating credit pool...", 90)

      logger.info("Creating credit pool for the new graph")
      try:
        from .credit_service import CreditService
        from ...models.iam.graph_credits import GraphTier

        # For now, use the graph tier as the subscription tier
        # TODO: When subscription management is implemented, get from user's subscription
        subscription_tier = tier.lower()  # Use the requested tier

        # Map instance tier to graph tier
        tier_mapping = {
          "enterprise": GraphTier.ENTERPRISE,
          "premium": GraphTier.PREMIUM,
          "standard": GraphTier.STANDARD,
        }
        graph_tier = tier_mapping.get(tier.lower(), GraphTier.STANDARD)

        # Create credit pool
        credit_service = CreditService(db)
        credit_service.create_graph_credits(
          graph_id=graph_id,
          user_id=user_id,
          billing_admin_id=user_id,  # Creator is billing admin
          subscription_tier=subscription_tier,
          graph_tier=graph_tier,
        )
        logger.info(f"Credit pool created for graph: {graph_id}")
      except Exception as credit_error:
        logger.error(
          f"Failed to create credit pool for graph {graph_id}: {credit_error}"
        )
        # Don't fail the entire graph creation if credit pool fails
        # The graph is already created and can have credits added later

    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass

    # Return success result
    result = {
      "graph_id": graph_id,
      "status": "created",
      "cluster_info": {
        "instance_id": cluster_info.instance_id,
        "private_ip": cluster_info.private_ip,
        "api_endpoint": f"http://{cluster_info.private_ip}:8001",
      },
      "metadata": {
        "graph_id": graph_id,
        "name": metadata.get("name", graph_id),
        "description": metadata.get("description", ""),
        "type": metadata.get("type", "generic"),
        "tags": metadata.get("tags", []),  # Return as list, not JSON string
        "user_id": user_id,
        "tier": tier,
        "schema_extensions": schema_extensions if not custom_schema else [],
        "schema_type": "custom" if custom_schema else "extensions",
        "custom_schema_name": custom_schema.get("name") if custom_schema else None,
      },
      "schema_info": {
        "type": "custom" if custom_schema else "extensions",
        "extensions": schema_extensions if not custom_schema else [],
        "custom_schema_name": custom_schema.get("name") if custom_schema else None,
        "custom_schema_version": custom_schema.get("version")
        if custom_schema
        else None,
      },
      "tier": tier,
      "created_at": datetime.now(timezone.utc).isoformat(),
    }

    logger.info(f"Graph creation completed: {graph_id}")
    return result


class GenericGraphServiceSync:
  """Synchronous wrapper for GenericGraphService."""

  def __init__(self):
    self._async_service = GenericGraphService()

  def create_graph(
    self,
    graph_id: Optional[str],
    schema_extensions: List[str],
    metadata: Dict[str, Any],
    tier: str,
    initial_data: Optional[Dict[str, Any]],
    user_id: str,
    custom_schema: Optional[Dict[str, Any]] = None,
    cancellation_callback: Optional[Callable] = None,
    progress_callback: Optional[Callable] = None,
  ) -> Dict[str, Any]:
    """Synchronous wrapper for async graph creation."""
    return asyncio.run(
      self._async_service.create_graph(
        graph_id=graph_id,
        schema_extensions=schema_extensions,
        metadata=metadata,
        tier=tier,
        initial_data=initial_data,
        user_id=user_id,
        custom_schema=custom_schema,
        cancellation_callback=cancellation_callback,
        progress_callback=progress_callback,
      )
    )
