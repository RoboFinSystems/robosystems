"""
Entity Graph Service for entity-specific graph creation and management.

This service handles the creation and management of entity-specific graph databases
with optimized workflows for entity entities. It integrates with the cluster-based
architecture and provides entity-specific schema and data management.

Key features:
- Entity graph creation using writer clusters
- Automatic cluster selection based on capacity and tier
- API-based database creation (no direct file access)
- Entity-specific schema installation via API
- Multi-tenant database routing
- Entity entity creation and management
"""

import hashlib
import logging
from collections.abc import Callable
from typing import Any

from sqlalchemy.orm import Session, scoped_session

from ...config import env
from ...config.graph_tier import GraphTier
from ...exceptions import (
  GraphAllocationError,
  InsufficientPermissionsError,
)
from ...graph_api.client import GraphClient, get_graph_client_for_instance
from ...middleware.graph.allocation_manager import LadybugAllocationManager
from ...models.api import EntityCreate, EntityResponse
from ...models.iam import GraphUser, OrgLimits, OrgUser

logger = logging.getLogger(__name__)


SUBSCRIPTION_TO_GRAPH_TIER = {
  "ladybug-standard": GraphTier.LADYBUG_STANDARD,
  "ladybug-large": GraphTier.LADYBUG_LARGE,
  "ladybug-xlarge": GraphTier.LADYBUG_XLARGE,
}


class EntityGraphService:
  """
  Service class for entity graph-related business operations.

  This service handles entity graph creation using the graph cluster
  architecture with writer/reader nodes and API-based access. It provides
  entity-specific workflows and optimizations.
  """

  def __init__(
    self, session: Session | scoped_session[Session] | None = None
  ) -> None:
    if session is None:
      # For backward compatibility, create a new session if not provided
      from ...database import session as global_session

      self.session = global_session
    else:
      self.session = session

  async def create_entity_with_new_graph(
    self,
    entity_data_dict: dict[str, Any],
    user_id: str,
    tier: str | None = None,
    cancellation_callback: Callable | None = None,
    progress_callback: Callable | None = None,
  ) -> dict[str, Any]:
    """
    Create a new entity with its own graph database.

    This method creates a entity graph using the graph cluster architecture:
    1. Validates user limits
    2. Selects optimal graph writer cluster
    3. Creates database via API
    4. Installs schema via API
    5. Creates entity node
    6. Sets up user-graph relationship

    Args:
        entity_data_dict: Entity creation data as dictionary
        user_id: ID of the user creating the entity
        tier: Service tier (ladybug-shared, ladybug-standard, ladybug-large, ladybug-xlarge)
        cancellation_callback: Optional callback to check for cancellation

    Returns:
        Dictionary containing graph_id and entity information

    Raises:
        Exception: If any step of the process fails
    """
    logger.info(f"Starting entity creation for user_id: {user_id}")

    # Convert dict to Pydantic model
    entity_data = EntityCreate(**entity_data_dict)
    logger.info(
      f"Entity data parsed: name='{entity_data.name}', uri='{entity_data.uri}'"
    )

    # Check org limits before proceeding
    logger.info("Checking organization limits for entity graph creation...")
    if progress_callback:
      progress_callback("Checking organization limits and permissions...", 20)

    user_orgs = OrgUser.get_user_orgs(user_id, self.session)
    if not user_orgs:
      raise InsufficientPermissionsError(
        required_permission="create_graph",
        resource="user_graph",
        user_id=user_id,
      )

    org_id = user_orgs[0].org_id
    org_limits = OrgLimits.get_or_create_for_org(org_id, self.session)
    can_create, reason = org_limits.can_create_graph(self.session)

    if not can_create:
      logger.error(f"Organization {org_id} cannot create graph: {reason}")
      raise InsufficientPermissionsError(
        required_permission="create_graph", resource="user_graph", user_id=user_id
      )

    logger.info(f"Organization {org_id} can create graph: {reason}")

    # Check for cancellation before proceeding
    if cancellation_callback:
      cancellation_callback()

    # Generate unique graph_id
    graph_id = self._generate_graph_id(entity_data.name)
    logger.info(f"Generated graph_id: {graph_id}")

    # Determine graph tier from subscription
    graph_tier = SUBSCRIPTION_TO_GRAPH_TIER.get(
      tier if tier else "ladybug-standard", GraphTier.LADYBUG_STANDARD
    )

    logger.info(f"Graph tier: {graph_tier.value}")

    # Check for cancellation before cluster selection
    if cancellation_callback:
      cancellation_callback()

    # Initialize resource tracking variables
    db_location = None
    graph_client = None
    allocation_manager = None

    try:
      # Allocate database using LadybugAllocationManager
      if progress_callback:
        progress_callback("Allocating database cluster...", 30)

      allocation_manager = LadybugAllocationManager(environment=env.ENVIRONMENT)

      logger.info(f"Allocating database {graph_id} for entity {entity_data.name}")

      # Allocate the database in DynamoDB and get location
      # Use graph_id as entity_id since it's already a valid identifier
      db_location = await allocation_manager.allocate_database(
        entity_id=graph_id,  # Use graph_id as entity_id
        graph_id=graph_id,
        graph_type="entity",  # This is a entity graph
        instance_tier=graph_tier,
      )

      logger.info(
        f"Database allocated to instance {db_location.instance_id} at {db_location.private_ip}"
      )

      # Create LadybugClient directly with the allocated instance endpoint
      # For database creation, we need direct instance access
      # Create client with direct instance access
      graph_client = await get_graph_client_for_instance(db_location.private_ip)

      logger.info(f"Creating graph database: {graph_id} on allocated instance")

      # Check for cancellation before database creation
      if cancellation_callback:
        cancellation_callback()

      # Create database via API
      if progress_callback:
        progress_callback("Creating graph database...", 40)

      try:
        await graph_client.get_database(graph_id)
        logger.info(f"Database {graph_id} already exists")
      except Exception as e:
        if getattr(e, "status_code", None) == 404:
          # Database doesn't exist, create it
          await graph_client.create_database(graph_id, schema_type="entity")
          logger.info(f"Created database {graph_id}")
        else:
          raise

      # Create Graph record FIRST before any dependent records
      # This must happen before GraphSchema and GraphTable creation
      from ...models.iam.graph import Graph

      graph_tier_str = entity_data_dict.get("graph_tier", "ladybug-standard")
      graph_tier = SUBSCRIPTION_TO_GRAPH_TIER.get(
        graph_tier_str, GraphTier.LADYBUG_STANDARD
      )
      schema_extensions = entity_data.extensions or []

      Graph.create(
        graph_id=graph_id,
        graph_name=entity_data.name,
        graph_type="entity",
        org_id=org_id,
        session=self.session,
        base_schema="base",
        schema_extensions=schema_extensions,
        graph_instance_id=db_location.instance_id,
        graph_tier=graph_tier,
        graph_metadata={
          "created_by": user_id,
          "entity_type": "entity",
        },
        commit=True,
      )
      logger.info(f"Graph metadata created for {graph_id}")

      # Install entity graph schema with selected extensions
      # Use the LadybugClient directly instead of repository
      if progress_callback:
        progress_callback("Installing schema extensions...", 60)

      schema_ddl = await self._install_entity_schema(
        graph_client, graph_id, entity_data.extensions
      )

      # Persist schema DDL to PostgreSQL for validation and versioning
      from ...models.iam import GraphSchema

      GraphSchema.create(
        graph_id=graph_id,
        schema_type="entity",
        schema_ddl=schema_ddl,
        schema_json={
          "base": "entity",
          "extensions": entity_data.extensions or [],
        },
        session=self.session,
        commit=False,
      )
      logger.info(f"Persisted schema DDL for graph {graph_id} to PostgreSQL")

      # Auto-create DuckDB staging tables from schema
      from ..graph.table_service import TableService

      table_service = TableService(self.session)
      created_tables = table_service.create_tables_from_schema(
        graph_id=graph_id,
        user_id=user_id,
        schema_ddl=schema_ddl,
      )
      self.session.commit()
      logger.info(
        f"Auto-created {len(created_tables)} DuckDB staging tables for graph {graph_id}"
      )

      # Create entity node in graph (if requested)
      create_entity = entity_data_dict.get("create_entity", True)
      if not isinstance(create_entity, bool):
        raise ValueError(
          f"create_entity must be a boolean, got {type(create_entity).__name__}"
        )
      entity_response = None

      if create_entity:
        if progress_callback:
          progress_callback("Creating entity node...", 70)

        entity_response = await self._create_entity_in_graph_lbug(
          graph_client, entity_data, graph_id, user_id, org_id
        )
      else:
        logger.info("Skipping entity node creation (create_entity=False)")

      # NOTE: Platform metadata (GraphMetadata, User, Connection nodes) are now
      # stored exclusively in PostgreSQL, not in the graph database.
      # This keeps the graph focused on business data only.

      # Create user-graph relationship in PostgreSQL
      # Graph record was already created earlier, just need GraphUser
      if progress_callback:
        progress_callback("Setting up user access...", 80)

      from ...models.iam.graph_user import GraphUser

      user_graph = GraphUser(
        user_id=user_id,
        graph_id=graph_id,
        role="admin",
        is_selected=True,
      )

      # Deselect other graphs for this user
      self.session.query(GraphUser).filter(
        GraphUser.user_id == user_id, GraphUser.graph_id != graph_id
      ).update({"is_selected": False})

      self.session.add(user_graph)
      self.session.commit()

      logger.info("User-graph relationship created successfully")

      # Create credit pool for the new graph
      try:
        from .credit_service import CreditService

        # Get graph tier from entity data (passed from the request)
        # This determines credit costs and allocation
        graph_tier_str = entity_data_dict.get("graph_tier", "ladybug-standard")
        credit_graph_tier = SUBSCRIPTION_TO_GRAPH_TIER.get(
          graph_tier_str, GraphTier.LADYBUG_STANDARD
        )

        # Map graph tier directly to subscription tier (1:1 mapping)
        # This determines the monthly credit allocation for the graph
        if credit_graph_tier == GraphTier.LADYBUG_XLARGE:
          subscription_tier = "ladybug-xlarge"
        elif credit_graph_tier == GraphTier.LADYBUG_LARGE:
          subscription_tier = "ladybug-large"
        else:
          subscription_tier = "ladybug-standard"

        # Create credit pool
        if progress_callback:
          progress_callback("Creating credit pool...", 90)

        credit_service = CreditService(self.session)
        credit_service.create_graph_credits(
          graph_id=graph_id,
          user_id=user_id,
          billing_admin_id=user_id,  # Creator is billing admin
          subscription_tier=subscription_tier,
          graph_tier=credit_graph_tier,
        )
        logger.info(f"Credit pool created for graph: {graph_id}")
      except Exception as credit_error:
        logger.error(
          f"Failed to create credit pool for graph {graph_id}: {credit_error}"
        )
        # Don't fail the entire graph creation if credit pool fails
        # The graph is already created and can have credits added later

      if entity_response:
        logger.info(
          f"Entity creation completed successfully! "
          f"Graph: {graph_id}, Entity: {entity_response.name}, "
          f"Instance: {db_location.instance_id}"
        )
      else:
        logger.info(
          f"Graph creation completed successfully (without entity)! "
          f"Graph: {graph_id}, Instance: {db_location.instance_id}"
        )

      # Close LadybugClient connection
      if graph_client:
        await graph_client.close()

      return {
        "graph_id": graph_id,
        "entity": entity_response.model_dump() if entity_response else None,
      }

    except Exception as e:
      logger.error(f"Entity creation failed: {type(e).__name__}: {e!s}")
      if "allocation" in str(e).lower():
        raise GraphAllocationError(
          reason=str(e), graph_id=graph_id, entity_name=entity_data.name
        )

      # Enhanced error logging for different failure types
      if db_location:
        logger.error(f"Database allocated but creation failed for graph: {graph_id}")
        logger.error(f"Database location was: {db_location}")
      else:
        logger.error(f"Database allocation failed for graph: {graph_id}")

      if graph_client:
        logger.error("LadybugClient was created but operation failed")

      # Cleanup on failure
      try:
        # Deallocate database if it was allocated
        if db_location and allocation_manager:
          logger.info(f"Deallocating database {graph_id} due to creation failure")
          await allocation_manager.deallocate_database(graph_id)
          logger.info(f"Successfully deallocated database {graph_id}")

        # Close LadybugClient if it was created
        if graph_client:
          logger.info("Closing LadybugClient for failed operation")
          await graph_client.close()
          logger.info("Successfully closed LadybugClient")
      except Exception as cleanup_error:
        logger.error(
          f"Cleanup failed for {graph_id}: {type(cleanup_error).__name__}: {cleanup_error}"
        )

      raise

  def _generate_graph_id(self, entity_name: str) -> str:
    """Generate time-ordered graph ID with entity-specific entropy.

    Uses ULID for sequential ordering (optimal B-tree performance) plus
    entity name hash for additional uniqueness and traceability.

    The ID must be 16+ hex characters after the 'kg' prefix to match
    the API validation pattern: ^(kg[a-f0-9]{16,}|sec|industry|economic)$
    """
    from ...utils.ulid import generate_ulid_hex

    # Generate time-ordered ULID (provides timestamp + randomness)
    base_id = generate_ulid_hex(14)

    # Add entity name entropy (4 chars) for traceability
    entity_hash = hashlib.sha256(entity_name.encode()).hexdigest()[:4]

    # Total: 14 + 4 = 18 chars after 'kg' prefix
    graph_id = f"kg{base_id}{entity_hash}"
    return graph_id

  async def _install_entity_schema(
    self, graph_client: GraphClient, graph_id: str, extensions: list | None = None
  ) -> str:
    """
    Install entity graph schema via Graph API using selected extensions.

    This is a direct version that uses GraphClient instead of repository.

    Returns:
        str: The generated DDL that was installed
    """
    logger.info(f"Installing entity schema for graph: {graph_id}")
    logger.info("Schema type: base + extensions")
    logger.info(f"Extensions requested: {extensions or ['none - base schema only']}")

    # Generate DDL from schema extensions
    try:
      from ...schemas.manager import SchemaManager

      # Create schema manager and configuration
      manager = SchemaManager()
      config = manager.create_schema_configuration(
        name="EntitySchema",
        description="Entity schema with extensions",
        extensions=extensions or [],
      )

      # Load and compile schema
      schema = manager.load_and_compile_schema(config)

      # Generate DDL
      ddl = schema.to_cypher()

      # Count DDL statements
      statement_count = len([s for s in ddl.split(";") if s.strip()])
      logger.info(f"Generated DDL: {statement_count} statements, {len(ddl)} characters")
      logger.debug(f"DDL preview (first 200 chars): {ddl[:200]}...")

      # Install schema using DDL
      result = await graph_client.install_schema(graph_id=graph_id, custom_ddl=ddl)
      logger.info("Schema installation completed successfully")
      logger.info(f"Result: {result}")
      logger.info(
        f"Entity graph schema installed with base + {len(extensions or [])} extensions"
      )

      return ddl
    except Exception as e:
      logger.error(f"Failed to install schema: {e}")
      raise

  def _map_schema_type_to_lbug(self, schema_type: str) -> str:
    """Map schema property types to LadybugDB types."""
    type_mapping = {
      "STRING": "STRING",
      "INT64": "INT64",
      "INT32": "INT32",
      "DOUBLE": "DOUBLE",
      "FLOAT": "FLOAT",
      "BOOLEAN": "BOOLEAN",
      "TIMESTAMP": "TIMESTAMP",
      "DATE": "DATE",
      "BLOB": "BLOB",
    }

    return type_mapping.get(schema_type.upper(), "STRING")

  def _generate_entity_data_for_upload(
    self, entity_data: EntityCreate, graph_id: str
  ) -> dict:
    """
    Generate entity data as a dictionary ready for Parquet conversion.

    Returns a single-row dict matching the Entity table schema.
    """
    import datetime

    entity_identifier = f"entity_{graph_id}"
    current_time = datetime.datetime.now(datetime.UTC).isoformat()

    entity_uri = entity_data.uri or f"https://robosystems.ai/entities#{graph_id}"

    return {
      "identifier": entity_identifier,
      "uri": entity_uri,
      "scheme": None,
      "cik": entity_data.cik,
      "ticker": None,
      "exchange": None,
      "name": entity_data.name,
      "legal_name": entity_data.name,
      "industry": None,
      "entity_type": None,
      "sic": entity_data.sic,
      "sic_description": entity_data.sic_description,
      "category": entity_data.category,
      "state_of_incorporation": entity_data.state_of_incorporation,
      "fiscal_year_end": entity_data.fiscal_year_end,
      "ein": entity_data.ein,
      "tax_id": None,
      "lei": None,
      "phone": None,
      "website": entity_data.uri,
      "status": "active",
      "is_parent": True,
      "parent_entity_id": None,
      "created_at": current_time,
      "updated_at": current_time,
    }

  async def _create_entity_in_graph_lbug(
    self,
    graph_client: GraphClient,
    entity_data: EntityCreate,
    graph_id: str,
    user_id: str,
    org_id: str,
  ) -> EntityResponse:
    """
    Create entity node using the controlled table → file → ingest pattern.

    This method follows the controlled ingestion workflow:
    1. Generate entity data
    2. Convert to Parquet
    3. Upload to S3
    4. Create GraphFile record
    5. Trigger ingestion
    """
    logger.info(
      f"Creating entity node in graph {graph_id} using controlled ingestion pattern"
    )

    import uuid

    try:
      # Step 1: Generate entity data
      entity_row = self._generate_entity_data_for_upload(entity_data, graph_id)
      logger.info(f"Generated entity data for {entity_row['identifier']}")

      # Step 2: Convert to Parquet in-memory
      import io

      import pyarrow as pa
      import pyarrow.parquet as pq

      schema_fields = [
        ("identifier", pa.string()),
        ("uri", pa.string()),
        ("scheme", pa.string()),
        ("cik", pa.string()),
        ("ticker", pa.string()),
        ("exchange", pa.string()),
        ("name", pa.string()),
        ("legal_name", pa.string()),
        ("industry", pa.string()),
        ("entity_type", pa.string()),
        ("sic", pa.string()),
        ("sic_description", pa.string()),
        ("category", pa.string()),
        ("state_of_incorporation", pa.string()),
        ("fiscal_year_end", pa.string()),
        ("ein", pa.string()),
        ("tax_id", pa.string()),
        ("lei", pa.string()),
        ("phone", pa.string()),
        ("website", pa.string()),
        ("status", pa.string()),
        ("is_parent", pa.bool_()),
        ("parent_entity_id", pa.string()),
        ("created_at", pa.string()),
        ("updated_at", pa.string()),
      ]
      schema = pa.schema(pa.field(name, type_) for name, type_ in schema_fields)
      table_data = {name: [entity_row.get(name)] for name, _ in schema_fields}
      table = pa.Table.from_pydict(table_data, schema=schema)
      parquet_buffer = io.BytesIO()
      pq.write_table(table, parquet_buffer)
      parquet_bytes = parquet_buffer.getvalue()
      parquet_buffer.seek(0)

      logger.info(f"Converted entity data to Parquet ({len(parquet_bytes)} bytes)")

      # Step 3: Upload to S3
      from robosystems.operations.aws.s3 import S3Client

      from ...models.iam import GraphFile, GraphTable

      s3_client = S3Client()
      file_id = str(uuid.uuid4())
      s3_key = f"user-staging/{user_id}/{graph_id}/Entity/{file_id}/entity.parquet"

      s3_client.s3_client.upload_fileobj(parquet_buffer, env.AWS_S3_BUCKET, s3_key)

      logger.info(f"Uploaded entity Parquet to S3: {s3_key}")

      # Step 4: Create GraphFile record
      entity_table = GraphTable.get_by_name(graph_id, "Entity", self.session)
      if not entity_table:
        raise RuntimeError(
          f"Entity table not found for graph {graph_id}. "
          "Ensure create_tables_from_schema was called."
        )

      graph_file = GraphFile.create(
        graph_id=graph_id,
        table_id=entity_table.id,
        file_name="entity.parquet",
        s3_key=s3_key,
        file_format="parquet",
        file_size_bytes=len(parquet_bytes),
        upload_method="direct",
        upload_status="completed",
        row_count=1,
        session=self.session,
        commit=False,
      )

      # Step 5: Update table file count
      entity_table.file_count = (entity_table.file_count or 0) + 1
      self.session.commit()

      logger.info(f"Created GraphFile record {graph_file.id}")

      # Step 6: Trigger ingestion via Graph API
      s3_pattern = f"s3://{env.AWS_S3_BUCKET}/user-staging/{user_id}/{graph_id}/Entity/**/*.parquet"

      logger.info(f"Creating DuckDB staging table with pattern: {s3_pattern}")
      await graph_client.create_table(
        graph_id=graph_id, table_name="Entity", s3_pattern=s3_pattern
      )

      logger.info("Materializing Entity table to LadybugDB graph database")
      ingest_response = await graph_client.materialize_table(
        graph_id=graph_id,
        table_name="Entity",
        ignore_errors=False,
      )

      rows_ingested = ingest_response.get("rows_ingested", 0)
      logger.info(
        f"Entity node created via controlled materialization: {rows_ingested} rows"
      )

      # Step 7: Return EntityResponse
      return EntityResponse(
        id=entity_row["identifier"],
        name=entity_row["name"],
        uri=entity_row["website"],
        cik=entity_row["cik"],
        database=graph_id,
        sic=entity_row["sic"],
        sic_description=entity_row["sic_description"],
        category=entity_row["category"],
        state_of_incorporation=entity_row["state_of_incorporation"],
        fiscal_year_end=entity_row["fiscal_year_end"],
        ein=entity_row["ein"],
        created_at=entity_row["created_at"],
        updated_at=entity_row["updated_at"],
      )

    except Exception as e:
      logger.error(
        f"Failed to create entity node via controlled ingestion: {type(e).__name__}: {e!s}"
      )
      raise RuntimeError(
        f"Failed to create entity node via controlled ingestion: {e!s}"
      )

  def _create_user_graph_relationship(
    self,
    user_id: str,
    graph_id: str,
    entity_name: str,
    cluster_id: str,
    org_id: str,
    extensions: list | None = None,
    graph_tier_str: str = "ladybug-standard",
  ) -> None:
    """Create user-graph relationship in PostgreSQL."""
    logger.info(f"Creating user-graph relationship: {user_id} -> {graph_id}")

    try:
      # First, create Graph entry to store metadata
      from ...config.graph_tier import GraphTier
      from ...models.iam.graph import Graph

      # Use the provided extensions or default to empty list
      schema_extensions = extensions or []

      # Convert tier string to GraphTier enum
      graph_tier = SUBSCRIPTION_TO_GRAPH_TIER.get(
        graph_tier_str, GraphTier.LADYBUG_STANDARD
      )

      graph = Graph.create(
        graph_id=graph_id,
        graph_name=entity_name,
        graph_type="entity",
        org_id=org_id,
        session=self.session,
        base_schema="base",
        schema_extensions=schema_extensions,
        graph_instance_id=cluster_id,
        graph_tier=graph_tier,
        graph_metadata={
          "created_by": user_id,
          "entity_type": "entity",
        },
        commit=False,
      )

      logger.info(f"Graph metadata created: {graph}")

      # Create GraphUser entry
      user_graph = GraphUser(
        user_id=user_id,
        graph_id=graph_id,
        role="admin",  # Creator gets admin role
        is_selected=True,  # New graph becomes selected
      )

      # Deselect other graphs for this user
      self.session.query(GraphUser).filter(
        GraphUser.user_id == user_id, GraphUser.graph_id != graph_id
      ).update({"is_selected": False})

      self.session.add(user_graph)
      self.session.commit()

      logger.info("User-graph relationship created successfully")

    except Exception as e:
      logger.error(f"Failed to create user-graph relationship: {e}")
      self.session.rollback()
      raise RuntimeError(f"User-graph relationship creation failed: {e}")

  async def _cleanup_failed_database(
    self, repository: GraphClient, graph_id: str
  ) -> None:
    """Clean up database resources after failed creation."""
    logger.warning(f"Cleaning up failed database creation: {graph_id}")

    try:
      # Note: Graph API would need a delete database endpoint for this
      # For now, log the cleanup requirement
      logger.warning(f"Manual cleanup required for graph database: {graph_id}")

      # Close repository connection if it has close method
      if hasattr(repository, "close"):
        if callable(repository.close):
          # Check if it's async
          import inspect

          if inspect.iscoroutinefunction(repository.close):
            await repository.close()
          else:
            _ = repository.close()

    except Exception as e:
      logger.error(f"Database cleanup failed: {e}")


# Synchronous wrapper for backward compatibility
class EntityGraphServiceSync:
  """
  Synchronous wrapper for EntityGraphService.

  This provides backward compatibility with existing synchronous code
  by wrapping async operations in asyncio.run().
  """

  def __init__(
    self, session: Session | scoped_session[Session] | None = None
  ) -> None:
    self._async_service = EntityGraphService(session=session)

  def create_entity_with_new_graph(
    self,
    entity_data_dict: dict[str, Any],
    user_id: str,
    tier: str | None = None,
    cancellation_callback: Callable | None = None,
    progress_callback: Callable | None = None,
  ) -> dict[str, Any]:
    """Synchronous wrapper for async entity creation."""
    import asyncio

    return asyncio.run(
      self._async_service.create_entity_with_new_graph(
        entity_data_dict, user_id, tier, cancellation_callback, progress_callback
      )
    )
