"""
Shared Repository Service for creating and managing shared graph repositories.

This service handles the creation of shared repositories (SEC, industry, economic, etc.)
that are accessible across multiple companies. These repositories contain public data
and are created on dedicated shared instances.

Follows the same pattern as EntityGraphService and GenericGraphService:
1. Create LadybugDB database via Graph API client
2. Install schema with extensions
3. Create Graph metadata record in PostgreSQL
4. Persist schema DDL (GraphSchema)
5. Auto-create DuckDB staging tables (TableService)
6. No GraphUser (repositories use UserRepository for access control)
7. No credit pool (repositories use UserRepositoryCredits per user)

Key differences from user graphs:
- Fixed instance routing (ladybug-shared-prod) instead of allocation
- System-managed, not user-created
- Multiple users subscribe individually
"""

from datetime import UTC, datetime
from typing import Any

from ...config.graph_tier import GraphTier
from ...logger import logger


class SharedRepositoryService:
  """Service for creating and managing shared graph repositories."""

  REPOSITORY_CONFIG = {
    "sec": {
      "name": "SEC EDGAR Filings",
      "schema_type": "shared",
      "extensions": ["roboledger"],
      "data_source_type": "sec_edgar",
      "data_source_url": "https://www.sec.gov/cgi-bin/browse-edgar",
      "sync_frequency": "daily",
    },
    "industry": {
      "name": "Industry Classifications",
      "schema_type": "shared",
      "extensions": [],
      "data_source_type": "industry_data",
      "data_source_url": None,
      "sync_frequency": "weekly",
    },
    "economic": {
      "name": "Economic Indicators",
      "schema_type": "shared",
      "extensions": [],
      "data_source_type": "economic_data",
      "data_source_url": None,
      "sync_frequency": "daily",
    },
  }

  async def create_shared_repository(
    self,
    repository_name: str,
    created_by: str | None = None,
    instance_id: str = "ladybug-shared-prod",
  ) -> dict[str, Any]:
    """
    Create a shared repository following the same pattern as user graphs.

    This method:
    1. Validates repository configuration
    2. Creates LadybugDB database via Graph API
    3. Installs schema with extensions
    4. Creates Graph metadata record in PostgreSQL
    5. Persists schema DDL (GraphSchema)
    6. Auto-creates DuckDB staging tables (TableService)

    Args:
        repository_name: Name of the repository (e.g., 'sec', 'industry')
        created_by: Optional user ID who initiated creation
        instance_id: Instance identifier (default: ladybug-shared-prod)

    Returns:
        Dictionary containing repository creation details
    """
    logger.info(f"Creating shared repository: {repository_name}")

    if repository_name not in self.REPOSITORY_CONFIG:
      raise ValueError(
        f"Invalid repository name: {repository_name}. "
        f"Must be one of: {', '.join(self.REPOSITORY_CONFIG.keys())}"
      )

    config = self.REPOSITORY_CONFIG[repository_name]
    graph_id = repository_name

    graph_client = None

    try:
      from ...graph_api.client.factory import GraphClientFactory

      logger.info(f"Connecting to shared instance for repository {graph_id}")
      client = await GraphClientFactory.create_client(
        graph_id=graph_id, operation_type="write"
      )
      graph_client = client

      try:
        await graph_client.get_database(graph_id)
        logger.info(f"Database {graph_id} already exists")
      except Exception as e:
        if getattr(e, "status_code", None) == 404:
          logger.info(f"Creating database {graph_id}")
          await graph_client.create_database(
            graph_id=graph_id,
            schema_type=config["schema_type"],
            repository_name=repository_name,
          )
          logger.info(f"Created database {graph_id}")
        else:
          raise

      schema_ddl = None
      if config["extensions"]:
        logger.info(f"Installing schema with extensions: {config['extensions']}")
        from ...schemas.manager import SchemaManager

        manager = SchemaManager()
        schema_config = manager.create_schema_configuration(
          name=f"{repository_name.upper()} Repository Schema",
          description=f"Schema for {config['name']}",
          extensions=config["extensions"],
        )
        schema = manager.load_and_compile_schema(schema_config)
        schema_ddl = schema.to_cypher()

        result = await graph_client.install_schema(
          graph_id=graph_id, custom_ddl=schema_ddl
        )
        logger.info(f"Schema installed: {result}")

      from ...database import get_db_session
      from ...models.iam.graph import Graph

      db_gen = get_db_session()
      db = next(db_gen)

      try:
        repository_graph = Graph.find_or_create_repository(
          graph_id=graph_id,
          graph_name=config["name"],
          repository_type=repository_name,
          session=db,
          base_schema=repository_name,
          data_source_type=config["data_source_type"],
          data_source_url=config["data_source_url"],
          sync_frequency=config["sync_frequency"],
          graph_tier=GraphTier.LADYBUG_SHARED,
          graph_instance_id=instance_id,
        )
        logger.info(f"Graph metadata created/verified: {repository_graph.graph_id}")

        if schema_ddl:
          from ...models.iam import GraphSchema

          existing_schema = GraphSchema.get_active_schema(graph_id, db)
          if not existing_schema:
            GraphSchema.create(
              graph_id=graph_id,
              schema_type=config["schema_type"],
              schema_ddl=schema_ddl,
              schema_json={
                "base": repository_name,
                "extensions": config["extensions"],
              },
              session=db,
              commit=False,
            )
            logger.info(f"Persisted schema DDL for {graph_id}")

            from ..graph.table_service import TableService

            table_service = TableService(db)
            created_tables = table_service.create_tables_from_schema(
              graph_id=graph_id,
              user_id="system",
              schema_ddl=schema_ddl,
            )
            logger.info(
              f"Auto-created {len(created_tables)} DuckDB staging tables for {graph_id}"
            )

        db.commit()

      finally:
        try:
          next(db_gen)
        except StopIteration:
          pass

      db_info = await graph_client.get_database_info(graph_id)

      return {
        "repository_name": repository_name,
        "graph_id": graph_id,
        "instance_id": instance_id,
        "status": "created",
        "created_at": datetime.now(UTC).isoformat(),
        "created_by": created_by or "system",
        "database_info": db_info,
        "config": config,
      }

    except ValueError as e:
      logger.error(f"Invalid repository configuration: {e}")
      raise
    except ConnectionError as e:
      logger.error(f"Failed to connect to graph instance: {e}")
      raise
    except TimeoutError as e:
      logger.error(f"Repository creation timed out: {e}")
      raise
    except Exception as e:
      logger.error(
        f"Unexpected error creating shared repository {repository_name}: {e}",
        exc_info=True,
      )
      raise

    finally:
      if graph_client:
        try:
          await graph_client.close()
        except Exception as e:
          logger.warning(f"Error closing lbug client: {e}")


async def ensure_shared_repository_exists(
  repository_name: str,
  created_by: str | None = None,
  instance_id: str = "ladybug-shared-prod",
) -> dict[str, Any]:
  """
  Ensure a shared repository exists, creating it if necessary.

  This is a convenience function that checks if a repository exists
  and creates it if not.

  Args:
      repository_name: Name of the repository (e.g., 'sec')
      created_by: Optional user ID who initiated creation
      instance_id: Instance identifier (default: ladybug-shared-prod)

  Returns:
      Dictionary with repository status
  """
  # Check if PostgreSQL records exist (Graph and GraphSchema)
  postgres_exists = False
  try:
    from ...database import get_db_session
    from ...models.iam import Graph, GraphSchema

    db_gen = get_db_session()
    db = next(db_gen)
    try:
      graph = db.query(Graph).filter(Graph.graph_id == repository_name).first()
      schema = GraphSchema.get_active_schema(repository_name, db) if graph else None
      postgres_exists = graph is not None and schema is not None
      if graph and not schema:
        logger.info(
          f"Repository {repository_name} has Graph record but missing GraphSchema"
        )
    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass
  except Exception as e:
    logger.warning(f"Could not check PostgreSQL records for {repository_name}: {e}")

  # Check if LadybugDB database exists
  ladybug_exists = False
  try:
    from ...graph_api.client.factory import GraphClientFactory

    client = await GraphClientFactory.create_client(
      graph_id=repository_name, operation_type="read"
    )

    try:
      db_info = await client.get_database_info(repository_name)
      ladybug_exists = db_info.get("is_healthy", False)
    finally:
      await client.close()
  except (ConnectionError, TimeoutError) as e:
    logger.warning(f"Could not connect to check repository {repository_name}: {e}")
  except Exception as e:
    logger.info(f"Repository {repository_name} not found in LadybugDB: {e}")

  # If both exist, we're done
  if postgres_exists and ladybug_exists:
    logger.info(f"Repository {repository_name} fully exists (LadybugDB + PostgreSQL)")
    return {
      "status": "exists",
      "repository_name": repository_name,
      "graph_id": repository_name,
    }

  # Otherwise, create/ensure everything exists
  logger.info(
    f"Repository {repository_name} needs setup "
    f"(postgres={postgres_exists}, ladybug={ladybug_exists})"
  )
  service = SharedRepositoryService()
  return await service.create_shared_repository(
    repository_name, created_by=created_by, instance_id=instance_id
  )
