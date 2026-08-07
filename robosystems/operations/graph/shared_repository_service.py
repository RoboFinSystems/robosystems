"""Creation of shared, platform-managed graph repositories (SEC, industry, …).

Shared repositories hold public data and are read by many users at once. They
follow the same build sequence as a user graph
(:mod:`robosystems.operations.graph.graph_creation_service`) but differ in
ownership:

- routed to a fixed shared instance rather than allocated one;
- created by the platform, never by a user;
- access is per-user ``UserRepository`` rows, not ``GraphUser``;
- credits come from each subscriber's ``UserRepositoryCredits``, so the
  repository itself has no credit pool.

Every entry point here is idempotent — pipelines call them on each run to
guarantee the repository exists before loading.
"""

from datetime import UTC, datetime
from typing import Any

from ...config.graph_tier import GraphTier
from ...logger import logger


class SharedRepositoryService:
  """Service for creating and managing shared graph repositories."""

  async def create_shared_repository(
    self,
    repository_name: str,
    created_by: str | None = None,
    instance_id: str | None = None,
  ) -> dict[str, Any]:
    """Create the database, schema, metadata, and staging tables in that order.

    ``repository_name`` must be registered in the manifest registry;
    ``instance_id`` defaults to the manifest's instance. Raises ValueError for
    an unregistered name.
    """
    from ...config.shared_repositories import get_all_repository_ids, get_manifest

    logger.info(f"Creating shared repository: {repository_name}")

    manifest = get_manifest(repository_name)
    if not manifest:
      raise ValueError(
        f"Invalid repository name: {repository_name}. "
        f"Must be one of: {', '.join(get_all_repository_ids())}"
      )

    if instance_id is None:
      instance_id = manifest.graph_instance_id

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
            schema_type=manifest.schema_type,
            repository_name=repository_name,
          )
          logger.info(f"Created database {graph_id}")
        else:
          raise

      schema_ddl = None
      extensions = list(manifest.schema_extensions)
      if extensions:
        logger.info(f"Installing schema with extensions: {extensions}")
        # Contextual compile (same path as the rebuild-on-materialize flow) —
        # for SEC this is the reporting-only view, not the full extension.
        from ...schemas.loader import compile_repository_schema

        schema = compile_repository_schema(repository_name)
        schema_ddl = schema.to_cypher()

        result = await graph_client.install_schema(
          graph_id=graph_id, custom_ddl=schema_ddl
        )
        logger.info(f"Schema installed: {result}")

      from ...database import get_db_session
      from ...models.core.graph import Graph

      db_gen = get_db_session()
      db = next(db_gen)

      try:
        repository_graph = Graph.find_or_create_repository(
          graph_id=graph_id,
          graph_name=manifest.name,
          repository_type=repository_name,
          session=db,
          base_schema="base",
          schema_extensions=list(manifest.schema_extensions),
          data_source_type=manifest.data_source_type,
          data_source_url=manifest.data_source_url,
          sync_frequency=manifest.sync_frequency,
          graph_tier=GraphTier.LADYBUG_SHARED,
          graph_instance_id=instance_id,
        )
        logger.info(f"Graph metadata created/verified: {repository_graph.graph_id}")

        if schema_ddl:
          from ...models.core import GraphSchema

          existing_schema = GraphSchema.get_active_schema(graph_id, db)
          if not existing_schema:
            GraphSchema.create(
              graph_id=graph_id,
              schema_type=manifest.schema_type,
              schema_ddl=schema_ddl,
              schema_json={
                "base": "base",
                "extensions": extensions,
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
        "manifest_id": manifest.id,
      }

    except Exception as e:
      context = {
        ValueError: "invalid configuration",
        ConnectionError: "connection failed",
        TimeoutError: "timed out",
      }.get(type(e), "unexpected error")
      logger.error(
        f"Failed creating shared repository {repository_name} ({context}): {e}",
        exc_info=True,
      )
      raise

    finally:
      if graph_client:
        try:
          await graph_client.close()
        except Exception as e:
          logger.warning(f"Error closing lbug client: {e}")


async def ensure_shared_subgraph_exists(
  parent_repository_name: str,
  subgraph_name: str,
  description: str | None = None,
  created_by: str | None = None,
  instance_id: str = "ladybug-shared-prod",
) -> dict[str, Any]:
  """Idempotently create a platform-managed subgraph of a shared repository.

  Data pipelines call this before loading. Ensures the parent repository
  exists, then reconciles both halves of the subgraph — the PostgreSQL record
  and the database on the instance — creating whichever is missing. The
  PostgreSQL record is what makes the subgraph visible in MCP workspace
  listings, so a database without it is invisible to users.
  """
  from ...config.shared_repositories import get_manifest, is_shared_repository
  from ...middleware.graph.utils import construct_subgraph_id

  if not is_shared_repository(parent_repository_name):
    raise ValueError(
      f"'{parent_repository_name}' is not a registered shared repository."
    )

  subgraph_id = construct_subgraph_id(parent_repository_name, subgraph_name)

  # Step 1: Ensure parent shared repository exists
  parent_result = await ensure_shared_repository_exists(
    repository_name=parent_repository_name,
    created_by=created_by,
    instance_id=instance_id,
  )
  logger.info(
    f"Parent repository '{parent_repository_name}' status: {parent_result.get('status')}"
  )

  # Step 2: Check if PostgreSQL record exists
  postgres_exists = False
  try:
    from ...database import get_db_session
    from ...models.core.graph import Graph

    db_gen = get_db_session()
    db = next(db_gen)
    try:
      graph = db.query(Graph).filter(Graph.graph_id == subgraph_id).first()
      postgres_exists = graph is not None
    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass
  except Exception as e:
    logger.warning(f"Could not check PostgreSQL for subgraph {subgraph_id}: {e}")

  # Step 3: Check if LadybugDB database exists
  ladybug_exists = False
  try:
    from ...graph_api.client.factory import GraphClientFactory

    client = await GraphClientFactory.create_client(
      graph_id=subgraph_id, operation_type="read"
    )
    try:
      db_info = await client.get_database_info(subgraph_id)
      ladybug_exists = db_info.get("is_healthy", False)
    finally:
      await client.close()
  except (ConnectionError, TimeoutError) as e:
    logger.warning(f"Could not connect to check subgraph {subgraph_id}: {e}")
  except Exception as e:
    logger.info(f"Subgraph {subgraph_id} not found in LadybugDB: {e}")

  # Ensure schema record exists (may be missing if subgraph was created
  # before schema propagation was added, or if a previous run failed)
  if postgres_exists:
    try:
      from ...database import get_db_session
      from ...models.core import GraphSchema

      db_gen = get_db_session()
      db = next(db_gen)
      try:
        if not GraphSchema.get_active_schema(subgraph_id, db):
          parent_schema = GraphSchema.get_active_schema(parent_repository_name, db)
          if parent_schema:
            GraphSchema.create(
              graph_id=subgraph_id,
              schema_type=parent_schema.schema_type,
              schema_ddl=parent_schema.schema_ddl,
              schema_json=parent_schema.schema_json,
              session=db,
              commit=False,
            )
            db.commit()
            logger.info(f"Backfilled schema DDL for existing subgraph {subgraph_id}")
      finally:
        try:
          next(db_gen)
        except StopIteration:
          pass
    except Exception as e:
      logger.warning(f"Could not check/create schema for {subgraph_id}: {e}")

  # If both exist, we're done
  if postgres_exists and ladybug_exists:
    logger.info(f"Subgraph {subgraph_id} fully exists (LadybugDB + PostgreSQL)")
    return {
      "status": "exists",
      "graph_id": subgraph_id,
      "parent_graph_id": parent_repository_name,
      "subgraph_name": subgraph_name,
    }

  logger.info(
    f"Subgraph {subgraph_id} needs setup "
    f"(postgres={postgres_exists}, ladybug={ladybug_exists})"
  )

  manifest = get_manifest(parent_repository_name)

  # Step 4a: Create LadybugDB database if needed
  if not ladybug_exists:
    from .subgraph_service import SubgraphService

    service = SubgraphService()
    db_result = await service.create_subgraph_database(
      parent_graph_id=parent_repository_name,
      subgraph_name=subgraph_name,
      schema_extensions=list(manifest.schema_extensions) if manifest else [],
      platform_managed=True,
    )
    logger.info(
      f"LadybugDB database created for {subgraph_id}: {db_result.get('status')}"
    )

  # Step 4b: Create PostgreSQL record if needed
  if not postgres_exists:
    from datetime import UTC, datetime

    from ...database import get_db_session
    from ...models.core.graph import Graph

    db_gen = get_db_session()
    db = next(db_gen)
    try:
      # Get parent graph record for inheriting properties
      parent_graph = (
        db.query(Graph).filter(Graph.graph_id == parent_repository_name).first()
      )
      if not parent_graph:
        raise ValueError(
          f"Parent graph '{parent_repository_name}' not found in PostgreSQL. "
          "Run ensure_shared_repository_exists() first."
        )

      # Determine next subgraph index
      existing_subgraphs = (
        db.query(Graph)
        .filter(Graph.parent_graph_id == parent_repository_name)
        .order_by(Graph.subgraph_index.desc())
        .all()
      )
      next_index = (
        (existing_subgraphs[0].subgraph_index + 1) if existing_subgraphs else 1
      )

      now = datetime.now(UTC)
      subgraph = Graph(
        graph_id=subgraph_id,
        org_id=None,  # Shared repo subgraphs are system-wide
        graph_name=description or f"{parent_graph.graph_name} ({subgraph_name})",
        graph_type=parent_graph.graph_type,
        base_schema=parent_graph.base_schema,
        schema_extensions=parent_graph.schema_extensions or [],
        graph_instance_id=parent_graph.graph_instance_id,
        graph_cluster_region=parent_graph.graph_cluster_region,
        graph_tier=parent_graph.graph_tier,
        parent_graph_id=parent_repository_name,
        subgraph_index=next_index,
        subgraph_name=subgraph_name,
        is_subgraph=True,
        subgraph_metadata={
          "subgraph_type": "static",
          "platform_managed": True,
        },
        is_repository=False,
        created_at=now,
        updated_at=now,
      )
      db.add(subgraph)
      db.flush()

      # Create GraphSchema record for subgraph (inheriting from parent)
      from ...models.core import GraphSchema

      existing_schema = GraphSchema.get_active_schema(subgraph_id, db)
      if not existing_schema:
        parent_schema = GraphSchema.get_active_schema(parent_repository_name, db)
        if parent_schema:
          GraphSchema.create(
            graph_id=subgraph_id,
            schema_type=parent_schema.schema_type,
            schema_ddl=parent_schema.schema_ddl,
            schema_json=parent_schema.schema_json,
            session=db,
            commit=False,
          )
          logger.info(f"Persisted schema DDL for subgraph {subgraph_id}")
        else:
          logger.warning(
            f"No parent schema found for {parent_repository_name}, "
            f"subgraph {subgraph_id} will not have a schema record"
          )

      db.commit()

      logger.info(
        f"Created PostgreSQL record for subgraph {subgraph_id} "
        f"(index {next_index}, parent {parent_repository_name})"
      )
    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass

  return {
    "status": "created",
    "graph_id": subgraph_id,
    "parent_graph_id": parent_repository_name,
    "subgraph_name": subgraph_name,
    "created_by": created_by or "system",
  }


async def ensure_shared_repository_exists(
  repository_name: str,
  created_by: str | None = None,
  instance_id: str = "ladybug-shared-prod",
) -> dict[str, Any]:
  """Idempotently create a shared repository if it is not already there.

  Checks the PostgreSQL records and the database independently, so a
  half-created repository is completed rather than reported as present.
  """
  postgres_exists = False
  try:
    from ...database import get_db_session
    from ...models.core import Graph, GraphSchema

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
