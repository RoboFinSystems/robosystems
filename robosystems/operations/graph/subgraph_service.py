"""Lifecycle operations for subgraphs.

A subgraph is a separate LadybugDB database living on the *parent's* instance,
so it shares the parent's hardware, memory pool, and credit pool while keeping
its data isolated. Its id is ``{parent_graph_id}_{subgraph_name}``, and how
many a parent may have is a function of its tier
(``GraphTierConfig.get_max_subgraphs``).

Creation writes in two places — the database on the instance, then the
PostgreSQL ``Graph`` metadata — so :meth:`SubgraphService.create_subgraph`
rolls the database back if the metadata write fails.
"""

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
  from ...graph_api.client.client import GraphClient
  from ...models.core import Graph, User

from ...config import env
from ...config.shared_repositories import is_shared_repository
from ...exceptions import GraphAllocationError
from ...graph_api.client.factory import get_graph_client_for_instance
from ...logger import logger
from ...middleware.graph.allocation_manager import LadybugAllocationManager
from ...middleware.graph.utils import (
  construct_subgraph_id,
  parse_subgraph_id,
  validate_parent_graph_id,
  validate_subgraph_name,
)


class SubgraphService:
  """Create, delete, and inspect subgraphs on their parent's instance."""

  def __init__(self):
    self.allocation_manager = LadybugAllocationManager(environment=env.ENVIRONMENT)

  async def create_subgraph_database(
    self,
    parent_graph_id: str,
    subgraph_name: str,
    schema_extensions: list[str] | None = None,
    include_base: bool = True,
    platform_managed: bool = False,
  ) -> dict[str, Any]:
    """Create the subgraph's database on the parent's instance.

    Creates only the database — no PostgreSQL metadata. Use
    :meth:`create_subgraph` for the full operation.

    ``subgraph_name`` must be alphanumeric, 1-20 characters. Creation is capped
    by the parent tier's subgraph limit. ``platform_managed`` is what allows a
    subgraph on a *shared repository*; the user-facing API never sets it, so
    shared-repo subgraphs stay platform-only.

    Returns ``{status: "created"|"exists", graph_id, database_name,
    parent_graph_id, instance_id, instance_ip}``. Raises
    ``GraphAllocationError`` when the parent is missing or the tier limit is
    hit, ValueError on malformed input.
    """
    if not validate_parent_graph_id(parent_graph_id):
      raise ValueError(f"Invalid parent graph ID: {parent_graph_id}")

    if is_shared_repository(parent_graph_id.lower()) and not platform_managed:
      raise ValueError(
        f"Shared repository '{parent_graph_id}' requires platform-managed subgraph creation. "
        "User-created subgraphs are only available for user-owned graphs."
      )

    if not validate_subgraph_name(subgraph_name):
      raise ValueError(
        f"Invalid subgraph name: {subgraph_name}. "
        "Must be alphanumeric and 1-20 characters."
      )

    from ...config.graph_tier import GraphTierConfig
    from ...database import get_db_session
    from ...models.core.graph import Graph

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

    subgraph_id = construct_subgraph_id(parent_graph_id, subgraph_name)
    database_name = subgraph_id

    logger.info(
      f"Creating subgraph database {subgraph_id} on parent {parent_graph_id}'s instance"
    )

    try:
      from robosystems.config import env
      from robosystems.database import get_db_session
      from robosystems.models.core.graph import Graph

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

      # Local dev graphs are not registered in DynamoDB, so their location is
      # synthesised rather than looked up.
      if (
        parent_graph_record.graph_instance_id
        and parent_graph_record.graph_instance_id.startswith("local-")
      ):
        from dataclasses import dataclass

        @dataclass
        class LocalGraphLocation:
          instance_id: str
          private_ip: str

        parent_location = LocalGraphLocation(
          instance_id=parent_graph_record.graph_instance_id,
          private_ip="graph-api" if env.ENVIRONMENT == "dev" else "localhost",
        )
        logger.info(
          f"Using local graph instance: {parent_location.instance_id} at {parent_location.private_ip}"
        )
      elif is_shared_repository(parent_graph_id.lower()):
        from ...graph_api.client.factory import GraphClientFactory

        master_url = await GraphClientFactory._get_shared_master_url()
        # http://10.0.1.123:8001 -> 10.0.1.123
        master_ip = master_url.replace("http://", "").split(":")[0]

        from dataclasses import dataclass

        @dataclass
        class SharedMasterLocation:
          instance_id: str
          private_ip: str

        parent_location = SharedMasterLocation(
          instance_id="shared-master",
          private_ip=master_ip,
        )
        logger.info(
          f"Using shared master for subgraph creation: {parent_location.private_ip}"
        )
      else:
        parent_location = await self.allocation_manager.find_database_location(
          parent_graph_id
        )

        if not parent_location:
          raise GraphAllocationError(
            f"Parent graph {parent_graph_id} not found. "
            "The parent graph must exist before creating subgraphs."
          )

      client = await get_graph_client_for_instance(parent_location.private_ip)

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

      logger.info(
        f"Creating database {database_name} on instance {parent_location.instance_id} "
        f"({parent_location.private_ip})"
      )

      await client.create_database(
        graph_id=database_name,
        schema_type="custom",  # Use custom to skip auto schema installation
        custom_schema_ddl=None,  # We'll install schema with extensions separately
        is_subgraph=True,  # Bypass max_databases check for Large/XLarge
      )

      # An empty subgraph (no base, no extensions) generates no DDL — skip the
      # install and leave a bare database for the caller to define.
      if not include_base and not schema_extensions:
        logger.info("Empty subgraph — no schema installed (bare database)")
      else:
        logger.info(
          f"Installing schema with extensions: {schema_extensions} (include_base={include_base})"
        )
        ddl = await self._generate_schema_ddl(
          schema_extensions or [], include_base=include_base
        )
        if ddl.strip():
          result = await client.install_schema(graph_id=database_name, custom_ddl=ddl)
          logger.info(f"Schema installation completed: {result}")
          logger.info(
            f"Installed schema with {len(schema_extensions or [])} extensions"
          )
        else:
          logger.info("Generated schema was empty — no DDL to install")

      logger.info(f"Successfully created subgraph database {subgraph_id}")

      return {
        "status": "created",
        "graph_id": subgraph_id,
        "database_name": database_name,
        "parent_graph_id": parent_graph_id,
        "instance_id": parent_location.instance_id,
        "instance_ip": parent_location.private_ip,
        "created_at": datetime.now(UTC).isoformat(),
      }

    except Exception as e:
      logger.error(f"Failed to create subgraph database {subgraph_id}: {e}")
      raise GraphAllocationError(f"Failed to create subgraph: {e!s}")

  async def create_subgraph(
    self,
    parent_graph: "Graph",
    user: "User",
    name: str,
    description: str | None = None,
    subgraph_type: str = "static",
    metadata: dict | None = None,
    fork_parent: bool = False,
    fork_options: dict[str, Any] | None = None,
    platform_managed: bool = False,
  ) -> dict[str, Any]:
    """Create a subgraph end to end: database, schema, and metadata.

    Access needs no row of its own — a subgraph is reachable by whoever holds
    a grant on the parent, at the parent's role.

    ``subgraph_type`` selects the schema: "knowledge" installs the knowledge
    extension without the base schema, "empty" installs nothing at all, and
    anything else inherits the parent's extensions on top of the base schema.

    If the PostgreSQL metadata write fails, the database created on the
    instance is deleted before re-raising — otherwise the instance accumulates
    databases no registry knows about. A failed ``fork_parent`` does *not*
    unwind the subgraph; it is reported in the returned ``fork_status``.
    """
    from ...database import get_db_session
    from ...models.core.graph import Graph

    subgraph_id = construct_subgraph_id(parent_graph.graph_id, name)

    logger.info(
      f"Creating LadybugDB database for subgraph {subgraph_id} on parent {parent_graph.graph_id}'s instance"
    )

    try:
      include_base = True
      if subgraph_type == "knowledge":
        extensions = ["knowledge"]
        include_base = False
        logger.info(
          "Using knowledge-only schema (no base schema) for knowledge subgraph"
        )
      elif subgraph_type == "empty":
        extensions = []
        include_base = False
        logger.info("Creating empty subgraph (bare database, no schema)")
      else:
        extensions = list(parent_graph.schema_extensions or [])

      db_creation_result = await self.create_subgraph_database(
        parent_graph_id=parent_graph.graph_id,
        subgraph_name=name,
        schema_extensions=extensions,
        include_base=include_base,
        platform_managed=platform_managed,
      )
      logger.info(f"LadybugDB database created: {db_creation_result}")
    except Exception as e:
      logger.error(f"Failed to create LadybugDB database for subgraph: {e}")
      raise

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

      now = datetime.now(UTC)

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

      # No GraphUser row for the subgraph: access to a subgraph is the
      # parent's grant (GraphUser.get_effective_role resolves subgraphs to
      # the parent), so a subgraph-scoped row would never be read for
      # authorization and would outlive the parent grant when a member is
      # removed.

      db.commit()
      db.refresh(subgraph)

      logger.info(
        f"Created subgraph {subgraph_id} (index {next_index}) for parent "
        f"{parent_graph.graph_id} by user {user.id}"
      )

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
          fork_status = {"status": "failed", "error": str(fork_error)}

      return {
        "graph_id": subgraph.graph_id,
        "subgraph_index": subgraph.subgraph_index,
        "graph_type": subgraph.graph_type,
        "status": "active",
        "created_at": subgraph.created_at.isoformat() if subgraph.created_at else None,
        "updated_at": subgraph.updated_at.isoformat() if subgraph.updated_at else None,
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
  ) -> dict[str, Any]:
    """Delete a subgraph's database. Destructive and not recoverable here.

    Refuses a database that holds any nodes unless ``force`` is set.
    ``create_backup`` is accepted but not yet implemented — take a backup
    through the backup operations before forcing.

    Returns ``{status: "deleted"|"not_found", ...}``.
    """
    subgraph_info = parse_subgraph_id(subgraph_id)
    if not subgraph_info:
      raise ValueError(f"Invalid subgraph ID: {subgraph_id}")

    parent_graph_id = subgraph_info.parent_graph_id
    database_name = subgraph_info.database_name

    logger.info(f"Deleting subgraph database {subgraph_id}")

    try:
      from ...config import env
      from ...graph_api.client import GraphClient
      from ...graph_api.client.factory import get_graph_client_for_instance

      # Locally there is one Graph API; in production the parent's instance
      # has to be resolved from DynamoDB first.
      is_local = env.GRAPH_API_URL and any(
        host in env.GRAPH_API_URL for host in ["localhost", "graph-api", "127.0.0.1"]
      )

      parent_location = None
      if is_local:
        logger.info(f"Using local Graph API URL for deletion: {env.GRAPH_API_URL}")
        client = GraphClient(base_url=env.GRAPH_API_URL)
      else:
        parent_location = await self.allocation_manager.find_database_location(
          parent_graph_id
        )

        if not parent_location:
          raise GraphAllocationError(
            f"Parent graph {parent_graph_id} not found. Cannot delete subgraph."
          )

        client = await get_graph_client_for_instance(parent_location.private_ip)

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

      if is_local:
        instance_id = "local-lbug-writer"
      else:
        instance_id = parent_location.instance_id if parent_location else "unknown"

      if not force:
        has_data = await self._check_database_has_data(client, database_name)
        if has_data:
          raise GraphAllocationError(
            f"Subgraph {subgraph_id} contains data. "
            "Use force=True to delete anyway, or create a backup first."
          )

      logger.info(f"Deleting database {database_name} from instance {instance_id}")
      await client.delete_database(database_name)

      logger.info(f"Successfully deleted subgraph database {subgraph_id}")

      return {
        "status": "deleted",
        "graph_id": subgraph_id,
        "database_name": database_name,
        "parent_graph_id": parent_graph_id,
        "instance_id": instance_id,
        "deleted_at": datetime.now(UTC).isoformat(),
      }

    except Exception as e:
      logger.error(f"Failed to delete subgraph database {subgraph_id}: {e}")
      raise GraphAllocationError(f"Failed to delete subgraph: {e!s}")

  async def list_subgraph_databases(
    self,
    parent_graph_id: str,
  ) -> list[dict[str, Any]]:
    """List the parent's subgraph databases as seen on its instance.

    Reads the instance rather than PostgreSQL, so it also surfaces databases
    the registry has lost track of. Returns an empty list on any failure.
    """
    logger.info(f"Listing subgraph databases for parent {parent_graph_id}")

    try:
      parent_location = await self.allocation_manager.find_database_location(
        parent_graph_id
      )

      if not parent_location:
        logger.warning(f"Parent graph {parent_graph_id} not found")
        return []

      client = await get_graph_client_for_instance(parent_location.private_ip)

      all_databases_response = await client.list_databases()
      all_database_ids = [
        db["graph_id"] for db in all_databases_response.get("databases", [])
      ]

      subgraphs = []
      parent_prefix = f"{parent_graph_id}_"

      for db_name in all_database_ids:
        if db_name.startswith(parent_prefix):
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
  ) -> dict[str, Any] | None:
    """Location and statistics for one subgraph, or None if it isn't there."""
    subgraph_info = parse_subgraph_id(subgraph_id)
    if not subgraph_info:
      logger.warning(f"Invalid subgraph ID: {subgraph_id}")
      return None

    parent_graph_id = subgraph_info.parent_graph_id
    database_name = subgraph_info.database_name

    try:
      parent_location = await self.allocation_manager.find_database_location(
        parent_graph_id
      )

      if not parent_location:
        logger.warning(f"Parent graph {parent_graph_id} not found")
        return None

      client = await get_graph_client_for_instance(parent_location.private_ip)

      existing_databases_response = await client.list_databases()
      existing_database_ids = [
        db["graph_id"] for db in existing_databases_response.get("databases", [])
      ]
      if database_name not in existing_database_ids:
        logger.warning(f"Subgraph database {database_name} not found")
        return None

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

  async def _generate_schema_ddl(
    self, extensions: list[str], include_base: bool = True
  ) -> str:
    """Compile DDL for the base schema plus ``extensions``.

    Set ``include_base=False`` for extension-only subgraphs, which then carry
    no Entity/Period tables.
    """
    from ...schemas.runtime.manager import SchemaManager

    manager = SchemaManager()
    config = manager.create_schema_configuration(
      name="SubgraphSchema",
      description="Subgraph schema with extensions",
      extensions=extensions,
      include_base=include_base,
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
    extensions: list[str],
  ) -> None:
    """Install the entity base schema plus ``extensions`` in one call."""
    try:
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
    """Install the entity base schema — the only base a subgraph uses."""
    try:
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
    """True when the database holds at least one node.

    Returns False when the check itself fails, so a caller relying on this as a
    delete guard should pair it with ``force`` semantics.
    """
    try:
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
      return False

  async def _get_database_stats(
    self,
    client: "GraphClient",
    database_name: str,
  ) -> dict[str, Any]:
    """Node/edge counts and size. Fields are None when unavailable."""
    try:
      node_result = await client.execute(
        graph_id=database_name, query="MATCH (n) RETURN count(n) as count"
      )
      node_count = node_result[0]["count"] if node_result else 0

      edge_result = await client.execute(
        graph_id=database_name, query="MATCH ()-[r]->() RETURN count(r) as count"
      )
      edge_count = edge_result[0]["count"] if edge_result else 0

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
    options: dict[str, Any] | None = None,
    progress_callback: Any | None = None,
  ) -> dict[str, Any]:
    """Copy the parent's data into the subgraph via the Graph API fork endpoint.

    The copy runs entirely on the instance where both databases live: the
    endpoint attaches the parent's DuckDB staging database and writes straight
    into the subgraph. Nothing streams through this process.

    ``options`` accepts ``tables`` (default: all). Returns
    ``{status, tables_copied, row_count, errors, ...}`` — failures come back as
    ``status: "failed"`` rather than raising.
    """
    options = options or {}
    tables_to_copy = options.get("tables", [])

    logger.info(
      f"Forking data from {parent_graph_id} to {subgraph_id} with options: {options}"
    )

    try:
      if progress_callback:
        progress_callback("Initiating fork from parent DuckDB", 10)

      from ...config import env
      from ...graph_api.client import GraphClient
      from ...graph_api.client.factory import get_graph_client_for_instance

      if progress_callback:
        progress_callback("Connecting to Graph API", 20)

      # Locally there is one Graph API; in production the parent's instance
      # has to be resolved from DynamoDB first.
      is_local = env.GRAPH_API_URL and any(
        host in env.GRAPH_API_URL for host in ["localhost", "graph-api", "127.0.0.1"]
      )

      if is_local:
        logger.info(f"Using local Graph API URL: {env.GRAPH_API_URL}")
        client = GraphClient(base_url=env.GRAPH_API_URL)
      else:
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
