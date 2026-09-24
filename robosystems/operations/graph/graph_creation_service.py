"""Graph creation, as a pipeline of discrete, independently testable steps.

One service covers both entity and generic graphs. The entity-specific step —
OLTP provisioning plus the entity node — runs only when
``config.create_entity`` is set.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from robosystems.config import env
from robosystems.config.graph_tier import GraphTier
from robosystems.logger import get_logger
from robosystems.middleware.graph.allocation_manager import (
  DatabaseLocation,
  LadybugAllocationManager,
)

logger = get_logger(__name__)

# Budget for the rollback alone: above ``delete_database``'s worst case with
# retries (~127s) plus deallocation and PostgreSQL cleanup. The worker's
# per-task budget must exceed the pipeline's waits plus this.
CLEANUP_TIMEOUT_SECONDS = 180

# Multiplied by the attempt number; a constant so tests can zero it.
CREDIT_POOL_RETRY_BACKOFF_SECONDS = 1.0


@dataclass
class GraphCreationConfig:
  """Every input the creation pipeline needs, in one object."""

  user_id: str
  tier: str
  graph_name: str
  graph_id: str | None = None
  graph_type: str = "entity"  # "entity" or "generic"
  schema_extensions: list[str] = field(default_factory=list)
  custom_schema: dict[str, Any] | None = None  # Only for generic
  entity_data: dict[str, Any] | None = None  # For entity with create_entity
  create_entity: bool = True
  description: str | None = None
  tags: list[str] = field(default_factory=list)
  progress: Callable[..., Any] | None = None  # SSE progress callback

  @property
  def graph_tier(self) -> GraphTier:
    return GraphTier(self.tier.lower())

  @property
  def has_custom_schema(self) -> bool:
    return self.custom_schema is not None


@dataclass
class GraphCreationResult:
  """Outcome of a successful graph creation."""

  graph_id: str
  org_id: str
  instance_id: str
  private_ip: str
  graph_type: str
  tier: str
  schema_type: str
  schema_extensions: list[str]
  entity: dict[str, Any] | None = None
  created_at: str = ""

  def __post_init__(self):
    if not self.created_at:
      self.created_at = datetime.now(UTC).isoformat()

  def to_dict(self) -> dict[str, Any]:
    return {
      "graph_id": self.graph_id,
      "status": "created",
      "cluster_info": {
        "instance_id": self.instance_id,
        "private_ip": self.private_ip,
        "api_endpoint": f"http://{self.private_ip}:8001",
      },
      "metadata": {
        "graph_id": self.graph_id,
        "tier": self.tier,
        "schema_type": self.schema_type,
        "schema_extensions": self.schema_extensions,
      },
      "tier": self.tier,
      "entity": self.entity,
      "created_at": self.created_at,
    }


class GraphCreationService:
  """Graph creation pipeline for all graph types; a failure rolls back what was built."""

  async def create(self, config: GraphCreationConfig) -> GraphCreationResult:
    logger.info(f"Starting graph creation for user {config.user_id}")

    org_id = self._validate_org(config)
    graph_id = config.graph_id or self._generate_graph_id()

    self._emit(config, "Allocating database cluster...", 20)
    location = await self._allocate(graph_id, config)

    graph_client = None
    try:
      self._emit(config, "Creating graph database...", 40)
      graph_client, custom_ddl = await self._create_database(graph_id, location, config)

      self._emit(config, "Installing schema...", 55)
      schema_ddl, schema_info = await self._install_schema(
        graph_client, graph_id, config, custom_ddl=custom_ddl
      )

      self._emit(config, "Setting up user access...", 75)
      self._persist_metadata(
        graph_id, org_id, location, config, schema_ddl, schema_info
      )

      # Provisioned for any graph whose extensions need one, entity or not: an
      # extensions-flagged graph without a schema has nowhere for writes to land.
      from robosystems.db.extensions import (
        needs_tenant_schema,
        provision_tenant_schema,
      )

      entity_dict = None
      if not config.has_custom_schema and needs_tenant_schema(config.schema_extensions):
        self._emit(config, "Provisioning tenant schema...", 82)
        provision_tenant_schema(graph_id)
        if (
          config.graph_type == "entity" and config.create_entity and config.entity_data
        ):
          self._emit(config, "Provisioning entity...", 85)
          entity_dict = await self._provision_entity(graph_id, config)

      self._emit(config, "Creating credit pool...", 92)
      await self._create_credits(graph_id, config)

      result = GraphCreationResult(
        graph_id=graph_id,
        org_id=org_id,
        instance_id=location.instance_id,
        private_ip=location.private_ip,
        graph_type=config.graph_type,
        tier=config.tier,
        schema_type="custom" if config.has_custom_schema else "extensions",
        schema_extensions=config.schema_extensions
        if not config.has_custom_schema
        else [],
        entity=entity_dict,
      )

      logger.info(f"Graph creation completed: {graph_id}")
      return result

    except BaseException as e:
      # BaseException: a worker timeout arrives as CancelledError, and it must
      # still roll back the allocation.
      logger.error(f"Graph creation failed: {type(e).__name__}: {e}")
      await self._cleanup_within_budget(graph_id, location, graph_client)
      # Re-raise untranslated; ``wait_for`` turns CancelledError into the
      # TimeoutError the consumer records.
      raise

    finally:
      if graph_client:
        try:
          await graph_client.close()
        except Exception:
          pass

  def _validate_org(self, config: GraphCreationConfig) -> str:
    """Return the user's org_id, raising ValueError if it may not create a graph."""
    from robosystems.database import get_db_session
    from robosystems.models.core import OrgLimits, OrgUser

    self._emit(config, "Checking organization limits...", 10)

    db_gen = get_db_session()
    db = next(db_gen)
    try:
      user_orgs = OrgUser.get_user_orgs(config.user_id, db)
      if not user_orgs:
        raise ValueError("User has no organization")

      membership = user_orgs[0]
      org_id = membership.org_id

      # Re-checked at run time: creation can be queued and retried.
      if not membership.can_create_graphs():
        raise ValueError("Only organization owners and admins can create graphs")

      org_limits = OrgLimits.get_or_create_for_org(org_id, db)
      can_create, reason = org_limits.can_create_graph(db)
      if not can_create:
        raise ValueError(reason)

      return org_id
    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass

  def _generate_graph_id(self) -> str:
    from robosystems.utils.ulid import generate_ulid_hex

    graph_id = f"kg{generate_ulid_hex(20)}"
    logger.info(f"Generated graph ID: {graph_id}")
    return graph_id

  async def _allocate(
    self,
    graph_id: str,
    config: GraphCreationConfig,
  ) -> DatabaseLocation:
    manager = LadybugAllocationManager(environment=env.ENVIRONMENT)
    location = await manager.allocate_database(
      entity_id=config.user_id,
      graph_id=graph_id,
      graph_type=config.graph_type,
      instance_tier=config.graph_tier,
    )
    if not location:
      raise RuntimeError(f"Failed to allocate database for graph {graph_id}")

    logger.info(
      f"Allocated {graph_id} on instance {location.instance_id} at {location.private_ip}"
    )
    return location

  async def _create_database(
    self,
    graph_id: str,
    location: DatabaseLocation,
    config: GraphCreationConfig,
  ) -> tuple[Any, str | None]:
    """Create the database; ``custom_ddl`` is returned for custom schemas only,
    so ``_install_schema`` need not re-parse it."""
    from robosystems.graph_api.client import get_graph_client_for_instance

    graph_client = await get_graph_client_for_instance(location.private_ip)

    schema_type = "custom" if config.has_custom_schema else "entity"
    custom_ddl = None
    if config.has_custom_schema:
      custom_ddl = self._resolve_custom_schema_ddl(config.custom_schema)

    await graph_client.create_database(
      graph_id=graph_id,
      schema_type=schema_type,
      custom_schema_ddl=custom_ddl,
    )
    logger.info(f"Database {graph_id} created on LadybugDB")
    return graph_client, custom_ddl

  async def _install_schema(
    self,
    graph_client: Any,
    graph_id: str,
    config: GraphCreationConfig,
    custom_ddl: str | None = None,
  ) -> tuple[str, dict[str, Any]]:
    """Install the extensions schema, returning (ddl, persistence_info).

    A custom schema was already applied by create_database; only its info is
    built here.
    """
    if config.has_custom_schema:
      ddl = custom_ddl or self._resolve_custom_schema_ddl(config.custom_schema)
      info = {
        "schema_type": "custom",
        "schema_ddl": ddl,
        "schema_json": config.custom_schema,
        "custom_schema_name": config.custom_schema.get("name"),
        "custom_schema_version": config.custom_schema.get("version"),
      }
      return ddl, info

    from robosystems.schemas.runtime.manager import SchemaManager

    manager = SchemaManager()
    schema_config = manager.create_schema_configuration(
      name="GraphSchema",
      description=f"Schema for {graph_id}",
      extensions=config.schema_extensions,
    )
    schema = manager.load_and_compile_schema(schema_config)
    ddl = schema.to_cypher()

    await graph_client.install_schema(graph_id=graph_id, custom_ddl=ddl)
    logger.info(f"Schema installed: base + {len(config.schema_extensions)} extensions")

    info = {
      "schema_type": "extensions",
      "schema_ddl": ddl,
      "schema_json": {"base": "base", "extensions": config.schema_extensions},
      "custom_schema_name": None,
      "custom_schema_version": None,
    }
    return ddl, info

  def _persist_metadata(
    self,
    graph_id: str,
    org_id: str,
    location: DatabaseLocation,
    config: GraphCreationConfig,
    schema_ddl: str,
    schema_info: dict[str, Any],
  ) -> None:
    """Persist Graph, GraphSchema, staging tables, and GraphUser in one transaction."""
    from robosystems.database import get_db_session
    from robosystems.models.core import GraphSchema, GraphUser
    from robosystems.models.core.graph import Graph

    from .table_service import TableService

    db_gen = get_db_session()
    db = next(db_gen)
    try:
      Graph.create(
        graph_id=graph_id,
        graph_name=config.graph_name,
        graph_type=config.graph_type,
        org_id=org_id,
        session=db,
        base_schema=None if config.has_custom_schema else "base",
        schema_extensions=config.schema_extensions
        if not config.has_custom_schema
        else [],
        graph_instance_id=location.instance_id,
        graph_tier=config.graph_tier,
        graph_metadata={
          "created_by": config.user_id,
          "description": config.description or "",
          "type": config.graph_type,
          "tags": config.tags,
        },
        commit=False,
      )

      user_graph = GraphUser(
        user_id=config.user_id,
        graph_id=graph_id,
        role="admin",
        is_selected=True,
      )
      db.add(user_graph)

      db.query(GraphUser).filter(
        GraphUser.user_id == config.user_id, GraphUser.graph_id != graph_id
      ).update({"is_selected": False})

      db.flush()

      GraphSchema.create(
        graph_id=graph_id,
        schema_type=schema_info["schema_type"],
        schema_ddl=schema_info["schema_ddl"],
        schema_json=schema_info["schema_json"],
        custom_schema_name=schema_info.get("custom_schema_name"),
        custom_schema_version=schema_info.get("custom_schema_version"),
        session=db,
        commit=False,
      )

      table_service = TableService(db)
      created_tables = table_service.create_tables_from_schema(
        graph_id=graph_id,
        user_id=config.user_id,
        schema_ddl=schema_ddl,
      )
      logger.info(f"Created {len(created_tables)} staging tables for {graph_id}")

      db.commit()

    except Exception:
      db.rollback()
      raise
    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass

  async def _provision_entity(
    self,
    graph_id: str,
    config: GraphCreationConfig,
  ) -> dict[str, Any]:
    """Create the entity row in the already-provisioned tenant schema."""
    from robosystems.db.extensions import extensions_session
    from robosystems.models.api import EntityCreate
    from robosystems.models.extensions.entity import Entity as LedgerEntity

    from .reporting_style_defaults import resolve_reporting_style_id

    entity_data = EntityCreate(**config.entity_data)
    current_time = datetime.now(UTC)

    # From the legal form, unless the request names one. Lives on the entity,
    # not the graph.
    reporting_style_id = resolve_reporting_style_id(config.entity_data)

    entity_identifier = f"entity_{graph_id}"
    entity_uri = entity_data.uri or f"https://robosystems.ai/entities#{graph_id}"

    ticker = getattr(entity_data, "ticker", None)
    if not ticker:
      import re

      # Initials of the name's words, max 6.
      words = re.sub(r"[^a-zA-Z0-9\s]", "", entity_data.name).split()
      if len(words) >= 2:
        ticker = "".join(w[0].upper() for w in words if w)[:6]
      else:
        ticker = entity_data.name[:4].upper().replace(" ", "")

    with extensions_session(graph_id) as session:
      entity = LedgerEntity(
        id=entity_identifier,
        name=entity_data.name,
        legal_name=entity_data.name,
        uri=entity_uri,
        ticker=ticker,
        cik=entity_data.cik,
        sic=entity_data.sic,
        sic_description=entity_data.sic_description,
        category=entity_data.category,
        state_of_incorporation=entity_data.state_of_incorporation,
        fiscal_year_end=entity_data.fiscal_year_end,
        tax_id=entity_data.ein,
        entity_type=entity_data.entity_type,
        reporting_style_id=reporting_style_id,
        website=entity_data.uri,
        status="active",
        is_parent=True,
        source="native",
        created_by=config.user_id,
        created_at=current_time,
        updated_at=current_time,
      )
      session.add(entity)
      session.commit()

    logger.info(f"Entity {entity_identifier} provisioned in OLTP for {graph_id}")

    return {
      "id": entity_identifier,
      "name": entity_data.name,
      "uri": entity_uri,
      "ticker": ticker,
      "database": graph_id,
    }

  async def _create_credits(self, graph_id: str, config: GraphCreationConfig) -> None:
    """Create the credit pool; failures are logged, not raised.

    Retried because a missing pool is permanent and silent: every AI run is
    denied, and nothing later creates it.
    """
    import asyncio

    last_error: Exception | None = None
    for attempt in range(3):
      try:
        from robosystems.database import get_db_session

        from .credit_service import CreditService

        db_gen = get_db_session()
        db = next(db_gen)
        try:
          credit_service = CreditService(db)
          credit_service.create_graph_credits(
            graph_id=graph_id,
            user_id=config.user_id,
            billing_admin_id=config.user_id,
            subscription_tier=config.tier.lower(),
            graph_tier=config.graph_tier,
          )
          logger.info(f"Credit pool created for {graph_id}")
          return
        finally:
          try:
            next(db_gen)
          except StopIteration:
            pass
      except ValueError as e:
        # A config error; retrying cannot help.
        logger.error(f"Failed to create credit pool for {graph_id}: {e}")
        return
      except Exception as e:
        last_error = e
        logger.warning(
          f"Credit pool creation attempt {attempt + 1}/3 failed for {graph_id}: {e}"
        )
        await asyncio.sleep(CREDIT_POOL_RETRY_BACKOFF_SECONDS * (attempt + 1))
    logger.error(
      f"Failed to create credit pool for {graph_id} after 3 attempts: {last_error}"
    )

  async def _cleanup_within_budget(
    self,
    graph_id: str,
    location: DatabaseLocation | None,
    graph_client=None,
  ) -> None:
    """Run the rollback under its own timeout.

    ``wait_for`` cancels only once, so after that cancellation is caught an
    unbounded rollback could hold the worker indefinitely. An overrun is logged
    and abandoned.
    """
    try:
      await asyncio.wait_for(
        self._cleanup(graph_id, location, graph_client),
        timeout=CLEANUP_TIMEOUT_SECONDS,
      )
    except (TimeoutError, asyncio.CancelledError):
      logger.error(
        f"Rollback for {graph_id} did not finish within {CLEANUP_TIMEOUT_SECONDS}s; "
        "resources may be left allocated"
      )
    except Exception as e:
      logger.error(f"Rollback for {graph_id} failed: {e}", exc_info=True)

  async def _cleanup(
    self,
    graph_id: str,
    location: DatabaseLocation | None,
    graph_client=None,
  ) -> None:
    """Undo a partially built graph, best-effort.

    After ``_persist_metadata`` commits there is a Graph row, and
    ``GraphDeprovisionService`` tears everything down. Before it, only the
    database file and the allocation exist, and are released directly.
    """
    try:
      if graph_client:
        await graph_client.close()
    except Exception as e:
      logger.error(f"Cleanup close failed for {graph_id}: {e}")

    try:
      from robosystems.db.platform import platform_session
      from robosystems.models.core.graph import Graph

      with platform_session() as db:
        row = db.query(Graph).filter(Graph.graph_id == graph_id).first()

        if row is not None:
          from .deprovision_service import GraphDeprovisionService

          service = GraphDeprovisionService(environment=env.ENVIRONMENT)
          result = await service.deprovision_graph(
            graph_id=graph_id,
            session=db,
            create_backup=False,
            skip_backup_check=True,
          )
          logger.info(
            f"Rolled back partially created graph {graph_id}: {result.status}",
            extra={"graph_id": graph_id, "errors": result.errors},
          )
          return

      if location:
        # Delete the `.lbug` before releasing the allocation: the node counts
        # on-disk databases while the allocator counts registry rows, so a
        # freed slot with a file still on it reads full to one and empty to
        # the other. Reclaim reconciliation is the backstop.
        try:
          from robosystems.graph_api.client.factory import get_graph_client

          cleanup_client = await get_graph_client(
            graph_id=graph_id, operation_type="write"
          )
          try:
            await cleanup_client.delete_database(graph_id)
            logger.info(f"Deleted orphaned database file for {graph_id} on cleanup")
          finally:
            await cleanup_client.close()
        except Exception as e:
          logger.warning(
            f"Cleanup database delete failed for {graph_id} "
            f"(reclaim reconciliation will catch it): {e}"
          )

        manager = LadybugAllocationManager(environment=env.ENVIRONMENT)
        await manager.deallocate_database(graph_id)
        logger.info(f"Deallocated {graph_id}")
    except Exception as e:
      logger.error(f"Cleanup failed for {graph_id}: {e}", exc_info=True)

  def _resolve_custom_schema_ddl(self, custom_schema: dict[str, Any]) -> str:
    from robosystems.schemas.runtime.custom import CustomSchemaManager

    manager = CustomSchemaManager()
    parsed = manager.create_from_dict(custom_schema)
    if custom_schema.get("extends") == "base":
      parsed = manager.merge_with_base(parsed)
    return parsed.to_cypher()

  def _emit(self, config: GraphCreationConfig, message: str, percent: float) -> None:
    if config.progress:
      config.progress(message, percent)
