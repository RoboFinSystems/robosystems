"""Unified graph creation pipeline.

Replaces EntityGraphService and GenericGraphService with a single service
that handles both entity and generic graph creation through a composable
pipeline of discrete steps.

Each step is an independently testable method. The entity-specific step
(OLTP provisioning + entity node) is conditional on config.create_entity.
"""

from __future__ import annotations

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


# ---------------------------------------------------------------------------
# Config / Result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class GraphCreationConfig:
  """All inputs needed to create a graph. Replaces sprawling kwargs."""

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


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class GraphCreationService:
  """Unified graph creation pipeline for all graph types.

  Pipeline steps:
    1. validate_org      — check org limits
    2. generate_graph_id — ULID-based
    3. allocate          — DynamoDB allocation via LadybugAllocationManager
    4. create_database   — Graph API create_database call
    5. install_schema    — resolve DDL + install via Graph API
    6. persist_metadata  — Graph, GraphSchema, staging tables, GraphUser (one txn)
    7. provision_entity  — [entity only] extensions OLTP tenant + LedgerEntity
    8. create_credits    — non-blocking credit pool setup
    9. cleanup           — deallocate on failure
  """

  # ---- Public entry point ------------------------------------------------

  async def create(self, config: GraphCreationConfig) -> GraphCreationResult:
    """Run the full graph creation pipeline."""
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

      entity_dict = None
      if config.graph_type == "entity" and config.create_entity and config.entity_data:
        self._emit(config, "Provisioning entity...", 85)
        entity_dict = await self._provision_entity(graph_id, config)

      self._emit(config, "Creating credit pool...", 92)
      self._create_credits(graph_id, config)

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

    except Exception as e:
      logger.error(f"Graph creation failed: {type(e).__name__}: {e}")
      await self._cleanup(graph_id, location, graph_client)
      raise

    finally:
      if graph_client:
        try:
          await graph_client.close()
        except Exception:
          pass

  # ---- Pipeline steps ----------------------------------------------------

  def _validate_org(self, config: GraphCreationConfig) -> str:
    """Validate user has an org and org can create graphs. Returns org_id."""
    from robosystems.database import get_db_session
    from robosystems.models.core import OrgLimits, OrgUser

    self._emit(config, "Checking organization limits...", 10)

    db_gen = get_db_session()
    db = next(db_gen)
    try:
      user_orgs = OrgUser.get_user_orgs(config.user_id, db)
      if not user_orgs:
        raise ValueError("User has no organization")

      org_id = user_orgs[0].org_id
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
    """Generate a ULID-based graph ID."""
    from robosystems.utils.ulid import generate_ulid_hex

    graph_id = f"kg{generate_ulid_hex(16)}"
    logger.info(f"Generated graph ID: {graph_id}")
    return graph_id

  async def _allocate(
    self,
    graph_id: str,
    config: GraphCreationConfig,
  ) -> DatabaseLocation:
    """Allocate database on a LadybugDB instance."""
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
    """Create the LadybugDB database via Graph API.

    Returns (graph_client, custom_ddl). custom_ddl is non-None only for
    custom schema graphs so _install_schema can reuse it without re-parsing.
    """
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
    """Resolve and install schema. Returns (ddl, persistence_info).

    For custom schemas, pass custom_ddl from _create_database to avoid
    re-parsing. DDL was already applied during create_database.
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

    # Extensions schema — generate DDL and install
    from robosystems.schemas.manager import SchemaManager

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
      # Create new Graph + GraphUser
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

      # Deselect other graphs for this user
      db.query(GraphUser).filter(
        GraphUser.user_id == config.user_id, GraphUser.graph_id != graph_id
      ).update({"is_selected": False})

      db.flush()

      # Schema record
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

      # Staging tables
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
    """Provision extensions OLTP tenant schema and create entity node.

    Only called for entity graphs with create_entity=True.
    """
    from robosystems.db.extensions import extensions_session, provision_tenant_schema
    from robosystems.models.api import EntityCreate
    from robosystems.models.extensions.entity import Entity as LedgerEntity

    entity_data = EntityCreate(**config.entity_data)
    current_time = datetime.now(UTC)

    provision_tenant_schema(graph_id)

    entity_identifier = f"entity_{graph_id}"
    entity_uri = entity_data.uri or f"https://robosystems.ai/entities#{graph_id}"

    with extensions_session(graph_id) as session:
      entity = LedgerEntity(
        id=entity_identifier,
        name=entity_data.name,
        legal_name=entity_data.name,
        uri=entity_uri,
        cik=entity_data.cik,
        sic=entity_data.sic,
        sic_description=entity_data.sic_description,
        category=entity_data.category,
        state_of_incorporation=entity_data.state_of_incorporation,
        fiscal_year_end=entity_data.fiscal_year_end,
        tax_id=entity_data.ein,
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
      "database": graph_id,
    }

  def _create_credits(self, graph_id: str, config: GraphCreationConfig) -> None:
    """Create credit pool. Non-blocking — failures are logged, not raised."""
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
      finally:
        try:
          next(db_gen)
        except StopIteration:
          pass
    except Exception as e:
      logger.error(f"Failed to create credit pool for {graph_id}: {e}")

  async def _cleanup(
    self,
    graph_id: str,
    location: DatabaseLocation | None,
    graph_client=None,
  ) -> None:
    """Deallocate database and close client on failure."""
    try:
      if location:
        manager = LadybugAllocationManager(environment=env.ENVIRONMENT)
        await manager.deallocate_database(graph_id)
        logger.info(f"Deallocated {graph_id}")
    except Exception as e:
      logger.error(f"Cleanup deallocate failed for {graph_id}: {e}")

    try:
      if graph_client:
        await graph_client.close()
    except Exception as e:
      logger.error(f"Cleanup close failed for {graph_id}: {e}")

  # ---- Helpers -----------------------------------------------------------

  def _resolve_custom_schema_ddl(self, custom_schema: dict[str, Any]) -> str:
    """Parse and validate a custom schema definition, returning DDL."""
    from robosystems.schemas.custom import CustomSchemaManager

    manager = CustomSchemaManager()
    parsed = manager.create_from_dict(custom_schema)
    if custom_schema.get("extends") == "base":
      parsed = manager.merge_with_base(parsed)
    return parsed.to_cypher()

  def _emit(self, config: GraphCreationConfig, message: str, percent: float) -> None:
    """Emit progress if callback is set."""
    if config.progress:
      config.progress(message, percent)
