"""One-time schema creation for a LadybugDB database.

Schema compilation is expensive, so it happens once at database creation
rather than on every ingest. Table creation is additive and idempotent: an
existing table is left alone unless ``force`` is set.

Platform metadata (graphs, users, connections) lives in PostgreSQL, not in the
graph — nothing here creates metadata nodes.
"""

from pathlib import Path
from typing import Any

from robosystems.adapters.sec.processors.schema import (
  XBRLSchemaConfigGenerator,
  create_roboledger_ingestion_processor,
)
from robosystems.graph_api.core.ladybug import Engine

from ....logger import logger


class LadybugSchemaManager:
  """Manages schema initialization and verification for graph databases."""

  def __init__(self, engine: Engine):
    self.engine = engine

  def schema_exists(self) -> bool:
    """True when the database has any table at all.

    An unreadable database counts as "no schema", so the caller retries
    creation rather than skipping it.
    """
    try:
      result = self.engine.execute_query("CALL show_tables() RETURN *")

      return len(result) > 0

    except Exception as e:
      logger.warning(f"Could not check schema existence: {e}")
      return False

  def get_existing_tables(self) -> dict[str, set[str]]:
    """Table names already present, as ``{"nodes": set, "relationships": set}``."""
    existing = {"nodes": set(), "relationships": set()}

    try:
      result = self.engine.execute_query("CALL show_tables() RETURN *")

      for row in result:
        table_name = row.get("name", "")
        table_type = row.get("type", "")

        if table_type == "NODE":
          existing["nodes"].add(table_name)
        elif table_type == "REL":
          existing["relationships"].add(table_name)

      logger.info(
        f"Found existing tables - Nodes: {len(existing['nodes'])}, "
        f"Relationships: {len(existing['relationships'])}"
      )

    except Exception as e:
      logger.error(f"Failed to get existing tables: {e}")

    return existing

  def initialize_schema(
    self, schema_config: dict[str, Any] | None = None, force: bool = False
  ) -> bool:
    """Create any missing tables. False when the schema was already there.

    ``schema_config`` defaults to the RoboLedger configuration. ``force``
    re-issues every CREATE, ignoring what already exists.
    """
    if not force and self.schema_exists():
      logger.info("Schema already exists in database, skipping initialization")
      return False

    logger.info("Initializing graph database schema")

    if schema_config:
      schema_processor = XBRLSchemaConfigGenerator(schema_config)
    else:
      schema_processor = create_roboledger_ingestion_processor()
      logger.info("Using default RoboLedger schema configuration")

    logger.info(
      f"Creating schema with {len(schema_processor.ingest_config.node_tables)} nodes "
      f"and {len(schema_processor.ingest_config.relationship_tables)} relationships"
    )

    existing = (
      self.get_existing_tables()
      if not force
      else {"nodes": set(), "relationships": set()}
    )

    created_nodes = 0
    created_relationships = 0

    for node_name, table_info in schema_processor.ingest_config.node_tables.items():
      if node_name in existing["nodes"] and not force:
        logger.debug(f"Node table {node_name} already exists, skipping")
        continue

      if self._create_node_table(node_name, table_info):
        created_nodes += 1

    for (
      rel_name,
      table_info,
    ) in schema_processor.ingest_config.relationship_tables.items():
      if rel_name in existing["relationships"] and not force:
        logger.debug(f"Relationship table {rel_name} already exists, skipping")
        continue

      if self._create_relationship_table(rel_name, table_info):
        created_relationships += 1

    logger.info(
      f"Schema initialization complete - Created {created_nodes} node tables "
      f"and {created_relationships} relationship tables"
    )

    return True

  def _create_node_table(self, table_name: str, table_info: Any) -> bool:
    """Create one node table. Column types are inferred from column *names*.

    Anything not matched by the name lists below lands as STRING.
    """
    try:
      column_defs = []

      for col in table_info.columns:
        data_type = "STRING"

        if col in ["created_at", "updated_at", "filing_date"]:
          data_type = "TIMESTAMP"
        elif col in ["amount", "value", "shares", "shares_outstanding"]:
          data_type = "DOUBLE"
        elif col in ["calendar_year", "fiscal_year_focus", "fiscal_year_end_month"]:
          data_type = "INT64"
        elif col in ["processed", "failed", "is_extension"]:
          data_type = "BOOLEAN"

        if col in table_info.primary_keys:
          column_defs.append(f"{col} {data_type} PRIMARY KEY")
        else:
          column_defs.append(f"{col} {data_type}")

      create_stmt = f"CREATE NODE TABLE {table_name}({', '.join(column_defs)})"

      logger.debug(f"Creating node table: {create_stmt}")
      self.engine.execute_query(create_stmt)

      return True

    except Exception as e:
      logger.error(f"Failed to create node table {table_name}: {e}")
      return False

  def _create_relationship_table(self, table_name: str, table_info: Any) -> bool:
    """Create one relationship table, inferring property types from names."""
    try:
      property_defs = []

      if table_info.properties:
        for prop in table_info.properties:
          data_type = "STRING"
          if prop in ["created_at", "updated_at"]:
            data_type = "TIMESTAMP"
          elif prop in ["weight", "confidence"]:
            data_type = "DOUBLE"

          property_defs.append(f"{prop} {data_type}")

      properties_str = f", {', '.join(property_defs)}" if property_defs else ""
      create_stmt = (
        f"CREATE REL TABLE {table_name}"
        f"(FROM {table_info.from_node} TO {table_info.to_node}{properties_str})"
      )

      logger.debug(f"Creating relationship table: {create_stmt}")
      self.engine.execute_query(create_stmt)

      return True

    except Exception as e:
      logger.error(f"Failed to create relationship table {table_name}: {e}")
      return False


def ensure_schema(
  db_path: str | Path,
  schema_config: dict[str, Any] | None = None,
  force: bool = False,
) -> bool:
  """Open ``db_path``, creating the directory and schema if needed.

  True when tables were created, False when the schema was already there.
  Unlike the methods it wraps, this raises on failure.
  """
  try:
    from .path_utils import (
      ensure_lbug_directory,
    )

    db_path = Path(db_path)
    ensure_lbug_directory(db_path)

    logger.info(f"Connecting to database for schema setup: {db_path}")
    engine = Engine(str(db_path))

    schema_manager = LadybugSchemaManager(engine)

    return schema_manager.initialize_schema(schema_config, force)

  except Exception as e:
    logger.error(f"Failed to ensure schema: {e}")
    raise
