"""
LadybugDB Database Schema Setup

This module handles one-time schema initialization for graph databases.
Schema is created only once when the database is first initialized,
eliminating redundant schema compilation during data ingestion.
"""

from typing import Dict, Any, Optional, Set, Union
from pathlib import Path

from ...logger import logger
from ...middleware.graph.engine import Engine
from ...processors.xbrl.schema_config_generator import (
  XBRLSchemaConfigGenerator,
  create_roboledger_ingestion_processor,
)


class LadybugSchemaManager:
  """Manages schema initialization and verification for graph databases."""

  def __init__(self, engine: Engine):
    """
    Initialize schema manager with a LadybugDB engine.

    Args:
        engine: Connected graph database engine
    """
    self.engine = engine

  def schema_exists(self) -> bool:
    """
    Check if schema has already been created in the database.

    Returns:
        True if schema exists, False otherwise
    """
    try:
      # Query to check if any node tables exist
      result = self.engine.execute_query("CALL show_tables() RETURN *")

      # If we have tables, schema exists
      return len(result) > 0

    except Exception as e:
      logger.warning(f"Could not check schema existence: {e}")
      # Assume schema doesn't exist if we can't check
      return False

  def get_existing_tables(self) -> Dict[str, Set[str]]:
    """
    Get existing node and relationship tables in the database.

    Returns:
        Dict with 'nodes' and 'relationships' sets
    """
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
    self, schema_config: Optional[Dict[str, Any]] = None, force: bool = False
  ) -> bool:
    """
    Initialize database schema if it doesn't exist.

    Args:
        schema_config: Schema configuration (defaults to RoboLedger)
        force: Force schema recreation even if it exists

    Returns:
        True if schema was created/updated, False if already existed
    """
    # Check if schema already exists
    if not force and self.schema_exists():
      logger.info("Schema already exists in database, skipping initialization")
      return False

    logger.info("Initializing graph database schema")

    # Create schema processor
    if schema_config:
      schema_processor = XBRLSchemaConfigGenerator(schema_config)
    else:
      schema_processor = create_roboledger_ingestion_processor()
      logger.info("Using default RoboLedger schema configuration")

    # Log what we're about to create
    logger.info(
      f"Creating schema with {len(schema_processor.ingest_config.node_tables)} nodes "
      f"and {len(schema_processor.ingest_config.relationship_tables)} relationships"
    )

    # Get existing tables to avoid recreation
    existing = (
      self.get_existing_tables()
      if not force
      else {"nodes": set(), "relationships": set()}
    )

    created_nodes = 0
    created_relationships = 0

    # Create node tables
    for node_name, table_info in schema_processor.ingest_config.node_tables.items():
      if node_name in existing["nodes"] and not force:
        logger.debug(f"Node table {node_name} already exists, skipping")
        continue

      if self._create_node_table(node_name, table_info):
        created_nodes += 1

    # Create relationship tables
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

    # NOTE: Platform metadata (GraphMetadata, User, Connection nodes) are now
    # stored exclusively in PostgreSQL, not in the LadybugDB graph database.
    # Shared repositories (SEC, industry, economic) no longer create GraphMetadata nodes.

    return True

  def _create_node_table(self, table_name: str, table_info: Any) -> bool:
    """
    Create a node table in LadybugDB.

    Args:
        table_name: Name of the node table
        table_info: Table information including columns and primary keys

    Returns:
        True if created successfully
    """
    try:
      # Build column definitions
      column_defs = []

      for col in table_info.columns:
        # Determine data type (simplified - you may need more mappings)
        data_type = "STRING"  # Default

        if col in ["created_at", "updated_at", "filing_date", "period_end_date"]:
          data_type = "TIMESTAMP"
        elif col in ["amount", "value", "shares", "shares_outstanding"]:
          data_type = "DOUBLE"
        elif col in ["fiscal_year", "fiscal_period"]:
          data_type = "INT64"
        elif col in ["processed", "failed", "is_extension"]:
          data_type = "BOOLEAN"

        # Check if it's a primary key
        if col in table_info.primary_keys:
          column_defs.append(f"{col} {data_type} PRIMARY KEY")
        else:
          column_defs.append(f"{col} {data_type}")

      # Create the CREATE NODE statement
      create_stmt = f"CREATE NODE TABLE {table_name}({', '.join(column_defs)})"

      logger.debug(f"Creating node table: {create_stmt}")
      self.engine.execute_query(create_stmt)

      return True

    except Exception as e:
      logger.error(f"Failed to create node table {table_name}: {e}")
      return False

  def _create_relationship_table(self, table_name: str, table_info: Any) -> bool:
    """
    Create a relationship table in LadybugDB.

    Args:
        table_name: Name of the relationship table
        table_info: Table information including from/to nodes and properties

    Returns:
        True if created successfully
    """
    try:
      # Build property definitions
      property_defs = []

      if table_info.properties:
        for prop in table_info.properties:
          # Simple type mapping
          data_type = "STRING"
          if prop in ["created_at", "updated_at"]:
            data_type = "TIMESTAMP"
          elif prop in ["weight", "confidence"]:
            data_type = "DOUBLE"

          property_defs.append(f"{prop} {data_type}")

      # Create the CREATE REL statement
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
  db_path: Union[str, Path],
  schema_config: Optional[Dict[str, Any]] = None,
  force: bool = False,
) -> bool:
  """
  Ensure schema exists for a graph database.

  This is a convenience function that handles the full flow:
  1. Connect to database
  2. Check if schema exists
  3. Create schema if needed
  4. Return status

  Args:
      db_path: Path to graph database
      schema_config: Optional schema configuration
      force: Force schema recreation

  Returns:
      True if schema was created, False if already existed
  """
  try:
    from ...operations.lbug.path_utils import (
      ensure_lbug_directory,
    )

    # Ensure we have the proper path
    db_path = Path(db_path)
    ensure_lbug_directory(db_path)

    # Connect to database
    logger.info(f"Connecting to database for schema setup: {db_path}")
    engine = Engine(str(db_path))

    # Initialize schema manager
    schema_manager = LadybugSchemaManager(engine)

    # Initialize schema
    return schema_manager.initialize_schema(schema_config, force)

  except Exception as e:
    logger.error(f"Failed to ensure schema: {e}")
    raise
