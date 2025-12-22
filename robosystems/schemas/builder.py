"""
LadybugDB Schema Builder

Dynamically generates and manages LadybugDB schemas based on configuration.
Uses the enhanced SchemaManager for better inheritance and compatibility.
"""

import importlib
from typing import Any

from robosystems.logger import logger

from .manager import SchemaConfiguration, SchemaManager
from .models import Schema


class LadybugSchemaBuilder:
  """
  Builds LadybugDB schemas by combining base schemas and extensions.

  This builder loads schema definitions from configuration and generates
  the complete Cypher DDL script for creating the database schema.

  Now uses the enhanced SchemaManager for better inheritance support.
  """

  def __init__(self, config: dict[str, Any]):
    """
    Initialize the schema builder.

    Args:
        config: Configuration dictionary with base_schema and extensions
    """
    self.config = config
    self.schema_manager = SchemaManager()
    self.schema = None

  def load_schemas(self) -> "LadybugSchemaBuilder":
    """
    Load base schema and all configured extensions using SchemaManager.

    Returns:
        Self for method chaining
    """
    logger.info(f"Loading LadybugDB schemas from configuration: {self.config}")

    # Create schema configuration
    schema_config = SchemaConfiguration(
      name=self.config.get("name", "Generated Schema"),
      description=self.config.get(
        "description", "Dynamically generated LadybugDB schema"
      ),
      version=self.config.get("version", "1.0.0"),
      base_schema=self.config.get("base_schema", "base"),
      extensions=self.config.get("extensions", []),
    )

    # Load and compile schema
    self.schema = self.schema_manager.load_and_compile_schema(schema_config)

    logger.info(
      f"Loaded schema with {len(self.schema.nodes)} nodes and {len(self.schema.relationships)} relationships"
    )
    return self

  def _load_base_schema(self, schema_name: str):
    """Load base schema module. (Deprecated - use SchemaManager)"""
    logger.warning("_load_base_schema is deprecated - using SchemaManager instead")
    try:
      module_path = f"robosystems.schemas.{schema_name}"
      module = importlib.import_module(module_path)

      # Load nodes
      if hasattr(module, "BASE_NODES"):
        if self.schema is None:
          self.schema = Schema(
            name=self.config.get("name", "Generated Schema"),
            description=self.config.get(
              "description", "Dynamically generated LadybugDB schema"
            ),
            version=self.config.get("version", "1.0.0"),
          )
        self.schema.nodes.extend(module.BASE_NODES)
        logger.debug(f"Loaded {len(module.BASE_NODES)} base nodes from {schema_name}")

      # Load relationships
      if hasattr(module, "BASE_RELATIONSHIPS"):
        if self.schema is not None:
          self.schema.relationships.extend(module.BASE_RELATIONSHIPS)
        logger.debug(
          f"Loaded {len(module.BASE_RELATIONSHIPS)} base relationships from {schema_name}"
        )

    except ImportError as e:
      logger.error(f"Failed to load base schema '{schema_name}': {e}")
      raise ValueError(f"Base schema '{schema_name}' not found")

  def _load_extension_schema(self, extension_name: str):
    """Load extension schema module. (Deprecated - use SchemaManager)"""
    logger.warning("_load_extension_schema is deprecated - using SchemaManager instead")
    try:
      module_path = f"robosystems.schemas.extensions.{extension_name}"
      module = importlib.import_module(module_path)

      # Load extension nodes
      if hasattr(module, "EXTENSION_NODES"):
        if self.schema is None:
          self.schema = Schema(
            name=self.config.get("name", "Generated Schema"),
            description=self.config.get(
              "description", "Dynamically generated LadybugDB schema"
            ),
            version=self.config.get("version", "1.0.0"),
          )
        self.schema.nodes.extend(module.EXTENSION_NODES)
        logger.debug(
          f"Loaded {len(module.EXTENSION_NODES)} extension nodes from {extension_name}"
        )

      # Load extension relationships
      if hasattr(module, "EXTENSION_RELATIONSHIPS"):
        if self.schema is not None:
          self.schema.relationships.extend(module.EXTENSION_RELATIONSHIPS)
        logger.debug(
          f"Loaded {len(module.EXTENSION_RELATIONSHIPS)} extension relationships from {extension_name}"
        )

    except ImportError as e:
      logger.error(f"Failed to load extension schema '{extension_name}': {e}")
      raise ValueError(f"Extension schema '{extension_name}' not found")

  def generate_cypher(self) -> str:
    """
    Generate the complete Cypher DDL script.

    Returns:
        Complete Cypher script as string
    """
    if self.schema is None:
      raise ValueError("Schema not loaded. Call load_schemas() first.")
    return self.schema.to_cypher()

  def apply_to_connection(self, connection):
    """
    Apply the schema to a LadybugDB database connection.

    Args:
        connection: LadybugDB database connection
    """
    cypher_script = self.generate_cypher()
    logger.info("Applying LadybugDB schema to database")
    logger.debug(f"Schema DDL:\n{cypher_script}")

    # Split and clean statements - handle multi-line comments properly
    raw_parts = cypher_script.split(";")
    statements = []

    for part in raw_parts:
      # Remove comments and whitespace
      lines = part.split("\n")
      clean_lines = []
      in_comment = False

      for line in lines:
        line = line.strip()
        if line.startswith("/*"):
          in_comment = True
        if not in_comment and line and not line.startswith("--"):
          clean_lines.append(line)
        if line.endswith("*/"):
          in_comment = False

      if clean_lines:
        clean_statement = "\n".join(clean_lines).strip()
        if clean_statement:
          statements.append(clean_statement)

    for i, statement in enumerate(statements):
      if statement:
        try:
          logger.info(
            f"Executing DDL statement {i + 1}/{len(statements)}: {statement[:200]}..."
          )
          connection.execute(statement + ";")
          logger.info(f"Successfully executed statement {i + 1}")
        except Exception as e:
          logger.error(f"Failed to execute statement {i + 1}: {statement}")
          logger.error(f"Error: {e}")
          raise

    logger.info("Successfully applied LadybugDB schema")

  def get_schema(self) -> Schema:
    """
    Get the built schema object.

    Returns:
        Complete schema object

    Raises:
        ValueError: If schema has not been loaded
    """
    if self.schema is None:
      raise ValueError("Schema not loaded. Call load_schemas() first.")
    return self.schema


def create_schema_from_config(config: dict[str, Any]) -> Schema:
  """
  Convenience function to create a schema from configuration.

  Args:
      config: Schema configuration dictionary

  Returns:
      Complete schema object
  """
  builder = LadybugSchemaBuilder(config)
  builder.load_schemas()
  return builder.get_schema()


def apply_schema_to_database(database_path: str, config: dict[str, Any]):
  """
  Convenience function to apply schema to a LadybugDB database.

  Args:
      database_path: Path to LadybugDB database
      config: Schema configuration dictionary
  """
  import real_ladybug as lbug

  db = lbug.Database(database_path)
  conn = lbug.Connection(db)

  try:
    builder = LadybugSchemaBuilder(config)
    builder.load_schemas()
    builder.apply_to_connection(conn)
  finally:
    conn.close()
