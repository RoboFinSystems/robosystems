"""
Schema installer for LadybugDB databases.

This module handles installing both predefined schema extensions and
custom custom schemas into LadybugDB databases.
"""

from typing import Dict, Any, List, Optional
import real_ladybug as lbug

from robosystems.logger import logger
from .manager import SchemaManager
from .custom import CustomSchemaManager


class SchemaInstaller:
  """
  Handles schema installation for LadybugDB databases.

  Supports both predefined extensions and custom user schemas.
  """

  def __init__(self, database_path: str):
    """
    Initialize schema installer for a specific database.

    Args:
        database_path: Path to the LadybugDB database directory
    """
    self.database_path = database_path
    self.schema_manager = SchemaManager()
    self.custom_schema_manager = CustomSchemaManager()

  def install_extensions(
    self, base_schema: str = "base", extensions: Optional[List[str]] = None
  ) -> Dict[str, Any]:
    """
    Install predefined schema extensions.

    Args:
        base_schema: Base schema name (default: "base")
        extensions: List of extension names to install

    Returns:
        Installation result with statistics
    """
    logger.info(f"Installing schema extensions to {self.database_path}")

    try:
      # Create schema configuration
      config = self.schema_manager.create_schema_configuration(
        name=f"Extended Schema ({', '.join(extensions or [])})",
        description="Schema with extensions",
        extensions=extensions or [],
      )

      # Compile schema
      schema = self.schema_manager.load_and_compile_schema(config)

      # Install to database
      return self._install_schema(schema)

    except Exception as e:
      logger.error(f"Failed to install schema extensions: {e}")
      raise

  def install_custom_schema(self, schema_definition: Dict[str, Any]) -> Dict[str, Any]:
    """
    Install custom custom schema.

    Args:
        schema_definition: Custom schema definition

    Returns:
        Installation result with statistics
    """
    logger.info(f"Installing custom schema to {self.database_path}")

    try:
      # Parse custom schema
      schema = self.custom_schema_manager.create_from_dict(schema_definition)

      # Check if it extends base
      if schema_definition.get("extends") == "base":
        schema = self.custom_schema_manager.merge_with_base(schema)

      # Install to database
      return self._install_schema(schema)

    except Exception as e:
      logger.error(f"Failed to install custom schema: {e}")
      raise

  def _install_schema(self, schema) -> Dict[str, Any]:
    """
    Install a compiled schema to the database.

    Args:
        schema: Compiled Schema object

    Returns:
        Installation result
    """
    # Connect to database
    db = lbug.Database(self.database_path)
    conn = lbug.Connection(db)

    try:
      # Generate DDL
      ddl = schema.to_cypher()

      # Split into individual statements
      statements = [s.strip() for s in ddl.split(";") if s.strip()]

      executed = 0
      skipped = 0
      errors = []

      for statement in statements:
        if statement.startswith("--") or statement.startswith("/*"):
          continue

        try:
          conn.execute(statement)
          executed += 1
        except Exception as e:
          error_msg = str(e)
          if "already exists" in error_msg.lower():
            skipped += 1
            logger.debug(f"Skipped existing: {statement[:50]}...")
          else:
            errors.append(
              {
                "statement": statement[:100] + "..."
                if len(statement) > 100
                else statement,
                "error": error_msg,
              }
            )
            logger.error(f"Failed to execute: {error_msg}")

      # Get final schema info
      schema_info = self._get_schema_info(conn)

      result = {
        "success": len(errors) == 0,
        "schema_name": schema.name,
        "schema_version": schema.version,
        "statements_executed": executed,
        "statements_skipped": skipped,
        "errors": errors,
        "final_schema": schema_info,
      }

      if result["success"]:
        logger.info(f"Schema installation completed: {executed} statements executed")
      else:
        logger.warning(
          f"Schema installation completed with errors: {len(errors)} errors"
        )

      return result

    finally:
      conn.close()

  def _get_schema_info(self, conn) -> Dict[str, Any]:
    """Get current schema information from database."""
    try:
      # Query node tables
      node_result = conn.execute("CALL lbug.node_table_names() RETURN *")
      nodes = []
      while node_result.has_next():
        nodes.append(node_result.get_next()[0])

      # Query rel tables
      rel_result = conn.execute("CALL lbug.rel_table_names() RETURN *")
      relationships = []
      while rel_result.has_next():
        relationships.append(rel_result.get_next()[0])

      return {
        "nodes": sorted(nodes),
        "relationships": sorted(relationships),
        "node_count": len(nodes),
        "relationship_count": len(relationships),
      }
    except Exception as e:
      logger.error(f"Failed to get schema info: {e}")
      return {"nodes": [], "relationships": [], "error": str(e)}
