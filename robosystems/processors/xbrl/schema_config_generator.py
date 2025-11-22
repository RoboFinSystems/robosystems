"""
XBRL Schema Configuration Generator

Generates dynamic ingestion configurations from XBRL schema definitions.
This processor eliminates hardcoded mappings by deriving all ingestion logic
from schema definitions.

Capabilities:
- Generates table definitions from schema nodes and relationships
- Creates file pattern recognition for parquet files
- Maps relationship structures for graph ingestion
- Produces column structures from schema properties
- Provides dynamic ingestion methods

All configurations are derived from schema definitions - no hardcoded arrays.
This is XBRL-specific and tailored for XBRL graph ingestion workflows.
"""

from typing import Dict, List, Tuple, Optional, Any
import re
from dataclasses import dataclass

from robosystems.schemas.manager import (
  SchemaManager,
  SchemaConfiguration,
)
from robosystems.schemas.models import Node, Relationship
from robosystems.logger import logger


@dataclass
class IngestTableInfo:
  """Complete table information for ingestion."""

  name: str
  is_relationship: bool
  file_patterns: List[str]
  primary_keys: List[str]
  columns: List[str]
  from_node: Optional[str] = None
  to_node: Optional[str] = None
  properties: Optional[List[str]] = None


@dataclass
class SchemaIngestConfig:
  """Configuration for schema-driven ingestion."""

  schema_name: str
  base_schema: str
  extensions: List[str]
  node_tables: Dict[str, IngestTableInfo]
  relationship_tables: Dict[str, IngestTableInfo]
  file_pattern_mapping: Dict[str, str]
  table_name_mapping: Dict[str, str]


class XBRLSchemaConfigGenerator:
  """
  Generates dynamic ingestion configurations from XBRL schema definitions.

  This processor takes schema configurations (base + extensions) and creates
  comprehensive ingestion mappings by:

  1. Analyzing all nodes and relationships in the compiled schema
  2. Generating file patterns for parquet file recognition
  3. Creating column mappings from schema properties
  4. Providing table information for LadybugDB ingestion
  5. Handling relationship structure and foreign key detection

  Replaces hardcoded ingestion logic with schema-driven automation.
  """

  def __init__(self, schema_config: Dict[str, Any]):
    """
    Initialize generator with schema configuration.

    Args:
        schema_config: Schema configuration with base_schema and extensions
    """
    self.schema_manager = SchemaManager()
    self.schema_config = schema_config
    self.config = self._create_schema_configuration(schema_config)
    self.compiled_schema = self.schema_manager.load_and_compile_schema(self.config)
    self.ingest_config = self._generate_ingest_config()

    logger.debug("XBRL schema config generator initialized")
    logger.debug(f"Schema: {self.config.name}")
    logger.debug(
      f"Base: {self.config.base_schema}, Extensions: {self.config.extensions}"
    )
    logger.debug(f"Generated {len(self.ingest_config.node_tables)} node tables")
    logger.debug(
      f"Generated {len(self.ingest_config.relationship_tables)} relationship tables"
    )

  def _create_schema_configuration(self, config: Dict[str, Any]) -> SchemaConfiguration:
    """Create schema configuration from input config."""
    return SchemaConfiguration(
      name=config.get("name", "Dynamic Ingestion Schema"),
      description=config.get("description", "Schema-driven ingestion configuration"),
      version=config.get("version", "1.0.0"),
      base_schema=config.get("base_schema", "base"),
      extensions=config.get("extensions", []),
    )

  def _generate_ingest_config(self) -> SchemaIngestConfig:
    """Generate complete ingestion configuration from schema."""
    logger.debug("Generating schema-driven ingestion configuration")

    node_tables = {}
    relationship_tables = {}
    file_pattern_mapping = {}
    table_name_mapping = {}

    for node in self.compiled_schema.nodes:
      table_info = self._create_node_table_info(node)
      node_tables[node.name] = table_info

      for pattern in table_info.file_patterns:
        file_pattern_mapping[pattern] = node.name

      table_name_mapping[node.name.lower()] = node.name

    for relationship in self.compiled_schema.relationships:
      table_info = self._create_relationship_table_info(relationship)
      relationship_tables[relationship.name] = table_info

      for pattern in table_info.file_patterns:
        file_pattern_mapping[pattern] = relationship.name

      table_name_mapping[relationship.name.lower()] = relationship.name

    return SchemaIngestConfig(
      schema_name=self.config.name,
      base_schema=self.config.base_schema,
      extensions=self.config.extensions,
      node_tables=node_tables,
      relationship_tables=relationship_tables,
      file_pattern_mapping=file_pattern_mapping,
      table_name_mapping=table_name_mapping,
    )

  def _create_node_table_info(self, node: Node) -> IngestTableInfo:
    """Create table info for a node."""
    patterns = self._generate_file_patterns(node.name, is_relationship=False)

    primary_keys = [prop.name for prop in node.properties if prop.is_primary_key]

    columns = [prop.name for prop in node.properties]

    return IngestTableInfo(
      name=node.name,
      is_relationship=False,
      file_patterns=patterns,
      primary_keys=primary_keys,
      columns=columns,
    )

  def _create_relationship_table_info(
    self, relationship: Relationship
  ) -> IngestTableInfo:
    """Create table info for a relationship."""
    patterns = self._generate_file_patterns(relationship.name, is_relationship=True)

    primary_keys = []

    columns = ["from", "to"]
    if relationship.properties:
      columns.extend([prop.name for prop in relationship.properties])

    properties = (
      [prop.name for prop in relationship.properties] if relationship.properties else []
    )

    return IngestTableInfo(
      name=relationship.name,
      is_relationship=True,
      file_patterns=patterns,
      primary_keys=primary_keys,
      columns=columns,
      from_node=relationship.from_node,
      to_node=relationship.to_node,
      properties=properties,
    )

  def _generate_file_patterns(
    self, table_name: str, is_relationship: bool
  ) -> List[str]:
    """
    Generate file patterns for a table name.

    Creates patterns that match typical parquet file naming conventions.
    """
    patterns = []

    snake_case_name = self._pascal_to_snake(table_name)

    patterns.append(snake_case_name)

    if is_relationship:
      patterns.append(f"rel_{snake_case_name}")
    else:
      patterns.append(f"node_{snake_case_name}")

    patterns.append(f"{snake_case_name}_")
    patterns.append(f"{snake_case_name.replace('_', '')}")

    return patterns

  def _pascal_to_snake(self, name: str) -> str:
    """Convert PascalCase to snake_case."""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

  def is_relationship_file(self, filename: str) -> bool:
    """
    Determine if a file contains relationship data based on schema.

    Args:
        filename: Name of the parquet file or full S3 path

    Returns:
        True if file contains relationship data
    """
    table_name = self.get_table_name_from_file(filename)
    if table_name:
      return table_name in self.ingest_config.relationship_tables

    return False

  def get_table_name_from_file(self, filename: str) -> Optional[str]:
    """
    Get proper table name from filename using schema mappings.

    With exact table names in directory structure, this is now much simpler.

    Args:
        filename: Name of the parquet file or full path (local or S3)

    Returns:
        Proper table name or None if not found
    """
    if "/" in filename or "\\" in filename:
      path_parts = filename.replace("\\", "/").split("/")

      if len(path_parts) >= 2:
        potential_table = path_parts[-2]

        if potential_table in self.ingest_config.node_tables:
          return potential_table
        if potential_table in self.ingest_config.relationship_tables:
          return potential_table

    return None

  def get_table_info(self, table_name: str) -> Optional[IngestTableInfo]:
    """
    Get complete table information for ingestion.

    Args:
        table_name: Name of the table

    Returns:
        Table information or None if not found
    """
    if table_name in self.ingest_config.node_tables:
      return self.ingest_config.node_tables[table_name]

    if table_name in self.ingest_config.relationship_tables:
      return self.ingest_config.relationship_tables[table_name]

    return None

  def get_relationship_info(self, table_name: str) -> Optional[Tuple[str, str, str]]:
    """
    Get relationship information for creating relationship tables.

    Args:
        table_name: Name of the relationship table

    Returns:
        Tuple of (relationship_name, from_node, to_node) or None
    """
    if table_name in self.ingest_config.relationship_tables:
      table_info = self.ingest_config.relationship_tables[table_name]
      if table_info.from_node and table_info.to_node:
        return (table_info.name, table_info.from_node, table_info.to_node)

    return None

  def get_all_node_tables(self) -> List[str]:
    """Get list of all node table names."""
    return list(self.ingest_config.node_tables.keys())

  def get_all_relationship_tables(self) -> List[str]:
    """Get list of all relationship table names."""
    return list(self.ingest_config.relationship_tables.keys())

  def get_schema_statistics(self) -> Dict[str, Any]:
    """Get statistics about the schema-driven configuration."""
    return {
      "schema_name": self.ingest_config.schema_name,
      "base_schema": self.ingest_config.base_schema,
      "extensions": self.ingest_config.extensions,
      "total_nodes": len(self.ingest_config.node_tables),
      "total_relationships": len(self.ingest_config.relationship_tables),
      "total_file_patterns": len(self.ingest_config.file_pattern_mapping),
      "node_tables": list(self.ingest_config.node_tables.keys()),
      "relationship_tables": list(self.ingest_config.relationship_tables.keys()),
    }

  def print_configuration_summary(self):
    """Print a summary of the generated configuration."""
    stats = self.get_schema_statistics()

    logger.debug("=== XBRL Schema-Driven Ingestion Configuration ===")
    logger.debug(f"Schema: {stats['schema_name']}")
    logger.debug(f"Base: {stats['base_schema']}")
    logger.debug(f"Extensions: {', '.join(stats['extensions'])}")
    logger.debug(f"Node Tables: {stats['total_nodes']}")
    logger.debug(f"Relationship Tables: {stats['total_relationships']}")
    logger.debug(f"File Patterns: {stats['total_file_patterns']}")

    logger.debug("\n--- Node Tables ---")
    for table_name in stats["node_tables"]:
      table_info = self.ingest_config.node_tables[table_name]
      logger.debug(
        f"  {table_name}: {len(table_info.columns)} columns, patterns: {table_info.file_patterns}"
      )

    logger.debug("\n--- Relationship Tables ---")
    for table_name in stats["relationship_tables"]:
      table_info = self.ingest_config.relationship_tables[table_name]
      logger.debug(
        f"  {table_name}: {table_info.from_node} -> {table_info.to_node}, patterns: {table_info.file_patterns}"
      )


def create_roboledger_ingestion_processor() -> XBRLSchemaConfigGenerator:
  """Create ingestion processor for RoboLedger schema (base + roboledger)."""
  config = {
    "name": "RoboLedger Ingestion Schema",
    "description": "Schema-driven ingestion for RoboLedger (base + roboledger)",
    "base_schema": "base",
    "extensions": ["roboledger"],
  }
  return XBRLSchemaConfigGenerator(config)


def create_custom_ingestion_processor(
  extensions: List[str],
) -> XBRLSchemaConfigGenerator:
  """Create ingestion processor for custom schema extensions."""
  config = {
    "name": f"Custom Ingestion Schema ({', '.join(extensions)})",
    "description": f"Schema-driven ingestion for {', '.join(extensions)}",
    "base_schema": "base",
    "extensions": extensions,
  }
  return XBRLSchemaConfigGenerator(config)
