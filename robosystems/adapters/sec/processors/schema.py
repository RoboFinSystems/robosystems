"""
XBRL Schema Utilities

Schema adapter and configuration generator for XBRL graph processing.
Handles schema validation, column mapping, and DataFrame structure compatibility
for XBRL data ingestion into the graph database.

Classes:
- XBRLSchemaAdapter: Adapts XBRL DataFrame structures to LadybugDB schemas
- XBRLSchemaConfigGenerator: Generates dynamic ingestion configurations from schemas
- IngestTableInfo: Complete table information for ingestion
- SchemaIngestConfig: Configuration for schema-driven ingestion
"""

import re
from dataclasses import dataclass
from typing import Any

import pandas as pd

from robosystems.logger import logger
from robosystems.schemas.builder import LadybugSchemaBuilder
from robosystems.schemas.manager import SchemaConfiguration, SchemaManager
from robosystems.schemas.models import Node, Relationship

# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class IngestTableInfo:
  """Complete table information for ingestion."""

  name: str
  is_relationship: bool
  file_patterns: list[str]
  primary_keys: list[str]
  columns: list[str]
  from_node: str | None = None
  to_node: str | None = None
  properties: list[str] | None = None


@dataclass
class SchemaIngestConfig:
  """Configuration for schema-driven ingestion."""

  schema_name: str
  base_schema: str
  extensions: list[str]
  node_tables: dict[str, IngestTableInfo]
  relationship_tables: dict[str, IngestTableInfo]
  file_pattern_mapping: dict[str, str]
  table_name_mapping: dict[str, str]


# =============================================================================
# Schema Adapter
# =============================================================================


class XBRLSchemaAdapter:
  """
  Adapts XBRL DataFrame structures to ensure compatibility with LadybugDB schemas.

  This adapter bridges the gap between XBRL data extraction and graph ingestion by:
  - Validating DataFrame structures against schema definitions
  - Creating schema-compatible DataFrames with proper columns
  - Transforming data to match expected schema formats
  - Providing schema introspection and validation capabilities
  """

  # Mapping from XBRLGraphProcessor table names to schema relationship names
  XBRL_TABLE_MAPPING = {
    # Node table mappings (no changes needed - already work)
    # Relationship table mappings - complete list from XBRL processing
    "EntityReports": "ENTITY_HAS_REPORT",
    "ReportFacts": "REPORT_HAS_FACT",
    "ReportFactSets": "REPORT_HAS_FACT_SET",
    "ReportTaxonomies": "REPORT_USES_TAXONOMY",
    "FactUnits": "FACT_HAS_UNIT",
    "FactDimensions": "FACT_HAS_DIMENSION",
    "FactDimensionsRel": "FACT_HAS_DIMENSION",
    "FactEntities": "FACT_HAS_ENTITY",
    "FactElements": "FACT_HAS_ELEMENT",
    "FactPeriods": "FACT_HAS_PERIOD",
    "FactSetFacts": "FACT_SET_CONTAINS_FACT",
    "ElementLabels": "ELEMENT_HAS_LABEL",
    "ElementReferences": "ELEMENT_HAS_REFERENCE",
    "StructureTaxonomies": "STRUCTURE_HAS_TAXONOMY",
    "TaxonomyLabels": "TAXONOMY_HAS_LABEL",
    "TaxonomyReferences": "TAXONOMY_HAS_REFERENCE",
    "StructureAssociations": "STRUCTURE_HAS_ASSOCIATION",
    "AssociationFromElements": "ASSOCIATION_HAS_FROM_ELEMENT",
    "AssociationToElements": "ASSOCIATION_HAS_TO_ELEMENT",
    "FactDimensionElements": "FACT_DIMENSION_REFERENCES_ELEMENT",
  }

  def __init__(self, schema_config: dict[str, Any]):
    """
    Initialize adapter with schema configuration.

    Args:
        schema_config: Configuration dict for schema building
    """
    self.schema_config = schema_config
    self.schema_builder = LadybugSchemaBuilder(schema_config)
    self.schema_builder.load_schemas()

    self.compiled_schema = self.schema_builder.schema
    self.node_schemas: dict[str, dict[str, Any]] = {}
    self.relationship_schemas: dict[str, dict[str, Any]] = {}

    self._extract_schema_definitions()

  def _extract_schema_definitions(self) -> None:
    """Extract and index schema definitions for efficient processing."""
    logger.debug("Extracting schema definitions for DataFrame processing")

    if not self.compiled_schema:
      logger.warning("No compiled schema available for extraction")
      return

    if self.compiled_schema.nodes:
      for node in self.compiled_schema.nodes:
        primary_keys = [prop.name for prop in node.properties if prop.is_primary_key]
        self.node_schemas[node.name] = {
          "primary_keys": primary_keys,
          "properties": node.properties,
          "table_type": "node",
        }
        logger.debug(
          f"Processed node schema {node.name}: {len(node.properties)} properties, "
          f"primary keys: {primary_keys}"
        )

    if self.compiled_schema.relationships:
      for relationship in self.compiled_schema.relationships:
        self.relationship_schemas[relationship.name] = {
          "from_node": relationship.from_node,
          "to_node": relationship.to_node,
          "properties": relationship.properties,
          "table_type": "relationship",
        }
        logger.debug(
          f"Processed relationship schema {relationship.name}: "
          f"{len(relationship.properties)} properties, "
          f"connects {relationship.from_node} -> {relationship.to_node}"
        )

    logger.debug(
      f"Schema extraction complete: {len(self.node_schemas)} nodes, "
      f"{len(self.relationship_schemas)} relationships"
    )

  def create_schema_compatible_dataframe(self, table_name: str) -> pd.DataFrame:
    """
    Create an empty DataFrame with columns matching the schema definition.

    Args:
        table_name: Name of the table/schema to create DataFrame for

    Returns:
        Empty DataFrame with correct column structure for the schema
    """
    schema_name = self._resolve_schema_name(table_name)
    schema_info = self._get_schema_info(schema_name)

    if not schema_info:
      logger.warning(
        f"Schema not found for table {table_name} (resolved to {schema_name}), "
        f"creating empty DataFrame"
      )
      return pd.DataFrame()

    columns = self._build_column_list(schema_info)

    logger.debug(
      f"Creating schema-compatible DataFrame for {table_name} "
      f"(schema: {schema_name}) with {len(columns)} columns: {columns}"
    )

    return pd.DataFrame(columns=columns)

  def process_dataframe_for_schema(
    self, table_name: str, data_dict: dict[str, Any]
  ) -> pd.DataFrame:
    """
    Process data dictionary into a schema-compatible DataFrame.

    Args:
        table_name: Name of the target table/schema
        data_dict: Raw data dictionary (may have missing columns)

    Returns:
        DataFrame with complete schema-compatible structure
    """
    schema_name = self._resolve_schema_name(table_name)
    schema_info = self._get_schema_info(schema_name)

    if not schema_info:
      logger.warning(
        f"Schema not found for table {table_name} (resolved to {schema_name}), "
        f"returning data as-is"
      )
      return pd.DataFrame([data_dict])

    processed_data = self._process_data_with_schema(data_dict, schema_info)
    result_df = pd.DataFrame([processed_data])

    logger.debug(
      f"Processed {table_name} data with {len(processed_data)} columns "
      f"for schema {schema_name}"
    )

    return result_df

  def validate_dataframe_schema(
    self, table_name: str, df: pd.DataFrame
  ) -> dict[str, Any]:
    """
    Validate DataFrame structure against schema requirements.

    Args:
        table_name: Name of the target schema
        df: DataFrame to validate

    Returns:
        Validation results with status and any issues found
    """
    schema_name = self._resolve_schema_name(table_name)
    schema_info = self._get_schema_info(schema_name)

    if not schema_info:
      return {
        "valid": False,
        "error": f"Schema not found for table {table_name} (resolved to {schema_name})",
      }

    expected_columns = self._build_column_list(schema_info)
    actual_columns = list(df.columns)

    if len(actual_columns) != len(expected_columns):
      return {
        "valid": False,
        "error": f"Column count mismatch. Expected {len(expected_columns)}, "
        f"got {len(actual_columns)}",
        "expected_columns": expected_columns,
        "actual_columns": actual_columns,
        "missing_columns": set(expected_columns) - set(actual_columns),
        "extra_columns": set(actual_columns) - set(expected_columns),
      }

    if set(actual_columns) != set(expected_columns):
      return {
        "valid": False,
        "error": "Column names do not match schema",
        "expected_columns": expected_columns,
        "actual_columns": actual_columns,
        "missing_columns": set(expected_columns) - set(actual_columns),
        "extra_columns": set(actual_columns) - set(expected_columns),
      }

    return {"valid": True, "message": "DataFrame structure matches schema"}

  def get_schema_info(self, table_name: str) -> dict[str, Any]:
    """
    Get comprehensive information about a table's schema.

    Args:
        table_name: Name of the table/schema

    Returns:
        Schema information including type, properties, and metadata
    """
    schema_name = self._resolve_schema_name(table_name)
    schema_info = self._get_schema_info(schema_name)

    if not schema_info:
      return {"type": "unknown", "schema": None, "column_count": 0}

    return {
      "type": schema_info["table_type"],
      "schema": schema_info,
      "column_count": len(schema_info["properties"]),
      "original_name": table_name,
      "schema_name": schema_name,
    }

  def get_available_schemas(self) -> list[str]:
    """Get list of all available schema names."""
    return list(self.node_schemas.keys()) + list(self.relationship_schemas.keys())

  def print_schema_summary(self) -> None:
    """Print comprehensive schema summary for debugging."""
    logger.debug("=== XBRL SCHEMA ADAPTER SUMMARY ===")
    logger.debug(f"Node Schemas: {len(self.node_schemas)}")
    for name, info in self.node_schemas.items():
      logger.debug(f"  {name}: {len(info['properties'])} properties")

    logger.debug(f"Relationship Schemas: {len(self.relationship_schemas)}")
    for name, info in self.relationship_schemas.items():
      logger.debug(f"  {name}: {len(info['properties'])} properties")

    logger.debug(f"XBRL Table Mappings: {len(self.XBRL_TABLE_MAPPING)}")
    for source, target in self.XBRL_TABLE_MAPPING.items():
      logger.debug(f"  {source} -> {target}")

  def _resolve_schema_name(self, table_name: str) -> str:
    """Resolve table name to schema name using mapping."""
    return self.XBRL_TABLE_MAPPING.get(table_name, table_name)

  def _get_schema_info(self, schema_name: str) -> dict[str, Any] | None:
    """Get schema info for a given schema name."""
    if schema_name in self.node_schemas:
      return self.node_schemas[schema_name]
    elif schema_name in self.relationship_schemas:
      return self.relationship_schemas[schema_name]
    return None

  def _build_column_list(self, schema_info: dict[str, Any]) -> list[str]:
    """Build complete column list for a schema."""
    columns = []

    if schema_info["table_type"] == "relationship":
      columns.extend(["from", "to"])

    for prop in schema_info["properties"]:
      columns.append(prop.name)

    return columns

  def _process_data_with_schema(
    self, data_dict: dict[str, Any], schema_info: dict[str, Any]
  ) -> dict[str, Any]:
    """Process data dictionary with schema requirements."""
    processed_data = {}

    if schema_info["table_type"] == "relationship":
      for fk_col in ["from", "to"]:
        if fk_col in data_dict:
          processed_data[fk_col] = data_dict[fk_col]

    for prop in schema_info["properties"]:
      column_name = prop.name

      if column_name in data_dict:
        processed_data[column_name] = data_dict[column_name]
      else:
        default_value = self._get_default_value_for_type(prop.type)
        processed_data[column_name] = default_value

        logger.debug(
          f"Added default value for missing column '{column_name}': {default_value}"
        )

    return processed_data

  def _get_default_value_for_type(self, data_type: str) -> Any:
    """Get appropriate default value for a data type."""
    data_type = data_type.upper()

    type_defaults = {
      ("STRING", "VARCHAR", "TEXT"): "",
      ("INT", "INT64", "INTEGER", "INT32"): 0,
      ("DOUBLE", "FLOAT", "DECIMAL"): 0.0,
      ("BOOLEAN", "BOOL"): False,
      ("DATE", "TIMESTAMP"): None,
    }

    for type_group, default in type_defaults.items():
      if data_type in type_group:
        return default

    return None


# =============================================================================
# Schema Config Generator
# =============================================================================


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

  def __init__(self, schema_config: dict[str, Any]):
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

  def _create_schema_configuration(self, config: dict[str, Any]) -> SchemaConfiguration:
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
  ) -> list[str]:
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

  def get_table_name_from_file(self, filename: str) -> str | None:
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

  def get_table_info(self, table_name: str) -> IngestTableInfo | None:
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

  def get_relationship_info(self, table_name: str) -> tuple[str, str, str] | None:
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

  def get_all_node_tables(self) -> list[str]:
    """Get list of all node table names."""
    return list(self.ingest_config.node_tables.keys())

  def get_all_relationship_tables(self) -> list[str]:
    """Get list of all relationship table names."""
    return list(self.ingest_config.relationship_tables.keys())

  def get_schema_statistics(self) -> dict[str, Any]:
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


# =============================================================================
# Factory Functions
# =============================================================================


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
  extensions: list[str],
) -> XBRLSchemaConfigGenerator:
  """Create ingestion processor for custom schema extensions."""
  config = {
    "name": f"Custom Ingestion Schema ({', '.join(extensions)})",
    "description": f"Schema-driven ingestion for {', '.join(extensions)}",
    "base_schema": "base",
    "extensions": extensions,
  }
  return XBRLSchemaConfigGenerator(config)
