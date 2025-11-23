"""
XBRL Schema Adapter

Adapts XBRL DataFrame structures to match LadybugDB schema definitions.
Handles schema validation, column mapping, and DataFrame structure compatibility
for seamless XBRL data ingestion into the graph database.

This is XBRL-specific and handles the transformation of XBRL processor output
(DataFrames) to match the required schema format for graph database ingestion.
"""

import pandas as pd
from typing import Dict, List, Any, Optional
from robosystems.logger import logger
from robosystems.schemas.builder import LadybugSchemaBuilder


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

  def __init__(self, schema_config: Dict[str, Any]):
    """
    Initialize adapter with schema configuration.

    Args:
        schema_config: Configuration dict for schema building
    """
    self.schema_config = schema_config
    self.schema_builder = LadybugSchemaBuilder(schema_config)
    self.schema_builder.load_schemas()

    self.compiled_schema = self.schema_builder.schema
    self.node_schemas: Dict[str, Dict[str, Any]] = {}
    self.relationship_schemas: Dict[str, Dict[str, Any]] = {}

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
    self, table_name: str, data_dict: Dict[str, Any]
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
  ) -> Dict[str, Any]:
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

  def get_schema_info(self, table_name: str) -> Dict[str, Any]:
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

  def get_available_schemas(self) -> List[str]:
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

  def _get_schema_info(self, schema_name: str) -> Optional[Dict[str, Any]]:
    """Get schema info for a given schema name."""
    if schema_name in self.node_schemas:
      return self.node_schemas[schema_name]
    elif schema_name in self.relationship_schemas:
      return self.relationship_schemas[schema_name]
    return None

  def _build_column_list(self, schema_info: Dict[str, Any]) -> List[str]:
    """Build complete column list for a schema."""
    columns = []

    if schema_info["table_type"] == "relationship":
      columns.extend(["from", "to"])

    for prop in schema_info["properties"]:
      columns.append(prop.name)

    return columns

  def _process_data_with_schema(
    self, data_dict: Dict[str, Any], schema_info: Dict[str, Any]
  ) -> Dict[str, Any]:
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
