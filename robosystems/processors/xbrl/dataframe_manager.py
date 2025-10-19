"""
XBRL DataFrame Management

Centralized DataFrame initialization and management for XBRL graph processing.
Handles schema-driven DataFrame creation, mapping, and completeness validation.
"""

import pandas as pd
from typing import Dict, Optional, cast
from robosystems.logger import logger
from robosystems.processors.xbrl.naming_utils import camel_to_snake, make_plural


class DataFrameManager:
  """
  Manages DataFrame initialization and operations for XBRL graph processing.

  This class centralizes all DataFrame-related operations including:
  - Dynamic DataFrame creation from schema definitions
  - Schema-to-DataFrame attribute mapping
  - Column standardization and schema completeness validation
  """

  def __init__(
    self, schema_adapter, ingest_adapter, enable_column_standardization=False
  ):
    """
    Initialize DataFrame manager with schema adapters.

    Args:
        schema_adapter: SchemaProcessor instance for DataFrame creation
        ingest_adapter: SchemaIngestionProcessor instance for filename generation
        enable_column_standardization: Whether to enable column name standardization
    """
    self.schema_adapter = schema_adapter
    self.ingest_adapter = ingest_adapter
    self.enable_column_standardization = enable_column_standardization

    self.dataframes: Dict[str, pd.DataFrame] = {}
    self.schema_to_dataframe_mapping: Dict[str, str] = {}

  def initialize_all_dataframes(self) -> Dict[str, pd.DataFrame]:
    """
    Initialize all DataFrames dynamically from schema definitions.

    Returns:
        Dictionary mapping DataFrame attribute names to initialized DataFrames

    Raises:
        ValueError: If schema-based initialization fails
    """
    if not self.schema_adapter:
      raise ValueError(
        "Schema configuration is required for XBRL processing. "
        "No manual DataFrame fallback is supported. "
        "Please provide a valid schema_config parameter."
      )

    logger.debug("Initializing DataFrames dynamically using SchemaProcessor")

    try:
      schema_builder = self.schema_adapter.schema_builder
      schema = schema_builder.schema

      if not schema or not schema.nodes:
        logger.warning("No schema or nodes found in schema builder")
        return self.dataframes

      node_types = [node.name for node in schema.nodes]
      for node_type in node_types:
        df_attr_name = self._convert_schema_name_to_dataframe_attr(
          node_type, is_node=True
        )
        try:
          df = self.schema_adapter.create_schema_compatible_dataframe(node_type)
          self.dataframes[df_attr_name] = df
          logger.debug(f"Initialized DataFrame: {df_attr_name} for node {node_type}")
        except Exception as e:
          logger.error(f"Failed to create DataFrame for node {node_type}: {e}")
          raise ValueError(
            f"Failed to initialize DataFrame for node type '{node_type}': {e}. "
            f"Schema-based initialization is required."
          )

      if schema.relationships:
        relationship_types = [rel.name for rel in schema.relationships]
        for rel_type in relationship_types:
          df_attr_name = self._convert_schema_name_to_dataframe_attr(
            rel_type, is_node=False
          )
          try:
            df = self.schema_adapter.create_schema_compatible_dataframe(rel_type)
            self.dataframes[df_attr_name] = df
            logger.debug(
              f"Initialized DataFrame: {df_attr_name} for relationship {rel_type}"
            )
          except Exception as e:
            logger.error(f"Failed to create DataFrame for relationship {rel_type}: {e}")
            raise ValueError(
              f"Failed to initialize DataFrame for relationship type '{rel_type}': {e}. "
              f"Schema-based initialization is required."
            )

        logger.debug(
          f"All DataFrames initialized dynamically: {len(node_types)} nodes, {len(relationship_types)} relationships"
        )

      additional_relationship_dfs = {
        "fact_has_dimension_rel_df": "Relationship: fact -> dimension",
        "fact_dimension_axis_element_rel_df": "Relationship: dimension -> axis element",
        "fact_dimension_member_element_rel_df": "Relationship: dimension -> member element",
        "fact_set_contains_facts_df": "Relationship: fact set -> facts",
      }

      for df_name, description in additional_relationship_dfs.items():
        if df_name not in self.dataframes:
          try:
            if df_name == "fact_has_dimension_rel_df":
              schema_name = "FACT_HAS_DIMENSION"
            elif df_name == "fact_dimension_axis_element_rel_df":
              schema_name = "FACT_DIMENSION_AXIS_ELEMENT"
            elif df_name == "fact_dimension_member_element_rel_df":
              schema_name = "FACT_DIMENSION_MEMBER_ELEMENT"
            elif df_name == "fact_set_contains_facts_df":
              schema_name = "FACT_SET_CONTAINS_FACT"
            else:
              rel_type = df_name.replace("_df", "").replace("_rel", "")
              parts = rel_type.split("_")
              schema_name = "".join(p.capitalize() for p in parts) + "Rel"

            df = self.schema_adapter.create_schema_compatible_dataframe(schema_name)
            self.dataframes[df_name] = df
            logger.debug(
              f"Initialized additional relationship DataFrame: {df_name} ({description})"
            )
          except Exception:
            logger.warning(
              f"Could not create schema-based DataFrame for {df_name}, trying empty with proper columns"
            )
            if "fact_has_dimension_rel" in df_name:
              df = pd.DataFrame(columns=["from", "to"])
            elif "axis_element" in df_name:
              df = pd.DataFrame(columns=["from", "to"])
            elif "member_element" in df_name:
              df = pd.DataFrame(columns=["from", "to"])
            elif "fact_set_contains" in df_name:
              df = pd.DataFrame(columns=["from", "to"])
            else:
              df = pd.DataFrame(columns=["from", "to"])
            self.dataframes[df_name] = df
            logger.debug(
              f"Initialized {df_name} with basic columns: {list(df.columns)}"
            )

    except Exception as e:
      logger.error(f"Failed to initialize DataFrames dynamically: {e}")
      raise ValueError(
        f"Failed to initialize DataFrames with schema adapter: {e}. "
        f"Schema-based initialization is required for XBRL processing."
      )

    return self.dataframes

  def create_dynamic_dataframe_mapping(self) -> Dict[str, str]:
    """
    Create mapping from schema names to DataFrame attributes for dynamic file saving.

    Returns:
        Dictionary mapping schema names to DataFrame attribute names
    """
    self.schema_to_dataframe_mapping = {}

    if not self.schema_adapter:
      logger.warning("No schema adapter available for dynamic mapping")
      return self.schema_to_dataframe_mapping

    try:
      schema_builder = self.schema_adapter.schema_builder
      schema = schema_builder.schema

      if not schema:
        logger.warning("No schema available for dynamic mapping")
        return self.schema_to_dataframe_mapping

      if schema.nodes:
        for node in schema.nodes:
          node_type = node.name
          df_attr_name = self._convert_schema_name_to_dataframe_attr(
            node_type, is_node=True
          )

          if df_attr_name in self.dataframes:
            self.schema_to_dataframe_mapping[node_type] = df_attr_name
            logger.debug(f"Mapped node {node_type} -> {df_attr_name}")
          else:
            logger.debug(
              f"DataFrame attribute {df_attr_name} not found for node {node_type}"
            )

      if schema.relationships:
        for rel in schema.relationships:
          rel_type = rel.name
          df_attr_name = self._convert_schema_name_to_dataframe_attr(
            rel_type, is_node=False
          )

          if df_attr_name in self.dataframes:
            self.schema_to_dataframe_mapping[rel_type] = df_attr_name
            logger.debug(f"Mapped relationship {rel_type} -> {df_attr_name}")
          else:
            logger.debug(
              f"DataFrame attribute {df_attr_name} not found for relationship {rel_type}"
            )

      additional_mappings = {
        "FACT_DIMENSION_AXIS_ELEMENT": "fact_dimension_axis_element_rel_df",
        "FACT_DIMENSION_MEMBER_ELEMENT": "fact_dimension_member_element_rel_df",
        "FACT_SET_CONTAINS_FACT": "fact_set_contains_facts_df",
      }

      for schema_name, df_attr in additional_mappings.items():
        if df_attr in self.dataframes:
          self.schema_to_dataframe_mapping[schema_name] = df_attr
          logger.debug(f"Added explicit mapping for {schema_name} -> {df_attr}")

      logger.debug(
        f"Dynamic DataFrame mapping created: {len(self.schema_to_dataframe_mapping)} mappings"
      )

    except Exception as e:
      logger.error(f"Failed to create dynamic DataFrame mapping: {e}")
      self.schema_to_dataframe_mapping = {
        "Entity": "entities_df",
        "Report": "reports_df",
        "Fact": "facts_df",
        "ENTITY_HAS_REPORT": "entity_reports_df",
        "REPORT_HAS_FACT": "report_facts_df",
      }
      logger.warning("Using fallback minimal DataFrame mapping")

    return self.schema_to_dataframe_mapping

  def get_dataframe(self, df_attr_name: str) -> Optional[pd.DataFrame]:
    """
    Get DataFrame by attribute name.

    Args:
        df_attr_name: DataFrame attribute name (e.g., 'entities_df')

    Returns:
        DataFrame if found, None otherwise
    """
    return self.dataframes.get(df_attr_name)

  def set_dataframe(self, df_attr_name: str, df: pd.DataFrame):
    """
    Set or update a DataFrame.

    Args:
        df_attr_name: DataFrame attribute name
        df: DataFrame to store
    """
    self.dataframes[df_attr_name] = df

  def standardize_dataframe_columns(
    self, df: pd.DataFrame, table_name: str
  ) -> pd.DataFrame:
    """
    Standardize DataFrame column names to match target schema.

    Args:
        df: Input DataFrame
        table_name: Target table name for column mapping

    Returns:
        DataFrame with standardized column names
    """
    if not self.enable_column_standardization or df.empty:
      return df

    column_mappings = {}

    if table_name in column_mappings:
      mapping = column_mappings[table_name]
      df = df.rename(columns=mapping)
      if mapping:
        logger.debug(f"Standardized columns for {table_name}: {mapping}")

    return df

  def ensure_schema_completeness(
    self, df: pd.DataFrame, table_name: str
  ) -> pd.DataFrame:
    """
    Ensure DataFrame has all schema-defined columns before saving to parquet.
    This prevents missing columns during Kuzu ingestion.

    Args:
        df: DataFrame to validate
        table_name: Schema table name

    Returns:
        DataFrame with all schema-defined columns
    """
    try:
      logger.debug(f"Checking schema completeness for table: {table_name}")

      if not self.schema_adapter:
        logger.warning(f"No schema adapter for table {table_name}")
        return df

      table_info = self.schema_adapter.get_schema_info(table_name)
      logger.debug(f"Schema lookup result for {table_name}: {table_info['type']}")

      if table_info["type"] == "unknown":
        logger.warning(f"No schema found for table {table_name}, saving as-is")
        return df

      schema_info = table_info["schema"]

      expected_columns = set()

      if table_info["type"] == "relationship":
        expected_columns.update(["from", "to"])

      for prop in schema_info["properties"]:
        expected_columns.add(prop.name)

      current_columns = set(df.columns)
      missing_columns = expected_columns - current_columns

      if missing_columns:
        logger.debug(
          f"Adding missing schema columns to {table_name}: {missing_columns}"
        )

        for col in missing_columns:
          prop_type = None
          for prop in schema_info["properties"]:
            if prop.name == col:
              prop_type = prop.type.upper()
              break

          if prop_type in ["STRING", "VARCHAR", "TEXT"]:
            df[col] = ""
          elif prop_type in ["INT", "INT64", "INTEGER", "INT32"]:
            df[col] = 0
          elif prop_type in ["DOUBLE", "FLOAT", "DECIMAL"]:
            df[col] = 0.0
          elif prop_type in ["BOOLEAN", "BOOL"]:
            df[col] = False
          else:
            df[col] = None

        ordered_columns: list[str] = []
        if table_info["type"] == "relationship":
          if "from" in expected_columns:
            ordered_columns.append("from")
          if "to" in expected_columns:
            ordered_columns.append("to")

        for prop in schema_info["properties"]:
          if prop.name in expected_columns:
            ordered_columns.append(prop.name)

        extra_columns = current_columns - expected_columns
        ordered_columns.extend(sorted(extra_columns))

        df = cast(pd.DataFrame, df[ordered_columns])

        logger.debug(
          f"Schema-complete {table_name} now has {len(df.columns)} columns: {list(df.columns)}"
        )

      return df

    except Exception as e:
      logger.warning(f"Failed to ensure schema completeness for {table_name}: {e}")
      return df

  def _convert_schema_name_to_dataframe_attr(
    self, schema_name: str, is_node: bool
  ) -> str:
    """
    Convert schema name to DataFrame attribute name following current naming conventions.

    Args:
        schema_name: Schema type name (e.g., 'Entity', 'FACT_HAS_ELEMENT')
        is_node: True for node types, False for relationship types

    Returns:
        DataFrame attribute name (e.g., 'entities_df', 'fact_elements_df')
    """
    if is_node:
      snake_case = camel_to_snake(schema_name)
      plural = make_plural(snake_case)
      return f"{plural}_df"
    else:
      if schema_name == "FACT_HAS_DIMENSION":
        return "fact_has_dimension_rel_df"

      parts = schema_name.lower().split("_")
      if len(parts) >= 3 and parts[1] == "has":
        entity = parts[0]
        property_name = "_".join(parts[2:])
        plural_property = make_plural(property_name)
        return f"{entity}_{plural_property}_df"
      else:
        snake_case = schema_name.lower()
        return f"{snake_case}_df"
