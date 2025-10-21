"""
XBRL Parquet File Writer

Handles all Parquet file I/O operations with schema validation, type fixes,
and standardized filename generation for XBRL graph data.
"""

import pandas as pd
from pathlib import Path
from robosystems.logger import logger
from robosystems.processors.xbrl.naming_utils import convert_schema_name_to_filename


class ParquetWriter:
  """
  Manages Parquet file output for XBRL graph processing.

  This class centralizes all file I/O operations including:
  - Directory structure creation (nodes/ and relationships/)
  - Column type fixes for specific tables
  - Schema completeness validation
  - Deduplication
  - Standardized filename generation
  """

  def __init__(
    self,
    output_dir: Path,
    schema_adapter,
    ingest_adapter,
    df_manager,
    enable_standardized_filenames=False,
    enable_type_prefixes=False,
    enable_column_standardization=False,
    sec_filer=None,
    sec_report=None,
  ):
    """
    Initialize Parquet writer with configuration.

    Args:
        output_dir: Base output directory for parquet files
        schema_adapter: SchemaProcessor instance for schema info
        ingest_adapter: SchemaIngestionProcessor instance for table lists
        df_manager: DataFrameManager instance for schema operations
        enable_standardized_filenames: Whether to add filing metadata to filenames
        enable_type_prefixes: Whether to prefix filenames with node_/rel_
        enable_column_standardization: Whether to standardize column names
        sec_filer: SEC filer information (optional)
        sec_report: SEC report information (optional)
    """
    self.output_dir = output_dir
    self.schema_adapter = schema_adapter
    self.ingest_adapter = ingest_adapter
    self.df_manager = df_manager
    self.enable_standardized_filenames = enable_standardized_filenames
    self.enable_type_prefixes = enable_type_prefixes
    self.enable_column_standardization = enable_column_standardization
    self.sec_filer = sec_filer
    self.sec_report = sec_report

  def write_all_dataframes(self, schema_to_dataframe_mapping: dict, processor):
    """
    Write all DataFrames to parquet files organized in nodes/ and relationships/ subdirectories.

    Args:
        schema_to_dataframe_mapping: Dict mapping schema names to DataFrame attribute names
        processor: XBRLGraphProcessor instance (for accessing DataFrames)
    """
    logger.debug(f"Creating output directory: {self.output_dir}")
    self.output_dir.mkdir(parents=True, exist_ok=True)

    nodes_dir = self.output_dir / "nodes"
    relationships_dir = self.output_dir / "relationships"
    nodes_dir.mkdir(parents=True, exist_ok=True)
    relationships_dir.mkdir(parents=True, exist_ok=True)

    logger.debug("Created nodes/ and relationships/ subdirectories")

    if self.schema_adapter and self.ingest_adapter:
      logger.debug("Using schema-driven file output with proper directory organization")

      for schema_name, dataframe_attr in schema_to_dataframe_mapping.items():
        if hasattr(processor, dataframe_attr):
          df = getattr(processor, dataframe_attr)
          filename = convert_schema_name_to_filename(schema_name)
          self.write_dataframe_schema_driven(df, filename, schema_name)
          logger.debug(f"Saved {schema_name} -> {filename} ({len(df)} rows)")

    else:
      logger.warning(
        "Schema adapter not available, using fallback file output with directory organization"
      )
      self._write_all_dataframes_fallback(processor)

    logger.info("All parquet files have been saved successfully")

  def write_dataframe_schema_driven(
    self, df: pd.DataFrame, filename: str, schema_name: str
  ):
    """
    Save a DataFrame to parquet using schema-driven approach with correct table name and directory.

    Args:
        df: DataFrame to save
        filename: Base filename
        schema_name: Schema table name (e.g., 'Entity', 'FACT_HAS_ELEMENT')
    """
    if not df.empty:
      df = df.copy()

      df = self._fix_column_types_by_schema(df, schema_name)

      if self.schema_adapter:
        logger.debug(
          f"Ensuring schema completeness for {schema_name} (from {filename})"
        )
        logger.debug(
          f"DataFrame before schema completeness: {len(df.columns)} columns: {list(df.columns)}"
        )
        df = self.df_manager.ensure_schema_completeness(df, schema_name)
        logger.debug(
          f"DataFrame after schema completeness: {len(df.columns)} columns: {list(df.columns)}"
        )

      if self.enable_column_standardization:
        df = self.df_manager.standardize_dataframe_columns(df, schema_name)

      is_relationship = (
        schema_name in self.ingest_adapter.get_all_relationship_tables()
        if self.ingest_adapter
        else False
      )

      if "identifier" in df.columns and not is_relationship:
        original_count = len(df)
        df = df.drop_duplicates(subset=["identifier"], keep="first")
        deduped_count = len(df)
        if original_count != deduped_count:
          logger.info(
            f"Deduplicated {filename}: {original_count} -> {deduped_count} rows"
          )

      subdir = "relationships" if is_relationship else "nodes"

      final_filename = self.generate_standardized_filename(schema_name, is_relationship)

      filepath = self.output_dir / subdir / final_filename

      logger.debug(
        f"Saving {schema_name} to {final_filename}: {len(df)} rows, {len(df.columns)} columns"
      )
      logger.debug(f"Columns in {schema_name}: {list(df.columns)}")

      df.to_parquet(filepath, index=False)

      if final_filename != filename:
        logger.info(f"📝 Standardized filename: {filename} -> {final_filename}")
      logger.debug(f"Saved {len(df)} rows to {filepath}")
    else:
      logger.debug(f"Skipping empty DataFrame for {filename}")

  def write_dataframe(self, df: pd.DataFrame, filename: str):
    """
    Save a DataFrame to parquet (fallback method with filename-based logic).

    Args:
        df: DataFrame to save
        filename: Filename (may include subdirectory like "nodes/Entity.parquet")
    """
    if not df.empty:
      df = df.copy()

      if "/" in filename:
        parts = filename.rsplit("/", 1)
        subdir = parts[0]
        base_filename = parts[1]
      else:
        subdir = ""
        base_filename = filename

      df = self._fix_column_types_by_filename(df, base_filename)

      base_name = base_filename.replace(".parquet", "")

      if base_name == "fact_has_dimension":
        table_name = "FactDimensionsRel"
      else:
        table_name = base_name.replace("_", " ").title().replace(" ", "")

      if self.schema_adapter:
        logger.info(
          f"Ensuring schema completeness for {table_name} (from {base_filename})"
        )
        logger.debug(
          f"DataFrame before schema completeness: {len(df.columns)} columns: {list(df.columns)}"
        )
        df = self.df_manager.ensure_schema_completeness(df, table_name)
        logger.debug(
          f"DataFrame after schema completeness: {len(df.columns)} columns: {list(df.columns)}"
        )

      if self.enable_column_standardization:
        df = self.df_manager.standardize_dataframe_columns(df, table_name)

      is_relationship = (
        table_name in self.ingest_adapter.get_all_relationship_tables()
        if self.ingest_adapter
        else False
      )

      if "identifier" in df.columns and not is_relationship:
        original_count = len(df)
        df = df.drop_duplicates(subset=["identifier"], keep="first")
        deduped_count = len(df)
        if original_count != deduped_count:
          logger.info(
            f"Deduplicated {base_filename}: {original_count} -> {deduped_count} rows"
          )

      base_name_clean = base_filename.replace(".parquet", "")
      is_relationship = self._is_relationship_filename(base_name_clean)
      final_filename = self.generate_standardized_filename(
        base_name_clean, is_relationship
      )

      if subdir:
        filepath = self.output_dir / subdir / final_filename
      else:
        filepath = self.output_dir / final_filename

      logger.info(
        f"Saving {table_name} to {final_filename}: {len(df)} rows, {len(df.columns)} columns"
      )
      logger.debug(f"Columns in {table_name}: {list(df.columns)}")

      df.to_parquet(filepath, index=False)

      if final_filename != filename:
        logger.info(f"📝 Standardized filename: {filename} -> {final_filename}")
      logger.debug(f"Saved {len(df)} rows to {filepath}")
    else:
      logger.debug(f"Skipping empty DataFrame for {filename}")

  def generate_standardized_filename(
    self, base_name: str, is_relationship: bool = False
  ) -> str:
    """
    Generate standardized filename based on filing information.

    Args:
        base_name: Base filename (e.g., "Entity", "FACT_HAS_ELEMENT")
        is_relationship: Whether this is relationship data

    Returns:
        Standardized filename with .parquet extension
    """
    if not self.enable_standardized_filenames and not self.enable_type_prefixes:
      return f"{base_name}.parquet"

    components = []

    if self.enable_type_prefixes:
      prefix = "rel_" if is_relationship else "node_"
      components.append(prefix)

    components.append(base_name)

    if self.enable_standardized_filenames and self.sec_filer and self.sec_report:
      date_str = getattr(self.sec_report, "filing_date", "").replace("-", "")
      cik = getattr(self.sec_filer, "cik", "unknown")
      if date_str and cik != "unknown":
        components.extend([date_str, cik])

    return "_".join(components) + ".parquet"

  def _fix_column_types_by_schema(
    self, df: pd.DataFrame, schema_name: str
  ) -> pd.DataFrame:
    """
    Fix column types for specific tables based on schema name.

    Args:
        df: DataFrame to fix
        schema_name: Schema table name

    Returns:
        DataFrame with fixed column types
    """
    if schema_name == "Entity":
      string_columns = [
        "ein",
        "tax_id",
        "ticker",
        "exchange",
        "name",
        "legal_name",
        "industry",
        "entity_type",
        "sic",
        "sic_description",
        "category",
        "state_of_incorporation",
        "fiscal_year_end",
        "lei",
        "phone",
        "website",
        "uri",
        "scheme",
        "cik",
        "status",
        "parent_entity_id",
      ]
      for col in string_columns:
        if col in df.columns:
          if col in ["ein", "tax_id"]:
            df[col] = df[col].apply(
              lambda x: str(int(x)).zfill(9)
              if pd.notna(x) and str(x).strip() != ""
              else None
            )
          df[col] = df[col].astype("object")

    elif schema_name == "Unit":
      string_columns = ["numerator_uri", "denominator_uri", "uri", "measure", "value"]
      for col in string_columns:
        if col in df.columns:
          df[col] = df[col].astype("object")

    elif schema_name == "Report":
      string_columns = [
        "name",
        "uri",
        "accession_number",
        "form",
        "xbrl_processor_version",
      ]
      for col in string_columns:
        if col in df.columns:
          df[col] = df[col].astype("object")

    elif schema_name == "Association":
      if "weight" in df.columns:
        df["weight"] = df["weight"].astype("float64")

    return df

  def _fix_column_types_by_filename(
    self, df: pd.DataFrame, filename: str
  ) -> pd.DataFrame:
    """
    Fix column types for specific tables based on filename.

    Args:
        df: DataFrame to fix
        filename: Filename (e.g., "Entity.parquet")

    Returns:
        DataFrame with fixed column types
    """
    if "Entity" in filename:
      string_columns = [
        "ein",
        "tax_id",
        "ticker",
        "exchange",
        "name",
        "legal_name",
        "industry",
        "entity_type",
        "sic",
        "sic_description",
        "category",
        "state_of_incorporation",
        "fiscal_year_end",
        "lei",
        "phone",
        "website",
        "uri",
        "scheme",
        "cik",
        "status",
        "parent_entity_id",
        "created_at",
        "updated_at",
      ]
      for col in string_columns:
        if col in df.columns:
          if col in ["ein", "tax_id"]:
            df[col] = df[col].apply(
              lambda x: str(int(x)).zfill(9)
              if pd.notna(x) and str(x).strip() != ""
              else None
            )
          else:
            df[col] = df[col].astype("object")

    elif "Unit" in filename:
      string_columns = ["numerator_uri", "denominator_uri", "uri", "measure", "value"]
      for col in string_columns:
        if col in df.columns:
          df[col] = df[col].fillna("").astype("object")
          df.loc[df[col] == "", col] = None

    elif "Report" in filename:
      string_columns = [
        "name",
        "uri",
        "accession_number",
        "form",
        "xbrl_processor_version",
        "filing_date",
        "report_date",
        "acceptance_date",
        "period_end_date",
        "updated_at",
      ]
      for col in string_columns:
        if col in df.columns:
          df[col] = df[col].fillna("").astype("object")
          df.loc[df[col] == "", col] = None

    elif "Fact" in filename:
      string_columns = [
        "identifier",
        "uri",
        "value",
        "fact_type",
        "decimals",
        "value_type",
        "content_type",
      ]
      for col in string_columns:
        if col in df.columns:
          df[col] = df[col].fillna("").astype("object")
          df.loc[df[col] == "", col] = None

    elif "Association" in filename:
      if "weight" in df.columns:
        df["weight"] = df["weight"].astype("float64")

    return df

  def _is_relationship_filename(self, base_name: str) -> bool:
    """
    Determine if a filename represents relationship data.

    Args:
        base_name: Base filename without extension

    Returns:
        True if this is a relationship table
    """
    relationship_patterns = [
      "entity_reports",
      "report_facts",
      "report_fact_sets",
      "report_taxonomies",
      "fact_units",
      "fact_has_dimension_rel",
      "fact_entities",
      "fact_elements",
      "fact_periods",
      "fact_set_facts",
      "element_labels",
      "element_references",
      "structure_taxonomies",
      "taxonomy_labels",
      "taxonomy_references",
      "structure_associations",
      "association_from_elements",
      "association_to_elements",
      "fact_dimension_elements",
    ]
    return base_name in relationship_patterns

  def _write_all_dataframes_fallback(self, processor):
    """
    Fallback method to write all DataFrames without schema adapter.

    Args:
        processor: XBRLGraphProcessor instance
    """
    self.write_dataframe(processor.entities_df, "nodes/Entity.parquet")
    self.write_dataframe(processor.reports_df, "nodes/Report.parquet")
    self.write_dataframe(processor.facts_df, "nodes/Fact.parquet")
    self.write_dataframe(processor.units_df, "nodes/Unit.parquet")
    self.write_dataframe(processor.fact_dimensions_df, "nodes/FactDimension.parquet")
    self.write_dataframe(processor.elements_df, "nodes/Element.parquet")
    self.write_dataframe(processor.labels_df, "nodes/Label.parquet")
    self.write_dataframe(processor.references_df, "nodes/Reference.parquet")
    self.write_dataframe(processor.structures_df, "nodes/Structure.parquet")
    self.write_dataframe(processor.associations_df, "nodes/Association.parquet")
    self.write_dataframe(processor.periods_df, "nodes/Period.parquet")
    self.write_dataframe(processor.taxonomies_df, "nodes/Taxonomy.parquet")
    self.write_dataframe(processor.fact_sets_df, "nodes/FactSet.parquet")
    self.write_dataframe(processor.taxonomy_labels_df, "nodes/Label.parquet")
    self.write_dataframe(processor.taxonomy_references_df, "nodes/Reference.parquet")

    self.write_dataframe(
      processor.entity_reports_df, "relationships/ENTITY_HAS_REPORT.parquet"
    )
    self.write_dataframe(
      processor.report_facts_df, "relationships/REPORT_HAS_FACT.parquet"
    )
    self.write_dataframe(
      processor.report_fact_sets_df, "relationships/REPORT_HAS_FACT_SET.parquet"
    )
    self.write_dataframe(
      processor.report_uses_taxonomy_df, "relationships/REPORT_USES_TAXONOMY.parquet"
    )
    self.write_dataframe(processor.fact_units_df, "relationships/FACT_HAS_UNIT.parquet")

    if (
      hasattr(processor, "fact_has_dimension_rel_df")
      and not processor.fact_has_dimension_rel_df.empty
    ):
      self.write_dataframe(
        processor.fact_has_dimension_rel_df, "relationships/FACT_HAS_DIMENSION.parquet"
      )

    self.write_dataframe(
      processor.fact_entities_df, "relationships/FACT_HAS_ENTITY.parquet"
    )
    self.write_dataframe(
      processor.fact_elements_df, "relationships/FACT_HAS_ELEMENT.parquet"
    )
    self.write_dataframe(
      processor.fact_periods_df, "relationships/FACT_HAS_PERIOD.parquet"
    )
    self.write_dataframe(
      processor.fact_set_contains_facts_df,
      "relationships/FACT_SET_CONTAINS_FACT.parquet",
    )
    self.write_dataframe(
      processor.element_labels_df, "relationships/ELEMENT_HAS_LABEL.parquet"
    )
    self.write_dataframe(
      processor.element_references_df, "relationships/ELEMENT_HAS_REFERENCE.parquet"
    )
    self.write_dataframe(
      processor.structure_taxonomies_df, "relationships/STRUCTURE_HAS_TAXONOMY.parquet"
    )
    self.write_dataframe(
      processor.taxonomy_labels_df, "relationships/TAXONOMY_HAS_LABEL.parquet"
    )
    self.write_dataframe(
      processor.taxonomy_references_df, "relationships/TAXONOMY_HAS_REFERENCE.parquet"
    )
    self.write_dataframe(
      processor.structure_associations_df,
      "relationships/STRUCTURE_HAS_ASSOCIATION.parquet",
    )
    self.write_dataframe(
      processor.association_from_elements_df,
      "relationships/ASSOCIATION_HAS_FROM_ELEMENT.parquet",
    )
    self.write_dataframe(
      processor.association_to_elements_df,
      "relationships/ASSOCIATION_HAS_TO_ELEMENT.parquet",
    )
    self.write_dataframe(
      processor.fact_dimension_axis_element_rel_df,
      "relationships/FACT_DIMENSION_AXIS_ELEMENT.parquet",
    )
    self.write_dataframe(
      processor.fact_dimension_member_element_rel_df,
      "relationships/FACT_DIMENSION_MEMBER_ELEMENT.parquet",
    )
