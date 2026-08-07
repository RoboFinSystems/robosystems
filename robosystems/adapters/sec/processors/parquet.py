"""
XBRL Parquet File Writer

Handles all Parquet file I/O operations with schema validation, type fixes,
and standardized filename generation for XBRL graph data.
"""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from robosystems.adapters.sec.processors.ids import (
  convert_schema_name_to_filename,
)
from robosystems.logger import logger


def _convert_to_string_dtype(df: pd.DataFrame, col: str) -> pd.DataFrame:
  """Force a column to pyarrow-backed string dtype, preserving NULLs.

  An all-NULL object column writes to Parquet as the `null` type, which makes
  DuckDB fail with a schema mismatch when it reads that file alongside files
  where the same column is a string.
  """
  df[col] = df[col].astype(pd.ArrowDtype(pa.string()))
  return df


class ParquetWriter:
  """
  Writes the processor's DataFrames out as parquet: creates the `nodes/` and
  `relationships/` tree, applies per-table column type fixes, fills in missing
  schema columns, dedupes, and generates the filenames.
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
    """Configure output.

    `enable_standardized_filenames` folds filing metadata into each filename;
    `enable_type_prefixes` prefixes them with `node_` / `rel_`. `sec_filer` and
    `sec_report` supply that filing metadata.
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
    """Write every DataFrame on `processor` into `nodes/` or `relationships/`.

    `schema_to_dataframe_mapping` comes from
    `DataFrameManager.create_dynamic_dataframe_mapping()`.
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
    """Save one DataFrame, routing it by its schema name (e.g. 'Entity')."""
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
      filepath.parent.mkdir(parents=True, exist_ok=True)

      logger.debug(
        f"Saving {schema_name} to {final_filename}: {len(df)} rows, {len(df.columns)} columns"
      )
      logger.debug(f"Columns in {schema_name}: {list(df.columns)}")

      table = pa.Table.from_pandas(df, preserve_index=False)
      with open(filepath, "wb") as f:
        pq.write_table(table, f)

      if final_filename != filename:
        logger.info(f"📝 Standardized filename: {filename} -> {final_filename}")
      logger.debug(f"Saved {len(df)} rows to {filepath}")
    else:
      logger.debug(f"Skipping empty DataFrame for {filename}")

  def write_dataframe(self, df: pd.DataFrame, filename: str):
    """Save a DataFrame by filename ("nodes/Entity.parquet"), deriving the
    table name from the file stem. Fallback for callers without a schema name.
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
      filepath.parent.mkdir(parents=True, exist_ok=True)

      logger.info(
        f"Saving {table_name} to {final_filename}: {len(df)} rows, {len(df.columns)} columns"
      )
      logger.debug(f"Columns in {table_name}: {list(df.columns)}")

      table = pa.Table.from_pandas(df, preserve_index=False)
      with open(filepath, "wb") as f:
        pq.write_table(table, f)

      if final_filename != filename:
        logger.info(f"📝 Standardized filename: {filename} -> {final_filename}")
      logger.debug(f"Saved {len(df)} rows to {filepath}")
    else:
      logger.debug(f"Skipping empty DataFrame for {filename}")

  def generate_standardized_filename(
    self, base_name: str, is_relationship: bool = False
  ) -> str:
    """Build the `.parquet` filename for a table.

    Plain `{base_name}.parquet` unless type prefixes or standardized filenames
    are enabled, in which case the filing date and CIK are folded in.
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
    """Coerce per-table columns to stable dtypes, keyed by schema name."""
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
              lambda x: (
                str(int(x)).zfill(9) if pd.notna(x) and str(x).strip() != "" else None
              )
            )
          _convert_to_string_dtype(df, col)

    elif schema_name == "Unit":
      string_columns = ["numerator_uri", "denominator_uri", "uri", "measure", "value"]
      for col in string_columns:
        if col in df.columns:
          _convert_to_string_dtype(df, col)

    elif schema_name == "Report":
      string_columns = [
        "name",
        "uri",
        "accession_number",
        "form",
        "xbrl_processor_version",
        "filing_date",
        "report_date",
        "acceptance_date",
      ]
      for col in string_columns:
        if col in df.columns:
          _convert_to_string_dtype(df, col)

    elif schema_name == "Association":
      if "weight" in df.columns:
        df["weight"] = df["weight"].astype("float64")

    return df

  def _fix_column_types_by_filename(
    self, df: pd.DataFrame, filename: str
  ) -> pd.DataFrame:
    """Coerce per-table columns to stable dtypes, keyed by filename."""
    if "Entity" in filename:
      string_columns = [
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
        # Ensure column exists (create with None if missing)
        if col not in df.columns:
          df[col] = None
        # Convert to explicit string dtype to avoid parquet type inference issues
        if col == "tax_id":
          df[col] = df[col].apply(
            lambda x: (
              str(int(x)).zfill(9) if pd.notna(x) and str(x).strip() != "" else None
            )
          )
        # Use pyarrow-backed string type for proper Parquet handling
        _convert_to_string_dtype(df, col)

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
    """True when the extension-less filename names a relationship table."""
    relationship_patterns = [
      "entity_reports",
      "report_facts",
      "structure_fact_sets",
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
      "dimension_has_axis_element",
      "dimension_has_member_element",
    ]
    return base_name in relationship_patterns

  def _write_all_dataframes_fallback(self, processor):
    """Write the DataFrames by hardcoded path, when no schema adapter exists."""
    self.write_dataframe(processor.entities_df, "nodes/Entity.parquet")
    self.write_dataframe(processor.reports_df, "nodes/Report.parquet")
    self.write_dataframe(processor.facts_df, "nodes/Fact.parquet")
    self.write_dataframe(processor.units_df, "nodes/Unit.parquet")
    self.write_dataframe(processor.dimensions_df, "nodes/Dimension.parquet")
    self.write_dataframe(processor.elements_df, "nodes/Element.parquet")
    self.write_dataframe(processor.labels_df, "nodes/Label.parquet")
    self.write_dataframe(processor.references_df, "nodes/Reference.parquet")
    self.write_dataframe(processor.structures_df, "nodes/Structure.parquet")
    self.write_dataframe(processor.associations_df, "nodes/Association.parquet")
    self.write_dataframe(processor.periods_df, "nodes/Period.parquet")
    self.write_dataframe(processor.taxonomies_df, "nodes/Taxonomy.parquet")
    # FactSet nodes are written by classify_associations()
    self.write_dataframe(processor.taxonomy_labels_df, "nodes/Label.parquet")
    self.write_dataframe(processor.taxonomy_references_df, "nodes/Reference.parquet")

    self.write_dataframe(
      processor.entity_reports_df, "relationships/ENTITY_HAS_REPORT.parquet"
    )
    self.write_dataframe(
      processor.report_facts_df, "relationships/REPORT_HAS_FACT.parquet"
    )
    # FactSets and STRUCTURE_HAS_FACT_SET are written by classify_associations()
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
    # FACT_SET_CONTAINS_FACT is written by classify_associations()
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
      processor.dimension_has_axis_element_rel_df,
      "relationships/DIMENSION_HAS_AXIS_ELEMENT.parquet",
    )
    self.write_dataframe(
      processor.dimension_has_member_element_rel_df,
      "relationships/DIMENSION_HAS_MEMBER_ELEMENT.parquet",
    )
