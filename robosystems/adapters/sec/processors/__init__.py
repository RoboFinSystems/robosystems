"""
XBRL Graph Processing

This package contains components for XBRL to graph transformation,
processing SEC XBRL filings into LadybugDB graph format via parquet files.

Main Components:
- xbrl_graph: Core XBRLGraphProcessor for XBRL to graph transformation
- ingestion: XBRLDuckDBGraphProcessor for DuckDB-based graph ingestion
- schema: Schema adapter and configuration generator
- dataframe: DataFrame initialization and management
- parquet: Schema-aware Parquet file output
- textblock: S3 externalization for large text values
- ids: UUID generation and naming utilities
"""

from .ids import (
  # ID generation
  create_element_id,
  create_label_id,
  create_taxonomy_id,
  create_reference_id,
  create_report_id,
  create_fact_id,
  create_entity_id,
  create_period_id,
  create_unit_id,
  create_factset_id,
  create_dimension_id,
  create_structure_id,
  # Naming utilities
  camel_to_snake,
  make_plural,
  convert_schema_name_to_filename,
  safe_concat,
)
from .dataframe import DataFrameManager
from .parquet import ParquetWriter
from .textblock import TextBlockExternalizer
from .xbrl_graph import XBRLGraphProcessor, XBRL_GRAPH_PROCESSOR_VERSION
from .schema import (
  XBRLSchemaAdapter,
  XBRLSchemaConfigGenerator,
  SchemaIngestConfig,
  IngestTableInfo,
  create_roboledger_ingestion_processor,
  create_custom_ingestion_processor,
)
from .ingestion import XBRLDuckDBGraphProcessor

__all__ = [
  # ID utilities
  "create_element_id",
  "create_label_id",
  "create_taxonomy_id",
  "create_reference_id",
  "create_report_id",
  "create_fact_id",
  "create_entity_id",
  "create_period_id",
  "create_unit_id",
  "create_factset_id",
  "create_dimension_id",
  "create_structure_id",
  # Naming utilities
  "camel_to_snake",
  "make_plural",
  "convert_schema_name_to_filename",
  "safe_concat",
  # DataFrame management
  "DataFrameManager",
  # Parquet file output
  "ParquetWriter",
  # S3 externalization
  "TextBlockExternalizer",
  # Graph processing
  "XBRLGraphProcessor",
  "XBRL_GRAPH_PROCESSOR_VERSION",
  # Schema utilities
  "XBRLSchemaAdapter",
  "XBRLSchemaConfigGenerator",
  "SchemaIngestConfig",
  "IngestTableInfo",
  "create_roboledger_ingestion_processor",
  "create_custom_ingestion_processor",
  # DuckDB ingestion
  "XBRLDuckDBGraphProcessor",
]
