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

from .dataframe import DataFrameManager
from .ids import (
  # Naming utilities
  camel_to_snake,
  convert_schema_name_to_filename,
  create_dimension_id,
  # ID generation
  create_element_id,
  create_entity_id,
  create_fact_id,
  create_factset_id,
  create_label_id,
  create_period_id,
  create_reference_id,
  create_report_id,
  create_structure_id,
  create_taxonomy_id,
  create_unit_id,
  make_plural,
  safe_concat,
)
from .ingestion import XBRLDuckDBGraphProcessor
from .parquet import ParquetWriter
from .schema import (
  IngestTableInfo,
  SchemaIngestConfig,
  XBRLSchemaAdapter,
  XBRLSchemaConfigGenerator,
  create_custom_ingestion_processor,
  create_roboledger_ingestion_processor,
)
from .textblock import TextBlockExternalizer
from .xbrl_graph import XBRL_GRAPH_PROCESSOR_VERSION, XBRLGraphProcessor

__all__ = [
  "XBRL_GRAPH_PROCESSOR_VERSION",
  # DataFrame management
  "DataFrameManager",
  "IngestTableInfo",
  # Parquet file output
  "ParquetWriter",
  "SchemaIngestConfig",
  # S3 externalization
  "TextBlockExternalizer",
  # DuckDB ingestion
  "XBRLDuckDBGraphProcessor",
  # Graph processing
  "XBRLGraphProcessor",
  # Schema utilities
  "XBRLSchemaAdapter",
  "XBRLSchemaConfigGenerator",
  # Naming utilities
  "camel_to_snake",
  "convert_schema_name_to_filename",
  "create_custom_ingestion_processor",
  "create_dimension_id",
  # ID utilities
  "create_element_id",
  "create_entity_id",
  "create_fact_id",
  "create_factset_id",
  "create_label_id",
  "create_period_id",
  "create_reference_id",
  "create_report_id",
  "create_roboledger_ingestion_processor",
  "create_structure_id",
  "create_taxonomy_id",
  "create_unit_id",
  "make_plural",
  "safe_concat",
]
