"""
XBRL Processing Utilities

This package contains modular components for XBRL graph processing,
extracted from the monolithic xbrl_graph.py for better maintainability.

Main Components:
- id_utils: UUID generation for XBRL entities
- naming_utils: String conversion utilities (camel_case, pluralization)
- dataframe_manager: DataFrame initialization and management
- parquet_writer: Schema-aware Parquet file output
- textblock_externalizer: S3 externalization for large text values (TODO)
"""

from .id_utils import (
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
)
from .naming_utils import (
  camel_to_snake,
  make_plural,
  convert_schema_name_to_filename,
  safe_concat,
)
from .dataframe_manager import DataFrameManager
from .parquet_writer import ParquetWriter
from .textblock_externalizer import TextBlockExternalizer
from .graph import XBRLGraphProcessor, XBRL_GRAPH_PROCESSOR_VERSION
from .schema_adapter import XBRLSchemaAdapter
from .schema_config_generator import (
  XBRLSchemaConfigGenerator,
  SchemaIngestConfig,
  IngestTableInfo,
  create_roboledger_ingestion_processor,
  create_custom_ingestion_processor,
)
from .duckdb_graph_ingestion import XBRLDuckDBGraphProcessor

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
  "XBRLSchemaAdapter",
  "XBRLSchemaConfigGenerator",
  "SchemaIngestConfig",
  "IngestTableInfo",
  "create_roboledger_ingestion_processor",
  "create_custom_ingestion_processor",
  "XBRLDuckDBGraphProcessor",
]
