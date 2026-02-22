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

from .consolidation import (
  atomic_s3_upload,
  consolidate_parquet_from_disk,
  consolidate_parquet_tables_by_date,
  get_quarter_end_date,
)
from .constants import QUARTER_END_DAYS, SHARED_NODE_TABLES
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
from .ingestion import (
  EntityUpdateResult,
  MaterializeResult,
  StagingResult,
  TableInfo,
  XBRLDuckDBGraphProcessor,
)
from .metadata import SECMetadataLoader
from .parquet import ParquetWriter
from .processing import ProcessedFilingResult, process_single_filing_to_memory
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
  # Constants
  "QUARTER_END_DAYS",
  "SHARED_NODE_TABLES",
  "XBRL_GRAPH_PROCESSOR_VERSION",
  # DataFrame management
  "DataFrameManager",
  # Entity update result
  "EntityUpdateResult",
  "IngestTableInfo",
  # Staging result models
  "MaterializeResult",
  # Parquet file output
  "ParquetWriter",
  # Filing processing
  "ProcessedFilingResult",
  # Metadata loading
  "SECMetadataLoader",
  "SchemaIngestConfig",
  "StagingResult",
  "TableInfo",
  # S3 externalization
  "TextBlockExternalizer",
  # DuckDB ingestion
  "XBRLDuckDBGraphProcessor",
  # Graph processing
  "XBRLGraphProcessor",
  # Schema utilities
  "XBRLSchemaAdapter",
  "XBRLSchemaConfigGenerator",
  # Consolidation functions
  "atomic_s3_upload",
  # Naming utilities
  "camel_to_snake",
  "consolidate_parquet_from_disk",
  "consolidate_parquet_tables_by_date",
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
  "get_quarter_end_date",
  "make_plural",
  "process_single_filing_to_memory",
  "safe_concat",
]
