"""
XBRL Graph Processing

This package contains components for XBRL to graph transformation,
processing SEC XBRL filings into LadybugDB graph format via parquet files.

Main Components:
- xbrl_graph: XBRLGraphProcessor — one filing to parquet, on xbrlkit's model and projection
- ingestion: XBRLDuckDBGraphProcessor for DuckDB-based graph ingestion
- schema: Schema adapter and configuration generator
- dataframe: DataFrame initialization and management
- parquet: Schema-aware Parquet file output
- textblock: S3 externalization for large text values
- ids: naming utilities (ids are minted by xbrlkit's projection)
"""

from .cache import (
  cache_exists,
  delete_cache_keys,
  download_and_extract,
  zip_and_upload,
)
from .consolidation import (
  atomic_s3_upload,
  consolidate_parquet_from_disk,
  consolidate_parquet_tables_by_date,
  get_quarter_end_date,
)
from .constants import QUARTER_END_DAYS, SHARED_NODE_TABLES
from .dataframe import DataFrameManager
from .ids import (
  camel_to_snake,
  convert_schema_name_to_filename,
  make_plural,
  safe_concat,
)
from .ingestion import (
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
  # Cache helpers
  "cache_exists",
  # Naming utilities
  "camel_to_snake",
  "consolidate_parquet_from_disk",
  "consolidate_parquet_tables_by_date",
  "convert_schema_name_to_filename",
  "create_custom_ingestion_processor",
  "create_roboledger_ingestion_processor",
  "delete_cache_keys",
  "download_and_extract",
  "get_quarter_end_date",
  "make_plural",
  "process_single_filing_to_memory",
  "safe_concat",
  "zip_and_upload",
]
