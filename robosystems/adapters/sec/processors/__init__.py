"""SEC XBRL filings to graph parquet, and the DuckDB → LadybugDB ingestion of it."""

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
  "QUARTER_END_DAYS",
  "SHARED_NODE_TABLES",
  "XBRL_GRAPH_PROCESSOR_VERSION",
  "DataFrameManager",
  "IngestTableInfo",
  "MaterializeResult",
  "ParquetWriter",
  "ProcessedFilingResult",
  "SECMetadataLoader",
  "SchemaIngestConfig",
  "StagingResult",
  "TableInfo",
  "TextBlockExternalizer",
  "XBRLDuckDBGraphProcessor",
  "XBRLGraphProcessor",
  "XBRLSchemaAdapter",
  "XBRLSchemaConfigGenerator",
  "atomic_s3_upload",
  "cache_exists",
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
