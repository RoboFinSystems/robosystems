"""
XBRL Graph Ingestion Processor (DuckDB-Based).

This package provides a decoupled ingestion approach using DuckDB staging tables
with persistent storage for independent retry of staging and materialization.

Architecture:
- **Stage 1 (DuckDBStager)**: Schema-driven table creation via glob patterns
- **Stage 2 (LadybugMaterializer)**: Schema-driven materialization to LadybugDB

Key benefits of decoupled stages:
- If LadybugDB materialization fails, don't lose 2+ hours of DuckDB staging work
- Staging persists to disk, enabling independent retry of materialization
- Schema-driven: table names come from robosystems.schemas (no manifest needed)

Modules:
    models: Result dataclasses and configuration constants
    staging: DuckDB staging operations (DuckDBStager)
    materialization: LadybugDB materialization operations (LadybugMaterializer)

Classes:
    XBRLDuckDBGraphProcessor: Unified processor combining staging and materialization
                              (for backward compatibility with existing callers)
    DuckDBStager: Handles DuckDB staging operations only
    LadybugMaterializer: Handles LadybugDB materialization only

Usage:
    # New recommended usage - separate concerns
    from robosystems.adapters.sec.processors.ingestion import DuckDBStager, LadybugMaterializer

    stager = DuckDBStager(graph_id="sec")
    staging_result = await stager.stage_to_duckdb()

    materializer = LadybugMaterializer(graph_id="sec")
    materialize_result = await materializer.materialize_from_duckdb()

    # Backward compatible usage - unified interface
    from robosystems.adapters.sec.processors.ingestion import XBRLDuckDBGraphProcessor

    processor = XBRLDuckDBGraphProcessor(graph_id="sec")
    staging_result = await processor.stage_to_duckdb()
    materialize_result = await processor.materialize_from_duckdb()
"""

from .materialization import LadybugMaterializer
from .models import (
  # Constants (for callers who need to reference them)
  CHUNKED_MATERIALIZATION_TIMEOUT,
  DEFAULT_MATERIALIZATION_TIMEOUT,
  DEFAULT_STAGING_TIMEOUT,
  INCREMENTAL_COPY_TIMEOUT,
  LARGE_MATERIALIZATION_TIMEOUT,
  LARGE_STAGING_TABLES,
  LARGE_TABLE_STAGING_TIMEOUT,
  MATERIALIZATION_BATCH_SIZE,
  STAGING_MAX_RETRIES,
  STAGING_RETRY_BACKOFF_BASE,
  # Result models
  MaterializeResult,
  ProgressCallback,
  StagingResult,
  TableInfo,
  # Helper functions
  get_materialization_timeout,
  get_staging_timeout,
  make_progress_logger,
  s3_get_table_patterns,
  s3_prefix_has_objects,
  s3_table_data_exists,
  s3_url_exists,
)
from .staging import DuckDBStager

# Backward compatibility aliases for underscore-prefixed names
# These were previously private functions but are imported by tests
_get_staging_timeout = get_staging_timeout
_get_materialization_timeout = get_materialization_timeout


class XBRLDuckDBGraphProcessor(DuckDBStager, LadybugMaterializer):
  """
  Unified XBRL graph data processor using DuckDB-based ingestion pattern.

  This class provides backward compatibility for existing callers by combining
  both DuckDBStager (staging operations) and LadybugMaterializer (materialization
  operations) into a single interface.

  For new code, consider using DuckDBStager and LadybugMaterializer directly
  for clearer separation of concerns.

  This processor communicates with the Graph API to:
  1. Create DuckDB staging tables from processed Parquet files (Stage 1)
  2. Trigger materialization to LadybugDB graph database (Stage 2)

  Architecture:
  - Uses Graph API client to communicate with Graph API container
  - DuckDB pool lives on Graph API side, not on worker
  - Supports both full rebuild and incremental update modes
  """

  def __init__(self, graph_id: str = "sec", source_prefix: str | None = None):
    """
    Initialize unified XBRL graph ingestion processor.

    Args:
        graph_id: Graph database identifier (default: "sec")
        source_prefix: S3 prefix for source files (default: "sec/processed")
    """
    # Initialize both parent classes
    DuckDBStager.__init__(self, graph_id=graph_id, source_prefix=source_prefix)
    LadybugMaterializer.__init__(self, graph_id=graph_id, source_prefix=source_prefix)


# Public API - what callers can import
__all__ = [
  # Timeout constants
  "CHUNKED_MATERIALIZATION_TIMEOUT",
  "DEFAULT_MATERIALIZATION_TIMEOUT",
  "DEFAULT_STAGING_TIMEOUT",
  "INCREMENTAL_COPY_TIMEOUT",
  "LARGE_MATERIALIZATION_TIMEOUT",
  # Constants (commonly referenced)
  "LARGE_STAGING_TABLES",
  "LARGE_TABLE_STAGING_TIMEOUT",
  "MATERIALIZATION_BATCH_SIZE",
  "STAGING_MAX_RETRIES",
  "STAGING_RETRY_BACKOFF_BASE",
  # Main processor classes
  "DuckDBStager",  # Stage 1: DuckDB staging
  "LadybugMaterializer",  # Stage 2: LadybugDB materialization
  "MaterializeResult",
  # Callback type
  "ProgressCallback",
  "StagingResult",
  "TableInfo",
  "XBRLDuckDBGraphProcessor",  # Unified (backward compat)
  # Backward compatibility aliases (underscore-prefixed)
  "_get_materialization_timeout",
  "_get_staging_timeout",
  # Helper functions
  "get_materialization_timeout",
  "get_staging_timeout",
  "make_progress_logger",
  "s3_get_table_patterns",
  "s3_prefix_has_objects",
  "s3_table_data_exists",
  "s3_url_exists",
]
