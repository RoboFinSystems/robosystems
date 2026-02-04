"""
XBRL Graph Ingestion Processor (DuckDB-Based).

This package provides a decoupled ingestion approach using DuckDB staging tables
with persistent storage for independent retry of staging and materialization.

Architecture:
- **Stage 1 (DuckDBStager)**: Schema-driven table creation via glob patterns
- **Stage 2 (LadybugMaterializer)**: Schema-driven materialization to LadybugDB
- **Alternative (LadybugDirectCopier)**: Direct S3 → LadybugDB copy (bypasses DuckDB)

Key benefits of decoupled stages:
- If LadybugDB materialization fails, don't lose 2+ hours of DuckDB staging work
- Staging persists to disk, enabling independent retry of materialization
- Schema-driven: table names come from robosystems.schemas (no manifest needed)

Modules:
    models: Result dataclasses and configuration constants
    staging: DuckDB staging operations (DuckDBStager)
    materialization: LadybugDB materialization operations (LadybugMaterializer)
    direct_copy: Direct S3 → LadybugDB copy operations (LadybugDirectCopier)

Classes:
    XBRLDuckDBGraphProcessor: Unified processor combining staging and materialization
                              (for backward compatibility with existing callers)
    DuckDBStager: Handles DuckDB staging operations only
    LadybugMaterializer: Handles LadybugDB materialization only
    LadybugDirectCopier: Handles direct S3 → LadybugDB copy (alternative workflow)

Usage:
    # New recommended usage - separate concerns
    from robosystems.adapters.sec.processors.ingestion import DuckDBStager, LadybugMaterializer

    stager = DuckDBStager(graph_id="sec")
    staging_result = await stager.stage_to_duckdb()

    materializer = LadybugMaterializer(graph_id="sec")
    materialize_result = await materializer.materialize_from_duckdb()

    # Alternative: Direct S3 → LadybugDB copy (bypasses DuckDB staging)
    from robosystems.adapters.sec.processors.ingestion import LadybugDirectCopier

    copier = LadybugDirectCopier(graph_id="sec")
    copy_result = await copier.copy_all_tables(client)

    # Backward compatible usage - unified interface
    from robosystems.adapters.sec.processors.ingestion import XBRLDuckDBGraphProcessor

    processor = XBRLDuckDBGraphProcessor(graph_id="sec")
    staging_result = await processor.stage_to_duckdb()
    materialize_result = await processor.materialize_from_duckdb()
"""

from .direct_copy import (
  LARGE_TABLES_FOR_BATCHING,
  DirectCopyResult,
  LadybugDirectCopier,
)
from .materialization import LadybugMaterializer
from .models import (
  # Constants (for callers who need to reference them)
  CHUNKED_MATERIALIZATION_TIMEOUT,
  DEFAULT_MATERIALIZATION_TIMEOUT,
  DEFAULT_STAGING_TIMEOUT,
  ENTITY_UPDATE_BATCH_SIZE,
  ENTITY_UPDATE_TIMEOUT,
  INCREMENTAL_COPY_TIMEOUT,
  LARGE_MATERIALIZATION_TIMEOUT,
  LARGE_STAGING_TABLES,
  LARGE_TABLE_STAGING_TIMEOUT,
  MATERIALIZATION_BATCH_SIZE,
  STAGING_MAX_RETRIES,
  STAGING_RETRY_BACKOFF_BASE,
  TAXONOMY_STRUCTURE_TABLES,
  # Result models
  EntityUpdateResult,
  MaterializeResult,
  ProgressCallback,
  StagingResult,
  TableInfo,
  # Helper functions
  get_materialization_timeout,
  get_staging_timeout,
  make_progress_logger,
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
  "ENTITY_UPDATE_BATCH_SIZE",
  "ENTITY_UPDATE_TIMEOUT",
  "INCREMENTAL_COPY_TIMEOUT",
  "LARGE_MATERIALIZATION_TIMEOUT",
  # Constants (commonly referenced)
  "LARGE_STAGING_TABLES",
  "LARGE_TABLES_FOR_BATCHING",
  "LARGE_TABLE_STAGING_TIMEOUT",
  "MATERIALIZATION_BATCH_SIZE",
  "STAGING_MAX_RETRIES",
  "STAGING_RETRY_BACKOFF_BASE",
  "TAXONOMY_STRUCTURE_TABLES",
  # Result models
  "DirectCopyResult",
  # Main processor classes
  "DuckDBStager",  # Stage 1: DuckDB staging
  "EntityUpdateResult",
  "LadybugDirectCopier",  # Alternative: Direct S3 → LadybugDB copy
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
  "s3_url_exists",
]
