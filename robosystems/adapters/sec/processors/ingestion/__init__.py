"""
XBRL Graph Ingestion Processor (DuckDB-Based).

Two decoupled stages, so a failed LadybugDB materialization doesn't discard
2+ hours of DuckDB staging work:

- **Stage 1 (`staging.DuckDBStager`)** — creates DuckDB tables from S3 parquet
  via glob patterns, persisted to disk so stage 2 can be retried on its own.
- **Stage 2 (`materialization.LadybugMaterializer`)** — materializes those
  tables into LadybugDB.

Both are schema-driven: table names come from `robosystems.schemas`, so there
is no manifest to keep in sync. `models` holds the result dataclasses, timeout
constants, and shared helpers.

Usage:
    from robosystems.adapters.sec.processors.ingestion import (
        DuckDBStager,
        LadybugMaterializer,
    )

    stager = DuckDBStager(graph_id="sec")
    staging_result = await stager.stage_to_duckdb()

    materializer = LadybugMaterializer(graph_id="sec")
    materialize_result = await materializer.materialize_from_duckdb()

`XBRLDuckDBGraphProcessor` subclasses both and exposes the same methods on one
object, for callers that run the two stages together.
"""

from .materialization import LadybugMaterializer
from .models import (
  # Constants (for callers who need to reference them)
  CHUNKED_MATERIALIZATION_TIMEOUT,
  DEFAULT_MATERIALIZATION_TIMEOUT,
  DEFAULT_STAGING_TIMEOUT,
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

# Underscore-prefixed aliases: tests import the timeout helpers by these names.
_get_staging_timeout = get_staging_timeout
_get_materialization_timeout = get_materialization_timeout


class XBRLDuckDBGraphProcessor(DuckDBStager, LadybugMaterializer):
  """
  Both ingestion stages on a single object, for callers that run them together.

  1. Create DuckDB staging tables from processed Parquet files (Stage 1)
  2. Trigger materialization to LadybugDB (Stage 2)

  Both stages drive the Graph API over HTTP — the DuckDB pool lives on the
  Graph API side, not on the worker. Prefer `DuckDBStager` /
  `LadybugMaterializer` directly when only one stage is needed.
  """

  def __init__(self, graph_id: str = "sec", source_prefix: str | None = None):
    """Init both stages against the same graph (`source_prefix`: "sec/processed")."""
    DuckDBStager.__init__(self, graph_id=graph_id, source_prefix=source_prefix)
    LadybugMaterializer.__init__(self, graph_id=graph_id, source_prefix=source_prefix)


# Public API - what callers can import
__all__ = [
  # Timeout constants
  "CHUNKED_MATERIALIZATION_TIMEOUT",
  "DEFAULT_MATERIALIZATION_TIMEOUT",
  "DEFAULT_STAGING_TIMEOUT",
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
  "XBRLDuckDBGraphProcessor",  # Both stages on one object
  # Underscore aliases (imported by tests)
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
