"""Graph ingestion in two decoupled stages: ``DuckDBStager`` stages S3 parquet
into persistent DuckDB tables, and ``LadybugMaterializer`` copies them into
LadybugDB. Staging persists, so a failed materialization retries alone.
"""

from .materialization import LadybugMaterializer
from .models import (
  CHUNKED_MATERIALIZATION_TIMEOUT,
  DEFAULT_MATERIALIZATION_TIMEOUT,
  DEFAULT_STAGING_TIMEOUT,
  LARGE_MATERIALIZATION_TIMEOUT,
  LARGE_STAGING_TABLES,
  LARGE_TABLE_STAGING_TIMEOUT,
  MATERIALIZATION_BATCH_SIZE,
  STAGING_MAX_RETRIES,
  STAGING_RETRY_BACKOFF_BASE,
  MaterializeResult,
  ProgressCallback,
  StagingResult,
  TableInfo,
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
  """Both ingestion stages on one object, for callers that run them together."""

  def __init__(self, graph_id: str = "sec", source_prefix: str | None = None):
    """Init both stages against the same graph (`source_prefix`: "sec/processed")."""
    DuckDBStager.__init__(self, graph_id=graph_id, source_prefix=source_prefix)
    LadybugMaterializer.__init__(self, graph_id=graph_id, source_prefix=source_prefix)


__all__ = [
  "CHUNKED_MATERIALIZATION_TIMEOUT",
  "DEFAULT_MATERIALIZATION_TIMEOUT",
  "DEFAULT_STAGING_TIMEOUT",
  "LARGE_MATERIALIZATION_TIMEOUT",
  "LARGE_STAGING_TABLES",
  "LARGE_TABLE_STAGING_TIMEOUT",
  "MATERIALIZATION_BATCH_SIZE",
  "STAGING_MAX_RETRIES",
  "STAGING_RETRY_BACKOFF_BASE",
  "DuckDBStager",
  "LadybugMaterializer",
  "MaterializeResult",
  "ProgressCallback",
  "StagingResult",
  "TableInfo",
  "XBRLDuckDBGraphProcessor",
  "_get_materialization_timeout",
  "_get_staging_timeout",
  "get_materialization_timeout",
  "get_staging_timeout",
  "make_progress_logger",
  "s3_get_table_patterns",
  "s3_prefix_has_objects",
  "s3_table_data_exists",
  "s3_url_exists",
]
