"""
XBRL Ingestion Models and Constants.

This module contains:
- Result dataclasses for staging and materialization operations
- Timeout constants tuned for production workloads
- Table classification sets for special handling (large tables, taxonomy tables, etc.)
- Shared helper functions for staging and materialization
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
  from robosystems.operations.aws.s3 import S3Client

from robosystems.logger import logger

# =============================================================================
# Result Models
# =============================================================================


@dataclass
class TableInfo:
  """Information about a staged table."""

  name: str
  row_count: int
  file_count: int
  staged_at: str  # ISO timestamp
  skipped: bool = False  # True if table was skipped (e.g., no files found)

  def to_dict(self) -> dict[str, Any]:
    """Convert to dictionary for JSON serialization."""
    return {
      "name": self.name,
      "row_count": self.row_count,
      "file_count": self.file_count,
      "staged_at": self.staged_at,
      "skipped": self.skipped,
    }

  @classmethod
  def from_dict(cls, data: dict[str, Any]) -> "TableInfo":
    """Create from dictionary."""
    return cls(
      name=data["name"],
      row_count=data["row_count"],
      file_count=data["file_count"],
      staged_at=data["staged_at"],
      skipped=data.get("skipped", False),
    )


@dataclass
class StagingResult:
  """Result from stage_to_duckdb() operation.

  Contains statistics about the staging operation and the list of
  tables that were successfully staged.
  """

  status: str  # "success", "partial", "error", "no_data", "already_staged"
  table_names: list[str]  # Successfully staged tables
  tables: dict[str, TableInfo] = field(default_factory=dict)
  total_files: int = 0
  total_rows: int = 0
  duration_ms: float = 0.0
  duckdb_path: str | None = None
  error: str | None = None

  def to_dict(self) -> dict[str, Any]:
    """Convert to dictionary for metadata output."""
    return {
      "status": self.status,
      "table_names": self.table_names,
      "tables": {name: info.to_dict() for name, info in self.tables.items()},
      "total_files": self.total_files,
      "total_rows": self.total_rows,
      "duration_ms": self.duration_ms,
      "duckdb_path": self.duckdb_path,
      "error": self.error,
    }


@dataclass
class MaterializeResult:
  """Result from materialize_from_duckdb() operation.

  Contains statistics about the materialization (ingestion) operation.
  """

  status: str  # "success", "partial", "error", "no_data"
  table_names: list[str] = field(default_factory=list)  # Successfully processed tables
  failed_tables: list[dict[str, Any]] = field(
    default_factory=list
  )  # Tables with errors
  total_rows_ingested: int = 0  # Alias for total_rows (backward compat)
  total_rows: int = 0  # Total rows copied/ingested
  duration_ms: float = 0.0
  tables: list[dict[str, Any]] = field(default_factory=list)
  error: str | None = None

  def to_dict(self) -> dict[str, Any]:
    """Convert to dictionary for metadata output."""
    return {
      "status": self.status,
      "table_names": self.table_names,
      "failed_tables": self.failed_tables,
      "total_rows_ingested": self.total_rows_ingested,
      "total_rows": self.total_rows,
      "duration_ms": self.duration_ms,
      "tables": self.tables,
      "error": self.error,
    }


# =============================================================================
# Progress Callback Type
# =============================================================================

# Progress callback type for Dagster logging integration
# Accepts a message string, called during staging/materialization for per-table progress
ProgressCallback = Callable[[str], None]


# =============================================================================
# Timeout Constants (seconds)
# =============================================================================
# These values are based on production testing with SEC data on r7g.medium/large
# instances. Each operation type has different memory and I/O characteristics.
#
# DuckDB staging timeouts:
# - INSERT INTO with S3 parquet reads, ~500K-1M rows/minute for large tables
# - Network I/O bound (S3 → DuckDB), memory usage is bounded
DEFAULT_STAGING_TIMEOUT = 300  # 5 min - small tables (<10M rows)
LARGE_TABLE_STAGING_TIMEOUT = 1800  # 30 min - large tables (Fact: 200M+ rows)
#
# LadybugDB materialization timeouts:
# - Materialize from DuckDB to graph, ~300K-500K rows/minute
# - CPU bound (graph construction), memory scales with batch size
DEFAULT_MATERIALIZATION_TIMEOUT = 600  # 10 min - small/medium tables
LARGE_MATERIALIZATION_TIMEOUT = 3600  # 60 min - direct COPY of 200M+ row tables
CHUNKED_MATERIALIZATION_TIMEOUT = 2400  # 40 min per 20M row batch
#
# Chunked materialization settings for large tables
# RE-ENABLED (2026-01-31): Direct COPY of 200M+ row tables causes OOM on r7g.2xlarge
# with 64GB RAM when LadybugDB buffer pool is boosted. Batching prevents memory
# exhaustion by materializing in chunks with cleanup between batches.
# 20M batches with 30min timeout balances memory vs timeout risk.
# 10M batches were conservative; 20M reduces batch count for large tables.
# Tables larger than this are batched; smaller tables use single COPY.
MATERIALIZATION_BATCH_SIZE = 20_000_000  # 20M rows per batch

# Retry configuration for staging operations
# On timeout or failure, retry the entire table from scratch
STAGING_MAX_RETRIES = 3  # Total attempts (1 initial + 2 retries)
STAGING_RETRY_BACKOFF_BASE = 30  # Base backoff in seconds (30s, 60s, 90s)


# =============================================================================
# Table Classification Sets
# =============================================================================

# Tables known to have millions of rows requiring extended timeouts
LARGE_STAGING_TABLES = frozenset(
  {
    # Large node tables
    "Fact",  # ~1B rows (hundreds of facts per filing)
    "Label",  # ~6M rows (multiple labels per element)
    "Element",  # ~10M rows (all XBRL elements across taxonomies)
    "Dimension",  # ~76M rows - Dimensional breakdowns of facts
    "Association",  # ~206M rows - XBRL associations
    "Structure",  # ~7M rows - Presentation/calculation structures
    # Large relationship tables (fact-related)
    "REPORT_HAS_FACT",  # Report -> Fact (1:many)
    "FACT_HAS_ELEMENT",  # Fact -> Element (high cardinality)
    "FACT_HAS_ENTITY",  # Fact -> Entity
    "FACT_HAS_PERIOD",  # Fact -> Period
    "FACT_HAS_UNIT",  # Fact -> Unit
    "FACT_HAS_DIMENSION",  # Fact -> Dimension
    "FACT_REPORTS_ELEMENT",  # Legacy name for FACT_HAS_ELEMENT
    "FACT_SET_CONTAINS_FACT",  # ~105M rows - FactSet -> Fact (1:1 with Fact)
    "DIMENSION_HAS_MEMBER_ELEMENT",  # ~70M rows - Dimension -> Element
    "DIMENSION_HAS_AXIS_ELEMENT",  # Dimension -> Element (axis)
    # Large relationship tables (shared reference)
    "ELEMENT_HAS_LABEL",  # ~34M rows - Element to Label
    "TAXONOMY_HAS_LABEL",  # ~106M rows - Taxonomy to Label
    # Large relationship tables (structure/association)
    "STRUCTURE_HAS_ASSOCIATION",  # ~200M rows - Structure -> Association
    "ASSOCIATION_HAS_FROM_ELEMENT",  # ~206M rows - Association -> Element
    "ASSOCIATION_HAS_TO_ELEMENT",  # ~206M rows - Association -> Element
    "ASSOCIATION_HAS_CLASSIFICATION",  # ~206M rows - Association -> Classification
  }
)


# Tables whose embedding columns should be NULLed out during DuckDB staging.
# Label and Structure embeddings are only used during enrichment (classification),
# not queried after staging. Element embeddings are kept for the LanceDB index.
# Data stays in parquet source files if future use is needed.
EMBEDDING_NULL_TABLES = frozenset({"Label", "Structure"})

# Tables kept even in instance-only mode (critical for fact exploration):
# - Taxonomy: Single node per report (small)
# - TAXONOMY_HAS_ELEMENT: Links taxonomy to elements (needed for element context)
# - Element: What each fact represents (us-gaap:Revenue, etc.)
# - Label: Human-readable element names
# - ELEMENT_HAS_LABEL: Links Element to Label


# =============================================================================
# Helper Functions
# =============================================================================


def get_staging_timeout(table_name: str) -> int:
  """Get appropriate staging timeout for a table based on expected size."""
  if table_name in LARGE_STAGING_TABLES:
    return LARGE_TABLE_STAGING_TIMEOUT
  return DEFAULT_STAGING_TIMEOUT


def get_materialization_timeout(table_name: str) -> float:
  """Get appropriate materialization timeout for a table based on expected size."""
  if table_name in LARGE_STAGING_TABLES:
    return float(LARGE_MATERIALIZATION_TIMEOUT)
  return float(DEFAULT_MATERIALIZATION_TIMEOUT)


def make_progress_logger(
  progress_callback: ProgressCallback | None,
) -> ProgressCallback:
  """Create a progress logger that logs to both logger and optional callback."""

  def log_progress(msg: str) -> None:
    logger.info(msg)
    if progress_callback:
      progress_callback(msg)

  return log_progress


def s3_url_exists(s3_client: "S3Client", s3_url: str) -> bool:
  """Check if an S3 URL (s3://bucket/key format) exists."""
  s3_path = s3_url.replace("s3://", "")
  bucket_end = s3_path.find("/")
  bucket = s3_path[:bucket_end]
  key = s3_path[bucket_end + 1 :]
  return s3_client.object_exists(bucket, key)


def s3_prefix_has_objects(s3_client: "S3Client", bucket: str, prefix: str) -> bool:
  """Check if any objects exist under an S3 prefix.

  Uses list_objects with max_keys=1 for minimal overhead.

  Args:
      s3_client: S3Client instance
      bucket: S3 bucket name
      prefix: S3 key prefix to check

  Returns:
      True if at least one object exists under the prefix
  """
  objects = s3_client.list_objects(bucket, prefix=prefix, max_keys=1)
  return len(objects) > 0


def s3_table_data_exists(
  s3_client: "S3Client",
  bucket: str,
  source_prefix: str,
  filed_pattern: str,
  entity_type: str,
  table_name: str,
) -> bool:
  """Check if table data exists in either old or new S3 format.

  Checks both formats:
  - Old: {source_prefix}/{filed_pattern}/{entity_type}/{table_name}.parquet
  - New: {source_prefix}/{filed_pattern}/{entity_type}/{table_name}/*.parquet

  Args:
      s3_client: S3Client instance
      bucket: S3 bucket name
      source_prefix: Base prefix (e.g. "sec/processed")
      filed_pattern: Filing partition (e.g. "filed=2024-Q1")
      entity_type: Entity type (e.g. "nodes", "relationships")
      table_name: Table name (e.g. "Element")

  Returns:
      True if data exists in either format
  """
  base = f"{source_prefix}/{filed_pattern}/{entity_type}/{table_name}"

  # Check old format: TABLE.parquet
  if s3_client.object_exists(bucket, f"{base}.parquet"):
    return True

  # Check new format: TABLE/*.parquet (any part file under the directory)
  if s3_prefix_has_objects(s3_client, bucket, f"{base}/"):
    return True

  return False


def s3_get_table_patterns(
  s3_client: "S3Client",
  bucket: str,
  source_prefix: str,
  filed_pattern: str,
  entity_type: str,
  table_name: str,
) -> list[str]:
  """Get S3 URL patterns for table data that actually exists.

  Returns only patterns for formats where data is present, preventing
  DuckDB errors from literal paths that don't exist. DuckDB treats paths
  without wildcards as literal files and errors if they're missing.

  Args:
      s3_client: S3Client instance
      bucket: S3 bucket name
      source_prefix: Base prefix (e.g. "sec/processed")
      filed_pattern: Filing partition (e.g. "filed=2024-Q1")
      entity_type: Entity type (e.g. "nodes", "relationships")
      table_name: Table name (e.g. "Element")

  Returns:
      List of S3 URL patterns (may be empty if no data exists)
  """
  base_key = f"{source_prefix}/{filed_pattern}/{entity_type}/{table_name}"
  base_url = f"s3://{bucket}/{base_key}"
  patterns: list[str] = []

  # Check old format: TABLE.parquet (literal path — must exist to include)
  if s3_client.object_exists(bucket, f"{base_key}.parquet"):
    patterns.append(f"{base_url}.parquet")

  # Check new format: TABLE/*.parquet (glob — DuckDB handles empty match)
  if s3_prefix_has_objects(s3_client, bucket, f"{base_key}/"):
    patterns.append(f"{base_url}/*.parquet")

  return patterns
