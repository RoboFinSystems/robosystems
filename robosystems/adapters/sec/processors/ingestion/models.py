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
  """Result from materialize_from_duckdb() or copy_incremental_to_ladybug() operation.

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


@dataclass
class EntityUpdateResult:
  """Result from update_entities_from_s3() operation.

  Contains statistics about Entity node updates using MERGE queries.
  """

  status: str  # "success", "partial", "error", "no_changes"
  entities_checked: int = 0  # Total entities in latest parquet
  entities_updated: int = 0  # Entities with actual changes
  entities_unchanged: int = 0  # Entities with no changes (skipped)
  entities_failed: int = 0  # Entities that failed to update
  duration_ms: float = 0.0
  error: str | None = None

  def to_dict(self) -> dict[str, Any]:
    """Convert to dictionary for metadata output."""
    return {
      "status": self.status,
      "entities_checked": self.entities_checked,
      "entities_updated": self.entities_updated,
      "entities_unchanged": self.entities_unchanged,
      "entities_failed": self.entities_failed,
      "duration_ms": self.duration_ms,
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
# Incremental COPY timeouts:
# - Direct S3 → LadybugDB COPY with ignore_errors, ~200K-400K rows/minute
# - Memory efficient (spill_to_disk), network I/O bound
INCREMENTAL_COPY_TIMEOUT = 600  # 10 min per table - incremental updates
#
# Entity update timeouts:
# - MERGE queries are 40x slower than COPY (~200ms per entity)
# - Only runs for entities with actual changes (typically 50-200 per quarter)
# - 5 min should handle up to ~1500 entity updates (worst case)
ENTITY_UPDATE_TIMEOUT = 300  # 5 min for batch MERGE operations
ENTITY_UPDATE_BATCH_SIZE = 100  # Entities per MERGE query batch

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
  }
)

# Taxonomy structure tables that can be skipped for instance-only mode
# These encode XBRL taxonomy hierarchy (calculation/presentation/definition linkbases)
# which are massive but not needed for basic fact analysis queries.
# Skipping these allows more historical data to fit in the same storage budget.
TAXONOMY_STRUCTURE_TABLES = frozenset(
  {
    # Structure nodes (XBRL presentation/calculation/definition trees)
    "Structure",  # XBRL presentation/calculation structures
    # Association nodes (element-to-element relationships - MASSIVE, larger than Facts)
    "Association",
    # Structure relationships
    "STRUCTURE_HAS_TAXONOMY",  # Structure -> Taxonomy
    "STRUCTURE_HAS_ASSOCIATION",  # Structure -> Association
    "STRUCTURE_HAS_CHILD",  # Structure tree hierarchy
    "STRUCTURE_HAS_PARENT",  # Structure tree hierarchy
    # Association relationships (the real storage hogs)
    "ASSOCIATION_HAS_FROM_ELEMENT",  # Association -> Element (source)
    "ASSOCIATION_HAS_TO_ELEMENT",  # Association -> Element (target)
  }
)

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
