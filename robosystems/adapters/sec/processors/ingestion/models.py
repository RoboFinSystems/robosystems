"""Result types, timeouts, table sets and S3 helpers shared by staging and materialization."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
  from robosystems.operations.aws.s3 import S3Client

from robosystems.logger import logger


@dataclass
class TableInfo:
  name: str
  row_count: int
  file_count: int
  staged_at: str  # ISO timestamp
  skipped: bool = False  # e.g. no files found

  def to_dict(self) -> dict[str, Any]:
    return {
      "name": self.name,
      "row_count": self.row_count,
      "file_count": self.file_count,
      "staged_at": self.staged_at,
      "skipped": self.skipped,
    }

  @classmethod
  def from_dict(cls, data: dict[str, Any]) -> "TableInfo":
    return cls(
      name=data["name"],
      row_count=data["row_count"],
      file_count=data["file_count"],
      staged_at=data["staged_at"],
      skipped=data.get("skipped", False),
    )


@dataclass
class StagingResult:
  status: str  # "success", "partial", "error", "no_data", "already_staged"
  table_names: list[str]  # successfully staged
  tables: dict[str, TableInfo] = field(default_factory=dict)
  total_files: int = 0
  total_rows: int = 0
  duration_ms: float = 0.0
  duckdb_path: str | None = None
  error: str | None = None

  def to_dict(self) -> dict[str, Any]:
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
  status: str  # "success", "partial", "error", "no_data"
  table_names: list[str] = field(default_factory=list)
  failed_tables: list[dict[str, Any]] = field(default_factory=list)
  total_rows_ingested: int = 0  # alias for total_rows
  total_rows: int = 0
  duration_ms: float = 0.0
  tables: list[dict[str, Any]] = field(default_factory=list)
  error: str | None = None

  def to_dict(self) -> dict[str, Any]:
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


# Receives per-table progress messages (Dagster logging).
ProgressCallback = Callable[[str], None]


# Timeouts in seconds, from production SEC runs.
DEFAULT_STAGING_TIMEOUT = 300  # 5 min - small tables (<10M rows)
LARGE_TABLE_STAGING_TIMEOUT = 1800  # 30 min - large tables (Fact: 200M+ rows)
DEFAULT_MATERIALIZATION_TIMEOUT = 600  # 10 min - small/medium tables
LARGE_MATERIALIZATION_TIMEOUT = 3600  # 60 min - direct COPY of 200M+ row tables
CHUNKED_MATERIALIZATION_TIMEOUT = 2400  # 40 min per 20M row batch

# Tables above this row count materialize in batches: a direct COPY of a 200M+
# row table OOMs once the LadybugDB buffer pool is boosted.
MATERIALIZATION_BATCH_SIZE = 20_000_000

STAGING_MAX_RETRIES = 3  # total attempts
STAGING_RETRY_BACKOFF_BASE = 30  # seconds; 30s, 60s, ...

# Tables large enough for extended timeouts and size-checked chunked staging.
LARGE_STAGING_TABLES = frozenset(
  {
    # Nodes
    "Fact",  # ~1B rows (hundreds of facts per filing)
    "Label",  # ~6M rows (multiple labels per element)
    "Element",  # ~10M rows (all XBRL elements across taxonomies)
    "Dimension",  # ~76M rows - Dimensional breakdowns of facts
    "Association",  # ~206M rows - XBRL associations
    "Structure",  # ~7M rows - Presentation/calculation structures
    # Fact relationships
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
    # Shared reference
    "ELEMENT_HAS_LABEL",  # ~34M rows - Element to Label
    "TAXONOMY_HAS_LABEL",  # ~106M rows - Taxonomy to Label
    # Structure/association
    "STRUCTURE_HAS_ASSOCIATION",  # ~200M rows - Structure -> Association
    "ASSOCIATION_HAS_FROM_ELEMENT",  # ~206M rows - Association -> Element
    "ASSOCIATION_HAS_TO_ELEMENT",  # ~206M rows - Association -> Element
    "ASSOCIATION_HAS_CLASSIFICATION",  # ~206M rows - Association -> Classification
  }
)


def get_staging_timeout(table_name: str) -> int:
  if table_name in LARGE_STAGING_TABLES:
    return LARGE_TABLE_STAGING_TIMEOUT
  return DEFAULT_STAGING_TIMEOUT


def get_materialization_timeout(table_name: str) -> float:
  if table_name in LARGE_STAGING_TABLES:
    return float(LARGE_MATERIALIZATION_TIMEOUT)
  return float(DEFAULT_MATERIALIZATION_TIMEOUT)


def make_progress_logger(
  progress_callback: ProgressCallback | None,
) -> ProgressCallback:
  def log_progress(msg: str) -> None:
    logger.info(msg)
    if progress_callback:
      progress_callback(msg)

  return log_progress


def s3_url_exists(s3_client: "S3Client", s3_url: str) -> bool:
  s3_path = s3_url.replace("s3://", "")
  bucket_end = s3_path.find("/")
  bucket = s3_path[:bucket_end]
  key = s3_path[bucket_end + 1 :]
  return s3_client.object_exists(bucket, key)


def s3_prefix_has_objects(s3_client: "S3Client", bucket: str, prefix: str) -> bool:
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
  """Whether table data exists as `{table_name}.parquet` or `{table_name}/*.parquet`."""
  base = f"{source_prefix}/{filed_pattern}/{entity_type}/{table_name}"

  if s3_client.object_exists(bucket, f"{base}.parquet"):
    return True

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
  """S3 patterns for the table layouts that exist (possibly none).

  DuckDB errors on a literal (wildcard-free) path that is missing.
  """
  base_key = f"{source_prefix}/{filed_pattern}/{entity_type}/{table_name}"
  base_url = f"s3://{bucket}/{base_key}"
  patterns: list[str] = []

  if s3_client.object_exists(bucket, f"{base_key}.parquet"):
    patterns.append(f"{base_url}.parquet")

  if s3_prefix_has_objects(s3_client, bucket, f"{base_key}/"):
    patterns.append(f"{base_url}/*.parquet")

  return patterns
