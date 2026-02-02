"""
XBRL Graph Ingestion Processor (DuckDB-Based).

This module provides a decoupled ingestion approach using DuckDB staging tables
with persistent storage for independent retry of staging and materialization.

Architecture:
- **Stage 1 (stage_to_duckdb)**: Schema-driven table creation via glob patterns
- **Stage 2 (materialize_from_duckdb)**: Schema-driven materialization to LadybugDB

Key benefits of decoupled stages:
- If LadybugDB materialization fails, don't lose 2+ hours of DuckDB staging work
- Staging persists to disk, enabling independent retry of materialization
- Schema-driven: table names come from robosystems.schemas (no manifest needed)

Glob Pattern Optimization (2026-01-20):
- Uses glob patterns instead of explicit file lists for DuckDB table creation
- DuckDB handles file discovery internally (more efficient than S3 ListObjects)
- union_by_name=true handles schema variations between filings
- Eliminates the bottleneck of passing 17K+ explicit file paths

Schema-Driven Design (2026-01-20):
- Table names derived from RoboLedgerContext.get_all_table_names_for_context()
- No manifest file needed - schema is the source of truth
- Works across services (Dagster/Graph API) without shared filesystem

Flow:
1. stage_to_duckdb(): Get tables from schema → Create DuckDB tables via glob → Persist
2. materialize_from_duckdb(): Get tables from schema → Ingest to LadybugDB

Full Rebuild Paradigm (2026-01-29):
- Pipeline always rebuilds from all S3 parquet files (no incremental mode)
- DuckDB staging and LadybugDB materialization are separate, retry-safe stages
- Memory management handled via explicit API endpoints (boost/restore)

Status: Production - enables independent retry of failed materialization.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
  from robosystems.graph_api.client.client import GraphClient

from robosystems.config import env
from robosystems.config.storage.shared import (
  get_staging_duckdb_path,
)
from robosystems.graph_api.client.factory import get_graph_client
from robosystems.logger import logger
from robosystems.operations.aws.s3 import S3Client
from robosystems.schemas.extensions.roboledger import RoboLedgerContext

# =============================================================================
# Staging Result Models
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

  status: str  # "success", "error", "no_data"
  total_rows_ingested: int = 0
  duration_ms: float = 0.0
  tables: list[dict[str, Any]] = field(default_factory=list)
  error: str | None = None

  def to_dict(self) -> dict[str, Any]:
    """Convert to dictionary for metadata output."""
    return {
      "status": self.status,
      "total_rows_ingested": self.total_rows_ingested,
      "duration_ms": self.duration_ms,
      "tables": self.tables,
      "error": self.error,
    }


# Progress callback type for Dagster logging integration
# Accepts a message string, called during staging/materialization for per-table progress
ProgressCallback = Callable[[str], None]

# Table-specific timeouts for DuckDB staging (seconds)
# For chunked operations (quarter-by-quarter), each chunk is smaller so use shorter timeout
# Default: 300s (5 min), Large non-chunked: 1800s (30 min), Large chunked: 600s (10 min)
DEFAULT_STAGING_TIMEOUT = 300  # 5 minutes
LARGE_TABLE_STAGING_TIMEOUT = 1800  # 30 minutes (for non-chunked large tables)
CHUNKED_STAGING_TIMEOUT = 600  # 10 minutes per quarter chunk

# Table-specific timeouts for LadybugDB materialization (seconds)
# For batched operations, each batch is smaller so use shorter timeout
# Default: 600s (10 min), Large non-chunked: 3600s (60 min), Batched: 1800s (30 min)
DEFAULT_MATERIALIZATION_TIMEOUT = 600  # 10 minutes
LARGE_MATERIALIZATION_TIMEOUT = 3600  # 60 minutes (for direct COPY of 200M+ row tables)
CHUNKED_MATERIALIZATION_TIMEOUT = (
  2400  # 40 minutes per 20M row batch - increased for larger batch size
)

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

# Tables known to have millions of rows requiring extended timeouts
LARGE_STAGING_TABLES = frozenset(
  {
    # Large node tables
    "Fact",  # ~1B rows (hundreds of facts per filing)
    "Label",  # ~6M rows (multiple labels per element)
    "Element",  # ~10M rows (all XBRL elements across taxonomies)
    "FactDimension",  # ~76M rows - Dimensional breakdowns of facts
    "Association",  # ~206M rows - XBRL associations
    "Structure",  # ~7M rows - Presentation/calculation structures
    # Large relationship tables (fact-related)
    "REPORT_HAS_FACT",  # Report -> Fact (1:many)
    "FACT_HAS_ELEMENT",  # Fact -> Element (high cardinality)
    "FACT_HAS_ENTITY",  # Fact -> Entity
    "FACT_HAS_PERIOD",  # Fact -> Period
    "FACT_HAS_UNIT",  # Fact -> Unit
    "FACT_HAS_DIMENSION",  # Fact -> FactDimension
    "FACT_REPORTS_ELEMENT",  # Legacy name for FACT_HAS_ELEMENT
    "FACT_SET_CONTAINS_FACT",  # ~105M rows - FactSet -> Fact (1:1 with Fact)
    "FACT_DIMENSION_MEMBER_ELEMENT",  # ~70M rows - FactDimension -> Element
    "FACT_DIMENSION_AXIS_ELEMENT",  # FactDimension -> Element (axis)
    # Large relationship tables (shared reference)
    "ELEMENT_HAS_LABEL",  # ~34M rows - Element to Label
    "TAXONOMY_HAS_LABEL",  # ~106M rows - Taxonomy to Label
    # Large relationship tables (structure/association)
    "STRUCTURE_HAS_ASSOCIATION",  # ~200M rows - Structure -> Association
    "ASSOCIATION_HAS_FROM_ELEMENT",  # ~206M rows - Association -> Element
    "ASSOCIATION_HAS_TO_ELEMENT",  # ~206M rows - Association -> Element
  }
)

# Tables safe to chunk by quarter (unique per filing, no cross-quarter duplicates)
# These tables have data that is specific to individual filings, so loading
# quarter-by-quarter won't create duplicates.
#
# Chunking threshold: Only chunk tables >100M rows. Full staging works reliably
# up to ~106M rows (TAXONOMY_HAS_LABEL completes in 90s). Smaller tables have
# unnecessary overhead from 17 sequential CREATE/INSERT operations.
#
# Note: FactSet/REPORT_HAS_FACT_SET excluded - only 1 per report, small tables.
QUARTER_CHUNKABLE_TABLES = frozenset(
  {
    # Fact node (~105M rows) - the core high-volume table
    "Fact",
    # Fact relationships (~94-105M rows each, 1:1 with Fact)
    "REPORT_HAS_FACT",
    "FACT_HAS_ELEMENT",
    "FACT_HAS_ENTITY",
    "FACT_HAS_PERIOD",
    "FACT_HAS_UNIT",
    "FACT_HAS_DIMENSION",
    "FACT_SET_CONTAINS_FACT",
    "FACT_DIMENSION_MEMBER_ELEMENT",  # ~70M rows - borderline but safer to chunk
    "FACT_REPORTS_ELEMENT",  # Legacy name for FACT_HAS_ELEMENT
    # Association tables (~200-206M rows) - largest tables, benefit most from chunking
    "Association",
    "STRUCTURE_HAS_ASSOCIATION",
    "ASSOCIATION_HAS_FROM_ELEMENT",
    "ASSOCIATION_HAS_TO_ELEMENT",
  }
)

# Tables removed from chunking (full staging is faster for these sizes):
# - Structure (7M) - well under 100M threshold
# - FactDimension (8M) - well under 100M threshold
# - STRUCTURE_HAS_TAXONOMY (7M) - well under 100M threshold
# - FACT_DIMENSION_AXIS_ELEMENT (8M) - well under 100M threshold

# Tables NOT safe to chunk (shared reference data across filings)
# These tables have data shared across many filings. Chunking would create
# duplicates because the same Element/Label appears in multiple quarters.
# Must be loaded in a single pass with deduplication.
# - Element, Label: Shared XBRL taxonomy elements (same us-gaap:Revenue across filings)
# - ELEMENT_HAS_LABEL, TAXONOMY_HAS_LABEL: Shared element-label mappings

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


def _get_staging_timeout(table_name: str) -> int:
  """Get appropriate staging timeout for a table based on expected size."""
  if table_name in LARGE_STAGING_TABLES:
    return LARGE_TABLE_STAGING_TIMEOUT
  return DEFAULT_STAGING_TIMEOUT


def _get_materialization_timeout(table_name: str) -> float:
  """Get appropriate materialization timeout for a table based on expected size."""
  if table_name in LARGE_STAGING_TABLES:
    return float(LARGE_MATERIALIZATION_TIMEOUT)
  return float(DEFAULT_MATERIALIZATION_TIMEOUT)


class XBRLDuckDBGraphProcessor:
  """
  XBRL graph data processor using DuckDB-based ingestion pattern.

  This processor communicates with the Graph API to:
  1. Create DuckDB staging tables from processed Parquet files
  2. Trigger ingestion to graph database

  Architecture:
  - Uses Graph API client to communicate with Graph API container
  - DuckDB pool lives on Graph API side, not on worker
  - Works directly with processed files (many small files) instead of
    consolidated files to test performance with high file counts
  """

  def __init__(self, graph_id: str = "sec", source_prefix: str | None = None):
    """
    Initialize XBRL graph ingestion processor.

    Args:
        graph_id: Graph database identifier (default: "sec")
        source_prefix: S3 prefix for source files (default: "sec/processed" for new filed= structure)
    """
    self.graph_id = graph_id
    self.s3_client = S3Client()
    self.bucket = env.SHARED_PROCESSED_BUCKET
    # New structure: sec/processed/filed=YYYY-MM-DD/nodes/TABLE/*.parquet
    # Old structure (deprecated): sec/year=YYYY/nodes/TABLE/*.parquet
    self.source_prefix = source_prefix or "sec/processed"

  def _s3_url_exists(self, s3_url: str) -> bool:
    """Check if an S3 URL (s3://bucket/key format) exists."""
    s3_path = s3_url.replace("s3://", "")
    bucket_end = s3_path.find("/")
    bucket = s3_path[:bucket_end]
    key = s3_path[bucket_end + 1 :]
    return self.s3_client.object_exists(bucket, key)

  async def stage_to_duckdb(
    self,
    year: int | None = None,
    reset_staging: bool = False,
    skip_taxonomy_relationships: bool = False,
    use_glob: bool = True,
    progress_callback: ProgressCallback | None = None,
  ) -> StagingResult:
    """
    Stage processed Parquet files to persistent DuckDB.

    This is Stage 1 of the decoupled pipeline. It ONLY stages data to DuckDB.
    LadybugDB operations are handled separately by materialize_from_duckdb().

    Schema-Driven Design:
    - Table names come from RoboLedgerContext.get_all_table_names_for_context()
    - No manifest file needed - schema is the source of truth
    - Works across services (Dagster/Graph API) without shared filesystem

    Full Rebuild Mode:
    - Always recreates DuckDB tables from all S3 parquet files (CREATE TABLE)
    - No incremental mode - pipeline always rebuilds from scratch

    Args:
        year: Optional year filter. If provided, only files from filings in that year
              will be included (filters to filed=YYYY-*).
        reset_staging: If True, delete entire DuckDB staging database for a fresh start.
            Use this when changing settings like skip_taxonomy_relationships.
            If False (default), preserve DuckDB for retry scenarios (tables are overwritten).
        use_glob: If True (default), use glob patterns for efficient file discovery.
            If False, use explicit file lists (legacy behavior, slower for many files).
        progress_callback: Optional callback for progress logging (e.g., Dagster context.log.info).
            Called with message strings during per-table staging operations.

    Returns:
        StagingResult with table counts and file counts
    """
    start_time = time.time()

    # Helper to log both to logger and optional callback (Dagster)
    def log_progress(msg: str) -> None:
      logger.info(msg)
      if progress_callback:
        progress_callback(msg)

    # Determine date filter for logging
    date_filter = f"{year}-*" if year else "all"
    logger.info(
      f"Starting DuckDB staging for graph {self.graph_id} "
      f"(filed={date_filter}, reset_staging={reset_staging})"
    )

    duckdb_path = get_staging_duckdb_path(self.graph_id)

    try:
      # Get graph client for API calls
      try:
        client = await get_graph_client(graph_id=self.graph_id, operation_type="write")
      except Exception as client_err:
        logger.error(
          f"Failed to initialize graph client for {self.graph_id}: {client_err}",
          exc_info=True,
        )
        return StagingResult(
          status="error",
          table_names=[],
          error=f"Graph client initialization failed: {client_err!s}",
          duration_ms=(time.time() - start_time) * 1000,
        )

      # Reset staging if requested - delete entire DuckDB staging database
      if reset_staging:
        log_progress("Resetting DuckDB staging - deleting staging database...")
        try:
          # Use staging_only=True to delete only DuckDB, preserve LadybugDB graph
          await client.delete_database(self.graph_id, staging_only=True)
          log_progress("DuckDB staging database deleted successfully")
        except Exception as reset_err:
          # Non-fatal - database might not exist yet
          logger.warning(f"Could not reset staging database: {reset_err}")
          log_progress(f"Reset skipped (staging may not exist): {reset_err}")

      # Refresh client connection after reset
      try:
        client = await get_graph_client(graph_id=self.graph_id, operation_type="write")
      except Exception as client_err:
        logger.error(
          f"Failed to initialize graph client for {self.graph_id}: {client_err}",
          exc_info=True,
        )
        return StagingResult(
          status="error",
          table_names=[],
          error=f"Graph client initialization failed: {client_err!s}",
          duration_ms=(time.time() - start_time) * 1000,
        )

      # Step 1: Get table names from schema (no S3 discovery needed for glob mode)
      # Initialize variables for type checker
      tables_by_type: dict[str, str] = {}
      tables_info: dict[str, list[str]] = {}

      if use_glob:
        logger.info("Step 1: Getting table names from schema (glob mode)...")
        # Schema-driven: get all tables for SEC repository context
        tables_by_type = RoboLedgerContext.get_all_table_names_for_context(
          RoboLedgerContext.SEC_REPOSITORY
        )
        logger.info(f"Schema defines {len(tables_by_type)} tables to stage")

        # Filter out taxonomy structure tables if requested (instance-only mode)
        if skip_taxonomy_relationships:
          original_count = len(tables_by_type)
          tables_by_type = {
            name: entity_type
            for name, entity_type in tables_by_type.items()
            if name not in TAXONOMY_STRUCTURE_TABLES
          }
          skipped_count = original_count - len(tables_by_type)
          log_progress(
            f"Instance-only mode: skipping {skipped_count} taxonomy structure tables "
            f"(Association, Structure, TAXONOMY_HAS_*, etc.)"
          )
          logger.info(f"After filtering: {len(tables_by_type)} tables to stage")

        # With glob, we don't know file count upfront - DuckDB will discover files
        total_files = 0
      else:
        logger.info("Step 1: Discovering processed Parquet files (legacy mode)...")
        tables_info = await self._discover_processed_files(year)

        if not tables_info:
          logger.warning("No processed files found")
          return StagingResult(
            status="no_data",
            table_names=[],
            error="No processed files found",
            duration_ms=(time.time() - start_time) * 1000,
          )

        logger.info(f"Found {len(tables_info)} tables to stage")

        # Filter out taxonomy structure tables if requested (instance-only mode)
        if skip_taxonomy_relationships:
          original_count = len(tables_info)
          tables_info = {
            name: files
            for name, files in tables_info.items()
            if name not in TAXONOMY_STRUCTURE_TABLES
          }
          skipped_count = original_count - len(tables_info)
          log_progress(
            f"Instance-only mode: skipping {skipped_count} taxonomy structure tables "
            f"(Association, Structure, TAXONOMY_HAS_*, etc.)"
          )
          logger.info(f"After filtering: {len(tables_info)} tables to stage")
        total_files = sum(len(files) for files in tables_info.values())
        logger.info(f"Total files: {total_files}")

      # Step 2: Create DuckDB staging tables via Graph API
      if use_glob:
        # Full mode: CREATE TABLE from glob patterns
        log_progress(f"Step 2: Creating {len(tables_by_type)} DuckDB staging tables...")
        successful_tables, table_infos = await self._create_duckdb_tables_with_glob(
          tables_by_type,
          client,
          year=year,
          progress_callback=log_progress,
        )
      else:
        log_progress(
          "Step 2: Creating DuckDB staging tables via file lists (legacy)..."
        )
        successful_tables, table_infos = await self._create_duckdb_tables_with_info(
          tables_info, client
        )

      # Determine expected table count based on mode
      expected_table_count = len(tables_by_type) if use_glob else len(tables_info)
      status = (
        "success" if len(successful_tables) == expected_table_count else "partial"
      )

      logger.info(
        f"Staging status: {status} ({len(successful_tables)}/{expected_table_count} tables)"
      )

      total_rows = sum(info.row_count for info in table_infos.values())
      duration = time.time() - start_time

      logger.info(
        f"DuckDB staging complete in {duration:.2f}s: "
        f"{len(successful_tables)} tables from {total_files} files"
      )

      return StagingResult(
        status=status,
        table_names=successful_tables,
        tables=table_infos,
        total_files=total_files,
        total_rows=total_rows,
        duration_ms=duration * 1000,
        duckdb_path=duckdb_path,
      )

    except Exception as e:
      logger.error(f"DuckDB staging failed: {e}", exc_info=True)
      return StagingResult(
        status="error",
        table_names=[],
        error=str(e),
        duration_ms=(time.time() - start_time) * 1000,
      )

  async def stage_incremental_to_duckdb(
    self,
    year: int | None = None,
    quarter: int | None = None,
    skip_taxonomy_relationships: bool = False,
    progress_callback: ProgressCallback | None = None,
  ) -> StagingResult:
    """
    Stage current quarter's files incrementally to existing DuckDB tables.

    Unlike stage_to_duckdb() which rebuilds all tables from scratch,
    this method INSERTs data into existing tables with deduplication.

    Since INSERT uses UNION ALL + ROW_NUMBER dedup, we can point at the
    entire quarter's files every time - only truly new rows are added.
    No need to track which specific dates have been staged.

    Safe to re-run daily - deduplication prevents data multiplication.

    Precondition: DuckDB tables must already exist from initial full staging.

    Args:
        year: Year to stage (default: current year)
        quarter: Quarter to stage 1-4 (default: current quarter)
        skip_taxonomy_relationships: If True, skip taxonomy structure tables
        progress_callback: Optional callback for Dagster logging

    Returns:
        StagingResult with tables staged and row counts (net new rows)
    """
    start_time = time.time()

    # Default to current year/quarter
    now = datetime.now(UTC)
    year = year or now.year
    quarter = quarter or ((now.month - 1) // 3 + 1)

    # Build list of quarters to scan (current + previous during overlap period)
    # This catches late-indexed filings near quarter boundaries (UTC/EST timing, SEC delays)
    quarters_to_scan: list[tuple[int, int]] = [(year, quarter)]

    quarter_start_month = (quarter - 1) * 3 + 1
    if now.month == quarter_start_month and now.day <= 5:
      # First 5 days of new quarter: also scan previous quarter
      if quarter == 1:
        quarters_to_scan.append((year - 1, 4))
      else:
        quarters_to_scan.append((year, quarter - 1))

    def log_progress(msg: str) -> None:
      logger.info(msg)
      if progress_callback:
        progress_callback(msg)

    quarters_str = ", ".join(f"{y}-Q{q}" for y, q in quarters_to_scan)
    logger.info(
      f"Starting incremental DuckDB staging for graph {self.graph_id} "
      f"(quarters: {quarters_str})"
    )

    try:
      client = await get_graph_client(graph_id=self.graph_id, operation_type="write")

      # Verify tables exist (must have done initial full staging)
      existing_tables = await client.list_tables(self.graph_id)
      if not existing_tables:
        return StagingResult(
          status="error",
          table_names=[],
          error=(
            "No existing DuckDB tables found for incremental staging. "
            "Run full staging first via the sec_duckdb_staged asset."
          ),
          duration_ms=(time.time() - start_time) * 1000,
        )

      log_progress(f"Found {len(existing_tables)} existing tables in DuckDB")

      # Get schema-defined tables
      tables_by_type = RoboLedgerContext.get_all_table_names_for_context(
        RoboLedgerContext.SEC_REPOSITORY
      )

      if skip_taxonomy_relationships:
        tables_by_type = {
          name: entity_type
          for name, entity_type in tables_by_type.items()
          if name not in TAXONOMY_STRUCTURE_TABLES
        }

      # Stage each table incrementally using quarterly partition format
      # New format: filed=2024-Q1 with single TABLE.parquet file
      successful_tables: list[str] = []
      table_infos: dict[str, TableInfo] = {}
      failed_tables: list[tuple[str, str]] = []

      total_tables = len(tables_by_type)
      for i, (table_name, entity_type) in enumerate(tables_by_type.items(), 1):
        # Build S3 patterns for all quarters to scan (handles quarter-end overlap)
        s3_patterns = [
          f"s3://{self.bucket}/{self.source_prefix}/filed={y}-Q{q}/{entity_type}/{table_name}.parquet"
          for y, q in quarters_to_scan
        ]

        # Filter to only existing files (important during overlap period)
        # DuckDB fails if any file in a list doesn't exist, so we must filter first
        if len(s3_patterns) > 1:
          s3_patterns = [p for p in s3_patterns if self._s3_url_exists(p)]
          if not s3_patterns:
            log_progress(
              f"[{i}/{total_tables}] Skipped {table_name}: no files for any quarter"
            )
            successful_tables.append(table_name)
            table_infos[table_name] = TableInfo(
              name=table_name,
              row_count=0,
              file_count=0,
              staged_at=datetime.now(UTC).isoformat(),
              skipped=True,
            )
            continue

        # Use single pattern if only one quarter, list if multiple
        s3_pattern: str | list[str] = (
          s3_patterns[0] if len(s3_patterns) == 1 else s3_patterns
        )

        timeout = _get_staging_timeout(table_name)
        log_progress(f"[{i}/{total_tables}] INSERT {table_name} (Q{quarter} {year})...")

        try:
          # INSERT INTO existing table (with dedup)
          response = await client.insert_into_table(
            graph_id=self.graph_id,
            table_name=table_name,
            s3_pattern=s3_pattern,
            timeout=timeout,
          )

          if response.get("status") == "failed":
            error = response.get("error", "Unknown error")
            if "No files found" in error:
              log_progress(
                f"[{i}/{total_tables}] Skipped {table_name}: no files for Q{quarter}"
              )
              successful_tables.append(table_name)
              table_infos[table_name] = TableInfo(
                name=table_name,
                row_count=0,
                file_count=0,
                staged_at=datetime.now(UTC).isoformat(),
                skipped=True,
              )
              continue
            failed_tables.append((table_name, error))
            continue

          result = response.get("result", {})
          row_count = result.get("row_count", 0)  # Net new rows after dedup
          duration = response.get("duration_seconds", 0)

          log_progress(
            f"[{i}/{total_tables}] Inserted {table_name}: "
            f"{row_count:,} net new rows in {duration:.1f}s"
          )

          successful_tables.append(table_name)
          table_infos[table_name] = TableInfo(
            name=table_name,
            row_count=row_count,
            file_count=0,
            staged_at=datetime.now(UTC).isoformat(),
          )

        except Exception as e:
          error_str = str(e)
          if "No files found" in error_str:
            successful_tables.append(table_name)
            table_infos[table_name] = TableInfo(
              name=table_name,
              row_count=0,
              file_count=0,
              staged_at=datetime.now(UTC).isoformat(),
              skipped=True,
            )
          else:
            failed_tables.append((table_name, error_str))

      status = "success" if len(successful_tables) == total_tables else "partial"
      total_rows = sum(info.row_count for info in table_infos.values())
      duration = time.time() - start_time

      logger.info(
        f"Incremental staging complete in {duration:.2f}s: "
        f"{len(successful_tables)}/{total_tables} tables, {total_rows:,} net new rows"
      )

      return StagingResult(
        status=status,
        table_names=successful_tables,
        tables=table_infos,
        total_rows=total_rows,
        duration_ms=duration * 1000,
        duckdb_path=get_staging_duckdb_path(self.graph_id),
      )

    except Exception as e:
      logger.error(f"Incremental staging failed: {e}", exc_info=True)
      return StagingResult(
        status="error",
        table_names=[],
        error=str(e),
        duration_ms=(time.time() - start_time) * 1000,
      )

  async def copy_incremental_to_ladybug(
    self,
    year: int | None = None,
    quarter: int | None = None,
    skip_taxonomy_relationships: bool = False,
    progress_callback: ProgressCallback | None = None,
  ) -> MaterializeResult:
    """
    Copy current quarter's files directly to LadybugDB (bypasses DuckDB staging).

    This is the preferred approach for incremental updates because:
    - LadybugDB's COPY with ignore_errors=true handles duplicates automatically
    - No need to diff what's new in DuckDB vs LadybugDB
    - Simpler and faster for daily updates

    Uses the same 5-day overlap logic at quarter boundaries to catch late-indexed
    filings. Files that don't exist are filtered out before COPY.

    Safe to run daily - duplicates are rejected by LadybugDB constraints.

    Precondition: LadybugDB database must already exist with SEC schema.

    Args:
        year: Year to copy (default: current year)
        quarter: Quarter to copy 1-4 (default: current quarter)
        skip_taxonomy_relationships: If True, skip taxonomy structure tables
        progress_callback: Optional callback for Dagster logging

    Returns:
        MaterializeResult with tables copied and row counts
    """
    start_time = time.time()

    # Default to current year/quarter
    now = datetime.now(UTC)
    year = year or now.year
    quarter = quarter or ((now.month - 1) // 3 + 1)

    # Build list of quarters to scan (current + previous during overlap period)
    # This catches late-indexed filings near quarter boundaries
    quarters_to_scan: list[tuple[int, int]] = [(year, quarter)]

    quarter_start_month = (quarter - 1) * 3 + 1
    if now.month == quarter_start_month and now.day <= 5:
      # First 5 days of new quarter: also scan previous quarter
      if quarter == 1:
        quarters_to_scan.append((year - 1, 4))
      else:
        quarters_to_scan.append((year, quarter - 1))

    def log_progress(msg: str) -> None:
      logger.info(msg)
      if progress_callback:
        progress_callback(msg)

    quarters_str = ", ".join(f"{y}-Q{q}" for y, q in quarters_to_scan)
    logger.info(
      f"Starting incremental LadybugDB COPY for graph {self.graph_id} "
      f"(quarters: {quarters_str})"
    )

    try:
      client = await get_graph_client(graph_id=self.graph_id, operation_type="write")

      # Get schema-defined tables
      tables_by_type = RoboLedgerContext.get_all_table_names_for_context(
        RoboLedgerContext.SEC_REPOSITORY
      )

      if skip_taxonomy_relationships:
        tables_by_type = {
          name: entity_type
          for name, entity_type in tables_by_type.items()
          if name not in TAXONOMY_STRUCTURE_TABLES
        }

      # Sort: nodes first, then relationships (uppercase names)
      node_tables = [
        (name, etype) for name, etype in tables_by_type.items() if not name.isupper()
      ]
      rel_tables = [
        (name, etype) for name, etype in tables_by_type.items() if name.isupper()
      ]
      ordered_tables = node_tables + rel_tables

      log_progress(
        f"Copying {len(ordered_tables)} tables ({len(node_tables)} nodes, "
        f"{len(rel_tables)} relationships)"
      )

      # Copy each table with ignore_errors=true for duplicate handling
      successful_tables: list[str] = []
      table_rows: dict[str, int] = {}
      failed_tables: list[tuple[str, str]] = []

      # Timeout per table (small since only 1-2 quarters)
      COPY_TIMEOUT = 600  # 10 minutes per table

      total_tables = len(ordered_tables)
      for i, (table_name, entity_type) in enumerate(ordered_tables, 1):
        # Build S3 paths for all quarters to scan
        s3_paths = [
          f"s3://{self.bucket}/{self.source_prefix}/filed={y}-Q{q}/{entity_type}/{table_name}.parquet"
          for y, q in quarters_to_scan
        ]

        # Filter to only existing files (LadybugDB fails if any file doesn't exist)
        if len(s3_paths) > 1:
          s3_paths = [p for p in s3_paths if self._s3_url_exists(p)]
          if not s3_paths:
            log_progress(
              f"[{i}/{total_tables}] Skipped {table_name}: no files for any quarter"
            )
            successful_tables.append(table_name)
            table_rows[table_name] = 0
            continue
        else:
          # Single path - check if it exists
          if not self._s3_url_exists(s3_paths[0]):
            log_progress(
              f"[{i}/{total_tables}] Skipped {table_name}: no files for Q{quarter}"
            )
            successful_tables.append(table_name)
            table_rows[table_name] = 0
            continue

        # Use single path or list depending on count
        s3_pattern: str | list[str] = (
          s3_paths[0] if len(s3_paths) == 1 else s3_paths
        )

        log_progress(f"[{i}/{total_tables}] COPY {table_name} (Q{quarter} {year})...")

        try:
          copy_result = await client.copy_from_s3(
            graph_id=self.graph_id,
            table_name=table_name,
            s3_pattern=s3_pattern,
            ignore_errors=True,  # Key: duplicates are silently skipped
            timeout=COPY_TIMEOUT,
            wait_for_completion=True,
          )

          if copy_result.get("status") == "completed":
            records = copy_result.get("records_loaded", 0)
            duration = copy_result.get("duration_seconds", 0)
            log_progress(
              f"[{i}/{total_tables}] Copied {table_name}: "
              f"{records:,} records in {duration:.1f}s"
            )
            successful_tables.append(table_name)
            table_rows[table_name] = records
          elif "No files found" in copy_result.get("error", ""):
            log_progress(
              f"[{i}/{total_tables}] Skipped {table_name}: no files for Q{quarter}"
            )
            successful_tables.append(table_name)
            table_rows[table_name] = 0
          else:
            error = copy_result.get("error", "Unknown error")
            log_progress(f"[{i}/{total_tables}] FAILED {table_name}: {error}")
            failed_tables.append((table_name, error))

        except Exception as e:
          error_str = str(e)
          if "No files found" in error_str:
            log_progress(
              f"[{i}/{total_tables}] Skipped {table_name}: no files for Q{quarter}"
            )
            successful_tables.append(table_name)
            table_rows[table_name] = 0
          else:
            log_progress(f"[{i}/{total_tables}] FAILED {table_name}: {error_str}")
            failed_tables.append((table_name, error_str))

      status = "success" if len(successful_tables) == total_tables else "partial"
      total_rows = sum(table_rows.values())
      duration = time.time() - start_time

      if failed_tables:
        logger.warning(f"Failed tables: {failed_tables}")

      logger.info(
        f"Incremental COPY complete in {duration:.2f}s: "
        f"{len(successful_tables)}/{total_tables} tables, {total_rows:,} records"
      )

      return MaterializeResult(
        status=status,
        table_names=successful_tables,
        failed_tables=[
          {"table": name, "error": error} for name, error in failed_tables
        ],
        total_rows=total_rows,
        duration_ms=duration * 1000,
      )

    except Exception as e:
      logger.error(f"Incremental COPY failed: {e}", exc_info=True)
      return MaterializeResult(
        status="error",
        table_names=[],
        error=str(e),
        duration_ms=(time.time() - start_time) * 1000,
      )

  async def materialize_from_duckdb(
    self,
    table_names: list[str] | None = None,
    rebuild: bool = False,
    skip_taxonomy_relationships: bool = False,
    progress_callback: ProgressCallback | None = None,
  ) -> MaterializeResult:
    """
    Materialize LadybugDB graph from existing DuckDB staging.

    This is Stage 2 of the decoupled pipeline. It uses the schema to determine
    which tables to materialize and triggers ingestion for each table.

    Schema-Driven Design:
    - Table names come from RoboLedgerContext.get_all_table_names_for_context()
    - No manifest file needed - schema is the source of truth
    - Works across services (Dagster/Graph API) without shared filesystem

    Precondition: stage_to_duckdb() must have been run successfully, creating
    DuckDB staging tables.

    Args:
        table_names: Optional list of specific tables to materialize.
                     If None, materializes all tables from the schema.
        rebuild: If True, delete and recreate the LadybugDB database with
                 the roboledger SEC schema before materializing.
                 DuckDB staging is preserved for retry scenarios.
        skip_taxonomy_relationships: If True, skip materializing taxonomy structure
                     tables (Association, Structure, TAXONOMY_HAS_*, etc.) to reduce storage.
        progress_callback: Optional callback for progress logging (e.g., Dagster context.log.info).
            Called with message strings during per-table materialization.

    Returns:
        MaterializeResult with rows ingested per table
    """
    start_time = time.time()

    # Helper to log both to logger and optional callback (Dagster)
    def log_progress(msg: str) -> None:
      logger.info(msg)
      if progress_callback:
        progress_callback(msg)

    log_progress(f"Starting materialization from DuckDB for graph {self.graph_id}")

    try:
      # Step 1: Determine which tables to materialize (schema-driven)
      if table_names is None:
        logger.info("Step 1: Getting table names from schema...")
        tables_by_type = RoboLedgerContext.get_all_table_names_for_context(
          RoboLedgerContext.SEC_REPOSITORY
        )
        table_names = list(tables_by_type.keys())
        logger.info(f"Schema defines {len(table_names)} tables to materialize")

      # Filter out taxonomy structure tables if requested (instance-only mode)
      if skip_taxonomy_relationships:
        original_count = len(table_names)
        table_names = [t for t in table_names if t not in TAXONOMY_STRUCTURE_TABLES]
        skipped_count = original_count - len(table_names)
        log_progress(
          f"Instance-only mode: skipping {skipped_count} taxonomy structure tables "
          f"(Association, Structure, TAXONOMY_HAS_*, etc.)"
        )
        logger.info(f"After filtering: {len(table_names)} tables to materialize")

      if not table_names:
        logger.warning("No tables to materialize")
        return MaterializeResult(
          status="no_data",
          error="No tables found in schema",
        )

      logger.info(f"Materializing {len(table_names)} tables...")

      # Step 2: Get graph client
      try:
        client = await get_graph_client(graph_id=self.graph_id, operation_type="write")
      except Exception as client_err:
        logger.error(
          f"Failed to initialize graph client for {self.graph_id}: {client_err}",
          exc_info=True,
        )
        return MaterializeResult(
          status="error",
          error=f"Graph client initialization failed: {client_err!s}",
        )

      # Step 3: Rebuild or ensure LadybugDB database exists
      if rebuild:
        # Rebuild: Delete and recreate the LadybugDB database with schema
        # DuckDB staging is preserved for retry scenarios
        logger.info(
          "Step 3: Rebuilding LadybugDB database (DuckDB staging preserved)..."
        )
        await self._rebuild_ladybug_database(client, reset_staging=False)
        logger.info("LadybugDB database rebuilt with roboledger SEC schema")
      else:
        # Ensure: Create database if it doesn't exist (for retry scenarios)
        # Uses ensure_shared_repository_exists for production-compatible routing
        logger.info("Step 3: Ensuring LadybugDB database exists with schema...")
        from robosystems.config import env
        from robosystems.operations.graph.shared_repository_service import (
          ensure_shared_repository_exists,
        )

        repo_result = await ensure_shared_repository_exists(
          repository_name=self.graph_id,
          created_by="system",
          instance_id="local-dev"
          if env.ENVIRONMENT == "dev"
          else "ladybug-shared-prod",
        )
        logger.info(f"Repository ensure result: {repo_result.get('status', 'unknown')}")

      # Step 4: Trigger ingestion for each table
      log_progress(f"Step 4: Materializing {len(table_names)} tables to LadybugDB...")
      ingestion_results = await self._trigger_ingestion(
        table_names, client, progress_callback=log_progress
      )

      duration = time.time() - start_time

      log_progress(
        f"Materialization complete in {duration:.2f}s: "
        f"{ingestion_results.get('total_rows_ingested', 0):,} rows ingested"
      )
      return MaterializeResult(
        status="success",
        total_rows_ingested=ingestion_results.get("total_rows_ingested", 0),
        duration_ms=ingestion_results.get("duration_ms", 0),
        tables=ingestion_results.get("tables", []),
      )

    except Exception as e:
      logger.error(f"Materialization failed: {e}", exc_info=True)
      return MaterializeResult(
        status="error",
        error=str(e),
      )

  async def _rebuild_ladybug_database(
    self, client, reset_staging: bool = False
  ) -> None:
    """Rebuild the LadybugDB database from scratch.

    Deletes the existing database and recreates it with the same schema.

    Args:
        client: Graph API client instance
        reset_staging: If True, also delete DuckDB staging for a fresh start.
            If False (default), preserve DuckDB for incremental/retry scenarios.
    """
    from robosystems.database import SessionFactory
    from robosystems.graph_api.client.exceptions import GraphClientError
    from robosystems.models.iam import GraphSchema

    logger.info(
      f"Rebuild requested - regenerating entire LadybugDB database for {self.graph_id}"
    )

    db = SessionFactory()
    try:
      # preserve_duckdb=True keeps DuckDB for retry/incremental scenarios
      # preserve_duckdb=False (reset_staging=True) deletes both for fresh start
      preserve_duckdb = not reset_staging
      try:
        await client.delete_database(self.graph_id, preserve_duckdb=preserve_duckdb)
        if reset_staging:
          logger.info(
            f"Deleted LadybugDB and DuckDB staging: {self.graph_id} (fresh start)"
          )
        else:
          logger.info(
            f"Deleted LadybugDB database: {self.graph_id} (DuckDB staging preserved)"
          )
      except GraphClientError as e:
        if "not found" in str(e).lower():
          logger.info(f"Database {self.graph_id} does not exist, will create fresh")
        else:
          raise

      schema = GraphSchema.get_active_schema(self.graph_id, db)
      if not schema:
        raise ValueError(f"No schema found for graph {self.graph_id}")

      create_db_kwargs = {
        "graph_id": self.graph_id,
        "schema_type": schema.schema_type,
        "custom_schema_ddl": schema.schema_ddl,
      }

      if schema.schema_type == "shared":
        create_db_kwargs["repository_name"] = self.graph_id

      await client.create_database(**create_db_kwargs)
      logger.info(
        f"Recreated LadybugDB database with schema type: {schema.schema_type}"
      )
    finally:
      db.close()

  async def _discover_processed_files(
    self, year: int | None = None
  ) -> dict[str, list[str]]:
    """
    Discover processed Parquet files from S3.

    Scans the processed files directory structure:
    processed/year=YYYY/nodes/TableName/file.parquet
    processed/year=YYYY/relationships/TableName/file.parquet

    Args:
        year: Optional year filter. If None, scans all year subdirectories.

    Returns:
        Dictionary mapping table names to list of S3 keys
    """
    tables_info: dict[str, list[str]] = {}

    # Determine which years to scan
    if year is None:
      # Discover all year subdirectories by listing the processed/ prefix
      year_prefix = f"{self.source_prefix}/"
      logger.info(f"Discovering year subdirectories in {self.bucket}/{year_prefix}")

      # List directories to find year= subdirectories
      paginator = self.s3_client.s3_client.get_paginator("list_objects_v2")
      pages = paginator.paginate(Bucket=self.bucket, Prefix=year_prefix, Delimiter="/")

      years_to_scan = []
      for page in pages:
        # CommonPrefixes contains the "directories" (year= prefixes)
        if "CommonPrefixes" in page:
          for prefix_info in page["CommonPrefixes"]:
            prefix_path = prefix_info["Prefix"]
            # Extract year from prefix like "processed/year=2025/"
            if "year=" in prefix_path:
              year_part = prefix_path.split("year=")[1].rstrip("/")
              try:
                year_num = int(year_part)
                years_to_scan.append(year_num)
                logger.debug(f"Found year subdirectory: {year_num}")
              except ValueError:
                logger.debug(f"Skipping non-year prefix: {prefix_path}")

      if not years_to_scan:
        logger.warning(f"No year subdirectories found under {year_prefix}")
        return tables_info

      logger.info(
        f"Discovered {len(years_to_scan)} years to scan: {sorted(years_to_scan)}"
      )
    else:
      # Single year specified
      years_to_scan = [year]

    # Scan both nodes and relationships directories across all years
    for entity_type in ["nodes", "relationships"]:
      for scan_year in years_to_scan:
        prefix = f"{self.source_prefix}/year={scan_year}/{entity_type}/"
        logger.debug(f"Scanning S3 bucket {self.bucket} with prefix {prefix}")

        # List all files recursively
        paginator = self.s3_client.s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

        for page in pages:
          if "Contents" not in page:
            continue

          for obj in page["Contents"]:
            key = obj["Key"]

            # Skip non-Parquet files
            if not key.endswith(".parquet"):
              continue

            # Extract table name from path: processed/year=YYYY/nodes|relationships/TableName/file.parquet
            # Structure: processed/year=YYYY/nodes|relationships/TableName/CIK_ACCESSION.parquet
            path_parts = key.replace(prefix, "").split("/")

            # First part after nodes/ or relationships/ is the table name
            if len(path_parts) >= 2:
              table_name = path_parts[0]
            else:
              logger.debug(f"Skipping file with unexpected path structure: {key}")
              continue

            if table_name not in tables_info:
              tables_info[table_name] = []

            tables_info[table_name].append(key)

    logger.info(f"Discovered {len(tables_info)} tables with files:")
    for table_name, files in tables_info.items():
      logger.info(f"  - {table_name}: {len(files)} files")

    return tables_info

  async def _stage_table_with_retry(
    self,
    table_name: str,
    stage_fn: Callable[[], Awaitable[tuple[bool, TableInfo | None, str | None]]],
    graph_client,
    log_progress: Callable[[str], None],
    table_index: int,
    total_tables: int,
  ) -> tuple[bool, TableInfo | None, str | None]:
    """
    Retry wrapper for table staging with exponential backoff.

    On failure (including timeout), drops the partial table and retries from scratch.
    This ensures we don't end up with partially staged tables missing quarters/data.

    Args:
        table_name: Name of the table being staged
        stage_fn: Async callable that performs the staging, returns (success, info, error)
        graph_client: Graph API client for dropping tables on retry
        log_progress: Progress logging callback
        table_index: Current table index for progress display
        total_tables: Total tables for progress display

    Returns:
        Tuple of (success, TableInfo or None, error message or None)
    """
    last_error: str | None = None

    for attempt in range(STAGING_MAX_RETRIES):
      success, table_info, error = await stage_fn()

      if success:
        return success, table_info, error

      last_error = error

      # Check if we have retries left
      if attempt < STAGING_MAX_RETRIES - 1:
        backoff = STAGING_RETRY_BACKOFF_BASE * (attempt + 1)
        log_progress(
          f"[{table_index}/{total_tables}] {table_name} failed: {error}. "
          f"Retry {attempt + 2}/{STAGING_MAX_RETRIES} in {backoff}s..."
        )

        # Drop partial table before retry to ensure clean slate
        try:
          await graph_client.delete_table(self.graph_id, table_name)
          logger.debug(f"Dropped partial table {table_name} before retry")
        except Exception as drop_err:
          # Table might not exist or already be gone - that's fine
          logger.debug(f"Could not drop table {table_name} before retry: {drop_err}")

        await asyncio.sleep(backoff)
      else:
        log_progress(
          f"[{table_index}/{total_tables}] {table_name} failed after "
          f"{STAGING_MAX_RETRIES} attempts: {error}"
        )

    return False, None, f"Failed after {STAGING_MAX_RETRIES} attempts: {last_error}"

  async def _stage_large_table_by_quarters(
    self,
    table_name: str,
    entity_type: str,
    graph_client: "GraphClient",
    quarters: list[str],
    log_progress: Callable[[str], None],
    table_index: int,
    total_tables: int,
  ) -> tuple[bool, TableInfo | None, str | None]:
    """
    Stage a large table using parallel chunk loading + final merge pattern.

    For tables like Fact (100M+ rows), this approach:
    1. Loads each quarter into a separate chunk table (no cross-quarter dedupe needed)
    2. Merges all chunks with deduplication into the final table
    3. Cleans up chunk tables

    This is more efficient than incremental INSERT because:
    - Each chunk load is O(chunk_size), not O(accumulated_size)
    - Final merge is identical to full (non-chunked) ingest - one pass over all data
    - No repeated DROP/RENAME cycles that can cause connection state issues

    Args:
        table_name: Name of the table to stage
        entity_type: "nodes" or "relationships"
        graph_client: Graph API client instance
        quarters: List of quarter partition keys (e.g., ["2024-Q1", "2024-Q2", ...])
        log_progress: Progress logging callback
        table_index: Current table index for progress display
        total_tables: Total number of tables for progress display

    Returns:
        Tuple of (success, TableInfo or None, error message or None)
    """
    timeout = CHUNKED_STAGING_TIMEOUT
    chunk_tables: list[str] = []
    chunk_row_counts: dict[str, int] = {}
    total_duration = 0.0

    log_progress(
      f"[{table_index}/{total_tables}] Staging {table_name} by quarter "
      f"({len(quarters)} quarters, chunk+merge)..."
    )

    # Phase 1: Load each quarter into a separate chunk table
    for q_idx, quarter_key in enumerate(quarters):
      # Chunk table name: Association_chunk_2024_Q1
      chunk_name = f"{table_name}_chunk_{quarter_key.replace('-', '_')}"

      # Single file per table per quarter: TABLE.parquet
      s3_pattern = (
        f"s3://{self.bucket}/{self.source_prefix}/filed={quarter_key}/"
        f"{entity_type}/{table_name}.parquet"
      )

      log_progress(f"  [{quarter_key}] Loading chunk {q_idx + 1}/{len(quarters)}...")

      # Retry logic for this chunk
      chunk_success = False
      last_error = None

      for attempt in range(STAGING_MAX_RETRIES):
        try:
          response = await graph_client.create_table(
            graph_id=self.graph_id,
            table_name=chunk_name,
            s3_pattern=s3_pattern,
            timeout=timeout,
          )

          if response.get("status") == "failed":
            error = response.get("error", "Unknown error")
            if "No files found" in error:
              log_progress(f"  [{quarter_key}] No files (skipped)")
              chunk_success = True
              break
            last_error = error
            raise RuntimeError(error)

          result = response.get("result", {})
          row_count = result.get("row_count", 0)
          duration = response.get("duration_seconds", result.get("duration_seconds", 0))
          total_duration += duration

          log_progress(
            f"  [{quarter_key}] Loaded {row_count:,} rows in {duration:.1f}s"
          )

          chunk_tables.append(chunk_name)
          chunk_row_counts[chunk_name] = row_count
          chunk_success = True
          break

        except Exception as e:
          # Capture error details - include type if message is empty/unhelpful
          error_str = str(e).strip() if str(e).strip() and str(e).strip() != "." else ""
          if not error_str:
            error_str = f"{type(e).__name__} (no message)"
          else:
            # Prepend exception type for clarity in logs
            error_str = f"{type(e).__name__}: {error_str}"

          if "No files found" in error_str:
            log_progress(f"  [{quarter_key}] No files (skipped)")
            chunk_success = True
            break

          last_error = error_str
          if attempt < STAGING_MAX_RETRIES - 1:
            backoff = STAGING_RETRY_BACKOFF_BASE * (attempt + 1)
            log_progress(
              f"  [{quarter_key}] Attempt {attempt + 1}/{STAGING_MAX_RETRIES} failed: {error_str[:100]}. "
              f"Retrying in {backoff}s..."
            )

            # CRITICAL: Get fresh client before retry to avoid corrupted httpx state
            # After timeout/SSE failures, the existing client's connection pool may be
            # in a bad state, causing subsequent requests to fail silently
            try:
              graph_client = await get_graph_client(
                graph_id=self.graph_id, operation_type="write"
              )
              logger.debug(
                f"Obtained fresh graph client for retry attempt {attempt + 2}"
              )
            except Exception as client_err:
              logger.warning(f"Could not refresh graph client: {client_err}")
              # Continue with existing client - better than nothing

            await asyncio.sleep(backoff)
          else:
            log_progress(
              f"  [{quarter_key}] Failed after {STAGING_MAX_RETRIES} attempts: {error_str[:200]}"
            )

      if not chunk_success:
        # Cleanup any created chunk tables before returning
        await self._cleanup_chunk_tables(graph_client, chunk_tables, log_progress)
        return False, None, f"Failed to load chunk {quarter_key}: {last_error}"

    # Phase 2: Merge all chunks with deduplication
    if not chunk_tables:
      log_progress("  [MERGE] No chunks to merge (all quarters empty)")
      return (
        True,
        TableInfo(
          name=table_name,
          row_count=0,
          file_count=0,
          staged_at=datetime.now(UTC).isoformat(),
        ),
        None,
      )

    total_chunk_rows = sum(chunk_row_counts.values())
    log_progress(
      f"  [MERGE] Merging {len(chunk_tables)} chunks ({total_chunk_rows:,} rows) with dedupe..."
    )

    # Build merge SQL - same pattern as full ingest uses
    # Dedupe columns depend on table type: nodes use 'identifier', relationships use 'src, dst'
    if entity_type == "nodes":
      dedupe_columns = "identifier"
    else:
      # Relationships - columns are already renamed to src/dst by create_table
      dedupe_columns = "src, dst"

    union_parts = " UNION ALL ".join(f'SELECT * FROM "{t}"' for t in chunk_tables)
    merge_sql = f"""
      CREATE OR REPLACE TABLE "{table_name}" AS
      SELECT * EXCLUDE (rn)
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY {dedupe_columns} ORDER BY {dedupe_columns}) AS rn
        FROM ({union_parts})
      )
      WHERE rn = 1
    """

    # Retry logic for merge
    merge_success = False
    last_merge_error = None
    final_row_count = 0

    # Merge timeout: Use same timeout as chunk operations (10 min)
    # The merge is O(n) like full ingest, so shouldn't need longer than chunk loading
    merge_timeout = float(CHUNKED_STAGING_TIMEOUT)  # 10 minutes

    try:
      for attempt in range(STAGING_MAX_RETRIES):
        try:
          merge_start = asyncio.get_event_loop().time()
          await graph_client.query_table(
            graph_id=self.graph_id,
            sql=merge_sql,
            timeout=merge_timeout,
          )
          merge_duration = asyncio.get_event_loop().time() - merge_start
          total_duration += merge_duration

          # Get final row count (short timeout is fine for COUNT)
          count_response = await graph_client.query_table(
            graph_id=self.graph_id,
            sql=f'SELECT COUNT(*) as cnt FROM "{table_name}"',
            timeout=60.0,  # 1 minute is plenty for COUNT(*)
          )
          if count_response.get("rows") and count_response["rows"][0]:
            final_row_count = count_response["rows"][0][0]

          log_progress(
            f"  [MERGE] Created {table_name}: {final_row_count:,} rows in {merge_duration:.1f}s"
          )
          merge_success = True
          break

        except Exception as e:
          # Capture error details - include type if message is empty/unhelpful
          error_str = str(e).strip() if str(e).strip() and str(e).strip() != "." else ""
          if not error_str:
            last_merge_error = f"{type(e).__name__} (no message)"
          else:
            last_merge_error = f"{type(e).__name__}: {error_str}"

          if attempt < STAGING_MAX_RETRIES - 1:
            backoff = STAGING_RETRY_BACKOFF_BASE * (attempt + 1)
            log_progress(
              f"  [MERGE] Attempt {attempt + 1}/{STAGING_MAX_RETRIES} failed: {last_merge_error[:100]}. "
              f"Retrying in {backoff}s..."
            )

            # CRITICAL: Get fresh client before retry to avoid corrupted httpx state
            try:
              graph_client = await get_graph_client(
                graph_id=self.graph_id, operation_type="write"
              )
              logger.debug(
                f"Obtained fresh graph client for merge retry attempt {attempt + 2}"
              )
            except Exception as client_err:
              logger.warning(f"Could not refresh graph client for merge: {client_err}")

            await asyncio.sleep(backoff)
          else:
            log_progress(
              f"  [MERGE] Failed after {STAGING_MAX_RETRIES} attempts: {last_merge_error[:200]}"
            )
    finally:
      # Phase 3: Cleanup chunk tables (always runs, even on crash/exception)
      await self._cleanup_chunk_tables(graph_client, chunk_tables, log_progress)

    if not merge_success:
      return False, None, f"Failed to merge chunks: {last_merge_error}"

    log_progress(
      f"[{table_index}/{total_tables}] Staged {table_name}: "
      f"{final_row_count:,} rows in {total_duration:.1f}s (chunk+merge)"
    )

    return (
      True,
      TableInfo(
        name=table_name,
        row_count=final_row_count,
        file_count=0,
        staged_at=datetime.now(UTC).isoformat(),
      ),
      None,
    )

  async def _cleanup_chunk_tables(
    self,
    graph_client: "GraphClient",
    chunk_tables: list[str],
    log_progress: Callable[[str], None],
  ) -> None:
    """Delete chunk tables after merge (best effort)."""
    if not chunk_tables:
      return

    log_progress(f"  [CLEANUP] Deleting {len(chunk_tables)} chunk tables...")
    deleted = 0
    for chunk_name in chunk_tables:
      try:
        await graph_client.delete_table(self.graph_id, chunk_name)
        deleted += 1
      except Exception as e:
        # Non-fatal - log and continue
        logger.warning(f"Could not delete chunk table {chunk_name}: {e}")

    log_progress(f"  [CLEANUP] Deleted {deleted}/{len(chunk_tables)} chunk tables")

  async def _create_duckdb_tables_with_glob(
    self,
    tables: dict[str, str],
    graph_client,
    year: int | None = None,
    progress_callback: ProgressCallback | None = None,
    chunk_large_tables: bool | None = None,
  ) -> tuple[list[str], dict[str, TableInfo]]:
    """
    Create DuckDB staging tables using glob patterns (efficient for many files).

    Instead of passing explicit file lists, this method constructs glob patterns
    and lets DuckDB handle file discovery internally.

    Partition structure:
      sec/processed/filed=YYYY-MM-DD/nodes/TABLE/*.parquet
      - Part files (part-001.parquet, part-002.parquet, etc.) per table per date
      - Each flush during processing creates a new part file
      - DuckDB reads all part files as a single dataset

    Large Table Chunking (chunk_large_tables=True):
      For tables in LARGE_STAGING_TABLES (Fact, Label, Element, etc.), loads
      data one quarter at a time to reduce memory pressure. Uses CREATE TABLE
      for first quarter, INSERT INTO for subsequent quarters.

    Args:
        tables: Dictionary mapping table names to entity type ("nodes" or "relationships")
        graph_client: Graph API client instance
        year: Optional year filter. If provided, uses filed=YYYY-* pattern.
        chunk_large_tables: If True, stage large tables quarter-by-quarter
            to reduce memory pressure. Defaults to SEC_LARGE_SCALE_MODE_ENABLED env var
            (False in dev, True in prod).

    Returns:
        Tuple of (successful_table_names, table_info_dict)
    """
    # Default to env var if not explicitly specified
    # SEC_LARGE_SCALE_MODE_ENABLED is False in dev (small data), True in prod
    if chunk_large_tables is None:
      chunk_large_tables = env.SEC_LARGE_SCALE_MODE_ENABLED

    successful_tables: list[str] = []
    table_infos: dict[str, TableInfo] = {}
    failed_tables: list[tuple[str, str]] = []
    skipped_tables: list[str] = []

    # Build partition patterns for glob
    # Uses quarterly partition structure: filed=YYYY-QN/nodes/TABLE.parquet
    if year:
      # Year filter for partial backfill (matches all quarters in that year)
      filed_pattern = f"filed={year}-Q*"
    else:
      # All quarters for full staging
      filed_pattern = "filed=*-Q*"

    # Helper for progress logging (both logger and optional callback)
    def log_progress(msg: str) -> None:
      logger.info(msg)
      if progress_callback:
        progress_callback(msg)

    # Discover quarterly partitions for chunked staging (only if needed)
    quarterly_partitions: list[str] | None = None
    if chunk_large_tables:
      # Discover quarterly partitions from S3
      quarterly_partitions = await self._discover_filed_partitions(year=year)
      if quarterly_partitions:
        logger.info(
          f"Quarter chunking enabled: {len(quarterly_partitions)} quarters discovered"
        )

    total_tables = len(tables)
    for i, (table_name, entity_type) in enumerate(tables.items(), 1):
      is_large = table_name in LARGE_STAGING_TABLES
      is_chunkable = table_name in QUARTER_CHUNKABLE_TABLES

      # Use quarter-based chunking for chunkable tables (if enabled and quarters discovered)
      # Only tables with per-filing data (no cross-quarter duplicates) can be chunked.
      if is_chunkable and quarterly_partitions and chunk_large_tables:
        # Per-quarter retry logic is inside _stage_large_table_by_quarters
        # No outer retry needed - that would restart from Q1 and waste completed work
        success, table_info, error = await self._stage_large_table_by_quarters(
          table_name=table_name,
          entity_type=entity_type,
          graph_client=graph_client,
          quarters=quarterly_partitions,
          log_progress=log_progress,
          table_index=i,
          total_tables=total_tables,
        )
        if success and table_info:
          successful_tables.append(table_name)
          table_infos[table_name] = table_info
        else:
          failed_tables.append((table_name, error or "Unknown error"))
        continue

      # Standard staging for small tables (or when chunking is disabled)
      # Build glob pattern for quarterly structure:
      # s3://bucket/sec/processed/filed=2024-Q*/nodes/Entity.parquet
      s3_pattern = (
        f"s3://{self.bucket}/{self.source_prefix}/"
        f"{filed_pattern}/{entity_type}/{table_name}.parquet"
      )

      # Get appropriate timeout for this table (large tables get extended timeout)
      timeout = _get_staging_timeout(table_name)
      size_hint = " (large table)" if is_large else ""
      log_progress(
        f"[{i}/{total_tables}] Staging {table_name}{size_hint} (timeout={timeout}s)..."
      )

      # Inner function for retry wrapper - returns (success, table_info, error)
      # Special case: "No files found" is treated as success (optional table)
      async def standard_stage_fn() -> tuple[bool, TableInfo | None, str | None]:
        try:
          # Use graph client to call Graph API's table creation endpoint
          # Pass glob pattern (string) instead of file list
          response = await graph_client.create_table(
            graph_id=self.graph_id,
            table_name=table_name,
            s3_pattern=s3_pattern,  # Glob pattern, not a file list
            timeout=timeout,
          )

          # Handle SSE-based response format
          if response.get("status") == "failed":
            error = response.get("error", "Unknown error")
            # "No files found" is expected for optional tables - return success with 0 rows
            if "No files found" in error:
              return (
                True,
                TableInfo(
                  name=table_name,
                  row_count=0,
                  file_count=0,
                  staged_at=datetime.now(UTC).isoformat(),
                  skipped=True,  # Mark as skipped for tracking
                ),
                None,
              )
            return False, None, error

          # Extract result from SSE response
          result = response.get("result", {})
          duration = response.get("duration_seconds", result.get("duration_seconds", 0))
          row_count = result.get("row_count", 0)

          log_progress(
            f"[{i}/{total_tables}] Staged {table_name}: {row_count:,} rows in {duration:.1f}s"
          )

          return (
            True,
            TableInfo(
              name=table_name,
              row_count=row_count,
              file_count=0,  # Unknown with glob pattern
              staged_at=datetime.now(UTC).isoformat(),
            ),
            None,
          )

        except Exception as e:
          error_str = str(e)
          # "No files found" from exceptions - return success with 0 rows
          if "No files found" in error_str:
            return (
              True,
              TableInfo(
                name=table_name,
                row_count=0,
                file_count=0,
                staged_at=datetime.now(UTC).isoformat(),
                skipped=True,
              ),
              None,
            )
          return False, None, error_str

      # Wrap standard staging with retry logic
      success, table_info, error = await self._stage_table_with_retry(
        table_name=table_name,
        stage_fn=standard_stage_fn,
        graph_client=graph_client,
        log_progress=log_progress,
        table_index=i,
        total_tables=total_tables,
      )

      if success and table_info:
        # Check if this was a skipped table (no files found)
        if table_info.skipped:
          log_progress(
            f"[{i}/{total_tables}] Skipped {table_name}: no files (optional)"
          )
          skipped_tables.append(table_name)
        successful_tables.append(table_name)
        table_infos[table_name] = table_info
      else:
        logger.error(f"Failed to create DuckDB table {table_name}: {error}")
        if progress_callback:
          progress_callback(f"[{i}/{total_tables}] FAILED {table_name}: {error}")
        failed_tables.append((table_name, error or "Unknown error"))

    # Report summary
    if skipped_tables:
      logger.info(
        f"Skipped {len(skipped_tables)} tables with no files: {skipped_tables}"
      )

    if failed_tables:
      logger.warning(
        f"DuckDB table creation: {len(successful_tables)} succeeded, "
        f"{len(failed_tables)} failed"
      )
      for table_name, error in failed_tables:
        logger.error(f"  Failed: {table_name} - {error}")

      # Raise after attempting all tables so we can see partial results
      raise RuntimeError(
        f"Failed to create {len(failed_tables)} DuckDB tables: "
        f"{[t[0] for t in failed_tables]}"
      )

    return successful_tables, table_infos

  async def _create_duckdb_tables_with_info(
    self,
    tables_info: dict[str, list[str]],
    graph_client,
  ) -> tuple[list[str], dict[str, TableInfo]]:
    """
    Create DuckDB staging tables and return detailed TableInfo for manifest.

    Returns TableInfo objects with row counts and timestamps for the staging manifest.

    Args:
        tables_info: Dictionary mapping table names to S3 keys
        graph_client: Graph API client instance

    Returns:
        Tuple of (successful_table_names, table_info_dict)

    Raises:
        RuntimeError: If any tables failed to create (after attempting all)
    """
    successful_tables: list[str] = []
    table_infos: dict[str, TableInfo] = {}
    failed_tables: list[tuple[str, str]] = []

    for table_name, s3_keys in tables_info.items():
      logger.info(f"Creating DuckDB table: {table_name} ({len(s3_keys)} files)")

      # Build list of full S3 URIs
      s3_files = [f"s3://{self.bucket}/{key}" for key in s3_keys]

      try:
        # Use graph client to call Graph API's table creation endpoint
        # Client uses SSE monitoring for long-running table creation
        response = await graph_client.create_table(
          graph_id=self.graph_id,
          table_name=table_name,
          s3_pattern=s3_files,  # Actually a list of files, not a pattern
          timeout=1800,  # 30 minutes for large file sets
        )

        # Handle SSE-based response format
        if response.get("status") == "failed":
          error = response.get("error", "Unknown error")
          logger.error(f"Failed to create DuckDB table {table_name}: {error}")
          failed_tables.append((table_name, error))
          continue

        # Extract result from SSE response
        result = response.get("result", {})
        duration = response.get("duration_seconds", result.get("duration_seconds", 0))
        row_count = result.get("row_count", 0)

        logger.info(
          f"Created DuckDB table {table_name} in {duration:.1f}s "
          f"(from {len(s3_keys)} files, {row_count} rows)"
        )

        successful_tables.append(table_name)
        table_infos[table_name] = TableInfo(
          name=table_name,
          row_count=row_count,
          file_count=len(s3_keys),
          staged_at=datetime.now(UTC).isoformat(),
        )

      except Exception as e:
        logger.error(f"Failed to create DuckDB table {table_name}: {e}")
        failed_tables.append((table_name, str(e)))
        continue

    # Report summary
    if failed_tables:
      logger.warning(
        f"DuckDB table creation: {len(successful_tables)} succeeded, "
        f"{len(failed_tables)} failed"
      )
      for table_name, error in failed_tables:
        logger.error(f"  Failed: {table_name} - {error}")

      # Raise after attempting all tables so we can see partial results
      raise RuntimeError(
        f"Failed to create {len(failed_tables)} DuckDB tables: "
        f"{[t[0] for t in failed_tables]}"
      )

    return successful_tables, table_infos

  async def _trigger_ingestion(
    self,
    table_names: list[str],
    graph_client,
    progress_callback: ProgressCallback | None = None,
  ) -> dict[str, Any]:
    """
    Trigger ingestion for all tables into LadybugDB graph via Graph API.

    For large tables (>100M rows), uses chunked materialization to avoid OOM.

    Args:
        table_names: List of table names to ingest
        graph_client: Graph API client instance
        progress_callback: Optional callback for progress logging (e.g., Dagster context.log.info)

    Returns:
        Ingestion results with statistics
    """

    # Helper for progress logging
    def log_progress(msg: str) -> None:
      logger.info(msg)
      if progress_callback:
        progress_callback(msg)

    total_rows = 0
    total_time_ms = 0.0
    results = []
    total_tables = len(table_names)

    for i, table_name in enumerate(table_names, 1):
      is_large = table_name in LARGE_STAGING_TABLES
      timeout = _get_materialization_timeout(table_name)

      try:
        # Get row count to determine if chunking is needed
        row_count = 0
        if is_large:
          try:
            count_response = await graph_client.query_table(
              graph_id=self.graph_id,
              sql=f"SELECT COUNT(*) FROM {table_name}",
            )
            if count_response.get("rows") and count_response["rows"][0]:
              row_count = count_response["rows"][0][0]  # First row, first column
          except Exception as count_err:
            logger.warning(f"Could not get row count for {table_name}: {count_err}")
            row_count = 0

        # Use hash-based batched materialization for very large tables to prevent OOM
        # Hash batching doesn't require sorted source tables (unlike LIMIT/OFFSET)
        if env.SEC_LARGE_SCALE_MODE_ENABLED and row_count > MATERIALIZATION_BATCH_SIZE:
          # Calculate number of batches needed (round up)
          num_batches = (
            row_count + MATERIALIZATION_BATCH_SIZE - 1
          ) // MATERIALIZATION_BATCH_SIZE
          log_progress(
            f"[{i}/{total_tables}] Materializing {table_name} in {num_batches} batches "
            f"({row_count:,} rows, hash-based)..."
          )

          table_rows = 0
          table_time_ms = 0.0

          for batch_num in range(num_batches):
            log_progress(
              f"  [{table_name}] Batch {batch_num + 1}/{num_batches} (hash-based)..."
            )

            response = await graph_client.materialize_table(
              graph_id=self.graph_id,
              table_name=table_name,
              ignore_errors=True,
              batch_num=batch_num,
              num_batches=num_batches,
              timeout=CHUNKED_MATERIALIZATION_TIMEOUT,
            )

            batch_rows = response.get("rows_ingested", 0)
            batch_time = response.get("execution_time_ms", 0)
            table_rows += batch_rows
            table_time_ms += batch_time

            log_progress(
              f"  [{table_name}] Batch {batch_num + 1}/{num_batches}: "
              f"{batch_rows:,} rows in {batch_time / 1000:.1f}s"
            )

          total_rows += table_rows
          total_time_ms += table_time_ms

          results.append(
            {
              "table_name": table_name,
              "rows_ingested": table_rows,
              "status": "success",
              "batches": num_batches,
            }
          )

          log_progress(
            f"[{i}/{total_tables}] Materialized {table_name}: "
            f"{table_rows:,} rows in {table_time_ms / 1000:.1f}s ({num_batches} batches)"
          )

        else:
          # Standard single-pass materialization for smaller tables
          size_hint = f" (large table, timeout={timeout:.0f}s)" if is_large else ""
          log_progress(f"[{i}/{total_tables}] Materializing {table_name}{size_hint}...")

          response = await graph_client.materialize_table(
            graph_id=self.graph_id,
            table_name=table_name,
            ignore_errors=True,
            timeout=timeout,
          )

          rows_ingested = response.get("rows_ingested", 0)
          exec_time_ms = response.get("execution_time_ms", 0)
          total_rows += rows_ingested
          total_time_ms += exec_time_ms

          results.append(
            {
              "table_name": table_name,
              "rows_ingested": rows_ingested,
              "status": response.get("status", "success"),
            }
          )

          log_progress(
            f"[{i}/{total_tables}] Materialized {table_name}: "
            f"{rows_ingested:,} rows in {exec_time_ms / 1000:.1f}s"
          )

      except Exception as e:
        logger.error(f"Failed to materialize table {table_name}: {e}")
        if progress_callback:
          progress_callback(f"[{i}/{total_tables}] FAILED {table_name}: {e}")
        results.append(
          {
            "table_name": table_name,
            "status": "error",
            "error": str(e),
          }
        )

    return {
      "total_rows_ingested": total_rows,
      "duration_ms": total_time_ms,
      "tables": results,
    }

  async def _discover_filed_partitions(self, year: int | None = None) -> list[str]:
    """
    Discover all filed= quarterly partitions from S3.

    Used for quarter-based chunking of large tables.

    Args:
        year: Optional year filter. If provided, only returns quarters from that year.

    Returns:
        Sorted list of quarterly partition keys (e.g., ["2024-Q1", "2024-Q2", ...])
    """
    partitions: list[str] = []
    prefix = f"{self.source_prefix}/"

    paginator = self.s3_client.s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter="/")

    for page in pages:
      if "CommonPrefixes" in page:
        for prefix_info in page["CommonPrefixes"]:
          prefix_path = prefix_info["Prefix"]
          if "filed=" in prefix_path:
            # Extract partition from "sec/processed/filed=2024-Q1/"
            filed_part = prefix_path.split("filed=")[1].rstrip("/")
            # Only accept quarterly format (YYYY-QN)
            if "-Q" not in filed_part:
              continue
            # Filter by year if specified
            if year and not filed_part.startswith(str(year)):
              continue
            partitions.append(filed_part)

    partitions.sort()
    logger.info(f"Discovered {len(partitions)} filed= quarterly partitions from S3")
    return partitions
