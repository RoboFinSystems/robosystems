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
from datetime import UTC, datetime
from typing import Any

from robosystems.adapters.sec.models.staging import (
  MaterializeResult,
  StagingResult,
  TableInfo,
)
from robosystems.config import env
from robosystems.config.storage.shared import (
  get_staging_duckdb_path,
)
from robosystems.graph_api.client.factory import get_graph_client
from robosystems.logger import logger
from robosystems.operations.aws.s3 import S3Client
from robosystems.schemas.extensions.roboledger import RoboLedgerContext

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
# For batched operations (50M rows), each batch is smaller so use shorter timeout
# Default: 600s (10 min), Large non-chunked: 1800s (30 min), Batched: 600s (10 min)
DEFAULT_MATERIALIZATION_TIMEOUT = 600  # 10 minutes
LARGE_MATERIALIZATION_TIMEOUT = 1800  # 30 minutes (for non-chunked large tables)
CHUNKED_MATERIALIZATION_TIMEOUT = 600  # 10 minutes per 50M row batch

# Chunked materialization settings for large tables
# Tables exceeding this row count will be materialized in batches to avoid OOM
MATERIALIZATION_BATCH_SIZE = 50_000_000  # 50M rows per batch
MATERIALIZATION_CHUNK_THRESHOLD = 100_000_000  # 100M rows triggers chunked mode

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
    "FACT_SET_CONTAINS_FACT",  # ~987M rows - FactSet -> Fact
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
# Note: FactSet/REPORT_HAS_FACT_SET excluded - only 1 per report, small tables.
QUARTER_CHUNKABLE_TABLES = frozenset(
  {
    # Fact nodes (high volume, benefit from chunking)
    "Fact",  # ~1B rows - unique per filing
    "FactDimension",  # ~76M rows - dimensional breakdowns per-fact
    # Fact relationships (all per-fact, safe to chunk)
    "REPORT_HAS_FACT",  # Report -> Fact
    "FACT_HAS_ELEMENT",  # Fact -> Element
    "FACT_HAS_ENTITY",  # Fact -> Entity
    "FACT_HAS_PERIOD",  # Fact -> Period
    "FACT_HAS_UNIT",  # Fact -> Unit
    "FACT_HAS_DIMENSION",  # Fact -> FactDimension
    "FACT_DIMENSION_AXIS_ELEMENT",  # FactDimension -> Element (axis)
    "FACT_DIMENSION_MEMBER_ELEMENT",  # ~70M rows - FactDimension -> Element (member)
    "FACT_SET_CONTAINS_FACT",  # ~987M rows - FactSet -> Fact
    "FACT_REPORTS_ELEMENT",  # Legacy name for FACT_HAS_ELEMENT
    # Structure and Association nodes (filing-specific, not shared across filings)
    # Structure ID includes accession_number, Association ID is random UUID
    "Structure",  # ~7M rows - per-filing presentation/calculation structures
    "Association",  # ~206M rows - per-filing element relationships
    # Structure/Association relationships
    "STRUCTURE_HAS_TAXONOMY",  # Structure -> Taxonomy
    "STRUCTURE_HAS_ASSOCIATION",  # ~200M rows - Structure -> Association
    "ASSOCIATION_HAS_FROM_ELEMENT",  # ~206M rows - Association -> Element (parent)
    "ASSOCIATION_HAS_TO_ELEMENT",  # ~206M rows - Association -> Element (child)
  }
)

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


def _group_dates_by_quarter(dates: list[str]) -> dict[str, list[str]]:
  """Group filing dates by quarter.

  Args:
      dates: List of filing dates in YYYY-MM-DD format

  Returns:
      Dictionary mapping quarter keys (e.g., "2024-Q1") to list of dates in that quarter.
      Sorted by quarter key.
  """
  quarters: dict[str, list[str]] = {}
  for date_str in dates:
    try:
      year, month, _ = date_str.split("-")
      quarter = (int(month) - 1) // 3 + 1
      quarter_key = f"{year}-Q{quarter}"
      if quarter_key not in quarters:
        quarters[quarter_key] = []
      quarters[quarter_key].append(date_str)
    except (ValueError, IndexError):
      logger.warning(f"Skipping invalid date format: {date_str}")
      continue
  # Sort by quarter key
  return dict(sorted(quarters.items()))


def _get_quarter_glob_pattern(
  quarter_key: str, use_quarter_end: bool = True
) -> list[str]:
  """Convert quarter key to glob patterns for filed= partitions.

  Args:
      quarter_key: Quarter in format "YYYY-QN" (e.g., "2024-Q1")
      use_quarter_end: If True (default), use quarter-end date pattern (filed=2024-03-31)
                       which matches the default processing output.
                       If False, use month patterns for daily-partitioned data.

  Returns:
      If use_quarter_end=True: Single pattern like ["filed=2024-03-31"]
      If use_quarter_end=False: Month patterns like ["filed=2024-01-*", ...]
  """
  year, q = quarter_key.split("-Q")
  quarter_num = int(q)

  if use_quarter_end:
    # Default: files are stored at quarter-end date (e.g., filed=2024-03-31 for Q1)
    quarter_end_dates = {
      1: f"{year}-03-31",
      2: f"{year}-06-30",
      3: f"{year}-09-30",
      4: f"{year}-12-31",
    }
    return [f"filed={quarter_end_dates[quarter_num]}"]
  else:
    # Daily mode: files distributed across the quarter by actual filing date
    start_month = (quarter_num - 1) * 3 + 1
    months = [start_month, start_month + 1, start_month + 2]
    return [f"filed={year}-{m:02d}-*" for m in months]


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
        reset_staging: If True, delete DuckDB file for a fresh start.
            If False (default), preserve DuckDB for retry scenarios.
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
          duration_seconds=time.time() - start_time,
        )

      # Reset staging if requested - delete all DuckDB tables before staging new ones
      if reset_staging:
        log_progress("Resetting DuckDB staging - clearing existing tables...")
        try:
          existing_tables = await client.list_tables(self.graph_id)
          if existing_tables:
            for table_info in existing_tables:
              table_name = (
                table_info.get("name")
                if isinstance(table_info, dict)
                else getattr(table_info, "name", None)
              )
              if table_name:
                await client.delete_table(self.graph_id, table_name)
                logger.debug(f"Deleted staging table: {table_name}")
            log_progress(f"Cleared {len(existing_tables)} existing staging tables")
          else:
            log_progress("No existing staging tables to clear")
        except Exception as reset_err:
          # Non-fatal - tables might not exist yet
          logger.warning(f"Could not reset staging tables: {reset_err}")
          log_progress(f"Reset skipped (tables may not exist): {reset_err}")

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
          duration_seconds=time.time() - start_time,
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
            duration_seconds=time.time() - start_time,
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
        duration_seconds=duration,
        duckdb_path=duckdb_path,
      )

    except Exception as e:
      logger.error(f"DuckDB staging failed: {e}", exc_info=True)
      return StagingResult(
        status="error",
        table_names=[],
        error=str(e),
        duration_seconds=time.time() - start_time,
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

    def log_progress(msg: str) -> None:
      logger.info(msg)
      if progress_callback:
        progress_callback(msg)

    logger.info(
      f"Starting incremental DuckDB staging for graph {self.graph_id} "
      f"(year={year}, Q{quarter})"
    )

    try:
      client = await get_graph_client(graph_id=self.graph_id, operation_type="write")

      # Verify tables exist (must have done initial full staging)
      existing_tables = await client.list_tables(self.graph_id)
      if not existing_tables:
        return StagingResult(
          status="error",
          table_names=[],
          error="No existing DuckDB tables. Run full staging first.",
          duration_seconds=time.time() - start_time,
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

      # Build quarter date pattern for glob
      # Q1: 01-*, 02-*, 03-*  Q2: 04-*, 05-*, 06-*  etc.
      quarter_months = {
        1: ["01", "02", "03"],
        2: ["04", "05", "06"],
        3: ["07", "08", "09"],
        4: ["10", "11", "12"],
      }
      months = quarter_months[quarter]

      # Stage each table incrementally
      successful_tables: list[str] = []
      table_infos: dict[str, TableInfo] = {}
      failed_tables: list[tuple[str, str]] = []

      total_tables = len(tables_by_type)
      for i, (table_name, entity_type) in enumerate(tables_by_type.items(), 1):
        # Build S3 patterns for all months in the quarter
        s3_patterns = [
          f"s3://{self.bucket}/{self.source_prefix}/filed={year}-{month}-*/{entity_type}/{table_name}/*.parquet"
          for month in months
        ]

        timeout = _get_staging_timeout(table_name)
        log_progress(
          f"[{i}/{total_tables}] INSERT {table_name} (Q{quarter} {year})..."
        )

        try:
          # INSERT INTO existing table (with dedup)
          response = await client.insert_into_table(
            graph_id=self.graph_id,
            table_name=table_name,
            s3_pattern=s3_patterns,
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
        duration_seconds=duration,
        duckdb_path=get_staging_duckdb_path(self.graph_id),
      )

    except Exception as e:
      logger.error(f"Incremental staging failed: {e}", exc_info=True)
      return StagingResult(
        status="error",
        table_names=[],
        error=str(e),
        duration_seconds=time.time() - start_time,
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
        table_names, client, rebuild=False, progress_callback=log_progress
      )

      duration = time.time() - start_time

      log_progress(
        f"Materialization complete in {duration:.2f}s: "
        f"{ingestion_results.get('total_rows_ingested', 0):,} rows ingested"
      )
      return MaterializeResult(
        status="success",
        total_rows_ingested=ingestion_results.get("total_rows_ingested", 0),
        total_time_ms=ingestion_results.get("total_time_ms", 0),
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
    graph_client,
    quarters: dict[str, list[str]],
    log_progress: Callable[[str], None],
    table_index: int,
    total_tables: int,
  ) -> tuple[bool, TableInfo | None, str | None]:
    """
    Stage a large table by loading one quarter at a time to reduce memory pressure.

    For tables like Fact (100M+ rows), loading all data at once can exceed memory.
    This method chunks the load by quarter:
    - First quarter: CREATE TABLE (with deduplication)
    - Subsequent quarters: INSERT INTO (merge + dedupe incrementally)

    Both operations use ROW_NUMBER() OVER (PARTITION BY identifier) for deduplication,
    so the final table is always deduplicated regardless of how many quarters are loaded.

    Args:
        table_name: Name of the table to stage
        entity_type: "nodes" or "relationships"
        graph_client: Graph API client instance
        quarters: Dict mapping quarter keys ("2024-Q1") to filing dates
        log_progress: Progress logging callback
        table_index: Current table index for progress display
        total_tables: Total number of tables for progress display

    Returns:
        Tuple of (success, TableInfo or None, error message or None)
    """
    # Use chunked timeout since we're processing quarter-by-quarter, not the full table
    timeout = CHUNKED_STAGING_TIMEOUT
    total_rows = 0
    total_duration = 0.0
    quarter_list = list(quarters.keys())

    log_progress(
      f"[{table_index}/{total_tables}] Staging {table_name} by quarter "
      f"({len(quarter_list)} quarters)..."
    )

    for q_idx, quarter_key in enumerate(quarter_list):
      # Use actual discovered dates for this quarter (not regenerated quarter-end dates)
      # This handles both quarterly partitions (filed=2024-03-31) and daily (filed=2026-01-02)
      actual_dates = quarters[quarter_key]

      # Build S3 patterns for each actual date in this quarter
      s3_patterns = [
        f"s3://{self.bucket}/{self.source_prefix}/filed={date}/{entity_type}/{table_name}/*.parquet"
        for date in actual_dates
      ]

      is_first = q_idx == 0
      operation = "CREATE" if is_first else "INSERT"

      log_progress(
        f"  [{quarter_key}] {operation} {table_name} "
        f"(quarter {q_idx + 1}/{len(quarter_list)})..."
      )

      try:
        if is_first:
          # First quarter: CREATE TABLE
          response = await graph_client.create_table(
            graph_id=self.graph_id,
            table_name=table_name,
            s3_pattern=s3_patterns,  # List of patterns for the quarter
            timeout=timeout,
          )
        else:
          # Subsequent quarters: INSERT INTO (append)
          response = await graph_client.insert_into_table(
            graph_id=self.graph_id,
            table_name=table_name,
            s3_pattern=s3_patterns,
            timeout=timeout,
          )

        if response.get("status") == "failed":
          error = response.get("error", "Unknown error")
          if "No files found" in error:
            # No data for this quarter - that's OK, continue
            log_progress(f"  [{quarter_key}] No files (skipped)")
            continue
          return False, None, f"Quarter {quarter_key}: {error}"

        result = response.get("result", {})
        duration = response.get("duration_seconds", result.get("duration_seconds", 0))
        row_count = result.get("row_count", 0)
        total_rows += row_count
        total_duration += duration

        log_progress(
          f"  [{quarter_key}] {row_count:,} rows in {duration:.1f}s "
          f"(cumulative: {total_rows:,})"
        )

      except Exception as e:
        error_str = str(e)
        if "No files found" in error_str:
          log_progress(f"  [{quarter_key}] No files (skipped)")
          continue
        return False, None, f"Quarter {quarter_key}: {error_str}"

    log_progress(
      f"[{table_index}/{total_tables}] Staged {table_name}: "
      f"{total_rows:,} rows in {total_duration:.1f}s (chunked by quarter)"
    )

    return (
      True,
      TableInfo(
        name=table_name,
        row_count=total_rows,
        file_count=0,
        staged_at=datetime.now(UTC).isoformat(),
      ),
      None,
    )

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
    # Uses filed=YYYY-MM-DD partition structure with part files
    # Pattern: sec/processed/filed=YYYY-MM-DD/nodes/Table/*.parquet
    if year:
      # Year filter for partial backfill (matches all dates in that year)
      filed_pattern = f"filed={year}-*"
    else:
      # All dates for full staging
      filed_pattern = "filed=*"

    # Helper for progress logging (both logger and optional callback)
    def log_progress(msg: str) -> None:
      logger.info(msg)
      if progress_callback:
        progress_callback(msg)

    # Discover quarters for chunked staging (only if needed)
    quarters_by_date: dict[str, list[str]] | None = None
    if chunk_large_tables:
      # Discover filing dates and group by quarter
      filing_dates = await self._discover_filed_dates(year=year)
      if filing_dates:
        quarters_by_date = _group_dates_by_quarter(filing_dates)
        logger.info(
          f"Quarter chunking enabled: {len(quarters_by_date)} quarters discovered"
        )

    total_tables = len(tables)
    for i, (table_name, entity_type) in enumerate(tables.items(), 1):
      is_large = table_name in LARGE_STAGING_TABLES
      is_chunkable = table_name in QUARTER_CHUNKABLE_TABLES

      # Use quarter-based chunking for chunkable tables (if enabled and quarters discovered)
      # Only tables with per-filing data (no cross-quarter duplicates) can be chunked.
      if is_chunkable and quarters_by_date and chunk_large_tables:
        # Wrap chunked staging with retry logic - on failure, retry entire table
        async def chunked_stage_fn() -> tuple[bool, TableInfo | None, str | None]:
          return await self._stage_large_table_by_quarters(
            table_name=table_name,
            entity_type=entity_type,
            graph_client=graph_client,
            quarters=quarters_by_date,
            log_progress=log_progress,
            table_index=i,
            total_tables=total_tables,
          )

        success, table_info, error = await self._stage_table_with_retry(
          table_name=table_name,
          stage_fn=chunked_stage_fn,
          graph_client=graph_client,
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
      # Build glob pattern for part file output structure:
      # s3://bucket/sec/processed/filed=*/nodes/Entity/*.parquet
      s3_pattern = (
        f"s3://{self.bucket}/{self.source_prefix}/"
        f"{filed_pattern}/{entity_type}/{table_name}/*.parquet"
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

    This is an enhanced version of _create_duckdb_tables() that also returns
    TableInfo objects with row counts and timestamps for the staging manifest.

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

  async def _create_duckdb_tables(
    self,
    tables_info: dict[str, list[str]],
    graph_client,
  ) -> list[str]:
    """
    Create DuckDB staging tables for each discovered table via Graph API.

    Uses SSE monitoring to handle long-running table creation from thousands
    of S3 files without HTTP timeout issues. Tables are created sequentially
    to avoid overwhelming the instance.

    Continues processing remaining tables on failure to maximize debugging info
    at scale. Failed tables are logged and reported at the end.

    Args:
        tables_info: Dictionary mapping table names to S3 keys
        graph_client: Graph API client instance

    Returns:
        List of successfully created table names

    Raises:
        RuntimeError: If any tables failed to create (after attempting all)
    """
    successful_tables: list[str] = []
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

        logger.info(
          f"Created DuckDB table {table_name} in {duration:.1f}s "
          f"(from {len(s3_keys)} files)"
        )
        successful_tables.append(table_name)

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

    return successful_tables

  async def _trigger_ingestion(
    self,
    table_names: list[str],
    graph_client,
    rebuild: bool = False,  # Kept for API compatibility
    progress_callback: ProgressCallback | None = None,
  ) -> dict[str, Any]:
    """
    Trigger ingestion for all tables into LadybugDB graph via Graph API.

    For large tables (>100M rows), uses chunked materialization to avoid OOM.

    Args:
        table_names: List of table names to ingest
        graph_client: Graph API client instance
        rebuild: Ignored - rebuild is now handled in process_files before table creation
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

        # Use chunked materialization for very large tables (only in large-scale mode)
        if (
          env.SEC_LARGE_SCALE_MODE_ENABLED
          and row_count > MATERIALIZATION_CHUNK_THRESHOLD
        ):
          num_batches = (
            row_count + MATERIALIZATION_BATCH_SIZE - 1
          ) // MATERIALIZATION_BATCH_SIZE
          log_progress(
            f"[{i}/{total_tables}] Materializing {table_name} in {num_batches} batches "
            f"({row_count:,} rows, {MATERIALIZATION_BATCH_SIZE:,} per batch)..."
          )

          table_rows = 0
          table_time_ms = 0.0

          for batch_num in range(num_batches):
            offset = batch_num * MATERIALIZATION_BATCH_SIZE
            log_progress(
              f"  [{table_name}] Batch {batch_num + 1}/{num_batches} "
              f"(offset={offset:,})..."
            )

            response = await graph_client.materialize_table(
              graph_id=self.graph_id,
              table_name=table_name,
              ignore_errors=True,
              batch_size=MATERIALIZATION_BATCH_SIZE,
              offset=offset,
              timeout=CHUNKED_MATERIALIZATION_TIMEOUT,
            )

            batch_rows = response.get("rows_ingested", 0)
            batch_time = response.get("execution_time_ms", 0)
            table_rows += batch_rows
            table_time_ms += batch_time

            log_progress(
              f"  [{table_name}] Batch {batch_num + 1}/{num_batches}: "
              f"{batch_rows:,} rows in {batch_time:.0f}ms"
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
            f"{table_rows:,} rows in {table_time_ms:.0f}ms ({num_batches} batches)"
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
            f"{rows_ingested:,} rows in {exec_time_ms:.0f}ms"
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
      "total_time_ms": total_time_ms,
      "tables": results,
    }

  async def _discover_filed_dates(self, year: int | None = None) -> list[str]:
    """
    Discover all filed= partition dates from S3.

    Used for quarter-based chunking of large tables.

    Args:
        year: Optional year filter. If provided, only returns dates from that year.

    Returns:
        Sorted list of filing dates (YYYY-MM-DD strings)
    """
    filed_dates: list[str] = []
    prefix = f"{self.source_prefix}/"

    paginator = self.s3_client.s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter="/")

    for page in pages:
      if "CommonPrefixes" in page:
        for prefix_info in page["CommonPrefixes"]:
          prefix_path = prefix_info["Prefix"]
          if "filed=" in prefix_path:
            # Extract date from "sec/processed/filed=2026-01-15/"
            filed_part = prefix_path.split("filed=")[1].rstrip("/")
            # Filter by year if specified
            if year and not filed_part.startswith(str(year)):
              continue
            filed_dates.append(filed_part)

    filed_dates.sort()
    logger.info(f"Discovered {len(filed_dates)} filed= dates from S3")
    return filed_dates
