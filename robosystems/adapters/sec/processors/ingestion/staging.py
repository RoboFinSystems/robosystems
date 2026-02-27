"""
DuckDB Staging Operations for XBRL Graph Ingestion.

This module handles Stage 1 of the ingestion pipeline: staging processed
Parquet files from S3 into DuckDB tables. The staged data can then be
materialized to LadybugDB using the materialization module.

Key features:
- Schema-driven: Table names come from RoboLedgerContext
- Glob patterns: Efficient file discovery via DuckDB (not S3 ListObjects)
- Spill-to-disk: DuckDB external aggregation handles large tables efficiently
- Retry logic: Automatic retries with backoff for transient failures

Classes:
    DuckDBStager: Handles all DuckDB staging operations
"""

import asyncio
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
  from robosystems.graph_api.client.client import GraphClient

from robosystems.config import env
from robosystems.config.storage.shared import get_staging_duckdb_path
from robosystems.graph_api.client.factory import get_graph_client
from robosystems.logger import logger
from robosystems.operations.aws.s3 import S3Client
from robosystems.schemas.extensions.roboledger import RoboLedgerContext

from .models import (
  LARGE_STAGING_TABLES,
  STAGING_MAX_RETRIES,
  STAGING_RETRY_BACKOFF_BASE,
  TAXONOMY_STRUCTURE_TABLES,
  ProgressCallback,
  StagingResult,
  TableInfo,
  get_staging_timeout,
  make_progress_logger,
  s3_get_table_patterns,
)


class DuckDBStager:
  """
  DuckDB staging operations for XBRL graph data.

  This class handles Stage 1 of the ingestion pipeline - staging processed
  Parquet files from S3 into DuckDB tables.

  Architecture:
  - Uses Graph API client to communicate with Graph API container
  - DuckDB pool lives on Graph API side, not on worker
  - Supports both full rebuild and incremental staging modes
  """

  def __init__(self, graph_id: str = "sec", source_prefix: str | None = None):
    """
    Initialize DuckDB stager.

    Args:
        graph_id: Graph database identifier (default: "sec")
        source_prefix: S3 prefix for source files (default: "sec/processed")
    """
    self.graph_id = graph_id
    self.s3_client = S3Client()
    self.bucket = env.SHARED_PROCESSED_BUCKET
    # New structure: sec/processed/filed=YYYY-MM-DD/nodes/TABLE/*.parquet
    self.source_prefix = source_prefix or "sec/processed"

  async def stage_to_duckdb(
    self,
    year: int | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    reset_staging: bool = False,
    skip_taxonomy_relationships: bool = False,
    use_glob: bool = True,
    duckdb_memory_mb: int | None = None,
    progress_callback: ProgressCallback | None = None,
  ) -> StagingResult:
    """
    Stage processed Parquet files to persistent DuckDB.

    This is Stage 1 of the decoupled pipeline. It ONLY stages data to DuckDB.
    LadybugDB operations are handled separately by materialize_from_duckdb().

    Schema-Driven Design:
    - Table names come from RoboLedgerContext.get_all_table_names_for_context()
    - No manifest file needed - schema is the source of truth

    Full Rebuild Mode:
    - Always recreates DuckDB tables from all S3 parquet files (CREATE TABLE)
    - No incremental mode - pipeline always rebuilds from scratch

    Memory Management:
    - Uses DuckDB's external aggregation with spill-to-disk for large tables
    - GROUP BY + FIRST() deduplication is more memory-efficient than ROW_NUMBER
    - Large tables (LARGE_STAGING_TABLES) use per-quarter chunked staging to
      avoid OOM when S3 data exceeds DuckDB's memory limit

    Args:
        year: Optional single year filter. If provided, only files from that year.
        start_year: Optional start of year range (inclusive). Used with end_year.
        end_year: Optional end of year range (inclusive). Used with start_year.
        reset_staging: If True, delete entire DuckDB staging database first.
        skip_taxonomy_relationships: If True, skip taxonomy structure tables.
        use_glob: If True (default), use glob patterns for efficient discovery.
        progress_callback: Optional callback for progress logging.

    Returns:
        StagingResult with table counts and file counts
    """
    start_time = time.time()
    log_progress = make_progress_logger(progress_callback)

    # Determine date filter for logging
    if year:
      date_filter = f"{year}-*"
    elif start_year or end_year:
      date_filter = f"{start_year or '?'}-{end_year or '?'}"
    else:
      date_filter = "all"
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

        # With glob, we don't know file count upfront
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

        # Filter out taxonomy structure tables if requested
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
        successful_tables, table_infos = await self._create_tables_with_glob(
          tables_by_type,
          client,
          year=year,
          start_year=start_year,
          end_year=end_year,
          duckdb_memory_mb=duckdb_memory_mb,
          progress_callback=log_progress,
        )
      else:
        log_progress(
          "Step 2: Creating DuckDB staging tables via file lists (legacy)..."
        )
        successful_tables, table_infos = await self._create_tables_with_info(
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
    from robosystems.adapters.sec import (
      get_current_quarter,
      get_previous_quarter,
      is_in_quarter_overlap_window,
    )

    start_time = time.time()
    log_progress = make_progress_logger(progress_callback)

    # Default to current year/quarter
    now = datetime.now(UTC)
    if year is None or quarter is None:
      year, quarter = get_current_quarter(now)

    # Build list of quarters to scan (current + previous during overlap period)
    quarters_to_scan: list[tuple[int, int]] = [(year, quarter)]

    if is_in_quarter_overlap_window(now):
      prev_year, prev_quarter = get_previous_quarter(year, quarter)
      quarters_to_scan.append((prev_year, prev_quarter))

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

      existing_table_names = {
        t.get("table_name", t) if isinstance(t, dict) else t for t in existing_tables
      }
      log_progress(f"Found {len(existing_table_names)} existing tables in DuckDB")

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

      # Stage each table incrementally
      successful_tables: list[str] = []
      table_infos: dict[str, TableInfo] = {}
      failed_tables: list[tuple[str, str]] = []

      total_tables = len(tables_by_type)
      for i, (table_name, entity_type) in enumerate(tables_by_type.items(), 1):
        # Skip tables that don't exist in DuckDB - no data for them
        if table_name not in existing_table_names:
          log_progress(
            f"[{i}/{total_tables}] Skipped {table_name}: not in DuckDB (no data)"
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

        # Build S3 patterns for all quarters, only including formats that exist.
        # s3_get_table_patterns checks each format individually to avoid DuckDB
        # errors from literal paths (no wildcards) that don't exist on S3.
        s3_patterns: list[str] = []
        for y, q in quarters_to_scan:
          filed_pattern = f"filed={y}-Q{q}"
          s3_patterns.extend(
            s3_get_table_patterns(
              self.s3_client,
              self.bucket,
              self.source_prefix,
              filed_pattern,
              entity_type,
              table_name,
            )
          )

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

        s3_pattern: str | list[str] = (
          s3_patterns[0] if len(s3_patterns) == 1 else s3_patterns
        )

        timeout = get_staging_timeout(table_name)
        log_progress(f"[{i}/{total_tables}] INSERT {table_name} (Q{quarter} {year})...")

        async def incremental_insert_fn() -> tuple[bool, TableInfo | None, str | None]:
          try:
            response = await client.insert_into_table(
              graph_id=self.graph_id,
              table_name=table_name,
              s3_pattern=s3_pattern,
              timeout=timeout,
            )

            if response.get("status") == "failed":
              error = response.get("error", "Unknown error")
              if "No files found" in error:
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
              return False, None, error

            result_data = response.get("result", {})
            row_count = result_data.get("row_count", 0)
            duration = response.get("duration_seconds", 0)

            log_progress(
              f"[{i}/{total_tables}] Inserted {table_name}: "
              f"{row_count:,} net new rows in {duration:.1f}s"
            )

            return (
              True,
              TableInfo(
                name=table_name,
                row_count=row_count,
                file_count=0,
                staged_at=datetime.now(UTC).isoformat(),
              ),
              None,
            )

          except Exception as e:
            error_str = str(e)
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

        success, table_info, error = await self._stage_table_with_retry(
          table_name=table_name,
          stage_fn=incremental_insert_fn,
          graph_client=client,
          log_progress=log_progress,
          table_index=i,
          total_tables=total_tables,
        )

        if success and table_info:
          successful_tables.append(table_name)
          table_infos[table_name] = table_info
        else:
          failed_tables.append((table_name, error or "Unknown error"))

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

  # =========================================================================
  # Private Helper Methods
  # =========================================================================

  # Default chunking threshold when DuckDB memory info is unavailable.
  # 20 GiB is conservative — Element (19 GiB) passes, Label (51 GiB) chunks.
  DEFAULT_CHUNKING_THRESHOLD_BYTES = 20 * 1024 * 1024 * 1024  # 20 GiB

  # Fraction of DuckDB memory to use as chunking threshold.
  # The hierarchical merge (quarterly → yearly → final) bounds peak memory
  # per merge step to ~4 quarters, so 75% is safe — single-shot staging
  # only runs when total S3 data fits comfortably in DuckDB's allocation.
  CHUNKING_MEMORY_FRACTION = 0.75

  def _get_chunking_threshold_bytes(self, duckdb_memory_mb: int | None) -> int:
    """
    Calculate the S3 data size threshold for chunked staging.

    If DuckDB memory is known (from boost response), threshold is
    CHUNKING_MEMORY_FRACTION of that. Otherwise falls back to a conservative
    20 GiB default.

    Args:
        duckdb_memory_mb: DuckDB memory limit in MB (from boost response), or None

    Returns:
        Threshold in bytes — tables with more S3 data than this get chunked
    """
    if duckdb_memory_mb and duckdb_memory_mb > 0:
      threshold = int(duckdb_memory_mb * 1024 * 1024 * self.CHUNKING_MEMORY_FRACTION)
      logger.info(
        f"Chunking threshold: {threshold / (1024**3):.1f} GiB "
        f"({int(self.CHUNKING_MEMORY_FRACTION * 100)}% of {duckdb_memory_mb}MB DuckDB memory)"
      )
      return threshold
    logger.info(
      f"Chunking threshold: {self.DEFAULT_CHUNKING_THRESHOLD_BYTES / (1024**3):.0f} GiB (default)"
    )
    return self.DEFAULT_CHUNKING_THRESHOLD_BYTES

  def _get_table_s3_size_bytes(
    self,
    entity_type: str,
    table_name: str,
    start_year: int,
    end_year: int,
  ) -> int:
    """
    Get total S3 parquet size for a table across quarterly partitions.

    Uses S3 ListObjectsV2 to sum file sizes. Queries per-year prefixes
    to avoid listing the entire bucket.

    Args:
        entity_type: "nodes" or "relationships"
        table_name: Table name (e.g., "Label", "Element")
        start_year: First year to check
        end_year: Last year to check (inclusive)

    Returns:
        Total size in bytes
    """
    total_bytes = 0
    boto_client = self.s3_client.s3_client

    for y in range(start_year, end_year + 1):
      for q in range(1, 5):
        prefix = f"{self.source_prefix}/filed={y}-Q{q}/{entity_type}/{table_name}/"
        try:
          paginator = boto_client.get_paginator("list_objects_v2")
          for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
              total_bytes += obj.get("Size", 0)
        except Exception:
          # Non-fatal — if we can't check size, skip this partition
          continue

    return total_bytes

  async def _discover_processed_files(
    self, year: int | None = None
  ) -> dict[str, list[str]]:
    """
    Discover processed Parquet files from S3 (legacy mode).

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
      year_prefix = f"{self.source_prefix}/"
      logger.info(f"Discovering year subdirectories in {self.bucket}/{year_prefix}")

      paginator = self.s3_client.s3_client.get_paginator("list_objects_v2")
      pages = paginator.paginate(Bucket=self.bucket, Prefix=year_prefix, Delimiter="/")

      years_to_scan = []
      for page in pages:
        if "CommonPrefixes" in page:
          for prefix_info in page["CommonPrefixes"]:
            prefix_path = prefix_info["Prefix"]
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
      years_to_scan = [year]

    # Scan both nodes and relationships directories
    for entity_type in ["nodes", "relationships"]:
      for scan_year in years_to_scan:
        prefix = f"{self.source_prefix}/year={scan_year}/{entity_type}/"
        logger.debug(f"Scanning S3 bucket {self.bucket} with prefix {prefix}")

        paginator = self.s3_client.s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

        for page in pages:
          if "Contents" not in page:
            continue

          for obj in page["Contents"]:
            key = obj["Key"]

            if not key.endswith(".parquet"):
              continue

            path_parts = key.replace(prefix, "").split("/")

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
    stage_fn,
    graph_client: "GraphClient",
    log_progress: ProgressCallback,
    table_index: int,
    total_tables: int,
  ) -> tuple[bool, TableInfo | None, str | None]:
    """
    Retry wrapper for table staging with exponential backoff.

    On failure, drops the partial table and retries from scratch.

    Args:
        table_name: Name of the table being staged
        stage_fn: Async callable that returns (success, info, error)
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

      if attempt < STAGING_MAX_RETRIES - 1:
        backoff = STAGING_RETRY_BACKOFF_BASE * (attempt + 1)
        log_progress(
          f"[{table_index}/{total_tables}] {table_name} failed: {error}. "
          f"Retry {attempt + 2}/{STAGING_MAX_RETRIES} in {backoff}s..."
        )

        # Drop partial table before retry
        try:
          await graph_client.delete_table(self.graph_id, table_name)
          logger.debug(f"Dropped partial table {table_name} before retry")
        except Exception as drop_err:
          logger.debug(f"Could not drop table {table_name} before retry: {drop_err}")

        # Re-apply DuckDB memory boost before retry. The boost is stored in an
        # in-memory dict on the Graph API — if the container restarted (OOM kill,
        # health check failure), the override is lost and new connections get the
        # default 10GB limit instead of the boosted 55GB. This is idempotent.
        try:
          await graph_client.boost_memory(self.graph_id, target="duckdb")
          logger.info(
            f"Re-verified DuckDB memory boost for {self.graph_id} before retry"
          )
        except Exception as boost_err:
          logger.warning(f"Could not re-verify memory boost before retry: {boost_err}")

        await asyncio.sleep(backoff)
      else:
        log_progress(
          f"[{table_index}/{total_tables}] {table_name} failed after "
          f"{STAGING_MAX_RETRIES} attempts: {error}"
        )

    return False, None, f"Failed after {STAGING_MAX_RETRIES} attempts: {last_error}"

  def _build_dedup_merge_sql(
    self,
    target_table: str,
    source_tables: list[str],
    columns: list[str],
  ) -> str:
    """
    Build a CREATE OR REPLACE TABLE ... AS SELECT ... GROUP BY SQL statement
    that deduplicates rows from multiple source tables via UNION ALL.

    Args:
        target_table: Name of the target table to create
        source_tables: List of source table names to union
        columns: Column names from schema probe

    Returns:
        SQL string (without SET threads prefix — caller adds that)
    """
    union_parts = [f'SELECT * FROM "{t}"' for t in source_tables]
    union_sql = " UNION ALL ".join(union_parts)

    if "identifier" in columns:
      # Node table: dedup on identifier
      other_cols = [c for c in columns if c != "identifier"]
      select_parts = ['"identifier"'] + [f'FIRST("{c}") AS "{c}"' for c in other_cols]
      return (
        f'CREATE OR REPLACE TABLE "{target_table}" AS '
        f"SELECT {', '.join(select_parts)} "
        f"FROM ({union_sql}) "
        f'GROUP BY "identifier"'
      )
    elif "src" in columns and "dst" in columns:
      # Relationship table (already renamed from/to to src/dst)
      other_cols = [c for c in columns if c not in ("src", "dst")]
      select_parts = ['"src"', '"dst"'] + [f'FIRST("{c}") AS "{c}"' for c in other_cols]
      return (
        f'CREATE OR REPLACE TABLE "{target_table}" AS '
        f"SELECT {', '.join(select_parts)} "
        f"FROM ({union_sql}) "
        f'GROUP BY "src", "dst"'
      )
    else:
      # Unknown schema — just union without dedup
      return f'CREATE OR REPLACE TABLE "{target_table}" AS SELECT * FROM ({union_sql})'

  async def _stage_table_chunked(
    self,
    table_name: str,
    entity_type: str,
    chunk_start_year: int,
    chunk_end_year: int,
    graph_client: "GraphClient",
    log_progress: ProgressCallback,
    table_index: int,
    total_tables: int,
  ) -> tuple[bool, TableInfo | None, str | None]:
    """
    Stage a large table using hierarchical temp-table merging.

    Quarterly temp tables → year tables (cross-quarter dedup) → final table.
    Each intermediate merge drastically reduces row count by eliminating
    cross-partition duplicates (e.g., same XBRL labels across filings).

    Steps:
    1. CREATE TABLE {table}__YYYY_QN for each quarter (S3 download + per-quarter dedup)
    2. Merge quarterly tables into year tables with GROUP BY dedup (cross-quarter dedup)
       - Drops quarterly temps after each year merge to free disk space
    3. Final merge of year tables into target table (cross-year dedup)
    4. DROP remaining intermediate tables

    This hierarchical approach keeps peak merge input at ~10M rows
    instead of 18M+ in a single-pass merge.

    Args:
        table_name: Name of the table to stage
        entity_type: Entity type ("nodes" or "relationships")
        chunk_start_year: First year to include
        chunk_end_year: Last year to include (inclusive)
        graph_client: Graph API client
        log_progress: Progress callback
        table_index: Current table index (for progress display)
        total_tables: Total number of tables (for progress display)

    Returns:
        Tuple of (success, TableInfo or None, error or None)
    """
    timeout = get_staging_timeout(table_name)
    all_temp_tables: list[str] = []  # All temp tables for cleanup on failure
    total_raw_rows = 0

    # Phase 1: Create independent temp tables per quarter
    # Track which temp tables belong to which year for hierarchical merge
    year_temps: dict[int, list[str]] = {}
    year_rows: dict[int, int] = {}

    quarters: list[tuple[int, int]] = []
    for y in range(chunk_start_year, chunk_end_year + 1):
      for q in range(1, 5):
        quarters.append((y, q))

    for year_val, q in quarters:
      quarter_pattern = (
        f"s3://{self.bucket}/{self.source_prefix}/"
        f"filed={year_val}-Q{q}/{entity_type}/{table_name}/*.parquet"
      )
      temp_name = f"{table_name}__{year_val}_Q{q}"

      try:
        response = await graph_client.create_table(
          graph_id=self.graph_id,
          table_name=temp_name,
          s3_pattern=quarter_pattern,
          timeout=timeout,
        )

        if response.get("status") == "failed":
          error = response.get("error", "Unknown error")
          if "No files found" in error:
            continue  # No data for this quarter, skip
          await self._cleanup_temp_tables(graph_client, all_temp_tables)
          return False, None, f"{table_name} {year_val}-Q{q}: {error}"

        result = response.get("result", {})
        rows = result.get("row_count", 0)
        duration = response.get("duration_seconds", result.get("duration_seconds", 0))
        total_raw_rows += rows
        all_temp_tables.append(temp_name)

        if year_val not in year_temps:
          year_temps[year_val] = []
          year_rows[year_val] = 0
        year_temps[year_val].append(temp_name)
        year_rows[year_val] += rows

        log_progress(
          f"  {table_name} chunk {year_val}-Q{q}: {rows:,} rows in {duration:.1f}s"
        )

      except Exception as e:
        error_str = str(e)
        if "No files found" in error_str:
          continue  # No data for this quarter, skip
        await self._cleanup_temp_tables(graph_client, all_temp_tables)
        return False, None, f"{table_name} {year_val}-Q{q}: {error_str}"

    if not all_temp_tables:
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

    # Probe schema from first temp table to determine dedup strategy
    try:
      probe_sql = f'SELECT * FROM "{all_temp_tables[0]}" LIMIT 0'
      probe_result = await graph_client.query_table(
        graph_id=self.graph_id, sql=probe_sql, timeout=30.0
      )
      columns = probe_result.get("columns", [])
    except Exception as e:
      await self._cleanup_temp_tables(graph_client, all_temp_tables)
      return False, None, f"{table_name} schema probe failed: {e}"

    # Phase 2: Merge quarterly tables into year tables (cross-quarter dedup)
    # Each year merge handles at most 4 quarters (~8M rows), well within memory.
    # After each year merge, drop quarterly temps to free disk space.
    year_tables: list[str] = []

    try:
      log_progress(
        f"  {table_name} merging {len(all_temp_tables)} partitions into "
        f"{len(year_temps)} year tables ({total_raw_rows:,} raw rows)..."
      )

      for year_val in sorted(year_temps.keys()):
        temps_for_year = year_temps[year_val]

        if len(temps_for_year) == 1:
          # Only one quarter for this year — use directly in final merge
          year_tables.append(temps_for_year[0])
          continue

        year_table_name = f"{table_name}__Y{year_val}"
        merge_sql = self._build_dedup_merge_sql(
          year_table_name, temps_for_year, columns
        )
        merge_sql = f"SET threads=1; {merge_sql}"

        merge_start = time.monotonic()
        await graph_client.query_table(
          graph_id=self.graph_id, sql=merge_sql, timeout=float(timeout)
        )
        merge_elapsed = time.monotonic() - merge_start

        # Get deduped row count
        count_result = await graph_client.query_table(
          graph_id=self.graph_id,
          sql=f'SELECT COUNT(*) as cnt FROM "{year_table_name}"',
          timeout=30.0,
        )
        year_count = count_result.get("rows", [[0]])[0][0]

        log_progress(
          f"  {table_name} year {year_val}: "
          f"{year_rows[year_val]:,} -> {year_count:,} rows in {merge_elapsed:.1f}s"
        )

        year_tables.append(year_table_name)
        all_temp_tables.append(year_table_name)

        # Drop quarterly temps for this year to free disk space
        for t in temps_for_year:
          try:
            await graph_client.delete_table(self.graph_id, t)
            all_temp_tables.remove(t)
          except Exception:
            pass  # Non-fatal, cleanup will try again

      # Phase 3: Final merge of year tables into target table
      log_progress(f"  {table_name} final merge of {len(year_tables)} year tables...")

      final_merge_sql = self._build_dedup_merge_sql(table_name, year_tables, columns)
      final_merge_sql = f"SET threads=1; {final_merge_sql}"

      await graph_client.query_table(
        graph_id=self.graph_id, sql=final_merge_sql, timeout=float(timeout)
      )

      await self._restore_threads(graph_client)

      # Get final row count
      count_result = await graph_client.query_table(
        graph_id=self.graph_id,
        sql=f'SELECT COUNT(*) as cnt FROM "{table_name}"',
        timeout=30.0,
      )
      final_rows = count_result.get("rows", [[0]])[0][0]

      log_progress(
        f"[{table_index}/{total_tables}] Staged {table_name}: "
        f"{final_rows:,} rows (from {total_raw_rows:,} raw across "
        f"{len(year_temps)} years)"
      )

    except Exception as e:
      # Restore thread count before cleanup — SET threads=1 persists on pooled connection
      await self._restore_threads(graph_client)
      await self._cleanup_temp_tables(graph_client, all_temp_tables)
      return False, None, f"{table_name} merge failed: {e}"

    # Phase 4: Cleanup intermediate tables (year tables + any remaining quarterly temps)
    cleanup_tables = [t for t in all_temp_tables if t != table_name]
    await self._cleanup_temp_tables(graph_client, cleanup_tables)

    return (
      True,
      TableInfo(
        name=table_name,
        row_count=final_rows,
        file_count=0,
        staged_at=datetime.now(UTC).isoformat(),
      ),
      None,
    )

  async def _restore_threads(self, graph_client: "GraphClient") -> None:
    """Restore DuckDB thread count after SET threads=1 merge operations."""
    try:
      from robosystems.config.graph_tier import GraphTierConfig

      tier = env.CLUSTER_TIER
      default_threads = GraphTierConfig.get_duckdb_max_threads(tier) if tier else 4
      await graph_client.query_table(
        graph_id=self.graph_id,
        sql=f"SET threads={default_threads}",
        timeout=30.0,
      )
    except Exception:
      pass  # Non-fatal

  async def _cleanup_temp_tables(
    self, graph_client: "GraphClient", temp_tables: list[str]
  ) -> None:
    """Drop temporary staging tables, logging but not raising on errors."""
    for temp_name in temp_tables:
      try:
        await graph_client.delete_table(self.graph_id, temp_name)
      except Exception as e:
        logger.warning(f"Could not drop temp table {temp_name}: {e}")

  async def _create_tables_with_glob(
    self,
    tables: dict[str, str],
    graph_client: "GraphClient",
    year: int | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    duckdb_memory_mb: int | None = None,
    progress_callback: ProgressCallback | None = None,
  ) -> tuple[list[str], dict[str, TableInfo]]:
    """
    Create DuckDB staging tables using glob patterns.

    For large tables (LARGE_STAGING_TABLES), checks S3 data size against
    the DuckDB memory threshold. Tables exceeding the threshold are staged
    in per-quarter chunks to avoid OOM.

    Args:
        tables: Dictionary mapping table names to entity type
        graph_client: Graph API client instance
        year: Optional single year filter
        start_year: Optional start of year range (inclusive)
        end_year: Optional end of year range (inclusive)
        duckdb_memory_mb: DuckDB memory limit in MB (for chunking threshold)
        progress_callback: Optional progress callback

    Returns:
        Tuple of (successful_table_names, table_info_dict)
    """
    successful_tables: list[str] = []
    table_infos: dict[str, TableInfo] = {}
    failed_tables: list[tuple[str, str]] = []
    skipped_tables: list[str] = []

    # Build partition pattern(s)
    # year_range_patterns is set when we need a list of per-year globs
    year_range_patterns: bool = False
    filed_pattern: str = "filed=*-Q*"
    range_start: int = 2009
    range_end: int = datetime.now(UTC).year
    if year:
      filed_pattern = f"filed={year}-Q*"
    elif start_year is not None or end_year is not None:
      # Year range: generate per-year patterns (passed as list to DuckDB)
      range_start = start_year or 2009
      range_end = end_year or datetime.now(UTC).year
      year_range_patterns = True

    log_progress = make_progress_logger(progress_callback)

    # Calculate chunking threshold from DuckDB memory config
    chunking_threshold = self._get_chunking_threshold_bytes(duckdb_memory_mb)

    total_tables = len(tables)
    for i, (table_name, entity_type) in enumerate(tables.items(), 1):
      is_large = table_name in LARGE_STAGING_TABLES

      timeout = get_staging_timeout(table_name)

      # For large tables, check S3 data size to decide if chunking is needed
      needs_chunking = False
      chunk_start = year if year else range_start
      chunk_end = year if year else range_end
      if is_large:
        s3_size = self._get_table_s3_size_bytes(
          entity_type, table_name, chunk_start, chunk_end
        )
        s3_size_gib = s3_size / (1024**3)
        threshold_gib = chunking_threshold / (1024**3)
        if s3_size > chunking_threshold:
          needs_chunking = True
          log_progress(
            f"[{i}/{total_tables}] Staging {table_name} "
            f"({s3_size_gib:.1f} GiB > {threshold_gib:.1f} GiB threshold, chunked by quarter) "
            f"(timeout={timeout}s)..."
          )
        else:
          log_progress(
            f"[{i}/{total_tables}] Staging {table_name} (large table, "
            f"{s3_size_gib:.1f} GiB) (timeout={timeout}s)..."
          )

      if needs_chunking:
        # No retries for chunked staging — retrying re-downloads all S3 data
        # and compounds disk usage. If it fails, something fundamental is wrong.
        # The _stage_table_chunked method cleans up temp tables on failure.
        success, table_info, error = await self._stage_table_chunked(
          table_name=table_name,
          entity_type=entity_type,
          chunk_start_year=chunk_start,
          chunk_end_year=chunk_end,
          graph_client=graph_client,
          log_progress=log_progress,
          table_index=i,
          total_tables=total_tables,
        )
      else:
        # Standard single-shot staging for small/medium tables
        # Build s3_pattern: dual-format globs supporting both old and new layouts
        if year_range_patterns:
          s3_pattern_list: list[str] = []
          for y in range(range_start, range_end + 1):
            base = (
              f"s3://{self.bucket}/{self.source_prefix}/"
              f"filed={y}-Q*/{entity_type}/{table_name}"
            )
            s3_pattern_list.append(f"{base}/*.parquet")
          s3_pattern: str | list[str] = s3_pattern_list
        else:
          base = (
            f"s3://{self.bucket}/{self.source_prefix}/"
            f"{filed_pattern}/{entity_type}/{table_name}"
          )
          s3_pattern = f"{base}/*.parquet"

        if not is_large:
          # Large tables already logged their size above
          log_progress(
            f"[{i}/{total_tables}] Staging {table_name} (timeout={timeout}s)..."
          )

        async def standard_stage_fn() -> tuple[bool, TableInfo | None, str | None]:
          try:
            response = await graph_client.create_table(
              graph_id=self.graph_id,
              table_name=table_name,
              s3_pattern=s3_pattern,
              timeout=timeout,
            )

            if response.get("status") == "failed":
              error = response.get("error", "Unknown error")
              if "No files found" in error:
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
              return False, None, error

            result = response.get("result", {})
            duration = response.get(
              "duration_seconds", result.get("duration_seconds", 0)
            )
            row_count = result.get("row_count", 0)

            log_progress(
              f"[{i}/{total_tables}] Staged {table_name}: {row_count:,} rows in {duration:.1f}s"
            )

            return (
              True,
              TableInfo(
                name=table_name,
                row_count=row_count,
                file_count=0,
                staged_at=datetime.now(UTC).isoformat(),
              ),
              None,
            )

          except Exception as e:
            error_str = str(e)
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

        success, table_info, error = await self._stage_table_with_retry(
          table_name=table_name,
          stage_fn=standard_stage_fn,
          graph_client=graph_client,
          log_progress=log_progress,
          table_index=i,
          total_tables=total_tables,
        )

      if success and table_info:
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
      for tbl_name, error in failed_tables:
        logger.error(f"  Failed: {tbl_name} - {error}")

      raise RuntimeError(
        f"Failed to create {len(failed_tables)} DuckDB tables: "
        f"{[t[0] for t in failed_tables]}"
      )

    return successful_tables, table_infos

  async def _create_tables_with_info(
    self,
    tables_info: dict[str, list[str]],
    graph_client: "GraphClient",
  ) -> tuple[list[str], dict[str, TableInfo]]:
    """
    Create DuckDB staging tables from explicit file lists (legacy mode).

    Args:
        tables_info: Dictionary mapping table names to S3 keys
        graph_client: Graph API client instance

    Returns:
        Tuple of (successful_table_names, table_info_dict)
    """
    successful_tables: list[str] = []
    table_infos: dict[str, TableInfo] = {}
    failed_tables: list[tuple[str, str]] = []

    for table_name, s3_keys in tables_info.items():
      logger.info(f"Creating DuckDB table: {table_name} ({len(s3_keys)} files)")

      s3_files = [f"s3://{self.bucket}/{key}" for key in s3_keys]

      try:
        response = await graph_client.create_table(
          graph_id=self.graph_id,
          table_name=table_name,
          s3_pattern=s3_files,
          timeout=1800,
        )

        if response.get("status") == "failed":
          error = response.get("error", "Unknown error")
          logger.error(f"Failed to create DuckDB table {table_name}: {error}")
          failed_tables.append((table_name, error))
          continue

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

    if failed_tables:
      logger.warning(
        f"DuckDB table creation: {len(successful_tables)} succeeded, "
        f"{len(failed_tables)} failed"
      )
      for tbl_name, error in failed_tables:
        logger.error(f"  Failed: {tbl_name} - {error}")

      raise RuntimeError(
        f"Failed to create {len(failed_tables)} DuckDB tables: "
        f"{[t[0] for t in failed_tables]}"
      )

    return successful_tables, table_infos
