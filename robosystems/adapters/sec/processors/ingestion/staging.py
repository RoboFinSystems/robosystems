"""
DuckDB Staging Operations for XBRL Graph Ingestion.

This module handles Stage 1 of the ingestion pipeline: staging processed
Parquet files from S3 into DuckDB tables. The staged data can then be
materialized to LadybugDB using the materialization module.

Key features:
- Schema-driven: Table names come from RoboLedgerContext
- Glob patterns: Efficient file discovery via DuckDB (not S3 ListObjects)
- Quarter chunking: Large tables are staged quarter-by-quarter to manage memory
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
  CHUNKED_STAGING_TIMEOUT,
  LARGE_STAGING_TABLES,
  QUARTER_CHUNKABLE_TABLES,
  STAGING_MAX_RETRIES,
  STAGING_RETRY_BACKOFF_BASE,
  TAXONOMY_STRUCTURE_TABLES,
  ProgressCallback,
  StagingResult,
  TableInfo,
  get_staging_timeout,
  make_progress_logger,
  s3_url_exists,
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
    reset_staging: bool = False,
    skip_taxonomy_relationships: bool = False,
    use_glob: bool = True,
    chunk_large_tables: bool = True,
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

    Args:
        year: Optional year filter. If provided, only files from that year.
        reset_staging: If True, delete entire DuckDB staging database first.
        skip_taxonomy_relationships: If True, skip taxonomy structure tables.
        use_glob: If True (default), use glob patterns for efficient discovery.
        chunk_large_tables: If True (default), stage large tables quarter-by-quarter.
        progress_callback: Optional callback for progress logging.

    Returns:
        StagingResult with table counts and file counts
    """
    start_time = time.time()
    log_progress = make_progress_logger(progress_callback)

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
          progress_callback=log_progress,
          chunk_large_tables=chunk_large_tables,
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

      # Stage each table incrementally
      successful_tables: list[str] = []
      table_infos: dict[str, TableInfo] = {}
      failed_tables: list[tuple[str, str]] = []

      total_tables = len(tables_by_type)
      for i, (table_name, entity_type) in enumerate(tables_by_type.items(), 1):
        # Build S3 patterns for all quarters to scan
        s3_patterns = [
          f"s3://{self.bucket}/{self.source_prefix}/filed={y}-Q{q}/{entity_type}/{table_name}.parquet"
          for y, q in quarters_to_scan
        ]

        # Filter to only existing files
        if len(s3_patterns) > 1:
          s3_patterns = [p for p in s3_patterns if s3_url_exists(self.s3_client, p)]
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

        # Use single pattern if only one quarter
        s3_pattern: str | list[str] = (
          s3_patterns[0] if len(s3_patterns) == 1 else s3_patterns
        )

        timeout = get_staging_timeout(table_name)
        log_progress(f"[{i}/{total_tables}] INSERT {table_name} (Q{quarter} {year})...")

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
          row_count = result.get("row_count", 0)
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

  # =========================================================================
  # Private Helper Methods
  # =========================================================================

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
    log_progress: ProgressCallback,
    table_index: int,
    total_tables: int,
  ) -> tuple[bool, TableInfo | None, str | None]:
    """
    Stage a large table using parallel chunk loading + final merge pattern.

    For tables like Fact (100M+ rows), this approach:
    1. Loads each quarter into a separate chunk table
    2. Merges all chunks with deduplication into the final table
    3. Cleans up chunk tables

    Args:
        table_name: Name of the table to stage
        entity_type: "nodes" or "relationships"
        graph_client: Graph API client instance
        quarters: List of quarter partition keys (e.g., ["2024-Q1", ...])
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
      chunk_name = f"{table_name}_chunk_{quarter_key.replace('-', '_')}"
      s3_pattern = (
        f"s3://{self.bucket}/{self.source_prefix}/filed={quarter_key}/"
        f"{entity_type}/{table_name}.parquet"
      )

      log_progress(f"  [{quarter_key}] Loading chunk {q_idx + 1}/{len(quarters)}...")

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
          error_str = str(e).strip() if str(e).strip() and str(e).strip() != "." else ""
          if not error_str:
            error_str = f"{type(e).__name__} (no message)"
          else:
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

            # Get fresh client before retry
            try:
              graph_client = await get_graph_client(
                graph_id=self.graph_id, operation_type="write"
              )
              logger.debug(
                f"Obtained fresh graph client for retry attempt {attempt + 2}"
              )
            except Exception as client_err:
              logger.warning(f"Could not refresh graph client: {client_err}")

            await asyncio.sleep(backoff)
          else:
            log_progress(
              f"  [{quarter_key}] Failed after {STAGING_MAX_RETRIES} attempts: {error_str[:200]}"
            )

      if not chunk_success:
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

    # Build merge SQL using GROUP BY + FIRST()
    if entity_type == "nodes":
      dedupe_columns = ["identifier"]
    else:
      dedupe_columns = ["src", "dst"]

    schema_response = await graph_client.query_table(
      graph_id=self.graph_id,
      sql=f'DESCRIBE "{chunk_tables[0]}"',
      timeout=60.0,
    )
    all_columns = [row[0] for row in schema_response.get("rows", [])]

    other_columns = [c for c in all_columns if c not in dedupe_columns]
    select_parts = [f'"{c}"' for c in dedupe_columns] + [
      f'FIRST("{c}") AS "{c}"' for c in other_columns
    ]
    select_clause = ", ".join(select_parts)
    group_by_clause = ", ".join(f'"{c}"' for c in dedupe_columns)

    union_parts = " UNION ALL ".join(f'SELECT * FROM "{t}"' for t in chunk_tables)
    merge_sql = f"""
            CREATE OR REPLACE TABLE "{table_name}" AS
            SELECT {select_clause}
            FROM ({union_parts})
            GROUP BY {group_by_clause}
        """

    merge_success = False
    last_merge_error = None
    final_row_count = 0
    merge_timeout = float(CHUNKED_STAGING_TIMEOUT)

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

          count_response = await graph_client.query_table(
            graph_id=self.graph_id,
            sql=f'SELECT COUNT(*) as cnt FROM "{table_name}"',
            timeout=60.0,
          )
          if count_response.get("rows") and count_response["rows"][0]:
            final_row_count = count_response["rows"][0][0]

          log_progress(
            f"  [MERGE] Created {table_name}: {final_row_count:,} rows in {merge_duration:.1f}s"
          )
          merge_success = True
          break

        except Exception as e:
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
      # Phase 3: Cleanup chunk tables
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
    log_progress: ProgressCallback,
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
        logger.warning(f"Could not delete chunk table {chunk_name}: {e}")

    log_progress(f"  [CLEANUP] Deleted {deleted}/{len(chunk_tables)} chunk tables")

  async def _create_tables_with_glob(
    self,
    tables: dict[str, str],
    graph_client: "GraphClient",
    year: int | None = None,
    progress_callback: ProgressCallback | None = None,
    chunk_large_tables: bool = True,
  ) -> tuple[list[str], dict[str, TableInfo]]:
    """
    Create DuckDB staging tables using glob patterns.

    Args:
        tables: Dictionary mapping table names to entity type
        graph_client: Graph API client instance
        year: Optional year filter
        progress_callback: Optional progress callback
        chunk_large_tables: If True, stage large tables quarter-by-quarter

    Returns:
        Tuple of (successful_table_names, table_info_dict)
    """
    successful_tables: list[str] = []
    table_infos: dict[str, TableInfo] = {}
    failed_tables: list[tuple[str, str]] = []
    skipped_tables: list[str] = []

    # Build partition pattern
    if year:
      filed_pattern = f"filed={year}-Q*"
    else:
      filed_pattern = "filed=*-Q*"

    log_progress = make_progress_logger(progress_callback)

    # Discover quarterly partitions for chunked staging
    quarterly_partitions: list[str] | None = None
    if chunk_large_tables:
      quarterly_partitions = await self._discover_filed_partitions(year=year)
      if quarterly_partitions:
        logger.info(
          f"Quarter chunking enabled: {len(quarterly_partitions)} quarters discovered"
        )

    total_tables = len(tables)
    for i, (table_name, entity_type) in enumerate(tables.items(), 1):
      is_large = table_name in LARGE_STAGING_TABLES
      is_chunkable = table_name in QUARTER_CHUNKABLE_TABLES

      # Use quarter-based chunking for chunkable tables
      if is_chunkable and quarterly_partitions and chunk_large_tables:
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

      # Standard staging for small tables
      s3_pattern = (
        f"s3://{self.bucket}/{self.source_prefix}/"
        f"{filed_pattern}/{entity_type}/{table_name}.parquet"
      )

      timeout = get_staging_timeout(table_name)
      size_hint = " (large table)" if is_large else ""
      log_progress(
        f"[{i}/{total_tables}] Staging {table_name}{size_hint} (timeout={timeout}s)..."
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
