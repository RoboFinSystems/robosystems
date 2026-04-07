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
  EMBEDDING_NULL_TABLES,
  LARGE_STAGING_TABLES,
  STAGING_MAX_RETRIES,
  STAGING_RETRY_BACKOFF_BASE,
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

  Embedding columns are NULLed per-table during staging: Label and Structure
  embeddings (only used during enrichment) are dropped, while Element embeddings
  are preserved for the LanceDB vector search index.
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

  @staticmethod
  def _get_null_columns(table_name: str) -> list[str] | None:
    """Get columns to NULL out for a given table during DuckDB staging.

    Label and Structure embeddings are only used during enrichment and are
    not needed after staging. Element embeddings are preserved for the
    LanceDB vector search index.
    """
    return ["embedding"] if table_name in EMBEDDING_NULL_TABLES else None

  async def stage_to_duckdb(
    self,
    year: int | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    reset_staging: bool = False,
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

      # Step 1: Get table names from schema
      logger.info("Step 1: Getting table names from schema...")
      tables_by_type = RoboLedgerContext.get_all_table_names_for_context(
        RoboLedgerContext.SEC_REPOSITORY
      )
      logger.info(f"Schema defines {len(tables_by_type)} tables to stage")

      total_files = 0

      # Step 2: Create DuckDB staging tables via Graph API
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

      expected_table_count = len(tables_by_type)
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
        table_null_columns = self._get_null_columns(table_name)
        is_entity = table_name == "Entity"

        if is_entity:
          # Entity uses DELETE+INSERT (upsert) because its attributes are mutable.
          # Other tables have immutable data and only need INSERT WHERE NOT EXISTS.
          log_progress(
            f"[{i}/{total_tables}] UPSERT {table_name} (Q{quarter} {year})..."
          )

          async def entity_upsert_fn() -> tuple[bool, TableInfo | None, str | None]:
            temp_name = f"_entity_upsert_{int(time.monotonic_ns())}"
            try:
              create_resp = await client.create_table(
                graph_id=self.graph_id,
                table_name=temp_name,
                s3_pattern=s3_pattern,
                timeout=timeout,
              )

              if create_resp.get("status") == "failed":
                error = create_resp.get("error", "Unknown error")
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
                try:
                  await client.delete_table(self.graph_id, temp_name)
                except Exception as cleanup_err:
                  logger.debug(
                    f"Could not clean up temp table {temp_name}: {cleanup_err}"
                  )
                return False, None, error

              # DELETE+INSERT is not atomic — if interrupted between steps,
              # deleted rows are lost but recovered automatically on retry
              # since the upsert re-creates the temp table from S3.
              await client.query_table(
                graph_id=self.graph_id,
                sql=(
                  f'DELETE FROM "{table_name}" WHERE identifier IN '
                  f'(SELECT identifier FROM "{temp_name}")'
                ),
                timeout=timeout,
              )

              # Insert all rows from temp (fresh data replaces deleted rows)
              await client.query_table(
                graph_id=self.graph_id,
                sql=f'INSERT INTO "{table_name}" SELECT * FROM "{temp_name}"',
                timeout=timeout,
              )

              # Get row count from temp table
              count_resp = await client.query_table(
                graph_id=self.graph_id,
                sql=f'SELECT COUNT(*) AS cnt FROM "{temp_name}"',
                timeout=30.0,
              )
              row_count = 0
              rows = count_resp.get("rows", [])
              if rows:
                row_count = rows[0][0]

              # Clean up temp table
              try:
                await client.delete_table(self.graph_id, temp_name)
              except Exception as cleanup_err:
                logger.debug(
                  f"Could not clean up temp table {temp_name}: {cleanup_err}"
                )

              log_progress(
                f"[{i}/{total_tables}] Upserted {table_name}: "
                f"{row_count:,} rows (DELETE+INSERT)"
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
              try:
                await client.delete_table(self.graph_id, temp_name)
              except Exception as cleanup_err:
                logger.debug(
                  f"Could not clean up temp table {temp_name}: {cleanup_err}"
                )
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

          stage_fn = entity_upsert_fn
        else:
          log_progress(
            f"[{i}/{total_tables}] INSERT {table_name} (Q{quarter} {year})..."
          )

          async def incremental_insert_fn() -> tuple[
            bool, TableInfo | None, str | None
          ]:
            try:
              response = await client.insert_into_table(
                graph_id=self.graph_id,
                table_name=table_name,
                s3_pattern=s3_pattern,
                timeout=timeout,
                null_columns=table_null_columns,
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

          stage_fn = incremental_insert_fn

        success, table_info, error = await self._stage_table_with_retry(
          table_name=table_name,
          stage_fn=stage_fn,
          graph_client=client,
          log_progress=log_progress,
          table_index=i,
          total_tables=total_tables,
          drop_on_retry=False,
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
  # Tables exceeding this are staged per-quarter with accumulative dedup.
  DEFAULT_CHUNKING_THRESHOLD_BYTES = 15 * 1024 * 1024 * 1024  # 15 GiB

  # Fraction of DuckDB memory to use as chunking threshold.
  # Single-shot CREATE TABLE from S3 must fit in DuckDB memory for decompression.
  # 0.50 routes tables like Element historical (34.7 GiB) to chunked staging.
  CHUNKING_MEMORY_FRACTION = 0.50

  def _get_chunking_threshold_bytes(self, duckdb_memory_mb: int | None) -> int:
    """
    Calculate the S3 data size threshold for chunked staging.

    If DuckDB memory is known (from boost response), threshold is
    CHUNKING_MEMORY_FRACTION of that. Otherwise falls back to a conservative
    15 GiB default.

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

  async def _stage_table_with_retry(
    self,
    table_name: str,
    stage_fn,
    graph_client: "GraphClient",
    log_progress: ProgressCallback,
    table_index: int,
    total_tables: int,
    drop_on_retry: bool = True,
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
        drop_on_retry: Whether to drop the table before retrying (default True).
            Set to False for tables like Entity where the stage_fn manages its
            own temp tables and dropping the main table would be destructive.

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

        # Drop partial table before retry (skip for tables that manage their own temps)
        if drop_on_retry:
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

  async def _stage_table_chunked(
    self,
    table_name: str,
    entity_type: str,
    chunk_start_year: int,
    chunk_end_year: int,
    graph_client: "GraphClient",
    null_columns: list[str] | None = None,
    log_progress: ProgressCallback | None = None,
    table_index: int = 0,
    total_tables: int = 0,
  ) -> tuple[bool, TableInfo | None, str | None]:
    """
    Stage a large table using accumulative INSERT with dedup.

    Downloads each quarter from S3, then inserts only new rows (by dedup key)
    into an accumulator table. Uses NOT EXISTS with a hash join on identifier
    strings — no GROUP BY or FIRST() on wide embedding columns.

    DuckDB's hash aggregate cannot spill FLOAT[384] aggregate state to disk,
    causing OOM for tables with 10M+ unique groups. This approach avoids hash
    aggregates entirely — the NOT EXISTS hash join only holds identifier strings
    (~500MB for 10.6M identifiers), which DuckDB spills correctly.

    Steps:
    1. Download first quarter from S3 → becomes the accumulator
    2. For each subsequent quarter:
       a. Download to temp table
       b. INSERT INTO accumulator rows WHERE NOT EXISTS (match on dedup key)
       c. Drop temp table
    3. Rename accumulator to target table

    Returns:
        Tuple of (success, TableInfo or None, error or None)
    """
    if log_progress is None:
      log_progress = make_progress_logger(None)
    timeout = get_staging_timeout(table_name)
    total_raw_rows = 0
    accumulator_name = f"{table_name}__acc"
    columns: list[str] | None = None
    accumulator_rows = 0
    current_temp: str | None = None  # Track for cleanup on failure

    # Re-verify DuckDB memory boost before chunked staging. If a previous table
    # OOMed and crashed the Graph API container, the boost (stored in-memory) is
    # lost. Without this, subsequent tables run with the default ~9 GiB limit.
    try:
      await graph_client.boost_memory(self.graph_id, target="duckdb")
    except Exception as boost_err:
      logger.warning(
        f"Could not re-verify memory boost for chunked staging: {boost_err}"
      )

    quarters: list[tuple[int, int]] = []
    for y in range(chunk_start_year, chunk_end_year + 1):
      for q in range(1, 5):
        quarters.append((y, q))

    try:
      for year_val, q in quarters:
        quarter_pattern = (
          f"s3://{self.bucket}/{self.source_prefix}/"
          f"filed={year_val}-Q{q}/{entity_type}/{table_name}/*.parquet"
        )
        temp_name = f"{table_name}__{year_val}_Q{q}"
        current_temp = temp_name

        # Download quarter from S3
        try:
          response = await graph_client.create_table(
            graph_id=self.graph_id,
            table_name=temp_name,
            s3_pattern=quarter_pattern,
            null_columns=null_columns,
            timeout=timeout,
          )

          if response.get("status") == "failed":
            error = response.get("error", "Unknown error")
            if "No files found" in error:
              continue
            await self._cleanup_temp_tables(graph_client, [accumulator_name, temp_name])
            return False, None, f"{table_name} {year_val}-Q{q}: {error}"

        except Exception as e:
          if "No files found" in str(e):
            continue
          await self._cleanup_temp_tables(graph_client, [accumulator_name, temp_name])
          return False, None, f"{table_name} {year_val}-Q{q}: {e}"

        result = response.get("result", {})
        rows = result.get("row_count", 0)
        dl_duration = response.get(
          "duration_seconds", result.get("duration_seconds", 0)
        )
        total_raw_rows += rows

        if columns is None:
          # First quarter — probe schema and rename to accumulator
          try:
            probe_result = await graph_client.query_table(
              graph_id=self.graph_id,
              sql=f'SELECT * FROM "{temp_name}" LIMIT 0',
              timeout=30.0,
            )
            columns = probe_result.get("columns", [])
          except Exception as e:
            await self._cleanup_temp_tables(graph_client, [temp_name])
            return False, None, f"{table_name} schema probe failed: {e}"

          rename_sql = f'ALTER TABLE "{temp_name}" RENAME TO "{accumulator_name}"'
          await graph_client.query_table(
            graph_id=self.graph_id, sql=rename_sql, timeout=30.0
          )
          current_temp = None
          accumulator_rows = rows
          log_progress(
            f"  {table_name} {year_val}-Q{q}: {rows:,} rows in {dl_duration:.1f}s "
            f"(accumulator initialized)"
          )
          continue

        # INSERT new rows only — NOT EXISTS hash join uses only dedup key strings
        if "identifier" in columns:
          dedup_where = (
            f'WHERE NOT EXISTS (SELECT 1 FROM "{accumulator_name}" a '
            f'WHERE a."identifier" = t."identifier")'
          )
        elif "src" in columns and "dst" in columns:
          dedup_where = (
            f'WHERE NOT EXISTS (SELECT 1 FROM "{accumulator_name}" a '
            f'WHERE a."src" = t."src" AND a."dst" = t."dst")'
          )
        else:
          dedup_where = ""

        insert_sql = (
          f'INSERT INTO "{accumulator_name}" '
          f'SELECT t.* FROM "{temp_name}" t {dedup_where}'
        )

        merge_start = time.monotonic()
        await graph_client.query_table(
          graph_id=self.graph_id, sql=insert_sql, timeout=float(timeout)
        )
        merge_elapsed = time.monotonic() - merge_start

        # Get new accumulator row count
        count_result = await graph_client.query_table(
          graph_id=self.graph_id,
          sql=f'SELECT COUNT(*) as cnt FROM "{accumulator_name}"',
          timeout=30.0,
        )
        new_acc_rows = count_result.get("rows", [[0]])[0][0]
        added = new_acc_rows - accumulator_rows

        log_progress(
          f"  {table_name} {year_val}-Q{q}: +{rows:,} raw, "
          f"+{added:,} new -> {new_acc_rows:,} total "
          f"(dl {dl_duration:.1f}s, dedup {merge_elapsed:.1f}s)"
        )

        accumulator_rows = new_acc_rows

        # Drop temp table to free disk space
        try:
          await graph_client.delete_table(self.graph_id, temp_name)
          current_temp = None
        except Exception:
          pass

      if columns is None:
        # No quarters had data
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

      # Drop existing target table before renaming accumulator
      drop_sql = f'DROP TABLE IF EXISTS "{table_name}"'
      await graph_client.query_table(graph_id=self.graph_id, sql=drop_sql, timeout=30.0)

      # Rename accumulator to final target table
      rename_sql = f'ALTER TABLE "{accumulator_name}" RENAME TO "{table_name}"'
      await graph_client.query_table(
        graph_id=self.graph_id, sql=rename_sql, timeout=30.0
      )

      log_progress(
        f"[{table_index}/{total_tables}] Staged {table_name}: "
        f"{accumulator_rows:,} rows (from {total_raw_rows:,} raw across "
        f"{chunk_end_year - chunk_start_year + 1} years)"
      )

      return (
        True,
        TableInfo(
          name=table_name,
          row_count=accumulator_rows,
          file_count=0,
          staged_at=datetime.now(UTC).isoformat(),
        ),
        None,
      )

    except Exception as e:
      cleanup = [accumulator_name]
      if current_temp:
        cleanup.append(current_temp)
      await self._cleanup_temp_tables(graph_client, cleanup)
      return False, None, f"{table_name} failed: {e}"

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
      null_columns = self._get_null_columns(table_name)

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
          null_columns=null_columns,
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
              null_columns=null_columns,
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
