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

Backward Compatibility:
- process_files() still works as before (runs staging then materialization)
- New Dagster assets can call staging and materialization independently

Status: Production - enables independent retry of failed materialization.
"""

import time
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
    rebuild: bool = True,
    year: int | None = None,
    filing_date: str | None = None,
    reset_staging: bool = False,
    use_glob: bool = True,
    incremental: bool = False,
  ) -> StagingResult:
    """
    Stage processed Parquet files to persistent DuckDB.

    This is Stage 1 of the decoupled pipeline. It uses the schema to determine
    which tables to create, optionally rebuilds the LadybugDB database, and
    creates DuckDB staging tables from S3 parquet files.

    Schema-Driven Design:
    - Table names come from RoboLedgerContext.get_all_table_names_for_context()
    - No manifest file needed - schema is the source of truth
    - Works across services (Dagster/Graph API) without shared filesystem

    Incremental Mode (incremental=True):
    - Uses INSERT INTO (not CREATE TABLE) to append new data
    - Skips LadybugDB rebuild to preserve existing data
    - Filters to specific filing_date if provided

    Args:
        rebuild: Whether to rebuild LadybugDB database from scratch (default: True).
        year: Optional year filter. If provided, only files from filings in that year
              will be included (filters to filed=YYYY-*).
        filing_date: Optional exact date filter (YYYY-MM-DD). If provided, only files
              from that specific filing date will be included. Takes precedence over year.
        reset_staging: If True, delete DuckDB staging alongside LadybugDB for a
            fresh start. If False (default), preserve DuckDB for incremental scenarios.
        use_glob: If True (default), use glob patterns for efficient file discovery.
            If False, use explicit file lists (legacy behavior, slower for many files).
        incremental: If True, append to existing tables instead of recreating them.
            Default: False.

    Returns:
        StagingResult with table counts and file counts
    """
    start_time = time.time()

    # Determine date filter for logging
    date_filter = filing_date or (f"{year}-*" if year else "all")
    mode_str = "incremental" if incremental else "full"
    logger.info(
      f"Starting DuckDB staging for graph {self.graph_id} "
      f"(mode={mode_str}, filed={date_filter}, rebuild={rebuild}, reset_staging={reset_staging})"
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

      # Step 0: Check if already staged (incremental mode only)
      # This prevents duplicate inserts if the same date is re-run
      if incremental and filing_date:
        try:
          staged_dates = await client.get_staged_dates(self.graph_id)
          if filing_date in staged_dates:
            logger.info(
              f"Filing date {filing_date} already staged - skipping to prevent duplicates"
            )
            return StagingResult(
              status="already_staged",
              table_names=[],
              total_files=0,
              total_rows=0,
              duration_seconds=time.time() - start_time,
            )
        except Exception as e:
          # If we can't check progress, continue anyway (table might not exist yet)
          logger.warning(f"Could not check staging progress (continuing): {e}")

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
        total_files = sum(len(files) for files in tables_info.values())
        logger.info(f"Total files: {total_files}")

      # Step 2: Handle LadybugDB database rebuild BEFORE creating DuckDB tables
      # Skip rebuild in incremental mode - we're appending to existing tables
      if rebuild and not incremental:
        logger.info("Step 2: Rebuilding LadybugDB database...")
        await self._rebuild_ladybug_database(client, reset_staging=reset_staging)
      elif incremental:
        logger.info("Step 2: Skipped (incremental mode - preserving existing data)")

      # Step 3: Create or append to DuckDB staging tables via Graph API
      if incremental and use_glob:
        # Incremental mode: INSERT INTO existing tables
        logger.info("Step 3: Inserting into DuckDB staging tables (incremental)...")
        (
          successful_tables,
          table_infos,
        ) = await self._insert_into_duckdb_tables_with_glob(
          tables_by_type, client, year=year, filing_date=filing_date
        )
      elif use_glob:
        # Full mode: CREATE TABLE
        logger.info("Step 3: Creating DuckDB staging tables via glob patterns...")
        successful_tables, table_infos = await self._create_duckdb_tables_with_glob(
          tables_by_type, client, year=year, filing_date=filing_date
        )
      else:
        logger.info("Step 3: Creating DuckDB staging tables via file lists (legacy)...")
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

      # Step 4: Record staging progress
      total_rows = sum(info.row_count for info in table_infos.values())
      if status == "success":
        await self._record_staging_progress(
          client=client,
          filing_date=filing_date,
          year=year,
          incremental=incremental,
          table_count=len(successful_tables),
          total_rows=total_rows,
        )

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

  async def materialize_from_duckdb(
    self,
    table_names: list[str] | None = None,
    rebuild: bool = False,
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

    Returns:
        MaterializeResult with rows ingested per table
    """
    start_time = time.time()

    logger.info(f"Starting materialization from DuckDB for graph {self.graph_id}")

    try:
      # Step 1: Determine which tables to materialize (schema-driven)
      if table_names is None:
        logger.info("Step 1: Getting table names from schema...")
        tables_by_type = RoboLedgerContext.get_all_table_names_for_context(
          RoboLedgerContext.SEC_REPOSITORY
        )
        table_names = list(tables_by_type.keys())
        logger.info(f"Schema defines {len(table_names)} tables to materialize")

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
      logger.info("Step 4: Triggering graph ingestion...")
      ingestion_results = await self._trigger_ingestion(
        table_names, client, rebuild=False
      )

      duration = time.time() - start_time

      logger.info(
        f"Materialization complete in {duration:.2f}s: "
        f"{ingestion_results.get('total_rows_ingested', 0)} rows ingested"
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

  async def process_files(
    self,
    rebuild: bool = True,
    year: int | None = None,
  ) -> dict[str, Any]:
    """
    Process Parquet files into graph database using DuckDB-based pattern.

    This is the backward-compatible method that runs both staging and
    materialization in sequence. For independent control, use:
    - stage_to_duckdb() for staging only
    - materialize_from_duckdb() for materialization only

    IMPORTANT: This approach always rebuilds the graph from scratch because
    DuckDB staging tables contain ALL processed files from S3.

    Args:
        rebuild: Whether to rebuild graph from scratch (default: True).
        year: Optional year filter for processing.

    Returns:
        Processing results with statistics
    """
    start_time = time.time()

    logger.info(
      f"Starting DuckDB-based SEC ingestion for graph {self.graph_id} "
      f"(year={year or 'all'}, rebuild={rebuild})"
    )

    # Stage 1: DuckDB staging
    staging_result = await self.stage_to_duckdb(rebuild=rebuild, year=year)

    if staging_result.status == "error":
      return {
        "status": "error",
        "error": staging_result.error,
        "duration_seconds": time.time() - start_time,
      }

    if staging_result.status == "no_data":
      return {
        "status": "no_data",
        "message": "No processed files found",
        "duration_seconds": time.time() - start_time,
      }

    # Stage 2: LadybugDB materialization
    materialize_result = await self.materialize_from_duckdb(
      table_names=staging_result.table_names
    )

    duration = time.time() - start_time

    if materialize_result.status == "error":
      return {
        "status": "error",
        "error": materialize_result.error,
        "staging_complete": True,  # Staging succeeded, materialization failed
        "duration_seconds": duration,
      }

    logger.info(
      f"SEC DuckDB-based ingestion complete in {duration:.2f}s: "
      f"{materialize_result.total_rows_ingested} rows ingested from {staging_result.total_files} files"
    )

    return {
      "status": "success",
      "tables_processed": len(staging_result.table_names),
      "total_files": staging_result.total_files,
      "ingestion_results": materialize_result.to_dict(),
      "duration_seconds": duration,
    }

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
    from robosystems.models.iam import GraphSchema

    logger.info(
      f"Rebuild requested - regenerating entire LadybugDB database for {self.graph_id}"
    )

    db = SessionFactory()
    try:
      # preserve_duckdb=True keeps DuckDB for retry/incremental scenarios
      # preserve_duckdb=False (reset_staging=True) deletes both for fresh start
      preserve_duckdb = not reset_staging
      await client.delete_database(self.graph_id, preserve_duckdb=preserve_duckdb)
      if reset_staging:
        logger.info(
          f"Deleted LadybugDB and DuckDB staging: {self.graph_id} (fresh start)"
        )
      else:
        logger.info(
          f"Deleted LadybugDB database: {self.graph_id} (DuckDB staging preserved)"
        )

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

  async def _discover_table_names(
    self,
    year: int | None = None,
    filing_date: str | None = None,
  ) -> dict[str, str]:
    """
    Discover table names via S3 directory listing (lightweight, no file enumeration).

    This is a faster alternative to _discover_processed_files() when using glob patterns.
    Instead of listing all files, it only lists directory prefixes to get table names.

    Uses the new filed=YYYY-MM-DD partition structure:
    sec/processed/filed=YYYY-MM-DD/nodes/TABLE/*.parquet

    Args:
        year: Optional year filter. If provided, discovers from filed=YYYY-* partitions.
        filing_date: Optional exact date filter (YYYY-MM-DD). Takes precedence over year.

    Returns:
        Dictionary mapping table names to entity type ("nodes" or "relationships")
    """
    tables: dict[str, str] = {}

    # Find a sample filed= prefix to discover table names
    # (S3 ListObjects doesn't support glob, so we need one concrete prefix)
    filed_prefix = f"{self.source_prefix}/"
    paginator = self.s3_client.s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=self.bucket, Prefix=filed_prefix, Delimiter="/")

    sample_filed_path = None
    for page in pages:
      if "CommonPrefixes" in page:
        for prefix_info in page["CommonPrefixes"]:
          prefix_path = prefix_info["Prefix"]
          if "filed=" in prefix_path:
            # Extract the date from filed=YYYY-MM-DD
            filed_date = prefix_path.split("filed=")[1].rstrip("/")

            # If filtering by exact date, only use that one
            if filing_date and filed_date == filing_date:
              sample_filed_path = prefix_path
              break

            # If filtering by year, check if date starts with that year
            if year and not filed_date.startswith(str(year)):
              continue

            # Use this prefix as sample (or first match)
            sample_filed_path = prefix_path
            break
      if sample_filed_path:
        break

    if not sample_filed_path:
      filter_desc = filing_date or (str(year) if year else "any")
      logger.warning(
        f"No filed= directories found under {filed_prefix} for filter {filter_desc}"
      )
      return tables

    # List table directories under nodes/ and relationships/
    for entity_type in ["nodes", "relationships"]:
      prefix = f"{sample_filed_path}{entity_type}/"
      paginator = self.s3_client.s3_client.get_paginator("list_objects_v2")
      pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter="/")

      for page in pages:
        if "CommonPrefixes" in page:
          for prefix_info in page["CommonPrefixes"]:
            # Extract table name from prefix like "sec/processed/filed=2025-01-15/nodes/Entity/"
            table_path = prefix_info["Prefix"]
            table_name = table_path.rstrip("/").split("/")[-1]
            if table_name and table_name not in tables:
              tables[table_name] = entity_type
              logger.debug(f"Discovered table: {table_name} ({entity_type})")

    logger.info(f"Discovered {len(tables)} tables via directory listing")
    return tables

  async def _create_duckdb_tables_with_glob(
    self,
    tables: dict[str, str],
    graph_client,
    year: int | None = None,
    filing_date: str | None = None,
  ) -> tuple[list[str], dict[str, TableInfo]]:
    """
    Create DuckDB staging tables using glob patterns (efficient for many files).

    Instead of passing explicit file lists, this method constructs glob patterns
    and lets DuckDB handle file discovery internally. This is much faster for
    large file counts (17K+ files).

    Uses the new filed=YYYY-MM-DD partition structure:
    sec/processed/filed=YYYY-MM-DD/nodes/TABLE/*.parquet

    Args:
        tables: Dictionary mapping table names to entity type ("nodes" or "relationships")
        graph_client: Graph API client instance
        year: Optional year filter. If provided, uses filed=YYYY-* pattern.
        filing_date: Optional exact date filter (YYYY-MM-DD). Takes precedence over year.

    Returns:
        Tuple of (successful_table_names, table_info_dict)
    """
    successful_tables: list[str] = []
    table_infos: dict[str, TableInfo] = {}
    failed_tables: list[tuple[str, str]] = []
    skipped_tables: list[str] = []

    # Build filed= pattern for glob
    # - filing_date: exact date (filed=2025-01-15)
    # - year: year prefix (filed=2025-*)
    # - neither: all dates (filed=*)
    if filing_date:
      filed_pattern = f"filed={filing_date}"
    elif year:
      filed_pattern = f"filed={year}-*"
    else:
      filed_pattern = "filed=*"

    for table_name, entity_type in tables.items():
      # Build glob pattern: s3://bucket/sec/processed/filed=*/nodes/Entity/*.parquet
      s3_pattern = (
        f"s3://{self.bucket}/{self.source_prefix}/"
        f"{filed_pattern}/{entity_type}/{table_name}/*.parquet"
      )

      logger.info(f"Creating DuckDB table: {table_name} (glob: {s3_pattern})")

      try:
        # Use graph client to call Graph API's table creation endpoint
        # Pass glob pattern (string) instead of file list
        response = await graph_client.create_table(
          graph_id=self.graph_id,
          table_name=table_name,
          s3_pattern=s3_pattern,  # Glob pattern, not a file list
          timeout=1800,  # 30 minutes for large file sets
        )

        # Handle SSE-based response format
        if response.get("status") == "failed":
          error = response.get("error", "Unknown error")
          # "No files found" is expected for optional tables (e.g., ENTITY_EVOLVED_FROM)
          if "No files found" in error:
            logger.info(f"Skipped {table_name}: no files found (optional table)")
            skipped_tables.append(table_name)
            successful_tables.append(table_name)
            table_infos[table_name] = TableInfo(
              name=table_name,
              row_count=0,
              file_count=0,
              staged_at=datetime.now(UTC).isoformat(),
            )
            continue
          logger.error(f"Failed to create DuckDB table {table_name}: {error}")
          failed_tables.append((table_name, error))
          continue

        # Extract result from SSE response
        result = response.get("result", {})
        duration = response.get("duration_seconds", result.get("duration_seconds", 0))
        row_count = result.get("row_count", 0)

        logger.info(f"Staged {table_name}: {row_count:,} rows in {duration:.1f}s")

        successful_tables.append(table_name)
        table_infos[table_name] = TableInfo(
          name=table_name,
          row_count=row_count,
          file_count=0,  # Unknown with glob pattern
          staged_at=datetime.now(UTC).isoformat(),
        )

      except Exception as e:
        error_str = str(e)
        # Also handle "No files found" from exceptions
        if "No files found" in error_str:
          logger.info(f"Skipped {table_name}: no files found (optional table)")
          skipped_tables.append(table_name)
          successful_tables.append(table_name)
          table_infos[table_name] = TableInfo(
            name=table_name,
            row_count=0,
            file_count=0,
            staged_at=datetime.now(UTC).isoformat(),
          )
          continue
        logger.error(f"Failed to create DuckDB table {table_name}: {e}")
        failed_tables.append((table_name, error_str))
        continue

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

  async def _insert_into_duckdb_tables_with_glob(
    self,
    tables: dict[str, str],
    graph_client,
    year: int | None = None,
    filing_date: str | None = None,
  ) -> tuple[list[str], dict[str, TableInfo]]:
    """
    Insert into existing DuckDB staging tables using glob patterns (incremental append).

    This is the incremental version of _create_duckdb_tables_with_glob(). Instead of
    CREATE TABLE, it uses INSERT INTO to append new data to existing tables.

    Uses the new filed=YYYY-MM-DD partition structure:
    sec/processed/filed=YYYY-MM-DD/nodes/TABLE/*.parquet

    Args:
        tables: Dictionary mapping table names to entity type ("nodes" or "relationships")
        graph_client: Graph API client instance
        year: Optional year filter. If provided, uses filed=YYYY-* pattern.
        filing_date: Optional exact date filter (YYYY-MM-DD). Takes precedence over year.

    Returns:
        Tuple of (successful_table_names, table_info_dict)
    """
    successful_tables: list[str] = []
    table_infos: dict[str, TableInfo] = {}
    failed_tables: list[tuple[str, str]] = []
    skipped_tables: list[str] = []

    # Build filed= pattern for glob
    if filing_date:
      filed_pattern = f"filed={filing_date}"
    elif year:
      filed_pattern = f"filed={year}-*"
    else:
      filed_pattern = "filed=*"

    for table_name, entity_type in tables.items():
      # Build glob pattern: s3://bucket/sec/processed/filed=*/nodes/Entity/*.parquet
      s3_pattern = (
        f"s3://{self.bucket}/{self.source_prefix}/"
        f"{filed_pattern}/{entity_type}/{table_name}/*.parquet"
      )

      logger.info(f"Inserting into DuckDB table: {table_name} (glob: {s3_pattern})")

      try:
        # Use graph client to call Graph API's insert_into_table endpoint
        response = await graph_client.insert_into_table(
          graph_id=self.graph_id,
          table_name=table_name,
          s3_pattern=s3_pattern,
          timeout=1800,  # 30 minutes for large file sets
        )

        # Handle SSE-based response format
        if response.get("status") == "failed":
          error = response.get("error", "Unknown error")
          # "No files found" is expected for optional tables
          if "No files found" in error:
            logger.info(f"Skipped {table_name}: no files found (optional table)")
            skipped_tables.append(table_name)
            successful_tables.append(table_name)
            table_infos[table_name] = TableInfo(
              name=table_name,
              row_count=0,
              file_count=0,
              staged_at=datetime.now(UTC).isoformat(),
            )
            continue
          logger.error(f"Failed to insert into DuckDB table {table_name}: {error}")
          failed_tables.append((table_name, error))
          continue

        # Extract result from SSE response
        result = response.get("result", {})
        duration = response.get("duration_seconds", result.get("duration_seconds", 0))
        row_count = result.get("row_count", 0)

        logger.info(
          f"Appended to {table_name}: {row_count:,} rows total in {duration:.1f}s"
        )

        successful_tables.append(table_name)
        table_infos[table_name] = TableInfo(
          name=table_name,
          row_count=row_count,
          file_count=0,  # Unknown with glob pattern
          staged_at=datetime.now(UTC).isoformat(),
        )

      except Exception as e:
        error_str = str(e)
        # Also handle "No files found" from exceptions
        if "No files found" in error_str:
          logger.info(f"Skipped {table_name}: no files found (optional table)")
          skipped_tables.append(table_name)
          successful_tables.append(table_name)
          table_infos[table_name] = TableInfo(
            name=table_name,
            row_count=0,
            file_count=0,
            staged_at=datetime.now(UTC).isoformat(),
          )
          continue
        logger.error(f"Failed to insert into DuckDB table {table_name}: {e}")
        failed_tables.append((table_name, error_str))
        continue

    # Report summary
    if skipped_tables:
      logger.info(
        f"Skipped {len(skipped_tables)} tables with no files: {skipped_tables}"
      )

    if failed_tables:
      logger.warning(
        f"DuckDB table insert: {len(successful_tables)} succeeded, "
        f"{len(failed_tables)} failed"
      )
      for table_name, error in failed_tables:
        logger.error(f"  Failed: {table_name} - {error}")

      raise RuntimeError(
        f"Failed to insert into {len(failed_tables)} DuckDB tables: "
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
    rebuild: bool = False,
  ) -> dict[str, Any]:
    """
    Trigger ingestion for all tables into LadybugDB graph via Graph API.

    Args:
        table_names: List of table names to ingest
        graph_client: Graph API client instance
        rebuild: Ignored - rebuild is now handled in process_files before table creation

    Returns:
        Ingestion results with statistics
    """
    total_rows = 0
    total_time_ms = 0.0
    results = []

    for table_name in table_names:
      logger.info(f"Materializing table: {table_name}")

      try:
        response = await graph_client.materialize_table(
          graph_id=self.graph_id,
          table_name=table_name,
          ignore_errors=True,
        )

        total_rows += response.get("rows_ingested", 0)
        total_time_ms += response.get("execution_time_ms", 0)

        results.append(
          {
            "table_name": table_name,
            "rows_ingested": response.get("rows_ingested", 0),
            "status": response.get("status", "success"),
          }
        )

        logger.info(
          f"✓ Materialized {table_name}: "
          f"{response.get('rows_ingested', 0)} rows in "
          f"{response.get('execution_time_ms', 0):.2f}ms"
        )

      except Exception as e:
        logger.error(f"Failed to materialize table {table_name}: {e}")
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

  async def _record_staging_progress(
    self,
    client,
    filing_date: str | None,
    year: int | None,
    incremental: bool,
    table_count: int,
    total_rows: int,
  ) -> None:
    """
    Record staging progress to the DuckDB progress table.

    For incremental staging (single filing_date): Records that specific date.
    For full staging: Discovers all filed= dates from S3 and records them.

    Args:
        client: Graph API client instance
        filing_date: Specific filing date (for incremental) or None (for full)
        year: Year filter (for full staging) or None
        incremental: Whether this is incremental mode
        table_count: Number of tables staged
        total_rows: Total rows inserted
    """
    try:
      if incremental and filing_date:
        # Incremental mode: record the specific filing_date
        logger.info(f"Recording staging progress for {filing_date}")
        await client.record_staging_progress(
          graph_id=self.graph_id,
          filing_date=filing_date,
          table_count=table_count,
          total_rows=total_rows,
          status="complete",
        )
      else:
        # Full staging: discover all filed= dates and record them
        logger.info("Discovering filed= dates from S3 for progress tracking...")
        filed_dates = await self._discover_filed_dates(year=year)

        if not filed_dates:
          logger.warning("No filed= dates discovered, skipping progress recording")
          return

        logger.info(f"Recording progress for {len(filed_dates)} filing dates")

        # Record each date (rows are distributed across dates, so we estimate)
        rows_per_date = total_rows // len(filed_dates) if filed_dates else 0

        for filed_date in filed_dates:
          await client.record_staging_progress(
            graph_id=self.graph_id,
            filing_date=filed_date,
            table_count=table_count,
            total_rows=rows_per_date,
            status="complete",
          )

        logger.info(f"Recorded staging progress for {len(filed_dates)} dates")

    except Exception as e:
      # Don't fail staging if progress recording fails - it's just bookkeeping
      logger.warning(f"Failed to record staging progress (non-fatal): {e}")

  async def _discover_filed_dates(self, year: int | None = None) -> list[str]:
    """
    Discover all filed= partition dates from S3.

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
