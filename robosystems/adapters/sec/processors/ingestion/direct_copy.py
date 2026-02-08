"""
Direct S3 → LadybugDB Copy Operations for XBRL Graph Ingestion.

This module handles direct ingestion from S3 parquet files to LadybugDB,
bypassing the DuckDB staging layer. This is an alternative workflow for
scenarios where the 2-stage pipeline (DuckDB staging → LadybugDB materialization)
is not needed.

Key features:
- Direct S3 → LadybugDB COPY with spill_to_disk=true
- Quarter-by-quarter batching for large tables to manage memory
- Schema-driven: Table names come from RoboLedgerContext

Classes:
    LadybugDirectCopier: Handles direct S3 → LadybugDB copy operations
"""

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
  from robosystems.graph_api.client.client import GraphClient

from robosystems.config import env
from robosystems.logger import logger
from robosystems.operations.aws.s3 import S3Client
from robosystems.schemas.extensions.roboledger import RoboLedgerContext

from .models import TAXONOMY_STRUCTURE_TABLES, ProgressCallback, make_progress_logger

# Large tables that benefit from quarter-by-quarter batching in LadybugDB
# These are batched to manage memory during direct S3→LadybugDB COPY
LARGE_TABLES_FOR_BATCHING = frozenset(
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
    "FACT_DIMENSION_MEMBER_ELEMENT",
    "FACT_REPORTS_ELEMENT",
    # Association tables (~200-206M rows) - largest tables
    "Association",
    "STRUCTURE_HAS_ASSOCIATION",
    "ASSOCIATION_HAS_FROM_ELEMENT",
    "ASSOCIATION_HAS_TO_ELEMENT",
  }
)


@dataclass
class DirectCopyResult:
  """Result from direct S3 → LadybugDB copy operation."""

  status: str  # "success", "partial", "error"
  tables_copied: list[str] = field(default_factory=list)
  failed_tables: list[dict[str, Any]] = field(default_factory=list)
  total_rows: int = 0
  duration_ms: float = 0.0
  error: str | None = None

  def to_dict(self) -> dict[str, Any]:
    """Convert to dictionary for metadata output."""
    return {
      "status": self.status,
      "tables_copied": self.tables_copied,
      "failed_tables": self.failed_tables,
      "total_rows": self.total_rows,
      "duration_ms": self.duration_ms,
      "error": self.error,
    }


class LadybugDirectCopier:
  """
  Direct S3 → LadybugDB copy operations.

  This class handles direct ingestion from S3 parquet files to LadybugDB,
  bypassing the DuckDB staging layer entirely. Uses LadybugDB's COPY FROM
  with spill_to_disk=true for memory-efficient loading of large tables.

  Architecture:
  - Uses Graph API client to communicate with Graph API container
  - Large tables are batched by quarter to manage memory
  - Small tables use single COPY with glob pattern
  """

  def __init__(self, graph_id: str = "sec", source_prefix: str | None = None):
    """
    Initialize direct copier.

    Args:
        graph_id: Graph database identifier (default: "sec")
        source_prefix: S3 prefix for source files (default: "sec/processed")
    """
    self.graph_id = graph_id
    self.s3_client = S3Client()
    self.bucket = env.SHARED_PROCESSED_BUCKET
    self.source_prefix = source_prefix or "sec/processed"

  async def discover_quarterly_partitions(
    self,
    year: int | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
  ) -> list[str]:
    """
    Discover all filed= quarterly partitions from S3.

    Used for quarter-by-quarter batching of large tables during direct copy.

    Args:
        year: Optional single year filter. If provided, only returns quarters from that year.
        start_year: Optional start of year range (inclusive).
        end_year: Optional end of year range (inclusive).

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
            # Filter by single year if specified
            if year and not filed_part.startswith(str(year)):
              continue
            # Filter by year range if specified
            if start_year or end_year:
              partition_year = int(filed_part.split("-")[0])
              if start_year and partition_year < start_year:
                continue
              if end_year and partition_year > end_year:
                continue
            partitions.append(filed_part)

    partitions.sort()
    logger.info(f"Discovered {len(partitions)} filed= quarterly partitions from S3")
    return partitions

  def get_tables_for_copy(
    self,
    skip_taxonomy_relationships: bool = False,
    skip_tables: list[str] | None = None,
    existing_table_names: set[str] | None = None,
  ) -> dict[str, str]:
    """
    Get tables to copy from schema, with optional filtering.

    Args:
        skip_taxonomy_relationships: If True, skip taxonomy structure tables
        skip_tables: Optional list of table names to skip
        existing_table_names: Optional set of tables that exist in the database
                              (filters to only copy tables that exist)

    Returns:
        Dictionary mapping table names to entity type ("nodes" or "relationships")
    """
    # Get all tables from schema
    tables_by_type = RoboLedgerContext.get_all_table_names_for_context(
      RoboLedgerContext.SEC_REPOSITORY
    )

    # Filter to only tables that exist in database
    if existing_table_names is not None:
      original_count = len(tables_by_type)
      tables_by_type = {
        name: entity_type
        for name, entity_type in tables_by_type.items()
        if name in existing_table_names
      }
      filtered_count = original_count - len(tables_by_type)
      if filtered_count > 0:
        logger.info(f"Filtered out {filtered_count} tables not in database schema")

    # Filter taxonomy tables if requested
    if skip_taxonomy_relationships:
      original_count = len(tables_by_type)
      tables_by_type = {
        name: entity_type
        for name, entity_type in tables_by_type.items()
        if name not in TAXONOMY_STRUCTURE_TABLES
      }
      skipped_count = original_count - len(tables_by_type)
      if skipped_count > 0:
        logger.info(f"Skipped {skipped_count} taxonomy structure tables")

    # Filter explicitly skipped tables
    if skip_tables:
      original_count = len(tables_by_type)
      tables_by_type = {
        name: entity_type
        for name, entity_type in tables_by_type.items()
        if name not in skip_tables
      }
      skipped_count = original_count - len(tables_by_type)
      if skipped_count > 0:
        logger.info(f"Skipped {skipped_count} tables from skip_tables: {skip_tables}")

    return tables_by_type

  def order_tables_for_copy(
    self, tables_by_type: dict[str, str]
  ) -> list[tuple[str, str]]:
    """
    Order tables for copy: nodes before relationships.

    Relationship tables have uppercase names (e.g., REPORT_HAS_FACT).
    Node tables have PascalCase names (e.g., Fact, Entity).

    Args:
        tables_by_type: Dictionary mapping table names to entity type

    Returns:
        Ordered list of (table_name, entity_type) tuples
    """
    node_tables = [
      (name, etype) for name, etype in tables_by_type.items() if not name.isupper()
    ]
    rel_tables = [
      (name, etype) for name, etype in tables_by_type.items() if name.isupper()
    ]
    return node_tables + rel_tables

  async def copy_table(
    self,
    client: "GraphClient",
    table_name: str,
    entity_type: str,
    quarterly_partitions: list[str] | None = None,
    year: int | None = None,
    quarter_copy_timeout: float = 1800.0,
    progress_callback: ProgressCallback | None = None,
  ) -> tuple[bool, int, str | None]:
    """
    Copy a single table from S3 to LadybugDB.

    Large tables (in LARGE_TABLES_FOR_BATCHING) are copied quarter-by-quarter.
    Small tables use a single COPY with glob pattern.

    Args:
        client: Graph API client
        table_name: Name of the table to copy
        entity_type: "nodes" or "relationships"
        quarterly_partitions: List of quarter keys for batching (e.g., ["2024-Q1", ...])
        year: Optional year filter for glob pattern
        quarter_copy_timeout: Timeout for each quarter copy operation
        progress_callback: Optional callback for progress logging

    Returns:
        Tuple of (success, rows_copied, error_message)
    """
    log_progress = make_progress_logger(progress_callback)
    is_large = table_name in LARGE_TABLES_FOR_BATCHING
    use_batching = is_large and quarterly_partitions and len(quarterly_partitions) > 0

    if use_batching:
      return await self._copy_table_batched(
        client=client,
        table_name=table_name,
        entity_type=entity_type,
        quarterly_partitions=quarterly_partitions,
        quarter_copy_timeout=quarter_copy_timeout,
        progress_callback=log_progress,
      )
    else:
      return await self._copy_table_single(
        client=client,
        table_name=table_name,
        entity_type=entity_type,
        year=year,
        timeout=quarter_copy_timeout,
        progress_callback=log_progress,
      )

  async def _copy_table_batched(
    self,
    client: "GraphClient",
    table_name: str,
    entity_type: str,
    quarterly_partitions: list[str],
    quarter_copy_timeout: float,
    progress_callback: ProgressCallback,
  ) -> tuple[bool, int, str | None]:
    """Copy a large table quarter-by-quarter."""
    log_progress = progress_callback
    total_rows = 0

    log_progress(
      f"Copying {table_name} by quarter ({len(quarterly_partitions)} quarters)..."
    )

    for q_idx, quarter_key in enumerate(quarterly_partitions, 1):
      s3_pattern = (
        f"s3://{self.bucket}/{self.source_prefix}/filed={quarter_key}/"
        f"{entity_type}/{table_name}.parquet"
      )

      log_progress(f"  [{quarter_key}] Quarter {q_idx}/{len(quarterly_partitions)}...")

      try:
        copy_result = await client.copy_from_s3(
          graph_id=self.graph_id,
          table_name=table_name,
          s3_pattern=s3_pattern,
          ignore_errors=True,
          timeout=quarter_copy_timeout,
          wait_for_completion=True,
        )

        if copy_result.get("status") == "completed":
          records = copy_result.get("records_loaded", 0)
          duration = copy_result.get("duration_seconds", 0)
          total_rows += records
          if records > 0:
            log_progress(f"  [{quarter_key}] {records:,} records in {duration:.1f}s")
          else:
            log_progress(f"  [{quarter_key}] No records (empty or no file)")
        elif copy_result.get("status") == "failed":
          error = copy_result.get("error", "Unknown error")
          if "No files found" in error or "does not exist" in error.lower():
            log_progress(f"  [{quarter_key}] No files (skipped)")
          else:
            log_progress(f"  [{quarter_key}] Failed: {error}")
            return False, total_rows, f"Quarter {quarter_key} failed: {error}"
        else:
          log_progress(f"  [{quarter_key}] Unknown status: {copy_result.get('status')}")

      except Exception as e:
        error_str = str(e)
        if "No files found" in error_str or "does not exist" in error_str.lower():
          log_progress(f"  [{quarter_key}] No files (skipped)")
        else:
          log_progress(f"  [{quarter_key}] Error: {error_str}")
          return False, total_rows, f"Quarter {quarter_key} error: {error_str}"

    log_progress(f"Copied {table_name}: {total_rows:,} total rows")
    return True, total_rows, None

  async def _copy_table_single(
    self,
    client: "GraphClient",
    table_name: str,
    entity_type: str,
    year: int | None,
    timeout: float,
    progress_callback: ProgressCallback,
  ) -> tuple[bool, int, str | None]:
    """Copy a table with a single glob pattern."""
    log_progress = progress_callback

    # Build glob pattern
    if year:
      filed_pattern = f"filed={year}-Q*"
    else:
      filed_pattern = "filed=*-Q*"

    s3_pattern = (
      f"s3://{self.bucket}/{self.source_prefix}/"
      f"{filed_pattern}/{entity_type}/{table_name}.parquet"
    )

    log_progress(f"Copying {table_name} (single COPY)...")

    try:
      copy_result = await client.copy_from_s3(
        graph_id=self.graph_id,
        table_name=table_name,
        s3_pattern=s3_pattern,
        ignore_errors=True,
        timeout=timeout,
        wait_for_completion=True,
      )

      if copy_result.get("status") == "completed":
        records = copy_result.get("records_loaded", 0)
        duration = copy_result.get("duration_seconds", 0)
        log_progress(f"Copied {table_name}: {records:,} records in {duration:.1f}s")
        return True, records, None
      elif copy_result.get("status") == "failed":
        error = copy_result.get("error", "Unknown error")
        if "No files found" in error or "does not exist" in error.lower():
          log_progress(f"Skipped {table_name}: no files")
          return True, 0, None
        else:
          log_progress(f"Failed {table_name}: {error}")
          return False, 0, error
      else:
        log_progress(f"Unknown status for {table_name}: {copy_result.get('status')}")
        return False, 0, f"Unknown status: {copy_result.get('status')}"

    except Exception as e:
      error_str = str(e)
      if "No files found" in error_str or "does not exist" in error_str.lower():
        log_progress(f"Skipped {table_name}: no files")
        return True, 0, None
      else:
        log_progress(f"Error copying {table_name}: {error_str}")
        return False, 0, error_str

  async def copy_all_tables(
    self,
    client: "GraphClient",
    year: int | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    skip_taxonomy_relationships: bool = False,
    skip_tables: list[str] | None = None,
    existing_table_names: set[str] | None = None,
    quarter_copy_timeout: float = 1800.0,
    progress_callback: ProgressCallback | None = None,
  ) -> DirectCopyResult:
    """
    Copy all tables from S3 to LadybugDB.

    Args:
        client: Graph API client
        year: Optional single year filter
        start_year: Optional start of year range (inclusive)
        end_year: Optional end of year range (inclusive)
        skip_taxonomy_relationships: If True, skip taxonomy structure tables
        skip_tables: Optional list of table names to skip
        existing_table_names: Optional set of tables that exist in the database
        quarter_copy_timeout: Timeout for each quarter copy operation
        progress_callback: Optional callback for progress logging

    Returns:
        DirectCopyResult with copy statistics
    """
    start_time = time.time()
    log_progress = make_progress_logger(progress_callback)

    # Get and order tables
    tables_by_type = self.get_tables_for_copy(
      skip_taxonomy_relationships=skip_taxonomy_relationships,
      skip_tables=skip_tables,
      existing_table_names=existing_table_names,
    )
    ordered_tables = self.order_tables_for_copy(tables_by_type)

    log_progress(f"Copying {len(ordered_tables)} tables from S3 to LadybugDB...")

    # Discover quarterly partitions for batching
    quarterly_partitions = await self.discover_quarterly_partitions(
      year=year, start_year=start_year, end_year=end_year
    )
    log_progress(f"Found {len(quarterly_partitions)} quarterly partitions")

    # Copy each table
    tables_copied = []
    failed_tables = []
    total_rows = 0

    for i, (table_name, entity_type) in enumerate(ordered_tables, 1):
      log_progress(f"[{i}/{len(ordered_tables)}] {table_name}...")

      success, rows, error = await self.copy_table(
        client=client,
        table_name=table_name,
        entity_type=entity_type,
        quarterly_partitions=quarterly_partitions,
        year=year,
        quarter_copy_timeout=quarter_copy_timeout,
        progress_callback=progress_callback,
      )

      if success:
        tables_copied.append(table_name)
        total_rows += rows
      else:
        failed_tables.append({"table": table_name, "error": error})

    duration_ms = (time.time() - start_time) * 1000
    status = (
      "success" if not failed_tables else ("partial" if tables_copied else "error")
    )

    log_progress(
      f"Direct copy complete: {len(tables_copied)}/{len(ordered_tables)} tables, "
      f"{total_rows:,} rows in {duration_ms / 1000:.1f}s"
    )

    return DirectCopyResult(
      status=status,
      tables_copied=tables_copied,
      failed_tables=failed_tables,
      total_rows=total_rows,
      duration_ms=duration_ms,
    )
