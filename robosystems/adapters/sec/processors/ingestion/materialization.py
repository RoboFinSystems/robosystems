"""
LadybugDB Materialization Operations for XBRL Graph Ingestion.

This module handles Stage 2 of the ingestion pipeline: materializing data
from DuckDB staging tables into the LadybugDB graph database.

Key features:
- Schema-driven: Table names come from RoboLedgerContext
- Batch materialization: Large tables are materialized in batches to prevent OOM
- Direct S3 copy: Incremental updates can bypass DuckDB staging
- Entity updates: MERGE-based updates for mutable Entity attributes

Classes:
    LadybugMaterializer: Handles all LadybugDB materialization operations
"""

import io
import math
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
  from robosystems.graph_api.client.client import GraphClient

from robosystems.config import env
from robosystems.graph_api.client.factory import get_graph_client
from robosystems.logger import logger
from robosystems.operations.aws.s3 import S3Client
from robosystems.schemas.extensions.roboledger import RoboLedgerContext

from .models import (
  CHUNKED_MATERIALIZATION_TIMEOUT,
  ENTITY_UPDATE_BATCH_SIZE,
  INCREMENTAL_COPY_TIMEOUT,
  LARGE_STAGING_TABLES,
  MATERIALIZATION_BATCH_SIZE,
  TAXONOMY_STRUCTURE_TABLES,
  EntityUpdateResult,
  MaterializeResult,
  ProgressCallback,
  get_materialization_timeout,
  make_progress_logger,
  s3_get_table_patterns,
  s3_table_data_exists,
)


def _is_empty_value(value: Any) -> bool:
  """Check if a value is empty/missing, handling pandas and numpy types."""
  if value is None:
    return True
  if isinstance(value, str):
    return value.strip() == ""
  try:
    import pandas as pd

    if pd.isna(value):
      return True
  except (TypeError, ValueError):
    pass
  return False


def _to_native(value: Any) -> Any:
  """Convert pandas/numpy value to a JSON-serializable Python native type."""
  if _is_empty_value(value):
    return None
  if hasattr(value, "item"):
    return value.item()
  if isinstance(value, str):
    return value.strip()
  if isinstance(value, float) and math.isfinite(value):
    return value
  if isinstance(value, (int, bool)):
    return value
  return str(value).strip()


class LadybugMaterializer:
  """
  LadybugDB materialization operations for XBRL graph data.

  This class handles Stage 2 of the ingestion pipeline - materializing
  data from DuckDB staging tables into the LadybugDB graph database.

  Also supports direct S3→LadybugDB copy for incremental updates.
  """

  def __init__(self, graph_id: str = "sec", source_prefix: str | None = None):
    """
    Initialize LadybugDB materializer.

    Args:
        graph_id: Graph database identifier (default: "sec")
        source_prefix: S3 prefix for source files (default: "sec/processed")
    """
    self.graph_id = graph_id
    self.s3_client = S3Client()
    self.bucket = env.SHARED_PROCESSED_BUCKET
    self.source_prefix = source_prefix or "sec/processed"

  async def materialize_from_duckdb(
    self,
    table_names: list[str] | None = None,
    rebuild: bool = False,
    skip_taxonomy_relationships: bool = False,
    batch_materialization: bool = True,
    batch_size: int = MATERIALIZATION_BATCH_SIZE,
    progress_callback: ProgressCallback | None = None,
  ) -> MaterializeResult:
    """
    Materialize LadybugDB graph from existing DuckDB staging.

    This is Stage 2 of the decoupled pipeline. It uses the schema to determine
    which tables to materialize and triggers ingestion for each table.

    Schema-Driven Design:
    - Table names come from RoboLedgerContext.get_all_table_names_for_context()
    - No manifest file needed - schema is the source of truth

    Precondition: stage_to_duckdb() must have been run successfully, creating
    DuckDB staging tables.

    Args:
        table_names: Optional list of specific tables to materialize.
                     If None, materializes all tables from the schema.
        rebuild: If True, delete and recreate the LadybugDB database.
        skip_taxonomy_relationships: If True, skip taxonomy structure tables.
        batch_materialization: If True (default), use hash-based batching
                     for large tables to prevent OOM.
        batch_size: Rows per batch (default: 20M).
        progress_callback: Optional callback for progress logging.

    Returns:
        MaterializeResult with rows ingested per table
    """
    start_time = time.time()
    log_progress = make_progress_logger(progress_callback)

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

      # Filter out taxonomy structure tables if requested
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
        logger.info(
          "Step 3: Rebuilding LadybugDB database (DuckDB staging preserved)..."
        )
        await self._rebuild_ladybug_database(client, reset_staging=False)
        logger.info("LadybugDB database rebuilt with roboledger SEC schema")
      else:
        logger.info("Step 3: Ensuring LadybugDB database exists with schema...")
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
        table_names,
        client,
        batch_materialization=batch_materialization,
        batch_size=batch_size,
        progress_callback=log_progress,
      )

      duration = time.time() - start_time
      failed_count = ingestion_results.get("failed_count", 0)
      status = "partial" if failed_count > 0 else "success"

      log_progress(
        f"Materialization complete in {duration:.2f}s: "
        f"{ingestion_results.get('total_rows_ingested', 0):,} rows ingested"
      )
      return MaterializeResult(
        status=status,
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

  async def copy_incremental_to_ladybug(
    self,
    year: int | None = None,
    quarter: int | None = None,
    skip_taxonomy_relationships: bool = False,
    copy_timeout: int = INCREMENTAL_COPY_TIMEOUT,
    progress_callback: ProgressCallback | None = None,
  ) -> MaterializeResult:
    """
    Copy current quarter's files directly to LadybugDB (bypasses DuckDB staging).

    This is the preferred approach for incremental updates because:
    - LadybugDB's COPY with ignore_errors=true handles duplicates automatically
    - No need to diff what's new in DuckDB vs LadybugDB
    - Simpler and faster for daily updates

    Uses overlap logic at quarter boundaries to catch late-indexed filings.

    Safe to run daily - duplicates are rejected by LadybugDB constraints.

    Precondition: LadybugDB database must already exist with SEC schema.

    Args:
        year: Year to copy (default: current year)
        quarter: Quarter to copy 1-4 (default: current quarter)
        skip_taxonomy_relationships: If True, skip taxonomy structure tables
        copy_timeout: Timeout per table (default: 10 min)
        progress_callback: Optional callback for Dagster logging

    Returns:
        MaterializeResult with tables copied and row counts
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

      # Sort: nodes first, then relationships
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

      total_tables = len(ordered_tables)
      for i, (table_name, entity_type) in enumerate(ordered_tables, 1):
        # Build S3 paths for all quarters, only including formats that exist.
        # s3_get_table_patterns checks each format individually to avoid DuckDB
        # errors from literal paths (no wildcards) that don't exist on S3.
        s3_paths: list[str] = []
        for y, q in quarters_to_scan:
          filed_pattern = f"filed={y}-Q{q}"
          s3_paths.extend(
            s3_get_table_patterns(
              self.s3_client,
              self.bucket,
              self.source_prefix,
              filed_pattern,
              entity_type,
              table_name,
            )
          )

        if not s3_paths:
          log_progress(
            f"[{i}/{total_tables}] Skipped {table_name}: no files for any quarter"
          )
          successful_tables.append(table_name)
          table_rows[table_name] = 0
          continue

        s3_pattern: str | list[str] = s3_paths[0] if len(s3_paths) == 1 else s3_paths

        log_progress(f"[{i}/{total_tables}] COPY {table_name} (Q{quarter} {year})...")

        try:
          copy_result = await client.copy_from_s3(
            graph_id=self.graph_id,
            table_name=table_name,
            s3_pattern=s3_pattern,
            ignore_errors=True,
            timeout=copy_timeout,
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

  async def update_entities_from_s3(
    self,
    year: int | None = None,
    quarter: int | None = None,
    progress_callback: ProgressCallback | None = None,
  ) -> EntityUpdateResult:
    """
    Update existing Entity nodes with latest data from S3 parquet files.

    This solves the Entity mutability problem: unlike other XBRL nodes (facts,
    periods, etc.) which are immutable, Entity attributes can change over time
    (company name, ticker, exchange, filer category, etc.).

    The incremental COPY operation only INSERTs new records - it cannot update
    existing ones due to primary key constraints. This method uses Cypher MERGE
    to update existing Entity nodes.

    Process:
    1. Read latest Entity parquet from S3 (current quarter)
    2. Query existing Entity nodes from LadybugDB
    3. Compare and identify entities with actual changes
    4. Execute MERGE queries in batches to update changed entities

    Note: MERGE is 40x slower than COPY (~200ms vs ~5ms per record), but
    Entity updates are typically small (50-200 per quarter).

    Args:
        year: Year to process (default: current year)
        quarter: Quarter to process 1-4 (default: current quarter)
        progress_callback: Optional callback for Dagster logging

    Returns:
        EntityUpdateResult with update statistics
    """
    from robosystems.adapters.sec import get_current_quarter

    start_time = time.time()
    log_progress = make_progress_logger(progress_callback)

    # Default to current year/quarter
    now = datetime.now(UTC)
    if year is None or quarter is None:
      year, quarter = get_current_quarter(now)

    log_progress(
      f"Starting Entity update for graph {self.graph_id} (Q{quarter} {year})"
    )

    try:
      # Step 1: Find Entity parquet(s) — supports both old and new format
      filed_pattern = f"filed={year}-Q{quarter}"
      has_data = s3_table_data_exists(
        self.s3_client,
        self.bucket,
        self.source_prefix,
        filed_pattern,
        "nodes",
        "Entity",
      )

      if not has_data:
        log_progress(f"No Entity file found for Q{quarter} {year}, skipping")
        return EntityUpdateResult(
          status="no_changes",
          entities_checked=0,
          duration_ms=(time.time() - start_time) * 1000,
        )

      # Step 2: Read Entity parquet from S3 using boto3 + pyarrow
      # Try old format first (single file), then new format (part files)
      log_progress(f"Reading Entity parquet from S3 for Q{quarter} {year}")

      old_key = f"{self.source_prefix}/{filed_pattern}/nodes/Entity.parquet"
      if self.s3_client.object_exists(self.bucket, old_key):
        # Old format: single file
        response = self.s3_client.s3_client.get_object(Bucket=self.bucket, Key=old_key)
        parquet_bytes = response["Body"].read()
        table = pq.read_table(io.BytesIO(parquet_bytes))
      else:
        # New format: part files under Entity/ directory
        part_prefix = f"{self.source_prefix}/{filed_pattern}/nodes/Entity/"
        part_keys = self.s3_client.list_objects(self.bucket, prefix=part_prefix)
        part_keys = [k for k in part_keys if k.endswith(".parquet")]
        if not part_keys:
          log_progress(f"No Entity part files found for Q{quarter} {year}, skipping")
          return EntityUpdateResult(
            status="no_changes",
            entities_checked=0,
            duration_ms=(time.time() - start_time) * 1000,
          )
        # Read and concat all part files (Entity is small, ~256KB/quarter)
        # Dedup on identifier — same company filing multiple forms in a quarter
        # appears in multiple flush batches and therefore multiple part files
        from robosystems.adapters.sec.processors.consolidation import (
          _dedup_arrow_table,
        )

        tables = []
        for key in part_keys:
          response = self.s3_client.s3_client.get_object(Bucket=self.bucket, Key=key)
          part_bytes = response["Body"].read()
          tables.append(pq.read_table(io.BytesIO(part_bytes)))
        table = pa.concat_tables(tables, promote_options="permissive")
        if "identifier" in table.column_names:
          table = _dedup_arrow_table(table, "identifier", "Entity")

      latest_entities = table.to_pandas()

      if latest_entities.empty:
        log_progress("Entity parquet is empty, no updates needed")
        return EntityUpdateResult(
          status="no_changes",
          entities_checked=0,
          duration_ms=(time.time() - start_time) * 1000,
        )

      entities_checked = len(latest_entities)
      log_progress(f"Loaded {entities_checked} entities from S3")

      # Step 3: Get existing Entity data from LadybugDB
      client = await get_graph_client(graph_id=self.graph_id, operation_type="write")

      identifiers = latest_entities["identifier"].tolist()

      # Batch identifiers to avoid huge IN clause
      existing_entities: dict[str, dict] = {}
      batch_size = 500
      for i in range(0, len(identifiers), batch_size):
        batch = identifiers[i : i + batch_size]
        result = await client.query(
          cypher="""
                        MATCH (e:Entity)
                        WHERE e.identifier IN $identifiers
                        RETURN e.identifier as identifier,
                               e.name as name,
                               e.legal_name as legal_name,
                               e.ticker as ticker,
                               e.exchange as exchange,
                               e.category as category,
                               e.fiscal_year_end as fiscal_year_end,
                               e.phone as phone,
                               e.website as website,
                               e.status as status,
                               e.sic as sic,
                               e.sic_description as sic_description
                    """,
          graph_id=self.graph_id,
          parameters={"identifiers": batch},
        )
        for row in result.get("data", []):
          existing_entities[row["identifier"]] = row

      log_progress(f"Found {len(existing_entities)} existing entities in graph")

      # Step 4: Compare and identify entities with changes
      updatable_fields = [
        "name",
        "legal_name",
        "ticker",
        "exchange",
        "category",
        "fiscal_year_end",
        "phone",
        "website",
        "status",
        "sic",
        "sic_description",
      ]

      entities_to_update: list[dict] = []
      for _, row in latest_entities.iterrows():
        identifier = row["identifier"]
        existing = existing_entities.get(identifier)

        if not existing:
          # Entity doesn't exist in graph yet (will be handled by COPY)
          continue

        # Check if any updatable field has changed
        has_changes = False
        update_data = {"identifier": identifier}

        for field in updatable_fields:
          new_value = row.get(field)
          old_value = existing.get(field)

          new_is_empty = _is_empty_value(new_value)
          old_is_empty = old_value is None or old_value == ""

          if new_is_empty and old_is_empty:
            continue

          new_str = "" if new_is_empty else str(new_value)
          old_str = "" if old_is_empty else str(old_value)

          if new_str != old_str:
            has_changes = True
            update_data[field] = _to_native(new_value) if not new_is_empty else None

        if has_changes:
          entities_to_update.append(update_data)

      entities_unchanged = len(existing_entities) - len(entities_to_update)
      log_progress(
        f"Found {len(entities_to_update)} entities with changes, "
        f"{entities_unchanged} unchanged"
      )

      if not entities_to_update:
        return EntityUpdateResult(
          status="no_changes",
          entities_checked=entities_checked,
          entities_unchanged=entities_unchanged,
          duration_ms=(time.time() - start_time) * 1000,
        )

      # Step 5: Execute MERGE queries in batches
      entities_updated = 0
      entities_failed = 0

      for i in range(0, len(entities_to_update), ENTITY_UPDATE_BATCH_SIZE):
        batch = entities_to_update[i : i + ENTITY_UPDATE_BATCH_SIZE]
        batch_num = i // ENTITY_UPDATE_BATCH_SIZE + 1
        total_batches = (
          len(entities_to_update) + ENTITY_UPDATE_BATCH_SIZE - 1
        ) // ENTITY_UPDATE_BATCH_SIZE

        log_progress(f"[{batch_num}/{total_batches}] Updating {len(batch)} entities...")

        for entity_data in batch:
          try:
            # Build SET clause dynamically for only changed fields
            set_clauses = []
            params = {"identifier": entity_data["identifier"]}

            for field, value in entity_data.items():
              if field == "identifier":
                continue
              param_name = f"p_{field}"
              set_clauses.append(f"e.{field} = ${param_name}")
              params[param_name] = value

            if not set_clauses:
              continue

            # Add updated_at timestamp
            set_clauses.append("e.updated_at = $updated_at")
            params["updated_at"] = datetime.now(UTC).isoformat()

            cypher = f"""
                            MATCH (e:Entity {{identifier: $identifier}})
                            SET {", ".join(set_clauses)}
                            RETURN e.identifier
                        """

            await client.query(
              cypher=cypher,
              graph_id=self.graph_id,
              parameters=params,
            )
            entities_updated += 1

          except Exception as e:
            logger.warning(f"Failed to update entity {entity_data['identifier']}: {e}")
            entities_failed += 1

      duration = time.time() - start_time
      status = "success" if entities_failed == 0 else "partial"

      log_progress(
        f"Entity update complete: {entities_updated} updated, "
        f"{entities_failed} failed, {duration:.2f}s"
      )

      return EntityUpdateResult(
        status=status,
        entities_checked=entities_checked,
        entities_updated=entities_updated,
        entities_unchanged=entities_unchanged,
        entities_failed=entities_failed,
        duration_ms=duration * 1000,
      )

    except Exception as e:
      logger.error(f"Entity update failed: {e}", exc_info=True)
      return EntityUpdateResult(
        status="error",
        error=str(e),
        duration_ms=(time.time() - start_time) * 1000,
      )

  # =========================================================================
  # Private Helper Methods
  # =========================================================================

  async def _rebuild_ladybug_database(
    self, client: "GraphClient", reset_staging: bool = False
  ) -> None:
    """Rebuild the LadybugDB database from scratch.

    Deletes the existing database and recreates it with the same schema.

    Args:
        client: Graph API client instance
        reset_staging: If True, also delete DuckDB staging.
    """
    from robosystems.database import SessionFactory
    from robosystems.graph_api.client.exceptions import GraphClientError
    from robosystems.middleware.graph.utils.subgraph import (
      is_subgraph,
      parse_subgraph_id,
    )
    from robosystems.models.iam import GraphSchema

    logger.info(
      f"Rebuild requested - regenerating entire LadybugDB database for {self.graph_id}"
    )

    db = SessionFactory()
    try:
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

      # For shared repos, always use the contextual schema loader (codebase source of truth).
      # Stored GraphSchema records can become stale when the schema evolves.
      from robosystems.schemas.loader import get_contextual_schema_loader
      from robosystems.schemas.models import Schema

      subgraph_info = parse_subgraph_id(self.graph_id)
      repo_name = subgraph_info.parent_graph_id if subgraph_info else self.graph_id

      loader = get_contextual_schema_loader("repository", repo_name)
      if not loader.nodes:
        raise ValueError(f"No schema found for graph {self.graph_id}")

      compiled = Schema(
        name=f"{repo_name.upper()} Repository Schema",
        description=f"Contextual schema for {repo_name} repository",
        nodes=list(loader.nodes.values()),
        relationships=list(loader.relationships.values()),
      )
      schema_ddl = compiled.to_cypher()
      schema_type = "shared"
      logger.info(
        f"Rebuild: loaded schema from codebase for {self.graph_id} "
        f"({len(loader.nodes)} nodes, {len(loader.relationships)} relationships)"
      )

      # Update the stored GraphSchema record so it stays in sync
      existing_schema = GraphSchema.get_active_schema(self.graph_id, db)
      if existing_schema:
        existing_schema.schema_ddl = schema_ddl
        db.commit()
        logger.info(f"Updated stored GraphSchema for {self.graph_id}")
      else:
        GraphSchema.create(
          graph_id=self.graph_id,
          schema_type=schema_type,
          schema_ddl=schema_ddl,
          session=db,
        )
        logger.info(f"Created new GraphSchema record for {self.graph_id}")

      # Create database (without schema DDL — subgraphs skip schema application)
      create_db_kwargs: dict[str, Any] = {
        "graph_id": self.graph_id,
        "schema_type": schema_type,
        "is_subgraph": is_subgraph(self.graph_id),
      }

      if schema_type == "shared":
        create_db_kwargs["repository_name"] = self.graph_id

      await client.create_database(**create_db_kwargs)
      logger.info(f"Recreated LadybugDB database with schema type: {schema_type}")

      # Install schema separately (create_database skips schema for subgraphs)
      result = await client.install_schema(
        graph_id=self.graph_id, custom_ddl=schema_ddl
      )
      logger.info(f"Schema installed: {result}")
    finally:
      db.close()

  async def _trigger_ingestion(
    self,
    table_names: list[str],
    graph_client: "GraphClient",
    batch_materialization: bool = True,
    batch_size: int = MATERIALIZATION_BATCH_SIZE,
    progress_callback: ProgressCallback | None = None,
  ) -> dict[str, Any]:
    """
    Trigger ingestion for all tables into LadybugDB graph via Graph API.

    For large tables (>batch_size rows), uses chunked materialization to avoid OOM.

    Args:
        table_names: List of table names to ingest
        graph_client: Graph API client instance
        batch_materialization: If True (default), use batching for large tables.
        batch_size: Rows per batch (default: 20M).
        progress_callback: Optional callback for progress logging

    Returns:
        Ingestion results with statistics
    """
    log_progress = make_progress_logger(progress_callback)

    total_rows = 0
    total_time_ms = 0.0
    results = []
    total_tables = len(table_names)

    for i, table_name in enumerate(table_names, 1):
      is_large = table_name in LARGE_STAGING_TABLES
      timeout = get_materialization_timeout(table_name)

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
              row_count = count_response["rows"][0][0]
          except Exception as count_err:
            logger.warning(f"Could not get row count for {table_name}: {count_err}")
            row_count = 0

        # Use hash-based batched materialization for very large tables
        if batch_materialization and row_count > batch_size:
          num_batches = (row_count + batch_size - 1) // batch_size
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
          # Standard single-pass materialization
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
        log_progress(f"[{i}/{total_tables}] FAILED {table_name}: {e}")
        results.append(
          {
            "table_name": table_name,
            "status": "error",
            "error": str(e),
          }
        )

    # Log summary of any failures so they're easy to find in logs
    failed = [r for r in results if r.get("status") == "error"]
    succeeded = [r for r in results if r.get("status") != "error"]

    if failed:
      failed_names = [r["table_name"] for r in failed]
      log_progress(
        f"MATERIALIZATION SUMMARY: {len(succeeded)}/{total_tables} tables succeeded, "
        f"{len(failed)} FAILED: {failed_names}"
      )
      for r in failed:
        log_progress(f"  FAILED: {r['table_name']} — {r.get('error', 'unknown error')}")
    else:
      log_progress(
        f"MATERIALIZATION SUMMARY: All {total_tables}/{total_tables} tables succeeded"
      )

    return {
      "total_rows_ingested": total_rows,
      "duration_ms": total_time_ms,
      "tables": results,
      "failed_count": len(failed),
    }
