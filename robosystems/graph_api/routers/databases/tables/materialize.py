import os
from uuid import uuid4

from fastapi import APIRouter, Body, Depends, HTTPException, Path
from fastapi import status as http_status

from robosystems.config import env
from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.models.fork import (
  ForkFromParentRequest,
  ForkFromParentResponse,
)
from robosystems.graph_api.models.tables import (
  TableMaterializationRequest,
  TableMaterializationResponse,
)
from robosystems.logger import logger
from robosystems.middleware.graph.instance_busy import (
  OP_KIND_MATERIALIZATION,
  instance_busy,
)

# Type mapping from LadybugDB types to DuckDB-compatible cast types
_LBUG_TO_DUCK_TYPE = {
  "STRING": "VARCHAR",
  "INT64": "BIGINT",
  "INT32": "INT",
  "DOUBLE": "DOUBLE",
  "BOOL": "BOOLEAN",
  "DATE": "DATE",
  "TIMESTAMP": "TIMESTAMP",
}


def _lbug_type_to_duck(lbug_type: str) -> str:
  """Map a LadybugDB column type to a DuckDB-compatible type for casting."""
  mapped = _LBUG_TO_DUCK_TYPE.get(lbug_type.upper())
  if mapped:
    return mapped
  # Pass through parameterized types like FLOAT[384] — DuckDB supports them natively
  if "[" in lbug_type:
    return lbug_type
  return "VARCHAR"


def _get_target_columns(
  ladybug_service, graph_id: str, table_name: str
) -> list[tuple[str, str]] | None:
  """Get column names and types from the LadybugDB target table.

  Returns list of (name, lbug_type) tuples, or None if unavailable.
  """
  try:
    with ladybug_service.db_manager.connection_pool.get_connection(graph_id) as conn:
      result = conn.execute(f"CALL TABLE_INFO('{table_name}') RETURN *")
      rows = result.get_as_list() if hasattr(result, "get_as_list") else list(result)
      columns = []
      for row in rows:
        # TABLE_INFO returns: [index, name, type, default, isPrimaryKey]
        if isinstance(row, (list, tuple)) and len(row) >= 3:
          columns.append((row[1], row[2]))
        elif isinstance(row, dict):
          values = next(iter(row.values())) if len(row) == 1 else row
          if isinstance(values, (list, tuple)) and len(values) >= 3:
            columns.append((values[1], values[2]))
      return columns if columns else None
  except Exception as exc:
    logger.debug(f"Could not get target columns for {table_name}: {exc}")
    return None


def _build_type_safe_select(
  source_columns: list[tuple[str, str]],
  exclude_cols: set[str] | None = None,
  null_cols: set[str] | None = None,
) -> str:
  """Build a SELECT for parquet export when the target schema is unknown.

  Preserves source column order; casts DECIMAL columns to DOUBLE because
  LadybugDB's parquet reader rejects DECIMAL converted types (postgres_scan
  stages Postgres NUMERIC as DuckDB DECIMAL). Columns in null_cols are
  exported as typed NULLs (preserving column count for positional COPY).
  """
  exclude = exclude_cols or set()
  nullify = null_cols or set()
  parts = []
  for col_name, duck_type in source_columns:
    if col_name in exclude:
      continue
    if col_name in nullify:
      parts.append(f'NULL::{duck_type} AS "{col_name}"')
    elif duck_type.upper().startswith("DECIMAL"):
      parts.append(f'CAST("{col_name}" AS DOUBLE) AS "{col_name}"')
    else:
      parts.append(f'"{col_name}"')
  return ", ".join(parts)


def _build_reconciled_select(
  target_columns: list[tuple[str, str]],
  source_column_names: list[str],
  source_table: str,
  exclude_cols: set[str] | None = None,
  null_cols: set[str] | None = None,
) -> str:
  """Build a SELECT expression that reconciles source columns to target schema.

  Adds missing columns as NULL with correct type. Casts existing columns
  to the target type (handles DuckDB inferring NULL columns as INT32).
  Columns in null_cols are included as NULL (preserving column count for COPY).

  For relationship tables, `from` and `to` are implicit in LadybugDB's
  TABLE_INFO but must be included in the DuckDB source. They are passed
  through first if present.
  """
  exclude = exclude_cols or set()
  nullify = null_cols or set()
  parts = []
  source_set = set(source_column_names)
  target_name_set = {c[0] for c in target_columns}

  # Pass through relationship key columns first — TABLE_INFO omits them for
  # rel tables but COPY requires them. DuckDB uses src/dst (from/to are reserved).
  for implicit_col in ("from", "to", "src", "dst"):
    if implicit_col in source_set and implicit_col not in target_name_set:
      parts.append(f'"{implicit_col}"')

  for col_name, lbug_type in target_columns:
    if col_name in exclude:
      continue
    duck_type = _lbug_type_to_duck(lbug_type)
    if col_name in nullify:
      parts.append(f"NULL::{duck_type} AS {col_name}")
    elif col_name in source_set:
      parts.append(f"TRY_CAST({col_name} AS {duck_type}) AS {col_name}")
    else:
      parts.append(f"NULL::{duck_type} AS {col_name}")
  return ", ".join(parts)


# Constants for checkpoint retry logic
CHECKPOINT_MAX_RETRIES = 3
CHECKPOINT_RETRY_DELAY_SECONDS = 1


def checkpoint_with_retry(conn, graph_id: str, context: str = "DuckDB") -> None:
  """Checkpoint DuckDB database with retry logic.

  Args:
      conn: DuckDB connection
      graph_id: Graph ID for logging
      context: Context string for log messages (e.g., "DuckDB", "parent DuckDB")

  Raises:
      HTTPException: If checkpoint fails after all retries
  """
  import time

  for attempt in range(CHECKPOINT_MAX_RETRIES):
    try:
      conn.execute("CHECKPOINT")
      logger.debug(f"[OK] {context} checkpointed successfully for {graph_id}")
      return
    except Exception as e:
      if attempt == CHECKPOINT_MAX_RETRIES - 1:
        logger.error(
          f"Failed to checkpoint {context} after {CHECKPOINT_MAX_RETRIES} attempts: {e}"
        )
        raise HTTPException(
          status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
          detail=f"Failed to checkpoint {context} after {CHECKPOINT_MAX_RETRIES} attempts: {e!s}",
        )
      logger.warning(f"Checkpoint attempt {attempt + 1} failed, retrying... Error: {e}")
      time.sleep(CHECKPOINT_RETRY_DELAY_SECONDS)


router = APIRouter(prefix="/databases/{graph_id}/tables")


@router.post("/{table_name}/materialize", response_model=TableMaterializationResponse)
async def materialize_table(
  graph_id: str = Path(..., description="Graph database identifier"),
  table_name: str = Path(..., description="Table name to materialize from DuckDB"),
  request: TableMaterializationRequest = Body(...),
  ladybug_service=Depends(get_ladybug_service),
) -> TableMaterializationResponse:
  import time

  start_time = time.time()

  logger.info(
    f"Materializing table {table_name} from DuckDB to LadybugDB graph {graph_id}"
  )

  if ladybug_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Materialization not allowed on read-only nodes",
    )

  # Mark this instance busy so GHA pre-refresh workflows don't cycle the
  # container mid-materialization. The bulk_table_create / bulk_table_insert
  # endpoints already do this; materialize was the missing piece. Wraps the
  # full body so the counter decrements on exception too.
  async with instance_busy(env.INSTANCE_ID, OP_KIND_MATERIALIZATION):
    return await _materialize_table_impl(
      graph_id=graph_id,
      table_name=table_name,
      request=request,
      ladybug_service=ladybug_service,
      start_time=start_time,
    )


async def _materialize_table_impl(
  graph_id: str,
  table_name: str,
  request: TableMaterializationRequest,
  ladybug_service,
  start_time: float,
) -> TableMaterializationResponse:
  import time

  export_path: str | None = None

  try:
    # Blue-green: read DuckDB from source graph if specified, otherwise from target
    duckdb_graph_id = request.source_graph_id or graph_id

    from robosystems.graph_api.core.duckdb import get_duckdb_pool

    duckdb_pool = get_duckdb_pool()

    # Get target table schema from LadybugDB for column reconciliation
    # This handles schema evolution: staging tables missing new columns or
    # having mistyped NULL columns (e.g., DuckDB infers all-NULL as INT32
    # but target expects FLOAT[384])
    target_columns = _get_target_columns(ladybug_service, graph_id, table_name)

    try:
      with duckdb_pool.get_connection(duckdb_graph_id) as duck_conn:
        # Flush WAL so the parquet export sees all committed staging data
        checkpoint_with_retry(duck_conn, duckdb_graph_id, context="DuckDB")

        # Check if table exists
        result = duck_conn.execute(
          "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
          [table_name],
        ).fetchone()

        if not result or result[0] == 0:
          logger.info(
            f"Table {table_name} does not exist in DuckDB - skipping materialization (no data uploaded yet)"
          )
          return TableMaterializationResponse(
            status="skipped",
            graph_id=graph_id,
            table_name=table_name,
            rows_ingested=0,
            execution_time_ms=0.0,
          )

        source_columns = duck_conn.execute(
          "SELECT column_name, data_type FROM information_schema.columns "
          "WHERE table_name = ? ORDER BY ordinal_position",
          [table_name],
        ).fetchall()
        column_names = [col[0] for col in source_columns]
        has_file_id = "file_id" in column_names

        # Columns to NULL out (keep column for schema match, but skip data).
        # Embeddings stay in DuckDB staging for LanceDB vector search; materializing
        # them to LadybugDB is optional. Pass materialize_embeddings=true to include them.
        null_cols: set[str] = set()
        if "embedding" in column_names and not request.materialize_embeddings:
          null_cols.add("embedding")
          logger.info(
            f"Nulling embedding column for {table_name} materialization "
            f"(pass materialize_embeddings=true to include)"
          )

        # Build WHERE clause for batched materialization
        batch_clause = ""
        has_hash_batching = (
          request.num_batches is not None and request.batch_num is not None
        )
        if has_hash_batching:
          assert request.batch_num is not None and request.num_batches is not None

          if "identifier" in column_names:
            hash_col = "identifier"
          elif "src" in column_names and "dst" in column_names:
            hash_col = "src || '|' || dst"
          else:
            hash_col = column_names[0] if column_names else "rowid"

          batch_clause = f" WHERE abs(hash({hash_col}::VARCHAR)) % {request.num_batches} = {request.batch_num}"
          logger.info(
            f"Using hash-based batching for {table_name}: batch {request.batch_num + 1}/{request.num_batches}"
          )

        # Build file filter clause
        file_filter = ""
        query_params: list[str] = []
        if request.file_ids and has_file_id:
          file_ids_placeholders = ", ".join(["?" for _ in request.file_ids])
          file_filter = f"file_id IN ({file_ids_placeholders})"
          query_params.extend(request.file_ids)

        # Build WHERE clause combining file filter and batch clause
        where = ""
        if file_filter and batch_clause:
          where = f" WHERE {file_filter} AND ({batch_clause.replace(' WHERE ', '')})"
        elif file_filter:
          where = f" WHERE {file_filter}"
        elif batch_clause:
          where = batch_clause

        # Columns to exclude from materialization
        exclude_cols: set[str] = set()
        if has_file_id:
          exclude_cols.add("file_id")

        # Build the export SELECT. When the LadybugDB target schema is known,
        # reconcile to it — target column order, NULLs for missing columns,
        # and casts to the target types. The casts double as parquet type
        # normalization: LadybugDB's parquet reader rejects DECIMAL converted
        # types, and postgres_scan-staged NUMERIC columns arrive as DECIMAL.
        if target_columns:
          select_expr = _build_reconciled_select(
            target_columns,
            column_names,
            table_name,
            exclude_cols=exclude_cols,
            null_cols=null_cols,
          )
        else:
          select_expr = _build_type_safe_select(
            [(col[0], col[1]) for col in source_columns],
            exclude_cols=exclude_cols,
            null_cols=null_cols,
          )

        export_dir = f"{env.DUCKDB_STAGING_PATH}/materialize-exports"
        os.makedirs(export_dir, exist_ok=True)
        # Filename is a bare UUID — never interpolate the request-supplied
        # graph_id / table_name into a filesystem path (path traversal). The
        # graph/table identity is carried in the surrounding log lines.
        export_path = f"{export_dir}/{uuid4().hex}.parquet"

        export_sql = (
          f"COPY (SELECT {select_expr} FROM {table_name}{where}) "
          f"TO '{export_path}' (FORMAT parquet)"
        )
        if query_params:
          duck_conn.execute(export_sql, query_params)
        else:
          duck_conn.execute(export_sql)

        if request.file_ids:
          logger.info(
            f"Exported {table_name} to parquet with {len(request.file_ids)} file(s)"
          )
        elif has_hash_batching:
          assert request.batch_num is not None and request.num_batches is not None
          logger.info(
            f"Exported {table_name} to parquet "
            f"(batch {request.batch_num + 1}/{request.num_batches})"
          )
        else:
          logger.debug(f"Exported {table_name} to parquet for full materialization")

    except Exception as err:
      logger.error(f"Could not export DuckDB table for materialization: {err}")
      raise

    try:
      with ladybug_service.db_manager.connection_pool.get_connection(graph_id) as conn:
        if request.file_ids:
          logger.info(
            f"COPY {table_name} → {graph_id} ({len(request.file_ids)} file(s))"
          )
        else:
          logger.info(f"COPY {table_name} → {graph_id}")

        if request.ignore_errors:
          copy_query = f"COPY {table_name} FROM '{export_path}' (ignore_errors=true)"
        else:
          copy_query = f"COPY {table_name} FROM '{export_path}'"

        # Set extended timeout for COPY operations (30 minutes)
        # Default connection timeout is 120s, but large tables like Fact
        # can take 2-3 minutes with millions of rows
        try:
          conn.execute("CALL timeout=3600000")  # 60 minutes
          logger.debug(f"Executing: {copy_query}")
          result = conn.execute(copy_query)
        finally:
          # Always reset timeout to default after COPY
          conn.execute("CALL timeout=120000")  # 2 minutes

      rows_ingested = 0
      if result and hasattr(result, "get_as_arrow"):
        arrow_table = result.get_as_arrow()
        if arrow_table.num_rows > 0 and arrow_table.num_columns > 0:
          result_msg = str(arrow_table.column(0)[0].as_py())
          import re

          match = re.search(r"(\d+)\s+tuples?", result_msg)
          if match:
            rows_ingested = int(match.group(1))

      execution_time_ms = (time.time() - start_time) * 1000

      logger.info(
        f"Materialized {rows_ingested} rows from {table_name} in {execution_time_ms / 1000:.1f}s"
      )

      # Checkpoint and release LadybugDB memory
      # This prevents memory accumulation during multi-table materialization
      try:
        with ladybug_service.db_manager.connection_pool.get_connection(
          graph_id
        ) as conn:
          conn.execute("CHECKPOINT")
          logger.debug(f"Checkpointed LadybugDB after {table_name} materialization")

          # Build vector index when embeddings are materialized (single-pass only).
          # For batched requests, the caller rebuilds the index after all batches
          # complete — creating it on batch 1 would only index partial data.
          is_batched = request.batch_num is not None
          if request.materialize_embeddings and not is_batched:
            ladybug_service.db_manager.create_vector_index(conn, table_name)
            conn.execute("CHECKPOINT")

        # Release buffer pool memory - data is safe on disk now
        ladybug_service.db_manager.connection_pool.force_database_cleanup(
          graph_id, aggressive=True
        )
        logger.debug(f"Released LadybugDB memory after {table_name} materialization")
      except Exception as cleanup_err:
        # Log but don't fail - materialization succeeded
        logger.warning(f"Could not release LadybugDB memory: {cleanup_err}")

      return TableMaterializationResponse(
        status="success",
        graph_id=graph_id,
        table_name=table_name,
        rows_ingested=rows_ingested,
        execution_time_ms=execution_time_ms,
      )

    except Exception as e:
      logger.error(f"Failed to materialize table {table_name}: {e}")
      raise HTTPException(
        status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Failed to materialize table: {e!s}",
      )

    finally:
      # Clean up the parquet export and release DuckDB memory
      if export_path:
        try:
          os.remove(export_path)
          logger.debug(f"Cleaned up parquet export: {export_path}")
        except FileNotFoundError:
          pass  # already gone (export never created, or removed on a retry) — nothing to clean
        except Exception as rm_err:
          logger.warning(f"Failed to remove parquet export {export_path}: {rm_err}")

      try:
        with duckdb_pool.get_connection(duckdb_graph_id) as duck_conn:
          duck_conn.execute("CHECKPOINT")
        duckdb_pool.close_database_connections(duckdb_graph_id)
        logger.debug(f"Released DuckDB connections for {duckdb_graph_id}")
      except Exception as release_err:
        logger.warning(f"Failed to release DuckDB connections: {release_err}")

  except Exception as outer_err:
    logger.error(f"Failed during table export or materialization: {outer_err}")
    # An export-stage failure skips the inner finally — remove the file here too
    if export_path:
      try:
        os.remove(export_path)
      except OSError:
        pass  # best-effort cleanup on the export-stage failure path; original error is re-raised below
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to materialize table: {outer_err!s}",
    )


@router.post(
  "/{subgraph_id}/fork-from/{parent_graph_id}", response_model=ForkFromParentResponse
)
async def fork_from_parent_duckdb(
  graph_id: str = Path(
    ..., description="Graph database identifier (must match subgraph_id)"
  ),
  parent_graph_id: str = Path(..., description="Parent graph database identifier"),
  subgraph_id: str = Path(..., description="Subgraph database identifier"),
  request: ForkFromParentRequest = Body(...),
  ladybug_service=Depends(get_ladybug_service),
) -> ForkFromParentResponse:
  """
  Fork data from parent graph's DuckDB directly into subgraph's LadybugDB.

  This endpoint:
  1. Attaches parent graph's DuckDB staging database
  2. Copies specified tables (or all tables) from parent DuckDB to subgraph LadybugDB
  3. Runs on the same EC2 instance where both DuckDB and LadybugDB databases live

  Args:
      graph_id: Graph database identifier from router prefix (must equal subgraph_id)
      parent_graph_id: Parent graph to copy data from
      subgraph_id: Subgraph to copy data to
      request: Fork options (tables to copy, error handling)

  Returns:
      ForkFromParentResponse with tables copied and row counts
  """
  import time

  start_time = time.time()

  if graph_id != subgraph_id:
    raise HTTPException(
      status_code=http_status.HTTP_400_BAD_REQUEST,
      detail=f"graph_id ({graph_id}) must match subgraph_id ({subgraph_id})",
    )

  logger.info(f"Forking data from {parent_graph_id} DuckDB to {subgraph_id} LadybugDB")

  if ladybug_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Fork not allowed on read-only nodes",
    )

  # Mark this instance busy so GHA pre-refresh workflows don't cycle the
  # container mid-fork. Same rationale as materialize_table — fork is a
  # multi-table COPY across DuckDB → LadybugDB, equally destructive.
  async with instance_busy(env.INSTANCE_ID, OP_KIND_MATERIALIZATION):
    return await _fork_from_parent_duckdb_impl(
      parent_graph_id=parent_graph_id,
      subgraph_id=subgraph_id,
      request=request,
      ladybug_service=ladybug_service,
      start_time=start_time,
    )


async def _fork_from_parent_duckdb_impl(
  parent_graph_id: str,
  subgraph_id: str,
  request: ForkFromParentRequest,
  ladybug_service,
  start_time: float,
) -> ForkFromParentResponse:
  import time

  try:
    parent_duck_path = f"{env.DUCKDB_STAGING_PATH}/{parent_graph_id}.duckdb"

    # Checkpoint parent DuckDB to flush WAL and create views
    logger.info(f"Checkpointing parent DuckDB before fork: {parent_duck_path}")
    from robosystems.graph_api.core.duckdb import get_duckdb_pool

    duckdb_pool = get_duckdb_pool()

    # Get list of tables and create views (excluding file_id column)
    with duckdb_pool.get_connection(parent_graph_id) as duck_conn:
      checkpoint_with_retry(duck_conn, parent_graph_id, context="parent DuckDB")

      result = duck_conn.execute("SHOW TABLES").fetchall()
      available_tables = [row[0] for row in result]

    # Filter tables
    if request.tables:
      tables_to_copy = [t for t in available_tables if t in request.tables]
    else:
      tables_to_copy = available_tables

    # Sort tables to copy nodes before relationships
    # Relationship tables are typically all uppercase (e.g., ENTITY_HAS_TRANSACTION)
    # Node tables are typically PascalCase (e.g., Entity, Element, LineItem)
    node_tables = [t for t in tables_to_copy if not t.isupper()]
    rel_tables = [t for t in tables_to_copy if t.isupper()]
    tables_to_copy = node_tables + rel_tables

    logger.info(f"Found {len(tables_to_copy)} tables to fork: {tables_to_copy}")
    logger.info(f"  Node tables ({len(node_tables)}): {node_tables}")
    logger.info(f"  Relationship tables ({len(rel_tables)}): {rel_tables}")

    if not tables_to_copy:
      raise HTTPException(
        status_code=http_status.HTTP_400_BAD_REQUEST,
        detail="No tables to copy",
      )

    # Export each table (excluding file_id) to parquet, then COPY into the
    # subgraph. Parquet handoff replaces the previous DuckDB-ATTACH path —
    # LadybugDB 0.14+ creates persistent shadow catalog entries on ATTACH
    # that collide with installed schema.
    export_dir = f"{env.DUCKDB_STAGING_PATH}/materialize-exports"
    exports: list[tuple[str, str]] = []
    try:
      os.makedirs(export_dir, exist_ok=True)
      with duckdb_pool.get_connection(parent_graph_id) as duck_conn:
        for table_name in tables_to_copy:
          source_columns = duck_conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = ? ORDER BY ordinal_position",
            [table_name],
          ).fetchall()
          exclude_cols = (
            {"file_id"} if any(col[0] == "file_id" for col in source_columns) else set()
          )
          select_expr = _build_type_safe_select(
            [(col[0], col[1]) for col in source_columns],
            exclude_cols=exclude_cols,
          )
          # Bare-UUID filename — never interpolate the request-supplied
          # parent_graph_id / table_name into a filesystem path (path traversal).
          export_path = f"{export_dir}/{uuid4().hex}.parquet"
          duck_conn.execute(
            f"COPY (SELECT {select_expr} FROM {table_name}) "
            f"TO '{export_path}' (FORMAT parquet)"
          )
          exports.append((table_name, export_path))
        logger.info(f"Exported {len(exports)} parent DuckDB tables to parquet for fork")

      total_rows = 0
      tables_copied = []

      with ladybug_service.db_manager.connection_pool.get_connection(
        subgraph_id
      ) as conn:
        # Copy each table from its parquet export
        for table_name, export_path in exports:
          try:
            if request.ignore_errors:
              copy_query = (
                f"COPY {table_name} FROM '{export_path}' (ignore_errors=true)"
              )
            else:
              copy_query = f"COPY {table_name} FROM '{export_path}'"

            logger.info(f"Copying {table_name} from parent export to subgraph")
            # Set extended timeout for COPY operations (30 minutes)
            try:
              conn.execute("CALL timeout=3600000")  # 60 minutes
              result = conn.execute(copy_query)
            finally:
              # Always reset timeout to default after COPY
              conn.execute("CALL timeout=120000")  # 2 minutes

            rows_ingested = 0
            if result and hasattr(result, "get_as_arrow"):
              arrow_table = result.get_as_arrow()
              if arrow_table.num_rows > 0 and arrow_table.num_columns > 0:
                result_msg = str(arrow_table.column(0)[0].as_py())
                import re

                match = re.search(r"(\d+)\s+tuples?", result_msg)
                if match:
                  rows_ingested = int(match.group(1))

            total_rows += rows_ingested
            tables_copied.append(table_name)
            logger.info(f"[OK] Copied {table_name}: {rows_ingested} rows")

          except Exception as table_err:
            logger.error(f"Failed to copy {table_name}: {table_err}")
            if not request.ignore_errors:
              raise

      execution_time_ms = (time.time() - start_time) * 1000

      logger.info(
        f"Fork completed: {len(tables_copied)} tables, {total_rows:,} rows in {execution_time_ms / 1000:.1f}s"
      )

      return ForkFromParentResponse(
        status="success",
        parent_graph_id=parent_graph_id,
        subgraph_id=subgraph_id,
        tables_copied=tables_copied,
        total_rows=total_rows,
        execution_time_ms=execution_time_ms,
      )

    except HTTPException:
      raise
    except Exception as e:
      logger.error(f"Failed to fork from {parent_graph_id} to {subgraph_id}: {e}")
      raise HTTPException(
        status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Failed to fork data: {e!s}",
      )

    finally:
      # Clean up parquet exports
      for _, export_path in exports:
        try:
          os.remove(export_path)
        except OSError:
          pass  # best-effort cleanup; a leftover export is harmless staging cruft
      if exports:
        logger.info(f"Cleaned up {len(exports)} parquet exports")

  except Exception as outer_err:
    logger.error(f"Failed during fork preparation: {outer_err}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to fork data: {outer_err!s}",
    )
