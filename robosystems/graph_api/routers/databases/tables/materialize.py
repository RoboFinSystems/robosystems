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


_RECONCILE_IGNORE_COLS = frozenset({"file_id", "from", "to", "src", "dst"})


def _needs_reconciliation(
  target_columns: list[tuple[str, str]] | None,
  source_column_names: list[str],
) -> bool:
  """Return True when source columns don't match target columns by name AND order.

  LadybugDB COPY is positional, so a source table whose column names match the
  target's but in a different order will silently misalign data into the wrong
  columns. This commonly happens when a new property is added in the middle of
  an existing node schema and the DuckDB staging table evolves via
  `ALTER TABLE ADD COLUMN` (which appends at the end). A pure set comparison
  misses this; we compare ordered lists.

  `from`/`to`/`src`/`dst`/`file_id` are excluded — they're implicit/synthetic.
  """
  if not target_columns:
    return False
  target_order = [c[0] for c in target_columns if c[0] not in _RECONCILE_IGNORE_COLS]
  source_order = [c for c in source_column_names if c not in _RECONCILE_IGNORE_COLS]
  return target_order != source_order


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

  try:
    # Blue-green: read DuckDB from source graph if specified, otherwise from target
    duckdb_graph_id = request.source_graph_id or graph_id
    duck_path = f"{env.DUCKDB_STAGING_PATH}/{duckdb_graph_id}.duckdb"

    # CRITICAL: Checkpoint DuckDB to flush WAL to main database BEFORE LadybugDB attaches
    # LadybugDB's DuckDB extension creates a new session that won't see uncommitted WAL data
    logger.debug(
      f"Checkpointing DuckDB database before LadybugDB materialization: {duck_path}"
    )
    from robosystems.graph_api.core.duckdb import get_duckdb_pool

    duckdb_pool = get_duckdb_pool()
    temp_table_name = f"{table_name}_temp_materialization"

    # Get target table schema from LadybugDB for column reconciliation
    # This handles schema evolution: parquet files missing new columns or
    # having mistyped NULL columns (e.g., DuckDB infers all-NULL as INT32
    # but target expects FLOAT[384])
    target_columns = _get_target_columns(ladybug_service, graph_id, table_name)

    # Check if table exists, create temp copy without file_id
    temp_table_created = False
    try:
      with duckdb_pool.get_connection(duckdb_graph_id) as duck_conn:
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

        # Drop temp table if it exists from previous failed run
        try:
          duck_conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
        except Exception:
          pass

        # Check if file_id column exists in the table
        columns_result = duck_conn.execute(
          "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
          [table_name],
        ).fetchall()
        column_names = [col[0] for col in columns_result]
        has_file_id = "file_id" in column_names

        needs_reconciliation = _needs_reconciliation(target_columns, column_names)

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

        # Optimization: Skip temp table when not needed (no file_id, no batching, no filter, no reconciliation, no null cols)
        has_hash_batching = (
          request.num_batches is not None and request.batch_num is not None
        )
        can_skip_temp_table = (
          not has_file_id
          and not has_hash_batching
          and not request.file_ids
          and not needs_reconciliation
          and not null_cols
        )

        if can_skip_temp_table:
          # Fast path: COPY directly from source table
          logger.info(f"Using direct COPY for {table_name} (no temp table needed)")
          temp_table_created = False
        else:
          # Build WHERE clause for batched materialization
          batch_clause = ""

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

          # Build SELECT expression
          # Force reconciliation when nulling columns — the reconciled path
          # iterates target columns (not source), so column count always matches.
          use_reconciliation = (needs_reconciliation or null_cols) and target_columns
          if use_reconciliation:
            select_expr = _build_reconciled_select(
              target_columns,
              column_names,
              table_name,
              exclude_cols=exclude_cols,
              null_cols=null_cols,
            )
            logger.debug(
              f"Reconciling columns for {table_name} "
              f"(source: {len(column_names)}, target: {len(target_columns)})"
            )
          elif exclude_cols:
            exclude_list = ", ".join(exclude_cols)
            select_expr = f"* EXCLUDE ({exclude_list})"
          else:
            select_expr = "*"

          create_sql = (
            f"CREATE TABLE {temp_table_name} AS "
            f"SELECT {select_expr} FROM {table_name}{where}"
          )
          if query_params:
            duck_conn.execute(create_sql, query_params)
          else:
            duck_conn.execute(create_sql)

          if request.file_ids:
            logger.info(
              f"Created temp DuckDB table {temp_table_name} with {len(request.file_ids)} file(s)"
            )
          elif has_hash_batching:
            assert request.batch_num is not None and request.num_batches is not None
            logger.info(
              f"Created temp DuckDB table {temp_table_name} for hash-based materialization "
              f"(batch {request.batch_num + 1}/{request.num_batches})"
            )
          else:
            logger.debug(
              f"Created temp DuckDB table {temp_table_name} for full materialization"
            )
          temp_table_created = True

    except Exception as err:
      logger.error(f"Could not prepare DuckDB table for materialization: {err}")
      raise

    try:
      with ladybug_service.db_manager.connection_pool.get_connection(graph_id) as conn:
        # Install and load DuckDB extension - LadybugDB finds it at
        # ~/.lbug/extension/{VERSION}/{PLATFORM}/duckdb/ (bundled in Docker image)
        try:
          conn.execute("INSTALL duckdb")
          conn.execute("LOAD duckdb")
          logger.debug("Loaded DuckDB extension")
        except Exception as e:
          if "already loaded" not in str(e).lower():
            logger.warning(f"Failed to load DuckDB extension: {e}")
            raise

        # Detach first if already attached
        try:
          conn.execute("DETACH duck")
        except Exception:
          pass

        # Attach DuckDB database
        conn.execute(f"ATTACH '{duck_path}' AS duck (DBTYPE duckdb)")
        logger.debug(f"Attached DuckDB database: {duck_path}")

        # Determine source table for COPY (temp table or direct source)
        source_table = table_name if not temp_table_created else temp_table_name

        if request.file_ids:
          logger.info(
            f"COPY {table_name} → {graph_id} ({len(request.file_ids)} file(s))"
          )
        else:
          logger.info(f"COPY {table_name} → {graph_id}")

        if request.ignore_errors:
          copy_query = (
            f"COPY {table_name} FROM duck.{source_table} (ignore_errors=true)"
          )
        else:
          copy_query = f"COPY {table_name} FROM duck.{source_table}"

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
      # Clean up temp table and release DuckDB memory
      if temp_table_created:
        # Drop temp table
        try:
          with duckdb_pool.get_connection(duckdb_graph_id) as duck_conn:
            duck_conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            logger.debug(f"Cleaned up temp DuckDB table: {temp_table_name}")
        except Exception as drop_err:
          logger.warning(f"Failed to drop temp table {temp_table_name}: {drop_err}")

        # Checkpoint and release connections (always attempt even if drop failed)
        try:
          with duckdb_pool.get_connection(duckdb_graph_id) as duck_conn:
            duck_conn.execute("CHECKPOINT")
          duckdb_pool.close_database_connections(duckdb_graph_id)
          logger.debug(f"Released DuckDB connections for {duckdb_graph_id}")
        except Exception as release_err:
          logger.warning(f"Failed to release DuckDB connections: {release_err}")

  except Exception as outer_err:
    logger.error(f"Failed during table preparation or materialization: {outer_err}")
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

    # Create temp physical tables in parent DuckDB for each table (excluding file_id if exists)
    temp_tables = []
    try:
      with duckdb_pool.get_connection(parent_graph_id) as duck_conn:
        for table_name in tables_to_copy:
          temp_table = f"{table_name}_temp_fork"
          temp_tables.append(temp_table)

          # Drop if exists from previous failed run
          duck_conn.execute(f"DROP TABLE IF EXISTS {temp_table}")

          # Check if file_id column exists in the table
          columns_result = duck_conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            [table_name],
          ).fetchall()
          column_names = [col[0] for col in columns_result]
          has_file_id = "file_id" in column_names

          # Create physical temp table without file_id (if it exists)
          if has_file_id:
            duck_conn.execute(
              f"CREATE TABLE {temp_table} AS SELECT * EXCLUDE (file_id) FROM {table_name}"
            )
          else:
            duck_conn.execute(
              f"CREATE TABLE {temp_table} AS SELECT * FROM {table_name}"
            )
        logger.info(f"Created {len(temp_tables)} temp tables in parent DuckDB for fork")

      # Connect to subgraph LadybugDB and attach parent DuckDB
      total_rows = 0
      tables_copied = []

      with ladybug_service.db_manager.connection_pool.get_connection(
        subgraph_id
      ) as conn:
        # Install and load DuckDB extension - LadybugDB finds it at
        # ~/.lbug/extension/{VERSION}/{PLATFORM}/duckdb/ (bundled in Docker image)
        try:
          conn.execute("INSTALL duckdb")
          conn.execute("LOAD duckdb")
          logger.debug("Loaded DuckDB extension")
        except Exception as e:
          if "already loaded" not in str(e).lower():
            logger.warning(f"Failed to load DuckDB extension: {e}")
            raise

        # Detach first if already attached
        try:
          conn.execute("DETACH parent_duck")
        except Exception:
          pass

        # Attach parent DuckDB as 'parent_duck'
        conn.execute(f"ATTACH '{parent_duck_path}' AS parent_duck (DBTYPE duckdb)")
        logger.info(f"Attached parent DuckDB: {parent_duck_path}")

        # Copy each table from the temp tables we created in parent DuckDB
        for idx, table_name in enumerate(tables_to_copy):
          try:
            temp_table = temp_tables[idx]

            if request.ignore_errors:
              copy_query = (
                f"COPY {table_name} FROM parent_duck.{temp_table} (ignore_errors=true)"
              )
            else:
              copy_query = f"COPY {table_name} FROM parent_duck.{temp_table}"

            logger.info(f"Copying {table_name} from parent to subgraph")
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

      # Detach parent DuckDB
      try:
        conn.execute("DETACH parent_duck")
      except Exception:
        pass

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
      # Clean up temp tables
      if temp_tables:
        try:
          with duckdb_pool.get_connection(parent_graph_id) as duck_conn:
            for temp_table in temp_tables:
              duck_conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
            logger.info(f"Cleaned up {len(temp_tables)} temp tables from parent DuckDB")
        except Exception as cleanup_err:
          logger.warning(f"Failed to clean up temp tables: {cleanup_err}")

  except Exception as outer_err:
    logger.error(f"Failed during fork preparation: {outer_err}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to fork data: {outer_err!s}",
    )
