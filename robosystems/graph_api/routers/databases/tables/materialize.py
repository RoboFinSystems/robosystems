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
    duck_path = f"{env.DUCKDB_STAGING_PATH}/{graph_id}.duckdb"

    # CRITICAL: Checkpoint DuckDB to flush WAL to main database BEFORE LadybugDB attaches
    # LadybugDB's DuckDB extension creates a new session that won't see uncommitted WAL data
    logger.info(
      f"Checkpointing DuckDB database before LadybugDB materialization: {duck_path}"
    )
    from robosystems.graph_api.core.duckdb import get_duckdb_pool

    duckdb_pool = get_duckdb_pool()
    temp_table_name = f"{table_name}_temp_materialization"

    # Check if table exists, create temp copy without file_id
    try:
      with duckdb_pool.get_connection(graph_id) as duck_conn:
        max_retries = 3
        for attempt in range(max_retries):
          try:
            duck_conn.execute("CHECKPOINT")
            logger.info(f"✅ DuckDB checkpointed successfully for {graph_id}")
            break
          except Exception as e:
            if attempt == max_retries - 1:
              logger.error(
                f"Failed to checkpoint DuckDB after {max_retries} attempts: {e}"
              )
              raise HTTPException(
                status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to checkpoint DuckDB after {max_retries} attempts: {e!s}",
              )
            logger.warning(
              f"Checkpoint attempt {attempt + 1} failed, retrying... Error: {e}"
            )
            import time

            time.sleep(1)

        # Check if table exists
        result = duck_conn.execute(
          f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
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
          f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
        ).fetchall()
        column_names = [col[0] for col in columns_result]
        has_file_id = "file_id" in column_names

        # Build ORDER BY + LIMIT/OFFSET clause for chunked materialization
        # ORDER BY is critical for deterministic pagination - without it, LIMIT/OFFSET
        # can return overlapping rows between batches, causing duplicate key errors
        limit_clause = ""
        if request.batch_size is not None:
          # Determine order column based on table type
          # Node tables use 'identifier', relationship tables use 'src, dst'
          if "identifier" in column_names:
            order_col = "identifier"
          elif "src" in column_names and "dst" in column_names:
            order_col = "src, dst"
          else:
            # Fallback to first column if neither standard column exists
            order_col = column_names[0] if column_names else "rowid"
          limit_clause = (
            f" ORDER BY {order_col} LIMIT {request.batch_size} OFFSET {request.offset}"
          )

        # Create physical copy of table without file_id column (if it exists)
        if request.file_ids:
          file_ids_str = ", ".join([f"'{fid}'" for fid in request.file_ids])
          if has_file_id:
            duck_conn.execute(
              f"CREATE TABLE {temp_table_name} AS "
              f"SELECT * EXCLUDE (file_id) FROM {table_name} "
              f"WHERE file_id IN ({file_ids_str}){limit_clause}"
            )
          else:
            duck_conn.execute(
              f"CREATE TABLE {temp_table_name} AS SELECT * FROM {table_name}{limit_clause}"
            )
          logger.info(
            f"Created temp DuckDB table {temp_table_name} with {len(request.file_ids)} file(s)"
          )
        else:
          if has_file_id:
            duck_conn.execute(
              f"CREATE TABLE {temp_table_name} AS "
              f"SELECT * EXCLUDE (file_id) FROM {table_name}{limit_clause}"
            )
          else:
            duck_conn.execute(
              f"CREATE TABLE {temp_table_name} AS SELECT * FROM {table_name}{limit_clause}"
            )
          if request.batch_size:
            logger.info(
              f"Created temp DuckDB table {temp_table_name} for chunked materialization "
              f"(batch={request.batch_size}, offset={request.offset})"
            )
          else:
            logger.info(
              f"Created temp DuckDB table {temp_table_name} for full materialization"
            )

    except Exception as err:
      logger.error(f"Could not prepare DuckDB table for materialization: {err}")
      raise

    temp_table_created = True

    try:
      with ladybug_service.db_manager.connection_pool.get_connection(graph_id) as conn:
        # Install and load DuckDB extension - LadybugDB finds it at
        # ~/.lbug/extension/{VERSION}/{PLATFORM}/duckdb/ (bundled in Docker image)
        try:
          conn.execute("INSTALL duckdb")
          conn.execute("LOAD duckdb")
          logger.info("Loaded DuckDB extension")
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
        logger.info(f"Attached DuckDB database: {duck_path}")

        # Copy from the temp table (which already has file_id excluded)
        if request.file_ids:
          logger.info(
            f"Executing selective materialization from DuckDB to graph: {table_name} "
            f"({len(request.file_ids)} file(s))"
          )
        else:
          logger.info(
            f"Executing full materialization from DuckDB to graph: {table_name}"
          )

        if request.ignore_errors:
          copy_query = (
            f"COPY {table_name} FROM duck.{temp_table_name} (ignore_errors=true)"
          )
        else:
          copy_query = f"COPY {table_name} FROM duck.{temp_table_name}"

        # Set extended timeout for COPY operations (30 minutes)
        # Default connection timeout is 120s, but large tables like Fact
        # can take 2-3 minutes with millions of rows
        try:
          conn.execute("CALL timeout=1800000")  # 30 minutes
          logger.info(f"Executing: {copy_query}")
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
        f"Materialized {rows_ingested} rows from {table_name} in {execution_time_ms:.2f}ms"
      )

      # Checkpoint and release LadybugDB memory after each table
      # This prevents memory accumulation during multi-table materialization
      try:
        # Checkpoint to flush data to disk
        with ladybug_service.db_manager.connection_pool.get_connection(
          graph_id
        ) as conn:
          conn.execute("CHECKPOINT")
          logger.debug(f"Checkpointed LadybugDB after {table_name} materialization")

        # Release buffer pool memory - data is safe on disk now
        ladybug_service.db_manager.connection_pool.force_database_cleanup(
          graph_id, aggressive=True
        )
        logger.info(f"Released LadybugDB memory after {table_name} materialization")
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
          with duckdb_pool.get_connection(graph_id) as duck_conn:
            duck_conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            logger.info(f"Cleaned up temp DuckDB table: {temp_table_name}")
        except Exception as drop_err:
          logger.warning(f"Failed to drop temp table {temp_table_name}: {drop_err}")

        # Checkpoint and release connections (always attempt even if drop failed)
        try:
          with duckdb_pool.get_connection(graph_id) as duck_conn:
            duck_conn.execute("CHECKPOINT")
          duckdb_pool.close_database_connections(graph_id)
          logger.debug(f"Released DuckDB connections for {graph_id}")
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
      max_retries = 3
      for attempt in range(max_retries):
        try:
          duck_conn.execute("CHECKPOINT")
          logger.info(f"✅ Parent DuckDB checkpointed for {parent_graph_id}")
          break
        except Exception as e:
          if attempt == max_retries - 1:
            logger.error(
              f"Failed to checkpoint parent DuckDB after {max_retries} attempts: {e}"
            )
            raise HTTPException(
              status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
              detail=f"Failed to checkpoint parent DuckDB after {max_retries} attempts: {e!s}",
            )
          logger.warning(
            f"Checkpoint attempt {attempt + 1} failed, retrying... Error: {e}"
          )
          import time

          time.sleep(1)

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
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
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
          logger.info("Loaded DuckDB extension")
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
              conn.execute("CALL timeout=1800000")  # 30 minutes
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
            logger.info(f"✅ Copied {table_name}: {rows_ingested} rows")

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
        f"Fork completed: {len(tables_copied)} tables, {total_rows:,} rows in {execution_time_ms:.2f}ms"
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
