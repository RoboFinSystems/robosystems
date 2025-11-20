from fastapi import APIRouter, HTTPException, Depends, Body, Path
from fastapi import status as http_status
from pydantic import BaseModel, Field

from robosystems.graph_api.core.cluster_manager import get_cluster_service
from robosystems.logger import logger
from robosystems.config import env

router = APIRouter(prefix="/databases/{graph_id}/tables")


class TableMaterializationRequest(BaseModel):
  ignore_errors: bool = Field(
    default=True, description="Continue materialization on row errors"
  )
  file_ids: list[str] | None = Field(
    default=None,
    description="Optional list of file IDs to materialize. If None, materializes all files (full materialization).",
  )

  class Config:
    extra = "forbid"


class TableMaterializationResponse(BaseModel):
  status: str = Field(..., description="Materialization status")
  graph_id: str = Field(..., description="Graph database identifier")
  table_name: str = Field(..., description="Table name")
  rows_ingested: int = Field(..., description="Number of rows materialized")
  execution_time_ms: float = Field(
    ..., description="Materialization time in milliseconds"
  )


@router.post("/{table_name}/materialize", response_model=TableMaterializationResponse)
async def materialize_table(
  graph_id: str = Path(..., description="Graph database identifier"),
  table_name: str = Path(..., description="Table name to materialize from DuckDB"),
  request: TableMaterializationRequest = Body(...),
  cluster_service=Depends(get_cluster_service),
) -> TableMaterializationResponse:
  import time
  from pathlib import Path as PathLib

  start_time = time.time()

  logger.info(f"Materializing table {table_name} from DuckDB to Kuzu graph {graph_id}")

  if cluster_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Materialization not allowed on read-only nodes",
    )

  try:
    duck_path = f"{env.DUCKDB_STAGING_PATH}/{graph_id}.duckdb"
    duckdb_extension_path = (
      PathLib.home() / ".kuzu" / "extension" / "duckdb" / "libduckdb.kuzu_extension"
    )

    # CRITICAL: Checkpoint DuckDB to flush WAL to main database BEFORE Kuzu attaches
    # Kuzu's DuckDB extension creates a new session that won't see uncommitted WAL data
    logger.info(
      f"Checkpointing DuckDB database before Kuzu materialization: {duck_path}"
    )
    from robosystems.graph_api.core.duckdb_pool import get_duckdb_pool

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
                detail=f"Failed to checkpoint DuckDB after {max_retries} attempts: {str(e)}",
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

        # Create physical copy of table without file_id column
        if request.file_ids:
          file_ids_str = ", ".join([f"'{fid}'" for fid in request.file_ids])
          duck_conn.execute(
            f"CREATE TABLE {temp_table_name} AS "
            f"SELECT * EXCLUDE (file_id) FROM {table_name} "
            f"WHERE file_id IN ({file_ids_str})"
          )
          logger.info(
            f"Created temp DuckDB table {temp_table_name} with {len(request.file_ids)} file(s)"
          )
        else:
          duck_conn.execute(
            f"CREATE TABLE {temp_table_name} AS "
            f"SELECT * EXCLUDE (file_id) FROM {table_name}"
          )
          logger.info(
            f"Created temp DuckDB table {temp_table_name} for full materialization"
          )

    except Exception as err:
      logger.error(f"Could not prepare DuckDB table for materialization: {err}")
      raise

    temp_table_created = True

    try:
      with cluster_service.db_manager.connection_pool.get_connection(graph_id) as conn:
        try:
          conn.execute(f"LOAD EXTENSION '{duckdb_extension_path}'")
          logger.info(f"Loaded DuckDB extension from {duckdb_extension_path}")
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

        logger.info(f"Executing: {copy_query}")
        result = conn.execute(copy_query)

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
        detail=f"Failed to materialize table: {str(e)}",
      )

    finally:
      # Clean up temp table
      if temp_table_created:
        try:
          with duckdb_pool.get_connection(graph_id) as duck_conn:
            duck_conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            logger.info(f"Cleaned up temp DuckDB table: {temp_table_name}")
        except Exception as cleanup_err:
          logger.warning(
            f"Failed to clean up temp table {temp_table_name}: {cleanup_err}"
          )

  except Exception as outer_err:
    logger.error(f"Failed during table preparation or materialization: {outer_err}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to materialize table: {str(outer_err)}",
    )


class ForkFromParentRequest(BaseModel):
  tables: list[str] = Field(
    default_factory=list,
    description="List of table names to copy from parent, or empty for all tables",
  )
  ignore_errors: bool = Field(
    default=True, description="Continue materialization on row errors"
  )

  class Config:
    extra = "forbid"


class ForkFromParentResponse(BaseModel):
  status: str = Field(..., description="Fork operation status")
  parent_graph_id: str = Field(..., description="Parent graph identifier")
  subgraph_id: str = Field(..., description="Subgraph identifier")
  tables_copied: list[str] = Field(..., description="Tables successfully copied")
  total_rows: int = Field(..., description="Total rows copied")
  execution_time_ms: float = Field(..., description="Total fork time in milliseconds")


@router.post(
  "/{subgraph_id}/fork-from/{parent_graph_id}", response_model=ForkFromParentResponse
)
async def fork_from_parent_duckdb(
  parent_graph_id: str = Path(..., description="Parent graph database identifier"),
  subgraph_id: str = Path(..., description="Subgraph database identifier"),
  request: ForkFromParentRequest = Body(...),
  cluster_service=Depends(get_cluster_service),
) -> ForkFromParentResponse:
  """
  Fork data from parent graph's DuckDB directly into subgraph's Kuzu.

  This endpoint:
  1. Attaches parent graph's DuckDB staging database
  2. Copies specified tables (or all tables) from parent DuckDB to subgraph Kuzu
  3. Runs on the same EC2 instance where both DuckDB and Kuzu databases live

  Args:
      parent_graph_id: Parent graph to copy data from
      subgraph_id: Subgraph to copy data to
      request: Fork options (tables to copy, error handling)

  Returns:
      ForkFromParentResponse with tables copied and row counts
  """
  import time
  from pathlib import Path as PathLib

  start_time = time.time()

  logger.info(f"Forking data from {parent_graph_id} DuckDB to {subgraph_id} Kuzu")

  if cluster_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Fork not allowed on read-only nodes",
    )

  try:
    parent_duck_path = f"{env.DUCKDB_STAGING_PATH}/{parent_graph_id}.duckdb"
    duckdb_extension_path = (
      PathLib.home() / ".kuzu" / "extension" / "duckdb" / "libduckdb.kuzu_extension"
    )

    # Checkpoint parent DuckDB to flush WAL and create views
    logger.info(f"Checkpointing parent DuckDB before fork: {parent_duck_path}")
    from robosystems.graph_api.core.duckdb_pool import get_duckdb_pool

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
              detail=f"Failed to checkpoint parent DuckDB after {max_retries} attempts: {str(e)}",
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

    # Create temp physical tables in parent DuckDB for each table (excluding file_id)
    temp_tables = []
    try:
      with duckdb_pool.get_connection(parent_graph_id) as duck_conn:
        for table_name in tables_to_copy:
          temp_table = f"{table_name}_temp_fork"
          temp_tables.append(temp_table)

          # Drop if exists from previous failed run
          duck_conn.execute(f"DROP TABLE IF EXISTS {temp_table}")

          # Create physical temp table without file_id
          duck_conn.execute(
            f"CREATE TABLE {temp_table} AS SELECT * EXCLUDE (file_id) FROM {table_name}"
          )
        logger.info(f"Created {len(temp_tables)} temp tables in parent DuckDB for fork")

      # Connect to subgraph Kuzu and attach parent DuckDB
      total_rows = 0
      tables_copied = []

      with cluster_service.db_manager.connection_pool.get_connection(
        subgraph_id
      ) as conn:
        try:
          conn.execute(f"LOAD EXTENSION '{duckdb_extension_path}'")
          logger.info(f"Loaded DuckDB extension from {duckdb_extension_path}")
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
            result = conn.execute(copy_query)

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
        detail=f"Failed to fork data: {str(e)}",
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
      detail=f"Failed to fork data: {str(outer_err)}",
    )
