from fastapi import APIRouter, HTTPException, Depends, Body, Path
from fastapi import status as http_status
from pydantic import BaseModel, Field
import kuzu
from sqlalchemy.orm import Session

from robosystems.graph_api.core.cluster_manager import get_cluster_service
from robosystems.logger import logger
from robosystems.database import get_db_session
from robosystems.models.iam import Graph, GraphSchema
from robosystems.utils.path_validation import get_kuzu_database_path
from robosystems.config import env

router = APIRouter(prefix="/databases/{graph_id}/tables")


class TableIngestRequest(BaseModel):
  ignore_errors: bool = Field(
    default=True, description="Continue ingestion on row errors"
  )
  rebuild: bool = Field(
    default=False,
    description="Rebuild graph database from scratch before ingestion. "
    "Safe operation - S3 is the source of truth, graph can always be regenerated.",
  )

  class Config:
    extra = "forbid"


class TableIngestResponse(BaseModel):
  status: str = Field(..., description="Ingestion status")
  graph_id: str = Field(..., description="Graph database identifier")
  table_name: str = Field(..., description="Table name")
  rows_ingested: int = Field(..., description="Number of rows ingested")
  execution_time_ms: float = Field(..., description="Ingestion time in milliseconds")


@router.post("/{table_name}/ingest", response_model=TableIngestResponse)
async def ingest_table_to_graph(
  graph_id: str = Path(..., description="Graph database identifier"),
  table_name: str = Path(..., description="Table name to ingest from DuckDB"),
  request: TableIngestRequest = Body(...),
  cluster_service=Depends(get_cluster_service),
  db: Session = Depends(get_db_session),
) -> TableIngestResponse:
  import time
  import shutil
  from pathlib import Path

  start_time = time.time()
  backup_key = None  # Track backup for error handling

  rebuild_msg = " (REBUILD)" if request.rebuild else ""
  logger.info(
    f"Ingesting table {table_name} from DuckDB to Kuzu graph {graph_id}{rebuild_msg}"
  )

  if cluster_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Ingestion not allowed on read-only nodes",
    )

  try:
    # Handle rebuild: delete and recreate Kuzu database
    if request.rebuild:
      logger.info(
        f"REBUILD requested for {graph_id} - regenerating Kuzu database from S3 source files"
      )

      # Mark graph as rebuilding
      graph = Graph.get_by_id(graph_id, db)
      if not graph:
        raise HTTPException(
          status_code=http_status.HTTP_404_NOT_FOUND,
          detail=f"Graph {graph_id} not found",
        )

      graph_metadata = {**graph.graph_metadata} if graph.graph_metadata else {}
      graph_metadata["status"] = "rebuilding"
      graph_metadata["rebuild_started_at"] = time.time()

      graph.graph_metadata = graph_metadata
      db.commit()

      db_path = get_kuzu_database_path(graph_id)

      # Close and remove all connections for this specific graph
      cluster_service.db_manager.connection_pool.force_database_cleanup(graph_id)

      # Delete Kuzu database file (using validated path)
      if db_path.exists():
        if db_path.is_dir():
          shutil.rmtree(db_path)
        else:
          db_path.unlink()
        logger.info(f"Deleted Kuzu database: {db_path}")

      # Recreate database with schema from graph_schemas table
      schema = GraphSchema.get_active_schema(graph_id, db)
      if not schema:
        raise HTTPException(
          status_code=http_status.HTTP_404_NOT_FOUND,
          detail=f"No schema found for graph {graph_id}",
        )

      logger.info(f"Recreating Kuzu database with schema type: {schema.schema_type}")

      # Create new database and apply schema
      new_db = kuzu.Database(str(db_path))
      temp_conn = kuzu.Connection(new_db)

      try:
        # Execute schema DDL to recreate table structure
        for ddl_statement in schema.schema_ddl.split(";"):
          ddl_statement = ddl_statement.strip()
          if ddl_statement:
            logger.debug(f"Executing schema DDL: {ddl_statement[:100]}...")
            temp_conn.execute(ddl_statement)
        logger.info(f"Schema recreated successfully for {graph_id}")
      finally:
        temp_conn.close()
        new_db.close()

      logger.info("Kuzu database recreated with schema - ready for ingestion")

      # After rebuild, re-register all DuckDB staging tables with the new Kuzu database
      # This ensures DuckDB tables are available for ingestion
      logger.info(f"Re-registering DuckDB staging tables after rebuild for {graph_id}")
      from robosystems.models.iam import GraphTable, UserGraph

      tables_to_register = GraphTable.get_all_for_graph(graph_id, db)

      if tables_to_register:
        # Get user_id from graph ownership
        user_graph = db.query(UserGraph).filter(UserGraph.graph_id == graph_id).first()
        if not user_graph:
          logger.warning(
            f"Could not find owner for graph {graph_id} - skipping DuckDB re-registration"
          )
        else:
          from robosystems.graph_api.core.duckdb_manager import (
            DuckDBTableManager,
            TableCreateRequest,
          )

          duckdb_manager = DuckDBTableManager()
          bucket = env.AWS_S3_BUCKET

          for table_rec in tables_to_register:
            if table_rec.file_count and table_rec.file_count > 0:
              # Construct S3 pattern using actual user_id
              s3_pattern = f"s3://{bucket}/user-staging/{user_graph.user_id}/{graph_id}/{table_rec.table_name}/**/*.parquet"
              try:
                create_req = TableCreateRequest(
                  graph_id=graph_id,
                  table_name=table_rec.table_name,
                  s3_pattern=s3_pattern,
                )
                duckdb_manager.create_table(create_req)
                logger.info(f"Re-registered DuckDB table: {table_rec.table_name}")
              except Exception as e:
                logger.warning(
                  f"Failed to re-register DuckDB table {table_rec.table_name}: {e}"
                )

      logger.info(
        f"Completed DuckDB table re-registration after rebuild for {graph_id}"
      )

    duck_path = f"{env.DUCKDB_STAGING_PATH}/{graph_id}.duckdb"
    duckdb_extension_path = (
      Path.home() / ".kuzu" / "extension" / "duckdb" / "libduckdb.kuzu_extension"
    )

    # CRITICAL: Checkpoint DuckDB to flush WAL to main database BEFORE Kuzu attaches
    # Kuzu's DuckDB extension creates a new session that won't see uncommitted WAL data
    logger.info(f"Checkpointing DuckDB database before Kuzu ingestion: {duck_path}")
    from robosystems.graph_api.core.duckdb_pool import get_duckdb_pool

    duckdb_pool = get_duckdb_pool()
    try:
      with duckdb_pool.get_connection(graph_id) as duck_conn:
        duck_conn.execute("CHECKPOINT")
        logger.info(f"✅ DuckDB checkpointed successfully for {graph_id}")
    except Exception as cp_err:
      logger.warning(f"Could not checkpoint DuckDB before ingestion: {cp_err}")

    with cluster_service.db_manager.connection_pool.get_connection(graph_id) as conn:
      try:
        conn.execute(f"LOAD EXTENSION '{duckdb_extension_path}'")
        logger.info(f"Loaded DuckDB extension from {duckdb_extension_path}")
      except Exception as e:
        if "already loaded" not in str(e).lower():
          logger.warning(f"Failed to load DuckDB extension: {e}")
          raise

      # Detach first if already attached (prevents duplicate attachment errors)
      try:
        conn.execute("DETACH duck")
      except Exception:
        pass

      # Attach DuckDB database (Kuzu syntax: ATTACH 'path' AS alias (DBTYPE duckdb))
      # Data is already materialized in DuckDB tables (not views), so no S3 access needed
      conn.execute(f"ATTACH '{duck_path}' AS duck (DBTYPE duckdb)")

      # IMPORTANT: Kuzu COPY command does NOT accept quoted table names anywhere
      # Both the target table and source reference must be unquoted
      # ignore_errors syntax: (ignore_errors=true) with parentheses, lowercase

      # NOTE: Partition columns (like 'year') are excluded when creating DuckDB tables
      # in duckdb_manager.py, so the schemas should match exactly now

      # Both node and relationship tables use the same COPY syntax when columns match
      # For relationships, Kuzu automatically maps 'src' and 'dst' columns to relationship endpoints
      if request.ignore_errors:
        copy_query = f"COPY {table_name} FROM duck.{table_name} (ignore_errors=true)"
      else:
        copy_query = f"COPY {table_name} FROM duck.{table_name}"

      logger.info(f"Executing copy from DuckDB to Kuzu: {table_name}")
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

    # Mark graph as available after successful rebuild
    if request.rebuild:
      graph = Graph.get_by_id(graph_id, db)
      if graph:
        graph_metadata = {**graph.graph_metadata} if graph.graph_metadata else {}
        graph_metadata["status"] = "available"
        graph_metadata["rebuild_completed_at"] = time.time()
        if "rebuild_started_at" in graph_metadata:
          rebuild_duration = (
            graph_metadata["rebuild_completed_at"]
            - graph_metadata["rebuild_started_at"]
          )
          graph_metadata["last_rebuild_duration_seconds"] = rebuild_duration
        graph.graph_metadata = graph_metadata
        db.commit()
        logger.info(f"Graph {graph_id} marked as available after rebuild")

    execution_time_ms = (time.time() - start_time) * 1000

    logger.info(
      f"Ingested {rows_ingested} rows from {table_name} in {execution_time_ms:.2f}ms"
    )

    return TableIngestResponse(
      status="success",
      graph_id=graph_id,
      table_name=table_name,
      rows_ingested=rows_ingested,
      execution_time_ms=execution_time_ms,
    )

  except Exception as e:
    # Mark graph as failed on rebuild failure and provide recovery information
    if request.rebuild:
      try:
        graph = Graph.get_by_id(graph_id, db)
        if (
          graph
          and graph.graph_metadata
          and graph.graph_metadata.get("status") == "rebuilding"
        ):
          graph_metadata = {**graph.graph_metadata} if graph.graph_metadata else {}
          graph_metadata["status"] = "rebuild_failed"
          graph_metadata["rebuild_failed_at"] = time.time()
          graph_metadata["rebuild_error"] = str(e)
          graph.graph_metadata = graph_metadata
          db.commit()

          backup_info = ""
          if graph_metadata.get("last_backup"):
            backup_info = f" Automatic backup available at: {graph_metadata['last_backup']}. Use the restore endpoint to recover."

          logger.error(
            f"Graph {graph_id} rebuild failed: {e}.{backup_info}",
            extra={
              "graph_id": graph_id,
              "backup_key": graph_metadata.get("last_backup"),
              "error": str(e),
            },
          )
      except Exception as meta_error:
        logger.error(f"Failed to update graph metadata after error: {meta_error}")

    logger.error(f"Failed to ingest table {table_name}: {e}")

    # Provide helpful error message with recovery options for rebuild failures
    error_detail = f"Failed to ingest table: {str(e)}"
    if request.rebuild and backup_key:
      error_detail += f" The database rebuild failed. Automatic backup is available at S3 key: {backup_key}. Use the restore endpoint to recover the previous state."

    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=error_detail,
    )
