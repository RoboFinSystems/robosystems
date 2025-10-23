from fastapi import APIRouter, HTTPException, Depends, Body, Path
from fastapi import status as http_status
from pydantic import BaseModel, Field
import kuzu
from sqlalchemy.orm import Session

from robosystems.graph_api.core.cluster_manager import get_cluster_service
from robosystems.logger import logger
from robosystems.database import get_db_session
from robosystems.models.iam import Graph, GraphSchema

router = APIRouter(prefix="/databases/{graph_id}/tables")


class TableIngestRequest(BaseModel):
  table_name: str = Field(..., description="Table name to ingest from DuckDB")
  ignore_errors: bool = Field(
    default=True, description="Continue ingestion on row errors"
  )
  rebuild: bool = Field(
    default=False,
    description="Rebuild graph database from scratch before ingestion",
  )

  class Config:
    extra = "forbid"


class TableIngestResponse(BaseModel):
  status: str = Field(..., description="Ingestion status")
  graph_id: str = Field(..., description="Graph database identifier")
  table_name: str = Field(..., description="Table name")
  rows_ingested: int = Field(..., description="Number of rows ingested")
  execution_time_ms: float = Field(..., description="Ingestion time in milliseconds")


@router.post("/ingest", response_model=TableIngestResponse)
async def ingest_table_to_graph(
  graph_id: str = Path(..., description="Graph database identifier"),
  request: TableIngestRequest = Body(...),
  cluster_service=Depends(get_cluster_service),
  db: Session = Depends(get_db_session),
) -> TableIngestResponse:
  import time
  import shutil
  from pathlib import Path as PathlibPath

  start_time = time.time()

  rebuild_msg = " (REBUILD)" if request.rebuild else ""
  logger.info(
    f"Ingesting table {request.table_name} from DuckDB to Kuzu graph {graph_id}{rebuild_msg}"
  )

  if cluster_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Ingestion not allowed on read-only nodes",
    )

  try:
    # Handle rebuild: delete and recreate Kuzu database
    if request.rebuild:
      logger.warning(
        f"REBUILD requested for {graph_id} - deleting existing Kuzu database"
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

      # Close and remove all connections from pool
      cluster_service.db_manager.connection_pool.close_all_connections(graph_id)

      # Delete Kuzu database file
      db_path = PathlibPath(f"/app/data/kuzu-dbs/{graph_id}.kuzu")
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
      from robosystems.config import env

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

          duckdb_manager = DuckDBTableManager(base_path="/app/data/staging")
          bucket = env.AWS_S3_BUCKET_NAME

          for table_rec in tables_to_register:
            if table_rec.file_count and table_rec.file_count > 0:
              # Construct S3 pattern using actual user_id
              s3_pattern = f"s3://{bucket}/user-staging/{user_graph.user_id}/{graph_id}/{table_rec.table_name}/*.parquet"
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

    duck_path = f"/app/data/staging/{graph_id}.duckdb"

    conn = cluster_service.db_manager.connection_pool.get_connection(graph_id)

    try:
      try:
        conn.execute("INSTALL duckdb")
        conn.execute("LOAD duckdb")
      except Exception as e:
        logger.debug(f"DuckDB extension already installed/loaded: {e}")

      ignore_errors_clause = "IGNORE_ERRORS = true" if request.ignore_errors else ""
      quoted_table = f'"{request.table_name}"'

      copy_query = f"""
              COPY {quoted_table} FROM (
                  ATTACH '{duck_path}' AS duck (TYPE duckdb);
                  SELECT * FROM duck.{quoted_table}
              )
              {ignore_errors_clause}
          """

      logger.info(f"Executing copy from DuckDB to Kuzu: {request.table_name}")
      result = conn.execute(copy_query)

      rows_ingested = 0
      if result and hasattr(result, "get_as_arrow"):
        arrow_table = result.get_as_arrow()
        rows_ingested = arrow_table.num_rows
    finally:
      cluster_service.db_manager.connection_pool.return_connection(graph_id, conn)

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
      f"Ingested {rows_ingested} rows from {request.table_name} in {execution_time_ms:.2f}ms"
    )

    return TableIngestResponse(
      status="success",
      graph_id=graph_id,
      table_name=request.table_name,
      rows_ingested=rows_ingested,
      execution_time_ms=execution_time_ms,
    )

  except Exception as e:
    # Mark graph as available on failure if it was rebuilding
    if request.rebuild:
      try:
        graph = Graph.get_by_id(graph_id, db)
        if (
          graph
          and graph.graph_metadata
          and graph.graph_metadata.get("status") == "rebuilding"
        ):
          graph_metadata = {**graph.graph_metadata} if graph.graph_metadata else {}
          graph_metadata["status"] = "available"
          graph_metadata["rebuild_failed_at"] = time.time()
          graph_metadata["rebuild_error"] = str(e)
          graph.graph_metadata = graph_metadata
          db.commit()
          logger.warning(f"Graph {graph_id} marked as available after rebuild failure")
      except Exception as meta_error:
        logger.error(f"Failed to update graph metadata after error: {meta_error}")

    logger.error(f"Failed to ingest table {request.table_name}: {e}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to ingest table: {str(e)}",
    )
