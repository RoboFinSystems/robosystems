"""
Graph Database Ingestion Endpoint.

This is the primary data pipeline endpoint that orchestrates the complete
ingestion workflow from S3 staging files into the Kuzu graph database.

Key Features:
- Bulk ingestion of all tables in a single operation
- Automatic DuckDB staging table creation from S3
- Row-by-row graph database population
- Optional full database rebuild capability
- Comprehensive error handling and recovery
- Detailed per-table metrics and status

Workflow:
1. Upload data files via file upload endpoints
2. Files are validated and marked as 'uploaded'
3. Trigger ingestion (this endpoint)
4. DuckDB tables created from S3 patterns
5. Data copied from DuckDB to Kuzu graph
6. Per-table results and metrics returned

Rebuild Feature:
- Setting rebuild=true regenerates the entire graph from scratch
- Deletes existing Kuzu database
- Recreates with fresh schema
- Ingests all data files
- Safe operation - S3 is source of truth
- Useful for schema changes or data corrections

Error Handling:
- Per-table error isolation
- Configurable error tolerance (ignore_errors)
- Partial success support
- Detailed error reporting per table
- Graph status tracking throughout process

Performance:
- Processes all tables in sequence
- Each table timed independently
- Total execution metrics
- Scales to thousands of files
- Optimized for large datasets

Security:
- Write access verification required
- Blocked on shared repositories
- Rate limited per subscription tier
- Full audit logging of all operations
- Graph metadata state tracking
"""

from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Path, Body, status
from sqlalchemy.orm import Session

from robosystems.models.iam import (
  User,
  GraphTable,
  UserGraph,
  GraphFile,
  Graph,
  GraphSchema,
)
from robosystems.models.api.table import (
  BulkIngestRequest,
  BulkIngestResponse,
  TableIngestResult,
  FileUploadStatus,
)
from robosystems.models.api.common import ErrorResponse
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.middleware.graph import get_universal_repository
from robosystems.database import get_db_session
from robosystems.logger import logger, api_logger
from robosystems.middleware.graph.types import (
  GraphTypeRegistry,
  SHARED_REPO_WRITE_ERROR_MESSAGE,
)
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.middleware.robustness import CircuitBreakerManager
from robosystems.config import env
from robosystems.config.valkey_registry import (
  ValkeyDatabase,
  create_async_redis_client,
)
import time

router = APIRouter()

circuit_breaker = CircuitBreakerManager()


@router.post(
  "/tables/ingest",
  response_model=BulkIngestResponse,
  operation_id="ingestTables",
  summary="Ingest Tables to Graph",
  description="""Load all files from S3 into DuckDB staging tables and ingest into Kuzu graph database.

**Purpose:**
Orchestrates the complete data pipeline from S3 staging files into the Kuzu graph database.
Processes all tables in a single bulk operation with comprehensive error handling and metrics.

**Use Cases:**
- Initial graph population from uploaded data
- Incremental data updates with new files
- Complete database rebuild from source files
- Recovery from failed ingestion attempts

**Workflow:**
1. Upload data files via `POST /tables/{table_name}/files`
2. Files are validated and marked as 'uploaded'
3. Trigger ingestion: `POST /tables/ingest`
4. DuckDB staging tables created from S3 patterns
5. Data copied row-by-row from DuckDB to Kuzu
6. Per-table results and metrics returned

**Rebuild Feature:**
Setting `rebuild=true` regenerates the entire graph database from scratch:
- Deletes existing Kuzu database
- Recreates with fresh schema from active GraphSchema
- Ingests all data files
- Safe operation - S3 is source of truth
- Useful for schema changes or data corrections
- Graph marked as 'rebuilding' during process

**Error Handling:**
- Per-table error isolation with `ignore_errors` flag
- Partial success support (some tables succeed, some fail)
- Detailed error reporting per table
- Graph status tracking throughout process
- Automatic failure recovery and cleanup

**Performance:**
- Processes all tables in sequence
- Each table timed independently
- Total execution metrics provided
- Scales to thousands of files
- Optimized for large datasets

**Example Request:**
```bash
curl -X POST "https://api.robosystems.ai/v1/graphs/kg123/tables/ingest" \\
  -H "Authorization: Bearer YOUR_TOKEN" \\
  -H "Content-Type: application/json" \\
  -d '{
    "ignore_errors": true,
    "rebuild": false
  }'
```

**Example Response:**
```json
{
  "status": "success",
  "graph_id": "kg123",
  "total_tables": 5,
  "successful_tables": 5,
  "failed_tables": 0,
  "skipped_tables": 0,
  "total_rows_ingested": 25000,
  "total_execution_time_ms": 15420.5,
  "results": [
    {
      "table_name": "Entity",
      "status": "success",
      "rows_ingested": 5000,
      "execution_time_ms": 3200.1,
      "error": null
    }
  ]
}
```

**Concurrency Control:**
Only one ingestion can run per graph at a time. If another ingestion is in progress,
you'll receive a 409 Conflict error. The distributed lock automatically expires after
the configured TTL (default: 1 hour) to prevent deadlocks from failed ingestions.

**Tips:**
- Only files with 'uploaded' status are processed
- Tables with no uploaded files are skipped
- Use `ignore_errors=false` for strict validation
- Monitor progress via per-table results
- Check graph metadata for rebuild status
- Wait for current ingestion to complete before starting another

**Note:**
Table ingestion is included - no credit consumption.""",
  responses={
    200: {
      "description": "Ingestion completed with detailed per-table results",
      "content": {
        "application/json": {
          "example": {
            "status": "success",
            "graph_id": "kg123",
            "total_tables": 3,
            "successful_tables": 3,
            "failed_tables": 0,
            "skipped_tables": 0,
            "total_rows_ingested": 15000,
            "total_execution_time_ms": 8500.2,
            "results": [
              {
                "table_name": "Entity",
                "status": "success",
                "rows_ingested": 5000,
                "execution_time_ms": 3000.1,
                "error": None,
              }
            ],
          }
        }
      },
    },
    403: {
      "description": "Access denied - shared repositories or insufficient permissions",
      "model": ErrorResponse,
    },
    404: {"description": "Graph not found", "model": ErrorResponse},
    409: {
      "description": "Conflict - another ingestion is already in progress for this graph",
      "model": ErrorResponse,
    },
    500: {
      "description": "Ingestion failed - check per-table results for details",
      "model": ErrorResponse,
    },
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/tables/ingest", business_event_type="tables_ingested"
)
async def ingest_tables(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern="^[a-zA-Z][a-zA-Z0-9_]{2,62}$",
  ),
  request: BulkIngestRequest = Body(..., description="Ingestion request"),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> BulkIngestResponse:
  """
  Ingest all staging tables into the graph database.

  Orchestrates the complete data pipeline from S3 staging files through DuckDB
  to Kuzu graph database, with optional full database rebuild capability.
  """
  start_time_dt = datetime.now(timezone.utc)
  start_time = time.time()

  circuit_breaker.check_circuit(graph_id, "table_ingest")

  if graph_id.lower() in GraphTypeRegistry.SHARED_REPOSITORIES:
    logger.warning(
      f"User {current_user.id} attempted table ingestion on shared repository {graph_id}"
    )
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=SHARED_REPO_WRITE_ERROR_MESSAGE,
    )

  redis_client = create_async_redis_client(ValkeyDatabase.DISTRIBUTED_LOCKS)
  lock_key = f"ingestion_lock:{graph_id}"
  lock_ttl = env.INGESTION_LOCK_TTL
  lock_acquired = False

  try:
    lock_acquired = await redis_client.set(
      lock_key,
      f"{current_user.id}:{start_time_dt.isoformat()}",
      nx=True,
      ex=lock_ttl,
    )

    if not lock_acquired:
      lock_info = await redis_client.get(lock_key)
      lock_timestamp = "unknown"
      if lock_info and ":" in lock_info:
        try:
          lock_timestamp = lock_info.split(":", 1)[1]
        except (IndexError, ValueError):
          lock_timestamp = "unknown"

      api_logger.warning(
        "Ingestion already in progress",
        extra={
          "component": "tables_api",
          "action": "ingestion_blocked",
          "user_id": str(current_user.id),
          "graph_id": graph_id,
          "lock_holder": lock_info,
          "lock_timestamp": lock_timestamp,
          "metadata": {
            "endpoint": "/v1/graphs/{graph_id}/tables/ingest",
          },
        },
      )

      raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail=f"Another ingestion is already in progress for graph {graph_id}. "
        f"Lock acquired at: {lock_timestamp}. "
        f"Please wait for it to complete before starting a new one. "
        f"The lock will automatically expire after {lock_ttl} seconds if the ingestion fails.",
      )

    api_logger.info(
      "Ingestion lock acquired",
      extra={
        "component": "tables_api",
        "action": "lock_acquired",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "lock_ttl": lock_ttl,
      },
    )
    repo = await get_universal_repository(graph_id, "write")

    if not repo:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Graph {graph_id} not found",
      )

    api_logger.info(
      "Table ingestion started",
      extra={
        "component": "tables_api",
        "action": "ingest_started",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "rebuild": request.rebuild,
        "ignore_errors": request.ignore_errors,
        "metadata": {
          "endpoint": "/v1/graphs/{graph_id}/tables/ingest",
        },
      },
    )

    from robosystems.graph_api.client.factory import get_graph_client

    client = await get_graph_client(graph_id=graph_id, operation_type="write")

    if request.rebuild:
      logger.info(
        f"Rebuild requested for {graph_id} - regenerating entire Kuzu database from S3 source files"
      )

      api_logger.info(
        "Database rebuild initiated",
        extra={
          "component": "tables_api",
          "action": "rebuild_started",
          "user_id": str(current_user.id),
          "graph_id": graph_id,
        },
      )

      graph = Graph.get_by_id(graph_id, db)
      if not graph:
        raise HTTPException(
          status_code=status.HTTP_404_NOT_FOUND,
          detail=f"Graph {graph_id} not found",
        )

      graph_metadata = {**graph.graph_metadata} if graph.graph_metadata else {}
      graph_metadata["status"] = "rebuilding"
      graph_metadata["rebuild_started_at"] = time.time()
      graph.graph_metadata = graph_metadata
      db.commit()

      try:
        logger.info(f"Deleting Kuzu database for {graph_id}")
        await client.delete_database(graph_id)

        schema = GraphSchema.get_active_schema(graph_id, db)
        if not schema:
          raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No schema found for graph {graph_id}",
          )

        logger.info(f"Recreating Kuzu database with schema type: {schema.schema_type}")
        await client.create_database(
          graph_id=graph_id,
          schema_type=schema.schema_type,
          custom_schema_ddl=schema.schema_ddl,
        )

        api_logger.info(
          "Database rebuild completed",
          extra={
            "component": "tables_api",
            "action": "rebuild_completed",
            "user_id": str(current_user.id),
            "graph_id": graph_id,
            "schema_type": schema.schema_type,
          },
        )

        logger.info(f"Kuzu database recreated successfully for {graph_id}")

      except Exception as e:
        graph_metadata["status"] = "rebuild_failed"
        graph_metadata["rebuild_failed_at"] = time.time()
        graph_metadata["rebuild_error"] = str(e)
        graph.graph_metadata = graph_metadata
        db.commit()

        api_logger.error(
          "Database rebuild failed",
          extra={
            "component": "tables_api",
            "action": "rebuild_failed",
            "user_id": str(current_user.id),
            "graph_id": graph_id,
            "error_type": type(e).__name__,
            "error_message": str(e),
          },
        )

        logger.error(f"Failed to rebuild graph {graph_id}: {e}")
        raise HTTPException(
          status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
          detail=f"Failed to rebuild graph database: {str(e)}",
        )

    tables = GraphTable.get_all_for_graph(graph_id, db)
    results: list[TableIngestResult] = []
    total_rows_ingested = 0
    successful_tables = 0
    failed_tables = 0
    skipped_tables = 0

    user_graph = UserGraph.get_by_user_and_graph(current_user.id, graph_id, db)
    if not user_graph:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"User does not have access to graph {graph_id}",
      )

    bucket = env.AWS_S3_BUCKET

    api_logger.info(
      "Processing tables for ingestion",
      extra={
        "component": "tables_api",
        "action": "tables_processing",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "total_tables": len(tables),
      },
    )

    for table in tables:
      table_start = time.time()

      all_table_files = GraphFile.get_all_for_table(table.id, db)
      uploaded_files = [
        f for f in all_table_files if f.upload_status == FileUploadStatus.UPLOADED.value
      ]

      if not uploaded_files:
        logger.info(
          f"Skipping table {table.table_name} - no files with '{FileUploadStatus.UPLOADED.value}' status (found {len(all_table_files)} files total)"
        )
        results.append(
          TableIngestResult(
            table_name=table.table_name,
            status="skipped",
            rows_ingested=0,
            execution_time_ms=0,
            error="No files with 'uploaded' status",
          )
        )
        skipped_tables += 1
        continue

      try:
        s3_pattern = f"s3://{bucket}/user-staging/{current_user.id}/{graph_id}/{table.table_name}/**/*.parquet"

        logger.info(
          f"Creating/updating DuckDB staging table: {table.table_name} with pattern: {s3_pattern}"
        )

        await client.create_table(
          graph_id=graph_id, table_name=table.table_name, s3_pattern=s3_pattern
        )

        logger.info(f"Ingesting table {table.table_name} from DuckDB to Kuzu")

        response = await client.ingest_table_to_graph(
          graph_id=graph_id,
          table_name=table.table_name,
          ignore_errors=request.ignore_errors,
        )

        table_execution_ms = (time.time() - table_start) * 1000
        rows = response.get("rows_ingested", 0)
        total_rows_ingested += rows
        successful_tables += 1

        results.append(
          TableIngestResult(
            table_name=table.table_name,
            status="success",
            rows_ingested=rows,
            execution_time_ms=table_execution_ms,
            error=None,
          )
        )

        logger.info(
          f"Successfully ingested {table.table_name}: {rows} rows in {table_execution_ms:.2f}ms"
        )

      except Exception as e:
        table_execution_ms = (time.time() - table_start) * 1000
        failed_tables += 1
        error_msg = str(e)

        results.append(
          TableIngestResult(
            table_name=table.table_name,
            status="failed",
            rows_ingested=0,
            execution_time_ms=table_execution_ms,
            error=error_msg,
          )
        )

        logger.error(f"Failed to ingest table {table.table_name}: {error_msg}")

    total_execution_ms = (time.time() - start_time) * 1000
    execution_time = (datetime.now(timezone.utc) - start_time_dt).total_seconds() * 1000
    overall_status = (
      "success"
      if failed_tables == 0
      else "partial"
      if successful_tables > 0
      else "failed"
    )

    logger.info(
      f"Table ingestion completed for graph {graph_id}: "
      f"{successful_tables} successful, {failed_tables} failed, {skipped_tables} skipped, "
      f"{total_rows_ingested} total rows in {total_execution_ms:.2f}ms"
    )

    if request.rebuild:
      graph = Graph.get_by_id(graph_id, db)
      if graph:
        graph_metadata = {**graph.graph_metadata} if graph.graph_metadata else {}
        if overall_status in ["success", "partial"]:
          graph_metadata["status"] = "available"
          graph_metadata["rebuild_completed_at"] = time.time()
          if "rebuild_started_at" in graph_metadata:
            rebuild_duration = (
              graph_metadata["rebuild_completed_at"]
              - graph_metadata["rebuild_started_at"]
            )
            graph_metadata["last_rebuild_duration_seconds"] = rebuild_duration
          logger.info(f"Graph {graph_id} marked as available after rebuild")
        elif overall_status == "failed":
          graph_metadata["status"] = "rebuild_failed"
          graph_metadata["rebuild_failed_at"] = time.time()
          graph_metadata["rebuild_failure_reason"] = "All tables failed ingestion"
          logger.warning(
            f"Graph {graph_id} marked as rebuild_failed - all tables failed ingestion"
          )
        graph.graph_metadata = graph_metadata
        db.commit()

    circuit_breaker.record_success(graph_id, "table_ingest")

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/tables/ingest",
      method="POST",
      event_type=f"tables_ingested_{overall_status}",
      event_data={
        "graph_id": graph_id,
        "total_tables": len(tables),
        "successful_tables": successful_tables,
        "failed_tables": failed_tables,
        "skipped_tables": skipped_tables,
        "total_rows_ingested": total_rows_ingested,
        "execution_time_ms": execution_time,
        "rebuild": request.rebuild,
      },
      user_id=current_user.id,
    )

    api_logger.info(
      "Table ingestion completed",
      extra={
        "component": "tables_api",
        "action": "ingest_completed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "duration_ms": execution_time,
        "total_tables": len(tables),
        "successful_tables": successful_tables,
        "failed_tables": failed_tables,
        "skipped_tables": skipped_tables,
        "total_rows": total_rows_ingested,
        "status": overall_status,
        "success": overall_status == "success",
      },
    )

    return BulkIngestResponse(
      status=overall_status,
      graph_id=graph_id,
      total_tables=len(tables),
      successful_tables=successful_tables,
      failed_tables=failed_tables,
      skipped_tables=skipped_tables,
      total_rows_ingested=total_rows_ingested,
      total_execution_time_ms=total_execution_ms,
      results=results,
    )

  except HTTPException:
    circuit_breaker.record_failure(graph_id, "table_ingest")
    raise

  except Exception as e:
    circuit_breaker.record_failure(graph_id, "table_ingest")

    execution_time = (datetime.now(timezone.utc) - start_time_dt).total_seconds() * 1000

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/tables/ingest",
      method="POST",
      event_type="table_ingest_failed",
      event_data={
        "graph_id": graph_id,
        "error_type": type(e).__name__,
        "error_message": str(e),
        "execution_time_ms": execution_time,
      },
      user_id=current_user.id,
    )

    api_logger.error(
      "Table ingestion failed",
      extra={
        "component": "tables_api",
        "action": "ingest_failed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "duration_ms": execution_time,
        "error_type": type(e).__name__,
        "error_message": str(e),
      },
    )

    logger.error(
      f"Failed to ingest tables for graph {graph_id}: {e}",
      extra={
        "component": "tables_api",
        "action": "ingest_failed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "error_type": type(e).__name__,
      },
    )

    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to ingest tables: {str(e)}",
    )

  finally:
    if lock_acquired:
      try:
        await redis_client.delete(lock_key)
        api_logger.info(
          "Ingestion lock released",
          extra={
            "component": "tables_api",
            "action": "lock_released",
            "user_id": str(current_user.id),
            "graph_id": graph_id,
            "duration_ms": (datetime.now(timezone.utc) - start_time_dt).total_seconds()
            * 1000,
          },
        )
      except Exception as lock_error:
        logger.error(
          f"Failed to release ingestion lock for graph {graph_id}: {lock_error}",
          extra={
            "component": "tables_api",
            "action": "lock_release_failed",
            "user_id": str(current_user.id),
            "graph_id": graph_id,
            "error_type": type(lock_error).__name__,
          },
        )

    try:
      await redis_client.close()
    except Exception as close_error:
      logger.warning(f"Failed to close Redis client: {close_error}")
