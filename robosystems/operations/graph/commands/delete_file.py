"""Delete-file command — remove a file from S3, DuckDB (cascade), and PostgreSQL.

Extracted verbatim from the old ``DELETE /files/{id}`` router body; called by the
``delete-file`` content-op handler. Returns a ``DeleteFileResponse``.
"""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from robosystems.config import env
from robosystems.logger import api_logger, logger
from robosystems.middleware.graph import get_universal_repository
from robosystems.middleware.otel.metrics import get_endpoint_metrics
from robosystems.models.api.graphs.tables import DeleteFileResponse, FileUploadStatus
from robosystems.models.core import Graph, GraphFile, GraphTable, User
from robosystems.operations.aws.s3 import S3Client

__all__ = ["delete_file_cmd"]


async def delete_file_cmd(
  graph_id: str,
  file_id: str,
  cascade: bool,
  current_user: User,
  db: Session,
) -> DeleteFileResponse:
  """Delete a file across all layers (S3 + DuckDB cascade + PG)."""
  start_time = datetime.now(UTC)

  repository = await get_universal_repository(graph_id, "write")

  if not repository:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"Graph {graph_id} not found",
    )

  file = GraphFile.get_by_id(file_id, db)
  if not file or file.graph_id != graph_id:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"File {file_id} not found in graph {graph_id}",
    )

  file_name = file.file_name
  file_size = file.file_size_bytes
  s3_key = file.s3_key

  api_logger.info(
    "File deletion initiated",
    extra={
      "component": "files_api",
      "action": "delete_file_started",
      "user_id": str(current_user.id),
      "graph_id": graph_id,
      "file_id": file_id,
      "file_name": file_name,
      "file_size_bytes": file_size,
      "cascade": cascade,
    },
  )

  tables_affected = []
  graph_marked_stale = False

  if cascade:
    logger.info(
      f"Cascade deletion enabled for file {file_id} - deleting from DuckDB tables"
    )

    from robosystems.graph_api.client.factory import get_graph_client

    client = await get_graph_client(graph_id=graph_id, operation_type="write")
    all_tables = GraphTable.get_all_for_graph(graph_id, db)

    for table in all_tables:
      try:
        result = await client.delete_file_data(
          graph_id=graph_id, table_name=table.table_name, file_id=file_id
        )
        if result.get("rows_deleted", 0) > 0:
          tables_affected.append(table.table_name)
          logger.info(
            f"Deleted {result['rows_deleted']} rows from table {table.table_name}"
          )
      except Exception as e:
        logger.warning(f"Failed to delete file data from table {table.table_name}: {e}")

    if tables_affected:
      graph = Graph.get_by_id(graph_id, db)
      if graph:
        graph.mark_stale(
          session=db,
          reason=f"file_deleted: {file_name} from tables {', '.join(tables_affected)}",
        )
        graph_marked_stale = True
        logger.info(f"Marked graph {graph_id} as stale due to file deletion")

  s3_client = S3Client()
  bucket = env.USER_DATA_BUCKET

  s3_client.s3_client.delete_object(Bucket=bucket, Key=s3_key)
  logger.info(f"Deleted file from S3: {s3_key}")

  table = GraphTable.get_by_id(file.table_id, db)
  if table:
    new_file_count = max(0, (table.file_count or 1) - 1)
    new_total_size = max(0, (table.total_size_bytes or 0) - file_size)

    if file.row_count is not None:
      new_row_count = max(0, (table.row_count or 0) - file.row_count)
    else:
      all_files = GraphFile.get_all_for_table(table.id, db)
      remaining_files = [
        f
        for f in all_files
        if f.id != file.id and f.upload_status == FileUploadStatus.UPLOADED.value
      ]
      new_row_count = sum(
        f.row_count for f in remaining_files if f.row_count is not None
      )

    table.update_stats(
      session=db,
      file_count=new_file_count,
      total_size_bytes=new_total_size,
      row_count=new_row_count,
    )

  db.delete(file)
  db.commit()

  execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

  metrics_instance = get_endpoint_metrics()
  metrics_instance.record_business_event(
    endpoint="/v1/graphs/{graph_id}/files/{file_id}",
    method="DELETE",
    event_type="file_deleted_successfully",
    event_data={
      "graph_id": graph_id,
      "file_id": file_id,
      "file_name": file_name,
      "file_size_bytes": file_size,
      "table_name": table.table_name if table else None,
      "cascade": cascade,
      "tables_affected_count": len(tables_affected),
      "graph_marked_stale": graph_marked_stale,
      "execution_time_ms": execution_time,
    },
    user_id=current_user.id,
  )

  api_logger.info(
    "File deleted successfully",
    extra={
      "component": "files_api",
      "action": "delete_file_completed",
      "user_id": str(current_user.id),
      "graph_id": graph_id,
      "file_id": file_id,
      "file_name": file_name,
      "cascade": cascade,
      "tables_affected_count": len(tables_affected),
      "graph_marked_stale": graph_marked_stale,
      "duration_ms": execution_time,
      "success": True,
    },
  )

  logger.info(
    f"Deleted file {file_id} ({file_name}) from graph {graph_id} - "
    f"DuckDB will automatically exclude it from queries"
  )

  message = "File deleted successfully."
  if cascade and tables_affected:
    message += f" Removed data from {len(tables_affected)} DuckDB table(s)."
  if graph_marked_stale:
    message += " Graph marked as stale - rebuild recommended."
  elif not cascade:
    message += " DuckDB will automatically exclude it from queries."

  return DeleteFileResponse(
    status="deleted",
    file_id=file_id,
    file_name=file_name,
    message=message,
    cascade_deleted=cascade,
    tables_affected=tables_affected if cascade else None,
    graph_marked_stale=graph_marked_stale,
  )
