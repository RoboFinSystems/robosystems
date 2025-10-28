from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from robosystems.models.iam import User, GraphTable, GraphFile
from robosystems.models.api.common import ErrorResponse
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.middleware.graph.dependencies import get_universal_repository_with_auth
from robosystems.database import get_db_session
from robosystems.adapters.s3 import S3Client
from robosystems.config import env
from robosystems.logger import logger
from robosystems.middleware.graph.types import (
  GraphTypeRegistry,
  SHARED_REPO_DELETE_ERROR_MESSAGE,
)

router = APIRouter()


@router.get(
  "/tables/{table_name}/files",
  response_model=dict,
  summary="List Files in Table",
  description="List all files uploaded to a staging table",
  responses={
    200: {"description": "Files retrieved successfully"},
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    404: {"description": "Graph or table not found", "model": ErrorResponse},
  },
)
async def list_table_files(
  graph_id: str = Path(..., description="Graph database identifier"),
  table_name: str = Path(..., description="Table name"),
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> dict:
  repository = await get_universal_repository_with_auth(
    graph_id, current_user, "read", db
  )

  if not repository:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"Graph {graph_id} not found",
    )

  table = GraphTable.get_by_name(graph_id, table_name, db)
  if not table:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"Table {table_name} not found in graph {graph_id}",
    )

  logger.info(f"Listing files for table {table_name} in graph {graph_id}")

  try:
    files = GraphFile.get_all_for_table(table.id, db)

    return {
      "graph_id": graph_id,
      "table_name": table_name,
      "files": [
        {
          "file_id": f.id,
          "file_name": f.file_name,
          "file_format": f.file_format,
          "size_bytes": f.file_size_bytes,
          "row_count": f.row_count,
          "upload_status": f.upload_status,
          "upload_method": f.upload_method,
          "created_at": f.created_at.isoformat() if f.created_at else None,
          "uploaded_at": f.uploaded_at.isoformat() if f.uploaded_at else None,
          "s3_key": f.s3_key,
        }
        for f in files
      ],
      "total_files": len(files),
      "total_size_bytes": sum(f.file_size_bytes for f in files),
    }

  except Exception as e:
    logger.error(f"Failed to list files for table {table_name}: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to list files: {str(e)}",
    )


@router.get(
  "/tables/files/{file_id}",
  summary="Get File Info",
  description="Get detailed information about a specific file",
  responses={
    200: {"description": "File info retrieved successfully"},
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    404: {"description": "File not found", "model": ErrorResponse},
  },
)
async def get_file_info(
  graph_id: str = Path(..., description="Graph database identifier"),
  file_id: str = Path(..., description="File ID"),
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> dict:
  repository = await get_universal_repository_with_auth(
    graph_id, current_user, "read", db
  )

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

  table = GraphTable.get_by_id(file.table_id, db)

  return {
    "file_id": file.id,
    "graph_id": file.graph_id,
    "table_id": file.table_id,
    "table_name": table.table_name if table else None,
    "file_name": file.file_name,
    "file_format": file.file_format,
    "size_bytes": file.file_size_bytes,
    "row_count": file.row_count,
    "upload_status": file.upload_status,
    "upload_method": file.upload_method,
    "created_at": file.created_at.isoformat() if file.created_at else None,
    "uploaded_at": file.uploaded_at.isoformat() if file.uploaded_at else None,
    "s3_key": file.s3_key,
  }


@router.delete(
  "/tables/files/{file_id}",
  summary="Delete File",
  description="Delete a specific file from S3 and database tracking. DuckDB will automatically exclude it from queries.",
  responses={
    200: {"description": "File deleted successfully"},
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    404: {"description": "File not found", "model": ErrorResponse},
  },
)
async def delete_file(
  graph_id: str = Path(..., description="Graph database identifier"),
  file_id: str = Path(..., description="File ID"),
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> dict:
  if graph_id.lower() in GraphTypeRegistry.SHARED_REPOSITORIES:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=SHARED_REPO_DELETE_ERROR_MESSAGE,
    )

  repository = await get_universal_repository_with_auth(
    graph_id, current_user, "read", db
  )

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

  logger.info(f"Deleting file {file_id} ({file.file_name}) from graph {graph_id}")

  s3_client = S3Client()

  try:
    # Delete from S3
    bucket = env.AWS_S3_BUCKET
    s3_client.s3_client.delete_object(Bucket=bucket, Key=file.s3_key)
    logger.info(f"Deleted file from S3: {file.s3_key}")

    # Update table stats
    table = GraphTable.get_by_id(file.table_id, db)
    if table:
      new_file_count = max(0, (table.file_count or 1) - 1)
      new_total_size = max(0, (table.total_size_bytes or 0) - file.file_size_bytes)

      if file.row_count is not None:
        new_row_count = max(0, (table.row_count or 0) - file.row_count)
      else:
        all_files = GraphFile.get_all_for_table(table.id, db)
        remaining_files = [
          f for f in all_files if f.id != file.id and f.upload_status == "completed"
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

    # Delete from database
    file_name = file.file_name
    db.delete(file)
    db.commit()

    logger.info(
      f"Deleted file {file_id} ({file_name}) from graph {graph_id} - "
      f"DuckDB will automatically exclude it from queries"
    )

    return {
      "status": "deleted",
      "file_id": file_id,
      "file_name": file_name,
      "message": "File deleted successfully. DuckDB will automatically exclude it from queries.",
    }

  except Exception as e:
    logger.error(f"Failed to delete file {file_id}: {e}")
    db.rollback()
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to delete file: {str(e)}",
    )
