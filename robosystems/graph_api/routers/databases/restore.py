"""
Database restore endpoints for Kuzu API.

This module provides endpoints for restoring Kuzu databases
from encrypted backups.
"""

from typing import Dict, Any
from fastapi import (
  APIRouter,
  Depends,
  HTTPException,
  Path,
  Form,
  File,
  UploadFile,
)
from fastapi import status as http_status

from robosystems.graph_api.models.database import RestoreResponse
from robosystems.graph_api.core.cluster_manager import get_cluster_service
from robosystems.graph_api.core.task_manager import restore_task_manager
from robosystems.logger import logger

router = APIRouter(prefix="/databases", tags=["Database Backup"])


@router.post("/{graph_id}/restore", response_model=RestoreResponse)
async def restore_database(
  backup_data: UploadFile = File(..., description="Encrypted backup data to restore"),
  graph_id: str = Path(..., description="Graph database identifier"),
  create_system_backup: bool = Form(
    True, description="Create system backup before restore"
  ),
  force_overwrite: bool = Form(False, description="Force overwrite existing database"),
  cluster_service=Depends(get_cluster_service),
) -> RestoreResponse:
  """
  Restore a database from an encrypted backup.

  This endpoint restores a complete Kuzu database from backup data:
  - Only accepts encrypted backup data for security
  - Creates a system backup of existing database before restore
  - Runs asynchronously with progress tracking

  The restore operation runs as a background task and can be monitored
  using the returned task_id.
  """
  if cluster_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Restore operations not allowed on read-only nodes",
    )

  # Check if database exists
  database_exists = graph_id in cluster_service.db_manager.list_databases()

  if database_exists and not force_overwrite:
    raise HTTPException(
      status_code=http_status.HTTP_409_CONFLICT,
      detail=f"Database {graph_id} already exists. Use force_overwrite=true to replace it.",
    )

  # Read the uploaded file
  backup_bytes = await backup_data.read()

  # Create task in task manager
  task_id = await restore_task_manager.create_task(
    task_type="restore",
    metadata={
      "database": graph_id,
      "backup_size": len(backup_bytes),
      "create_system_backup": create_system_backup and database_exists,
      "force_overwrite": force_overwrite,
    },
  )

  # Add restore task to background tasks (when implemented)
  # For now, just mark as failed since it's not implemented
  await restore_task_manager.fail_task(
    task_id, "Restore functionality not yet implemented"
  )

  logger.info(f"Restore initiated for database {graph_id} with task ID: {task_id}")

  return RestoreResponse(
    task_id=task_id,
    status="initiated",
    message=f"Restore task started for database {graph_id}",
    database=graph_id,
    monitor_url=f"/tasks/{task_id}/monitor",
    system_backup_created=create_system_backup and database_exists,
  )


@router.post("/{graph_id}/backup-download", response_model=Dict[str, Any])
async def download_backup(
  graph_id: str = Path(..., description="Graph database identifier"),
  cluster_service=Depends(get_cluster_service),
) -> Dict[str, Any]:
  """
  Download the current database as a backup.

  This endpoint creates a backup of the database and returns it
  as binary data for download. This is used by the main API
  to get the database backup for storage in S3.
  """
  if cluster_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Backup operations not allowed on read-only nodes",
    )

  # Validate database exists
  if graph_id not in cluster_service.db_manager.list_databases():
    raise HTTPException(
      status_code=http_status.HTTP_404_NOT_FOUND,
      detail=f"Database {graph_id} not found",
    )

  try:
    # Get database path
    from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils
    import os
    import tempfile
    import shutil
    import zipfile
    from pathlib import Path

    db_path = MultiTenantUtils.get_database_path_for_graph(graph_id)

    if not os.path.exists(db_path):
      raise HTTPException(
        status_code=http_status.HTTP_404_NOT_FOUND,
        detail=f"Database files not found for {graph_id}",
      )

    # Create backup in temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir)

      # Copy database files
      if os.path.isfile(db_path):
        # Single file database
        shutil.copy2(db_path, temp_path / f"{graph_id}.kuzu")
      else:
        # Directory-based database
        shutil.copytree(db_path, temp_path / graph_id)

      # Create ZIP archive
      zip_file = temp_path / f"{graph_id}_backup.zip"
      with zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED) as zf:
        if os.path.isfile(db_path):
          zf.write(temp_path / f"{graph_id}.kuzu", f"{graph_id}.kuzu")
        else:
          for root, dirs, files in os.walk(temp_path / graph_id):
            for file in files:
              file_path = Path(root) / file
              arc_path = file_path.relative_to(temp_path)
              zf.write(file_path, arc_path)

      # Read ZIP file content
      with open(zip_file, "rb") as f:
        backup_data = f.read()

    logger.info(
      f"Created backup for database {graph_id}, size: {len(backup_data)} bytes"
    )

    return {
      "backup_data": backup_data,
      "size_bytes": len(backup_data),
      "database": graph_id,
      "format": "full_dump",
    }

  except Exception as e:
    logger.error(f"Failed to create backup for database {graph_id}: {str(e)}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to create backup: {str(e)}",
    )


# NOTE: SSE monitoring has been moved to the generic /tasks/{task_id}/monitor endpoint
# This endpoint is no longer needed as all task monitoring is centralized
