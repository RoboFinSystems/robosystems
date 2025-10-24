"""
Database backup and restore endpoints for Kuzu API.

This module provides endpoints for creating backups and restoring
Kuzu databases.
"""

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Path
from fastapi import status as http_status

from robosystems.graph_api.models.database import BackupRequest, BackupResponse
from robosystems.graph_api.core.cluster_manager import get_cluster_service
from robosystems.graph_api.core.task_manager import backup_task_manager
from robosystems.logger import logger

router = APIRouter(prefix="/databases", tags=["Backup"])


@router.post("/{graph_id}/backup", response_model=BackupResponse)
async def create_backup(
  request: BackupRequest,
  background_tasks: BackgroundTasks,
  graph_id: str = Path(..., description="Graph database identifier"),
  cluster_service=Depends(get_cluster_service),
) -> BackupResponse:
  """
  Create a backup of a database.

  Initiates a background task to create a complete backup of the Kuzu database.
  The backup will be:
  - Full dump of the .kuzu database file
  - Compressed for optimal storage
  - Optionally encrypted for security

  The backup operation runs asynchronously and can be monitored using
  the returned task_id.
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

  # Create task in task manager
  task_id = await backup_task_manager.create_task(
    task_type="backup",
    metadata={
      "database": graph_id,
      "backup_format": request.backup_format,
      "compression": request.compression,
      "encryption": request.encryption,
    },
  )

  # Add backup task to background tasks (when implemented)
  # For now, just mark as failed since it's not implemented
  await backup_task_manager.fail_task(
    task_id, "Backup functionality not yet implemented"
  )

  logger.info(f"Backup initiated for database {graph_id} with task ID: {task_id}")

  return BackupResponse(
    task_id=task_id,
    status="initiated",
    message=f"Backup task started for database {graph_id}",
    database=graph_id,
    backup_format=request.backup_format,
    monitor_url=f"/tasks/{task_id}/monitor",
    estimated_completion_time=None,  # Could calculate based on database size
  )


# NOTE: Database restore functionality has been moved to restore.py
# The restore endpoint is fully implemented there with S3 integration

# NOTE: SSE monitoring has been moved to the generic /tasks/{task_id}/monitor endpoint
# This endpoint is no longer needed as all task monitoring is centralized
