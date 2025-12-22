"""
Database backup and restore endpoints for Graph API.

This module provides endpoints for creating backups and restoring
LadybugDB databases.
"""

from datetime import UTC, datetime

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Path
from fastapi import status as http_status

from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.core.task_manager import backup_task_manager
from robosystems.graph_api.models.database import BackupRequest, BackupResponse
from robosystems.logger import logger
from robosystems.operations.lbug.backup_manager import (
  BackupFormat,
  BackupJob,
  BackupType,
  create_backup_manager,
)

router = APIRouter(prefix="/databases", tags=["Backup"])


async def perform_backup(
  task_id: str,
  graph_id: str,
  backup_format: str,
  compression: bool,
  encryption: bool,
) -> None:
  """
  Perform the actual backup in the background.
  Updates task status for monitoring.
  """
  try:
    # Update task status to running
    await backup_task_manager.update_task(
      task_id,
      status="running",
      metadata={"started_at": datetime.now(UTC).isoformat()},
    )

    logger.info(f"[Task {task_id}] Starting backup for database '{graph_id}'")

    # Create backup manager and execute backup
    backup_manager = create_backup_manager()

    backup_job = BackupJob(
      graph_id=graph_id,
      backup_type=BackupType.FULL,
      backup_format=BackupFormat(backup_format),
      compression=compression,
      encryption=encryption,
      allow_export=not encryption,
    )

    # Run backup (this is async)
    backup_info = await backup_manager.create_backup(backup_job)

    # Ensure checksum is a string (convert from bytes if needed)
    checksum_str = backup_info.checksum
    if isinstance(checksum_str, bytes):
      checksum_str = checksum_str.hex()

    # Mark task as completed
    await backup_task_manager.complete_task(
      task_id,
      result={
        "s3_key": backup_info.s3_key,
        "original_size": backup_info.original_size,
        "compressed_size": backup_info.compressed_size,
        "checksum": checksum_str,
        "duration_seconds": backup_info.backup_duration_seconds,
      },
    )

    logger.info(f"[Task {task_id}] Backup completed successfully")

  except Exception as e:
    logger.error(f"[Task {task_id}] Backup failed: {e!s}")
    await backup_task_manager.fail_task(task_id, str(e))


@router.post("/{graph_id}/backup", response_model=BackupResponse)
async def create_backup(
  request: BackupRequest,
  background_tasks: BackgroundTasks,
  graph_id: str = Path(..., description="Graph database identifier"),
  ladybug_service=Depends(get_ladybug_service),
) -> BackupResponse:
  """
  Create a backup of a database.

  Initiates a background task to create a complete backup of the LadybugDB database.
  The backup will be:
  - Full dump of the .lbug database file
  - Compressed for optimal storage
  - Optionally encrypted for security

  The backup operation runs asynchronously and can be monitored using
  the returned task_id.
  """
  if ladybug_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Backup operations not allowed on read-only nodes",
    )

  # Validate database exists
  if graph_id not in ladybug_service.db_manager.list_databases():
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

  # Add backup task to FastAPI background tasks
  background_tasks.add_task(
    perform_backup,
    task_id=task_id,
    graph_id=graph_id,
    backup_format=request.backup_format,
    compression=request.compression,
    encryption=request.encryption,
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
