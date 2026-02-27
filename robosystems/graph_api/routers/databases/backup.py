"""
Database backup and restore endpoints for Graph API.

This module provides endpoints for creating backups and restoring
LadybugDB databases.

Supports four backup types:
- standard: Existing BackupManager flow (ZIP, optional encrypt, S3 upload)
- replica: Raw .lbug upload to S3 via OnInstanceBackupService (for replica fleet download)
- shared_repository: Compressed tar.gz to S3 via OnInstanceBackupService
- duckdb_staging: Raw .duckdb upload to S3 via OnInstanceBackupService
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
  backup_type: str = "standard",
  s3_destination: dict | None = None,
  checkpoint: bool = True,
  vacuum: bool = False,
  db_manager=None,
  duckdb_pool=None,
) -> None:
  """
  Perform the actual backup in the background.
  Updates task status for monitoring.

  Routes to OnInstanceBackupService for replica/shared_repository/duckdb_staging types,
  or to BackupManager for standard backups.
  """
  try:
    if (
      backup_type in ("replica", "shared_repository", "duckdb_staging")
      and s3_destination
    ):
      # On-instance backup: CHECKPOINT + direct S3 upload
      from robosystems.graph_api.core.backup_service import OnInstanceBackupService

      service = OnInstanceBackupService(
        db_manager=db_manager,
        task_manager=backup_task_manager,
        duckdb_pool=duckdb_pool,
      )

      result = await service.execute_backup(
        task_id=task_id,
        graph_id=graph_id,
        backup_type=backup_type,
        s3_destination=s3_destination,
        compression=compression,
        encryption=encryption,
        checkpoint=checkpoint,
        vacuum=vacuum,
      )

      await backup_task_manager.complete_task(task_id, result=result)
      logger.info(f"[Task {task_id}] On-instance backup completed successfully")

    else:
      # Standard backup via BackupManager (existing flow)
      await backup_task_manager.update_task(
        task_id,
        status="running",
        metadata={"started_at": datetime.now(UTC).isoformat()},
      )

      logger.info(f"[Task {task_id}] Starting backup for database '{graph_id}'")

      backup_manager = create_backup_manager()

      backup_job = BackupJob(
        graph_id=graph_id,
        backup_type=BackupType.FULL,
        backup_format=BackupFormat(backup_format),
        compression=compression,
        encryption=encryption,
        allow_export=not encryption,
      )

      backup_info = await backup_manager.create_backup(backup_job)

      checksum_str = backup_info.checksum
      if isinstance(checksum_str, bytes):
        checksum_str = checksum_str.hex()

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

  Backup types:
  - **standard**: Full dump ZIP, optionally encrypted, uploaded to S3
  - **replica**: Raw .lbug uploaded to S3 (downloaded by replica fleet at startup)
  - **shared_repository**: Compressed tar.gz uploaded to S3 (for subscriber downloads)
  - **duckdb_staging**: Raw .duckdb uploaded to S3 (for local dev / analytics)

  For replica/shared_repository/duckdb_staging types, s3_destination is required.
  """
  if ladybug_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Backup operations not allowed on read-only nodes",
    )

  # Validate database exists (DuckDB staging uses its own file, not LadybugDB)
  if request.backup_type == "duckdb_staging":
    import re
    from pathlib import Path

    from robosystems.config.storage.shared import get_staging_duckdb_path

    # Validate graph_id to prevent path traversal
    if not re.match(r"^[a-zA-Z0-9_]+$", graph_id):
      raise HTTPException(
        status_code=http_status.HTTP_400_BAD_REQUEST,
        detail="Invalid graph_id: must be alphanumeric with underscores only",
      )

    duckdb_path = Path(get_staging_duckdb_path(graph_id))
    if not duckdb_path.exists():
      raise HTTPException(
        status_code=http_status.HTTP_404_NOT_FOUND,
        detail=f"DuckDB staging database not found: {graph_id}",
      )
  else:
    if graph_id not in ladybug_service.db_manager.list_databases():
      raise HTTPException(
        status_code=http_status.HTTP_404_NOT_FOUND,
        detail=f"Database {graph_id} not found",
      )

  # Validate s3_destination for non-standard backup types
  if (
    request.backup_type in ("replica", "shared_repository", "duckdb_staging")
    and not request.s3_destination
  ):
    raise HTTPException(
      status_code=http_status.HTTP_400_BAD_REQUEST,
      detail=f"s3_destination is required for backup_type '{request.backup_type}'",
    )

  # Create task in task manager
  task_id = await backup_task_manager.create_task(
    task_type="backup",
    metadata={
      "database": graph_id,
      "backup_format": request.backup_format,
      "backup_type": request.backup_type,
      "compression": request.compression,
      "encryption": request.encryption,
    },
  )

  # Serialize s3_destination for background task
  s3_dest_dict = None
  if request.s3_destination:
    s3_dest_dict = {
      "bucket": request.s3_destination.bucket,
      "key": request.s3_destination.key,
    }

  # Get DuckDB pool for duckdb_staging backups
  duckdb_pool = None
  if request.backup_type == "duckdb_staging":
    from robosystems.graph_api.core.duckdb import get_duckdb_pool

    duckdb_pool = get_duckdb_pool()

  # Add backup task to FastAPI background tasks
  background_tasks.add_task(
    perform_backup,
    task_id=task_id,
    graph_id=graph_id,
    backup_format=request.backup_format,
    compression=request.compression,
    encryption=request.encryption,
    backup_type=request.backup_type,
    s3_destination=s3_dest_dict,
    checkpoint=request.checkpoint,
    vacuum=request.vacuum,
    db_manager=ladybug_service.db_manager,
    duckdb_pool=duckdb_pool,
  )

  logger.info(
    f"Backup initiated for database {graph_id} with task ID: {task_id} "
    f"(type={request.backup_type})"
  )

  return BackupResponse(
    task_id=task_id,
    status="initiated",
    message=f"Backup task started for database {graph_id}",
    database=graph_id,
    backup_format=request.backup_format,
    monitor_url=f"/tasks/{task_id}/monitor",
    estimated_completion_time=None,
  )
