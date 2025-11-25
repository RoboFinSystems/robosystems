"""
Database restore endpoints for Graph API.

This module provides endpoints for restoring LadybugDB databases
from encrypted backups.
"""

from datetime import datetime, timezone
from fastapi import (
  APIRouter,
  BackgroundTasks,
  Depends,
  HTTPException,
  Path,
  Form,
  Response,
)
from fastapi import status as http_status

from robosystems.graph_api.models.database import RestoreResponse
from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.core.task_manager import restore_task_manager
from robosystems.graph_api.core.utils import validate_database_name
from robosystems.logger import logger
from robosystems.operations.lbug.backup_manager import (
  create_backup_manager,
  RestoreJob,
  BackupFormat,
)

router = APIRouter(prefix="/databases", tags=["Backup"])


async def perform_restore(
  task_id: str,
  graph_id: str,
  s3_bucket: str,
  s3_key: str,
  create_system_backup: bool,
  force_overwrite: bool,
  encrypted: bool,
  compressed: bool,
  connection_pool=None,
) -> None:
  """
  Perform the actual restore in the background.
  Updates task status for monitoring.

  Args:
      connection_pool: LadybugDB connection pool for closing connections before restore
  """
  try:
    # Update task status to running
    await restore_task_manager.update_task(
      task_id,
      status="running",
      metadata={"started_at": datetime.now(timezone.utc).isoformat()},
    )

    logger.info(
      f"[Task {task_id}] Starting restore for database '{graph_id}' from {s3_bucket}/{s3_key}"
    )

    # Create backup manager
    backup_manager = create_backup_manager()

    # Download actual metadata from S3
    backup_metadata = await backup_manager.s3_adapter.get_backup_metadata_by_key(s3_key)

    if not backup_metadata:
      logger.error(f"[Task {task_id}] Failed to download backup metadata from S3")
      raise RuntimeError(f"Failed to download backup metadata for {s3_key}")

    logger.info(
      f"[Task {task_id}] Downloaded metadata - checksum: {backup_metadata.checksum}, "
      f"original_size: {backup_metadata.original_size}"
    )

    # If force_overwrite, delete the existing database first
    # This happens in the Graph API so we properly close connections and clean up
    if connection_pool and force_overwrite:
      from pathlib import Path
      import shutil
      from robosystems.middleware.graph.utils import MultiTenantUtils

      logger.info(f"[Task {task_id}] Deleting existing database for {graph_id}")

      # Close all LadybugDB connections first
      connection_pool.close_database_connections(graph_id)
      logger.info(f"[Task {task_id}] Closed LadybugDB connections")

      # Delete the database files
      db_path = Path(MultiTenantUtils.get_database_path_for_graph(graph_id))
      if db_path.exists():
        if db_path.is_file():
          db_path.unlink()
        else:
          shutil.rmtree(db_path)
        logger.info(f"[Task {task_id}] Deleted existing database files at {db_path}")

    # Create restore job
    # drop_existing=False because we already deleted it above if needed
    restore_job = RestoreJob(
      graph_id=graph_id,
      backup_metadata=backup_metadata,
      backup_format=BackupFormat.FULL_DUMP,
      create_new_database=False,
      drop_existing=False,
      verify_after_restore=True,
      progress_tracker=None,
    )

    # Run restore (this is async)
    success = await backup_manager.restore_backup(restore_job)

    if not success:
      raise RuntimeError("Restore verification failed")

    # Mark task as completed
    await restore_task_manager.complete_task(
      task_id,
      result={
        "graph_id": graph_id,
        "s3_key": s3_key,
        "restored_at": datetime.now(timezone.utc).isoformat(),
      },
    )

    logger.info(f"[Task {task_id}] Restore completed successfully")

  except Exception as e:
    logger.error(f"[Task {task_id}] Restore failed: {str(e)}")
    await restore_task_manager.fail_task(task_id, str(e))


@router.post("/{graph_id}/restore", response_model=RestoreResponse)
async def restore_database(
  background_tasks: BackgroundTasks,
  s3_bucket: str = Form(..., description="S3 bucket containing the backup"),
  s3_key: str = Form(..., description="S3 key path to the backup"),
  graph_id: str = Path(..., description="Graph database identifier"),
  create_system_backup: bool = Form(
    True, description="Create system backup before restore"
  ),
  force_overwrite: bool = Form(False, description="Force overwrite existing database"),
  encrypted: bool = Form(True, description="Whether the backup is encrypted"),
  compressed: bool = Form(True, description="Whether the backup is compressed"),
  ladybug_service=Depends(get_ladybug_service),
) -> RestoreResponse:
  """
  Restore a database from S3 backup.

  This endpoint restores a complete LadybugDB database from S3:
  - Downloads backup from S3
  - Decrypts if encrypted
  - Decompresses if compressed
  - Creates a system backup of existing database before restore
  - Runs asynchronously with progress tracking

  The restore operation runs as a background task and can be monitored
  using the returned task_id.
  """
  # Validate graph_id to prevent path injection
  graph_id = validate_database_name(graph_id)

  if ladybug_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Restore operations not allowed on read-only nodes",
    )

  # Check if database exists
  database_exists = graph_id in ladybug_service.db_manager.list_databases()

  if database_exists and not force_overwrite:
    raise HTTPException(
      status_code=http_status.HTTP_409_CONFLICT,
      detail=f"Database {graph_id} already exists. Use force_overwrite=true to replace it.",
    )

  # Create task in task manager
  task_id = await restore_task_manager.create_task(
    task_type="restore",
    metadata={
      "database": graph_id,
      "s3_bucket": s3_bucket,
      "s3_key": s3_key,
      "create_system_backup": create_system_backup and database_exists,
      "force_overwrite": force_overwrite,
      "encrypted": encrypted,
      "compressed": compressed,
    },
  )

  # Add restore task to FastAPI background tasks
  background_tasks.add_task(
    perform_restore,
    task_id=task_id,
    graph_id=graph_id,
    s3_bucket=s3_bucket,
    s3_key=s3_key,
    create_system_backup=create_system_backup and database_exists,
    force_overwrite=force_overwrite,
    encrypted=encrypted,
    compressed=compressed,
    connection_pool=ladybug_service.db_manager.connection_pool,
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


@router.post("/{graph_id}/backup-download")
async def download_backup(
  graph_id: str = Path(..., description="Graph database identifier"),
  ladybug_service=Depends(get_ladybug_service),
) -> Response:
  """
  Download the current database as a backup.

  This endpoint creates a backup of the database and returns it
  as binary data for download. This is used by the main API
  to get the database backup for storage in S3.
  """
  # Validate graph_id to prevent path injection
  graph_id = validate_database_name(graph_id)

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

  try:
    # Get database path
    from robosystems.middleware.graph.utils import MultiTenantUtils
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
        shutil.copy2(db_path, temp_path / f"{graph_id}.lbug")
      else:
        # Directory-based database
        shutil.copytree(db_path, temp_path / graph_id)

      # Create ZIP archive
      zip_file = temp_path / f"{graph_id}_backup.zip"
      with zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED) as zf:
        if os.path.isfile(db_path):
          zf.write(temp_path / f"{graph_id}.lbug", f"{graph_id}.lbug")
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

    return Response(
      content=backup_data,
      media_type="application/zip",
      headers={
        "Content-Disposition": f'attachment; filename="{graph_id}_backup.zip"',
        "X-Database": graph_id,
        "X-Backup-Format": "full_dump",
        "X-Backup-Size": str(len(backup_data)),
      },
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to create backup for database {graph_id}: {str(e)}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to create backup: {str(e)}",
    )


# NOTE: SSE monitoring has been moved to the generic /tasks/{task_id}/monitor endpoint
# This endpoint is no longer needed as all task monitoring is centralized
