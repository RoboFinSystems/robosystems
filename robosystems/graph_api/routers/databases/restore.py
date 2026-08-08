"""Restore LadybugDB databases from S3 backups.

Restores run as background tasks; progress is monitored through the generic
``GET /tasks/{task_id}/monitor`` SSE endpoint in
``robosystems/graph_api/routers/tasks.py``.
"""

from datetime import UTC, datetime

from fastapi import (
  APIRouter,
  BackgroundTasks,
  Depends,
  Form,
  HTTPException,
  Path,
  Response,
)
from fastapi import status as http_status

from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.core.task_manager import restore_task_manager
from robosystems.graph_api.core.utils import validate_database_name
from robosystems.graph_api.models.database import RestoreResponse
from robosystems.logger import logger
from robosystems.operations.graph.engine.backup_manager import (
  BackupFormat,
  RestoreJob,
  create_backup_manager,
)

router = APIRouter(prefix="/databases", tags=["Backup"])


async def perform_restore(
  task_id: str,
  graph_id: str,
  s3_bucket: str,
  s3_key: str,
  force_overwrite: bool,
  create_system_backup: bool = True,
  connection_pool=None,
) -> None:
  """Run the restore in the background, updating task status for monitoring.

  ``connection_pool`` is the LadybugDB pool, needed to close open connections
  before the database files are replaced.

  ``create_system_backup`` snapshots the existing database to S3 before it is
  overwritten; a failed snapshot aborts the restore rather than leaving the
  old data unrecoverable.

  Nothing on disk is touched until the replacement payload has been downloaded
  and its checksum verified. ``restore_backup`` does both before it reaches the
  importer, and the importer takes the safety snapshot immediately before it
  overwrites — so every abort short of the final copy leaves the existing
  database exactly as it was.

  Encryption and compression are read from the backup's own S3 metadata, so
  the caller does not describe them.
  """
  try:
    await restore_task_manager.update_task(
      task_id,
      status="running",
      metadata={"started_at": datetime.now(UTC).isoformat()},
    )

    logger.info(
      f"[Task {task_id}] Starting restore for database '{graph_id}' from {s3_bucket}/{s3_key}"
    )

    backup_manager = create_backup_manager()

    backup_metadata = await backup_manager.s3_adapter.get_backup_metadata_by_key(s3_key)

    if not backup_metadata:
      logger.error(f"[Task {task_id}] Failed to download backup metadata from S3")
      raise RuntimeError(f"Failed to download backup metadata for {s3_key}")

    logger.info(
      f"[Task {task_id}] Downloaded metadata - checksum: {backup_metadata.checksum}, "
      f"original_size: {backup_metadata.original_size}"
    )

    # Only this process holds the LadybugDB connections, so they must close
    # here — but closing is all that happens now. The files themselves are
    # replaced by the importer, after the payload is verified and the safety
    # snapshot is taken. Deleting them here instead used to defeat both: a
    # failed download left the graph with nothing, and the importer's
    # abort-on-failed-snapshot guard was dead code because it is conditioned on
    # the database still existing.
    if connection_pool and force_overwrite:
      connection_pool.close_database_connections(graph_id)
      logger.info(f"[Task {task_id}] Closed LadybugDB connections for {graph_id}")

    # drop_existing=False: the full-dump importer overwrites in place, and
    # doing it there keeps the existing database intact until the replacement
    # is on disk and checksum-verified.
    restore_job = RestoreJob(
      graph_id=graph_id,
      backup_metadata=backup_metadata,
      backup_format=BackupFormat.FULL_DUMP,
      create_new_database=False,
      drop_existing=False,
      verify_after_restore=True,
      create_system_backup=create_system_backup,
      progress_tracker=None,
    )

    success = await backup_manager.restore_backup(restore_job)

    if not success:
      raise RuntimeError("Restore verification failed")

    await restore_task_manager.complete_task(
      task_id,
      result={
        "graph_id": graph_id,
        "s3_key": s3_key,
        "restored_at": datetime.now(UTC).isoformat(),
      },
    )

    logger.info(f"[Task {task_id}] Restore completed successfully")

  except Exception as e:
    logger.error(f"[Task {task_id}] Restore failed: {e!s}")
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
  compressed: bool = Form(True, description="Whether the backup is compressed"),
  ladybug_service=Depends(get_ladybug_service),
) -> RestoreResponse:
  """
  Restore a database from S3 backup.

  This endpoint restores a complete LadybugDB database from S3:
  - Downloads the backup from S3
  - Decompresses according to the backup's stored metadata
  - Snapshots the existing database to S3 first, unless
    ``create_system_backup`` is false; a failed snapshot aborts the restore
  - Replaces the existing database, which requires ``force_overwrite``
  - Runs asynchronously with progress tracking

  The restore operation runs as a background task and can be monitored
  using the returned task_id.

  ``compressed`` is accepted for wire compatibility but inert: the backup's
  own stored metadata governs it.
  """
  # Validate graph_id to prevent path injection
  graph_id = validate_database_name(graph_id)

  if ladybug_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Restore operations not allowed on read-only nodes",
    )

  database_exists = graph_id in ladybug_service.db_manager.list_databases()

  if database_exists and not force_overwrite:
    raise HTTPException(
      status_code=http_status.HTTP_409_CONFLICT,
      detail=f"Database {graph_id} already exists. Use force_overwrite=true to replace it.",
    )

  task_id = await restore_task_manager.create_task(
    task_type="restore",
    metadata={
      "database": graph_id,
      "s3_bucket": s3_bucket,
      "s3_key": s3_key,
      "create_system_backup": create_system_backup and database_exists,
      "force_overwrite": force_overwrite,
      "compressed": compressed,
    },
  )

  background_tasks.add_task(
    perform_restore,
    task_id=task_id,
    graph_id=graph_id,
    s3_bucket=s3_bucket,
    s3_key=s3_key,
    force_overwrite=force_overwrite,
    create_system_backup=create_system_backup and database_exists,
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

  if graph_id not in ladybug_service.db_manager.list_databases():
    raise HTTPException(
      status_code=http_status.HTTP_404_NOT_FOUND,
      detail=f"Database {graph_id} not found",
    )

  try:
    import os
    import shutil
    import tempfile
    import zipfile
    from pathlib import Path

    from robosystems.middleware.graph.utils import MultiTenantUtils

    # Fold the WAL into the main database file before copying it. LadybugDB
    # holds recent writes in {graph_id}.lbug.wal until a checkpoint, and this
    # path copies only the main file — so without this, a graph that has not
    # been evicted since its last write backs up as of its last checkpoint,
    # which for a young graph is an empty database. The default
    # checkpoint_threshold is 512 MB, which a tenant graph never reaches.
    #
    # Mirrors OnInstanceBackupService._checkpoint, which the replica and
    # duckdb_staging paths have always used. Safe to open here because the
    # list_databases() membership check above already ran: LadybugDB creates a
    # database when the path is absent, so checkpointing an unknown graph would
    # mint an empty one and pass the existence guard below.
    try:
      with ladybug_service.db_manager.get_connection(graph_id, read_only=False) as conn:
        conn.execute("CHECKPOINT")
    except Exception as e:
      # Fail the backup rather than fall through to a silently stale copy —
      # a backup that reports success over incomplete data is worse than none.
      logger.error(f"CHECKPOINT failed for {graph_id}, aborting backup: {e!s}")
      raise HTTPException(
        status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Could not checkpoint the database; backup aborted rather than "
        "producing a possibly-stale copy.",
      )

    db_path = MultiTenantUtils.get_database_path_for_graph(graph_id)

    if not os.path.exists(db_path):
      raise HTTPException(
        status_code=http_status.HTTP_404_NOT_FOUND,
        detail=f"Database files not found for {graph_id}",
      )

    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir)

      if os.path.isfile(db_path):
        shutil.copy2(db_path, temp_path / f"{graph_id}.lbug")
      else:
        shutil.copytree(db_path, temp_path / graph_id)

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
    logger.error(f"Failed to create backup for database {graph_id}: {e!s}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to create backup: {e!s}",
    )
