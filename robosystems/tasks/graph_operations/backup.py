"""
Celery tasks for graph database backup management.

This module provides Celery tasks for automated backup creation, restoration,
and maintenance operations in the RoboSystems multitenant environment.
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

from celery import Task
from ...celery import celery_app, QUEUE_DEFAULT
from robosystems.operations.aws.s3 import S3BackupAdapter
from ...models.iam import GraphBackup, BackupStatus
from ...database import session
from ...logger import logger
from ...operations.lbug.backup_manager import BackupJob, BackupFormat, BackupType
from ...graph_api.client.factory import GraphClientFactory


class CallbackTask(Task):
  """Base task class with callback support."""

  def on_success(self, retval, task_id, args, kwargs):
    """Called on task success."""
    logger.info(f"Task {task_id} completed successfully")

  def on_failure(self, exc, task_id, args, kwargs, einfo):
    """Called on task failure."""
    logger.error(f"Task {task_id} failed: {exc}")


@celery_app.task(bind=True, base=CallbackTask, queue=QUEUE_DEFAULT)
def cleanup_expired_backups(self, dry_run: bool = False) -> Dict[str, Any]:
  """
  Clean up expired backups from S3 and database.

  Args:
      dry_run: If True, only report what would be deleted

  Returns:
      Dict containing cleanup statistics
  """
  task_id = self.request.id
  logger.info(f"Starting backup cleanup task {task_id} (dry_run={dry_run})")

  try:
    # Get expired backups
    expired_backups = GraphBackup.get_expired_backups(session)

    deleted_count = 0
    errors = []
    total_size_freed = 0

    for backup in expired_backups:
      try:
        if not dry_run:
          # Delete from S3
          s3_adapter = S3BackupAdapter(
            enable_compression=backup.compression_enabled,
          )
          success = asyncio.run(
            s3_adapter.delete_backup(
              graph_id=backup.graph_id,
              timestamp=backup.completed_at,
              backup_type=backup.backup_type,
            )
          )

          if success:
            # Mark as expired in database
            backup.expire_backup(session)
            deleted_count += 1
            total_size_freed += backup.encrypted_size_bytes
          else:
            errors.append(f"Failed to delete S3 backup: {backup.s3_key}")
        else:
          # Dry run - just count
          deleted_count += 1
          total_size_freed += backup.encrypted_size_bytes

      except Exception as e:
        errors.append(f"Error processing backup {backup.id}: {str(e)}")

    result = {
      "task_id": task_id,
      "dry_run": dry_run,
      "expired_backups_found": len(expired_backups),
      "deleted_count": deleted_count,
      "total_size_freed_bytes": total_size_freed,
      "errors": errors,
    }

    logger.info(
      f"Backup cleanup task {task_id} completed: {deleted_count} backups processed"
    )
    return result

  except Exception as e:
    logger.error(f"Backup cleanup task {task_id} failed: {e}")
    raise


@celery_app.task(bind=True, base=CallbackTask, queue=QUEUE_DEFAULT)
def backup_retention_management(
  self,
  graph_id: Optional[str] = None,
  retention_days: int = 90,
  dry_run: bool = False,
) -> Dict[str, Any]:
  """
  Manage backup retention for specific graphs or all graphs.

  Args:
      graph_id: Specific graph to manage (optional)
      retention_days: Number of days to retain backups
      dry_run: If True, only report what would be deleted

  Returns:
      Dict containing retention management statistics
  """
  task_id = self.request.id
  logger.info(
    f"Starting retention management task {task_id} for graph '{graph_id or 'all'}'"
  )

  try:
    from robosystems.operations.lbug.backup_manager import BackupManager

    s3_adapter = S3BackupAdapter()
    backup_manager = BackupManager(s3_adapter=s3_adapter)

    deleted_count = 0

    if graph_id:
      # Manage specific graph
      if not dry_run:
        deleted_count = asyncio.run(
          backup_manager.delete_old_backups(graph_id, retention_days)
        )
      else:
        # Count what would be deleted
        backups = asyncio.run(backup_manager.list_backups(graph_id))
        cutoff_date = datetime.now(timezone.utc).timestamp() - (
          retention_days * 24 * 3600
        )
        deleted_count = sum(
          1 for b in backups if b["last_modified"].timestamp() < cutoff_date
        )
    else:
      # Manage all graphs
      all_backups = GraphBackup.query.all()
      unique_graphs = set(backup.graph_id for backup in all_backups)

      for gid in unique_graphs:
        if not dry_run:
          count = asyncio.run(backup_manager.delete_old_backups(gid, retention_days))
          deleted_count += count
        else:
          # Count what would be deleted
          backups = asyncio.run(backup_manager.list_backups(gid))
          cutoff_date = datetime.now(timezone.utc).timestamp() - (
            retention_days * 24 * 3600
          )
          count = sum(
            1 for b in backups if b["last_modified"].timestamp() < cutoff_date
          )
          deleted_count += count

    result = {
      "task_id": task_id,
      "graph_id": graph_id,
      "retention_days": retention_days,
      "dry_run": dry_run,
      "deleted_count": deleted_count,
    }

    logger.info(
      f"Retention management task {task_id} completed: {deleted_count} backups deleted"
    )
    return result

  except Exception as e:
    logger.error(f"Retention management task {task_id} failed: {e}")
    raise


@celery_app.task(bind=True, base=CallbackTask, queue=QUEUE_DEFAULT)
def backup_health_check(self) -> Dict[str, Any]:
  """
  Perform health check on backup system components.

  Returns:
      Dict containing health status information
  """
  task_id = self.request.id
  logger.info(f"Starting backup health check task {task_id}")

  try:
    from robosystems.operations.lbug.backup_manager import BackupManager

    s3_adapter = S3BackupAdapter()
    backup_manager = BackupManager(s3_adapter=s3_adapter)

    # Perform health checks
    health_status = backup_manager.health_check()

    # Add database health check
    try:
      recent_backups = GraphBackup.get_pending_backups(session)
      stuck_backups = [
        b
        for b in recent_backups
        if b.started_at
        and (datetime.now(timezone.utc) - b.started_at).total_seconds() > 3600
      ]

      health_status["database"] = {
        "status": "healthy",
        "pending_backups": len(recent_backups),
        "stuck_backups": len(stuck_backups),
      }

    except Exception as e:
      health_status["database"] = {
        "status": "unhealthy",
        "error": str(e),
      }

    # Overall health assessment
    components_healthy = all(
      component.get("status") == "healthy"
      for component in [
        health_status.get("s3", {}),
        health_status.get("graph", {}),
        health_status.get("database", {}),
      ]
    )

    health_status["overall_status"] = "healthy" if components_healthy else "unhealthy"
    health_status["task_id"] = task_id
    health_status["check_time"] = datetime.now(timezone.utc).isoformat()

    logger.info(
      f"Backup health check task {task_id} completed: {health_status['overall_status']}"
    )
    return health_status

  except Exception as e:
    logger.error(f"Backup health check task {task_id} failed: {e}")
    raise


@celery_app.task(bind=True, base=CallbackTask, queue=QUEUE_DEFAULT)
def restore_graph_backup(
  self,
  graph_id: str,
  backup_id: str,
  user_id: Optional[str] = None,
  create_system_backup: bool = True,
  verify_after_restore: bool = True,
) -> Dict[str, Any]:
  """
  Restore a graph database from an encrypted backup.

  Args:
      graph_id: Graph database identifier
      backup_id: Backup ID to restore from
      user_id: User requesting the restore (optional)
      create_system_backup: Create system backup before restore
      verify_after_restore: Verify database after restore

  Returns:
      Dict containing restore information
  """
  task_id = self.request.id
  logger.info(
    f"Starting restore task {task_id} for graph '{graph_id}' from backup '{backup_id}' (user: {user_id or 'system'})"
  )

  try:
    # Get backup record from PostgreSQL
    backup_record = GraphBackup.get_by_id(backup_id, session)
    if not backup_record:
      raise ValueError(f"Backup not found: {backup_id}")

    if backup_record.graph_id != graph_id:
      raise ValueError(f"Backup {backup_id} does not belong to graph {graph_id}")

    # Check if backup is encrypted (only encrypted backups can be restored for security)
    if not backup_record.encryption_enabled:
      raise ValueError("Only encrypted backups can be restored for security reasons")

    # Check backup format (only full_dump supported)
    backup_format = backup_record.backup_metadata.get("backup_format", "full_dump")
    if backup_format != "full_dump":
      raise ValueError(f"Restore only supports full_dump format, got: {backup_format}")

    # Create backup manager to download from S3
    s3_adapter = S3BackupAdapter(
      enable_compression=backup_record.compression_enabled,
    )

    # Download backup from S3
    logger.info(f"Downloading encrypted backup from S3: {backup_record.s3_key}")
    backup_data = asyncio.run(s3_adapter.download_backup_by_key(backup_record.s3_key))

    if not backup_data:
      raise RuntimeError(f"Failed to download backup from S3: {backup_record.s3_key}")

    # Decompress if needed (for S3 storage)
    if backup_record.compression_enabled:
      logger.info("Decompressing backup data")
      import gzip

      backup_data = gzip.decompress(backup_data)

    # Decrypt if needed
    if backup_record.encryption_enabled:
      logger.info("Decrypting backup data")
      from ...security.encryption import decrypt_data

      backup_data = decrypt_data(backup_data)

    # Call Graph API to restore the backup
    logger.info(f"Calling Graph API to restore database for graph '{graph_id}'")

    # Get properly routed LadybugDB client
    client = GraphClientFactory.create_client(graph_id, operation_type="write")

    try:
      # Call restore endpoint on LadybugDB instance
      restore_result = asyncio.run(
        client.restore_backup(
          graph_id=graph_id,
          backup_data=backup_data,
          create_system_backup=create_system_backup,
          force_overwrite=True,  # Force overwrite since we're restoring
        )
      )

      logger.info(
        f"Graph API restore initiated with task ID: {restore_result.get('task_id')}"
      )

      # Wait for restore to complete (with timeout)
      import time

      max_wait = 300  # 5 minutes
      start_time = time.time()

      while time.time() - start_time < max_wait:
        # Check task status
        # Note: We might need to implement a task status endpoint in Graph API
        # For now, assume it completes quickly
        time.sleep(5)
        break  # Remove this when task status checking is implemented

    finally:
      asyncio.run(client.close())

    # Verify restore if requested
    verification_status = "not_verified"
    if verify_after_restore:
      try:
        # Create a simple verification by checking if we can connect and query

        client = asyncio.run(
          GraphClientFactory.create_client(graph_id, operation_type="read")
        )

        # Try to get database info to verify it exists and is accessible
        db_info = asyncio.run(client.get_database_info(graph_id=graph_id))
        asyncio.run(client.close())

        if db_info.get("database_name") == graph_id:
          verification_status = "verified"
          logger.info(f"Restore verification successful for graph '{graph_id}'")
        else:
          verification_status = "failed"
          logger.warning(f"Restore verification failed for graph '{graph_id}'")
      except Exception as e:
        verification_status = "error"
        logger.error(f"Error during restore verification: {str(e)}")

    # Update backup record to track restore
    if hasattr(backup_record, "last_restored_at"):
      backup_record.last_restored_at = datetime.now(timezone.utc)
      session.commit()

    result = {
      "task_id": task_id,
      "graph_id": graph_id,
      "backup_id": backup_id,
      "status": "completed",
      "verification_status": verification_status,
      "system_backup_created": create_system_backup,
      "restored_at": datetime.now(timezone.utc).isoformat(),
      "backup_details": {
        "original_size": backup_record.original_size_bytes,
        "encrypted_size": backup_record.encrypted_size_bytes,
        "created_at": backup_record.created_at.isoformat(),
      },
    }

    logger.info(f"Restore task {task_id} completed successfully for graph '{graph_id}'")
    return result

  except Exception as e:
    logger.error(f"Restore task {task_id} failed for graph '{graph_id}': {e}")
    raise


@celery_app.task(bind=True, base=CallbackTask, queue=QUEUE_DEFAULT)
def delete_single_backup(self, backup_id: str) -> Dict[str, Any]:
  """
  Delete a specific backup from S3 and mark as expired.

  Args:
      backup_id: ID of the backup to delete

  Returns:
      Dict containing deletion information
  """
  task_id = self.request.id
  logger.info(f"Starting delete backup task {task_id} for backup '{backup_id}'")

  try:
    # Get backup record
    backup_record = GraphBackup.get_by_id(backup_id, session)
    if not backup_record:
      raise ValueError(f"Backup not found: {backup_id}")

    # Delete from S3
    s3_adapter = S3BackupAdapter(
      enable_compression=backup_record.compression_enabled,
    )
    success = asyncio.run(
      s3_adapter.delete_backup(
        graph_id=backup_record.graph_id,
        timestamp=backup_record.completed_at or backup_record.created_at,
        backup_type=backup_record.backup_type,
      )
    )

    if success:
      # Mark as expired in database
      backup_record.expire_backup(session)

      result = {
        "backup_id": backup_id,
        "graph_id": backup_record.graph_id,
        "status": "deleted",
        "s3_key": backup_record.s3_key,
        "size_freed_bytes": backup_record.encrypted_size_bytes,
      }

      logger.info(
        f"Delete backup task {task_id} completed successfully for backup '{backup_id}'"
      )
      return result
    else:
      raise RuntimeError(f"Failed to delete backup from S3: {backup_record.s3_key}")

  except Exception as e:
    logger.error(f"Delete backup task {task_id} failed for backup '{backup_id}': {e}")
    raise


@celery_app.task(bind=True, base=CallbackTask, queue=QUEUE_DEFAULT)
def generate_backup_metrics(self, graph_id: Optional[str] = None) -> Dict[str, Any]:
  """
  Generate backup metrics for monitoring and reporting.

  Args:
      graph_id: Specific graph to analyze (optional)

  Returns:
      Dict containing backup metrics
  """
  task_id = self.request.id
  logger.info(f"Starting backup metrics generation task {task_id}")

  try:
    from sqlalchemy import func

    metrics = {
      "task_id": task_id,
      "generated_at": datetime.now(timezone.utc).isoformat(),
      "graphs": {},
      "overall": {},
    }

    if graph_id:
      # Metrics for specific graph
      stats = GraphBackup.get_backup_stats(graph_id, session)
      metrics["graphs"][graph_id] = stats
    else:
      # Metrics for all graphs
      all_backups = session.query(GraphBackup).all()
      unique_graphs = set(backup.graph_id for backup in all_backups)

      for gid in unique_graphs:
        stats = GraphBackup.get_backup_stats(gid, session)
        metrics["graphs"][gid] = stats

    # Overall metrics
    total_backups = session.query(func.count(GraphBackup.id)).scalar()
    successful_backups = (
      session.query(func.count(GraphBackup.id))
      .filter(GraphBackup.status == BackupStatus.COMPLETED.value)
      .scalar()
    )

    total_size = (
      session.query(func.sum(GraphBackup.original_size_bytes))
      .filter(GraphBackup.status == BackupStatus.COMPLETED.value)
      .scalar()
      or 0
    )

    total_compressed_size = (
      session.query(func.sum(GraphBackup.compressed_size_bytes))
      .filter(GraphBackup.status == BackupStatus.COMPLETED.value)
      .scalar()
      or 0
    )

    metrics["overall"] = {
      "total_backups": total_backups,
      "successful_backups": successful_backups,
      "success_rate": successful_backups / total_backups if total_backups > 0 else 0,
      "total_original_size_bytes": total_size,
      "total_compressed_size_bytes": total_compressed_size,
      "total_storage_saved_bytes": total_size - total_compressed_size,
      "average_compression_ratio": (total_size - total_compressed_size) / total_size
      if total_size > 0
      else 0,
    }

    logger.info(f"Backup metrics generation task {task_id} completed")
    return metrics

  except Exception as e:
    logger.error(f"Backup metrics generation task {task_id} failed: {e}")
    raise


# Periodic task schedules (to be added to Celery beat)
BACKUP_SCHEDULES = {
  # Daily backup cleanup at 2 AM
  "backup-cleanup": {
    "task": "robosystems.tasks.backup_tasks.cleanup_expired_backups",
    "schedule": "0 2 * * *",  # Cron: 2:00 AM daily
    "kwargs": {"dry_run": False},
  },
  # Weekly retention management on Sundays at 3 AM
  "backup-retention": {
    "task": "robosystems.tasks.backup_tasks.backup_retention_management",
    "schedule": "0 3 * * 0",  # Cron: 3:00 AM on Sundays
    "kwargs": {"retention_days": 90, "dry_run": False},
  },
  # Health check every 4 hours
  "backup-health-check": {
    "task": "robosystems.tasks.backup_tasks.backup_health_check",
    "schedule": "0 */4 * * *",  # Cron: Every 4 hours
  },
  # Daily metrics generation at 1 AM
  "backup-metrics": {
    "task": "robosystems.tasks.backup_tasks.generate_backup_metrics",
    "schedule": "0 1 * * *",  # Cron: 1:00 AM daily
  },
}


@celery_app.task(bind=True, base=CallbackTask, queue=QUEUE_DEFAULT)
def create_graph_backup(
  self,
  graph_id: str,
  backup_type: str = "full",
  user_id: Optional[str] = None,
  retention_days: int = 90,
  compression: bool = True,
  encryption: bool = True,
  backup_format: str = "full_dump",
  operation_id: str = None,
) -> Dict[str, Any]:
  """
  Create a backup of a graph database with SSE progress tracking.

  This SSE-compatible version emits real-time progress events through
  the unified operation monitoring system.

  Args:
      graph_id: Graph database identifier
      backup_type: 'full' or 'incremental'
      user_id: User requesting the backup (optional)
      retention_days: Number of days to retain backup
      compression: Enable compression
      encryption: Enable encryption
      backup_format: Backup format (csv, json, parquet, full_dump)
      operation_id: SSE operation ID for progress tracking

  Returns:
      Dict containing backup information
  """
  task_id = self.request.id
  logger.info(
    f"Starting SSE backup task {task_id} for graph '{graph_id}', operation_id: {operation_id}"
  )

  # Initialize SSE progress tracker
  from robosystems.middleware.sse.task_progress import TaskSSEProgressTracker

  progress_tracker = TaskSSEProgressTracker(operation_id)

  try:
    # Emit started event
    progress_tracker.emit_progress(
      "Starting backup operation...",
      0,
      {
        "graph_id": graph_id,
        "backup_type": backup_type,
        "backup_format": backup_format,
      },
    )

    # Import at function level to avoid circular imports
    from robosystems.middleware.graph.utils import MultiTenantUtils

    # Validate graph_id
    progress_tracker.emit_progress("Validating graph database...", 10)
    if not MultiTenantUtils.is_shared_repository(graph_id):
      MultiTenantUtils.validate_graph_id(graph_id)

    database_name = MultiTenantUtils.get_database_name(graph_id)

    # Create backup record in PostgreSQL
    progress_tracker.emit_progress("Initializing backup infrastructure...", 20)
    s3_adapter = S3BackupAdapter(
      enable_compression=compression,
    )
    s3_bucket = s3_adapter.bucket_name

    # Generate S3 key path
    timestamp = datetime.now(timezone.utc)
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")

    # Determine base extension based on format
    format_extensions = {
      "csv": ".csv.zip",
      "json": ".json.zip",
      "parquet": ".parquet.zip",
      "full_dump": ".lbug.zip",
    }
    extension = format_extensions.get(backup_format.lower(), ".lbug.zip")

    if compression:
      extension += ".gz"
    if encryption:
      extension += ".enc"

    s3_key = f"graph-backups/databases/{graph_id}/{backup_type}/backup-{timestamp_str}{extension}"

    logger.info(f"Creating backup with timestamp: {timestamp.isoformat()}")
    logger.info(f"Generated S3 key for database record: {s3_key}")

    # Create backup record
    progress_tracker.emit_progress("Creating backup record...", 30)
    backup_record = GraphBackup.create(
      graph_id=graph_id,
      database_name=database_name,
      backup_type=backup_type,
      s3_bucket=s3_bucket,
      s3_key=s3_key,
      session=session,
      created_by_user_id=user_id,
      compression_enabled=compression,
      encryption_enabled=encryption,
      expires_at=datetime.now(timezone.utc) + timedelta(days=retention_days),
    )

    # Start backup process
    backup_record.start_backup(session)

    # Create the actual backup
    progress_tracker.emit_progress(
      "Creating database backup...",
      40,
      {"status": "backing_up", "backup_id": str(backup_record.id)},
    )

    from robosystems.operations.lbug.backup_manager import create_backup_manager

    backup_manager = create_backup_manager()

    # Create the backup with progress tracking
    backup_job = BackupJob(
      graph_id=graph_id,
      backup_type=BackupType(backup_type),
      backup_format=BackupFormat(backup_format),
      retention_days=retention_days,
      compression=compression,
      encryption=encryption,
      allow_export=not encryption,
    )
    backup_info = asyncio.run(backup_manager.create_backup(backup_job))

    # Update backup record's S3 key with the actual key used by S3 adapter
    actual_s3_key = backup_info.s3_key
    logger.info(
      f"SSE backup: Updating s3_key from '{backup_record.s3_key}' to '{actual_s3_key}'"
    )
    backup_record.s3_key = actual_s3_key
    session.commit()
    session.refresh(backup_record)
    logger.info(f"SSE backup: S3 key updated successfully to '{backup_record.s3_key}'")

    # Update backup record with results
    progress_tracker.emit_progress("Finalizing backup metadata...", 85)
    backup_record.complete_backup(
      session=session,
      original_size=backup_info.original_size,
      compressed_size=backup_info.compressed_size,
      encrypted_size=backup_info.compressed_size,
      checksum=backup_info.checksum,
      node_count=backup_info.node_count,
      relationship_count=backup_info.relationship_count,
      backup_duration=backup_info.backup_duration_seconds,
      metadata={
        "backup_format": backup_info.backup_format,
        "compression_ratio": backup_info.compression_ratio,
        "is_encrypted": backup_info.is_encrypted,
        "encryption_method": backup_info.encryption_method,
      },
    )

    # Emit completion
    progress_tracker.emit_progress(
      "Backup completed successfully!",
      100,
      {
        "backup_id": str(backup_record.id),
        "graph_id": graph_id,
        "backup_size": backup_info.compressed_size,
        "compression_ratio": backup_info.compression_ratio,
      },
    )

    # Emit completion event with backup-specific context
    progress_tracker.emit_completion(
      {
        "backup_id": str(backup_record.id),
        "s3_key": actual_s3_key,
        "size_bytes": backup_info.compressed_size,
      },
      additional_context={
        "graph_id": graph_id,
        "backup_type": backup_type,
        "backup_format": backup_format,
        "created_at": timestamp.isoformat(),
      },
    )

    result = {
      "task_id": task_id,
      "backup_id": str(backup_record.id),
      "graph_id": graph_id,
      "backup_type": backup_type,
      "backup_format": backup_format,
      "s3_bucket": s3_bucket,
      "s3_key": actual_s3_key,
      "size_bytes": backup_info.compressed_size,
      "created_at": timestamp.isoformat(),
    }

    logger.info(
      f"SSE backup task {task_id} completed successfully for graph '{graph_id}'"
    )
    return result

  except Exception as e:
    logger.error(f"SSE backup task {task_id} failed for graph '{graph_id}': {e}")

    # Emit error event with backup-specific context
    progress_tracker.emit_error(
      e,
      additional_context={
        "graph_id": graph_id,
        "user_id": user_id,
      },
    )

    raise


@celery_app.task(bind=True, base=CallbackTask, queue=QUEUE_DEFAULT)
def restore_graph_backup_sse(
  self,
  graph_id: str,
  backup_id: str,
  user_id: Optional[str] = None,
  create_system_backup: bool = True,
  verify_after_restore: bool = True,
  operation_id: str = None,
) -> Dict[str, Any]:
  """
  Restore a graph database from backup with SSE progress tracking.

  This SSE-compatible version emits real-time progress events through
  the unified operation monitoring system.

  Args:
      graph_id: Graph database identifier
      backup_id: ID of backup to restore
      user_id: User requesting the restore (optional)
      create_system_backup: Create backup before restore
      verify_after_restore: Verify database after restore
      operation_id: SSE operation ID for progress tracking

  Returns:
      Dict containing restore information
  """
  task_id = self.request.id
  logger.info(
    f"Starting SSE restore task {task_id} for graph '{graph_id}', operation_id: {operation_id}"
  )

  # Initialize SSE progress tracker
  from robosystems.middleware.sse.task_progress import TaskSSEProgressTracker

  progress_tracker = TaskSSEProgressTracker(operation_id)

  try:
    # Emit started event
    progress_tracker.emit_progress(
      "Starting restore operation...", 0, {"graph_id": graph_id, "backup_id": backup_id}
    )

    # Get backup record
    progress_tracker.emit_progress("Retrieving backup information...", 10)
    backup_record = GraphBackup.get_by_id(backup_id, session)
    if not backup_record:
      raise ValueError(f"Backup not found: {backup_id}")

    if backup_record.graph_id != graph_id:
      raise ValueError(f"Backup {backup_id} does not belong to graph {graph_id}")

    # Create system backup if requested
    if create_system_backup:
      progress_tracker.emit_progress(
        "Creating system backup before restore...",
        20,
        {"status": "creating_system_backup"},
      )

      try:
        from robosystems.operations.lbug.backup_manager import create_backup_manager

        backup_manager = create_backup_manager()

        system_backup_job = BackupJob(
          graph_id=graph_id,
          backup_type=BackupType.FULL,
          backup_format=BackupFormat.FULL_DUMP,
          encryption=True,
          allow_export=False,
        )
        system_backup = asyncio.run(backup_manager.create_backup(system_backup_job))

        progress_tracker.emit_progress(
          "System backup created",
          30,
          {"system_backup_s3_key": system_backup.s3_key},
        )
      except Exception as e:
        logger.warning(f"Failed to create system backup: {e}")
        progress_tracker.emit_progress(
          "Warning: System backup failed, continuing with restore...", 30
        )

    # Perform the restore using Graph API with SSE monitoring
    progress_tracker.emit_progress("Initiating restore via Graph API...", 40)

    # Create Graph API client
    client = asyncio.run(
      GraphClientFactory.create_client(graph_id, operation_type="write")
    )

    try:
      progress_tracker.emit_progress(
        "Starting restore and monitoring progress...", 50, {"status": "restoring"}
      )

      # Call Graph API to restore and monitor via SSE
      logger.info(
        f"Starting restore from S3: {backup_record.s3_bucket}/{backup_record.s3_key}"
      )
      restore_result = asyncio.run(
        client.restore_with_sse(
          graph_id=graph_id,
          s3_bucket=backup_record.s3_bucket,
          s3_key=backup_record.s3_key,
          create_system_backup=create_system_backup,
          force_overwrite=True,
          encrypted=backup_record.encryption_enabled,
          compressed=backup_record.compression_enabled,
          timeout=3600,  # 1 hour timeout
        )
      )

      # Check result status
      if restore_result.get("status") == "completed":
        logger.info("Restore completed successfully")
        progress_tracker.emit_progress("Database restored successfully", 80)
        verification_status = "verified"
      elif restore_result.get("status") == "failed":
        error_msg = restore_result.get("error", "Unknown error")
        logger.error(f"Restore failed: {error_msg}")
        raise RuntimeError(f"Restore failed: {error_msg}")
      else:
        logger.warning(f"Unexpected restore status: {restore_result.get('status')}")
        verification_status = "not_verified"

    finally:
      # Try to close client, but don't fail if event loop is already closed
      try:
        asyncio.run(client.close())
      except RuntimeError as e:
        logger.debug(f"Event loop already closed during client cleanup: {e}")

    # Update backup record
    if hasattr(backup_record, "last_restored_at"):
      backup_record.last_restored_at = datetime.now(timezone.utc)
      session.commit()

    # Emit completion
    progress_tracker.emit_progress(
      "Restore completed successfully!",
      100,
      {
        "graph_id": graph_id,
        "backup_id": backup_id,
        "verification_status": verification_status,
      },
    )

    # Emit completion event with restore-specific context
    progress_tracker.emit_completion(
      {
        "verification_status": verification_status,
        "system_backup_created": create_system_backup,
        "restored_at": datetime.now(timezone.utc).isoformat(),
      },
      additional_context={
        "graph_id": graph_id,
        "backup_id": backup_id,
      },
    )

    result = {
      "task_id": task_id,
      "graph_id": graph_id,
      "backup_id": backup_id,
      "status": "completed",
      "verification_status": verification_status,
      "system_backup_created": create_system_backup,
      "restored_at": datetime.now(timezone.utc).isoformat(),
    }

    logger.info(
      f"SSE restore task {task_id} completed successfully for graph '{graph_id}'"
    )
    return result

  except Exception as e:
    logger.error(f"SSE restore task {task_id} failed for graph '{graph_id}': {e}")

    # Emit error event with restore-specific context
    progress_tracker.emit_error(
      e,
      additional_context={
        "graph_id": graph_id,
        "backup_id": backup_id,
        "user_id": user_id,
      },
    )

    raise
