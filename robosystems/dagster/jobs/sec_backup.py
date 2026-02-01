"""Dagster job for SEC repository backup generation.

This job creates downloadable .lbug backups of the SEC database for users
with repository subscriptions. It runs after SEC materialization and creates
unencrypted, compressed backups that can be downloaded.

Usage:
  just dagster-run sec_backup_job

The job:
1. Creates a BackupJob with encryption=False, allow_export=True
2. Uses BackupManager to create the backup
3. Stores in S3 with 14-day retention
4. Creates GraphBackup record with graph_id="sec"
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from dagster import (
  Config,
  OpExecutionContext,
  job,
  op,
)

from robosystems.dagster.resources import DatabaseResource, S3Resource


class SECBackupConfig(Config):
  """Configuration for SEC backup generation."""

  graph_id: str = "sec"
  retention_days: int = 14
  compression: bool = True
  encryption: bool = False  # Unencrypted for downloads
  backup_type: str = "full"
  backup_format: str = "full_dump"


@op
def create_sec_backup(
  context: OpExecutionContext,
  db: DatabaseResource,
  s3: S3Resource,
  config: SECBackupConfig,
) -> dict[str, Any]:
  """Create a downloadable backup of the SEC database.

  This creates an unencrypted, compressed .lbug backup file that users
  with SEC repository subscriptions can download.

  The backup is stored in S3 and tracked in the GraphBackup table.
  """
  import asyncio

  from robosystems.middleware.graph.utils import MultiTenantUtils
  from robosystems.models.iam import GraphBackup
  from robosystems.operations.aws.s3 import S3BackupAdapter
  from robosystems.operations.lbug.backup_manager import (
    BackupFormat,
    BackupJob,
    BackupType,
    create_backup_manager,
  )

  context.log.info("Creating downloadable backup for SEC repository")

  # Validate graph_id is a shared repository
  if not MultiTenantUtils.is_shared_repository(config.graph_id):
    context.log.warning(f"{config.graph_id} is not a shared repository")

  database_name = MultiTenantUtils.get_database_name(config.graph_id)

  # Generate S3 key with timestamp
  timestamp = datetime.now(UTC)
  timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")

  extension = ".lbug.zip"
  if config.compression:
    extension += ".gz"

  s3_key = (
    f"shared-repository-backups/{config.graph_id}/backup-{timestamp_str}{extension}"
  )

  # Create backup record in database
  with db.get_session() as session:
    s3_adapter = S3BackupAdapter(enable_compression=config.compression)

    backup_record = GraphBackup.create(
      graph_id=config.graph_id,
      database_name=database_name,
      backup_type=config.backup_type,
      s3_bucket=s3_adapter.bucket_name,
      s3_key=s3_key,
      session=session,
      created_by_user_id=None,  # System-generated backup
      compression_enabled=config.compression,
      encryption_enabled=config.encryption,
      expires_at=datetime.now(UTC) + timedelta(days=config.retention_days),
    )

    backup_record.start_backup(session)
    backup_id = str(backup_record.id)

  context.log.info(f"Created backup record {backup_id}, starting backup...")

  # Create the actual backup using BackupManager
  backup_manager = create_backup_manager()

  backup_job = BackupJob(
    graph_id=config.graph_id,
    backup_type=BackupType(config.backup_type),
    backup_format=BackupFormat(config.backup_format),
    retention_days=config.retention_days,
    compression=config.compression,
    encryption=config.encryption,
    allow_export=True,  # Enable downloads
  )

  loop = asyncio.new_event_loop()
  try:
    backup_info = loop.run_until_complete(backup_manager.create_backup(backup_job))
  finally:
    loop.close()

  # Update backup record with results
  with db.get_session() as session:
    backup_record = GraphBackup.get_by_id(backup_id, session)
    if backup_record:
      backup_record.s3_key = backup_info.s3_key
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
          "created_by": "sec_backup_job",
          "dagster_run_id": context.run_id,
        },
      )

  context.log.info(
    f"SEC backup completed: {backup_info.compressed_size} bytes, "
    f"ratio: {backup_info.compression_ratio:.2f}"
  )

  return {
    "backup_id": backup_id,
    "graph_id": config.graph_id,
    "s3_key": backup_info.s3_key,
    "size_bytes": backup_info.compressed_size,
    "compression_ratio": backup_info.compression_ratio,
    "node_count": backup_info.node_count,
    "relationship_count": backup_info.relationship_count,
    "created_at": timestamp.isoformat(),
    "expires_at": (timestamp + timedelta(days=config.retention_days)).isoformat(),
  }


@job(
  name="sec_backup",
  description="Create downloadable backup of SEC database for user downloads.",
  tags={
    "pipeline": "sec",
    "phase": "backup",
    "dagster/priority": "-1",
    "dagster/max_retries": 3,
    # Standard profile - backup is I/O bound
    "ecs/cpu": "2048",
    "ecs/memory": "8192",
    "ecs/ephemeral_storage": "50",
    # Use on-demand to avoid Spot interruptions
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)
def sec_backup_job():
  """Create downloadable backup of SEC database.

  This job generates a .lbug backup file that users with SEC repository
  subscriptions can download. The backup is:
  - Unencrypted (allow_export=True)
  - Compressed
  - Stored with 14-day retention

  Run this job after sec_materialize_job to provide updated backups
  to users.
  """
  create_sec_backup()
