"""SEC Backup Asset.

This asset creates downloadable .lbug backups of the SEC database for users
with repository subscriptions. It runs after SEC materialization and creates
unencrypted, compressed backups that can be downloaded.

The asset:
1. Creates a GraphBackup record in PostgreSQL
2. Uses Graph Client Factory to call the backup endpoint on the shared master
3. The Graph API creates a tar.gz backup and uploads to S3 on-instance
4. Updates the GraphBackup record with results

NOTE: Previous implementation used BackupManager which downloaded the entire
85GB database over HTTP, held it in memory, then re-uploaded to S3.
Now uses on-instance backup via Graph API (CHECKPOINT + tar.gz + S3 upload).
"""

import asyncio
from datetime import UTC, datetime, timedelta

from dagster import (
  AssetExecutionContext,
  MaterializeResult,
  asset,
)

from robosystems.config.constants import TASK_TIME_LIMIT
from robosystems.config.storage.graph import get_shared_repo_backup_key
from robosystems.dagster.resources import DatabaseResource, S3Resource

from .configs import SECBackupConfig


@asset(
  group_name="sec_pipeline",
  description="Create downloadable SEC database backup",
  kinds={"backup"},
  deps=["sec_graph_materialized"],
  metadata={
    "pipeline": "sec",
    "graph_id": "sec",
    "stage": "backup",
    "mode": "full",
  },
)
def sec_backup(
  context: AssetExecutionContext,
  config: SECBackupConfig,
  db: DatabaseResource,
  s3: S3Resource,
) -> MaterializeResult:
  """Create a downloadable backup of the SEC database.

  Uses Graph Client Factory to call the backup endpoint on the shared master.
  The backup runs entirely on-instance (CHECKPOINT + tar.gz + S3 upload),
  so this Dagster task only orchestrates and monitors via SSE.

  Returns:
      MaterializeResult with backup statistics
  """
  from robosystems.config import env
  from robosystems.middleware.graph.utils import MultiTenantUtils
  from robosystems.models.iam import GraphBackup
  from robosystems.operations.aws.s3 import S3BackupAdapter

  context.log.info(
    f"Creating downloadable backup for SEC repository: {config.graph_id}"
  )

  # Validate graph_id is a shared repository
  if not MultiTenantUtils.is_shared_repository(config.graph_id):
    context.log.warning(f"{config.graph_id} is not a shared repository")

  database_name = MultiTenantUtils.get_database_name(config.graph_id)

  # Generate S3 key with timestamp
  timestamp = datetime.now(UTC)
  s3_key = get_shared_repo_backup_key(config.graph_id, timestamp)

  # Create backup record in database
  s3_adapter = S3BackupAdapter(enable_compression=config.compression)

  with db.get_session() as session:
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

  # Skip actual backup in dev environment
  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping backup in dev environment")
    return MaterializeResult(
      metadata={
        "backup_id": backup_id,
        "status": "skipped",
        "reason": "dev_environment",
      }
    )

  # Use Graph Client Factory to trigger on-instance backup
  import boto3

  from robosystems.graph_api.client.factory import get_graph_client_for_sec_ingestion

  sts = boto3.client("sts", region_name=env.AWS_REGION)
  account_id = sts.get_caller_identity()["Account"]
  bucket = s3_adapter.bucket_name or f"robosystems-{account_id}-user-{env.ENVIRONMENT}"

  loop = asyncio.new_event_loop()
  client = None
  try:
    client = loop.run_until_complete(get_graph_client_for_sec_ingestion())

    context.log.info("Calling backup endpoint on shared master...")
    result = loop.run_until_complete(
      client.backup_with_sse(
        graph_id=config.graph_id,
        backup_type="shared_repository",
        s3_destination={"bucket": bucket, "key": s3_key},
        compression=True,
        checkpoint=True,
        timeout=TASK_TIME_LIMIT,
      )
    )
  finally:
    if client:
      loop.run_until_complete(client.close())
    loop.close()

  if result.get("status") != "completed":
    error = result.get("error", "Unknown error")
    # Mark backup record as failed
    with db.get_session() as session:
      backup_record = GraphBackup.get_by_id(backup_id, session)
      if backup_record:
        backup_record.fail_backup(session, error_message=error)
    raise RuntimeError(f"SEC backup failed: {error}")

  # Extract result metadata from completed task
  task_result = result.get("result", {})
  original_size = task_result.get("original_size_bytes", 0)
  compressed_size = task_result.get("compressed_size_bytes", 0)
  compression_ratio = task_result.get("compression_ratio", 0)
  checksum = task_result.get("checksum", "")
  duration = task_result.get("duration_seconds", 0)

  # Update backup record with results
  with db.get_session() as session:
    backup_record = GraphBackup.get_by_id(backup_id, session)
    if backup_record:
      backup_record.s3_key = task_result.get("s3_key", s3_key)
      backup_record.complete_backup(
        session=session,
        original_size=original_size,
        compressed_size=compressed_size,
        encrypted_size=compressed_size,
        checksum=checksum,
        node_count=0,
        relationship_count=0,
        backup_duration=duration,
        metadata={
          "backup_format": "tar.gz",
          "compression_ratio": compression_ratio,
          "is_encrypted": False,
          "created_by": "sec_backup_asset",
          "dagster_run_id": context.run_id,
        },
      )

  context.log.info(
    f"SEC backup completed: {compressed_size} bytes, ratio: {compression_ratio:.2f}"
  )

  return MaterializeResult(
    metadata={
      "backup_id": backup_id,
      "graph_id": config.graph_id,
      "s3_key": task_result.get("s3_key", s3_key),
      "size_bytes": compressed_size,
      "compression_ratio": compression_ratio,
      "created_at": timestamp.isoformat(),
      "expires_at": (timestamp + timedelta(days=config.retention_days)).isoformat(),
    }
  )
