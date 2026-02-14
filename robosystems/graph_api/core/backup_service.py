"""
On-instance backup service for Graph API.

This service executes backup operations directly on the graph instance,
uploading results to S3 via multipart upload with progress tracking.
All heavy work (CHECKPOINT, compression, upload) happens on-instance,
avoiding the need to transfer large databases over HTTP.

Supports three backup types:
- replica: Raw .lbug upload to S3 (downloaded by replica fleet at startup)
- shared_repository: Compressed tar.gz to S3 (for subscriber downloads)
- standard: ZIP + optional encrypt to S3 (existing user backup flow)
"""

import hashlib
import json
import tarfile
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from robosystems.config import env
from robosystems.graph_api.core.task_manager import GenericTaskManager
from robosystems.logger import logger

# Multipart upload config: 100MB chunks for large database files
S3_MULTIPART_CHUNKSIZE = 100 * 1024 * 1024  # 100 MB
S3_MULTIPART_THRESHOLD = 100 * 1024 * 1024  # 100 MB
S3_MAX_CONCURRENCY = 4

# Shared repository manifest prefix
SHARED_REPLICA_PREFIX = "shared-replica-data"


class OnInstanceBackupService:
  """Execute backup operations directly on the graph instance.

  This service runs within the Graph API process and has direct access to:
  - Local database files (.lbug)
  - LadybugDB connection pool for CHECKPOINT
  - boto3 for S3 multipart upload with progress
  """

  def __init__(
    self,
    db_manager,
    task_manager: GenericTaskManager,
  ):
    self.db_manager = db_manager
    self.task_manager = task_manager

  async def execute_backup(
    self,
    task_id: str,
    graph_id: str,
    backup_type: str,
    s3_destination: dict[str, str],
    compression: bool = True,
    encryption: bool = False,
    checkpoint: bool = True,
  ) -> dict[str, Any]:
    """Execute backup entirely on-instance, upload to S3.

    Args:
        task_id: Background task ID for progress tracking
        graph_id: Database identifier
        backup_type: "replica", "shared_repository", or "standard"
        s3_destination: Dict with "bucket" and "key"
        compression: Enable compression (for shared_repository/standard)
        encryption: Enable encryption (for standard only)
        checkpoint: Run CHECKPOINT before backup

    Returns:
        Dict with backup result metadata
    """
    start_time = datetime.now(UTC)

    try:
      await self.task_manager.update_task(
        task_id,
        status="running",
        metadata={
          "started_at": start_time.isoformat(),
          "backup_type": backup_type,
          "graph_id": graph_id,
        },
      )

      # Step 1: CHECKPOINT if requested
      if checkpoint:
        logger.info(f"[Task {task_id}] Running CHECKPOINT on {graph_id}")
        await self.task_manager.update_task(
          task_id, progress_percent=5, metadata={"stage": "checkpoint"}
        )
        self._checkpoint(graph_id)
        logger.info(f"[Task {task_id}] CHECKPOINT completed")

      # Step 2: Resolve database path
      db_path = self._resolve_db_path(graph_id)
      if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

      db_size = db_path.stat().st_size
      logger.info(f"[Task {task_id}] Database size: {db_size / (1024**3):.2f} GB")

      await self.task_manager.update_task(
        task_id,
        progress_percent=10,
        metadata={"stage": "uploading", "db_size_bytes": db_size},
      )

      # Step 3: Route to type-specific handler
      bucket = s3_destination["bucket"]
      key = s3_destination["key"]

      if backup_type == "replica":
        result = self._upload_replica(db_path, bucket, key, task_id, db_size)
      elif backup_type == "shared_repository":
        result = self._upload_shared_repository(
          db_path, bucket, key, task_id, graph_id, db_size
        )
      else:
        raise ValueError(f"Unsupported on-instance backup type: {backup_type}")

      duration = (datetime.now(UTC) - start_time).total_seconds()
      result["duration_seconds"] = round(duration, 1)
      result["graph_id"] = graph_id
      result["backup_type"] = backup_type

      logger.info(
        f"[Task {task_id}] Backup completed in {duration:.1f}s: s3://{bucket}/{key}"
      )

      return result

    except Exception as e:
      logger.error(f"[Task {task_id}] Backup failed: {e}")
      await self.task_manager.fail_task(task_id, str(e))
      raise

  def _checkpoint(self, graph_id: str) -> None:
    """CHECKPOINT via connection pool (no HTTP roundtrip)."""
    with self.db_manager.get_connection(graph_id, read_only=False) as conn:
      conn.execute("CHECKPOINT")

  def _resolve_db_path(self, graph_id: str) -> Path:
    """Get local .lbug path for a database."""
    from robosystems.operations.lbug.path_utils import get_lbug_database_path

    return get_lbug_database_path(graph_id)

  def _get_s3_client(self):
    """Create S3 client using instance credentials."""
    s3_config = env.get_s3_config()
    kwargs = {"region_name": s3_config.get("region_name")}
    if s3_config.get("endpoint_url"):
      kwargs["endpoint_url"] = s3_config["endpoint_url"]
    if s3_config.get("aws_access_key_id"):
      kwargs["aws_access_key_id"] = s3_config["aws_access_key_id"]
      kwargs["aws_secret_access_key"] = s3_config.get("aws_secret_access_key")
    return boto3.client("s3", **kwargs)

  def _upload_replica(
    self,
    db_path: Path,
    bucket: str,
    key: str,
    task_id: str,
    db_size: int,
  ) -> dict[str, Any]:
    """Raw .lbug upload to S3 (no compression) for replica fleet download."""
    logger.info(f"[Task {task_id}] Uploading raw .lbug to s3://{bucket}/{key}")

    s3_client = self._get_s3_client()
    transfer_config = TransferConfig(
      multipart_chunksize=S3_MULTIPART_CHUNKSIZE,
      multipart_threshold=S3_MULTIPART_THRESHOLD,
      max_concurrency=S3_MAX_CONCURRENCY,
    )

    # Upload with progress tracking
    uploaded_bytes = 0

    def progress_callback(bytes_transferred):
      nonlocal uploaded_bytes
      uploaded_bytes += bytes_transferred
      percent = min(10 + int((uploaded_bytes / max(db_size, 1)) * 85), 95)
      # Fire-and-forget progress update (sync context)
      logger.debug(
        f"[Task {task_id}] Upload progress: {uploaded_bytes / (1024**3):.2f} GB "
        f"({percent}%)"
      )

    s3_client.upload_file(
      str(db_path),
      bucket,
      key,
      Config=transfer_config,
      Callback=progress_callback,
      ExtraArgs={
        "Metadata": {
          "backup_type": "replica",
          "created_at": datetime.now(UTC).isoformat(),
          "original_size": str(db_size),
        },
      },
    )

    # Verify upload
    head = s3_client.head_object(Bucket=bucket, Key=key)
    uploaded_size = head["ContentLength"]

    return {
      "status": "success",
      "s3_uri": f"s3://{bucket}/{key}",
      "s3_bucket": bucket,
      "s3_key": key,
      "original_size_bytes": db_size,
      "uploaded_size_bytes": uploaded_size,
      "uploaded_at": datetime.now(UTC).isoformat(),
    }

  def _upload_shared_repository(
    self,
    db_path: Path,
    bucket: str,
    key: str,
    task_id: str,
    graph_id: str,
    db_size: int,
  ) -> dict[str, Any]:
    """Create tar.gz in temp dir and upload to S3 for subscriber downloads."""
    logger.info(f"[Task {task_id}] Creating tar.gz backup for shared repository")

    s3_client = self._get_s3_client()

    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir)
      backup_file = temp_path / f"{graph_id}.tar.gz"

      # Stream compress the database
      logger.info(f"[Task {task_id}] Compressing database...")
      compress_start = datetime.now(UTC)
      with tarfile.open(backup_file, "w:gz", compresslevel=6) as tar:
        tar.add(db_path, arcname=db_path.name)
      compress_time = (datetime.now(UTC) - compress_start).total_seconds()

      compressed_size = backup_file.stat().st_size
      compression_ratio = compressed_size / max(db_size, 1)

      logger.info(
        f"[Task {task_id}] Compression complete: "
        f"{db_size / (1024**3):.2f}GB -> {compressed_size / (1024**3):.2f}GB "
        f"({compression_ratio:.1%}) in {compress_time:.1f}s"
      )

      # Calculate checksum
      sha256 = hashlib.sha256()
      with open(backup_file, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
          sha256.update(chunk)
      checksum = sha256.hexdigest()

      # Upload compressed backup to S3
      logger.info(f"[Task {task_id}] Uploading to s3://{bucket}/{key}")
      transfer_config = TransferConfig(
        multipart_chunksize=S3_MULTIPART_CHUNKSIZE,
        multipart_threshold=S3_MULTIPART_THRESHOLD,
        max_concurrency=S3_MAX_CONCURRENCY,
      )

      s3_client.upload_file(
        str(backup_file),
        bucket,
        key,
        Config=transfer_config,
        ExtraArgs={
          "Metadata": {
            "backup_type": "shared_repository",
            "checksum": checksum,
            "created_at": datetime.now(UTC).isoformat(),
            "original_size": str(db_size),
          },
          "StorageClass": "STANDARD",
        },
      )

      # Write latest.json manifest
      manifest_key = f"{SHARED_REPLICA_PREFIX}/{graph_id}/latest.json"
      manifest = {
        "repo_name": graph_id,
        "s3_bucket": bucket,
        "s3_key": key,
        "timestamp": datetime.now(UTC).isoformat(),
        "checksum": checksum,
        "original_size_bytes": db_size,
        "compressed_size_bytes": compressed_size,
        "compression_ratio": round(compression_ratio, 3),
        "environment": env.ENVIRONMENT,
      }

      try:
        s3_client.put_object(
          Bucket=bucket,
          Key=manifest_key,
          Body=json.dumps(manifest, indent=2),
          ContentType="application/json",
        )
        logger.info(f"[Task {task_id}] Updated manifest: s3://{bucket}/{manifest_key}")
      except ClientError as e:
        logger.warning(f"[Task {task_id}] Failed to update manifest: {e}")

    return {
      "status": "success",
      "s3_uri": f"s3://{bucket}/{key}",
      "s3_bucket": bucket,
      "s3_key": key,
      "original_size_bytes": db_size,
      "compressed_size_bytes": compressed_size,
      "compression_ratio": round(compression_ratio, 3),
      "checksum": checksum,
      "compress_time_seconds": round(compress_time, 1),
      "uploaded_at": datetime.now(UTC).isoformat(),
    }
