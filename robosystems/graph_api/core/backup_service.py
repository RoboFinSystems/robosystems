"""
On-instance backup service for Graph API.

This service executes backup operations directly on the graph instance,
uploading results to S3 via multipart upload with progress tracking.
All heavy work (CHECKPOINT, compression, upload) happens on-instance,
avoiding the need to transfer large databases over HTTP.

Supports four backup types:
- replica: Raw .lbug upload to S3 (downloaded by replica fleet at startup)
- duckdb_staging: Raw .duckdb upload to S3 (for local dev / analytics)
- r2_download: zstd-compressed .lbug.zst upload to Cloudflare R2 (zero-egress subscriber downloads)
- standard: ZIP + optional encrypt to S3 (existing user backup flow)
"""

import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
from boto3.s3.transfer import TransferConfig

from robosystems.config import env
from robosystems.graph_api.core.task_manager import GenericTaskManager
from robosystems.logger import logger

# Multipart upload config: 100MB chunks for large database files
S3_MULTIPART_CHUNKSIZE = 100 * 1024 * 1024  # 100 MB
S3_MULTIPART_THRESHOLD = 100 * 1024 * 1024  # 100 MB
S3_MAX_CONCURRENCY = 4


class OnInstanceBackupService:
  """Run backups inside the Graph API process, where the files are.

  Having direct access to the ``.lbug`` files and the connection pool is the
  point: CHECKPOINT costs no HTTP round trip, and the database never crosses
  the network on its way to S3.
  """

  def __init__(
    self,
    db_manager,
    task_manager: GenericTaskManager,
    duckdb_pool=None,
  ):
    self.db_manager = db_manager
    self.task_manager = task_manager
    self.duckdb_pool = duckdb_pool

  async def execute_backup(
    self,
    task_id: str,
    graph_id: str,
    backup_type: str,
    s3_destination: dict[str, str],
    compression: bool = True,
    encryption: bool = False,
    checkpoint: bool = True,
    vacuum: bool = False,
  ) -> dict[str, Any]:
    """Back up a database on-instance and upload it, reporting progress.

    ``backup_type`` selects the handler: ``replica`` and ``duckdb_staging``
    upload the raw file to S3, ``r2_download`` compresses with zstd and
    uploads to R2. ``s3_destination`` carries ``bucket`` and ``key``.

    ``checkpoint`` flushes the WAL first, so the uploaded file is consistent
    on its own; ``vacuum`` compacts beforehand and applies to DuckDB only.
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

      # VACUUM runs before CHECKPOINT so the checkpoint reflects the
      # compacted file.
      is_duckdb = backup_type == "duckdb_staging"
      if vacuum and is_duckdb:
        logger.info(f"[Task {task_id}] Running VACUUM on {graph_id}")
        await self.task_manager.update_task(
          task_id, progress_percent=2, metadata={"stage": "vacuum"}
        )
        self._duckdb_vacuum(graph_id)
        logger.info(f"[Task {task_id}] VACUUM completed")

      if checkpoint:
        logger.info(f"[Task {task_id}] Running CHECKPOINT on {graph_id}")
        await self.task_manager.update_task(
          task_id, progress_percent=5, metadata={"stage": "checkpoint"}
        )
        if is_duckdb:
          self._duckdb_checkpoint(graph_id)
        else:
          self._checkpoint(graph_id)
        logger.info(f"[Task {task_id}] CHECKPOINT completed")

      if is_duckdb:
        db_path = self._resolve_duckdb_path(graph_id)
      else:
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

      bucket = s3_destination["bucket"]
      key = s3_destination["key"]

      if backup_type in ("replica", "duckdb_staging"):
        result = self._upload_replica(
          db_path, bucket, key, task_id, db_size, backup_type=backup_type
        )
      elif backup_type == "r2_download":
        result = self._compress_and_upload_replica(
          db_path,
          bucket,
          key,
          task_id,
          db_size,
          s3_client=self._get_r2_client(),
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
    """CHECKPOINT via LadybugDB connection pool (no HTTP roundtrip)."""
    with self.db_manager.get_connection(graph_id, read_only=False) as conn:
      conn.execute("CHECKPOINT")

  def _duckdb_checkpoint(self, graph_id: str) -> None:
    """CHECKPOINT via DuckDB connection pool."""
    if self.duckdb_pool is None:
      raise RuntimeError("DuckDB pool not provided for duckdb_staging backup")
    with self.duckdb_pool.get_connection(graph_id) as conn:
      conn.execute("CHECKPOINT")

  def _duckdb_vacuum(self, graph_id: str) -> None:
    """VACUUM via DuckDB connection pool to compact the database file."""
    if self.duckdb_pool is None:
      raise RuntimeError("DuckDB pool not provided for duckdb_staging vacuum")
    with self.duckdb_pool.get_connection(graph_id) as conn:
      conn.execute("VACUUM")

  def _resolve_db_path(self, graph_id: str) -> Path:
    """Get local .lbug path for a database."""
    from robosystems.operations.graph.engine.path_utils import get_lbug_database_path

    return get_lbug_database_path(graph_id)

  def _resolve_duckdb_path(self, graph_id: str) -> Path:
    """Get local .duckdb path for a staging database."""
    from robosystems.config.storage.shared import get_staging_duckdb_path

    return Path(get_staging_duckdb_path(graph_id))

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

  def _get_r2_client(self):
    """Create Cloudflare R2 client (S3-compatible) for zero-egress downloads."""
    r2_config = env.get_r2_config()
    if not r2_config:
      raise RuntimeError("R2 not configured — set R2_ENDPOINT_URL and R2 credentials")
    return boto3.client("s3", **r2_config)

  @staticmethod
  def _cleanup_stale_temp_dirs(parent_dir: Path, max_age_hours: int = 24) -> None:
    """Remove temp directories older than max_age_hours from a previous crash."""
    import shutil
    import time

    cutoff = time.time() - (max_age_hours * 3600)
    for entry in parent_dir.iterdir():
      if entry.is_dir():
        try:
          if entry.stat().st_mtime < cutoff:
            logger.info(f"Removing stale backup temp dir: {entry}")
            shutil.rmtree(entry)
        except OSError:
          pass

  def _upload_replica(
    self,
    db_path: Path,
    bucket: str,
    key: str,
    task_id: str,
    db_size: int,
    backup_type: str = "replica",
    s3_client=None,
  ) -> dict[str, Any]:
    """Raw database upload to S3 or R2 (no compression).

    Used for .lbug replica uploads, .duckdb staging uploads, and R2 downloads.
    """
    logger.info(f"[Task {task_id}] Uploading {db_path.name} to s3://{bucket}/{key}")

    if s3_client is None:
      s3_client = self._get_s3_client()
    transfer_config = TransferConfig(
      multipart_chunksize=S3_MULTIPART_CHUNKSIZE,
      multipart_threshold=S3_MULTIPART_THRESHOLD,
      max_concurrency=S3_MAX_CONCURRENCY,
    )

    uploaded_bytes = 0

    def progress_callback(bytes_transferred):
      nonlocal uploaded_bytes
      uploaded_bytes += bytes_transferred
      percent = min(10 + int((uploaded_bytes / max(db_size, 1)) * 85), 95)
      # boto3 calls this synchronously, so progress can only be logged here,
      # not awaited onto the task manager.
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
          "backup_type": backup_type,
          "created_at": datetime.now(UTC).isoformat(),
          "original_size": str(db_size),
        },
      },
    )

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

  def _compress_and_upload_replica(
    self,
    db_path: Path,
    bucket: str,
    key: str,
    task_id: str,
    db_size: int,
    s3_client=None,
  ) -> dict[str, Any]:
    """Compress database with zstd and upload to R2.

    Uses the zstd binary bundled in the graph_api container image with
    multithreading for maximum throughput on ARM64 (r7g) instances.
    Temp file is written to EBS-backed directory, not /tmp (RAM-backed).

    Level 15 with --long (128MB window) balances egress savings against
    compress wall-time. The export instance is spun up per-publish and
    billed for the compression, so higher levels (19+) roughly break even
    at this file's low (~2.2x) compressibility -- the extra instance-time
    cancels the egress they save. The window stays at 128MB so subscribers
    decompress with a plain `zstd -d`; a larger --long window would force a
    matching `--long` flag on their side for an unmeasured ratio gain.
    """
    logger.info(f"[Task {task_id}] Compressing {db_path.name} with zstd before upload")

    if s3_client is None:
      s3_client = self._get_r2_client()

    # EBS-backed, not /tmp: /tmp is tmpfs (RAM) and too small to hold a
    # compressed multi-GB database.
    ebs_temp_dir = Path(env.LBUG_DATABASE_PATH).parent / "backup-tmp"
    ebs_temp_dir.mkdir(parents=True, exist_ok=True)
    self._cleanup_stale_temp_dirs(ebs_temp_dir, max_age_hours=24)

    with tempfile.TemporaryDirectory(dir=ebs_temp_dir) as temp_dir:
      temp_path = Path(temp_dir)
      compressed_file = temp_path / f"{db_path.stem}.lbug.zst"

      # zstd -T0 (all cores), --long (128MB window), -15 (see docstring for level rationale)
      compress_start = datetime.now(UTC)
      try:
        subprocess.run(
          ["zstd", "-T0", "--long", "-15", str(db_path), "-o", str(compressed_file)],
          check=True,
          capture_output=True,
          text=True,
        )
      except subprocess.CalledProcessError as e:
        logger.error(f"[Task {task_id}] zstd compression failed: {e.stderr}")
        raise
      compress_time = (datetime.now(UTC) - compress_start).total_seconds()

      compressed_size = compressed_file.stat().st_size
      compression_ratio = compressed_size / max(db_size, 1)

      logger.info(
        f"[Task {task_id}] zstd compression complete: "
        f"{db_size / (1024**3):.2f}GB -> {compressed_size / (1024**3):.2f}GB "
        f"({compression_ratio:.1%}) in {compress_time:.1f}s"
      )

      transfer_config = TransferConfig(
        multipart_chunksize=S3_MULTIPART_CHUNKSIZE,
        multipart_threshold=S3_MULTIPART_THRESHOLD,
        max_concurrency=S3_MAX_CONCURRENCY,
      )

      uploaded_bytes = 0

      def progress_callback(bytes_transferred):
        nonlocal uploaded_bytes
        uploaded_bytes += bytes_transferred
        percent = min(10 + int((uploaded_bytes / max(compressed_size, 1)) * 85), 95)
        logger.debug(
          f"[Task {task_id}] Upload progress: {uploaded_bytes / (1024**3):.2f} GB "
          f"({percent}%)"
        )

      s3_client.upload_file(
        str(compressed_file),
        bucket,
        key,
        Config=transfer_config,
        Callback=progress_callback,
        ExtraArgs={
          "Metadata": {
            "backup_type": "r2_download",
            "created_at": datetime.now(UTC).isoformat(),
            "original_size": str(db_size),
            "compression": "zstd",
            "compression_level": "12",
          },
        },
      )

    # Verify upload (after temp dir cleanup)
    head = s3_client.head_object(Bucket=bucket, Key=key)
    uploaded_size = head["ContentLength"]

    return {
      "status": "success",
      "s3_uri": f"s3://{bucket}/{key}",
      "s3_bucket": bucket,
      "s3_key": key,
      "original_size_bytes": db_size,
      "compressed_size_bytes": uploaded_size,
      "compression_ratio": round(compression_ratio, 3),
      "compress_time_seconds": round(compress_time, 1),
      "uploaded_at": datetime.now(UTC).isoformat(),
    }
