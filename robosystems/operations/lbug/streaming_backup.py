"""
Shared Repository Backup Service

This service handles streaming backups of shared repositories (SEC, industry, etc.)
to S3 for distribution to read replicas. Unlike the customer backup service,
this is designed for large databases (84GB+) and prioritizes replica availability.

Key features:
- Streaming tar.gz compression (no memory loading)
- No size limits - handles databases of any size
- Manifest file for replica discovery
- Optimized for replica boot time
"""

import hashlib
import json
import tarfile
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

from ...config import env
from ...logger import logger

# S3 path structure for shared replica data
# s3://{bucket}/shared-replica-data/{repo_name}/
#   - {repo_name}-{timestamp}.tar.gz  (the backup)
#   - latest.json                      (manifest pointing to current backup)
SHARED_REPLICA_PREFIX = "shared-replica-data"
DEFAULT_COMPRESSION_LEVEL = 6


class SharedRepositoryBackupError(Exception):
  """Custom exception for shared repository backup operations."""

  pass


class SharedRepositoryBackupService:
  """
  Service for creating streaming backups of shared repositories.

  This service runs on the shared master instance and creates compressed
  backups that replicas can download during boot. It uses streaming I/O
  to handle large databases without memory issues.
  """

  def __init__(
    self,
    environment: str,
    base_path: str,
    s3_bucket: str | None = None,
    compression_level: int = DEFAULT_COMPRESSION_LEVEL,
  ):
    """
    Initialize shared repository backup service.

    Args:
        environment: Environment name (dev/staging/prod)
        base_path: Base path where graph databases are stored
        s3_bucket: S3 bucket for backups (defaults to USER_DATA_BUCKET)
        compression_level: Gzip compression level (1-9)
    """
    self.environment = environment
    self.base_path = Path(base_path)
    self.compression_level = compression_level
    self.s3_bucket = s3_bucket or env.USER_DATA_BUCKET

    # AWS clients
    s3_config = env.get_s3_config()
    self.s3_client = boto3.client(
      "s3",
      aws_access_key_id=s3_config.get("aws_access_key_id"),
      aws_secret_access_key=s3_config.get("aws_secret_access_key"),
      region_name=s3_config.get("region_name"),
      endpoint_url=s3_config.get("endpoint_url"),
    )

    logger.info(f"Initialized shared repository backup service for {environment}")

  def _get_s3_prefix(self, repo_name: str) -> str:
    """Get S3 prefix for a shared repository."""
    return f"{SHARED_REPLICA_PREFIX}/{repo_name}"

  def _get_backup_key(self, repo_name: str, timestamp: datetime) -> str:
    """Generate S3 key for a backup file."""
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
    return f"{self._get_s3_prefix(repo_name)}/{repo_name}-{timestamp_str}.tar.gz"

  def _get_manifest_key(self, repo_name: str) -> str:
    """Get S3 key for the manifest file."""
    return f"{self._get_s3_prefix(repo_name)}/latest.json"

  def create_backup(self, repo_name: str) -> dict[str, Any]:
    """
    Create a streaming backup of a shared repository.

    This method:
    1. Creates a tar.gz archive using streaming I/O
    2. Uploads to S3 using multipart upload
    3. Updates the manifest file for replicas

    Args:
        repo_name: Name of the shared repository (e.g., "sec")

    Returns:
        Backup result with S3 location and metadata
    """
    start_time = datetime.now(UTC)
    logger.info(f"Starting backup of shared repository: {repo_name}")

    # Find the database file
    db_path = self.base_path / f"{repo_name}.lbug"

    if not db_path.exists():
      # Try nested path (lbug-dbs subdirectory)
      db_path = self.base_path / "lbug-dbs" / f"{repo_name}.lbug"

    if not db_path.exists():
      raise SharedRepositoryBackupError(
        f"Database not found: {repo_name}.lbug (searched {self.base_path})"
      )

    # Get database size for logging
    db_size_bytes = self._get_path_size(db_path)
    db_size_gb = db_size_bytes / (1024**3)
    logger.info(f"Database size: {db_size_gb:.2f} GB")

    try:
      # Create compressed backup in temp directory
      with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        backup_file = temp_path / f"{repo_name}.tar.gz"

        # Stream compress the database
        logger.info("Creating compressed archive (streaming)...")
        compress_start = datetime.now(UTC)
        self._create_compressed_backup(db_path, backup_file)
        compress_time = (datetime.now(UTC) - compress_start).total_seconds()

        backup_size_bytes = backup_file.stat().st_size
        backup_size_gb = backup_size_bytes / (1024**3)
        compression_ratio = backup_size_bytes / db_size_bytes

        logger.info(
          f"Compression complete: {db_size_gb:.2f}GB -> {backup_size_gb:.2f}GB "
          f"({compression_ratio:.1%}) in {compress_time:.1f}s"
        )

        # Calculate checksum
        logger.info("Calculating checksum...")
        checksum = self._calculate_file_checksum(backup_file)

        # Upload to S3
        s3_key = self._get_backup_key(repo_name, start_time)
        logger.info(f"Uploading to S3: {s3_key}")
        upload_start = datetime.now(UTC)
        self._upload_to_s3(backup_file, s3_key, checksum)
        upload_time = (datetime.now(UTC) - upload_start).total_seconds()
        logger.info(f"Upload complete in {upload_time:.1f}s")

        # Update manifest
        manifest = {
          "repo_name": repo_name,
          "s3_bucket": self.s3_bucket,
          "s3_key": s3_key,
          "timestamp": start_time.isoformat(),
          "checksum": checksum,
          "original_size_bytes": db_size_bytes,
          "compressed_size_bytes": backup_size_bytes,
          "compression_ratio": compression_ratio,
          "environment": self.environment,
        }
        self._update_manifest(repo_name, manifest)

        total_time = (datetime.now(UTC) - start_time).total_seconds()

        logger.info(
          f"Shared repository backup complete: {repo_name} "
          f"({backup_size_gb:.2f}GB) in {total_time:.1f}s"
        )

        return {
          "status": "success",
          "repo_name": repo_name,
          "s3_bucket": self.s3_bucket,
          "s3_key": s3_key,
          "manifest_key": self._get_manifest_key(repo_name),
          "checksum": checksum,
          "original_size_gb": round(db_size_gb, 2),
          "compressed_size_gb": round(backup_size_gb, 2),
          "compression_ratio": round(compression_ratio, 3),
          "compress_time_seconds": round(compress_time, 1),
          "upload_time_seconds": round(upload_time, 1),
          "total_time_seconds": round(total_time, 1),
        }

    except Exception as e:
      logger.error(f"Failed to backup shared repository {repo_name}: {e}")
      raise SharedRepositoryBackupError(f"Backup failed: {e}") from e

  def _get_path_size(self, path: Path) -> int:
    """Get total size of a path (file or directory) in bytes."""
    if path.is_file():
      return path.stat().st_size
    total = 0
    for file_path in path.rglob("*"):
      if file_path.is_file():
        total += file_path.stat().st_size
    return total

  def _create_compressed_backup(self, db_path: Path, backup_file: Path) -> None:
    """
    Create a compressed tar.gz backup using streaming I/O.

    This uses tarfile's streaming mode which reads and compresses
    in chunks, never loading the full database into memory.
    """
    with tarfile.open(backup_file, "w:gz", compresslevel=self.compression_level) as tar:
      # Add the database file/directory
      tar.add(db_path, arcname=db_path.name)

  def _calculate_file_checksum(self, file_path: Path) -> str:
    """Calculate SHA256 checksum of a file using streaming."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
      for chunk in iter(lambda: f.read(8192), b""):
        sha256_hash.update(chunk)
    return sha256_hash.hexdigest()

  def _upload_to_s3(self, file_path: Path, s3_key: str, checksum: str) -> None:
    """
    Upload file to S3 with multipart upload for large files.

    boto3's upload_file automatically handles multipart uploads
    for files larger than the multipart threshold.
    """
    try:
      extra_args = {
        "Metadata": {
          "checksum": checksum,
          "created_at": datetime.now(UTC).isoformat(),
          "backup_type": "shared_repository",
        },
        "StorageClass": "STANDARD",  # Use standard for frequently accessed replica data
      }

      self.s3_client.upload_file(
        str(file_path),
        self.s3_bucket,
        s3_key,
        ExtraArgs=extra_args,
      )

    except ClientError as e:
      raise SharedRepositoryBackupError(f"S3 upload failed: {e}") from e

  def _update_manifest(self, repo_name: str, manifest: dict) -> None:
    """
    Update the manifest file that replicas use to find the latest backup.

    The manifest is a JSON file at a well-known location that replicas
    read during boot to discover the current backup.
    """
    manifest_key = self._get_manifest_key(repo_name)

    try:
      self.s3_client.put_object(
        Bucket=self.s3_bucket,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2),
        ContentType="application/json",
      )
      logger.info(f"Updated manifest: s3://{self.s3_bucket}/{manifest_key}")

    except ClientError as e:
      raise SharedRepositoryBackupError(f"Failed to update manifest: {e}") from e

  def get_latest_manifest(self, repo_name: str) -> dict[str, Any] | None:
    """
    Get the latest manifest for a shared repository.

    Args:
        repo_name: Name of the shared repository

    Returns:
        Manifest dict or None if not found
    """
    manifest_key = self._get_manifest_key(repo_name)

    try:
      response = self.s3_client.get_object(
        Bucket=self.s3_bucket,
        Key=manifest_key,
      )
      return json.loads(response["Body"].read().decode("utf-8"))

    except ClientError as e:
      if e.response["Error"]["Code"] == "NoSuchKey":
        return None
      raise

  def cleanup_old_backups(self, repo_name: str, keep_count: int = 3) -> int:
    """
    Clean up old backups, keeping only the most recent ones.

    Args:
        repo_name: Name of the shared repository
        keep_count: Number of recent backups to keep

    Returns:
        Number of backups deleted
    """
    prefix = self._get_s3_prefix(repo_name)

    try:
      # List all backup files (exclude manifest)
      response = self.s3_client.list_objects_v2(
        Bucket=self.s3_bucket,
        Prefix=prefix,
      )

      if "Contents" not in response:
        return 0

      # Filter to only .tar.gz files and sort by date (newest first)
      backups = [obj for obj in response["Contents"] if obj["Key"].endswith(".tar.gz")]
      backups.sort(key=lambda x: x["LastModified"], reverse=True)

      # Delete old backups beyond keep_count
      to_delete = backups[keep_count:]
      if not to_delete:
        return 0

      delete_keys = [{"Key": obj["Key"]} for obj in to_delete]
      self.s3_client.delete_objects(
        Bucket=self.s3_bucket,
        Delete={"Objects": delete_keys},
      )

      logger.info(
        f"Cleaned up {len(to_delete)} old backups for {repo_name}, "
        f"kept {keep_count} most recent"
      )
      return len(to_delete)

    except ClientError as e:
      logger.error(f"Failed to cleanup old backups: {e}")
      return 0


def create_shared_repository_backup_service(
  environment: str | None = None,
  base_path: str | None = None,
) -> SharedRepositoryBackupService:
  """
  Factory function to create shared repository backup service.

  Args:
      environment: Environment name (defaults to env.ENVIRONMENT)
      base_path: Database path (defaults to env.LBUG_DATABASE_PATH)

  Returns:
      Configured SharedRepositoryBackupService instance
  """
  return SharedRepositoryBackupService(
    environment=environment or env.ENVIRONMENT,
    base_path=base_path or env.LBUG_DATABASE_PATH,
  )
