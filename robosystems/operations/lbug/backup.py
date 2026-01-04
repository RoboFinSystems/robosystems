"""
LadybugDB Graph Database Backup Service

This service handles automated backup of customer-specific LadybugDB graph databases to S3.
Unlike shared repositories, customer graph databases require regular automated backup
to prevent data loss from hardware failures or corruption.

Key features:
- Automated daily backup of all customer graph databases
- Incremental backup based on modification times
- S3 lifecycle management for cost optimization
- Backup verification and integrity checks
- Integration with DynamoDB allocation registry
- CloudWatch metrics for backup monitoring
"""

import hashlib
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

from ...config import env
from ...logger import logger
from ...middleware.graph.allocation_manager import LadybugAllocationManager

# Backup configuration
# Graph backups are stored in the USER_DATA_BUCKET under graph-databases/ prefix
DEFAULT_RETENTION_DAYS = 30  # Keep customer backups longer than shared repos
DEFAULT_COMPRESSION_LEVEL = 6
MAX_BACKUP_SIZE_GB = 10  # Skip backup if database > 10GB (log warning)


class LadybugGraphBackupError(Exception):
  """Custom exception for graph backup operations."""

  pass


class LadybugGraphBackupService:
  """
  Service for backing up LadybugDB graph databases to S3.

  This service runs on writer instances and backs up all customer graph databases
  allocated to the instance. It integrates with the allocation manager to
  discover databases and tracks backup status in CloudWatch.
  """

  def __init__(
    self,
    environment: str,
    base_path: str,
    s3_bucket: str | None = None,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    compression_level: int = DEFAULT_COMPRESSION_LEVEL,
  ):
    """
    Initialize graph backup service.

    Args:
        environment: Environment name (dev/staging/prod)
        base_path: Base path where graph databases are stored
        s3_bucket: S3 bucket for backups (defaults to env-specific bucket)
        retention_days: Number of days to keep backups
        compression_level: Gzip compression level (1-9)
    """
    self.environment = environment
    self.base_path = Path(base_path)
    self.retention_days = retention_days
    self.compression_level = compression_level

    # S3 configuration - use canonical USER_DATA_BUCKET for customer graph backups
    self.s3_bucket = s3_bucket or env.USER_DATA_BUCKET
    self.s3_prefix = f"graph-databases/{environment}"

    # AWS clients - use S3-specific credentials
    s3_config = env.get_s3_config()
    self.s3_client = boto3.client(
      "s3",
      aws_access_key_id=s3_config.get("aws_access_key_id"),
      aws_secret_access_key=s3_config.get("aws_secret_access_key"),
      region_name=s3_config.get("region_name"),
      endpoint_url=s3_config.get("endpoint_url"),
    )
    self.cloudwatch = boto3.client("cloudwatch")

    # Allocation manager for discovering databases
    self.allocation_manager = LadybugAllocationManager(environment)

    # Get current instance ID for filtering
    try:
      # In production, get from EC2 metadata
      import requests

      self.instance_id = requests.get(
        "http://169.254.169.254/latest/meta-data/instance-id", timeout=2
      ).text
    except Exception:
      # Fallback for development
      self.instance_id = "dev-instance"

    logger.info(
      f"Initialized graph backup service for {environment} on instance {self.instance_id}"
    )

  async def backup_all_graph_databases(self) -> dict[str, Any]:
    """
    Backup all customer graph databases allocated to this instance.

    Returns:
        Backup summary with success/failure counts and details
    """
    start_time = datetime.now(UTC)
    logger.info("Starting automated backup of all graph databases")

    try:
      # Get all databases allocated to this instance
      databases = await self._get_instance_databases()

      if not databases:
        logger.info("No graph databases found on this instance")
        return {
          "status": "success",
          "total_databases": 0,
          "backed_up": 0,
          "skipped": 0,
          "failed": 0,
          "execution_time_minutes": 0,
        }

      logger.info(f"Found {len(databases)} graph databases to backup")

      # Backup each database
      results = []
      backed_up = 0
      skipped = 0
      failed = 0

      for graph_id in databases:
        try:
          result = await self.backup_graph_database(graph_id)
          results.append(result)

          if result["status"] == "success":
            backed_up += 1
          elif result["status"] == "skipped":
            skipped += 1
          else:
            failed += 1

        except Exception as e:
          logger.error(f"Failed to backup database {graph_id}: {e}")
          failed += 1
          results.append({"graph_id": graph_id, "status": "failed", "error": str(e)})

      # Calculate summary
      execution_time = (datetime.now(UTC) - start_time).total_seconds() / 60

      summary = {
        "status": "success" if failed == 0 else "partial_failure",
        "total_databases": len(databases),
        "backed_up": backed_up,
        "skipped": skipped,
        "failed": failed,
        "execution_time_minutes": round(execution_time, 2),
        "results": results,
      }

      # Publish metrics to CloudWatch
      await self._publish_backup_metrics(summary)

      logger.info(
        f"Graph backup completed: {backed_up} backed up, {skipped} skipped, {failed} failed"
      )
      return summary

    except Exception as e:
      logger.error(f"Graph database backup failed: {e}")
      raise LadybugGraphBackupError(f"Backup operation failed: {e}")

  async def backup_graph_database(self, graph_id: str) -> dict[str, Any]:
    """
    Backup a specific graph database to S3.

    Args:
        graph_id: Graph database identifier

    Returns:
        Backup result details
    """
    start_time = datetime.now(UTC)

    try:
      # Construct database path
      db_path = self.base_path / f"{graph_id}.lbug"

      if not db_path.exists():
        return {
          "graph_id": graph_id,
          "status": "skipped",
          "reason": "Database not found on this instance",
        }

      # Check database size
      db_size_gb = self._get_directory_size(db_path) / (1024**3)
      if db_size_gb > MAX_BACKUP_SIZE_GB:
        logger.warning(
          f"Database {graph_id} is {db_size_gb:.2f}GB, skipping backup (exceeds {MAX_BACKUP_SIZE_GB}GB limit)"
        )
        return {
          "graph_id": graph_id,
          "status": "skipped",
          "reason": f"Database too large ({db_size_gb:.2f}GB)",
        }

      # Check if backup is needed (based on modification time)
      if await self._is_backup_current(graph_id, db_path):
        return {
          "graph_id": graph_id,
          "status": "skipped",
          "reason": "Backup is current",
        }

      logger.info(f"Backing up graph database {graph_id} ({db_size_gb:.2f}GB)")

      # Create compressed backup
      with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        backup_file = (
          temp_path / f"{graph_id}_{start_time.strftime('%Y%m%d_%H%M%S')}.tar.gz"
        )

        # Create compressed archive
        await self._create_compressed_backup(db_path, backup_file)

        # Calculate checksum
        checksum = self._calculate_file_checksum(backup_file)

        # Upload to S3
        s3_key = f"{self.s3_prefix}/{graph_id}/{backup_file.name}"
        await self._upload_backup_to_s3(backup_file, s3_key, checksum)

        backup_size_mb = backup_file.stat().st_size / (1024**2)
        execution_time = (datetime.now(UTC) - start_time).total_seconds()

        logger.info(
          f"Successfully backed up {graph_id}: {backup_size_mb:.1f}MB in {execution_time:.1f}s"
        )

        return {
          "graph_id": graph_id,
          "status": "success",
          "backup_size_mb": round(backup_size_mb, 1),
          "execution_time_seconds": round(execution_time, 1),
          "s3_key": s3_key,
          "checksum": checksum,
        }

    except Exception as e:
      logger.error(f"Failed to backup graph database {graph_id}: {e}")
      return {"graph_id": graph_id, "status": "failed", "error": str(e)}

  async def _get_instance_databases(self) -> list[str]:
    """Get all customer graph databases allocated to this instance."""
    try:
      # Get databases from allocation manager
      databases = await self.allocation_manager.get_instance_databases(self.instance_id)

      # Filter out shared repositories (keep customer databases)
      # Customer databases don't have specific prefixes like 'sec', 'industry', etc.
      shared_prefixes = ["sec", "industry", "economic", "stock"]
      customer_databases = [
        db
        for db in databases
        if not any(db.startswith(prefix) for prefix in shared_prefixes)
      ]

      return customer_databases

    except Exception as e:
      logger.error(f"Failed to get instance databases: {e}")
      return []

  def _get_directory_size(self, path: Path) -> int:
    """Calculate total size of directory in bytes."""
    total_size = 0
    for file_path in path.rglob("*"):
      if file_path.is_file():
        total_size += file_path.stat().st_size
    return total_size

  async def _is_backup_current(self, graph_id: str, db_path: Path) -> bool:
    """Check if an up-to-date backup already exists."""
    try:
      # Get latest backup from S3
      s3_prefix = f"{self.s3_prefix}/{graph_id}/"

      response = self.s3_client.list_objects_v2(
        Bucket=self.s3_bucket, Prefix=s3_prefix, MaxKeys=1
      )

      if "Contents" not in response:
        return False  # No backups exist

      # Get most recent backup timestamp
      latest_backup = max(response["Contents"], key=lambda x: x["LastModified"])
      backup_time = latest_backup["LastModified"].replace(tzinfo=None)

      # Get database modification time
      db_modified = datetime.fromtimestamp(db_path.stat().st_mtime)

      # Backup is current if it's newer than database modification
      return backup_time > db_modified

    except ClientError as e:
      if e.response["Error"]["Code"] == "NoSuchBucket":
        logger.warning(f"Backup bucket {self.s3_bucket} does not exist")
      return False
    except Exception as e:
      logger.error(f"Failed to check backup currency for {graph_id}: {e}")
      return False

  async def _create_compressed_backup(self, db_path: Path, backup_file: Path) -> None:
    """Create a compressed tar.gz backup of the database."""
    import tarfile

    with tarfile.open(backup_file, "w:gz", compresslevel=self.compression_level) as tar:
      tar.add(db_path, arcname=db_path.name)

  def _calculate_file_checksum(self, file_path: Path) -> str:
    """Calculate SHA256 checksum of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
      for chunk in iter(lambda: f.read(4096), b""):
        sha256_hash.update(chunk)
    return sha256_hash.hexdigest()

  async def _upload_backup_to_s3(
    self, backup_file: Path, s3_key: str, checksum: str
  ) -> None:
    """Upload backup file to S3 with metadata."""
    try:
      extra_args = {
        "Metadata": {
          "checksum": checksum,
          "created_at": datetime.now(UTC).isoformat(),
          "instance_id": self.instance_id,
          "backup_type": "graph_database",
        },
        "StorageClass": "STANDARD_IA",  # Cheaper storage for backups
      }

      self.s3_client.upload_file(
        str(backup_file), self.s3_bucket, s3_key, ExtraArgs=extra_args
      )

      logger.debug(f"Successfully uploaded backup to s3://{self.s3_bucket}/{s3_key}")

    except ClientError as e:
      logger.error(f"Failed to upload backup to S3: {e}")
      raise LadybugGraphBackupError(f"S3 upload failed: {e}")

  async def _publish_backup_metrics(self, summary: dict[str, Any]) -> None:
    """Publish backup metrics to CloudWatch."""
    try:
      # Use environment-specific namespace instead of Environment dimension
      metric_data = [
        {
          "MetricName": "GraphDatabaseBackups",
          "Value": summary["backed_up"],
          "Unit": "Count",
          "Dimensions": [
            {"Name": "InstanceId", "Value": self.instance_id},
            {"Name": "Status", "Value": "Success"},
          ],
        },
        {
          "MetricName": "GraphDatabaseBackups",
          "Value": summary["failed"],
          "Unit": "Count",
          "Dimensions": [
            {"Name": "InstanceId", "Value": self.instance_id},
            {"Name": "Status", "Value": "Failed"},
          ],
        },
        {
          "MetricName": "GraphBackupExecutionTime",
          "Value": summary["execution_time_minutes"],
          "Unit": "Minutes",
          "Dimensions": [
            {"Name": "InstanceId", "Value": self.instance_id},
          ],
        },
      ]

      # Publish to environment-specific Graph namespace
      namespace = f"RoboSystems/Graph/{self.environment}"
      self.cloudwatch.put_metric_data(Namespace=namespace, MetricData=metric_data)

    except Exception as e:
      logger.error(f"Failed to publish backup metrics: {e}")

  async def cleanup_old_backups(self) -> int:
    """
    Clean up old backup files based on retention policy.

    Returns:
        Number of files deleted
    """
    try:
      cutoff_date = datetime.now(UTC) - timedelta(days=self.retention_days)
      deleted_count = 0

      # List all objects in the backup prefix
      paginator = self.s3_client.get_paginator("list_objects_v2")
      pages = paginator.paginate(Bucket=self.s3_bucket, Prefix=self.s3_prefix)

      objects_to_delete = []

      for page in pages:
        if "Contents" not in page:
          continue

        for obj in page["Contents"]:
          if obj["LastModified"].replace(tzinfo=None) < cutoff_date:
            objects_to_delete.append({"Key": obj["Key"]})

            # Delete in batches of 1000 (S3 limit)
            if len(objects_to_delete) >= 1000:
              self._delete_s3_objects(objects_to_delete)
              deleted_count += len(objects_to_delete)
              objects_to_delete = []

      # Delete remaining objects
      if objects_to_delete:
        self._delete_s3_objects(objects_to_delete)
        deleted_count += len(objects_to_delete)

      if deleted_count > 0:
        logger.info(
          f"Cleaned up {deleted_count} old backup files (older than {self.retention_days} days)"
        )

      return deleted_count

    except Exception as e:
      logger.error(f"Failed to cleanup old backups: {e}")
      return 0

  def _delete_s3_objects(self, objects: list[dict[str, str]]) -> None:
    """Delete a batch of S3 objects."""
    try:
      self.s3_client.delete_objects(Bucket=self.s3_bucket, Delete={"Objects": objects})
    except ClientError as e:
      logger.error(f"Failed to delete S3 objects: {e}")


# Factory function for easy integration
def create_graph_backup_service(
  environment: str = "prod", base_path: str = "/data/lbug-dbs"
) -> LadybugGraphBackupService:
  """Create graph backup service for the specified environment."""
  return LadybugGraphBackupService(environment=environment, base_path=base_path)
