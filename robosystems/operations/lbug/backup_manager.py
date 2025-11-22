"""
Backup Manager for graph database operations.

This module provides comprehensive backup and restore functionality for graph
databases with support for multiple backup formats and storage options.

Key features:
- Multiple backup formats: CSV, JSON, Parquet, Full Database Dump
- S3 storage with compression and encryption
- Multi-tenant database support
- Schema-driven export/import
- Progress tracking and monitoring
"""

import asyncio
import json
import os
import shutil
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum

from ...adapters.s3 import S3BackupAdapter, BackupMetadata
from ...middleware.graph import get_universal_repository
from ...middleware.graph.multitenant_utils import MultiTenantUtils
from ...logger import logger


class BackupFormat(str, Enum):
  """Supported backup formats."""

  CSV = "csv"
  PARQUET = "parquet"
  JSON = "json"
  FULL_DUMP = "full_dump"


class BackupType(str, Enum):
  """Backup type options."""

  FULL = "full"
  INCREMENTAL = "incremental"


@dataclass
class BackupJob:
  """Represents a backup job configuration."""

  graph_id: str
  backup_format: BackupFormat = BackupFormat.FULL_DUMP
  backup_type: BackupType = BackupType.FULL
  timestamp: Optional[datetime] = None
  schedule: Optional[str] = None
  retention_days: int = 90
  compression: bool = True
  encryption: bool = True
  allow_export: bool = True  # If True, backup can be exported/downloaded

  def __post_init__(self):
    """Validate backup job configuration."""
    if self.timestamp is None:
      self.timestamp = datetime.now(timezone.utc)

    if self.backup_format not in BackupFormat:
      raise ValueError(f"Invalid backup_format: {self.backup_format}")

    if self.backup_type not in BackupType:
      raise ValueError(f"Invalid backup_type: {self.backup_type}")

    # Validate encryption constraints
    if self.encryption and self.allow_export:
      raise ValueError(
        "Encryption can only be enabled for non-exportable backups. "
        "Set allow_export=False to enable encryption."
      )

    # Restore is only available for full dumps with encryption
    if self.encryption and self.backup_format != BackupFormat.FULL_DUMP:
      raise ValueError("Encryption is only supported for full dump backups")

    # Validate graph_id
    MultiTenantUtils.validate_graph_id(self.graph_id)


@dataclass
class RestoreJob:
  """Represents a restore job configuration."""

  graph_id: str
  backup_metadata: BackupMetadata
  backup_format: BackupFormat
  create_new_database: bool = True
  drop_existing: bool = False
  verify_after_restore: bool = True
  progress_tracker: Optional[Any] = None

  def __post_init__(self):
    """Validate restore job configuration."""
    # Restore is only available for full dumps with encryption
    if self.backup_format != BackupFormat.FULL_DUMP:
      raise ValueError(
        "Restore is only supported for full dump backups. "
        "CSV, JSON, and Parquet backups are intended for export/import workflows."
      )


class BackupManager:
  """Manager for graph database backups with multiple format support."""

  def __init__(
    self,
    s3_adapter: Optional[S3BackupAdapter] = None,
    graph_router=None,
  ):
    """
    Initialize backup manager.

    Args:
        s3_adapter: S3 adapter for backup storage
        graph_router: Graph router for API communication
    """
    self._s3_adapter = s3_adapter
    self._graph_router = graph_router
    logger.info("BackupManager initialized")

  @property
  def s3_adapter(self):
    """Lazy initialization of S3 adapter."""
    if self._s3_adapter is None:
      self._s3_adapter = S3BackupAdapter()
    return self._s3_adapter

  @property
  def graph_router(self):
    """Lazy initialization of graph router."""
    if self._graph_router is None:
      from robosystems.middleware.graph.router import get_graph_router

      self._graph_router = get_graph_router()
    return self._graph_router

  async def get_backup_download_url(
    self, graph_id: str, backup_id: str, expires_in: int = 3600
  ) -> Optional[str]:
    """
    Get a temporary download URL for a backup.

    Args:
        graph_id: Graph database identifier
        backup_id: Backup identifier
        expires_in: URL expiration time in seconds

    Returns:
        Temporary download URL or None if backup not found
    """
    try:
      from robosystems.database import session as SessionLocal
      from robosystems.models.iam.graph_backup import GraphBackup

      # Get backup record from database
      db_session = SessionLocal()
      try:
        backup = GraphBackup.get_by_id(backup_id, db_session)

        if not backup:
          logger.warning(f"Backup {backup_id} not found in database")
          return None

        # Check if backup is completed
        if not backup.is_completed:
          logger.warning(
            f"Backup {backup_id} is not completed (status: {backup.status})"
          )
          return None

        # Check if backup is exportable (not encrypted)
        if backup.encryption_enabled:
          raise ValueError("Encrypted backups cannot be downloaded")

        # Format timestamp for filename (aligned with created_at shown in frontend)
        timestamp_str = backup.created_at.strftime("%Y%m%d_%H%M%S")
        filename = f"{graph_id}_{timestamp_str}.zip"

        # Generate presigned URL using the S3 key from the backup record
        import asyncio

        url = await asyncio.get_event_loop().run_in_executor(
          None,
          lambda: self.s3_adapter.s3_client.generate_presigned_url(
            "get_object",
            Params={
              "Bucket": backup.s3_bucket,
              "Key": backup.s3_key,
              "ResponseContentDisposition": f'attachment; filename="{filename}"',
            },
            ExpiresIn=expires_in,
          ),
        )

        # Replace container hostname with localhost for development (LocalStack)
        if "localstack:" in url:
          url = url.replace("localstack:4566", "localhost:4566")
          logger.info(
            "Replaced LocalStack container hostname with localhost in presigned URL"
          )

        logger.info(
          f"Generated download URL for backup {backup_id} (expires in {expires_in}s)"
        )
        return url
      finally:
        SessionLocal.remove()

    except Exception as e:
      logger.error(f"Failed to generate download URL for backup {backup_id}: {e}")
      return None

  async def download_backup(
    self, graph_id: str, backup_id: str, target_format: Optional[str] = None
  ) -> Tuple[bytes, str, str]:
    """
    Download a backup file with optional format conversion.

    Args:
        graph_id: Graph database identifier
        backup_id: Backup identifier
        target_format: Optional target format for conversion (csv, json, zip)

    Returns:
        Tuple of (file_data, content_type, filename)
    """
    try:
      # Get backup metadata
      backup_metadata = await self.s3_adapter.get_backup_metadata(graph_id, backup_id)

      if not backup_metadata:
        raise ValueError(f"Backup {backup_id} not found")

      # Check if backup is exportable
      if backup_metadata.get("encryption_enabled", False):
        raise ValueError("Encrypted backups cannot be downloaded")

      # Download backup data from S3
      backup_data = await self.s3_adapter.download_backup(graph_id, backup_id)

      original_format = backup_metadata.get("backup_format", "unknown")

      # Handle format conversion if requested
      if target_format and target_format != original_format:
        backup_data, content_type, filename = await self._convert_backup_format(
          backup_data, original_format, target_format, backup_id
        )
      else:
        # Use original format
        content_type, filename = self._get_content_type_and_filename(
          original_format, backup_id
        )

      logger.info(f"Downloaded backup {backup_id} ({len(backup_data)} bytes)")
      return backup_data, content_type, filename

    except Exception as e:
      logger.error(f"Failed to download backup {backup_id}: {e}")
      raise

  async def _convert_backup_format(
    self, backup_data: bytes, from_format: str, to_format: str, backup_id: str
  ) -> Tuple[bytes, str, str]:
    """
    Convert backup data between formats.

    Args:
        backup_data: Original backup data
        from_format: Source format
        to_format: Target format
        backup_id: Backup identifier

    Returns:
        Tuple of (converted_data, content_type, filename)
    """
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir)

      # Extract original backup
      original_file = temp_path / f"original_{backup_id}"
      original_file.write_bytes(backup_data)

      # Perform conversion based on format pairs
      if from_format == "csv" and to_format == "json":
        converted_data = await self._convert_csv_to_json(original_file)
        content_type = "application/json"
        filename = f"{backup_id}.json"

      elif from_format == "json" and to_format == "csv":
        converted_data = await self._convert_json_to_csv(original_file)
        content_type = "text/csv"
        filename = f"{backup_id}.csv"

      elif from_format == "full_dump" and to_format == "zip":
        converted_data = await self._convert_full_dump_to_zip(original_file)
        content_type = "application/zip"
        filename = f"{backup_id}_database.lbug.zip"

      else:
        raise ValueError(f"Conversion from {from_format} to {to_format} not supported")

      return converted_data, content_type, filename

  def _get_content_type_and_filename(
    self, format: str, backup_id: str
  ) -> Tuple[str, str]:
    """Get appropriate content type and filename for backup format."""
    format_map = {
      "csv": ("text/csv", f"{backup_id}.csv.zip"),
      "json": ("application/json", f"{backup_id}.json.zip"),
      "parquet": ("application/octet-stream", f"{backup_id}.parquet.zip"),
      "full_dump": ("application/zip", f"{backup_id}_database.lbug.zip"),
    }

    return format_map.get(format, ("application/octet-stream", f"{backup_id}.zip"))

  async def _convert_csv_to_json(self, csv_file: Path) -> bytes:
    """Convert CSV backup to JSON format."""
    import pandas as pd

    # Read CSV data
    df = pd.read_csv(csv_file)

    # Convert to JSON
    json_data = df.to_json(orient="records", indent=2)

    return (json_data or "").encode("utf-8")

  async def _convert_json_to_csv(self, json_file: Path) -> bytes:
    """Convert JSON backup to CSV format."""
    import pandas as pd
    import json

    # Read JSON data
    with open(json_file, "r") as f:
      json_data = json.load(f)

    # Convert to DataFrame and then CSV
    df = pd.DataFrame(json_data)
    csv_data = df.to_csv(index=False)

    return csv_data.encode("utf-8")

  async def _convert_full_dump_to_zip(self, dump_file: Path) -> bytes:
    """Convert full dump to structured ZIP archive."""
    import zipfile
    import io

    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
      # Add the database file with .lbug extension
      zip_file.write(dump_file, f"database/{dump_file.stem}.lbug")

      # Add metadata file
      metadata = {
        "export_timestamp": datetime.now(timezone.utc).isoformat(),
        "database_type": "ladybug",
        "format": "full_dump",
        "contents": ["database_files", "metadata"],
      }

      zip_file.writestr("metadata.json", json.dumps(metadata, indent=2).encode("utf-8"))

    return zip_buffer.getvalue()

  async def create_backup(self, backup_job: BackupJob) -> BackupMetadata:
    """
    Create a backup of a graph database.

    Args:
        backup_job: Backup job configuration

    Returns:
        BackupMetadata: Information about the created backup
    """
    start_time = asyncio.get_event_loop().time()
    graph_id = backup_job.graph_id

    timestamp_str = backup_job.timestamp.isoformat() if backup_job.timestamp else "auto"
    logger.info(
      f"Starting {backup_job.backup_format.value} backup for graph '{graph_id}' "
      f"with timestamp: {timestamp_str}"
    )

    try:
      # Get database statistics
      stats = await self._get_database_stats(graph_id)

      # Export database based on format
      backup_data, file_extension = await self._export_database(
        graph_id, backup_job.backup_format, backup_job.backup_type
      )

      # Calculate backup duration
      backup_duration = asyncio.get_event_loop().time() - start_time

      # Prepare metadata
      metadata = {
        "node_count": stats["node_count"],
        "relationship_count": stats["relationship_count"],
        "backup_duration_seconds": backup_duration,
        "backup_format": backup_job.backup_format.value,
        "database_engine": "graph",
        "allow_export": backup_job.allow_export,
        "encryption_enabled": backup_job.encryption,
        "compression_enabled": backup_job.compression,
      }

      # Upload to S3
      backup_metadata = await self.s3_adapter.upload_backup(
        graph_id=graph_id,
        backup_data=backup_data,
        backup_type=backup_job.backup_type.value,
        metadata=metadata,
        timestamp=backup_job.timestamp,
        file_extension=file_extension,
      )

      logger.info(
        f"Backup completed for graph '{graph_id}': "
        f"{backup_metadata.original_size} bytes, "
        f"format: {backup_job.backup_format.value}, "
        f"compression: {backup_metadata.compression_ratio:.1%}, "
        f"duration: {backup_duration:.2f}s"
      )

      return backup_metadata

    except Exception as e:
      logger.error(f"Backup failed for graph '{graph_id}': {e}")
      raise

  async def restore_backup(self, restore_job: RestoreJob) -> bool:
    """
    Restore a graph database from backup.

    Args:
        restore_job: Restore job configuration

    Returns:
        bool: True if restore was successful
    """
    graph_id = restore_job.graph_id
    metadata = restore_job.backup_metadata

    logger.info(
      f"Starting restore for graph '{graph_id}' from {restore_job.backup_format.value} "
      f"backup {metadata.timestamp.isoformat()}"
    )

    try:
      # Download backup data
      if metadata.s3_key:
        backup_data = await self.s3_adapter.download_backup_by_key(metadata.s3_key)
      else:
        backup_data = await self.s3_adapter.download_backup_by_timestamp(
          graph_id=graph_id,
          timestamp=metadata.timestamp,
          backup_type=metadata.backup_type,
        )

      # Validate checksum
      if not await self._validate_backup_integrity(backup_data, metadata):
        raise ValueError("Backup integrity check failed")

      # Prepare target database
      if restore_job.drop_existing:
        await self._drop_database_if_exists(graph_id)

      if restore_job.create_new_database:
        await self._ensure_database_exists(graph_id)

      # Import backup data based on format
      await self._import_backup_data(
        graph_id, backup_data, restore_job.backup_format, restore_job.progress_tracker
      )

      # Verify restore if requested
      if restore_job.verify_after_restore:
        if not await self._verify_restore(graph_id, metadata):
          logger.warning(f"Restore verification failed for graph '{graph_id}'")
          return False

      logger.info(f"Restore completed successfully for graph '{graph_id}'")
      return True

    except Exception as e:
      logger.error(f"Restore failed for graph '{graph_id}': {e}")
      raise

  async def list_backups(self, graph_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List available backups.

    Args:
        graph_id: Optional filter by graph ID

    Returns:
        List of backup information
    """
    return await self.s3_adapter.list_backups(graph_id)

  async def delete_old_backups(self, graph_id: str, retention_days: int) -> int:
    """
    Delete backups older than retention period.

    Args:
        graph_id: Graph identifier
        retention_days: Number of days to retain backups

    Returns:
        int: Number of backups deleted
    """
    backups = await self.list_backups(graph_id)
    cutoff_date = datetime.now(timezone.utc).timestamp() - (retention_days * 24 * 3600)

    deleted_count = 0
    for backup in backups:
      if backup["last_modified"].timestamp() < cutoff_date:
        try:
          # Extract timestamp from backup key
          key_parts = backup["key"].split("/")
          filename = key_parts[-1]
          timestamp_str = filename.split("-")[1].split(".")[0]
          timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S").replace(
            tzinfo=timezone.utc
          )

          success = await self.s3_adapter.delete_backup(
            graph_id=backup["graph_id"],
            timestamp=timestamp,
            backup_type=backup["backup_type"],
          )

          if success:
            deleted_count += 1

        except Exception as e:
          logger.warning(f"Failed to delete backup {backup['key']}: {e}")

    logger.info(f"Deleted {deleted_count} old backups for graph '{graph_id}'")
    return deleted_count

  async def _get_database_stats(self, graph_id: str) -> Dict[str, Any]:
    """Get database statistics for backup metadata."""
    repository = await get_universal_repository(graph_id, operation_type="read")

    async with repository:
      # Get node and relationship counts
      node_count_result = await repository.execute_single(
        "MATCH (n) RETURN count(n) as count"
      )
      node_count = node_count_result["count"] if node_count_result else 0

      rel_count_result = await repository.execute_single(
        "MATCH ()-[r]->() RETURN count(r) as count"
      )
      rel_count = rel_count_result["count"] if rel_count_result else 0

    return {
      "node_count": node_count,
      "relationship_count": rel_count,
    }

  async def _export_database(
    self, graph_id: str, backup_format: BackupFormat, backup_type: BackupType
  ) -> Tuple[bytes, str]:
    """
    Export database based on specified format.

    Returns:
        Tuple of (backup_data, file_extension)
    """
    if backup_format == BackupFormat.CSV:
      return await self._export_to_csv(graph_id, backup_type)
    elif backup_format == BackupFormat.JSON:
      return await self._export_to_json(graph_id, backup_type)
    elif backup_format == BackupFormat.PARQUET:
      return await self._export_to_parquet(graph_id, backup_type)
    elif backup_format == BackupFormat.FULL_DUMP:
      return await self._export_full_dump(graph_id, backup_type)
    else:
      raise ValueError(f"Unsupported backup format: {backup_format}")

  async def _export_to_csv(
    self, graph_id: str, backup_type: BackupType
  ) -> Tuple[bytes, str]:
    """Export database to CSV format."""
    logger.info(f"Exporting graph '{graph_id}' to CSV format")

    repository = await get_universal_repository(graph_id, operation_type="read")

    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir)

      async with repository:
        # For LadybugDB, we need to get schema from our schema loader
        from ...schemas.loader import LadybugSchemaLoader

        try:
          # Get schema information
          schema_loader = LadybugSchemaLoader()
          node_types = schema_loader.list_node_types()
          relationship_types = schema_loader.list_relationship_types()

          logger.info(
            f"Found {len(node_types)} node types and {len(relationship_types)} relationship types"
          )

          # Export nodes by type
          for node_type in node_types:
            csv_file = temp_path / f"nodes_{node_type.lower()}.csv"

            # First check if the table exists and has data
            try:
              count_result = await repository.execute_single(
                f"MATCH (n:{node_type}) RETURN count(n) as count"
              )
              if count_result and count_result.get("count", 0) > 0:
                # LadybugDB COPY TO syntax for exporting
                export_query = f"""
                          COPY (MATCH (n:{node_type}) RETURN n.*)
                          TO '{csv_file}' (header=true)
                          """
                await repository.execute_query(export_query)
                logger.info(
                  f"Exported {count_result['count']} {node_type} nodes to CSV"
                )
            except Exception as e:
              logger.warning(f"Skipping {node_type} nodes: {e}")
              continue

          # Export relationships by type
          for rel_type in relationship_types:
            csv_file = temp_path / f"relationships_{rel_type.lower()}.csv"

            # First check if the relationship exists and has data
            try:
              count_result = await repository.execute_single(
                f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count"
              )
              if count_result and count_result.get("count", 0) > 0:
                # LadybugDB COPY TO syntax for exporting
                export_query = f"""
                          COPY (MATCH ()-[r:{rel_type}]->() RETURN r.*)
                          TO '{csv_file}' (header=true)
                          """
                await repository.execute_query(export_query)
                logger.info(
                  f"Exported {count_result['count']} {rel_type} relationships to CSV"
                )
            except Exception as e:
              logger.warning(f"Skipping {rel_type} relationships: {e}")
              continue

        except Exception as e:
          logger.error(f"Failed to get schema information: {e}")
          # Fallback: try to export known common tables
          common_node_types = ["Entity", "Account", "Transaction", "Report", "Fact"]
          common_rel_types = [
            "HAS_ACCOUNT",
            "HAS_TRANSACTION",
            "HAS_REPORT",
            "CONTAINS_FACT",
          ]

          for node_type in common_node_types:
            csv_file = temp_path / f"nodes_{node_type.lower()}.csv"
            try:
              export_query = f"""
                        COPY (MATCH (n:{node_type}) RETURN n.*)
                        TO '{csv_file}' (header=true)
                        """
              await repository.execute_query(export_query)
            except Exception:
              pass

          for rel_type in common_rel_types:
            csv_file = temp_path / f"relationships_{rel_type.lower()}.csv"
            try:
              export_query = f"""
                        COPY (MATCH ()-[r:{rel_type}]->() RETURN r.*)
                        TO '{csv_file}' (header=true)
                        """
              await repository.execute_query(export_query)
            except Exception:
              pass

      # Create ZIP archive
      zip_file = temp_path / "backup.zip"
      with zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED) as zf:
        for csv_file in temp_path.glob("*.csv"):
          zf.write(csv_file, csv_file.name)

      # Read ZIP file content
      with open(zip_file, "rb") as f:
        backup_data = f.read()

    return backup_data, ".csv.zip"

  async def _export_to_parquet(
    self, graph_id: str, backup_type: BackupType
  ) -> Tuple[bytes, str]:
    """Export database to Parquet format."""
    logger.info(f"Exporting graph '{graph_id}' to Parquet format")

    repository = await get_universal_repository(graph_id, operation_type="read")

    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir)

      async with repository:
        # For LadybugDB, we need to get schema from our schema loader
        from ...schemas.loader import LadybugSchemaLoader

        try:
          # Get schema information
          schema_loader = LadybugSchemaLoader()
          node_types = schema_loader.list_node_types()
          relationship_types = schema_loader.list_relationship_types()

          logger.info(
            f"Found {len(node_types)} node types and {len(relationship_types)} relationship types for Parquet export"
          )

          # Export nodes by type to Parquet
          for node_type in node_types:
            parquet_file = temp_path / f"nodes_{node_type.lower()}.parquet"

            # First check if the table exists and has data
            try:
              count_result = await repository.execute_single(
                f"MATCH (n:{node_type}) RETURN count(n) as count"
              )
              if count_result and count_result.get("count", 0) > 0:
                # LadybugDB COPY TO syntax for exporting to Parquet
                # Note: LadybugDB uses file extension to determine format, not FORMAT parameter
                export_query = f"""
                          COPY (MATCH (n:{node_type}) RETURN n.*)
                          TO '{parquet_file}'
                          """
                await repository.execute_query(export_query)
                logger.info(
                  f"Exported {count_result['count']} {node_type} nodes to Parquet"
                )
            except Exception as e:
              logger.warning(f"Skipping {node_type} nodes: {e}")
              continue

          # Export relationships by type to Parquet
          for rel_type in relationship_types:
            parquet_file = temp_path / f"relationships_{rel_type.lower()}.parquet"

            # First check if the relationship exists and has data
            try:
              count_result = await repository.execute_single(
                f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count"
              )
              if count_result and count_result.get("count", 0) > 0:
                # LadybugDB COPY TO syntax for exporting to Parquet
                # Note: LadybugDB uses file extension to determine format, not FORMAT parameter
                export_query = f"""
                          COPY (MATCH ()-[r:{rel_type}]->() RETURN r.*)
                          TO '{parquet_file}'
                          """
                await repository.execute_query(export_query)
                logger.info(
                  f"Exported {count_result['count']} {rel_type} relationships to Parquet"
                )
            except Exception as e:
              logger.warning(f"Skipping {rel_type} relationships: {e}")
              continue

        except Exception as e:
          logger.error(f"Failed to get schema information: {e}")
          # Fallback: try to export known common tables
          common_node_types = ["Entity", "Account", "Transaction", "Report", "Fact"]
          common_rel_types = [
            "HAS_ACCOUNT",
            "HAS_TRANSACTION",
            "HAS_REPORT",
            "CONTAINS_FACT",
          ]

          for node_type in common_node_types:
            parquet_file = temp_path / f"nodes_{node_type.lower()}.parquet"
            try:
              export_query = f"""
                        COPY (MATCH (n:{node_type}) RETURN n.*)
                        TO '{parquet_file}'
                        """
              await repository.execute_query(export_query)
            except Exception:
              pass

          for rel_type in common_rel_types:
            parquet_file = temp_path / f"relationships_{rel_type.lower()}.parquet"
            try:
              export_query = f"""
                        COPY (MATCH ()-[r:{rel_type}]->() RETURN r.*)
                        TO '{parquet_file}'
                        """
              await repository.execute_query(export_query)
            except Exception:
              pass

      # Create ZIP archive
      zip_file = temp_path / "backup.zip"
      with zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED) as zf:
        for parquet_file in temp_path.glob("*.parquet"):
          zf.write(parquet_file, parquet_file.name)

      # Read ZIP file content
      with open(zip_file, "rb") as f:
        backup_data = f.read()

    return backup_data, ".parquet.zip"

  async def _export_to_json(
    self, graph_id: str, backup_type: BackupType
  ) -> Tuple[bytes, str]:
    """Export database to JSON format."""
    logger.info(f"Exporting graph '{graph_id}' to JSON format")

    repository = await get_universal_repository(graph_id, operation_type="read")

    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir)

      async with repository:
        # For LadybugDB, we need to get schema from our schema loader
        from ...schemas.loader import LadybugSchemaLoader

        try:
          # Get schema information
          schema_loader = LadybugSchemaLoader()
          node_types = schema_loader.list_node_types()
          relationship_types = schema_loader.list_relationship_types()

          logger.info(
            f"Found {len(node_types)} node types and {len(relationship_types)} relationship types for JSON export"
          )

          # Export nodes by type to JSON
          for node_type in node_types:
            json_file = temp_path / f"nodes_{node_type.lower()}.json"

            # First check if the table exists and has data
            try:
              count_result = await repository.execute_single(
                f"MATCH (n:{node_type}) RETURN count(n) as count"
              )
              if count_result and count_result.get("count", 0) > 0:
                # Get all nodes of this type
                nodes_result = await repository.execute_query(
                  f"MATCH (n:{node_type}) RETURN n"
                )

                # Convert to JSON manually since LadybugDB doesn't support direct JSON export
                import json

                nodes_data = []
                for record in nodes_result:
                  node = record.get("n", {})
                  nodes_data.append(node)

                with open(json_file, "w") as f:
                  json.dump(nodes_data, f, indent=2)

                logger.info(
                  f"Exported {count_result['count']} {node_type} nodes to JSON"
                )
            except Exception as e:
              logger.warning(f"Skipping {node_type} nodes: {e}")
              continue

          # Export relationships by type to JSON
          for rel_type in relationship_types:
            json_file = temp_path / f"relationships_{rel_type.lower()}.json"

            # First check if the relationship exists and has data
            try:
              count_result = await repository.execute_single(
                f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count"
              )
              if count_result and count_result.get("count", 0) > 0:
                # Get all relationships of this type with start/end node IDs
                rels_result = await repository.execute_query(
                  f"MATCH (a)-[r:{rel_type}]->(b) RETURN a.id as start_id, r, b.id as end_id"
                )

                # Convert to JSON manually
                import json

                rels_data = []
                for record in rels_result:
                  rel_data = {
                    "start_id": record.get("start_id"),
                    "end_id": record.get("end_id"),
                    "properties": record.get("r", {}),
                  }
                  rels_data.append(rel_data)

                with open(json_file, "w") as f:
                  json.dump(rels_data, f, indent=2)

                logger.info(
                  f"Exported {count_result['count']} {rel_type} relationships to JSON"
                )
            except Exception as e:
              logger.warning(f"Skipping {rel_type} relationships: {e}")
              continue

        except Exception as e:
          logger.error(f"Failed to get schema information: {e}")
          # Fallback: try to export known common tables
          common_node_types = ["Entity", "Account", "Transaction", "Report", "Fact"]
          common_rel_types = [
            "HAS_ACCOUNT",
            "HAS_TRANSACTION",
            "HAS_REPORT",
            "CONTAINS_FACT",
          ]

          import json

          for node_type in common_node_types:
            json_file = temp_path / f"nodes_{node_type.lower()}.json"
            try:
              nodes_result = await repository.execute_query(
                f"MATCH (n:{node_type}) RETURN n"
              )
              nodes_data = [record.get("n", {}) for record in nodes_result]
              with open(json_file, "w") as f:
                json.dump(nodes_data, f, indent=2)
            except Exception:
              pass

          for rel_type in common_rel_types:
            json_file = temp_path / f"relationships_{rel_type.lower()}.json"
            try:
              rels_result = await repository.execute_query(
                f"MATCH (a)-[r:{rel_type}]->(b) RETURN a.id as start_id, r, b.id as end_id"
              )
              rels_data = []
              for record in rels_result:
                rel_data = {
                  "start_id": record.get("start_id"),
                  "end_id": record.get("end_id"),
                  "properties": record.get("r", {}),
                }
                rels_data.append(rel_data)
              with open(json_file, "w") as f:
                json.dump(rels_data, f, indent=2)
            except Exception:
              pass

      # Create ZIP archive
      zip_file = temp_path / "backup.zip"
      with zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED) as zf:
        for json_file in temp_path.glob("*.json"):
          zf.write(json_file, json_file.name)

      # Read ZIP file content
      with open(zip_file, "rb") as f:
        backup_data = f.read()

    return backup_data, ".json.zip"

  async def _export_full_dump(
    self, graph_id: str, backup_type: BackupType
  ) -> Tuple[bytes, str]:
    """Export full database dump via Graph API."""
    logger.info(f"Creating full dump for graph '{graph_id}' via Graph API")

    # Use Graph API to get the database backup
    # This works across containers (worker -> graph-api)
    graph_client = await self.graph_router.get_repository(
      graph_id=graph_id,
      operation_type="read",
    )

    try:
      # Call Graph API backup endpoint
      response = await graph_client.download_backup(graph_id=graph_id)

      if not response or "backup_data" not in response:
        raise RuntimeError(f"Failed to get backup from Graph API for {graph_id}")

      # Get raw binary backup data from Graph API client
      backup_data = response["backup_data"]
      logger.info(
        f"Successfully retrieved backup from Graph API: {len(backup_data)} bytes"
      )

      return backup_data, ".lbug.zip"

    except Exception as e:
      logger.error(f"Failed to create backup via Graph API for {graph_id}: {e}")
      raise

  async def _import_backup_data(
    self,
    graph_id: str,
    backup_data: bytes,
    backup_format: BackupFormat,
    progress_tracker=None,
  ) -> None:
    """Import backup data based on format."""
    if backup_format == BackupFormat.CSV:
      await self._import_from_csv(graph_id, backup_data, progress_tracker)
    elif backup_format == BackupFormat.JSON:
      await self._import_from_json(graph_id, backup_data, progress_tracker)
    elif backup_format == BackupFormat.PARQUET:
      await self._import_from_parquet(graph_id, backup_data, progress_tracker)
    elif backup_format == BackupFormat.FULL_DUMP:
      await self._import_full_dump(graph_id, backup_data, progress_tracker)
    else:
      raise ValueError(f"Unsupported backup format: {backup_format}")

  async def _import_from_csv(
    self, graph_id: str, backup_data: bytes, progress_tracker=None
  ) -> None:
    """Import database from CSV format."""
    logger.info(f"Importing CSV backup to graph '{graph_id}'")

    repository = await get_universal_repository(graph_id, operation_type="write")

    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir)

      # Extract ZIP archive
      zip_file = temp_path / "backup.zip"
      with open(zip_file, "wb") as f:
        f.write(backup_data)

      with zipfile.ZipFile(zip_file, "r") as zf:
        zf.extractall(temp_path)

      async with repository:
        # Import node CSV files
        for csv_file in temp_path.glob("nodes_*.csv"):
          table_name = csv_file.stem.replace("nodes_", "").title()

          import_query = f"""
                    COPY {table_name} FROM '{csv_file}' (HEADER true)
                    """
          await repository.execute_query(import_query)

          if progress_tracker:
            progress_tracker.update_import_progress(
              message=f"Imported {table_name} nodes"
            )

        # Import relationship CSV files
        for csv_file in temp_path.glob("relationships_*.csv"):
          rel_type = csv_file.stem.replace("relationships_", "").upper()

          import_query = f"""
                    COPY {rel_type} FROM '{csv_file}' (HEADER true)
                    """
          await repository.execute_query(import_query)

          if progress_tracker:
            progress_tracker.update_import_progress(
              message=f"Imported {rel_type} relationships"
            )

  async def _import_from_json(
    self, graph_id: str, backup_data: bytes, progress_tracker=None
  ) -> None:
    """Import database from JSON format."""
    logger.info(f"Importing JSON backup to graph '{graph_id}'")

    repository = await get_universal_repository(graph_id, operation_type="write")

    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir)

      # Extract ZIP archive
      zip_file = temp_path / "backup.zip"
      with open(zip_file, "wb") as f:
        f.write(backup_data)

      with zipfile.ZipFile(zip_file, "r") as zf:
        zf.extractall(temp_path)

      async with repository:
        # Import node JSON files
        for json_file in temp_path.glob("nodes_*.json"):
          table_name = json_file.stem.replace("nodes_", "").title()

          import_query = f"""
                    COPY {table_name} FROM '{json_file}'
                    """
          await repository.execute_query(import_query)

          if progress_tracker:
            progress_tracker.update_import_progress(
              message=f"Imported {table_name} nodes"
            )

        # Import relationship JSON files
        for json_file in temp_path.glob("relationships_*.json"):
          rel_type = json_file.stem.replace("relationships_", "").upper()

          import_query = f"""
                    COPY {rel_type} FROM '{json_file}'
                    """
          await repository.execute_query(import_query)

          if progress_tracker:
            progress_tracker.update_import_progress(
              message=f"Imported {rel_type} relationships"
            )

  async def _import_from_parquet(
    self, graph_id: str, backup_data: bytes, progress_tracker=None
  ) -> None:
    """Import database from Parquet format."""
    logger.info(f"Importing Parquet backup to graph '{graph_id}'")

    repository = await get_universal_repository(graph_id, operation_type="write")

    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir)

      # Extract ZIP archive
      zip_file = temp_path / "backup.zip"
      with open(zip_file, "wb") as f:
        f.write(backup_data)

      with zipfile.ZipFile(zip_file, "r") as zf:
        zf.extractall(temp_path)

      async with repository:
        # Import node Parquet files
        for parquet_file in temp_path.glob("nodes_*.parquet"):
          table_name = parquet_file.stem.replace("nodes_", "").title()

          import_query = f"""
                    COPY {table_name} FROM '{parquet_file}'
                    """
          await repository.execute_query(import_query)

          if progress_tracker:
            progress_tracker.update_import_progress(
              message=f"Imported {table_name} nodes"
            )

        # Import relationship Parquet files
        for parquet_file in temp_path.glob("relationships_*.parquet"):
          rel_type = parquet_file.stem.replace("relationships_", "").upper()

          import_query = f"""
                    COPY {rel_type} FROM '{parquet_file}'
                    """
          await repository.execute_query(import_query)

          if progress_tracker:
            progress_tracker.update_import_progress(
              message=f"Imported {rel_type} relationships"
            )

  async def _import_full_dump(
    self,
    graph_id: str,
    backup_data: bytes,
    progress_tracker=None,
    create_system_backup: bool = True,
  ) -> None:
    """Import from full database dump with optional system backup of existing database."""
    logger.info(f"Importing full dump to graph '{graph_id}'")

    # Get target database path
    target_path = MultiTenantUtils.get_database_path_for_graph(graph_id)
    target_dir = os.path.dirname(target_path)

    # Ensure target directory exists
    os.makedirs(target_dir, exist_ok=True)

    # Create system backup of existing database before restore
    if create_system_backup and os.path.exists(target_path):
      logger.info(f"Creating system backup of existing database for graph '{graph_id}'")

      try:
        # Create a temporary directory for the backup process
        with tempfile.TemporaryDirectory() as backup_temp_dir:
          backup_temp_path = Path(backup_temp_dir)

          # Copy existing database to temp location with .bak extension
          final_backup_file = None
          final_backup_dir = None

          if os.path.isfile(target_path):
            # Single file database
            backup_file = backup_temp_path / f"{graph_id}.lbug.bak"
            shutil.copy2(target_path, backup_file)
            # Remove .bak extension for zip
            final_backup_file = backup_temp_path / f"{graph_id}.lbug"
            shutil.move(backup_file, final_backup_file)
          else:
            # Directory-based database
            backup_dir = backup_temp_path / f"{graph_id}.bak"
            shutil.copytree(target_path, backup_dir)
            # Remove .bak extension for zip
            final_backup_dir = backup_temp_path / graph_id
            shutil.move(backup_dir, final_backup_dir)

          # Create ZIP archive of the backup
          timestamp = datetime.now(timezone.utc)
          timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
          system_backup_zip = backup_temp_path / f"system_backup_{timestamp_str}.zip"

          with zipfile.ZipFile(system_backup_zip, "w", zipfile.ZIP_DEFLATED) as zf:
            if os.path.isfile(target_path):
              assert final_backup_file is not None
              zf.write(final_backup_file, f"{graph_id}.lbug")
            else:
              assert final_backup_dir is not None
              for root, dirs, files in os.walk(final_backup_dir):
                for file in files:
                  file_path = Path(root) / file
                  arc_path = file_path.relative_to(backup_temp_path)
                  zf.write(file_path, arc_path)

          # Upload system backup to S3
          with open(system_backup_zip, "rb") as f:
            system_backup_data = f.read()

          # Create metadata for the system backup
          metadata = {
            "backup_type": "system_restore",
            "timestamp": timestamp_str,
            "compression_enabled": True,
            "encryption_enabled": False,
          }

          # Upload to S3 (compression handled internally)
          backup_metadata = await self.s3_adapter.upload_backup(
            graph_id=graph_id,
            backup_data=system_backup_data,
            backup_type="full",
            metadata=metadata,
            timestamp=timestamp,
            file_extension=".lbug.zip",
          )

          success = backup_metadata is not None

          if success:
            logger.info("System backup created successfully")
          else:
            error_msg = f"Failed to upload system backup to S3 for graph '{graph_id}'"
            logger.error(error_msg)
            raise RuntimeError(
              f"System backup failed for graph '{graph_id}' - aborting restore for safety. "
              "Set create_system_backup=False to skip backup and force restore."
            )

      except Exception as e:
        logger.error(f"Error creating system backup for graph '{graph_id}': {str(e)}")
        raise RuntimeError(
          f"Failed to create system backup before restore: {str(e)}. "
          "Aborting restore for safety. Set create_system_backup=False to skip backup and force restore."
        ) from e

    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir)

      # Extract ZIP archive
      zip_file = temp_path / "backup.zip"
      with open(zip_file, "wb") as f:
        f.write(backup_data)

      with zipfile.ZipFile(zip_file, "r") as zf:
        zf.extractall(temp_path)

      # Copy database files to target location
      if (temp_path / f"{graph_id}.lbug").exists():
        # Single file database
        shutil.copy2(temp_path / f"{graph_id}.lbug", target_path)
      else:
        # Directory-based database
        if os.path.exists(target_path):
          shutil.rmtree(target_path)
        shutil.copytree(temp_path / graph_id, target_path)

      if progress_tracker:
        progress_tracker.update_import_progress(message="Restored full database dump")

  async def _ensure_database_exists(self, graph_id: str) -> None:
    """Ensure database exists for import."""
    # Database creation is handled by the repository
    repository = await get_universal_repository(graph_id, operation_type="write")
    async with repository:
      # Database is created automatically when first accessed
      await repository.execute_query("MATCH (n) RETURN count(n) LIMIT 1")
    logger.info(f"Database ensured for graph: {graph_id}")

  async def _drop_database_if_exists(self, graph_id: str) -> None:
    """Drop database if it exists."""
    db_path = MultiTenantUtils.get_database_path_for_graph(graph_id)

    if os.path.exists(db_path):
      if os.path.isfile(db_path):
        os.remove(db_path)
      else:
        shutil.rmtree(db_path)
      logger.info(f"Dropped existing database: {graph_id}")

  async def _validate_backup_integrity(
    self, backup_data: bytes, metadata: BackupMetadata
  ) -> bool:
    """Validate backup data integrity using checksum."""
    import hashlib

    calculated_checksum = hashlib.sha256(backup_data).hexdigest()
    expected_checksum = metadata.checksum

    logger.info(
      f"Integrity check - Data size: {len(backup_data)} bytes, "
      f"Calculated checksum: {calculated_checksum}, "
      f"Expected checksum: {expected_checksum}, "
      f"Match: {calculated_checksum == expected_checksum}"
    )

    return calculated_checksum == expected_checksum

  async def _verify_restore(
    self, graph_id: str, original_metadata: BackupMetadata
  ) -> bool:
    """Verify that restore completed successfully."""
    try:
      # Get current database stats
      current_stats = await self._get_database_stats(graph_id)

      # Compare with original backup metadata
      node_count_match = current_stats["node_count"] == original_metadata.node_count
      rel_count_match = (
        current_stats["relationship_count"] == original_metadata.relationship_count
      )

      if node_count_match and rel_count_match:
        logger.info(f"Restore verification passed for graph: {graph_id}")
        return True
      else:
        logger.warning(
          f"Restore verification failed for graph: {graph_id}. "
          f"Expected nodes: {original_metadata.node_count}, got: {current_stats['node_count']}. "
          f"Expected relationships: {original_metadata.relationship_count}, got: {current_stats['relationship_count']}"
        )
        return False

    except Exception as e:
      logger.error(f"Restore verification error for graph {graph_id}: {e}")
      return False

  async def export_backup(
    self, backup_metadata: BackupMetadata, export_format: str = "original"
  ) -> Optional[bytes]:
    """
    Export backup data for download.

    Args:
        backup_metadata: Metadata of the backup to export
        export_format: Export format (original, csv, json)

    Returns:
        bytes: Backup data if exportable, None if encryption prevents export
    """
    # Check if backup is exportable
    if backup_metadata.metadata.get(
      "encryption_enabled"
    ) and not backup_metadata.metadata.get("allow_export", True):
      logger.warning(
        f"Cannot export encrypted backup {backup_metadata.s3_key} - export not allowed"
      )
      return None

    try:
      # Download backup data
      if backup_metadata.s3_key:
        backup_data = await self.s3_adapter.download_backup_by_key(
          backup_metadata.s3_key
        )
      else:
        backup_data = await self.s3_adapter.download_backup_by_timestamp(
          graph_id=backup_metadata.graph_id,
          timestamp=backup_metadata.timestamp,
          backup_type=backup_metadata.backup_type,
        )

      # Validate integrity
      if not await self._validate_backup_integrity(backup_data, backup_metadata):
        raise ValueError("Backup integrity check failed")

      # Convert format if requested
      if export_format != "original":
        backup_data = await self._convert_backup_to_format(
          backup_data, export_format, backup_metadata
        )

      logger.info(
        f"Successfully exported backup {backup_metadata.s3_key} in {export_format} format"
      )
      return backup_data

    except Exception as e:
      logger.error(f"Failed to export backup {backup_metadata.s3_key}: {e}")
      raise

  async def _convert_backup_to_format(
    self, backup_data: bytes, target_format: str, metadata: BackupMetadata
  ) -> bytes:
    """
    Convert backup data to different format.

    Args:
        backup_data: Original backup data
        target_format: Target format (csv, json)
        metadata: Backup metadata

    Returns:
        bytes: Converted data
    """
    if target_format == "original":
      return backup_data

    # For now, return original data - format conversion can be implemented later
    logger.info(
      f"Format conversion to {target_format} not yet implemented, returning original"
    )
    return backup_data

  def health_check(self) -> Dict[str, Any]:
    """Perform health check on backup system."""
    s3_health = self.s3_adapter.health_check()

    # Test graph database connectivity via repository
    try:
      # Use default graph for health check
      from ...middleware.graph import get_universal_repository

      async def test_connection():
        repository = await get_universal_repository("default", operation_type="read")
        with repository:
          await repository.execute_single("RETURN 1 as test")
        return True

      # Run async test, handling the case where we're already in an event loop
      import asyncio

      try:
        # Try to get the current event loop
        asyncio.get_running_loop()
        # If we're in an event loop, we can't use asyncio.run()
        # Instead, we'll skip the async test in this case to avoid the warning
        logger.info("Already in event loop, skipping async graph health check")
        graph_healthy = True  # Assume healthy to avoid blocking
      except RuntimeError:
        # No event loop running, safe to use asyncio.run()
        graph_healthy = asyncio.run(test_connection())

    except Exception as e:
      graph_healthy = False
      logger.error(f"Graph database health check failed: {e}")

    return {
      "status": "healthy"
      if s3_health["status"] == "healthy" and graph_healthy
      else "unhealthy",
      "s3": s3_health,
      "graph_database": {"status": "healthy" if graph_healthy else "unhealthy"},
    }


# Factory function for creating backup manager
def create_backup_manager(**kwargs) -> BackupManager:
  """
  Factory function to create backup manager with default configuration.

  Args:
      **kwargs: Optional configuration overrides

  Returns:
      BackupManager: Configured manager instance
  """
  return BackupManager(**kwargs)
