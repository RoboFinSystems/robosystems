"""
LadybugDB version migration service.

Handles export and import of all databases on an instance for
safe version upgrades. Uses EXPORT DATABASE / IMPORT DATABASE
commands with Parquet format.

The export runs on the old version (pre-deploy), the import runs
on the new version (post-deploy). EBS volume persists the exported
Parquet files across container swaps.
"""

import json
import os
import shutil
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
import ladybug as lbug
from boto3.s3.transfer import TransferConfig

from robosystems.config import env
from robosystems.config.storage.graph import get_backup_key
from robosystems.graph_api.core.task_manager import migration_task_manager
from robosystems.logger import logger

# Migration state — health endpoint returns 503 while import is running
_migration_in_progress = False
_export_in_progress = False
_migration_lock = threading.Lock()
_active_task_id: str | None = None


def is_migration_in_progress() -> bool:
  """Check if a migration import is currently running."""
  return _migration_in_progress


def _set_migration_in_progress(value: bool, task_id: str | None = None) -> None:
  """Set migration state (thread-safe)."""
  global _migration_in_progress, _active_task_id
  with _migration_lock:
    _migration_in_progress = value
    _active_task_id = task_id if value else None


def _try_start_export() -> bool:
  """Atomically check and set export state. Returns True if acquired."""
  global _export_in_progress
  with _migration_lock:
    if _export_in_progress:
      return False
    _export_in_progress = True
    return True


def _clear_export_in_progress() -> None:
  """Clear export state."""
  global _export_in_progress
  with _migration_lock:
    _export_in_progress = False


def _try_start_import(task_id: str) -> bool:
  """Atomically check and set import state. Returns True if acquired."""
  global _migration_in_progress, _active_task_id
  with _migration_lock:
    if _migration_in_progress:
      return False
    _migration_in_progress = True
    _active_task_id = task_id
    return True


def get_active_task_id() -> str | None:
  """Get the currently active migration task ID."""
  return _active_task_id


class MigrationService:
  """Export and import databases for version migration."""

  def __init__(self, base_path: Path | None = None):
    self.base_path = base_path or Path(env.LBUG_DATABASE_PATH)
    self.exports_path = self.base_path / "exports"
    self.manifest_path = self.base_path / "migration.json"

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

  def _upload_system_backup(
    self,
    db_file: Path,
    graph_id: str,
    source_version: str,
    target_version: str,
    bucket: str,
  ) -> str:
    """Upload .lbug file to S3 as a system backup before migration.

    Returns:
        S3 key of the uploaded backup
    """
    timestamp = datetime.now(UTC)
    s3_key = get_backup_key(graph_id, "system", timestamp, extension=".lbug")
    # The bucket is passed in by the caller. The migration op resolves
    # USER_DATA_BUCKET in the worker (where it is set) and forwards it, matching
    # the backup/restore endpoints, which also take the bucket from the request.
    # The graph-api writer container does not set USER_DATA_BUCKET, so reading it
    # here would resolve to the bogus default and 404 on upload.

    s3_client = self._get_s3_client()
    transfer_config = TransferConfig(
      multipart_chunksize=100 * 1024 * 1024,
      multipart_threshold=100 * 1024 * 1024,
      max_concurrency=4,
    )

    db_size = db_file.stat().st_size
    logger.info(
      f"Uploading system backup for {graph_id} "
      f"({db_size / 1024 / 1024:.1f} MB) to s3://{bucket}/{s3_key}"
    )

    s3_client.upload_file(
      str(db_file),
      bucket,
      s3_key,
      Config=transfer_config,
      ExtraArgs={
        "Metadata": {
          "backup_type": "system",
          "source_version": source_version,
          "target_version": target_version,
          "created_at": timestamp.isoformat(),
          "original_size": str(db_size),
        },
      },
    )

    logger.info(f"System backup uploaded: s3://{bucket}/{s3_key}")
    return s3_key

  def _discover_databases(self) -> list[str]:
    """Find all .lbug database files on this instance."""
    if not self.base_path.exists():
      return []
    databases = []
    for f in sorted(self.base_path.iterdir()):
      if f.suffix == ".lbug" and f.is_file():
        databases.append(f.stem)
    return databases

  def _get_node_count(self, conn) -> int:
    """Count total nodes across all node tables."""
    node_count = 0
    result = conn.execute("CALL show_tables() RETURN *")
    tables = []
    while result.has_next():
      row = result.get_next()
      tables.append((row[1], row[2]))

    for name, ttype in tables:
      if ttype == "NODE":
        try:
          cr = conn.execute(f"MATCH (n:{name}) RETURN count(n) as c")
          if cr.has_next():
            node_count += cr.get_next()[0]
        except Exception as e:
          logger.warning(f"Failed to count nodes in {name}: {e}")
    return node_count

  def _get_rel_table_count(self, conn) -> int:
    """Count relationship tables."""
    count = 0
    result = conn.execute("CALL show_tables() RETURN *")
    while result.has_next():
      row = result.get_next()
      if row[2] == "REL":
        count += 1
    return count

  def _check_disk_space(self, databases: list[str]) -> None:
    """Verify sufficient disk space for exports."""
    total_db_size = 0
    for db_name in databases:
      db_file = self.base_path / f"{db_name}.lbug"
      if db_file.exists():
        total_db_size += db_file.stat().st_size

    # Parquet exports are typically 25-60% of .lbug size, but be conservative
    estimated_export_size = total_db_size

    stat = os.statvfs(self.base_path)
    available = stat.f_bavail * stat.f_frsize

    if available < estimated_export_size:
      raise RuntimeError(
        f"Insufficient disk space for export. "
        f"Available: {available / 1024 / 1024:.0f} MB, "
        f"Estimated needed: {estimated_export_size / 1024 / 1024:.0f} MB"
      )

  async def export_all_databases(
    self, task_id: str, source_version: str, target_version: str, bucket: str
  ) -> None:
    """
    Background task: export all databases to Parquet for version migration.

    1. Check disk space
    2. Discover all .lbug files
    3. For each database: EXPORT DATABASE to exports/{graph_id}/
    4. Write migration.json manifest
    """
    if not _try_start_export():
      await migration_task_manager.fail_task(task_id, "Export already in progress")
      return

    try:
      await migration_task_manager.update_task(
        task_id,
        status="running",
        started_at=datetime.now(UTC).isoformat(),
      )

      databases = self._discover_databases()
      if not databases:
        await migration_task_manager.complete_task(
          task_id, result={"message": "No databases found to export", "databases": []}
        )
        return

      logger.info(
        f"Migration export: {len(databases)} databases, "
        f"{source_version} -> {target_version}"
      )

      self._check_disk_space(databases)

      # Clean any previous export
      if self.exports_path.exists():
        shutil.rmtree(self.exports_path)

      manifest_databases: list[dict[str, Any]] = []

      for i, db_name in enumerate(databases):
        db_file = self.base_path / f"{db_name}.lbug"
        export_dir = self.exports_path / db_name

        logger.info(f"Exporting {db_name} ({i + 1}/{len(databases)})")

        db = None
        conn = None
        try:
          db = lbug.Database(str(db_file), read_only=True)
          conn = lbug.Connection(db)

          # Collect counts for verification
          node_count = self._get_node_count(conn)
          rel_tables = self._get_rel_table_count(conn)

          # Export to Parquet
          conn.execute(f"EXPORT DATABASE '{export_dir}'")
        except Exception as e:
          logger.error(f"Failed to export {db_name}: {e}")
          raise RuntimeError(f"Export failed for {db_name}: {e}")
        finally:
          if conn:
            conn.close()
          if db:
            db.close()

        # Upload system backup to S3 (safety net)
        db_size = db_file.stat().st_size
        system_backup_key = self._upload_system_backup(
          db_file, db_name, source_version, target_version, bucket
        )

        manifest_databases.append(
          {
            "graph_id": db_name,
            "export_path": f"exports/{db_name}",
            "node_count": node_count,
            "relationship_tables": rel_tables,
            "size_bytes": db_size,
            "system_backup_key": system_backup_key,
            "exported_at": datetime.now(UTC).isoformat(),
          }
        )

        logger.info(
          f"Exported {db_name}: {node_count} nodes, "
          f"{rel_tables} rel tables, {db_size / 1024 / 1024:.1f} MB"
        )

        # Update progress
        progress = int((i + 1) / len(databases) * 100)
        await migration_task_manager.update_task(
          task_id,
          progress_percent=progress,
          metadata={
            "operation": "export",
            "source_version": source_version,
            "target_version": target_version,
            "current_database": db_name,
            "databases_completed": i + 1,
            "databases_total": len(databases),
          },
        )

      # Write manifest
      manifest = {
        "source_version": source_version,
        "target_version": target_version,
        "created_at": datetime.now(UTC).isoformat(),
        "export_format": "parquet",
        "databases": manifest_databases,
      }
      with open(self.manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

      logger.info(
        f"Migration export complete: {len(databases)} databases, "
        f"manifest written to {self.manifest_path}"
      )

      await migration_task_manager.complete_task(
        task_id,
        result={
          "databases_exported": len(databases),
          "manifest_path": str(self.manifest_path),
          "source_version": source_version,
          "target_version": target_version,
        },
      )

    except Exception as e:
      logger.error(f"Migration export failed: {e}")
      await migration_task_manager.fail_task(task_id, str(e))
    finally:
      _clear_export_in_progress()

  async def import_all_databases(self, task_id: str) -> None:
    """
    Background task: import all databases from a previous export.

    1. Set _migration_in_progress (health -> 503)
    2. Read migration.json
    3. For each database: rename .lbug -> .pre-migration, create fresh, IMPORT DATABASE
    4. Verify node counts
    5. On success: cleanup and clear flag
    6. On failure: restore .pre-migration files and clear flag
    """
    if not _try_start_import(task_id):
      await migration_task_manager.fail_task(task_id, "Import already in progress")
      return

    renamed_files: list[tuple[Path, Path]] = []

    try:
      logger.info("Migration import started - health returning 503")

      await migration_task_manager.update_task(
        task_id,
        status="running",
        started_at=datetime.now(UTC).isoformat(),
      )

      # Read manifest
      if not self.manifest_path.exists():
        logger.info("No migration.json found - nothing to import")
        _set_migration_in_progress(False)
        await migration_task_manager.complete_task(
          task_id, result={"message": "No migration pending"}
        )
        return

      with open(self.manifest_path) as f:
        manifest = json.load(f)

      databases = manifest.get("databases", [])
      if not databases:
        _set_migration_in_progress(False)
        await migration_task_manager.complete_task(
          task_id, result={"message": "Manifest has no databases"}
        )
        return

      logger.info(
        f"Migration import: {len(databases)} databases from "
        f"{manifest['source_version']} -> {manifest['target_version']}"
      )

      # Sort: shorter graph_ids first (parents before subgraphs)
      databases.sort(key=lambda d: len(d["graph_id"]))

      results: list[dict[str, Any]] = []

      for i, entry in enumerate(databases):
        db_name = entry["graph_id"]
        export_dir = self.base_path / entry["export_path"]
        db_file = self.base_path / f"{db_name}.lbug"
        pre_migration_file = Path(f"{db_file}.pre-migration")
        wal_file = Path(f"{db_file}.wal")

        logger.info(f"Importing {db_name} ({i + 1}/{len(databases)})")

        # Validate export directory exists
        if not export_dir.exists():
          raise RuntimeError(f"Export directory missing for {db_name}: {export_dir}")

        # Rename old .lbug -> .pre-migration
        if db_file.exists():
          os.rename(db_file, pre_migration_file)
          renamed_files.append((pre_migration_file, db_file))
          logger.info(f"Renamed {db_name}.lbug -> .pre-migration")

        # Remove WAL
        if wal_file.exists():
          os.remove(wal_file)

        # Create fresh database and import
        start_time = time.time()
        db = None
        conn = None
        try:
          db = lbug.Database(str(db_file))
          conn = lbug.Connection(db)
          conn.execute(f"IMPORT DATABASE '{export_dir}'")

          # Verify node counts
          actual_nodes = self._get_node_count(conn)
          expected_nodes = entry.get("node_count", 0)
          duration = time.time() - start_time
        except Exception as e:
          raise RuntimeError(f"IMPORT DATABASE failed for {db_name}: {e}")
        finally:
          if conn:
            conn.close()
          if db:
            db.close()

        if expected_nodes > 0 and actual_nodes != expected_nodes:
          # Allow small tolerance for concurrent writes between export and import
          tolerance = max(1, int(expected_nodes * 0.001))
          if abs(actual_nodes - expected_nodes) > tolerance:
            raise RuntimeError(
              f"Node count mismatch for {db_name}: "
              f"expected {expected_nodes}, got {actual_nodes}"
            )

        new_size = db_file.stat().st_size
        results.append(
          {
            "graph_id": db_name,
            "node_count": actual_nodes,
            "expected_nodes": expected_nodes,
            "verified": actual_nodes == expected_nodes,
            "size_bytes": new_size,
            "import_duration_seconds": round(duration, 2),
          }
        )

        logger.info(
          f"Imported {db_name}: {actual_nodes} nodes "
          f"(expected {expected_nodes}), {duration:.1f}s, "
          f"{new_size / 1024 / 1024:.1f} MB"
        )

        # Update progress
        progress = int((i + 1) / len(databases) * 100)
        await migration_task_manager.update_task(
          task_id,
          progress_percent=progress,
          metadata={
            "operation": "import",
            "current_database": db_name,
            "databases_completed": i + 1,
            "databases_total": len(databases),
          },
        )

      # Success: cleanup
      os.remove(self.manifest_path)
      if self.exports_path.exists():
        shutil.rmtree(self.exports_path)

      _set_migration_in_progress(False)

      logger.info(f"Migration import complete: {len(databases)} databases imported")

      await migration_task_manager.complete_task(
        task_id,
        result={
          "databases_imported": len(databases),
          "results": results,
          "source_version": manifest.get("source_version"),
          "target_version": manifest.get("target_version"),
        },
      )

    except Exception as e:
      logger.error(f"Migration import failed: {e}")

      # Restore all .pre-migration files
      for pre_migration, original in renamed_files:
        try:
          if pre_migration.exists():
            # Remove any partial .lbug that was created
            if original.exists():
              os.remove(original)
            os.rename(pre_migration, original)
            logger.info(f"Restored {original.name} from .pre-migration")
        except Exception as restore_err:
          logger.error(f"Failed to restore {original.name}: {restore_err}")

      _set_migration_in_progress(False)
      await migration_task_manager.fail_task(task_id, str(e))

  def get_status(self) -> dict[str, Any]:
    """Check migration status on this instance."""
    manifest = None
    if self.manifest_path.exists():
      try:
        with open(self.manifest_path) as f:
          manifest = json.load(f)
      except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"Failed to read migration manifest: {e}")

    # Find .pre-migration files
    pre_migration_files = []
    if self.base_path.exists():
      for f in sorted(self.base_path.iterdir()):
        if f.name.endswith(".pre-migration"):
          pre_migration_files.append(f.name)

    return {
      "migration_pending": manifest is not None,
      "migration_in_progress": is_migration_in_progress(),
      "manifest": manifest,
      "pre_migration_files": pre_migration_files,
      "active_task_id": get_active_task_id(),
    }

  def cleanup_pre_migration_files(self) -> dict[str, Any]:
    """Delete .pre-migration files after successful migration.

    These files are the original .lbug databases from the old version.
    System backups in S3 provide the safety net after cleanup.

    Returns:
        Cleanup result with files deleted and total size freed
    """
    deleted = []
    total_freed = 0

    if not self.base_path.exists():
      return {"files_deleted": [], "total_freed_bytes": 0}

    for f in sorted(self.base_path.iterdir()):
      if f.name.endswith(".pre-migration"):
        size = f.stat().st_size
        os.remove(f)
        deleted.append(f.name)
        total_freed += size
        logger.info(f"Deleted {f.name} ({size / 1024 / 1024:.1f} MB)")

    logger.info(
      f"Cleanup complete: {len(deleted)} files, "
      f"{total_freed / 1024 / 1024:.1f} MB freed"
    )

    return {
      "files_deleted": deleted,
      "total_freed_bytes": total_freed,
    }
