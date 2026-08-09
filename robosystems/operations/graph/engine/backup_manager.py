"""Backup and restore for graph databases, backed by S3.

Two families of backup, and they are not interchangeable:

- ``FULL_DUMP`` is the only restorable format. It is the on-disk database, not
  a re-importable export.
- ``CSV`` / ``JSON`` / ``PARQUET`` are export formats. They can be downloaded
  and converted between each other, but never restored.

Backups are not encrypted at the application layer. Objects are written with
S3 server-side encryption (SSE-AES256) and served over TLS through short-lived
signed URLs; the plaintext is a ``.lbug`` database the owner can open directly,
which is the point of offering a download at all. Storage lives in
:mod:`robosystems.operations.aws.s3`.
"""

import asyncio
import json
import os
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from robosystems.config.storage.graph import get_download_extension
from robosystems.operations.aws.s3 import BackupMetadata, S3BackupAdapter

from ....logger import logger
from ....middleware.graph import get_universal_repository
from ....middleware.graph.utils import MultiTenantUtils


class BackupPayloadMismatch(RuntimeError):
  """The archive's contents disagree with the database it claims to back up."""


# Name of the manifest inside a full-dump archive, mirroring the producer in
# ``graph_api/routers/databases/restore.py``.
BACKUP_MANIFEST_NAME = "backup-manifest.json"


def _restore_memory_store(graph_id: str, extracted: Path) -> str:
  """Apply the archive's memory claim to the graph's live memory store.

  The manifest is a **tri-state**, and the third state is carried by the
  manifest's *absence*:

  - ``included`` — the archive holds a memory store; replace the live one.
  - ``absent``   — a memory-aware backup of a graph that had none. **Leave the
    live store alone.** Memory has no upstream, so the asymmetry favours
    keeping data over matching the snapshot exactly; the same fail-closed
    instinct that made restore abort on a failed safety backup.
  - *no manifest* — the archive predates memory capture and makes **no claim
    either way**. Also leave the live store alone: deleting data on the
    strength of a format gap is the worst available reading.

  Only ``included`` writes. Returns what happened, for the operation log —
  a restore that silently did nothing to memory is exactly what this contract
  exists to prevent.
  """
  from robosystems.utils.path_validation import get_lance_index_path

  manifest_path = extracted / BACKUP_MANIFEST_NAME
  if not manifest_path.exists():
    logger.info(
      f"Backup for '{graph_id}' carries no manifest; leaving existing memory "
      f"store untouched (archive makes no claim)"
    )
    return "unchanged (no claim)"

  try:
    manifest = json.loads(manifest_path.read_text())
  except Exception as e:
    logger.warning(f"Unreadable backup manifest for '{graph_id}': {e}")
    return "unchanged (unreadable manifest)"

  claim = manifest.get("memory")
  if claim != "included":
    logger.info(
      f"Backup for '{graph_id}' records memory as '{claim}'; leaving existing "
      f"memory store untouched"
    )
    return f"unchanged ({claim})"

  source = extracted / "memory"
  if not source.is_dir():
    # The manifest says included but the payload has no memory directory.
    # Refuse rather than guess: replacing with nothing would delete a store the
    # archive claims to contain, and skipping silently would leave a restore
    # reporting success over a claim it did not honour.
    raise BackupPayloadMismatch(
      f"Backup for '{graph_id}' records memory as included but the archive "
      f"contains no memory directory."
    )

  destination = get_lance_index_path(graph_id)
  if destination.exists():
    shutil.rmtree(destination)
  destination.parent.mkdir(parents=True, exist_ok=True)
  shutil.copytree(source, destination)

  logger.info(f"Restored semantic memory store for graph '{graph_id}'")
  return "replaced"


def _reconcile_payload_against_stats(
  graph_id: str,
  stats: dict[str, Any],
  payload: dict[str, Any] | None,
) -> dict[str, Any] | None:
  """Check the archive holds what the record is about to claim.

  The record's counts come from a live query while the archive is produced
  separately, and nothing used to compare them — so a backup could report
  ``completed, node_count: 23`` over an archive containing nothing at all.

  **The check is deliberately not strict equality.** The live count is read
  before the export, so any write in between moves the numbers legitimately;
  failing on that would make a nightly backup flap red on an active graph, and
  a job that cries wolf is one nobody reads. Instead:

  - **an empty archive over a non-empty database fails the backup.** That is
    the entire dropped-WAL failure class, and it has no benign explanation — a
    graph does not go from thousands of nodes to zero between two adjacent
    operations.
  - **any other difference is recorded, not raised.** It rides in the backup
    metadata as ``payload_delta`` so drift is visible and reviewable without
    being load-bearing.

  Skipped when the payload was not measured: export formats have no artifact to
  reconcile, and a graph node predating the measurement sends no counts. Both
  arrive as ``None``, meaning "no claim" — treating that as zero would fail
  every backup taken against an older node.
  """
  if payload is None:
    return None

  payload_nodes = payload.get("node_count")
  payload_rels = payload.get("relationship_count")
  if payload_nodes is None or payload_rels is None:
    logger.info(
      f"Backup payload for '{graph_id}' was not measured; skipping reconciliation"
    )
    return None

  live_nodes = stats.get("node_count", 0) or 0
  live_rels = stats.get("relationship_count", 0) or 0

  if payload_nodes == 0 and payload_rels == 0 and (live_nodes or live_rels):
    raise BackupPayloadMismatch(
      f"Backup archive for '{graph_id}' is empty while the database holds "
      f"{live_nodes} nodes / {live_rels} relationships. Refusing to record a "
      f"completed backup over an archive with no contents."
    )

  if payload_nodes != live_nodes or payload_rels != live_rels:
    delta = {
      "payload_node_count": payload_nodes,
      "payload_relationship_count": payload_rels,
      "live_node_count": live_nodes,
      "live_relationship_count": live_rels,
    }
    logger.warning(
      f"Backup archive for '{graph_id}' differs from the live database "
      f"({payload_nodes}/{payload_rels} vs {live_nodes}/{live_rels}); "
      f"expected when the graph is written during the backup"
    )
    return delta

  return None


def restore_unsupported_reason(graph_id: str, db) -> tuple[int, str] | None:
  """Why this graph can't be restored to as ``(status_code, detail)``, or None.

  Restore is a graph-type property, not a property of an individual backup.
  Shared repositories are platform-managed and download-only; entity graphs are
  a projection of the extensions OLTP database, so overwriting one with a
  snapshot desynchronizes it from its source of truth.

  Subgraphs are the exception to the entity rule, and they inherit
  ``graph_type`` from their parent. Only the parent is materialized —
  ``materialize`` refuses subgraphs outright — so a subgraph is a separate
  database written directly via Cypher with no upstream to rebuild from.
  Refusing restore there would leave it with no recovery path at all.

  Fails closed on an unresolvable graph: a restore is destructive, so an
  unknown graph type is a refusal rather than a default-allow.

  The status code is vestigial — this is no longer reachable from HTTP, since
  restore is operator-run rather than customer-facing. It is retained so the
  reason reads identically wherever the guard is surfaced.
  """
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
  from robosystems.models.core import Graph

  if is_shared_repository_or_subgraph(graph_id):
    return (
      403,
      f"Restore operations are not allowed on shared repository '{graph_id}'. "
      "Shared repositories are platform-managed and download-only.",
    )

  graph_record = Graph.get_by_id(graph_id, db)
  if graph_record is None:
    return (
      400,
      f"Graph '{graph_id}' was not found in the registry, so its type cannot be "
      "confirmed. Restore is refused rather than attempted.",
    )

  if graph_record.graph_type == "entity" and not graph_record.is_subgraph:
    return (
      400,
      "Cannot restore backups for entity graphs. The graph is automatically "
      "materialized from the extensions database.",
    )

  return None


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
  timestamp: datetime | None = None
  schedule: str | None = None
  retention_days: int = 90
  compression: bool = True

  def __post_init__(self):
    """Fill in the timestamp and reject invalid formats."""
    if self.timestamp is None:
      self.timestamp = datetime.now(UTC)

    if self.backup_format not in BackupFormat:
      raise ValueError(f"Invalid backup_format: {self.backup_format}")

    if self.backup_type not in BackupType:
      raise ValueError(f"Invalid backup_type: {self.backup_type}")

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
  # Snapshot the existing database to S3 before overwriting it. Defaults on:
  # a restore is destructive, and a failed snapshot aborts it rather than
  # leaving the old data unrecoverable.
  create_system_backup: bool = True
  progress_tracker: Any | None = None

  def __post_init__(self):
    """Reject any format other than a full dump."""
    if self.backup_format != BackupFormat.FULL_DUMP:
      raise ValueError(
        "Restore is only supported for full dump backups. "
        "CSV, JSON, and Parquet backups are intended for export/import workflows."
      )


class BackupManager:
  """Manager for graph database backups with multiple format support."""

  def __init__(
    self,
    s3_adapter: S3BackupAdapter | None = None,
    graph_router=None,
  ):
    """Both collaborators are optional and built lazily on first use."""
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
  ) -> str | None:
    """Presign a download URL, or None when the backup can't be served."""
    try:
      from robosystems.database import session as SessionLocal
      from robosystems.models.core.graph.graph_backup import GraphBackup

      db_session = SessionLocal()
      try:
        # Scope the lookup to graph_id — access to graph_id is authorized by
        # the route dependency, but the backup_id is caller-supplied, so a
        # bare get_by_id would let one graph fetch another graph's backup.
        backup = GraphBackup.get_by_id_and_graph(backup_id, graph_id, db_session)

        if not backup:
          logger.warning(
            f"Backup {backup_id} not found in database for graph {graph_id}"
          )
          return None

        if not backup.is_completed:
          logger.warning(
            f"Backup {backup_id} is not completed (status: {backup.status})"
          )
          return None

        # Use completed_at for filename timestamp — for shared repos the record
        # is upserted so created_at stays fixed while completed_at reflects
        # the actual publish date
        timestamp = backup.completed_at or backup.created_at
        timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
        is_r2 = (
          backup.backup_metadata
          and isinstance(backup.backup_metadata, dict)
          and backup.backup_metadata.get("storage") == "r2"
        )
        # Derive the served extension from the stored key rather than the
        # storage backend, so the filename describes what is inside it
        # (".lbug.zip", not a bare ".zip" that hides the payload).
        extension = get_download_extension(backup.s3_key)
        filename = f"{graph_id}_{timestamp_str}{extension}"

        import asyncio

        if is_r2:
          import boto3

          from robosystems.config import env

          r2_config = env.get_r2_config()
          if not r2_config:
            raise RuntimeError("R2 not configured for R2-backed backup download")
          presign_client = boto3.client("s3", **r2_config)
        else:
          presign_client = self.s3_adapter.s3_client

        url = await asyncio.get_event_loop().run_in_executor(
          None,
          lambda: presign_client.generate_presigned_url(
            "get_object",
            Params={
              "Bucket": backup.s3_bucket,
              "Key": backup.s3_key,
              "ResponseContentDisposition": f'attachment; filename="{filename}"',
            },
            ExpiresIn=expires_in,
          ),
        )

        # Dev only: the LocalStack container hostname isn't browser-reachable.
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
    self, graph_id: str, backup_id: str, target_format: str | None = None
  ) -> tuple[bytes, str, str]:
    """Fetch backup bytes as ``(data, content_type, filename)``.

    Converts to ``target_format`` when it differs from what was stored.
    """
    try:
      backup_metadata = await self.s3_adapter.get_backup_metadata(graph_id, backup_id)

      if not backup_metadata:
        raise ValueError(f"Backup {backup_id} not found")

      backup_data = await self.s3_adapter.download_backup(graph_id, backup_id)

      original_format = backup_metadata.get("backup_format", "unknown")

      if target_format and target_format != original_format:
        backup_data, content_type, filename = await self._convert_backup_format(
          backup_data, original_format, target_format, backup_id
        )
      else:
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
  ) -> tuple[bytes, str, str]:
    """Convert between formats, returning ``(data, content_type, filename)``.

    Only csv→json, json→csv and full_dump→zip are supported; anything else
    raises.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir)

      original_file = temp_path / f"original_{backup_id}"
      original_file.write_bytes(backup_data)

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
  ) -> tuple[str, str]:
    """Content type and download filename for a stored backup format."""
    format_map = {
      "csv": ("text/csv", f"{backup_id}.csv.zip"),
      "json": ("application/json", f"{backup_id}.json.zip"),
      "parquet": ("application/octet-stream", f"{backup_id}.parquet.zip"),
      "full_dump": ("application/zip", f"{backup_id}_database.lbug.zip"),
    }

    return format_map.get(format, ("application/octet-stream", f"{backup_id}.zip"))

  async def _convert_csv_to_json(self, csv_file: Path) -> bytes:
    """Convert a CSV backup to a JSON record array."""
    import pandas as pd

    df = pd.read_csv(csv_file)

    json_data = df.to_json(orient="records", indent=2)

    return (json_data or "").encode("utf-8")

  async def _convert_json_to_csv(self, json_file: Path) -> bytes:
    """Convert a JSON backup to CSV."""
    import json

    import pandas as pd

    with open(json_file) as f:
      json_data = json.load(f)

    df = pd.DataFrame(json_data)
    csv_data = df.to_csv(index=False)

    return csv_data.encode("utf-8")

  async def _convert_full_dump_to_zip(self, dump_file: Path) -> bytes:
    """Wrap a full dump in a ZIP holding ``database/*.lbug`` plus metadata."""
    import io
    import zipfile

    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
      zip_file.write(dump_file, f"database/{dump_file.stem}.lbug")

      metadata = {
        "export_timestamp": datetime.now(UTC).isoformat(),
        "database_type": "ladybug",
        "format": "full_dump",
        "contents": ["database_files", "metadata"],
      }

      zip_file.writestr("metadata.json", json.dumps(metadata, indent=2).encode("utf-8"))

    return zip_buffer.getvalue()

  async def create_backup(self, backup_job: BackupJob) -> BackupMetadata:
    """Export a graph and upload it, returning the stored metadata.

    ``backup_job.timestamp`` becomes the S3 key for both the payload and its
    metadata sidecar, so it must be stable for the life of the job.
    """
    start_time = asyncio.get_event_loop().time()
    graph_id = backup_job.graph_id

    timestamp_str = backup_job.timestamp.isoformat() if backup_job.timestamp else "auto"
    logger.info(
      f"Starting {backup_job.backup_format.value} backup for graph '{graph_id}' "
      f"with timestamp: {timestamp_str}"
    )

    try:
      stats = await self._get_database_stats(graph_id)

      backup_data, file_extension, payload = await self._export_database(
        graph_id, backup_job.backup_format, backup_job.backup_type
      )

      payload_delta = _reconcile_payload_against_stats(graph_id, stats, payload)

      backup_duration = asyncio.get_event_loop().time() - start_time

      # The record describes the ARCHIVE, not the live database, whenever the
      # archive was measured. This matters because reconciliation deliberately
      # tolerates drift: a backup taken while writes land records live=100 over
      # an archive holding 98, and `_verify_restore` compares a restored
      # database against these numbers. Recording the live count would make
      # every drifted backup fail verification *after* overwriting the target.
      # The live counts are not lost — they ride in `payload_delta` below.
      measured_nodes = (payload or {}).get("node_count")
      measured_rels = (payload or {}).get("relationship_count")

      metadata = {
        "node_count": (
          measured_nodes if measured_nodes is not None else stats["node_count"]
        ),
        "relationship_count": (
          measured_rels if measured_rels is not None else stats["relationship_count"]
        ),
        "backup_duration_seconds": backup_duration,
        "backup_format": backup_job.backup_format.value,
        "database_engine": "graph",
        "compression_enabled": backup_job.compression,
      }
      if payload and payload.get("memory") is not None:
        metadata["memory"] = payload["memory"]
      if payload_delta is not None:
        metadata["payload_delta"] = payload_delta

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
    """Restore a graph from a full-dump backup.

    Steps in order: download, checksum-verify, optionally drop/create the
    target, import, then optionally verify. A failed checksum aborts before
    anything touches the target database; a failed post-restore verification
    returns False with the restored data left in place.
    """
    graph_id = restore_job.graph_id
    metadata = restore_job.backup_metadata

    logger.info(
      f"Starting restore for graph '{graph_id}' from {restore_job.backup_format.value} "
      f"backup {metadata.timestamp.isoformat()}"
    )

    try:
      if metadata.s3_key:
        backup_data = await self.s3_adapter.download_backup_by_key(metadata.s3_key)
      else:
        backup_data = await self.s3_adapter.download_backup_by_timestamp(
          graph_id=graph_id,
          timestamp=metadata.timestamp,
          backup_type=metadata.backup_type,
        )

      if not await self._validate_backup_integrity(backup_data, metadata):
        raise ValueError("Backup integrity check failed")

      if restore_job.drop_existing:
        await self._drop_database_if_exists(graph_id)

      if restore_job.create_new_database:
        await self._ensure_database_exists(graph_id)

      await self._import_backup_data(
        graph_id,
        backup_data,
        restore_job.backup_format,
        restore_job.progress_tracker,
        restore_job.create_system_backup,
      )

      if restore_job.verify_after_restore:
        if not await self._verify_restore(graph_id, metadata):
          logger.warning(f"Restore verification failed for graph '{graph_id}'")
          return False

      logger.info(f"Restore completed successfully for graph '{graph_id}'")
      return True

    except Exception as e:
      logger.error(f"Restore failed for graph '{graph_id}': {e}")
      raise

  async def list_backups(self, graph_id: str | None = None) -> list[dict[str, Any]]:
    """List backups, newest first, optionally scoped to one graph."""
    return await self.s3_adapter.list_backups(graph_id)

  async def delete_old_backups(self, graph_id: str, retention_days: int) -> int:
    """Delete backups past the retention window and return how many went.

    Deletion is per-object and best-effort: a failure is logged and skipped
    rather than aborting the sweep, so the count can be lower than the number
    of expired backups.
    """
    backups = await self.list_backups(graph_id)
    cutoff_date = datetime.now(UTC).timestamp() - (retention_days * 24 * 3600)

    deleted_count = 0
    for backup in backups:
      if backup["last_modified"].timestamp() < cutoff_date:
        try:
          key_parts = backup["key"].split("/")
          filename = key_parts[-1]
          timestamp_str = filename.split("-")[1].split(".")[0]
          timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S").replace(
            tzinfo=UTC
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

  async def _get_database_stats(self, graph_id: str) -> dict[str, Any]:
    """Node and relationship counts, recorded in the backup metadata."""
    repository = await get_universal_repository(graph_id, operation_type="read")

    async with repository:
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
  ) -> tuple[bytes, str, dict[str, Any] | None]:
    """Dispatch to the per-format exporter.

    Returns ``(data, file_extension, payload_measurement)``. Only the full dump
    measures itself — the export formats are re-serializations of query results
    rather than a copy of the database file, so there is no artifact to
    reconcile. ``None`` therefore means "not applicable", not "did not match".
    """
    if backup_format == BackupFormat.CSV:
      data, ext = await self._export_to_csv(graph_id, backup_type)
      return data, ext, None
    elif backup_format == BackupFormat.JSON:
      data, ext = await self._export_to_json(graph_id, backup_type)
      return data, ext, None
    elif backup_format == BackupFormat.PARQUET:
      data, ext = await self._export_to_parquet(graph_id, backup_type)
      return data, ext, None
    elif backup_format == BackupFormat.FULL_DUMP:
      return await self._export_full_dump(graph_id, backup_type)
    else:
      raise ValueError(f"Unsupported backup format: {backup_format}")

  async def _export_to_csv(
    self, graph_id: str, backup_type: BackupType
  ) -> tuple[bytes, str]:
    """Export database to CSV format."""
    logger.info(f"Exporting graph '{graph_id}' to CSV format")

    repository = await get_universal_repository(graph_id, operation_type="read")

    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir)

      async with repository:
        # LadybugDB has no introspection call; types come from the schema loader.
        from ....schemas.loader import LadybugSchemaLoader

        try:
          schema_loader = LadybugSchemaLoader()
          node_types = schema_loader.list_node_types()
          relationship_types = schema_loader.list_relationship_types()

          logger.info(
            f"Found {len(node_types)} node types and {len(relationship_types)} relationship types"
          )

          for node_type in node_types:
            csv_file = temp_path / f"nodes_{node_type.lower()}.csv"

            # Count first: COPY on an absent or empty type is not worth a file.
            try:
              count_result = await repository.execute_single(
                f"MATCH (n:{node_type}) RETURN count(n) as count"
              )
              if count_result and count_result.get("count", 0) > 0:
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

          for rel_type in relationship_types:
            csv_file = temp_path / f"relationships_{rel_type.lower()}.csv"

            # Count first: COPY on an absent or empty type is not worth a file.
            try:
              count_result = await repository.execute_single(
                f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count"
              )
              if count_result and count_result.get("count", 0) > 0:
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
          # Schema unavailable: fall back to a fixed guess at the core types.
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

      zip_file = temp_path / "backup.zip"
      with zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED) as zf:
        for csv_file in temp_path.glob("*.csv"):
          zf.write(csv_file, csv_file.name)

      with open(zip_file, "rb") as f:
        backup_data = f.read()

    return backup_data, ".csv.zip"

  async def _export_to_parquet(
    self, graph_id: str, backup_type: BackupType
  ) -> tuple[bytes, str]:
    """Export database to Parquet format."""
    logger.info(f"Exporting graph '{graph_id}' to Parquet format")

    repository = await get_universal_repository(graph_id, operation_type="read")

    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir)

      async with repository:
        # LadybugDB has no introspection call; types come from the schema loader.
        from ....schemas.loader import LadybugSchemaLoader

        try:
          schema_loader = LadybugSchemaLoader()
          node_types = schema_loader.list_node_types()
          relationship_types = schema_loader.list_relationship_types()

          logger.info(
            f"Found {len(node_types)} node types and {len(relationship_types)} relationship types for Parquet export"
          )

          for node_type in node_types:
            parquet_file = temp_path / f"nodes_{node_type.lower()}.parquet"

            # Count first: COPY on an absent or empty type is not worth a file.
            try:
              count_result = await repository.execute_single(
                f"MATCH (n:{node_type}) RETURN count(n) as count"
              )
              if count_result and count_result.get("count", 0) > 0:
                # LadybugDB picks the format from the file extension, not a
                # FORMAT parameter — the ".parquet" suffix is load-bearing.
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

          for rel_type in relationship_types:
            parquet_file = temp_path / f"relationships_{rel_type.lower()}.parquet"

            # Count first: COPY on an absent or empty type is not worth a file.
            try:
              count_result = await repository.execute_single(
                f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count"
              )
              if count_result and count_result.get("count", 0) > 0:
                # LadybugDB picks the format from the file extension, not a
                # FORMAT parameter — the ".parquet" suffix is load-bearing.
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
          # Schema unavailable: fall back to a fixed guess at the core types.
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

      zip_file = temp_path / "backup.zip"
      with zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED) as zf:
        for parquet_file in temp_path.glob("*.parquet"):
          zf.write(parquet_file, parquet_file.name)

      with open(zip_file, "rb") as f:
        backup_data = f.read()

    return backup_data, ".parquet.zip"

  async def _export_to_json(
    self, graph_id: str, backup_type: BackupType
  ) -> tuple[bytes, str]:
    """Export database to JSON format."""
    logger.info(f"Exporting graph '{graph_id}' to JSON format")

    repository = await get_universal_repository(graph_id, operation_type="read")

    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir)

      async with repository:
        # LadybugDB has no introspection call; types come from the schema loader.
        from ....schemas.loader import LadybugSchemaLoader

        try:
          schema_loader = LadybugSchemaLoader()
          node_types = schema_loader.list_node_types()
          relationship_types = schema_loader.list_relationship_types()

          logger.info(
            f"Found {len(node_types)} node types and {len(relationship_types)} relationship types for JSON export"
          )

          for node_type in node_types:
            json_file = temp_path / f"nodes_{node_type.lower()}.json"

            # Count first: COPY on an absent or empty type is not worth a file.
            try:
              count_result = await repository.execute_single(
                f"MATCH (n:{node_type}) RETURN count(n) as count"
              )
              if count_result and count_result.get("count", 0) > 0:
                nodes_result = await repository.execute_query(
                  f"MATCH (n:{node_type}) RETURN n"
                )

                # LadybugDB has no JSON COPY target; serialize by hand.
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

          for rel_type in relationship_types:
            json_file = temp_path / f"relationships_{rel_type.lower()}.json"

            # Count first: COPY on an absent or empty type is not worth a file.
            try:
              count_result = await repository.execute_single(
                f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count"
              )
              if count_result and count_result.get("count", 0) > 0:
                rels_result = await repository.execute_query(
                  f"MATCH (a)-[r:{rel_type}]->(b) RETURN a.id as start_id, r, b.id as end_id"
                )

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
          # Schema unavailable: fall back to a fixed guess at the core types.
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

      zip_file = temp_path / "backup.zip"
      with zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED) as zf:
        for json_file in temp_path.glob("*.json"):
          zf.write(json_file, json_file.name)

      with open(zip_file, "rb") as f:
        backup_data = f.read()

    return backup_data, ".json.zip"

  async def _export_full_dump(
    self, graph_id: str, backup_type: BackupType
  ) -> tuple[bytes, str, dict[str, Any] | None]:
    """Pull the on-disk database through the Graph API, not the filesystem.

    The worker doesn't share a volume with the graph nodes, so the dump has to
    cross the container boundary as an HTTP response.

    The third element carries what the *payload* contains, measured on the copy
    at dump time, so the caller can check it against the live database rather
    than trusting that they agree.
    """
    logger.info(f"Creating full dump for graph '{graph_id}' via Graph API")

    graph_client = await self.graph_router.get_repository(
      graph_id=graph_id,
      operation_type="read",
    )

    try:
      response = await graph_client.download_backup(graph_id=graph_id)

      if not response or "backup_data" not in response:
        raise RuntimeError(f"Failed to get backup from Graph API for {graph_id}")

      backup_data = response["backup_data"]
      logger.info(
        f"Successfully retrieved backup from Graph API: {len(backup_data)} bytes"
      )

      payload = {
        "node_count": response.get("payload_node_count"),
        "relationship_count": response.get("payload_relationship_count"),
        "memory": response.get("memory"),
      }
      return backup_data, ".lbug.zip", payload

    except Exception as e:
      logger.error(f"Failed to create backup via Graph API for {graph_id}: {e}")
      raise

  async def _import_backup_data(
    self,
    graph_id: str,
    backup_data: bytes,
    backup_format: BackupFormat,
    progress_tracker=None,
    create_system_backup: bool = True,
  ) -> None:
    """Dispatch to the per-format importer.

    ``create_system_backup`` only reaches the full-dump path — it is the only
    importer that overwrites the database in place.
    """
    if backup_format == BackupFormat.CSV:
      await self._import_from_csv(graph_id, backup_data, progress_tracker)
    elif backup_format == BackupFormat.JSON:
      await self._import_from_json(graph_id, backup_data, progress_tracker)
    elif backup_format == BackupFormat.PARQUET:
      await self._import_from_parquet(graph_id, backup_data, progress_tracker)
    elif backup_format == BackupFormat.FULL_DUMP:
      await self._import_full_dump(
        graph_id, backup_data, progress_tracker, create_system_backup
      )
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

      zip_file = temp_path / "backup.zip"
      with open(zip_file, "wb") as f:
        f.write(backup_data)

      with zipfile.ZipFile(zip_file, "r") as zf:
        zf.extractall(temp_path)

      async with repository:
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

      zip_file = temp_path / "backup.zip"
      with open(zip_file, "wb") as f:
        f.write(backup_data)

      with zipfile.ZipFile(zip_file, "r") as zf:
        zf.extractall(temp_path)

      async with repository:
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

      zip_file = temp_path / "backup.zip"
      with open(zip_file, "wb") as f:
        f.write(backup_data)

      with zipfile.ZipFile(zip_file, "r") as zf:
        zf.extractall(temp_path)

      async with repository:
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
    """Overwrite the on-disk database with a full dump.

    Destructive: the target files are replaced in place. Unless
    ``create_system_backup`` is False, the existing database is snapshotted to
    S3 first and a snapshot failure aborts the restore — losing the old data
    with no rollback is worse than not restoring.

    This is the only place the existing database is removed. Callers must not
    clear it first: the snapshot above is conditioned on the target still
    existing, so a caller that deletes ahead of us silently turns the safety
    net off, and a download or checksum failure then has nothing to fall back
    to. ``backup_data`` is already verified by the time it arrives here.
    """
    logger.info(f"Importing full dump to graph '{graph_id}'")

    target_path = MultiTenantUtils.get_database_path_for_graph(graph_id)
    target_dir = os.path.dirname(target_path)

    os.makedirs(target_dir, exist_ok=True)

    if create_system_backup and os.path.exists(target_path):
      logger.info(f"Creating system backup of existing database for graph '{graph_id}'")

      try:
        with tempfile.TemporaryDirectory() as backup_temp_dir:
          backup_temp_path = Path(backup_temp_dir)

          # A LadybugDB database is a single file or a directory; both shapes
          # are copied aside, then renamed so the archive holds the plain name.
          final_backup_file = None
          final_backup_dir = None

          if os.path.isfile(target_path):
            backup_file = backup_temp_path / f"{graph_id}.lbug.bak"
            shutil.copy2(target_path, backup_file)
            final_backup_file = backup_temp_path / f"{graph_id}.lbug"
            shutil.move(backup_file, final_backup_file)
          else:
            backup_dir = backup_temp_path / f"{graph_id}.bak"
            shutil.copytree(target_path, backup_dir)
            final_backup_dir = backup_temp_path / graph_id
            shutil.move(backup_dir, final_backup_dir)

          timestamp = datetime.now(UTC)
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

          with open(system_backup_zip, "rb") as f:
            system_backup_data = f.read()

          metadata = {
            "backup_type": "system_restore",
            "timestamp": timestamp_str,
            "compression_enabled": True,
          }

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
        logger.error(f"Error creating system backup for graph '{graph_id}': {e!s}")
        raise RuntimeError(
          f"Failed to create system backup before restore: {e!s}. "
          "Aborting restore for safety. Set create_system_backup=False to skip backup and force restore."
        ) from e

    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir)

      zip_file = temp_path / "backup.zip"
      with open(zip_file, "wb") as f:
        f.write(backup_data)

      with zipfile.ZipFile(zip_file, "r") as zf:
        zf.extractall(temp_path)

      # The WAL goes before the database file, same as DatabaseManager.delete:
      # left behind, LadybugDB replays the outgoing database's un-checkpointed
      # writes on top of the restored one.
      wal_path = Path(f"{target_path}.wal")
      if wal_path.exists():
        wal_path.unlink()
        logger.info(f"Removed stale WAL before restoring graph '{graph_id}'")

      # Clear the existing target whatever shape it has — a single .lbug file
      # in current engine versions, a directory in older ones — since the
      # incoming dump may not be the same shape.
      if os.path.exists(target_path):
        if os.path.isfile(target_path):
          os.unlink(target_path)
        else:
          shutil.rmtree(target_path)

      if (temp_path / f"{graph_id}.lbug").exists():
        shutil.copy2(temp_path / f"{graph_id}.lbug", target_path)
      else:
        shutil.copytree(temp_path / graph_id, target_path)

      memory_outcome = _restore_memory_store(graph_id, temp_path)

      if progress_tracker:
        progress_tracker.update_import_progress(
          message=f"Restored full database dump (memory: {memory_outcome})"
        )

  async def _ensure_database_exists(self, graph_id: str) -> None:
    """Touch the database so the repository creates it if absent.

    There is no explicit create call: the trivial query is what materialises
    the database on first access.
    """
    repository = await get_universal_repository(graph_id, operation_type="write")
    async with repository:
      await repository.execute_query("MATCH (n) RETURN count(n) LIMIT 1")
    logger.info(f"Database ensured for graph: {graph_id}")

  async def _drop_database_if_exists(self, graph_id: str) -> None:
    """Delete the graph's files from disk. No-op when nothing is there."""
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
    """Compare SHA-256 against the recorded checksum of the uncompressed data."""
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
    """Check that node and relationship counts match the backup's."""
    try:
      current_stats = await self._get_database_stats(graph_id)

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
  ) -> bytes | None:
    """Download backup bytes for export, or None when the fetch fails.

    Verifies the checksum before returning; a mismatch raises.
    """
    try:
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

      if not await self._validate_backup_integrity(backup_data, backup_metadata):
        raise ValueError("Backup integrity check failed")

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
    """Convert exported bytes to ``target_format``.

    Unimplemented: any target other than "original" returns the input
    unchanged. Use :meth:`_convert_backup_format` for the download path.
    """
    if target_format == "original":
      return backup_data

    logger.info(
      f"Format conversion to {target_format} not yet implemented, returning original"
    )
    return backup_data

  def health_check(self) -> dict[str, Any]:
    """Report S3 and graph reachability.

    Sync by design, so the graph probe can only run outside an event loop;
    called from async code it reports the graph as healthy without checking.
    """
    s3_health = self.s3_adapter.health_check()

    try:
      from ....middleware.graph import get_universal_repository

      async def test_connection():
        repository = await get_universal_repository("default", operation_type="read")
        with repository:
          await repository.execute_single("RETURN 1 as test")
        return True

      import asyncio

      try:
        asyncio.get_running_loop()
        logger.info("Already in event loop, skipping async graph health check")
        graph_healthy = True
      except RuntimeError:
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


def create_backup_manager(**kwargs) -> BackupManager:
  """Build a :class:`BackupManager`; ``kwargs`` override the lazy defaults."""
  return BackupManager(**kwargs)
