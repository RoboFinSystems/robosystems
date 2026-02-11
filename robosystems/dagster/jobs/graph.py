"""Dagster graph operations jobs.

These jobs handle:
- Backup and restore
- DuckDB staging and graph materialization
- Wait-for-capacity graph creation (when ASG scale-up is needed)

Graph creation (generic, entity, subgraph) is normally handled directly via
middleware/sse/direct_monitor.py. When capacity is unavailable and ASG scale-up
is triggered, the wait_and_create_graph_job polls for the new instance and
completes graph creation once capacity appears.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from dagster import (
  AssetKey,
  AssetMaterialization,
  Config,
  Failure,
  MetadataValue,
  OpExecutionContext,
  Out,
  job,
  op,
)

from robosystems.dagster.resources import DatabaseResource, GraphResource, S3Resource

# ============================================================================
# Helper Functions
# ============================================================================


def _emit_graph_result_to_sse(
  context: OpExecutionContext,
  operation_id: str,
  result: dict[str, Any],
) -> None:
  """
  Update SSE operation metadata with the job result.

  This stores the graph_id in the operation metadata so that when the
  monitor emits the final OPERATION_COMPLETED event, it includes the
  graph_id in the result.
  """
  try:
    from robosystems.middleware.sse.event_storage import SSEEventStorage

    storage = SSEEventStorage()
    storage.update_operation_result_sync(operation_id, result)
    context.log.info(
      f"Updated SSE metadata with graph_id={result.get('graph_id')} for operation {operation_id}"
    )
  except Exception as e:
    context.log.warning(f"Failed to update SSE operation metadata: {e}")


def _emit_progress_sync(operation_id: str, message: str, percent: float) -> None:
  """Emit SSE progress event from sync Dagster op context."""
  try:
    from robosystems.middleware.sse.event_storage import EventType, SSEEventStorage

    storage = SSEEventStorage()
    storage.store_event_sync(
      operation_id,
      EventType.OPERATION_PROGRESS,
      {"message": message, "progress_percent": percent},
    )
  except Exception as e:
    from robosystems.logger import logger

    logger.warning(f"Failed to emit SSE progress: {e}")


def _emit_failure_sync(operation_id: str, error: str) -> None:
  """Emit SSE failure event from sync Dagster op context."""
  try:
    from robosystems.middleware.sse.event_storage import EventType, SSEEventStorage

    storage = SSEEventStorage()
    storage.store_event_sync(
      operation_id,
      EventType.OPERATION_ERROR,
      {"message": f"Operation failed: {error}", "error": error},
    )
  except Exception as e:
    from robosystems.logger import logger

    logger.warning(f"Failed to emit SSE failure: {e}")


def _emit_completion_sync(operation_id: str, result: dict[str, Any]) -> None:
  """Emit SSE completion event from sync Dagster op context."""
  try:
    from robosystems.middleware.sse.event_storage import EventType, SSEEventStorage

    storage = SSEEventStorage()
    storage.store_event_sync(
      operation_id,
      EventType.OPERATION_COMPLETED,
      {"message": "Operation completed successfully", "result": result},
    )
  except Exception as e:
    from robosystems.logger import logger

    logger.warning(f"Failed to emit SSE completion: {e}")


# ============================================================================
# Configuration Classes
# ============================================================================


class WaitAndCreateGraphConfig(Config):
  """Configuration for wait-for-capacity graph creation."""

  operation_id: str
  user_id: str
  graph_name: str
  graph_id: str = ""  # Pre-existing graph_id from creation queue
  tier: str = "ladybug-standard"
  schema_extensions: str = ""  # comma-separated
  description: str = ""
  tags: str = ""  # comma-separated
  custom_schema_json: str = ""  # JSON string or empty
  poll_interval_seconds: int = 30
  max_wait_seconds: int = 600  # 10 minutes


class BackupGraphConfig(Config):
  """Configuration for graph backup."""

  graph_id: str
  user_id: str | None = None
  backup_type: str = "full"
  backup_format: str = "full_dump"
  retention_days: int = 90
  compression: bool = True
  encryption: bool = True


class RestoreGraphConfig(Config):
  """Configuration for graph restore."""

  graph_id: str
  backup_id: str
  user_id: str | None = None
  create_system_backup: bool = True
  verify_after_restore: bool = True


class StageFileConfig(Config):
  """Configuration for DuckDB staging."""

  file_id: str
  graph_id: str
  table_id: str
  ingest_to_graph: bool = False


class MaterializeFileConfig(Config):
  """Configuration for single file graph materialization."""

  file_id: str
  graph_id: str
  table_name: str


class MaterializeGraphConfig(Config):
  """Configuration for full graph materialization from DuckDB."""

  graph_id: str
  user_id: str
  force: bool = False
  rebuild: bool = False
  ignore_errors: bool = True
  operation_id: str | None = None  # For SSE result updates


# ============================================================================
# Backup Operations
# Replaces: robosystems.tasks.graph_operations.backup
# ============================================================================


@op(out={"backup_info": Out(dict)})
def create_backup(
  context: OpExecutionContext,
  db: DatabaseResource,
  s3: S3Resource,
  config: BackupGraphConfig,
) -> dict[str, Any]:
  """Create a backup of a graph database."""
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

  context.log.info(f"Creating backup for graph {config.graph_id}")

  # Validate graph_id
  if not MultiTenantUtils.is_shared_repository(config.graph_id):
    MultiTenantUtils.validate_graph_id(config.graph_id)

  database_name = MultiTenantUtils.get_database_name(config.graph_id)

  # Generate S3 key
  timestamp = datetime.now(UTC)
  timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")

  format_extensions = {
    "csv": ".csv.zip",
    "json": ".json.zip",
    "parquet": ".parquet.zip",
    "full_dump": ".lbug.zip",
  }
  extension = format_extensions.get(config.backup_format.lower(), ".lbug.zip")

  if config.compression:
    extension += ".gz"
  if config.encryption:
    extension += ".enc"

  s3_key = f"graph-backups/databases/{config.graph_id}/{config.backup_type}/backup-{timestamp_str}{extension}"

  # Create backup record
  with db.get_session() as session:
    s3_adapter = S3BackupAdapter(enable_compression=config.compression)

    backup_record = GraphBackup.create(
      graph_id=config.graph_id,
      database_name=database_name,
      backup_type=config.backup_type,
      s3_bucket=s3_adapter.bucket_name,
      s3_key=s3_key,
      session=session,
      created_by_user_id=config.user_id,
      compression_enabled=config.compression,
      encryption_enabled=config.encryption,
      expires_at=datetime.now(UTC) + timedelta(days=config.retention_days),
    )

    backup_record.start_backup(session)
    backup_id = str(backup_record.id)

  context.log.info(f"Created backup record {backup_id}, starting backup...")

  # Create the actual backup
  backup_manager = create_backup_manager()

  backup_job = BackupJob(
    graph_id=config.graph_id,
    backup_type=BackupType(config.backup_type),
    backup_format=BackupFormat(config.backup_format),
    retention_days=config.retention_days,
    compression=config.compression,
    encryption=config.encryption,
    allow_export=not config.encryption,
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
        },
      )

  context.log.info(
    f"Backup completed: {backup_info.compressed_size} bytes, "
    f"ratio: {backup_info.compression_ratio:.2f}"
  )

  return {
    "backup_id": backup_id,
    "graph_id": config.graph_id,
    "s3_key": backup_info.s3_key,
    "size_bytes": backup_info.compressed_size,
    "compression_ratio": backup_info.compression_ratio,
    "created_at": timestamp.isoformat(),
  }


@job(
  tags={
    "dagster/priority": "1",
    "dagster/max_retries": 3,
    # Backup operations can be long-running - use on-demand
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  }
)
def backup_graph_job():
  """Create a backup of a graph database."""
  create_backup()


@op(out={"restore_result": Out(dict)})
def restore_backup(
  context: OpExecutionContext,
  db: DatabaseResource,
  s3: S3Resource,
  config: RestoreGraphConfig,
) -> dict[str, Any]:
  """Restore a graph database from backup."""
  import asyncio

  from robosystems.graph_api.client.factory import GraphClientFactory
  from robosystems.models.iam import GraphBackup
  from robosystems.operations.lbug.backup_manager import (
    BackupFormat,
    BackupJob,
    BackupType,
    create_backup_manager,
  )

  context.log.info(f"Restoring graph {config.graph_id} from backup {config.backup_id}")

  # Get backup record
  with db.get_session() as session:
    backup_record = GraphBackup.get_by_id(config.backup_id, session)
    if not backup_record:
      raise Failure(f"Backup not found: {config.backup_id}")

    if backup_record.graph_id != config.graph_id:
      raise Failure(
        f"Backup {config.backup_id} does not belong to graph {config.graph_id}"
      )

    s3_bucket = backup_record.s3_bucket
    s3_key = backup_record.s3_key
    encryption_enabled = backup_record.encryption_enabled
    compression_enabled = backup_record.compression_enabled

  # Create system backup before restore if requested
  if config.create_system_backup:
    context.log.info("Creating system backup before restore...")
    try:
      backup_manager = create_backup_manager()
      system_backup_job = BackupJob(
        graph_id=config.graph_id,
        backup_type=BackupType.FULL,
        backup_format=BackupFormat.FULL_DUMP,
        encryption=True,
        allow_export=False,
      )

      loop = asyncio.new_event_loop()
      try:
        system_backup = loop.run_until_complete(
          backup_manager.create_backup(system_backup_job)
        )
        context.log.info(f"System backup created: {system_backup.s3_key}")
      finally:
        loop.close()
    except Exception as e:
      context.log.warning(f"Failed to create system backup: {e}")

  # Perform restore via Graph API
  context.log.info(f"Restoring from {s3_bucket}/{s3_key}")

  loop = asyncio.new_event_loop()
  try:
    client = loop.run_until_complete(
      GraphClientFactory.create_client(config.graph_id, operation_type="write")
    )

    try:
      restore_result = loop.run_until_complete(
        client.restore_with_sse(
          graph_id=config.graph_id,
          s3_bucket=s3_bucket,
          s3_key=s3_key,
          create_system_backup=False,  # Already done above
          force_overwrite=True,
          encrypted=encryption_enabled,
          compressed=compression_enabled,
          timeout=3600,
        )
      )
    finally:
      loop.run_until_complete(client.close())
  finally:
    loop.close()

  # Verify restore
  verification_status = "not_verified"
  if config.verify_after_restore:
    if restore_result.get("status") == "completed":
      verification_status = "verified"
      context.log.info("Restore verification successful")
    else:
      verification_status = "failed"
      context.log.warning(f"Restore status: {restore_result.get('status')}")

  # Update backup record
  with db.get_session() as session:
    backup_record = GraphBackup.get_by_id(config.backup_id, session)
    if backup_record and hasattr(backup_record, "last_restored_at"):
      backup_record.last_restored_at = datetime.now(UTC)
      session.commit()

  context.log.info(f"Restore completed: {verification_status}")

  return {
    "graph_id": config.graph_id,
    "backup_id": config.backup_id,
    "status": "completed",
    "verification_status": verification_status,
    "restored_at": datetime.now(UTC).isoformat(),
  }


@job(
  tags={
    "dagster/priority": "1",
    "dagster/max_retries": 3,
    # Restore operations can be long-running - use on-demand
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  }
)
def restore_graph_job():
  """Restore a graph database from backup."""
  restore_backup()


# ============================================================================
# Wait-for-Capacity Graph Creation
# When allocate_database() finds no capacity but triggers ASG scale-up,
# this job polls for the new instance then creates the graph.
# ============================================================================


def _fail_graph_provisioning(graph_id: str, session_factory, context) -> None:
  """Mark a provisioning graph as deprovisioned after a timeout or error.

  No retry — the user is notified via SSE and can try again themselves.
  """
  if not graph_id:
    return
  try:
    from robosystems.models.iam.graph import Graph, GraphStatus

    db = session_factory()
    try:
      graph = Graph.get_by_id(graph_id, db)
      if graph and graph.status == GraphStatus.PROVISIONING.value:
        graph.transition_status(GraphStatus.DEPROVISIONED, db)
        context.log.info(f"Marked graph {graph_id} as deprovisioned after failure")
    finally:
      session_factory.remove()
  except Exception as recovery_error:
    context.log.warning(f"Failed to update graph status: {recovery_error}")


async def _wait_and_create(
  context: OpExecutionContext,
  config: WaitAndCreateGraphConfig,
) -> dict[str, Any]:
  """
  Async core logic for wait-for-capacity graph creation.

  Runs a unified poll loop: each iteration checks for capacity, then
  attempts graph creation. If creation fails (e.g. another job won the
  race for the last slot), it loops back and keeps polling. This handles
  the thundering-herd scenario where multiple jobs compete for capacity.

  The entire function is wrapped in a top-level try/except so that any
  unhandled error emits an SSE failure event — the user is never left
  with a dangling operation.
  """
  import asyncio
  import json
  import time

  from robosystems.config import env
  from robosystems.config.graph_tier import GraphTier
  from robosystems.database import session
  from robosystems.middleware.graph.allocation_manager import LadybugAllocationManager
  from robosystems.operations.graph.generic_graph_service import GenericGraphService
  from robosystems.operations.graph.subscription_service import GraphSubscriptionService

  operation_id = config.operation_id

  try:
    tier = GraphTier(config.tier)

    # Parse config fields
    schema_extensions = [
      s.strip() for s in config.schema_extensions.split(",") if s.strip()
    ]
    tags = [t.strip() for t in config.tags.split(",") if t.strip()]
    custom_schema = (
      json.loads(config.custom_schema_json) if config.custom_schema_json else None
    )

    _emit_progress_sync(
      operation_id,
      "Provisioning new infrastructure... this may take 3-5 minutes",
      5,
    )
    context.log.info(
      f"Polling for {config.tier} capacity (max {config.max_wait_seconds}s)"
    )

    manager = LadybugAllocationManager(environment=env.ENVIRONMENT)
    start_time = time.time()
    poll_count = 0
    create_attempts = 0

    # Unified loop: poll for capacity, attempt creation, retry on race loss
    while True:
      elapsed = time.time() - start_time
      if elapsed > config.max_wait_seconds:
        error_msg = (
          f"Timed out waiting for {config.tier} capacity after "
          f"{int(elapsed)}s ({poll_count} polls, "
          f"{create_attempts} create attempts)"
        )
        context.log.error(error_msg)
        _emit_failure_sync(operation_id, error_msg)
        raise Failure(description=error_msg)

      # Check for capacity
      instance = await manager._find_best_instance(tier)
      poll_count += 1

      if not instance:
        # No capacity yet — emit progress and sleep
        minutes = int(elapsed) // 60
        seconds = int(elapsed) % 60
        if config.max_wait_seconds > 0:
          progress_pct = min(5 + (elapsed / config.max_wait_seconds) * 45, 50)
        else:
          progress_pct = 50
        _emit_progress_sync(
          operation_id,
          f"Waiting for infrastructure... {minutes}:{seconds:02d} elapsed",
          progress_pct,
        )
        context.log.info(
          f"Poll {poll_count}: No capacity yet ({minutes}:{seconds:02d} elapsed)"
        )
        await asyncio.sleep(config.poll_interval_seconds)
        continue

      # Capacity found — attempt graph creation
      context.log.info(
        f"Capacity found on instance {instance.instance_id} after "
        f"{int(elapsed)}s ({poll_count} polls)"
      )
      _emit_progress_sync(operation_id, "Infrastructure ready, creating graph...", 55)

      def progress_callback(message: str, percent: float):
        """Map service 0-100% to overall 55-95%."""
        overall_pct = 55 + (percent / 100) * 40
        _emit_progress_sync(operation_id, message, overall_pct)

      try:
        create_attempts += 1
        service = GenericGraphService()
        result = await service.create_graph(
          graph_id=config.graph_id or None,
          schema_extensions=schema_extensions,
          metadata={
            "name": config.graph_name,
            "description": config.description,
            "tags": tags,
          },
          tier=config.tier,
          initial_data=None,
          user_id=config.user_id,
          custom_schema=custom_schema,
          progress_callback=progress_callback,
        )
      except Exception as create_error:
        # Creation failed (likely lost race for capacity) — loop back
        context.log.warning(
          f"Create attempt {create_attempts} failed: {create_error}. "
          "Returning to capacity poll."
        )
        _emit_progress_sync(
          operation_id,
          "Slot taken by another request, waiting for more capacity...",
          10,
        )
        await asyncio.sleep(config.poll_interval_seconds)
        continue

      # Creation succeeded — finalize
      graph_id = result.get("graph_id")
      context.log.info(f"Graph created: {graph_id}")

      # Create billing subscription — failure here means a graph exists without
      # a billing record, so we log at error level for operational visibility.
      db = session()
      try:
        subscription_service = GraphSubscriptionService(db)
        subscription_service.create_graph_subscription(
          user_id=config.user_id,
          graph_id=graph_id,
          plan_name=config.tier,
          tier=tier,
        )
        context.log.info(f"Created billing subscription for graph {graph_id}")
      except Exception as sub_error:
        context.log.error(
          f"Failed to create billing subscription for graph {graph_id}: {sub_error}"
        )
      finally:
        session.remove()

      # Emit result to SSE
      _emit_graph_result_to_sse(context, operation_id, result)
      _emit_completion_sync(operation_id, result)

      # Report AssetMaterialization
      total_elapsed = time.time() - start_time
      context.log_event(
        AssetMaterialization(
          asset_key=AssetKey("user_graph_creation"),
          description=f"Wait-for-capacity creation of graph {graph_id}",
          metadata={
            "graph_id": MetadataValue.text(graph_id or ""),
            "graph_name": MetadataValue.text(config.graph_name),
            "user_id": MetadataValue.text(config.user_id),
            "tier": MetadataValue.text(config.tier),
            "provisioning_method": MetadataValue.text("wait_for_capacity"),
            "wait_time_seconds": MetadataValue.float(total_elapsed),
            "poll_count": MetadataValue.int(poll_count),
            "create_attempts": MetadataValue.int(create_attempts),
          },
        )
      )

      return result

  except Failure:
    # Timeout or failure — mark deprovisioned, no retry
    _fail_graph_provisioning(config.graph_id, session, context)
    raise
  except Exception as e:
    # Unexpected error — emit SSE failure so user isn't left dangling
    error_msg = f"Graph creation failed unexpectedly: {e}"
    context.log.error(error_msg)
    _emit_failure_sync(operation_id, error_msg)
    _fail_graph_provisioning(config.graph_id, session, context)
    raise Failure(description=error_msg) from e


@op
def wait_for_capacity_and_create_graph(
  context: OpExecutionContext,
  config: WaitAndCreateGraphConfig,
) -> dict[str, Any]:
  """
  Wait for instance capacity then create graph.

  Used when allocate_database() triggers ASG scale-up but no instance
  is immediately available. Polls _find_best_instance() until capacity
  appears, then creates the graph and emits SSE progress throughout.
  """
  import asyncio

  loop = asyncio.new_event_loop()
  try:
    return loop.run_until_complete(_wait_and_create(context, config))
  finally:
    loop.close()


@job(
  tags={
    "dagster/priority": "2",
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  }
)
def wait_and_create_graph_job():
  """Wait for instance capacity then create a graph database."""
  wait_for_capacity_and_create_graph()


# ============================================================================
# DuckDB Staging and Graph Materialization
# Replaces: robosystems.tasks.table_operations
# ============================================================================


@op(out={"staging_result": Out(dict)})
def stage_file_in_duckdb(
  context: OpExecutionContext,
  db: DatabaseResource,
  graph: GraphResource,
  config: StageFileConfig,
) -> dict[str, Any]:
  """Stage a file in DuckDB with file_id provenance tracking."""
  import asyncio

  from robosystems.config import env
  from robosystems.graph_api.client.factory import GraphClientFactory
  from robosystems.models.iam import GraphFile, GraphTable

  context.log.info(
    f"Staging file {config.file_id} in graph {config.graph_id}, table {config.table_id}"
  )

  with db.get_session() as session:
    graph_file = GraphFile.get_by_id(config.file_id, session)
    if not graph_file:
      raise Failure(f"File {config.file_id} not found")

    table = GraphTable.get_by_id(config.table_id, session)
    if not table:
      raise Failure(f"Table {config.table_id} not found")

    # Get all uploaded files for this table
    all_files = GraphFile.get_all_for_table(config.table_id, session)
    uploaded_files = [f for f in all_files if f.upload_status == "uploaded"]

    if not uploaded_files:
      context.log.warning(f"No uploaded files found for table {table.table_name}")
      return {
        "status": "skipped",
        "message": "No uploaded files to stage",
        "file_id": config.file_id,
      }

    # Build file list with S3 URIs
    bucket = env.USER_DATA_BUCKET
    s3_files = [f"s3://{bucket}/{f.s3_key}" for f in uploaded_files]
    file_id_map = {f"s3://{bucket}/{f.s3_key}": f.id for f in uploaded_files}

    context.log.info(
      f"Staging {len(s3_files)} files in DuckDB table {table.table_name}"
    )

    # Stage via Graph API
    loop = asyncio.new_event_loop()
    try:
      client = loop.run_until_complete(
        GraphClientFactory.create_client(
          graph_id=config.graph_id, operation_type="write"
        )
      )

      staging_result = loop.run_until_complete(
        client.create_table(
          graph_id=config.graph_id,
          table_name=table.table_name,
          s3_pattern=s3_files,
          file_id_map=file_id_map,
        )
      )
      context.log.info(f"DuckDB staging result: {staging_result}")
    finally:
      loop.close()

    # Mark file as staged
    graph_file.mark_duckdb_staged(session=session, row_count=graph_file.row_count or 0)

    context.log.info(f"File {config.file_id} staged successfully")

    return {
      "status": "success",
      "file_id": config.file_id,
      "graph_id": config.graph_id,
      "table_name": table.table_name,
      "files_staged": len(s3_files),
      "duckdb_status": "staged",
      "ingest_to_graph": config.ingest_to_graph,
    }


@op
def materialize_file_to_graph(
  context: OpExecutionContext,
  db: DatabaseResource,
  graph: GraphResource,
  staging_result: dict[str, Any],
  config: StageFileConfig,
) -> dict[str, Any]:
  """Materialize a file from DuckDB staging to graph database."""
  if not config.ingest_to_graph:
    context.log.info("Skipping graph materialization (not requested)")
    return {**staging_result, "graph_status": "not_ingested"}

  import asyncio

  from robosystems.graph_api.client.factory import GraphClientFactory
  from robosystems.models.iam import GraphFile

  file_id = staging_result["file_id"]
  graph_id = staging_result["graph_id"]
  table_name = staging_result["table_name"]

  context.log.info(
    f"Materializing file {file_id} to graph {graph_id} from table {table_name}"
  )

  loop = asyncio.new_event_loop()
  try:
    client = loop.run_until_complete(
      GraphClientFactory.create_client(graph_id=graph_id, operation_type="write")
    )

    result = loop.run_until_complete(
      client.materialize_table(
        graph_id=graph_id,
        table_name=table_name,
        ignore_errors=True,
        file_ids=[file_id],
      )
    )
  finally:
    loop.close()

  rows_ingested = result.get("rows_ingested", 0)

  # Mark file as ingested
  with db.get_session() as session:
    graph_file = GraphFile.get_by_id(file_id, session)
    if graph_file:
      graph_file.mark_graph_ingested(session=session)

  context.log.info(f"Materialized {rows_ingested} rows for file {file_id}")

  return {
    **staging_result,
    "rows_ingested": rows_ingested,
    "graph_status": "ingested",
  }


@job(
  tags={
    "dagster/priority": "1",
    # User data operations can be long-running - use on-demand
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  }
)
def stage_file_job():
  """Stage a file in DuckDB with optional graph materialization."""
  result = stage_file_in_duckdb()
  materialize_file_to_graph(result)


# Standalone materialization job (for files already staged)
@op(out={"materialize_result": Out(dict)})
def materialize_staged_file(
  context: OpExecutionContext,
  db: DatabaseResource,
  graph: GraphResource,
  config: MaterializeFileConfig,
) -> dict[str, Any]:
  """Materialize an already-staged file to graph database."""
  import asyncio

  from robosystems.graph_api.client.factory import GraphClientFactory
  from robosystems.models.iam import GraphFile

  context.log.info(f"Materializing file {config.file_id} to graph {config.graph_id}")

  # Verify file is staged
  with db.get_session() as session:
    graph_file = GraphFile.get_by_id(config.file_id, session)
    if not graph_file:
      raise Failure(f"File {config.file_id} not found")

    if graph_file.duckdb_status != "staged":
      context.log.warning(
        f"File not staged (status: {graph_file.duckdb_status}), skipping"
      )
      return {
        "status": "skipped",
        "message": f"File not staged (status: {graph_file.duckdb_status})",
        "file_id": config.file_id,
      }

  # Materialize via Graph API
  loop = asyncio.new_event_loop()
  try:
    client = loop.run_until_complete(
      GraphClientFactory.create_client(graph_id=config.graph_id, operation_type="write")
    )

    result = loop.run_until_complete(
      client.materialize_table(
        graph_id=config.graph_id,
        table_name=config.table_name,
        ignore_errors=True,
        file_ids=[config.file_id],
      )
    )
  finally:
    loop.close()

  rows_ingested = result.get("rows_ingested", 0)

  # Mark file as ingested
  with db.get_session() as session:
    graph_file = GraphFile.get_by_id(config.file_id, session)
    if graph_file:
      graph_file.mark_graph_ingested(session=session)

  context.log.info(f"Materialized {rows_ingested} rows for file {config.file_id}")

  return {
    "status": "success",
    "file_id": config.file_id,
    "graph_id": config.graph_id,
    "table_name": config.table_name,
    "rows_ingested": rows_ingested,
    "graph_status": "ingested",
  }


@job(
  tags={
    "dagster/priority": "1",
    # User data operations can be long-running - use on-demand
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  }
)
def materialize_file_job():
  """Materialize a staged file to graph database."""
  materialize_staged_file()


# ============================================================================
# Full Graph Materialization Job
# Replaces: robosystems.routers.graphs.materialize synchronous endpoint logic
# ============================================================================


def _restage_stale_files_sync(
  context,
  session,
  graph_id: str,
  file_ids: list[str],
) -> None:
  """Re-stage stale files synchronously for Dagster op context."""
  import asyncio

  from robosystems.models.iam import GraphFile
  from robosystems.operations.lbug.direct_staging import stage_file_directly

  loop = asyncio.new_event_loop()
  try:
    for file_id in file_ids:
      graph_file = GraphFile.get_by_id(file_id, session)
      if not graph_file:
        context.log.warning(f"Stale file {file_id} not found during recovery, skipping")
        continue

      context.log.info(f"Re-staging stale file {file_id} ({graph_file.file_name})")
      try:
        result = loop.run_until_complete(
          stage_file_directly(
            db=session,
            file_id=file_id,
            graph_id=graph_id,
            table_id=graph_file.table_id,
            s3_key=graph_file.s3_key,
            file_size_bytes=graph_file.file_size_bytes,
            row_count=graph_file.row_count,
          )
        )
        if result.get("status") == "success":
          context.log.info(f"Successfully re-staged stale file {file_id}")
        else:
          context.log.warning(
            f"Re-staging stale file {file_id} returned: {result.get('status')}"
          )
      except Exception as e:
        context.log.error(f"Failed to re-stage stale file {file_id}: {e}")
  finally:
    loop.close()


@op(out={"materialize_result": Out(dict)})
def materialize_graph_tables(
  context: OpExecutionContext,
  db: DatabaseResource,
  graph: GraphResource,
  config: MaterializeGraphConfig,
) -> dict[str, Any]:
  """
  Materialize all DuckDB staging tables to the graph database.

  This is the Dagster equivalent of the synchronous materialize_graph API endpoint.
  It provides full observability into the materialization process.
  """
  import asyncio
  import time

  from robosystems.models.iam import Graph, GraphFile, GraphSchema, GraphTable
  from robosystems.operations.lbug.chunked_materialization import (
    materialize_table_chunked,
  )

  start_time = time.time()
  graph_id = config.graph_id
  context.log.info(
    f"Starting graph materialization for {graph_id} "
    f"(force={config.force}, rebuild={config.rebuild}, ignore_errors={config.ignore_errors})"
  )

  with db.get_session() as session:
    # Verify graph exists
    graph_record = Graph.get_by_id(graph_id, session)
    if not graph_record:
      raise Failure(
        description=f"Graph {graph_id} not found",
        metadata={"graph_id": graph_id},
      )

    # Recover stale files (uploaded to S3 but never staged to DuckDB)
    recovered_ids = GraphFile.recover_stale_files(graph_id, session)
    if recovered_ids:
      context.log.info(
        f"Recovered {len(recovered_ids)} stale files for graph {graph_id}"
      )
      _restage_stale_files_sync(context, session, graph_id, recovered_ids)

    # Check staleness
    was_stale = graph_record.graph_stale or False
    stale_reason = graph_record.graph_stale_reason

    if not was_stale and not config.force and not config.rebuild:
      # Check if there are any tables with staged data in DuckDB
      staged_tables_count = (
        session.query(GraphTable)
        .join(GraphFile, GraphTable.id == GraphFile.table_id)
        .filter(GraphTable.graph_id == graph_id)
        .filter(GraphFile.duckdb_status == "staged")
        .distinct()
        .count()
      )
      if staged_tables_count > 0:
        was_stale = True
        stale_reason = "new_data_staged"
        context.log.info(
          f"Graph {graph_id} has {staged_tables_count} tables with staged data to materialize"
        )

    if not was_stale and not config.force and not config.rebuild:
      context.log.info(
        f"Graph {graph_id} is not stale and force=false, rebuild=false - skipping"
      )
      result = {
        "status": "skipped",
        "graph_id": graph_id,
        "was_stale": False,
        "tables_materialized": [],
        "total_rows": 0,
        "duration_ms": (time.time() - start_time) * 1000,
        "message": "Graph is fresh - no materialization needed",
      }
      if config.operation_id:
        _emit_graph_result_to_sse(context, config.operation_id, result)
      return result

    # Get async event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
      # Get graph client - use GraphClientFactory for materialize_table support
      from robosystems.graph_api.client.factory import get_graph_client

      client = loop.run_until_complete(
        get_graph_client(graph_id=graph_id, operation_type="write")
      )

      # Handle rebuild if requested
      if config.rebuild:
        context.log.info("[10%] Rebuild requested - regenerating graph database")

        graph_metadata = (
          {**graph_record.graph_metadata} if graph_record.graph_metadata else {}
        )
        graph_metadata["status"] = "rebuilding"
        graph_metadata["rebuild_started_at"] = time.time()
        graph_record.graph_metadata = graph_metadata
        session.commit()

        try:
          context.log.info(f"[20%] Deleting graph database for {graph_id}")
          loop.run_until_complete(client.delete_database(graph_id))

          schema = GraphSchema.get_active_schema(graph_id, session)
          if not schema:
            raise Failure(
              description=f"No schema found for graph {graph_id}",
              metadata={"graph_id": graph_id},
            )

          schema_type_for_rebuild = "custom" if schema.schema_ddl else "entity"
          context.log.info(
            f"[30%] Recreating graph database with schema type: {schema_type_for_rebuild}"
          )
          loop.run_until_complete(
            client.create_database(
              graph_id=graph_id,
              schema_type=schema_type_for_rebuild,
              custom_schema_ddl=schema.schema_ddl,
            )
          )
          context.log.info("[40%] Graph database recreated successfully")

        except Exception as e:
          graph_metadata["status"] = "rebuild_failed"
          graph_metadata["rebuild_failed_at"] = time.time()
          graph_metadata["rebuild_error"] = str(e)
          graph_record.graph_metadata = graph_metadata
          session.commit()
          raise Failure(
            description=f"Failed to rebuild graph database: {e!s}",
            metadata={"graph_id": graph_id, "error": str(e)},
          )

      # Get tables that have staged data in DuckDB (ready for materialization)
      # Only materialize tables with files that have duckdb_status="staged"
      tables_with_staged_data = (
        session.query(GraphTable)
        .join(GraphFile, GraphTable.id == GraphFile.table_id)
        .filter(GraphTable.graph_id == graph_id)
        .filter(GraphFile.duckdb_status == "staged")
        .distinct()
        .all()
      )

      if not tables_with_staged_data:
        context.log.info(f"No tables with staged data found for graph {graph_id}")
        result = {
          "status": "success",
          "graph_id": graph_id,
          "was_stale": was_stale,
          "stale_reason": stale_reason,
          "tables_materialized": [],
          "total_rows": 0,
          "duration_ms": (time.time() - start_time) * 1000,
          "message": "No tables with staged data to materialize",
        }
        if config.operation_id:
          _emit_graph_result_to_sse(context, config.operation_id, result)
        return result

      # Sort tables: nodes before relationships (using table_type field)
      node_tables = [
        t.table_name for t in tables_with_staged_data if t.table_type == "node"
      ]
      rel_tables = [
        t.table_name for t in tables_with_staged_data if t.table_type == "relationship"
      ]
      ordered_tables = node_tables + rel_tables

      context.log.info(
        f"[50%] Materializing {len(ordered_tables)} tables: "
        f"{len(node_tables)} nodes, {len(rel_tables)} relationships"
      )

      # Materialize each table
      tables_materialized = []
      total_rows = 0
      base_progress = 50
      progress_per_table = 40 / max(len(ordered_tables), 1)

      for i, table_name in enumerate(ordered_tables):
        progress = int(base_progress + (i * progress_per_table))
        try:
          context.log.info(f"[{progress}%] Materializing table {table_name}")

          mat_result = loop.run_until_complete(
            materialize_table_chunked(
              client=client,
              graph_id=graph_id,
              table_name=table_name,
              tier=graph_record.graph_tier or "ladybug-standard",
              ignore_errors=config.ignore_errors,
              file_ids=None,
            )
          )

          rows_ingested = mat_result.get("rows_ingested", 0)
          total_rows += rows_ingested
          tables_materialized.append(table_name)

          context.log.info(f"Materialized {table_name}: {rows_ingested:,} rows")

        except Exception as e:
          context.log.error(f"Failed to materialize table {table_name}: {e}")
          if not config.ignore_errors:
            raise Failure(
              description=f"Materialization failed on table {table_name}: {e!s}",
              metadata={
                "graph_id": graph_id,
                "table_name": table_name,
                "error": str(e),
              },
            )

      # Mark graph as fresh
      context.log.info("[95%] Marking graph as fresh")
      graph_record.mark_fresh(session=session)

      # Update graph metadata if rebuild was performed
      if config.rebuild:
        graph_metadata = (
          {**graph_record.graph_metadata} if graph_record.graph_metadata else {}
        )
        graph_metadata["status"] = "available"
        graph_metadata["rebuild_completed_at"] = time.time()
        if "rebuild_started_at" in graph_metadata:
          rebuild_duration = (
            graph_metadata["rebuild_completed_at"]
            - graph_metadata["rebuild_started_at"]
          )
          graph_metadata["last_rebuild_duration_seconds"] = rebuild_duration
        graph_record.graph_metadata = graph_metadata
        session.commit()

      duration_ms = (time.time() - start_time) * 1000

      context.log.info(
        f"[100%] Graph materialization complete: {len(tables_materialized)} tables, "
        f"{total_rows:,} rows in {duration_ms / 1000:.2f}s"
      )

      result = {
        "status": "success",
        "graph_id": graph_id,
        "was_stale": was_stale,
        "stale_reason": stale_reason,
        "tables_materialized": tables_materialized,
        "total_rows": total_rows,
        "duration_ms": duration_ms,
        "rebuild": config.rebuild,
        "message": f"Graph materialized successfully from {len(tables_materialized)} tables",
      }

      # Report AssetMaterialization for observability in Dagster UI
      context.log_event(
        AssetMaterialization(
          asset_key=AssetKey("user_graph_materialized"),
          description=f"Materialized {len(tables_materialized)} tables to graph {graph_id}",
          metadata={
            "graph_id": MetadataValue.text(graph_id),
            "tables_materialized": MetadataValue.int(len(tables_materialized)),
            "total_rows": MetadataValue.int(total_rows),
            "duration_ms": MetadataValue.float(duration_ms),
            "rebuild": MetadataValue.bool(config.rebuild),
            "materialization_method": MetadataValue.text("dagster_job"),
          },
        )
      )

      if config.operation_id:
        _emit_graph_result_to_sse(context, config.operation_id, result)

      return result

    finally:
      loop.close()


@job(
  tags={
    "dagster/priority": "1",
    # User data operations can be long-running - use on-demand
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  }
)
def materialize_graph_job():
  """Materialize all DuckDB staging tables to graph database."""
  materialize_graph_tables()
