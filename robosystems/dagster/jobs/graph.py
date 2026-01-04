"""Dagster graph operations jobs.

These jobs handle user-triggered graph operations:
- Graph creation (generic, entity, subgraph)
- Backup and restore
- DuckDB staging and graph materialization

Usage:
- Jobs are triggered via DagsterGraphQLClient from FastAPI endpoints
- FastAPI background tasks monitor run status and emit SSE events
- User gets real-time progress via SSE connection
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from dagster import (
  Config,
  Failure,
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


# ============================================================================
# Configuration Classes
# ============================================================================


class CreateGraphConfig(Config):
  """Configuration for graph creation."""

  user_id: str
  tier: str = "ladybug-standard"
  graph_name: str | None = None
  description: str | None = None
  schema_extensions: list[str] = []
  tags: list[str] = []
  skip_billing: bool = False
  operation_id: str | None = None  # For SSE result updates
  custom_schema: dict | None = None  # Custom schema definition for custom graphs


class CreateEntityGraphConfig(Config):
  """Configuration for entity graph creation."""

  user_id: str
  entity_name: str
  entity_identifier: str | None = None
  entity_identifier_type: str | None = None
  tier: str = "ladybug-standard"
  graph_name: str | None = None
  description: str | None = None
  schema_extensions: list[str] = []
  tags: list[str] = []
  create_entity: bool = True
  skip_billing: bool = False
  operation_id: str | None = None  # For SSE result updates


class CreateSubgraphConfig(Config):
  """Configuration for subgraph creation."""

  user_id: str
  parent_graph_id: str
  name: str
  description: str | None = None
  subgraph_type: str = "static"
  fork_parent: bool = False
  fork_tables: list[str] = []
  fork_exclude_patterns: list[str] = []
  operation_id: str | None = None  # For SSE result updates


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
# Graph Creation Jobs
# Replaces: robosystems.tasks.graph_operations.create_graph
# ============================================================================


@op(out={"graph_result": Out(dict)})
def create_graph_database(
  context: OpExecutionContext,
  db: DatabaseResource,
  config: CreateGraphConfig,
) -> dict[str, Any]:
  """Create a new graph database."""
  from robosystems.operations.graph.generic_graph_service import GenericGraphServiceSync

  context.log.info(f"Creating graph for user {config.user_id}, tier {config.tier}")

  graph_service = GenericGraphServiceSync()

  def progress_callback(message: str, progress: int | None = None):
    context.log.info(f"[{progress}%] {message}" if progress else message)

  result = graph_service.create_graph(
    graph_id=None,  # Auto-generate
    schema_extensions=config.schema_extensions,
    metadata={
      "graph_name": config.graph_name,
      "description": config.description,
      "tags": config.tags,
    },
    tier=config.tier,
    initial_data=None,
    user_id=config.user_id,
    custom_schema=config.custom_schema,
    progress_callback=progress_callback,
  )

  context.log.info(f"Created graph: {result.get('graph_id')}")
  return result


@op
def create_graph_subscription(
  context: OpExecutionContext,
  db: DatabaseResource,
  graph_result: dict[str, Any],
  config: CreateGraphConfig,
) -> dict[str, Any]:
  """Create billing subscription for the graph."""
  if config.skip_billing:
    context.log.info("Skipping billing subscription (provision flow)")
    result = {**graph_result, "subscription_created": False}
  else:
    from robosystems.operations.graph.subscription_service import (
      GraphSubscriptionService,
    )

    with db.get_session() as session:
      subscription_service = GraphSubscriptionService(session)
      subscription = subscription_service.create_graph_subscription(
        user_id=config.user_id,
        graph_id=graph_result["graph_id"],
        plan_name=config.tier,
      )

      context.log.info(
        f"Created subscription {subscription.id} for graph {graph_result['graph_id']}"
      )

      result = {
        **graph_result,
        "subscription_id": str(subscription.id),
        "subscription_created": True,
      }

  # Emit graph_id to SSE storage if operation_id provided
  if config.operation_id:
    _emit_graph_result_to_sse(context, config.operation_id, result)

  return result


@job
def create_graph_job():
  """Create a new graph database with billing subscription."""
  result = create_graph_database()
  create_graph_subscription(result)


# ============================================================================
# Entity Graph Creation
# Replaces: robosystems.tasks.graph_operations.create_entity_graph
# ============================================================================


@op(out={"entity_graph_result": Out(dict)})
def create_entity_graph_database(
  context: OpExecutionContext,
  db: DatabaseResource,
  config: CreateEntityGraphConfig,
) -> dict[str, Any]:
  """Create a new entity with its own graph database."""
  from robosystems.operations.graph.entity_graph_service import EntityGraphServiceSync

  context.log.info(
    f"Creating entity graph for user {config.user_id}, entity: {config.entity_name}"
  )

  with db.get_session() as session:
    entity_service = EntityGraphServiceSync(session=session)

    def progress_callback(message: str, progress: int | None = None):
      context.log.info(f"[{progress}%] {message}" if progress else message)

    entity_data = {
      "name": config.entity_name,
      "identifier": config.entity_identifier,
      "identifier_type": config.entity_identifier_type,
      "graph_name": config.graph_name,
      "description": config.description,
      "graph_tier": config.tier,
      "extensions": config.schema_extensions,  # EntityCreate expects 'extensions' not 'schema_extensions'
      "tags": config.tags,
      "create_entity": config.create_entity,
      "skip_billing": config.skip_billing,
    }

    result = entity_service.create_entity_with_new_graph(
      entity_data_dict=entity_data,
      user_id=config.user_id,
      progress_callback=progress_callback,
    )

    context.log.info(f"Created entity graph: {result.get('graph_id')}")
    return result


@op
def create_entity_graph_subscription(
  context: OpExecutionContext,
  db: DatabaseResource,
  entity_graph_result: dict[str, Any],
  config: CreateEntityGraphConfig,
) -> dict[str, Any]:
  """Create billing subscription for the entity graph."""
  if config.skip_billing:
    context.log.info("Skipping billing subscription (provision flow)")
    result = {**entity_graph_result, "subscription_created": False}
  else:
    from robosystems.operations.graph.subscription_service import (
      GraphSubscriptionService,
    )

    with db.get_session() as session:
      subscription_service = GraphSubscriptionService(session)
      subscription = subscription_service.create_graph_subscription(
        user_id=config.user_id,
        graph_id=entity_graph_result["graph_id"],
        plan_name=config.tier,
      )

      context.log.info(
        f"Created subscription {subscription.id} for entity graph {entity_graph_result['graph_id']}"
      )

      result = {
        **entity_graph_result,
        "subscription_id": str(subscription.id),
        "subscription_created": True,
      }

  # Emit graph_id to SSE storage if operation_id provided
  if config.operation_id:
    _emit_graph_result_to_sse(context, config.operation_id, result)

  return result


@job
def create_entity_graph_job():
  """Create a new entity with its own graph database."""
  result = create_entity_graph_database()
  create_entity_graph_subscription(result)


# ============================================================================
# Subgraph Creation
# Replaces: robosystems.tasks.graph_operations.create_subgraph
# ============================================================================


@op(out={"subgraph_result": Out(dict)})
def create_subgraph_database(
  context: OpExecutionContext,
  db: DatabaseResource,
  config: CreateSubgraphConfig,
) -> dict[str, Any]:
  """Create a new subgraph database."""
  import asyncio

  from robosystems.models.iam.graph import Graph
  from robosystems.models.iam.user import User
  from robosystems.operations.graph.subgraph_service import SubgraphService

  context.log.info(
    f"Creating subgraph {config.name} under parent {config.parent_graph_id}"
  )

  with db.get_session() as session:
    parent_graph = (
      session.query(Graph).filter(Graph.graph_id == config.parent_graph_id).first()
    )
    if not parent_graph:
      raise Failure(f"Parent graph {config.parent_graph_id} not found")

    user = session.query(User).filter(User.id == config.user_id).first()
    if not user:
      raise Failure(f"User {config.user_id} not found")

    service = SubgraphService()

    # Run async create_subgraph in sync context
    loop = asyncio.new_event_loop()
    try:
      result = loop.run_until_complete(
        service.create_subgraph(
          parent_graph=parent_graph,
          user=user,
          name=config.name,
          description=config.description,
          subgraph_type=config.subgraph_type,
          metadata=None,
          fork_parent=False,  # Handle fork separately
          fork_options=None,
        )
      )
    finally:
      loop.close()

    context.log.info(f"Created subgraph: {result.get('graph_id')}")
    return result


@op
def fork_parent_to_subgraph(
  context: OpExecutionContext,
  db: DatabaseResource,
  subgraph_result: dict[str, Any],
  config: CreateSubgraphConfig,
) -> dict[str, Any]:
  """Fork data from parent graph to subgraph if requested."""
  if not config.fork_parent:
    context.log.info("Skipping fork (not requested)")
    return {**subgraph_result, "fork_status": None}

  import asyncio

  from robosystems.operations.graph.subgraph_service import SubgraphService

  service = SubgraphService()
  subgraph_id = subgraph_result["graph_id"]

  context.log.info(f"Forking parent data to subgraph {subgraph_id}")

  fork_options = {
    "tables": config.fork_tables,
    "exclude_patterns": config.fork_exclude_patterns,
  }

  def progress_callback(msg: str, pct: float):
    context.log.info(f"[{int(pct)}%] {msg}")

  loop = asyncio.new_event_loop()
  try:
    fork_status = loop.run_until_complete(
      service.fork_parent_data(
        parent_graph_id=config.parent_graph_id,
        subgraph_id=subgraph_id,
        options=fork_options,
        progress_callback=progress_callback,
      )
    )
    context.log.info(f"Fork completed: {fork_status.get('row_count', 0)} rows copied")
  except Exception as e:
    context.log.error(f"Fork failed: {e}")
    fork_status = {"status": "failed", "error": str(e)}
  finally:
    loop.close()

  result = {**subgraph_result, "fork_status": fork_status}

  # Emit result to SSE storage if operation_id provided
  if config.operation_id:
    _emit_graph_result_to_sse(context, config.operation_id, result)

  return result


@job
def create_subgraph_job():
  """Create a new subgraph with optional data fork from parent."""
  result = create_subgraph_database()
  fork_parent_to_subgraph(result)


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


@job
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


@job
def restore_graph_job():
  """Restore a graph database from backup."""
  restore_backup()


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


@job
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


@job
def materialize_file_job():
  """Materialize a staged file to graph database."""
  materialize_staged_file()


# ============================================================================
# Full Graph Materialization Job
# Replaces: robosystems.routers.graphs.materialize synchronous endpoint logic
# ============================================================================


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
        "execution_time_ms": (time.time() - start_time) * 1000,
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
          "execution_time_ms": (time.time() - start_time) * 1000,
          "message": "No tables with staged data to materialize",
        }
        if config.operation_id:
          _emit_graph_result_to_sse(context, config.operation_id, result)
        return result

      # Sort tables: nodes before relationships
      table_names = [t.table_name for t in tables_with_staged_data]
      node_tables = [t for t in table_names if not t.isupper()]
      rel_tables = [t for t in table_names if t.isupper()]
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
            client.materialize_table(
              graph_id=graph_id,
              table_name=table_name,
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

      execution_time_ms = (time.time() - start_time) * 1000

      context.log.info(
        f"[100%] Graph materialization complete: {len(tables_materialized)} tables, "
        f"{total_rows:,} rows in {execution_time_ms:.2f}ms"
      )

      result = {
        "status": "success",
        "graph_id": graph_id,
        "was_stale": was_stale,
        "stale_reason": stale_reason,
        "tables_materialized": tables_materialized,
        "total_rows": total_rows,
        "execution_time_ms": execution_time_ms,
        "rebuild": config.rebuild,
        "message": f"Graph materialized successfully from {len(tables_materialized)} tables",
      }

      if config.operation_id:
        _emit_graph_result_to_sse(context, config.operation_id, result)

      return result

    finally:
      loop.close()


@job
def materialize_graph_job():
  """Materialize all DuckDB staging tables to graph database."""
  materialize_graph_tables()
