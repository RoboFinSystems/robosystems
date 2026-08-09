"""Shared Repository Publish Helpers (S3 + R2).

Provides the core publish logic that per-repository assets call.
Each shared repository (SEC, future industry/economic) defines a thin
asset with deps on its own materialization, then delegates here.

Two publish targets:
- S3: Raw .lbug/.duckdb for replica fleet (downloaded to local disk on boot)
- R2: Raw .lbug for subscriber downloads (zero egress fees)
"""

import asyncio
from datetime import UTC, datetime
from typing import Any

from dagster import AssetExecutionContext, MaterializeResult

from robosystems.config import env
from robosystems.config.constants import TASK_TIME_LIMIT
from robosystems.config.storage.graph import (
  get_r2_download_key,
  get_shared_repo_database_key,
)


def _validate_shared_graph_id(graph_id: str) -> None:
  """Raise ValueError if graph_id is not a shared repository or subgraph of one."""
  from robosystems.middleware.graph.utils import MultiTenantUtils
  from robosystems.middleware.graph.utils.subgraph import parse_subgraph_id

  if MultiTenantUtils.is_shared_repository(graph_id):
    return
  subgraph_info = parse_subgraph_id(graph_id)
  if not subgraph_info or not MultiTenantUtils.is_shared_repository(
    subgraph_info.parent_graph_id
  ):
    raise ValueError(f"{graph_id} is not a shared repository or subgraph of one")


def publish_to_s3(
  context: AssetExecutionContext,
  graph_id: str,
) -> MaterializeResult:
  """Publish a shared repository database (e.g. "sec") to S3 for replicas.

  Calls the backup endpoint on the shared master via the Graph Client Factory,
  which handles auth, routing, and circuit breakers. The backup runs entirely
  on-instance (CHECKPOINT + S3 multipart upload), so this only orchestrates and
  monitors via SSE. Returns the S3 URI and upload statistics as metadata.
  """
  context.log.info(f"Publishing {graph_id} database to S3 for replica cluster")

  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping S3 publish in dev environment")
    return MaterializeResult(
      metadata={
        "status": "skipped",
        "reason": "dev_environment",
        "graph_id": graph_id,
      }
    )

  import boto3

  _validate_shared_graph_id(graph_id)

  s3_info = _build_s3_destination(graph_id)
  bucket = s3_info["bucket"]
  s3_key = s3_info["s3_key"]
  s3_uri = s3_info["s3_uri"]

  context.log.info(f"Target: {s3_uri}")

  result = _run_backup(graph_id, bucket, s3_key)

  if result.get("status") != "completed":
    error = result.get("error", "Unknown error")
    raise RuntimeError(f"S3 publish failed for {graph_id}: {error}")

  # head_object confirms the upload landed before reporting success
  s3 = boto3.client("s3", region_name=env.AWS_REGION)
  head = s3.head_object(Bucket=bucket, Key=s3_key)
  file_size = head["ContentLength"]
  last_modified = head["LastModified"]

  context.log.info(
    f"Database published to S3: {s3_uri} "
    f"(size: {file_size / (1024**3):.2f}GB, modified: {last_modified})"
  )

  return MaterializeResult(
    metadata={
      "s3_uri": s3_uri,
      "s3_bucket": bucket,
      "s3_key": s3_key,
      "file_size_bytes": file_size,
      "file_size_gb": round(file_size / (1024**3), 2),
      "last_modified": last_modified.isoformat(),
      "graph_id": graph_id,
      "published_at": datetime.now(UTC).isoformat(),
    }
  )


def publish_duckdb_to_s3(
  context: AssetExecutionContext,
  graph_id: str,
) -> MaterializeResult:
  """Publish a DuckDB staging database (e.g. "sec_historical") to S3.

  Same on-instance backup pattern as publish_to_s3(): the shared master runs
  DuckDB CHECKPOINT + S3 multipart upload, and this only orchestrates and
  monitors via SSE.
  """
  context.log.info(f"Publishing {graph_id} DuckDB staging to S3")

  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping DuckDB S3 publish in dev environment")
    return MaterializeResult(
      metadata={
        "status": "skipped",
        "reason": "dev_environment",
        "graph_id": graph_id,
      }
    )

  import boto3

  _validate_shared_graph_id(graph_id)

  # .duckdb extension instead of .lbug
  s3_info = _build_duckdb_s3_destination(graph_id)
  bucket = s3_info["bucket"]
  s3_key = s3_info["s3_key"]
  s3_uri = s3_info["s3_uri"]

  context.log.info(f"Target: {s3_uri}")

  result = _run_duckdb_backup(graph_id, bucket, s3_key)

  if result.get("status") != "completed":
    error = result.get("error", "Unknown error")
    raise RuntimeError(f"DuckDB S3 publish failed for {graph_id}: {error}")

  s3 = boto3.client("s3", region_name=env.AWS_REGION)
  head = s3.head_object(Bucket=bucket, Key=s3_key)
  file_size = head["ContentLength"]
  last_modified = head["LastModified"]

  context.log.info(
    f"DuckDB published to S3: {s3_uri} "
    f"(size: {file_size / (1024**3):.2f}GB, modified: {last_modified})"
  )

  return MaterializeResult(
    metadata={
      "s3_uri": s3_uri,
      "s3_bucket": bucket,
      "s3_key": s3_key,
      "file_size_bytes": file_size,
      "file_size_gb": round(file_size / (1024**3), 2),
      "last_modified": last_modified.isoformat(),
      "graph_id": graph_id,
      "published_at": datetime.now(UTC).isoformat(),
    }
  )


def _build_duckdb_s3_destination(graph_id: str) -> dict[str, str]:
  """Build S3 bucket/key/uri for a DuckDB staging database."""
  import boto3

  sts = boto3.client("sts", region_name=env.AWS_REGION)
  account_id = sts.get_caller_identity()["Account"]
  bucket = f"robosystems-{account_id}-user-{env.ENVIRONMENT}"
  s3_key = get_shared_repo_database_key(graph_id, ".duckdb")
  s3_uri = f"s3://{bucket}/{s3_key}"
  return {"bucket": bucket, "s3_key": s3_key, "s3_uri": s3_uri}


def _run_duckdb_backup(graph_id: str, bucket: str, s3_key: str) -> dict[str, Any]:
  """Run the DuckDB backup via Graph Client Factory."""
  from robosystems.graph_api.client.factory import get_graph_client_for_sec_ingestion

  async def _execute():
    client = await get_graph_client_for_sec_ingestion()
    try:
      return await client.backup_with_sse(
        graph_id=graph_id,
        backup_type="duckdb_staging",
        s3_destination={"bucket": bucket, "key": s3_key},
        compression=False,
        checkpoint=True,
        vacuum=True,
        timeout=TASK_TIME_LIMIT,
      )
    finally:
      await client.close()

  return asyncio.run(_execute())


def _build_s3_destination(graph_id: str) -> dict[str, str]:
  """Build S3 bucket/key/uri for a shared repository."""
  import boto3

  sts = boto3.client("sts", region_name=env.AWS_REGION)
  account_id = sts.get_caller_identity()["Account"]
  bucket = f"robosystems-{account_id}-user-{env.ENVIRONMENT}"
  s3_key = get_shared_repo_database_key(graph_id, ".lbug")
  s3_uri = f"s3://{bucket}/{s3_key}"
  return {"bucket": bucket, "s3_key": s3_key, "s3_uri": s3_uri}


def _run_backup(graph_id: str, bucket: str, s3_key: str) -> dict[str, Any]:
  """Run the backup via Graph Client Factory."""
  from robosystems.graph_api.client.factory import get_graph_client_for_sec_ingestion

  async def _execute():
    client = await get_graph_client_for_sec_ingestion()
    try:
      return await client.backup_with_sse(
        graph_id=graph_id,
        backup_type="replica",
        s3_destination={"bucket": bucket, "key": s3_key},
        compression=False,
        checkpoint=True,
        timeout=TASK_TIME_LIMIT,
      )
    finally:
      await client.close()

  return asyncio.run(_execute())


# =============================================================================
# R2 Publish Helpers (zero-egress subscriber downloads)
# =============================================================================


def publish_to_r2(
  context: AssetExecutionContext,
  graph_id: str,
  db_resource,
) -> MaterializeResult:
  """Publish a shared repository .lbug to R2 for subscriber downloads.

  Same on-instance backup pattern as publish_to_s3(), but uploads to
  Cloudflare R2 instead of AWS S3. Creates/updates a GraphBackup record
  (via ``db_resource``) so the file appears in the backup list and can be
  downloaded via presigned URL with zero egress fees.
  """
  context.log.info(f"Publishing {graph_id} database to R2 for subscriber downloads")

  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping R2 publish in dev environment")
    return MaterializeResult(
      metadata={
        "status": "skipped",
        "reason": "dev_environment",
        "graph_id": graph_id,
      }
    )

  import boto3

  _validate_shared_graph_id(graph_id)

  r2_info = _build_r2_destination(graph_id)
  bucket = r2_info["bucket"]
  r2_key = r2_info["r2_key"]
  r2_uri = r2_info["r2_uri"]

  context.log.info(f"Target: {r2_uri}")

  result = _run_r2_backup(graph_id, bucket, r2_key)

  if result.get("status") != "completed":
    error = result.get("error", "Unknown error")
    raise RuntimeError(f"R2 publish failed for {graph_id}: {error}")

  r2_config = env.get_r2_config()
  r2_client = boto3.client("s3", **r2_config)
  head = r2_client.head_object(Bucket=bucket, Key=r2_key)
  compressed_size = head["ContentLength"]
  last_modified = head["LastModified"]

  # original_size_bytes is the uncompressed size reported by the backup service
  backup_result = result.get("result", {})
  original_size = backup_result.get("original_size_bytes", compressed_size)
  compression_ratio = backup_result.get("compression_ratio", 1.0)

  context.log.info(
    f"Database published to R2: {r2_uri} "
    f"(original: {original_size / (1024**3):.2f}GB, "
    f"compressed: {compressed_size / (1024**3):.2f}GB, "
    f"ratio: {compression_ratio:.1%}, modified: {last_modified})"
  )

  _upsert_r2_backup_record(
    graph_id=graph_id,
    bucket=bucket,
    key=r2_key,
    original_size=original_size,
    compressed_size=compressed_size,
    backup_type="r2_download",
    db_resource=db_resource,
    dagster_run_id=context.run_id,
  )

  return MaterializeResult(
    metadata={
      "r2_uri": r2_uri,
      "r2_bucket": bucket,
      "r2_key": r2_key,
      "original_size_bytes": original_size,
      "compressed_size_bytes": compressed_size,
      "compression_ratio": round(compression_ratio, 3),
      "file_size_gb": round(compressed_size / (1024**3), 2),
      "last_modified": last_modified.isoformat(),
      "graph_id": graph_id,
      "published_at": datetime.now(UTC).isoformat(),
    }
  )


def _build_r2_destination(graph_id: str) -> dict[str, str]:
  """Build R2 bucket/key/uri for a subscriber download."""
  bucket = env.R2_BUCKET_NAME
  if not bucket:
    raise RuntimeError("R2_BUCKET_NAME not configured")
  r2_key = get_r2_download_key(graph_id, ".lbug.zst")
  r2_uri = f"r2://{bucket}/{r2_key}"
  return {"bucket": bucket, "r2_key": r2_key, "r2_uri": r2_uri}


def _run_r2_backup(graph_id: str, bucket: str, r2_key: str) -> dict[str, Any]:
  """Run the R2 backup via Graph Client Factory."""
  from robosystems.graph_api.client.factory import get_graph_client_for_sec_ingestion

  async def _execute():
    client = await get_graph_client_for_sec_ingestion()
    try:
      return await client.backup_with_sse(
        graph_id=graph_id,
        backup_type="r2_download",
        s3_destination={"bucket": bucket, "key": r2_key},
        compression=True,
        checkpoint=True,
        timeout=TASK_TIME_LIMIT,
      )
    finally:
      await client.close()

  return asyncio.run(_execute())


def _upsert_r2_backup_record(
  graph_id: str,
  bucket: str,
  key: str,
  original_size: int,
  compressed_size: int,
  backup_type: str,
  db_resource,
  dagster_run_id: str = "",
) -> None:
  """Create or update a GraphBackup record for an R2 download.

  Uses a fixed R2 key (overwritten each publish), so we update the existing
  record if one exists rather than creating a new one each time.
  """
  from robosystems.middleware.graph.utils import MultiTenantUtils
  from robosystems.models.core.graph.graph_backup import GraphBackup

  database_name = MultiTenantUtils.get_database_name(graph_id)
  now = datetime.now(UTC)

  with db_resource.get_session() as session:
    existing = GraphBackup.get_latest_successful(graph_id, backup_type, session)

    if existing:
      existing.completed_at = now
      existing.original_size_bytes = original_size
      existing.compressed_size_bytes = compressed_size
      existing.s3_bucket = bucket
      existing.s3_key = key
      existing.backup_metadata = {
        "storage": "r2",
        "format": "zstd",
        "dagster_run_id": dagster_run_id,
      }
      existing.updated_at = now
      session.commit()
    else:
      backup = GraphBackup.create(
        graph_id=graph_id,
        database_name=database_name,
        backup_type=backup_type,
        s3_bucket=bucket,
        s3_key=key,
        session=session,
        compression_enabled=True,
        encryption_enabled=False,
      )
      backup.complete_backup(
        session=session,
        original_size=original_size,
        compressed_size=compressed_size,
        checksum="",
        metadata={
          "storage": "r2",
          "format": "zstd",
          "dagster_run_id": dagster_run_id,
        },
      )
