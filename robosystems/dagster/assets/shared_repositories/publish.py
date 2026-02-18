"""Shared Repository S3 Publish Helpers.

Provides the core publish-to-S3 logic that per-repository assets call.
Each shared repository (SEC, future industry/economic) defines a thin
asset with deps on its own materialization, then delegates to publish_to_s3().

Replicas use LadybugDB S3 ATTACH to connect directly to the published
database via the httpfs extension. This is distinct from user backup
which creates compressed, downloadable backups for subscribers.
"""

import asyncio
from datetime import UTC, datetime
from typing import Any

from dagster import AssetExecutionContext, MaterializeResult

from robosystems.config import env
from robosystems.config.constants import TASK_TIME_LIMIT
from robosystems.config.storage.graph import get_shared_repo_database_key


def publish_to_s3(
  context: AssetExecutionContext,
  graph_id: str,
) -> MaterializeResult:
  """Publish a shared repository database to S3 for replica consumption.

  Uses Graph Client Factory to call the backup endpoint on the shared master.
  The backup runs entirely on-instance (CHECKPOINT + S3 multipart upload),
  so this only orchestrates and monitors via SSE.

  Args:
      context: Dagster asset execution context
      graph_id: Shared repository graph ID (e.g., "sec", "industry")

  Returns:
      MaterializeResult with S3 URI and upload statistics
  """
  context.log.info(f"Publishing {graph_id} database to S3 for replica cluster")

  # Skip in dev environment
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

  from robosystems.middleware.graph.utils import MultiTenantUtils
  from robosystems.middleware.graph.utils.subgraph import parse_subgraph_id

  # Validate graph_id is a shared repository or subgraph of one
  is_shared = MultiTenantUtils.is_shared_repository(graph_id)
  if not is_shared:
    subgraph_info = parse_subgraph_id(graph_id)
    if not subgraph_info or not MultiTenantUtils.is_shared_repository(
      subgraph_info.parent_graph_id
    ):
      raise ValueError(f"{graph_id} is not a shared repository or subgraph of one")

  # Build S3 destination
  s3_info = _build_s3_destination(graph_id)
  bucket = s3_info["bucket"]
  s3_key = s3_info["s3_key"]
  s3_uri = s3_info["s3_uri"]

  context.log.info(f"Target: {s3_uri}")

  # Use Graph Client Factory (handles auth, routing, circuit breakers)
  result = _run_backup(graph_id, bucket, s3_key)

  if result.get("status") != "completed":
    error = result.get("error", "Unknown error")
    raise RuntimeError(f"S3 publish failed for {graph_id}: {error}")

  # Verify upload
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
  """Publish a DuckDB staging database to S3.

  Uses Graph Client Factory to call the backup endpoint on the shared master.
  The backup runs entirely on-instance (DuckDB CHECKPOINT + S3 multipart upload),
  so this only orchestrates and monitors via SSE.

  Args:
      context: Dagster asset execution context
      graph_id: Shared repository graph ID (e.g., "sec", "sec_historical")

  Returns:
      MaterializeResult with S3 URI and upload statistics
  """
  context.log.info(f"Publishing {graph_id} DuckDB staging to S3")

  # Skip in dev environment
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

  from robosystems.middleware.graph.utils import MultiTenantUtils

  # Validate graph_id is a shared repository
  if not MultiTenantUtils.is_shared_repository(graph_id):
    raise ValueError(f"{graph_id} is not a shared repository")

  # Build S3 destination (uses .duckdb extension instead of .lbug)
  s3_info = _build_duckdb_s3_destination(graph_id)
  bucket = s3_info["bucket"]
  s3_key = s3_info["s3_key"]
  s3_uri = s3_info["s3_uri"]

  context.log.info(f"Target: {s3_uri}")

  # Use Graph Client Factory (handles auth, routing, circuit breakers)
  result = _run_duckdb_backup(graph_id, bucket, s3_key)

  if result.get("status") != "completed":
    error = result.get("error", "Unknown error")
    raise RuntimeError(f"DuckDB S3 publish failed for {graph_id}: {error}")

  # Verify upload
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
