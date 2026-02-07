"""Shared Repository S3 Publish Asset.

This asset publishes a shared repository database to S3 for replica cluster
consumption. Replicas use LadybugDB S3 ATTACH to connect directly to the
published database via the httpfs extension.

The asset:
1. Uses Graph Client Factory to call the backup endpoint on the shared master
2. The Graph API runs CHECKPOINT and uploads raw .lbug to S3 on-instance

Replica fleet refresh is handled by the separate shared_replicas_refreshed asset.

This is distinct from user backup which creates compressed, downloadable backups
for subscribers. This asset creates the source-of-truth for the replica cluster.
"""

import asyncio
from datetime import UTC, datetime

from dagster import (
  AssetExecutionContext,
  Config,
  MaterializeResult,
  asset,
)

from robosystems.config import env
from robosystems.config.constants import TASK_TIME_LIMIT


class SharedRepositoryPublishConfig(Config):
  """Configuration for S3 publish operations."""

  graph_id: str = "sec"


@asset(
  group_name="shared_repositories",
  description="Publish shared repository database to S3 for replica cluster (S3 ATTACH source)",
  kinds={"s3", "ladybug"},
  metadata={
    "stage": "publish",
    "replica_source": True,
  },
)
def shared_repository_s3_published(
  context: AssetExecutionContext,
  config: SharedRepositoryPublishConfig,
) -> MaterializeResult:
  """Publish shared repository database to S3 for replica consumption via ATTACH.

  Uses Graph Client Factory to call the backup endpoint on the shared master.
  The backup runs entirely on-instance (CHECKPOINT + S3 multipart upload),
  so this Dagster task only orchestrates and monitors via SSE.

  Returns:
      MaterializeResult with S3 URI and upload statistics
  """
  graph_id = config.graph_id
  context.log.info(f"Publishing {graph_id} database to S3 for replica cluster")

  # Skip in dev environment
  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping S3 publish in dev environment")
    return MaterializeResult(
      metadata={
        "status": "skipped",
        "reason": "dev_environment",
      }
    )

  import boto3

  from robosystems.graph_api.client.factory import get_graph_client_for_sec_ingestion
  from robosystems.middleware.graph.utils import MultiTenantUtils

  # Validate graph_id is a shared repository
  if not MultiTenantUtils.is_shared_repository(graph_id):
    raise ValueError(f"{graph_id} is not a shared repository")

  # Build S3 destination
  sts = boto3.client("sts", region_name=env.AWS_REGION)
  account_id = sts.get_caller_identity()["Account"]
  bucket = f"robosystems-{account_id}-user-{env.ENVIRONMENT}"
  s3_key = f"shared-repos/{graph_id}.lbug"
  s3_uri = f"s3://{bucket}/{s3_key}"

  context.log.info(f"Target: {s3_uri}")

  # Use Graph Client Factory (handles auth, routing, circuit breakers)
  loop = asyncio.new_event_loop()
  client = None
  try:
    client = loop.run_until_complete(get_graph_client_for_sec_ingestion())

    context.log.info("Calling backup endpoint on shared master...")
    result = loop.run_until_complete(
      client.backup_with_sse(
        graph_id=graph_id,
        backup_type="replica",
        s3_destination={"bucket": bucket, "key": s3_key},
        compression=False,
        checkpoint=True,
        timeout=TASK_TIME_LIMIT,
      )
    )
  finally:
    if client:
      loop.run_until_complete(client.close())
    loop.close()

  if result.get("status") != "completed":
    error = result.get("error", "Unknown error")
    raise RuntimeError(f"S3 publish failed: {error}")

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
