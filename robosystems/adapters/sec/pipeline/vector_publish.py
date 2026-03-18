"""SEC Vector Index S3 Publish Asset.

Exports the LanceDB vector index from the graph instance and uploads to S3
for replica consumption. Runs after DuckDB S3 publish in the nightly chain:

  materialize → lbug S3 → duckdb S3 → vector S3 → replica refresh

The vector index is built during DuckDB staging (sec_duckdb_staged) via the
Graph API vector/build endpoint. This asset calls vector/export with S3
parameters so the Graph API instance uploads the tar.gz directly to S3
(the Dagster worker on Fargate cannot access the instance's filesystem).

Replicas download the tar.gz at boot and extract it for local vector search.
"""

import asyncio

import boto3
from dagster import (
  AssetExecutionContext,
  MaterializeResult,
  asset,
)

from robosystems.config import env
from robosystems.config.storage.graph import get_shared_repo_database_key


@asset(
  group_name="sec_pipeline",
  description="Export and publish SEC vector index to S3 for replica cluster",
  kinds={"s3", "lancedb"},
  deps=["sec_duckdb_staged"],
  metadata={
    "pipeline": "sec",
    "stage": "vector_publish",
    "replica_source": True,
  },
)
def sec_vector_s3_published(
  context: AssetExecutionContext,
) -> MaterializeResult:
  """Export vector index from graph instance and upload to S3.

  Calls the Graph API vector/export endpoint on the shared master with S3
  bucket/key parameters. The Graph API instance packages the lance index as
  tar.gz and uploads directly to S3 (since this Dagster worker on Fargate
  cannot access the instance's filesystem).

  Returns:
      MaterializeResult with S3 URI and upload statistics
  """
  graph_id = "sec"
  table_name = "Element"

  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping vector S3 publish in dev environment")
    return MaterializeResult(
      metadata={
        "status": "skipped",
        "reason": "dev_environment",
        "graph_id": graph_id,
      }
    )

  # Determine S3 destination
  sts = boto3.client("sts", region_name=env.AWS_REGION)
  account_id = sts.get_caller_identity()["Account"]
  bucket = f"robosystems-{account_id}-user-{env.ENVIRONMENT}"
  s3_key = get_shared_repo_database_key(graph_id, f".{table_name}.lance.tar.gz")

  context.log.info(
    f"Exporting vector index for {graph_id}/{table_name} → s3://{bucket}/{s3_key}"
  )

  # Call vector/export with S3 params — the Graph API instance uploads directly
  from robosystems.graph_api.client.factory import get_graph_client_for_sec_ingestion

  async def run_export():
    client = await get_graph_client_for_sec_ingestion()
    try:
      return await client.vector_export(
        graph_id=graph_id,
        table_name=table_name,
        s3_bucket=bucket,
        s3_key=s3_key,
      )
    finally:
      await client.close()

  try:
    export_result = asyncio.run(run_export())
  except Exception as e:
    context.log.warning(f"Vector export+upload failed (non-fatal): {e}")
    return MaterializeResult(
      metadata={
        "status": "skipped",
        "reason": f"export_failed: {e}",
        "graph_id": graph_id,
      }
    )

  s3_uri = export_result.get("s3_uri")
  if not s3_uri:
    context.log.warning("Vector export did not produce S3 URI")
    return MaterializeResult(
      metadata={"status": "skipped", "reason": "no_s3_uri", "graph_id": graph_id}
    )

  context.log.info(
    f"Vector index published: {s3_uri} "
    f"({export_result.get('size_mb', 0):.1f} MB, "
    f"{export_result.get('duration_ms', 0) / 1000:.1f}s)"
  )

  return MaterializeResult(
    metadata={
      "graph_id": graph_id,
      "table_name": table_name,
      "s3_uri": s3_uri,
      "s3_bucket": bucket,
      "s3_key": s3_key,
      "file_size_mb": export_result.get("size_mb", 0),
      "export_duration_ms": export_result.get("duration_ms", 0),
    }
  )
