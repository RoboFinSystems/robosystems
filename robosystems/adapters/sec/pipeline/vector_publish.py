"""SEC Vector Index S3 Publish Asset.

Exports the LanceDB vector index from the graph instance and uploads to S3
for replica consumption. Runs after DuckDB S3 publish in the nightly chain:

  materialize → lbug S3 → duckdb S3 → vector S3 → replica refresh

The vector index is built during DuckDB staging (sec_duckdb_staged) via the
Graph API vector/build endpoint. This asset calls vector/export to package
it as tar.gz, then uploads to S3 alongside the .lbug and .duckdb files.

Replicas download the tar.gz at boot and extract it for local vector search.
"""

import asyncio

from dagster import (
  AssetExecutionContext,
  MaterializeResult,
  asset,
)

from robosystems.config import env


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

  Calls the Graph API vector/export endpoint on the shared master, then
  uploads the resulting tar.gz to S3 where replicas download it at boot.

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

  # Step 1: Call vector/export on the graph instance to produce tar.gz
  context.log.info(f"Exporting vector index for {graph_id}/{table_name}...")

  from robosystems.graph_api.client.factory import get_graph_client_for_sec_ingestion

  async def run_export():
    client = await get_graph_client_for_sec_ingestion()
    try:
      return await client.vector_export(graph_id=graph_id, table_name=table_name)
    finally:
      await client.close()

  try:
    export_result = asyncio.run(run_export())
  except Exception as e:
    context.log.warning(f"Vector export failed (non-fatal): {e}")
    return MaterializeResult(
      metadata={
        "status": "skipped",
        "reason": f"export_failed: {e}",
        "graph_id": graph_id,
      }
    )

  tar_path = export_result.get("tar_path")
  if not tar_path:
    context.log.warning("Vector export returned no tar_path")
    return MaterializeResult(
      metadata={"status": "skipped", "reason": "no_tar_path", "graph_id": graph_id}
    )

  context.log.info(
    f"Vector index exported: {export_result.get('size_mb', 0):.1f} MB "
    f"in {export_result.get('duration_ms', 0) / 1000:.1f}s"
  )

  # Step 2: Upload tar.gz to S3
  import boto3

  from robosystems.config.storage.graph import get_shared_repo_database_key

  sts = boto3.client("sts", region_name=env.AWS_REGION)
  account_id = sts.get_caller_identity()["Account"]
  bucket = f"robosystems-{account_id}-user-{env.ENVIRONMENT}"
  s3_key = get_shared_repo_database_key(graph_id, f".{table_name}.lance.tar.gz")

  context.log.info(f"Uploading vector index to s3://{bucket}/{s3_key}")

  s3 = boto3.client("s3", region_name=env.AWS_REGION)
  s3.upload_file(tar_path, bucket, s3_key)

  # Verify upload
  head = s3.head_object(Bucket=bucket, Key=s3_key)
  file_size = head["ContentLength"]

  context.log.info(
    f"Vector index published to S3: s3://{bucket}/{s3_key} "
    f"({file_size / (1024**2):.1f} MB)"
  )

  return MaterializeResult(
    metadata={
      "graph_id": graph_id,
      "table_name": table_name,
      "s3_uri": f"s3://{bucket}/{s3_key}",
      "s3_bucket": bucket,
      "s3_key": s3_key,
      "file_size_bytes": file_size,
      "file_size_mb": round(file_size / (1024**2), 2),
      "export_duration_ms": export_result.get("duration_ms", 0),
    }
  )
