"""SEC DuckDB S3 Publish Assets.

Publishes DuckDB staging databases to S3 as raw .duckdb files.

Two assets for the two DuckDB staging databases:
- sec_duckdb_s3_published: Publishes sec.duckdb (2024+ data, ~27GB)
- sec_historical_duckdb_s3_published: Publishes sec_historical.duckdb (2009-2023, ~33GB)

These complement the .lbug publish assets (sec_lbug_s3_published,
sec_historical_lbug_s3_published) which serve the replica cluster.
"""

from dagster import (
  AssetExecutionContext,
  MaterializeResult,
  asset,
)

from robosystems.dagster.assets.shared_repositories.publish import publish_duckdb_to_s3


@asset(
  group_name="sec_pipeline",
  description="Publish SEC DuckDB staging to S3 (raw .duckdb)",
  kinds={"s3", "duckdb"},
  deps=["sec_duckdb_staged"],
  metadata={
    "pipeline": "sec",
    "stage": "duckdb_s3_publish",
  },
)
def sec_duckdb_s3_published(
  context: AssetExecutionContext,
) -> MaterializeResult:
  """Publish SEC DuckDB staging database to S3.

  Delegates to the shared publish_duckdb_to_s3() helper which handles:
  - Graph Client Factory (auth, routing, circuit breakers)
  - DuckDB CHECKPOINT + S3 multipart upload on-instance
  - Upload verification

  Returns:
      MaterializeResult with S3 URI and upload statistics
  """
  return publish_duckdb_to_s3(context, graph_id="sec")


@asset(
  group_name="sec_pipeline",
  description="Publish SEC historical DuckDB staging to S3 (raw .duckdb)",
  kinds={"s3", "duckdb"},
  deps=["sec_historical_duckdb_staged"],
  metadata={
    "pipeline": "sec",
    "stage": "duckdb_s3_publish",
  },
)
def sec_historical_duckdb_s3_published(
  context: AssetExecutionContext,
) -> MaterializeResult:
  """Publish SEC historical DuckDB staging database to S3.

  Returns:
      MaterializeResult with S3 URI and upload statistics
  """
  return publish_duckdb_to_s3(context, graph_id="sec_historical")
