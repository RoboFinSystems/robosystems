"""SEC DuckDB S3 Publish Assets.

Publishes DuckDB staging databases to S3 for local development and analytics.
Each DuckDB file (~27-33GB) is uploaded as a raw .duckdb file to S3.

Two assets for the two DuckDB staging databases:
- sec_duckdb_s3_published: Publishes sec.duckdb (2024+ data, ~27GB)
- sec_historical_duckdb_s3_published: Publishes sec_historical.duckdb (2009-2023, ~33GB)

These complement the existing .lbug publish (sec_s3_published) which serves
the replica cluster. DuckDB files are useful for direct SQL analytics without
needing a running LadybugDB instance.
"""

from dagster import (
  AssetExecutionContext,
  Config,
  MaterializeResult,
  asset,
)

from robosystems.dagster.assets.shared_repositories.publish import publish_duckdb_to_s3


class SECDuckDBPublishConfig(Config):
  """Configuration for SEC DuckDB S3 publish."""

  graph_id: str = "sec"


@asset(
  group_name="sec_pipeline",
  description="Publish SEC DuckDB staging to S3 (raw .duckdb for local dev / analytics)",
  kinds={"s3", "duckdb"},
  deps=["sec_duckdb_staged"],
  metadata={
    "pipeline": "sec",
    "stage": "duckdb_s3_publish",
  },
)
def sec_duckdb_s3_published(
  context: AssetExecutionContext,
  config: SECDuckDBPublishConfig,
) -> MaterializeResult:
  """Publish SEC DuckDB staging database to S3.

  Delegates to the shared publish_duckdb_to_s3() helper which handles:
  - Graph Client Factory (auth, routing, circuit breakers)
  - DuckDB CHECKPOINT + S3 multipart upload on-instance
  - Upload verification

  Returns:
      MaterializeResult with S3 URI and upload statistics
  """
  return publish_duckdb_to_s3(context, graph_id=config.graph_id)


@asset(
  group_name="sec_pipeline",
  description="Publish SEC historical DuckDB staging to S3 (raw .duckdb for local dev / analytics)",
  kinds={"s3", "duckdb"},
  deps=["sec_historical_duckdb_staged"],
  metadata={
    "pipeline": "sec",
    "stage": "duckdb_s3_publish",
  },
)
def sec_historical_duckdb_s3_published(
  context: AssetExecutionContext,
  config: SECDuckDBPublishConfig,
) -> MaterializeResult:
  """Publish SEC historical DuckDB staging database to S3.

  Returns:
      MaterializeResult with S3 URI and upload statistics
  """
  return publish_duckdb_to_s3(context, graph_id="sec_historical")
