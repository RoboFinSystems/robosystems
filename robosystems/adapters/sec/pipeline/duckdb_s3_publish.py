"""Publish the sec and sec_historical DuckDB staging databases to S3 as raw
.duckdb files (VACUUMed first to reclaim space from incremental staging)."""

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
  """Publish the sec DuckDB staging database to S3."""
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
  """Publish the sec_historical DuckDB staging database to S3."""
  return publish_duckdb_to_s3(context, graph_id="sec_historical")
