"""Publish the sec and sec_historical .lbug databases to S3; replicas download
them to local disk on boot."""

from dagster import (
  AssetExecutionContext,
  MaterializeResult,
  asset,
)

from robosystems.dagster.assets.shared_repositories.publish import publish_to_s3


@asset(
  group_name="sec_pipeline",
  description="Publish SEC database to S3 for replica cluster",
  kinds={"s3", "ladybug"},
  deps=["sec_graph_materialized"],
  metadata={
    "pipeline": "sec",
    "stage": "s3_publish",
    "replica_source": True,
  },
)
def sec_lbug_s3_published(
  context: AssetExecutionContext,
) -> MaterializeResult:
  """Publish the SEC database to S3 for the replica fleet."""
  return publish_to_s3(context, graph_id="sec")


@asset(
  group_name="sec_pipeline",
  description="Publish SEC historical database to S3 for replica cluster",
  kinds={"s3", "ladybug"},
  deps=["sec_historical_materialized"],
  metadata={
    "pipeline": "sec",
    "stage": "s3_publish",
    "replica_source": True,
  },
)
def sec_historical_lbug_s3_published(
  context: AssetExecutionContext,
) -> MaterializeResult:
  """Publish the SEC historical database to S3, for replicas to ATTACH."""
  return publish_to_s3(context, graph_id="sec_historical")
