"""SEC S3 Publish Asset.

Publishes the SEC shared repository database to S3 for replica cluster
consumption. Runs after SEC materialization completes.

This is a thin wrapper around the shared publish_to_s3() helper.
Each shared repository defines its own
publish asset with deps on its own materialization asset.

The nightly chain (sensor-driven):
  materialize → sec_lbug_s3_published → sec_duckdb_s3_published → replica refresh

This asset creates the raw .lbug source-of-truth for the replica fleet.
Replicas download this file from S3 to local disk on boot.
"""

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
  """Publish SEC database to S3 for replica fleet.

  Delegates to the shared publish_to_s3() helper which handles:
  - Graph Client Factory (auth, routing, circuit breakers)
  - CHECKPOINT + S3 multipart upload on-instance
  - Upload verification
  """
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
