"""SEC S3 Publish Asset.

Publishes the SEC shared repository database to S3 for replica cluster
consumption. Runs after SEC materialization completes.

This is a thin wrapper around the shared publish_to_s3() helper.
Each shared repository (SEC, future industry/economic) defines its own
publish asset with deps on its own materialization asset.

The lineage chain:
  sec_graph_materialized -> sec_s3_published -> shared_replicas_refreshed

This complements sec_backup which creates compressed, downloadable backups
for users. This asset creates the raw .lbug source-of-truth for the
replica fleet via S3 ATTACH.
"""

from dagster import (
  AssetExecutionContext,
  Config,
  MaterializeResult,
  asset,
)

from robosystems.dagster.assets.shared_repositories.publish import publish_to_s3


class SECPublishConfig(Config):
  """Configuration for SEC S3 publish.

  By default publishes the primary "sec" graph. Override graph_id to
  "sec_historical" when rebuilding the historical graph.
  """

  graph_id: str = "sec"


@asset(
  group_name="sec_pipeline",
  description="Publish SEC database to S3 for replica cluster (S3 ATTACH source)",
  kinds={"s3", "ladybug"},
  deps=["sec_graph_materialized"],
  metadata={
    "pipeline": "sec",
    "stage": "s3_publish",
    "replica_source": True,
  },
)
def sec_s3_published(
  context: AssetExecutionContext,
  config: SECPublishConfig,
) -> MaterializeResult:
  """Publish SEC database to S3 for replica consumption via ATTACH.

  Delegates to the shared publish_to_s3() helper which handles:
  - Graph Client Factory (auth, routing, circuit breakers)
  - CHECKPOINT + S3 multipart upload on-instance
  - Upload verification

  Configure graph_id to "sec_historical" to publish the historical graph.

  Returns:
      MaterializeResult with S3 URI and upload statistics
  """
  return publish_to_s3(context, graph_id=config.graph_id)
