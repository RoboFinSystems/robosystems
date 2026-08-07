"""SEC R2 Publish Asset.

Publishes the SEC shared repository .lbug database to Cloudflare R2 for
zero-egress subscriber downloads. Runs after SEC materialization completes.

This is a thin wrapper around the shared publish_to_r2() helper.
The lineage chain:
  sec_graph_materialized -> sec_lbug_r2_published

This complements sec_lbug_s3_published (which serves the replica fleet).
R2 provides the same raw .lbug file but with zero download egress costs.
"""

from dagster import (
  AssetExecutionContext,
  MaterializeResult,
  asset,
)

from robosystems.dagster.assets.shared_repositories.publish import publish_to_r2
from robosystems.dagster.resources import DatabaseResource


@asset(
  group_name="sec_pipeline",
  description="Publish SEC database to R2 for zero-egress subscriber downloads",
  kinds={"r2", "ladybug"},
  deps=["sec_graph_materialized"],
  metadata={
    "pipeline": "sec",
    "stage": "r2_publish",
    "download_source": True,
  },
)
def sec_lbug_r2_published(
  context: AssetExecutionContext,
  db: DatabaseResource,
) -> MaterializeResult:
  """Publish SEC database to R2 for subscriber downloads.

  Delegates to the shared publish_to_r2() helper which handles:
  - Graph Client Factory (auth, routing, circuit breakers)
  - CHECKPOINT + R2 multipart upload on-instance
  - Upload verification
  - GraphBackup record upsert
  """
  return publish_to_r2(context, graph_id="sec", db_resource=db)
