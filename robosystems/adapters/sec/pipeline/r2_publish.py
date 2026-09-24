"""sec_lbug_r2_published: the raw .lbug on Cloudflare R2 for zero-egress
subscriber downloads."""

import asyncio
from typing import Any

from dagster import (
  AssetExecutionContext,
  MaterializeResult,
  asset,
)

from robosystems.adapters.sec.pipeline.dataset_card import capture_snapshot_stats
from robosystems.config import env
from robosystems.dagster.assets.shared_repositories.publish import publish_to_r2
from robosystems.dagster.resources import DatabaseResource


def _capture_stats(context: AssetExecutionContext) -> dict[str, Any] | None:
  """Count the graph on the master the snapshot is about to be cut from.

  A failure here does not block the subscriber download: the snapshot still
  publishes without a stats file, and the Hugging Face publish refuses to run
  until a snapshot carries one.
  """
  from robosystems.graph_api.client.factory import get_graph_client_for_sec_ingestion

  if env.ENVIRONMENT == "dev":
    return None

  async def _execute():
    client = await get_graph_client_for_sec_ingestion()
    try:
      return await capture_snapshot_stats(client)
    finally:
      await client.close()

  try:
    return asyncio.run(_execute())
  except Exception as e:
    context.log.warning(f"Snapshot stats not captured; publishing without them: {e}")
    return None


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
  """Publish the SEC database to R2, with the stats file counted just before
  the cut."""
  stats = _capture_stats(context)
  return publish_to_r2(context, graph_id="sec", db_resource=db, stats=stats)
