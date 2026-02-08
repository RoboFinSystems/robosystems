"""SEC Entity Update Asset.

This module contains the sec_entity_incremental_update asset for updating
mutable Entity attributes in LadybugDB.
"""

from dagster import AssetExecutionContext, MaterializeResult, asset

from .configs import SECEntityUpdateConfig


@asset(
  group_name="sec_pipeline",
  description="Update mutable Entity attributes via Cypher MERGE",
  kinds={"ladybug"},
  deps=["sec_graph_materialized"],  # Run after graph materialization
  metadata={
    "pipeline": "sec",
    "graph_id": "sec",
    "stage": "entity_update",
    "mode": "incremental",
  },
)
def sec_entity_incremental_update(
  context: AssetExecutionContext,
  config: SECEntityUpdateConfig,
) -> MaterializeResult:
  """Update existing Entity nodes with latest attribute values.

  This solves the Entity mutability problem: unlike other XBRL nodes (facts,
  periods, etc.) which are immutable, Entity attributes can change over time:
  - Company name changes
  - Ticker/exchange changes (listing updates)
  - Filer category changes (large accelerated filer, etc.)
  - Fiscal year end changes
  - Contact info updates (phone, website)

  The incremental COPY operation only INSERTs new records - it cannot update
  existing ones. This asset uses Cypher MERGE to update existing Entity nodes.

  Process:
  1. Read latest Entity parquet from S3 (current quarter)
  2. Query existing Entity nodes from LadybugDB
  3. Compare and identify entities with actual changes
  4. Execute MERGE queries in batches to update changed entities

  Note: Only entities with actual changes are updated (typically 50-200 per
  quarter). MERGE is 40x slower than COPY, but this is acceptable for the
  small number of updates.

  Run with:
    uv run dagster asset materialize -m robosystems.dagster --select sec_entity_incremental_update
  """
  import asyncio

  from robosystems.adapters.sec import XBRLDuckDBGraphProcessor

  context.log.info(
    f"Starting Entity update for graph {config.graph_id} "
    f"(Q{config.quarter or 'current'} {config.year or 'current'})"
  )

  processor = XBRLDuckDBGraphProcessor(graph_id=config.graph_id)

  async def run_entity_update():
    return await processor.update_entities_from_s3(
      year=config.year,
      quarter=config.quarter,
      progress_callback=context.log.info,
    )

  result = asyncio.run(run_entity_update())

  if result.status == "error":
    context.log.error(f"Entity update failed: {result.error}")
    return MaterializeResult(
      metadata={
        "graph_id": config.graph_id,
        "status": "error",
        "error": result.error or "Unknown error",
        "duration_ms": result.duration_ms,
      }
    )

  context.log.info(
    f"Entity update complete: {result.entities_updated} updated, "
    f"{result.entities_unchanged} unchanged, {result.entities_failed} failed "
    f"({result.duration_ms / 1000:.2f}s)"
  )

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "status": result.status,
      "year": config.year,
      "quarter": config.quarter,
      "entities_checked": result.entities_checked,
      "entities_updated": result.entities_updated,
      "entities_unchanged": result.entities_unchanged,
      "entities_failed": result.entities_failed,
      "duration_ms": result.duration_ms,
    }
  )
