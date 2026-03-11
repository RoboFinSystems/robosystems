"""SEC Materialization Assets.

This module contains the graph materialization assets:
- sec_graph_materialized: Full DuckDB → LadybugDB materialization
- sec_historical_materialized: Full DuckDB → LadybugDB materialization for sec_historical
"""

from dagster import AssetExecutionContext, Failure, MaterializeResult, asset

from robosystems.config import env

from .configs import SECMaterializeConfig


@asset(
  group_name="sec_pipeline",
  description="Materialize SEC graph from DuckDB to LadybugDB",
  kinds={"ladybug"},
  deps=["sec_duckdb_staged", "sec_duckdb_incremental_staged"],
  metadata={
    "pipeline": "sec",
    "graph_id": "sec",
    "stage": "materialize",
    "mode": "full",
  },
)
def sec_graph_materialized(
  context: AssetExecutionContext,
  config: SECMaterializeConfig,
) -> MaterializeResult:
  """Materialize LadybugDB graph from DuckDB staging.

  This is Stage 2 of the pipeline. It reads from the persistent
  DuckDB staging tables and materializes to LadybugDB.

  Precondition: sec_duckdb_staged must have completed successfully,
  creating a valid staging manifest.

  Key features:
  - Reads from persisted DuckDB (no S3 access needed)
  - Can be retried independently if materialization fails
  - Uses manifest to verify staging completeness

  Run with:
    uv run dagster asset materialize -m robosystems.dagster --select sec_graph_materialized

  Returns:
      MaterializeResult with materialization statistics
  """
  import asyncio

  from robosystems.adapters.sec import XBRLDuckDBGraphProcessor

  context.log.info(f"Materializing graph from DuckDB staging: {config.graph_id}")
  if config.rebuild_graph:
    context.log.info("Rebuild requested - will delete and recreate LadybugDB database")

  # Boost LadybugDB memory before materialization (only applies to ladybug-shared tier)
  try:
    from robosystems.graph_api.client.factory import boost_graph_memory

    boost_result = asyncio.run(boost_graph_memory(config.graph_id, target="ladybug"))
    context.log.info(f"Memory boost: {boost_result.get('message', 'done')}")
  except Exception as boost_err:
    context.log.warning(f"Could not boost memory (non-fatal): {boost_err}")

  processor = XBRLDuckDBGraphProcessor(graph_id=config.graph_id)

  # Progress callback for Dagster logging (visible in Dagster UI)
  def dagster_progress(msg: str) -> None:
    context.log.info(msg)

  async def run_materialization():
    result = await processor.materialize_from_duckdb(
      rebuild=config.rebuild_graph,
      skip_taxonomy_relationships=config.skip_taxonomy_relationships,
      batch_materialization=config.batch_materialization,
      batch_size=config.materialization_batch_size,
      progress_callback=dagster_progress,
    )
    return result

  result = asyncio.run(run_materialization())

  if result.status == "error":
    context.log.error(f"Materialization failed: {result.error}")
    raise Failure(
      description=f"Materialization failed: {result.error}",
      metadata={
        "graph_id": config.graph_id,
        "status": "error",
        "error": result.error or "",
      },
    )

  if result.status == "partial":
    failed = [t for t in (result.tables or []) if t.get("status") == "error"]
    failed_names = [t["table_name"] for t in failed]
    for t in failed:
      context.log.error(f"FAILED: {t['table_name']} — {t.get('error', 'unknown')}")
    raise Failure(
      description=(
        f"Materialization incomplete: {len(failed)} table(s) failed: {failed_names}. "
        f"Blocking S3 publish to prevent bad data reaching replicas."
      ),
      metadata={
        "graph_id": config.graph_id,
        "status": "partial",
        "total_rows_ingested": result.total_rows_ingested,
        "duration_ms": result.duration_ms,
        "failed_tables": failed_names,
        "tables": result.tables or [],
      },
    )

  context.log.info(
    f"Materialization complete: {result.total_rows_ingested} rows, "
    f"{result.duration_ms / 1000:.2f}s"
  )

  # Release memory after materialization (closes connections, frees buffers to OS)
  # This is more aggressive than restore - it actually releases the memory
  try:
    from robosystems.graph_api.client.factory import release_graph_memory

    release_result = asyncio.run(release_graph_memory(config.graph_id, target="both"))
    context.log.info(f"Memory release: {release_result.get('message', 'done')}")
  except Exception as release_err:
    # Don't fail the job if release fails - materialization succeeded
    context.log.warning(f"Could not release memory (non-fatal): {release_err}")

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "rebuild_graph": config.rebuild_graph,
      "status": result.status,
      "total_rows_ingested": result.total_rows_ingested,
      "duration_ms": result.duration_ms,
      "tables": result.tables,
    }
  )


@asset(
  group_name="sec_pipeline",
  description="Materialize SEC historical graph from DuckDB to LadybugDB",
  kinds={"ladybug"},
  deps=["sec_historical_duckdb_staged"],
  metadata={
    "pipeline": "sec",
    "graph_id": "sec_historical",
    "stage": "materialize",
    "mode": "full",
  },
)
def sec_historical_materialized(
  context: AssetExecutionContext,
  config: SECMaterializeConfig,
) -> MaterializeResult:
  """Materialize sec_historical LadybugDB graph from DuckDB staging.

  This is Stage 2 of the historical pipeline. It reads from the persistent
  DuckDB staging tables (sec_historical.duckdb) and materializes to the
  sec_historical LadybugDB subgraph.

  Precondition: sec_historical_duckdb_staged must have completed successfully.

  Run with:
    uv run dagster asset materialize -m robosystems.dagster --select sec_historical_materialized

  Returns:
      MaterializeResult with materialization statistics
  """
  import asyncio

  from robosystems.adapters.sec import XBRLDuckDBGraphProcessor
  from robosystems.operations.graph.shared_repository_service import (
    ensure_shared_subgraph_exists,
  )

  # Apply defaults for historical graph
  graph_id = config.graph_id if config.graph_id != "sec" else "sec_historical"

  context.log.info(f"Materializing historical graph from DuckDB staging: {graph_id}")
  if config.rebuild_graph:
    context.log.info("Rebuild requested - will delete and recreate LadybugDB database")

  # Boost LadybugDB memory before materialization
  try:
    from robosystems.graph_api.client.factory import boost_graph_memory

    boost_result = asyncio.run(boost_graph_memory(graph_id, target="ladybug"))
    context.log.info(f"Memory boost: {boost_result.get('message', 'done')}")
  except Exception as boost_err:
    context.log.warning(f"Could not boost memory (non-fatal): {boost_err}")

  processor = XBRLDuckDBGraphProcessor(graph_id=graph_id)

  def dagster_progress(msg: str) -> None:
    context.log.info(msg)

  async def run_materialization():
    # Ensure the sec_historical subgraph exists
    subgraph_result = await ensure_shared_subgraph_exists(
      parent_repository_name="sec",
      subgraph_name="historical",
      description="SEC Historical Filings (2009-2023)",
      created_by="system",
      instance_id="local-dev" if env.ENVIRONMENT == "dev" else "ladybug-shared-prod",
    )
    context.log.info(f"Subgraph status: {subgraph_result.get('status')}")

    result = await processor.materialize_from_duckdb(
      rebuild=config.rebuild_graph,
      skip_taxonomy_relationships=config.skip_taxonomy_relationships,
      batch_materialization=config.batch_materialization,
      batch_size=config.materialization_batch_size,
      progress_callback=dagster_progress,
    )
    return result

  result = asyncio.run(run_materialization())

  if result.status == "error":
    context.log.error(f"Materialization failed: {result.error}")
    raise Failure(
      description=f"Historical materialization failed: {result.error}",
      metadata={
        "graph_id": graph_id,
        "status": "error",
        "error": result.error or "",
      },
    )

  if result.status == "partial":
    failed = [t for t in (result.tables or []) if t.get("status") == "error"]
    failed_names = [t["table_name"] for t in failed]
    for t in failed:
      context.log.error(f"FAILED: {t['table_name']} — {t.get('error', 'unknown')}")
    raise Failure(
      description=(
        f"Historical materialization incomplete: {len(failed)} table(s) failed: {failed_names}. "
        f"Blocking S3 publish to prevent bad data reaching replicas."
      ),
      metadata={
        "graph_id": graph_id,
        "status": "partial",
        "total_rows_ingested": result.total_rows_ingested,
        "duration_ms": result.duration_ms,
        "failed_tables": failed_names,
        "tables": result.tables or [],
      },
    )

  context.log.info(
    f"Historical materialization complete: {result.total_rows_ingested} rows, "
    f"{result.duration_ms / 1000:.2f}s"
  )

  # Release memory after materialization
  try:
    from robosystems.graph_api.client.factory import release_graph_memory

    release_result = asyncio.run(release_graph_memory(graph_id, target="both"))
    context.log.info(f"Memory release: {release_result.get('message', 'done')}")
  except Exception as release_err:
    context.log.warning(f"Could not release memory (non-fatal): {release_err}")

  return MaterializeResult(
    metadata={
      "graph_id": graph_id,
      "rebuild_graph": config.rebuild_graph,
      "status": result.status,
      "total_rows_ingested": result.total_rows_ingested,
      "duration_ms": result.duration_ms,
      "tables": result.tables,
    }
  )
