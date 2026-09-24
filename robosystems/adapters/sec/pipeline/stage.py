"""SEC DuckDB staging assets: full rebuilds for sec and sec_historical, and the
nightly incremental quarter."""

from dagster import AssetExecutionContext, Failure, MaterializeResult, asset

from robosystems.config import env

from .configs import SECHistoricalStageConfig, SECIncrementalStageConfig, SECStageConfig


@asset(
  group_name="sec_pipeline",
  description="Stage SEC parquet files to DuckDB (full rebuild)",
  kinds={"duckdb"},
  metadata={
    "pipeline": "sec",
    "graph_id": "sec",
    "stage": "stage",
    "mode": "full",
  },
)
def sec_duckdb_staged(
  context: AssetExecutionContext,
  config: SECStageConfig,
) -> MaterializeResult:
  """Rebuild the persistent DuckDB staging from the processed parquet files, so
  materialization can run (and re-run) on its own."""
  import asyncio

  from robosystems.adapters.sec import XBRLDuckDBGraphProcessor
  from robosystems.operations.graph.shared_repository_service import (
    ensure_shared_repository_exists,
  )

  context.log.info(f"Staging SEC data to DuckDB for graph: {config.graph_id}")
  if config.year:
    context.log.info(f"Year filter: {config.year}")
  if config.start_year or config.end_year:
    context.log.info(f"Year range: {config.start_year}-{config.end_year}")
  if config.reset_staging:
    context.log.info("Reset staging enabled - will delete DuckDB file first")

  # Only takes effect on the ladybug-shared tier.
  duckdb_memory_mb: int | None = None
  try:
    from robosystems.graph_api.client.factory import boost_graph_memory

    boost_result = asyncio.run(boost_graph_memory(config.graph_id, target="duckdb"))
    duckdb_memory_mb = boost_result.get("duckdb_boost_mb")
    context.log.info(f"Memory boost: {boost_result.get('message', 'done')}")
  except Exception as boost_err:
    context.log.warning(f"Could not boost memory (non-fatal): {boost_err}")

  processor = XBRLDuckDBGraphProcessor(graph_id=config.graph_id)

  def dagster_progress(msg: str) -> None:
    context.log.info(msg)

  async def run_staging():
    context.log.info("Ensuring SEC repository metadata exists...")
    repo_result = await ensure_shared_repository_exists(
      repository_name=config.graph_id,
      created_by="system",
      instance_id="local-dev" if env.ENVIRONMENT == "dev" else "ladybug-shared-prod",
    )
    context.log.info(f"SEC repository status: {repo_result.get('status', 'unknown')}")

    # The busy counter on the master running the DuckDB writes makes a deploy's
    # pre-refresh wait before cycling it. Imported lazily to keep boto3 and
    # GraphClientFactory out of SEC module load.
    from robosystems.middleware.graph.instance_busy import (
      OP_KIND_SEC_STAGING,
      begin_destructive_op,
      end_destructive_op,
      resolve_instance_id_for_graph,
    )

    busy_instance_id = await resolve_instance_id_for_graph(config.graph_id)
    await begin_destructive_op(busy_instance_id, OP_KIND_SEC_STAGING)
    try:
      result = await processor.stage_to_duckdb(
        year=config.year,
        start_year=config.start_year,
        end_year=config.end_year,
        reset_staging=config.reset_staging,
        duckdb_memory_mb=duckdb_memory_mb,
        progress_callback=dagster_progress,
      )
      return result
    finally:
      await end_destructive_op(busy_instance_id, OP_KIND_SEC_STAGING)

  result = asyncio.run(run_staging())

  if result.status == "error":
    context.log.error(f"Staging failed: {result.error}")
    raise Failure(
      description=f"DuckDB staging failed: {result.error}",
      metadata={
        "graph_id": config.graph_id,
        "status": "error",
        "error": result.error or "",
        "duration_ms": result.duration_ms,
      },
    )

  context.log.info(
    f"Staging complete: {len(result.table_names)} tables, "
    f"{result.total_files} files, {result.duration_ms / 1000:.2f}s"
  )

  try:
    from robosystems.graph_api.client.factory import release_graph_memory

    release_result = asyncio.run(release_graph_memory(config.graph_id, target="duckdb"))
    context.log.info(f"Memory release: {release_result.get('message', 'done')}")
  except Exception as release_err:
    context.log.warning(f"Could not release memory (non-fatal): {release_err}")

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "status": result.status,
      "tables_staged": len(result.table_names),
      "table_names": result.table_names,
      "total_files": result.total_files,
      "total_rows": result.total_rows,
      "duckdb_path": result.duckdb_path,
      "duration_ms": result.duration_ms,
    }
  )


@asset(
  group_name="sec_pipeline",
  description="Stage SEC historical parquet files to DuckDB (full rebuild)",
  kinds={"duckdb"},
  metadata={
    "pipeline": "sec",
    "graph_id": "sec_historical",
    "stage": "stage",
    "mode": "full",
  },
)
def sec_historical_duckdb_staged(
  context: AssetExecutionContext,
  config: SECHistoricalStageConfig,
) -> MaterializeResult:
  """Stage the historical year range of the same processed files into the
  sec_historical subgraph's own DuckDB, creating the subgraph if needed."""
  import asyncio

  from robosystems.adapters.sec import XBRLDuckDBGraphProcessor
  from robosystems.operations.graph.shared_repository_service import (
    ensure_shared_subgraph_exists,
  )

  graph_id = config.graph_id
  start_year = config.start_year
  end_year = config.end_year

  context.log.info(
    f"Staging SEC historical data to DuckDB: {graph_id} ({start_year}-{end_year})"
  )
  if config.reset_staging:
    context.log.info("Reset staging enabled - will delete DuckDB file first")

  # Only takes effect on the ladybug-shared tier.
  duckdb_memory_mb: int | None = None
  try:
    from robosystems.graph_api.client.factory import boost_graph_memory

    boost_result = asyncio.run(boost_graph_memory(graph_id, target="duckdb"))
    duckdb_memory_mb = boost_result.get("duckdb_boost_mb")
    context.log.info(f"Memory boost: {boost_result.get('message', 'done')}")
  except Exception as boost_err:
    context.log.warning(f"Could not boost memory (non-fatal): {boost_err}")

  processor = XBRLDuckDBGraphProcessor(graph_id=graph_id)

  def dagster_progress(msg: str) -> None:
    context.log.info(msg)

  async def run_staging():
    subgraph_result = await ensure_shared_subgraph_exists(
      parent_repository_name="sec",
      subgraph_name="historical",
      description="SEC Historical Filings (2009-2023)",
      created_by="system",
      instance_id="local-dev" if env.ENVIRONMENT == "dev" else "ladybug-shared-prod",
    )
    context.log.info(f"Subgraph status: {subgraph_result.get('status')}")

    # Busy counter: see sec_duckdb_staged.
    from robosystems.middleware.graph.instance_busy import (
      OP_KIND_SEC_STAGING,
      begin_destructive_op,
      end_destructive_op,
      resolve_instance_id_for_graph,
    )

    busy_instance_id = await resolve_instance_id_for_graph(graph_id)
    await begin_destructive_op(busy_instance_id, OP_KIND_SEC_STAGING)
    try:
      result = await processor.stage_to_duckdb(
        start_year=start_year,
        end_year=end_year,
        reset_staging=config.reset_staging,
        duckdb_memory_mb=duckdb_memory_mb,
        progress_callback=dagster_progress,
      )
      return result
    finally:
      await end_destructive_op(busy_instance_id, OP_KIND_SEC_STAGING)

  result = asyncio.run(run_staging())

  try:
    from robosystems.graph_api.client.factory import release_graph_memory

    release_result = asyncio.run(release_graph_memory(graph_id, target="duckdb"))
    context.log.info(f"Memory release: {release_result.get('message', 'done')}")
  except Exception as release_err:
    context.log.warning(f"Could not release memory (non-fatal): {release_err}")

  if result.status == "error":
    context.log.error(f"Staging failed: {result.error}")
    raise Failure(
      description=f"Historical DuckDB staging failed: {result.error}",
      metadata={
        "graph_id": graph_id,
        "status": "error",
        "error": result.error or "",
        "duration_ms": result.duration_ms,
      },
    )

  context.log.info(
    f"Historical staging complete: {len(result.table_names)} tables, "
    f"{result.total_files} files, {result.duration_ms / 1000:.2f}s"
  )

  return MaterializeResult(
    metadata={
      "graph_id": graph_id,
      "start_year": start_year,
      "end_year": end_year,
      "status": result.status,
      "tables_staged": len(result.table_names),
      "table_names": result.table_names,
      "total_files": result.total_files,
      "total_rows": result.total_rows,
      "duckdb_path": result.duckdb_path,
      "duration_ms": result.duration_ms,
    }
  )


@asset(
  group_name="sec_pipeline",
  description="Stage current quarter to SEC DuckDB (incremental)",
  kinds={"duckdb"},
  metadata={
    "pipeline": "sec",
    "graph_id": "sec",
    "stage": "stage",
    "mode": "incremental",
  },
)
def sec_duckdb_incremental_staged(
  context: AssetExecutionContext,
  config: SECIncrementalStageConfig,
) -> MaterializeResult:
  """INSERT one quarter's net-new rows into the existing staging tables.

  Idempotent (ROW_NUMBER dedup), so safe to repeat. Requires a prior full
  staging. A partial result fails the run so materialization never proceeds on
  incomplete data.
  """
  import asyncio

  from robosystems.adapters.sec import XBRLDuckDBGraphProcessor

  # Only takes effect on the ladybug-shared tier.
  try:
    from robosystems.graph_api.client.factory import boost_graph_memory

    boost_result = asyncio.run(boost_graph_memory(config.graph_id, target="duckdb"))
    context.log.info(f"Memory boost: {boost_result.get('message', 'done')}")
  except Exception as boost_err:
    context.log.warning(f"Could not boost memory (non-fatal): {boost_err}")

  processor = XBRLDuckDBGraphProcessor(graph_id=config.graph_id)

  async def run_incremental():
    # Busy counter: see sec_duckdb_staged.
    from robosystems.middleware.graph.instance_busy import (
      OP_KIND_SEC_STAGING,
      begin_destructive_op,
      end_destructive_op,
      resolve_instance_id_for_graph,
    )

    busy_instance_id = await resolve_instance_id_for_graph(config.graph_id)
    await begin_destructive_op(busy_instance_id, OP_KIND_SEC_STAGING)
    try:
      return await processor.stage_incremental_to_duckdb(
        year=config.year,
        quarter=config.quarter,
        progress_callback=context.log.info,
      )
    finally:
      await end_destructive_op(busy_instance_id, OP_KIND_SEC_STAGING)

  result = asyncio.run(run_incremental())

  try:
    from robosystems.graph_api.client.factory import release_graph_memory

    release_result = asyncio.run(release_graph_memory(config.graph_id, target="duckdb"))
    context.log.info(f"Memory release: {release_result.get('message', 'done')}")
  except Exception as release_err:
    context.log.warning(f"Could not release memory (non-fatal): {release_err}")

  if result.status == "error":
    context.log.error(f"Incremental staging failed: {result.error}")
    raise Failure(
      description=f"Incremental DuckDB staging failed: {result.error}",
      metadata={
        "graph_id": config.graph_id,
        "status": "error",
        "error": result.error or "Unknown error",
        "duration_ms": result.duration_ms,
      },
    )

  if result.status == "partial":
    context.log.error(
      f"Incremental staging partially failed: "
      f"{len(result.table_names)} tables succeeded but some failed. "
      f"Downstream materialization should not proceed with incomplete data."
    )
    raise Failure(
      description="Partial staging failure - not all tables were updated",
      metadata={
        "graph_id": config.graph_id,
        "status": "partial",
        "tables_staged": len(result.table_names),
        "duration_ms": result.duration_ms,
      },
    )

  context.log.info(
    f"Incremental staging complete: {len(result.table_names)} tables, "
    f"{result.total_rows} rows, {result.duration_ms / 1000:.2f}s"
  )

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "status": result.status,
      "year": config.year,
      "quarter": config.quarter,
      "tables_staged": len(result.table_names),
      "total_rows": result.total_rows,  # net new
      "duration_ms": result.duration_ms,
    }
  )
