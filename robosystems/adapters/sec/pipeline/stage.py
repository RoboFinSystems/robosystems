"""SEC DuckDB Staging Assets.

This module contains the DuckDB staging assets:
- sec_duckdb_staged: Full rebuild staging for sec (primary) graph
- sec_historical_duckdb_staged: Full rebuild staging for sec_historical graph
- sec_duckdb_incremental_staged: Incremental staging for current quarter
"""

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
  """Stage SEC processed files to persistent DuckDB (full rebuild).

  Creates DuckDB tables from scratch using all S3 parquet files.
  Persists to disk so materialization can run independently.

  Options:
  - reset_staging: Delete existing DuckDB file before staging (fresh start)
  - year: Optional single year filter for partial staging
  - start_year/end_year: Optional year range filter

  Run with:
    uv run dagster asset materialize -m robosystems.dagster --select sec_duckdb_staged

  Returns:
      MaterializeResult with staging statistics
  """
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

  # Boost DuckDB memory before staging (only applies to ladybug-shared tier)
  duckdb_memory_mb: int | None = None
  try:
    from robosystems.graph_api.client.factory import boost_graph_memory

    boost_result = asyncio.run(boost_graph_memory(config.graph_id, target="duckdb"))
    duckdb_memory_mb = boost_result.get("duckdb_boost_mb")
    context.log.info(f"Memory boost: {boost_result.get('message', 'done')}")
  except Exception as boost_err:
    context.log.warning(f"Could not boost memory (non-fatal): {boost_err}")

  processor = XBRLDuckDBGraphProcessor(graph_id=config.graph_id)

  # Progress callback for Dagster logging (visible in Dagster UI)
  def dagster_progress(msg: str) -> None:
    context.log.info(msg)

  async def run_staging():
    # Ensure repository exists
    context.log.info("Ensuring SEC repository metadata exists...")
    repo_result = await ensure_shared_repository_exists(
      repository_name=config.graph_id,
      created_by="system",
      instance_id="local-dev" if env.ENVIRONMENT == "dev" else "ladybug-shared-prod",
    )
    context.log.info(f"SEC repository status: {repo_result.get('status', 'unknown')}")

    # Run full staging from all S3 parquet files
    result = await processor.stage_to_duckdb(
      year=config.year,
      start_year=config.start_year,
      end_year=config.end_year,
      reset_staging=config.reset_staging,
      skip_taxonomy_relationships=config.skip_taxonomy_relationships,
      duckdb_memory_mb=duckdb_memory_mb,
      stage_embeddings=config.stage_embeddings,
      progress_callback=dagster_progress,
    )
    return result

  result = asyncio.run(run_staging())

  # Release DuckDB memory after staging (closes connections, frees buffers)
  try:
    from robosystems.graph_api.client.factory import release_graph_memory

    release_result = asyncio.run(release_graph_memory(config.graph_id, target="duckdb"))
    context.log.info(f"Memory release: {release_result.get('message', 'done')}")
  except Exception as release_err:
    context.log.warning(f"Could not release memory (non-fatal): {release_err}")

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
  """Stage SEC historical data to a separate DuckDB database.

  Creates a DuckDB staging database for the sec_historical subgraph.
  Year range is controlled by config (start_year/end_year).

  Uses the same processed S3 parquet files as the primary sec graph,
  but filtered to the historical year range. The sec_historical subgraph
  is automatically created if it doesn't exist.

  Run with:
    uv run dagster asset materialize -m robosystems.dagster --select sec_historical_duckdb_staged

  Returns:
      MaterializeResult with staging statistics
  """
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

  # Boost DuckDB memory before staging (only applies to ladybug-shared tier)
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
    # Ensure the sec_historical subgraph exists (LadybugDB + PostgreSQL)
    subgraph_result = await ensure_shared_subgraph_exists(
      parent_repository_name="sec",
      subgraph_name="historical",
      description="SEC Historical Filings (2009-2023)",
      created_by="system",
      instance_id="local-dev" if env.ENVIRONMENT == "dev" else "ladybug-shared-prod",
    )
    context.log.info(f"Subgraph status: {subgraph_result.get('status')}")

    # Run staging with year range filter
    result = await processor.stage_to_duckdb(
      start_year=start_year,
      end_year=end_year,
      reset_staging=config.reset_staging,
      skip_taxonomy_relationships=config.skip_taxonomy_relationships,
      duckdb_memory_mb=duckdb_memory_mb,
      stage_embeddings=config.stage_embeddings,
      progress_callback=dagster_progress,
    )
    return result

  result = asyncio.run(run_staging())

  # Release DuckDB memory after staging
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
  """INSERT current quarter's files into existing DuckDB tables.

  Points at entire quarter's parquet files and uses INSERT INTO with
  UNION ALL + ROW_NUMBER deduplication. Safe to run daily - only net
  new rows are added, duplicates are automatically filtered out.

  Precondition: Initial full staging must have been done (tables exist).

  Run with:
    uv run dagster asset materialize -m robosystems.dagster --select sec_duckdb_incremental_staged
  """
  import asyncio

  from robosystems.adapters.sec import XBRLDuckDBGraphProcessor

  # Boost DuckDB memory before staging (only applies to ladybug-shared tier)
  try:
    from robosystems.graph_api.client.factory import boost_graph_memory

    boost_result = asyncio.run(boost_graph_memory(config.graph_id, target="duckdb"))
    context.log.info(f"Memory boost: {boost_result.get('message', 'done')}")
  except Exception as boost_err:
    context.log.warning(f"Could not boost memory (non-fatal): {boost_err}")

  processor = XBRLDuckDBGraphProcessor(graph_id=config.graph_id)

  async def run_incremental():
    return await processor.stage_incremental_to_duckdb(
      year=config.year,
      quarter=config.quarter,
      skip_taxonomy_relationships=config.skip_taxonomy_relationships,
      stage_embeddings=config.stage_embeddings,
      progress_callback=context.log.info,
    )

  result = asyncio.run(run_incremental())

  # Release DuckDB memory after staging (closes connections, frees buffers)
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
      "total_rows": result.total_rows,  # Net new rows
      "duration_ms": result.duration_ms,
    }
  )
