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

    # Publish the busy counter against the shared-tier master that actually
    # runs the DuckDB writes, so GHA pre-refresh waits before cycling it.
    # Imports are lazy so an adapter-load-time import chain does not pull
    # boto3 / GraphClientFactory into every SEC module at startup.
    from robosystems.middleware.graph.instance_busy import (
      OP_KIND_SEC_STAGING,
      begin_destructive_op,
      end_destructive_op,
      resolve_instance_id_for_graph,
    )

    busy_instance_id = await resolve_instance_id_for_graph(config.graph_id)
    await begin_destructive_op(busy_instance_id, OP_KIND_SEC_STAGING)
    try:
      # Run full staging from all S3 parquet files
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

  # Build vector index from Element embeddings (while DuckDB memory is still boosted).
  # Non-fatal: if the build fails, staging still succeeds — vector search falls
  # back to canonical matching. The index is built on-instance from the DuckDB
  # staging table that was just created above.
  # SEC-specific query: only numeric, non-textblock elements that have facts,
  # deduplicated by qname (highest canonical confidence wins).
  SEC_ELEMENT_VECTOR_QUERY = """
    SELECT
      e.qname,
      e.name,
      e.canonical_concept,
      e.canonical_confidence,
      e.classification,
      e.balance,
      e.embedding::FLOAT[384] AS vector
    FROM Element e
    WHERE e.is_numeric = true
      AND e.is_textblock = false
      AND e.embedding IS NOT NULL
      AND e.identifier IN (
        SELECT DISTINCT dst FROM FACT_HAS_ELEMENT
      )
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY e.qname
      ORDER BY e.canonical_confidence DESC NULLS LAST, e.identifier
    ) = 1
  """

  lance_metadata: dict = {}
  if "Element" in result.table_names:
    try:
      from robosystems.graph_api.client.factory import (
        get_graph_client_for_sec_ingestion,
      )

      async def run_vector_build():
        client = await get_graph_client_for_sec_ingestion()
        try:
          return await client.vector_build(
            graph_id=config.graph_id,
            table_name="Element",
            query=SEC_ELEMENT_VECTOR_QUERY,
          )
        finally:
          await client.close()

      context.log.info("Building vector index for Element table...")
      lance_result = asyncio.run(run_vector_build())
      lance_metadata = {
        "lance_row_count": lance_result.get("row_count", 0),
        "lance_size_mb": lance_result.get("index_size_mb", 0),
        "lance_duration_ms": lance_result.get("duration_ms", 0),
      }
      context.log.info(
        f"Vector index built: {lance_result.get('row_count', 0):,} rows, "
        f"{lance_result.get('index_size_mb', 0):.1f} MB, "
        f"{lance_result.get('duration_ms', 0) / 1000:.1f}s"
      )
    except Exception as lance_err:
      context.log.warning(f"Vector index build failed (non-fatal): {lance_err}")
  else:
    context.log.info("Skipping vector index build (Element table not staged)")

  # Release DuckDB memory after staging + vector build (closes connections, frees buffers)
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
      **lance_metadata,
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

    # See sec_duckdb_staged above for why we publish a busy counter here.
    from robosystems.middleware.graph.instance_busy import (
      OP_KIND_SEC_STAGING,
      begin_destructive_op,
      end_destructive_op,
      resolve_instance_id_for_graph,
    )

    busy_instance_id = await resolve_instance_id_for_graph(graph_id)
    await begin_destructive_op(busy_instance_id, OP_KIND_SEC_STAGING)
    try:
      # Run staging with year range filter
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
    # See sec_duckdb_staged above for why we publish a busy counter here.
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
