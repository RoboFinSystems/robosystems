"""SEC Materialization Assets.

This module contains the graph materialization assets:
- sec_graph_incremental_copy: Incremental S3 → LadybugDB copy
- sec_graph_materialized: Full DuckDB → LadybugDB materialization
- sec_graph_direct_copy: Direct S3 → LadybugDB copy (bypasses DuckDB)
- sec_historical_direct_copy: Direct S3 → LadybugDB copy for sec_historical (2009-2023)
"""

import time

from dagster import AssetExecutionContext, MaterializeResult, asset

from robosystems.config import env

from .configs import (
  SECDirectCopyConfig,
  SECIncrementalCopyConfig,
  SECMaterializeConfig,
)


@asset(
  group_name="sec_pipeline",
  description="Incremental S3 → LadybugDB copy (bypasses DuckDB, uses ignore_errors)",
  kinds={"ladybug"},
  metadata={
    "pipeline": "sec",
    "stage": "incremental_copy",
    "decoupled": True,
  },
)
def sec_graph_incremental_copy(
  context: AssetExecutionContext,
  config: SECIncrementalCopyConfig,
) -> MaterializeResult:
  """Copy current quarter's files directly to LadybugDB (bypasses DuckDB staging).

  This is the preferred approach for daily incremental updates:
  1. Copies directly from S3 parquet to LadybugDB
  2. Uses ignore_errors=true to skip duplicates (constraint violations)
  3. Only scans current quarter + previous quarter during 5-day overlap

  Benefits over DuckDB incremental staging:
  - No need to diff what's new in DuckDB vs LadybugDB
  - Simpler and faster for daily updates
  - LadybugDB handles deduplication via constraints

  Precondition: LadybugDB database must already exist with SEC schema.

  Run with:
    uv run dagster asset materialize -m robosystems.dagster --select sec_graph_incremental_copy
  """
  import asyncio

  from robosystems.adapters.sec import XBRLDuckDBGraphProcessor

  processor = XBRLDuckDBGraphProcessor(graph_id=config.graph_id)

  async def run_incremental_copy():
    return await processor.copy_incremental_to_ladybug(
      year=config.year,
      quarter=config.quarter,
      skip_taxonomy_relationships=config.skip_taxonomy_relationships,
      copy_timeout=config.copy_timeout,
      progress_callback=context.log.info,
    )

  result = asyncio.run(run_incremental_copy())

  if result.status == "error":
    context.log.error(f"Incremental copy failed: {result.error}")
    return MaterializeResult(
      metadata={
        "graph_id": config.graph_id,
        "status": "error",
        "error": result.error or "Unknown error",
        "duration_ms": result.duration_ms,
      }
    )

  context.log.info(
    f"Incremental copy complete: {len(result.table_names)} tables, "
    f"{result.total_rows} records, {result.duration_ms / 1000:.2f}s"
  )

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "status": result.status,
      "year": config.year,
      "quarter": config.quarter,
      "tables_copied": len(result.table_names),
      "total_records": result.total_rows,
      "duration_ms": result.duration_ms,
    }
  )


@asset(
  group_name="sec_pipeline",
  description="Materialize LadybugDB graph from staged DuckDB (Stage 2)",
  kinds={"ladybug"},
  deps=["sec_duckdb_staged"],  # Explicit dependency on staging
  metadata={
    "pipeline": "sec",
    "stage": "materialization",
    "decoupled": True,
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
    return MaterializeResult(
      metadata={
        "graph_id": config.graph_id,
        "status": "error",
        "error": result.error,
      }
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
  description="Direct S3 → LadybugDB copy (bypasses DuckDB staging)",
  kinds={"ladybug"},
  metadata={
    "pipeline": "sec",
    "stage": "direct_copy",
    "decoupled": True,
  },
)
def sec_graph_direct_copy(
  context: AssetExecutionContext,
  config: SECDirectCopyConfig,
) -> MaterializeResult:
  """Copy SEC data directly from S3 to LadybugDB (bypasses DuckDB staging).

  This asset provides an alternative materialization path that:
  1. Reads parquet files directly from S3 using LadybugDB's httpfs extension
  2. Uses spill_to_disk=true for memory-efficient loading of large tables
  3. Handles duplicates via ignore_errors=true (constraint violations skipped)
  4. Uses quarter-by-quarter batching for large tables (100M+ rows)

  When to use this vs sec_graph_materialized:
  - Use this for faster loading (single pass, no DuckDB intermediate)
  - Use sec_graph_materialized when you need DuckDB's query capabilities

  Run with:
    uv run dagster asset materialize -m robosystems.dagster --select sec_graph_direct_copy

  Returns:
      MaterializeResult with copy statistics
  """
  import asyncio

  from robosystems.adapters.sec.processors.ingestion import (
    LadybugDirectCopier,
    LadybugMaterializer,
  )
  from robosystems.graph_api.client.factory import (
    boost_graph_memory,
    get_graph_client,
    release_graph_memory,
  )
  from robosystems.operations.graph.shared_repository_service import (
    ensure_shared_repository_exists,
  )

  context.log.info(f"Direct S3 → LadybugDB copy for graph: {config.graph_id}")
  if config.year:
    context.log.info(f"Year filter: {config.year}")
  if config.rebuild_graph:
    context.log.info("Rebuild enabled - will delete and recreate database")

  start_time = time.time()

  # Boost LadybugDB memory before copy
  try:
    boost_result = asyncio.run(boost_graph_memory(config.graph_id, target="ladybug"))
    context.log.info(f"Memory boost: {boost_result.get('message', 'done')}")
  except Exception as boost_err:
    context.log.warning(f"Could not boost memory (non-fatal): {boost_err}")

  async def run_direct_copy():
    # Create copier for direct S3 → LadybugDB copy operations
    copier = LadybugDirectCopier(graph_id=config.graph_id)

    # Ensure repository exists
    context.log.info("Ensuring SEC repository metadata exists...")
    repo_result = await ensure_shared_repository_exists(
      repository_name=config.graph_id,
      created_by="system",
      instance_id="local-dev" if env.ENVIRONMENT == "dev" else "ladybug-shared-prod",
    )
    context.log.info(f"SEC repository status: {repo_result.get('status', 'unknown')}")

    # Get graph client
    client = await get_graph_client(graph_id=config.graph_id, operation_type="write")

    # Rebuild LadybugDB if requested (use materializer for schema management)
    if config.rebuild_graph:
      context.log.info("Rebuilding LadybugDB database with SEC schema...")
      materializer = LadybugMaterializer(graph_id=config.graph_id)
      await materializer._rebuild_ladybug_database(client, reset_staging=False)
      context.log.info("LadybugDB database rebuilt with SEC schema")

    # Query database for existing tables to filter copy list
    context.log.info("Querying database for existing tables...")
    existing_table_names = None
    try:
      existing_tables_result = await client.query(
        cypher="CALL show_tables() RETURN *",
        graph_id=config.graph_id,
      )
      existing_table_names = {
        row.get("name") for row in existing_tables_result.get("data", [])
      }
      context.log.info(f"Database has {len(existing_table_names)} tables")
    except Exception as e:
      context.log.warning(f"Could not query existing tables: {e}")

    # Execute direct copy using the adapter
    copy_result = await copier.copy_all_tables(
      client=client,
      year=config.year,
      start_year=config.start_year,
      end_year=config.end_year,
      skip_taxonomy_relationships=config.skip_taxonomy_relationships,
      skip_tables=config.skip_tables,
      existing_table_names=existing_table_names,
      quarter_copy_timeout=config.quarter_copy_timeout,
      progress_callback=lambda msg: context.log.info(msg),
    )

    return copy_result

  result = asyncio.run(run_direct_copy())

  duration_ms = (time.time() - start_time) * 1000

  # Release memory after direct copy (closes connections, frees buffers to OS)
  try:
    release_result = asyncio.run(
      release_graph_memory(config.graph_id, target="ladybug")
    )
    context.log.info(f"Memory release: {release_result.get('message', 'done')}")
  except Exception as release_err:
    context.log.warning(f"Could not release memory (non-fatal): {release_err}")

  context.log.info(
    f"Direct copy complete: {len(result.tables_copied)} tables, "
    f"{result.total_rows:,} rows, {duration_ms / 1000:.1f}s"
  )

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "rebuild_graph": config.rebuild_graph,
      "status": result.status,
      "total_rows": result.total_rows,
      "tables_copied": result.tables_copied,
      "failed_tables": result.failed_tables,
      "duration_ms": duration_ms,
    }
  )


@asset(
  group_name="sec_pipeline",
  description="Direct S3 → LadybugDB copy for sec_historical (2009-2023, load-once)",
  kinds={"ladybug"},
  metadata={
    "pipeline": "sec",
    "stage": "historical_direct_copy",
    "decoupled": True,
  },
)
def sec_historical_direct_copy(
  context: AssetExecutionContext,
  config: SECDirectCopyConfig,
) -> MaterializeResult:
  """Copy historical SEC data (2009-2023) directly from S3 to sec_historical subgraph.

  This asset loads the historical portion of SEC data into the sec_historical
  subgraph. It uses the same direct copy pipeline as sec_graph_direct_copy but
  defaults to the sec_historical graph and the 2009-2023 year range.

  Designed for load-once operation. After initial load, this graph is static
  and only needs to be rebuilt if the schema changes or data is reprocessed.

  Run with:
    uv run dagster asset materialize -m robosystems.dagster --select sec_historical_direct_copy

  Returns:
      MaterializeResult with copy statistics
  """
  import asyncio

  from robosystems.adapters.sec.processors.ingestion import (
    LadybugDirectCopier,
    LadybugMaterializer,
  )
  from robosystems.graph_api.client.factory import (
    boost_graph_memory,
    get_graph_client,
    release_graph_memory,
  )

  from .configs import SEC_HISTORICAL_END_YEAR, SEC_START_YEAR

  # Apply defaults for historical graph
  graph_id = config.graph_id if config.graph_id != "sec" else "sec_historical"
  start_year = config.start_year if config.start_year is not None else SEC_START_YEAR
  end_year = config.end_year if config.end_year is not None else SEC_HISTORICAL_END_YEAR

  context.log.info(f"Historical direct copy: {graph_id} ({start_year}-{end_year})")
  if config.rebuild_graph:
    context.log.info("Rebuild enabled - will delete and recreate database")

  start_time = time.time()

  # Boost LadybugDB memory before copy
  try:
    boost_result = asyncio.run(boost_graph_memory(graph_id, target="ladybug"))
    context.log.info(f"Memory boost: {boost_result.get('message', 'done')}")
  except Exception as boost_err:
    context.log.warning(f"Could not boost memory (non-fatal): {boost_err}")

  async def run_historical_copy():
    # Ensure the sec_historical subgraph exists (LadybugDB + PostgreSQL)
    from robosystems.operations.graph.shared_repository_service import (
      ensure_shared_subgraph_exists,
    )

    subgraph_result = await ensure_shared_subgraph_exists(
      parent_repository_name="sec",
      subgraph_name="historical",
      description="SEC Historical Filings (2009-2023)",
      created_by="system",
      instance_id="local-dev" if env.ENVIRONMENT == "dev" else "ladybug-shared-prod",
    )
    context.log.info(f"Subgraph status: {subgraph_result.get('status')}")

    copier = LadybugDirectCopier(graph_id=graph_id)
    client = await get_graph_client(graph_id=graph_id, operation_type="write")

    if config.rebuild_graph:
      context.log.info("Rebuilding LadybugDB database with SEC schema...")
      materializer = LadybugMaterializer(graph_id=graph_id)
      await materializer._rebuild_ladybug_database(client, reset_staging=False)
      context.log.info("LadybugDB database rebuilt with SEC schema")

    # Query database for existing tables
    existing_table_names = None
    try:
      existing_tables_result = await client.query(
        cypher="CALL show_tables() RETURN *",
        graph_id=graph_id,
      )
      existing_table_names = {
        row.get("name") for row in existing_tables_result.get("data", [])
      }
      context.log.info(f"Database has {len(existing_table_names)} tables")
    except Exception as e:
      context.log.warning(f"Could not query existing tables: {e}")

    copy_result = await copier.copy_all_tables(
      client=client,
      start_year=start_year,
      end_year=end_year,
      skip_taxonomy_relationships=config.skip_taxonomy_relationships,
      skip_tables=config.skip_tables,
      existing_table_names=existing_table_names,
      quarter_copy_timeout=config.quarter_copy_timeout,
      progress_callback=lambda msg: context.log.info(msg),
    )

    return copy_result

  result = asyncio.run(run_historical_copy())

  duration_ms = (time.time() - start_time) * 1000

  # Release memory after copy
  try:
    release_result = asyncio.run(release_graph_memory(graph_id, target="ladybug"))
    context.log.info(f"Memory release: {release_result.get('message', 'done')}")
  except Exception as release_err:
    context.log.warning(f"Could not release memory (non-fatal): {release_err}")

  context.log.info(
    f"Historical copy complete: {len(result.tables_copied)} tables, "
    f"{result.total_rows:,} rows, {duration_ms / 1000:.1f}s"
  )

  return MaterializeResult(
    metadata={
      "graph_id": graph_id,
      "rebuild_graph": config.rebuild_graph,
      "start_year": start_year,
      "end_year": end_year,
      "status": result.status,
      "total_rows": result.total_rows,
      "tables_copied": result.tables_copied,
      "failed_tables": result.failed_tables,
      "duration_ms": duration_ms,
    }
  )
