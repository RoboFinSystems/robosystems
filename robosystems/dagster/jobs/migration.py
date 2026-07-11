"""Dagster jobs for LadybugDB version migration.

Two jobs orchestrate migration across the fleet:
1. Export job (pre-deploy): exports all databases on each instance
2. Import job (post-deploy): imports exported databases on each instance

Both jobs discover instances from DynamoDB, skipping shared replicas
(replicas re-attach from S3 and don't need export/import).

Instances are processed in parallel — each instance exports/imports
its own local databases independently.
"""

import asyncio
import time
from typing import Any

from dagster import Config, OpExecutionContext, job, op

from robosystems.config import env

# Default timeout for polling long-running migration tasks (2 hours)
DEFAULT_TASK_TIMEOUT = 7200


class MigrationExportConfig(Config):
  """Configuration for migration export job."""

  source_version: str
  target_version: str
  skip_instances: list[str] = []
  include_shared_master: bool = False


class MigrationImportConfig(Config):
  """Configuration for migration import job."""

  skip_instances: list[str] = []
  include_shared_master: bool = False
  cleanup: bool = False


def _get_writer_instances(
  context: OpExecutionContext, include_shared_master: bool = False
) -> list[dict]:
  """Discover writer instances for migration.

  Always excludes shared replicas (they re-attach from S3).
  Excludes shared masters by default — they're faster to rebuild
  from DuckDB staging than to export/import.

  In dev, returns a single localhost instance.
  In prod/staging, queries DynamoDB for healthy instances.
  """
  if env.is_development():
    # This op runs inside the Dagster worker container and the migration client
    # builds http://{private_ip}:8001 — so "localhost" would resolve to the
    # worker, not graph_api. Use the graph_api host from GRAPH_API_URL
    # (e.g. "graph-api" under Docker Compose) so the client reaches the right
    # container. Prod/staging never take this branch (DynamoDB below).
    from urllib.parse import urlparse

    graph_api_host = urlparse(env.GRAPH_API_URL).hostname or "localhost"
    context.log.info(f"Dev mode: using graph_api host '{graph_api_host}'")
    return [
      {
        "instance_id": "local",
        "private_ip": graph_api_host,
        "node_type": "dev",
      }
    ]

  from robosystems.middleware.graph.allocation_manager import AllocationManager

  manager = AllocationManager()

  # Paginated scan of ALL instances (not just healthy)
  # Migration needs to reach every instance that has data, even temporarily unhealthy ones
  all_items = []
  scan_params = {}
  while True:
    response = manager.instance_table.scan(**scan_params)
    all_items.extend(response.get("Items", []))
    last_key = response.get("LastEvaluatedKey")
    if not last_key:
      break
    scan_params["ExclusiveStartKey"] = last_key

  # Filter by node type
  excluded_types = {"shared_replica"}
  if not include_shared_master:
    excluded_types.add("shared_master")

  writers = [inst for inst in all_items if inst.get("node_type") not in excluded_types]

  # Log what we found
  healthy = sum(1 for w in writers if w.get("status") == "healthy")
  unhealthy = len(writers) - healthy
  filtered = len(all_items) - len(writers)
  context.log.info(
    f"Discovered {len(writers)} instances for migration "
    f"({healthy} healthy, {unhealthy} unhealthy, "
    f"filtered {filtered} replicas/shared, "
    f"include_shared_master={include_shared_master})"
  )

  if unhealthy > 0:
    context.log.warning(
      f"{unhealthy} instance(s) are not healthy — migration will still be attempted"
    )

  return writers


async def _get_client(ip: str):
  """Get a graph client for a specific instance."""
  from robosystems.graph_api.client.factory import get_graph_client_for_instance

  return await get_graph_client_for_instance(ip)


async def _poll_task(
  client, task_id: str, poll_interval: int = 5, max_wait: int = DEFAULT_TASK_TIMEOUT
) -> dict:
  """Poll a task until completion or failure."""
  start = time.time()

  while True:
    task = await client.get_task_status(task_id)
    status = task.get("status")

    if status == "completed":
      return task
    elif status == "failed":
      raise RuntimeError(f"Task {task_id} failed: {task.get('error', 'unknown error')}")

    if time.time() - start > max_wait:
      raise RuntimeError(
        f"Task {task_id} timed out after {max_wait}s (status: {status})"
      )

    await asyncio.sleep(poll_interval)


async def _export_instance(
  instance_id: str,
  ip: str,
  source_version: str,
  target_version: str,
) -> dict[str, Any]:
  """Export a single instance. Returns result dict."""
  start = time.time()
  client = await _get_client(ip)

  export_response = await client.migration_export(source_version, target_version)
  task_id = export_response["task_id"]

  task_result = await _poll_task(client, task_id)
  duration = time.time() - start
  result_data = task_result.get("result", {})

  return {
    "instance_id": instance_id,
    "status": "success",
    "duration_seconds": round(duration, 1),
    **result_data,
  }


async def _import_instance(
  instance_id: str, ip: str, cleanup: bool = False
) -> dict[str, Any]:
  """Import a single instance. Returns result dict."""
  client = await _get_client(ip)

  # Check if migration is pending
  status = await client.migration_status()
  if not status.get("migration_pending"):
    return {"instance_id": instance_id, "status": "no_migration_pending"}

  start = time.time()
  import_response = await client.migration_import()
  task_id = import_response["task_id"]

  task_result = await _poll_task(client, task_id)
  duration = time.time() - start
  result_data = task_result.get("result", {})

  if cleanup:
    cleanup_result = await client.migration_cleanup()
    result_data["cleanup"] = cleanup_result

  return {
    "instance_id": instance_id,
    "status": "success",
    "duration_seconds": round(duration, 1),
    **result_data,
  }


async def _run_parallel(coros: list, instance_ids: list[str]) -> list[dict[str, Any]]:
  """Run coroutines in parallel, collecting results and errors."""
  raw_results = await asyncio.gather(*coros, return_exceptions=True)

  results = []
  for instance_id, result in zip(instance_ids, raw_results, strict=True):
    if isinstance(result, Exception):
      results.append(
        {"instance_id": instance_id, "status": "failed", "error": str(result)}
      )
    else:
      results.append(result)

  return results


@op
def export_all_instances(
  context: OpExecutionContext, config: MigrationExportConfig
) -> dict:
  """Export all databases on each writer instance for version migration.

  All instances are exported in parallel. Each instance:
  1. Receives POST /migration/export with source/target versions
  2. Exports databases to local Parquet + uploads system backup to S3
  3. Writes migration.json manifest
  """
  instances = _get_writer_instances(context, config.include_shared_master)

  # Separate skipped from active
  active = []
  results = []
  for inst in instances:
    instance_id = inst.get("instance_id", "unknown")
    if instance_id in config.skip_instances:
      context.log.info(f"Skipping instance {instance_id}")
      results.append({"instance_id": instance_id, "status": "skipped"})
    else:
      active.append(inst)

  if active:
    context.log.info(
      f"Exporting {len(active)} instances in parallel "
      f"({config.source_version} -> {config.target_version})"
    )

    coros = [
      _export_instance(
        inst.get("instance_id", "unknown"),
        inst.get("private_ip", "localhost"),
        config.source_version,
        config.target_version,
      )
      for inst in active
    ]
    instance_ids = [inst.get("instance_id", "unknown") for inst in active]

    parallel_results = asyncio.run(_run_parallel(coros, instance_ids))
    results.extend(parallel_results)

  # Log results
  for r in results:
    if r["status"] == "success":
      context.log.info(
        f"  {r['instance_id']}: exported "
        f"{r.get('databases_exported', '?')} databases in {r.get('duration_seconds', '?')}s"
      )
    elif r["status"] == "failed":
      context.log.error(f"  {r['instance_id']}: {r.get('error', 'unknown error')}")

  succeeded = sum(1 for r in results if r["status"] == "success")
  failed = sum(1 for r in results if r["status"] == "failed")
  skipped = sum(1 for r in results if r["status"] == "skipped")

  context.log.info(
    f"Export complete: {succeeded} succeeded, {failed} failed, {skipped} skipped"
  )

  if failed > 0:
    errors = [r["error"] for r in results if r["status"] == "failed"]
    raise RuntimeError(f"Export failed on {failed} instance(s): {'; '.join(errors)}")

  return {
    "source_version": config.source_version,
    "target_version": config.target_version,
    "instances": results,
    "summary": {"succeeded": succeeded, "failed": failed, "skipped": skipped},
  }


@op
def import_all_instances(
  context: OpExecutionContext, config: MigrationImportConfig
) -> dict:
  """Import exported databases on each writer instance after version upgrade.

  All instances are imported in parallel. Each instance:
  1. Checks migration status (skip if no migration pending)
  2. Imports from local Parquet exports into fresh databases
  3. Verifies node counts match the manifest
  4. Optionally cleans up .pre-migration files
  """
  instances = _get_writer_instances(context, config.include_shared_master)

  # Separate skipped from active
  active = []
  results = []
  for inst in instances:
    instance_id = inst.get("instance_id", "unknown")
    if instance_id in config.skip_instances:
      context.log.info(f"Skipping instance {instance_id}")
      results.append({"instance_id": instance_id, "status": "skipped"})
    else:
      active.append(inst)

  if active:
    context.log.info(f"Importing {len(active)} instances in parallel")

    coros = [
      _import_instance(
        inst.get("instance_id", "unknown"),
        inst.get("private_ip", "localhost"),
        cleanup=config.cleanup,
      )
      for inst in active
    ]
    instance_ids = [inst.get("instance_id", "unknown") for inst in active]

    parallel_results = asyncio.run(_run_parallel(coros, instance_ids))
    results.extend(parallel_results)

  # Log results
  for r in results:
    if r["status"] == "success":
      context.log.info(
        f"  {r['instance_id']}: imported "
        f"{r.get('databases_imported', '?')} databases in {r.get('duration_seconds', '?')}s"
      )
    elif r["status"] == "failed":
      context.log.error(f"  {r['instance_id']}: {r.get('error', 'unknown error')}")
    elif r["status"] == "no_migration_pending":
      context.log.info(f"  {r['instance_id']}: no migration pending")

  succeeded = sum(1 for r in results if r["status"] == "success")
  failed = sum(1 for r in results if r["status"] == "failed")
  skipped = sum(
    1 for r in results if r["status"] in ("skipped", "no_migration_pending")
  )

  context.log.info(
    f"Import complete: {succeeded} succeeded, {failed} failed, {skipped} skipped"
  )

  if failed > 0:
    errors = [r["error"] for r in results if r["status"] == "failed"]
    raise RuntimeError(f"Import failed on {failed} instance(s): {'; '.join(errors)}")

  return {
    "instances": results,
    "summary": {"succeeded": succeeded, "failed": failed, "skipped": skipped},
  }


@job(tags={"dagster/priority": "1"})
def ladybug_migration_export_job():
  """Export all databases on writer instances for version migration.

  Run BEFORE deploying the new LadybugDB version.
  """
  export_all_instances()


@job(tags={"dagster/priority": "1"})
def ladybug_migration_import_job():
  """Import exported databases on writer instances after version upgrade.

  Run AFTER deploying the new LadybugDB version.
  """
  import_all_instances()
