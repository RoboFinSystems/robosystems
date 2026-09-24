"""Dagster jobs for LadybugDB version migration across the writer fleet.

Export (pre-deploy), import (post-deploy), cleanup (post-verify). Instances run
in parallel, each on its own local databases; shared replicas re-attach from S3
and are skipped.
"""

import asyncio
import time
from typing import Any

from dagster import Config, OpExecutionContext, job, op

from robosystems.config import env

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


class MigrationCleanupConfig(Config):
  """Configuration for migration cleanup job."""

  skip_instances: list[str] = []
  include_shared_master: bool = False


def _get_writer_instances(
  context: OpExecutionContext, include_shared_master: bool = False
) -> list[dict]:
  """Discover writer instances for migration.

  Shared masters are excluded by default: rebuilding from DuckDB staging is
  faster than export/import.
  """
  if env.is_development():
    # The client builds http://{private_ip}:8001 from inside the worker
    # container, where "localhost" is the worker; use GRAPH_API_URL's host.
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

  from robosystems.middleware.graph.allocation_manager import LadybugAllocationManager

  manager = LadybugAllocationManager(environment=env.ENVIRONMENT)

  # All instances, not just healthy: every instance holding data must migrate.
  all_items = []
  scan_params = {}
  while True:
    response = manager.instance_table.scan(**scan_params)
    all_items.extend(response.get("Items", []))
    last_key = response.get("LastEvaluatedKey")
    if not last_key:
      break
    scan_params["ExclusiveStartKey"] = last_key

  excluded_types = {"shared_replica"}
  if not include_shared_master:
    excluded_types.add("shared_master")

  writers = [inst for inst in all_items if inst.get("node_type") not in excluded_types]

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

  export_response = await client.migration_export(
    source_version, target_version, env.USER_DATA_BUCKET
  )
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

  status = await client.migration_status()
  if not status.get("migration_pending"):
    # Still honor cleanup so a rerun removes leftover .pre-migration backups;
    # the endpoint no-ops when there is nothing to clean.
    result = {"instance_id": instance_id, "status": "no_migration_pending"}
    if cleanup:
      result["cleanup"] = await client.migration_cleanup()
    return result

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


async def _cleanup_instance(instance_id: str, ip: str) -> dict[str, Any]:
  """Delete .pre-migration backups on one instance.

  Idempotent: the endpoint refuses while a migration is in progress or
  migration.json exists, and no-ops when there is nothing to remove.
  """
  start = time.time()
  client = await _get_client(ip)

  cleanup_result = await client.migration_cleanup()
  duration = time.time() - start

  return {
    "instance_id": instance_id,
    "status": "success",
    "duration_seconds": round(duration, 1),
    "cleanup": cleanup_result,
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
  """Export every writer's databases to local Parquet (plus an S3 system backup)
  and a migration.json manifest, in parallel."""
  instances = _get_writer_instances(context, config.include_shared_master)

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
  """Import each writer's Parquet exports into fresh databases, in parallel.

  Instances verify node counts against the manifest; those with no pending
  migration are skipped.
  """
  instances = _get_writer_instances(context, config.include_shared_master)

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


@op
def cleanup_all_instances(
  context: OpExecutionContext, config: MigrationCleanupConfig
) -> dict:
  """Delete .pre-migration rollback backups fleet-wide, after a verified migration.

  A shared master (include_shared_master=true) must be awake for its volume to
  be attached.
  """
  instances = _get_writer_instances(context, config.include_shared_master)

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
    context.log.info(f"Cleaning up {len(active)} instances in parallel")

    coros = [
      _cleanup_instance(
        inst.get("instance_id", "unknown"),
        inst.get("private_ip", "localhost"),
      )
      for inst in active
    ]
    instance_ids = [inst.get("instance_id", "unknown") for inst in active]

    parallel_results = asyncio.run(_run_parallel(coros, instance_ids))
    results.extend(parallel_results)

  for r in results:
    if r["status"] == "success":
      context.log.info(
        f"  {r['instance_id']}: cleanup {r.get('cleanup', {})} "
        f"in {r.get('duration_seconds', '?')}s"
      )
    elif r["status"] == "failed":
      context.log.error(f"  {r['instance_id']}: {r.get('error', 'unknown error')}")

  succeeded = sum(1 for r in results if r["status"] == "success")
  failed = sum(1 for r in results if r["status"] == "failed")
  skipped = sum(1 for r in results if r["status"] == "skipped")

  context.log.info(
    f"Cleanup complete: {succeeded} succeeded, {failed} failed, {skipped} skipped"
  )

  if failed > 0:
    errors = [r["error"] for r in results if r["status"] == "failed"]
    raise RuntimeError(f"Cleanup failed on {failed} instance(s): {'; '.join(errors)}")

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


@job(tags={"dagster/priority": "1"})
def ladybug_migration_cleanup_job():
  """Delete .pre-migration rollback backups on writer instances.

  Run after a migration is verified good. Idempotent. The import job's cleanup
  only reaches instances that still had a pending migration.
  """
  cleanup_all_instances()
