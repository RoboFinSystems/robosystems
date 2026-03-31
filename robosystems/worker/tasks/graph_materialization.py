"""Worker task handler for direct graph materialization.

Materializes DuckDB staging tables to the graph database (LadybugDB).
Replaces the BackgroundTasks path in the materialize router for the
direct (non-Dagster) materialization flow.

The distributed lock is acquired by the router before enqueue and
released by this task on completion or failure.
"""

from __future__ import annotations

from typing import Any

from robosystems.logger import get_logger
from robosystems.worker.tasks import register_task
from robosystems.worker.tasks.base import BaseTask

logger = get_logger(__name__)


@register_task("graph_materialization")
class GraphMaterializationTask(BaseTask):
  """Materialize staged data from DuckDB to the graph database."""

  async def execute(self) -> dict[str, Any]:
    from robosystems.database import get_db_session
    from robosystems.operations.lbug.direct_materialization import (
      materialize_graph_directly,
    )

    force = self.params.get("force", False)
    rebuild = self.params.get("rebuild", False)
    ignore_errors = self.params.get("ignore_errors", True)
    lock_key = self.params.get("lock_key")

    db_gen = get_db_session()
    db = next(db_gen)

    try:
      result = await materialize_graph_directly(
        db=db,
        graph_id=self.graph_id,
        force=force,
        rebuild=rebuild,
        ignore_errors=ignore_errors,
        operation_id=self.task_id,
      )
      return result

    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass
      self._release_lock(lock_key)

  def _release_lock(self, lock_key: str | None) -> None:
    """Release the distributed materialization lock if held."""
    if not lock_key:
      return

    try:
      from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client

      redis_client = create_redis_client(ValkeyDatabase.LOCKS)
      # Delete the lock key directly — the router acquired it, we just need
      # to ensure it's released. The lock_id ownership check isn't possible
      # across processes, but the TTL protects against permanent deadlocks.
      redis_client.delete(f"lock:{lock_key}")
      redis_client.close()
      logger.debug(f"Released materialization lock: {lock_key}")
    except Exception as e:
      logger.warning(f"Failed to release materialization lock {lock_key}: {e}")
