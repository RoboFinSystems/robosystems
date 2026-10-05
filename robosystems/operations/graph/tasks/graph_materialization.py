"""Worker task for direct (non-Dagster) graph materialization.

Copies the DuckDB staging tables into LadybugDB. The materialization lock is
acquired by the router *before* enqueue, kept fresh while the copy runs, and
released here when the copy has finished or failed outright. It is kept, to
expire on its TTL, when the copy may still be running: the Graph API's COPY is
synchronous and carries on after the client goes away (a budget cancel, a
timed-out chunk), and releasing then would admit a second copy into the same
database.
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

  LOCK_EXTEND_INTERVAL_SECONDS = 60

  async def execute(self) -> dict[str, Any]:
    from robosystems.database import get_db_session
    from robosystems.operations.graph.engine.direct_materialization import (
      materialize_graph_directly,
    )

    force = self.params.get("force", False)
    rebuild = self.params.get("rebuild", False)
    materialize_embeddings = self.params.get("materialize_embeddings", False)
    lock_token = self.params.get("materialization_lock_token")

    import asyncio

    from robosystems.graph_api.client.exceptions import GraphTransientError

    db_gen = get_db_session()
    db = next(db_gen)
    release = True
    keep_fresh = (
      asyncio.create_task(self._keep_lock_fresh(lock_token)) if lock_token else None
    )

    try:
      result = await materialize_graph_directly(
        db=db,
        graph_id=self.graph_id,
        force=force,
        rebuild=rebuild,
        materialize_embeddings=materialize_embeddings,
        operation_id=self.task_id,
        lock_token=lock_token,
      )
      if isinstance(result, dict) and result.get("copy_may_still_run"):
        release = False
      return result

    except (asyncio.CancelledError, GraphTransientError):
      release = False
      raise

    finally:
      if keep_fresh is not None:
        keep_fresh.cancel()
      try:
        next(db_gen)
      except StopIteration:
        pass
      if release:
        self.release_lock()

  async def _keep_lock_fresh(self, token: str) -> None:
    """Push the lock's TTL out while the copy runs, so a run (or a requeued
    attempt) that outlasts one TTL window is not left writing unlocked."""
    import asyncio

    from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
    from robosystems.graph_api.core.ladybug.materialization_lock import extend_token

    while True:
      await asyncio.sleep(self.LOCK_EXTEND_INTERVAL_SECONDS)
      client = create_redis_client(ValkeyDatabase.LOCKS)
      try:
        held = await asyncio.to_thread(extend_token, client, self.graph_id, token)
      finally:
        client.close()
      if not held:
        logger.warning(
          f"Materialization lock for {self.graph_id} is no longer held by this task"
        )
