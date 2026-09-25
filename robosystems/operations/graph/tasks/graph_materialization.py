"""Worker task for direct (non-Dagster) graph materialization.

Copies the DuckDB staging tables into LadybugDB. The distributed lock is
acquired by the router *before* enqueue and released here when the copy has
finished or failed outright. It is kept, to expire on its TTL, when the copy
may still be running: the Graph API's COPY is synchronous and carries on after
the client goes away (a budget cancel, a timed-out chunk), and releasing then
would admit a second copy into the same database.
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
    from robosystems.operations.graph.engine.direct_materialization import (
      materialize_graph_directly,
    )

    force = self.params.get("force", False)
    rebuild = self.params.get("rebuild", False)
    materialize_embeddings = self.params.get("materialize_embeddings", False)
    lock_key = self.params.get("lock_key")

    import asyncio

    from robosystems.graph_api.client.exceptions import GraphTransientError

    db_gen = get_db_session()
    db = next(db_gen)
    release = True

    try:
      result = await materialize_graph_directly(
        db=db,
        graph_id=self.graph_id,
        force=force,
        rebuild=rebuild,
        materialize_embeddings=materialize_embeddings,
        operation_id=self.task_id,
      )

      return result

    except (asyncio.CancelledError, GraphTransientError):
      release = False
      raise

    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass
      if release:
        self.release_lock(lock_key)
