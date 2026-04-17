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
    from robosystems.operations.graph.engine.direct_materialization import (
      materialize_graph_directly,
    )

    force = self.params.get("force", False)
    rebuild = self.params.get("rebuild", False)
    ignore_errors = self.params.get("ignore_errors", True)
    materialize_embeddings = self.params.get("materialize_embeddings", False)
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
        materialize_embeddings=materialize_embeddings,
        operation_id=self.task_id,
      )

      return result

    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass
      self.release_lock(lock_key)
