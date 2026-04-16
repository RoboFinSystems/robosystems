"""Worker task handler for extensions OLTP → LadybugDB materialization.

Runs ExtensionsMaterializer directly without Dagster as an intermediary.
The Dagster sensor handles automatic (staleness-driven) triggers; this
task handles user-initiated immediate materialization with SSE progress.

On completion, marks the graph fresh so the Dagster sensor does not
re-submit the job for the same staleness event.
"""

from __future__ import annotations

from typing import Any

from robosystems.logger import get_logger
from robosystems.worker.tasks import register_task
from robosystems.worker.tasks.base import BaseTask

logger = get_logger(__name__)


@register_task("extensions_materialize")
class ExtensionsMaterializeTask(BaseTask):
  """Materialize extensions OLTP data to LadybugDB directly."""

  async def execute(self) -> dict[str, Any]:
    from robosystems.database import get_db_session
    from robosystems.models.core.graph.graph import Graph
    from robosystems.operations.extensions.materialize import ExtensionsMaterializer

    rebuild = self.params.get("rebuild", True)
    lock_key = self.params.get("lock_key")

    await self.report_progress("Starting extensions materialization...", percent=5)

    db_gen = get_db_session()
    db = next(db_gen)

    try:
      materializer = ExtensionsMaterializer()
      result = await materializer.materialize(
        graph_id=self.graph_id,
        rebuild=rebuild,
      )

      if result.status == "error":
        logger.error(
          f"Extensions materialization failed for {self.graph_id}: {result.errors}"
        )
        return {
          "graph_id": self.graph_id,
          "status": "error",
          "errors": result.errors,
          "duration_ms": result.duration_ms,
        }

      await self.report_progress("Marking graph fresh...", percent=95)

      # Clear staleness so the Dagster sensor does not re-submit for this event
      graph = db.query(Graph).filter(Graph.graph_id == self.graph_id).first()
      if graph:
        graph.mark_fresh(session=db)

      logger.info(
        f"Extensions materialization complete for {self.graph_id}: "
        f"{len(result.tables_materialized)} tables, "
        f"{result.total_rows} rows, "
        f"{result.duration_ms:.0f}ms"
      )

      # Report to Dagster asset catalog for observability (fire-and-forget)
      from robosystems.dagster.reporting import report_asset_materialization

      await report_asset_materialization(
        asset_key="extensions_materialize",
        description=f"Extensions materialized for {self.graph_id}",
        metadata={
          "graph_id": self.graph_id,
          "tables_staged": result.tables_staged,
          "tables_materialized": result.tables_materialized,
          "total_rows": result.total_rows,
          "duration_ms": result.duration_ms,
          "rebuild": rebuild,
          "trigger": "manual",
          "operation_id": self.task_id,
        },
      )

      return {
        "graph_id": self.graph_id,
        "status": result.status,
        "tables_staged": result.tables_staged,
        "tables_materialized": result.tables_materialized,
        "total_rows": result.total_rows,
        "duration_ms": result.duration_ms,
        "errors": result.errors,
      }

    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass
      self.release_lock(lock_key)
