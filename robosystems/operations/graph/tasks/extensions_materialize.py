"""Worker task for user-initiated extensions OLTP -> LadybugDB materialization.

Runs ``ExtensionsMaterializer`` in the worker with SSE progress, rather than
through Dagster; the Dagster sensor still owns the automatic, staleness-driven
path. On success the graph is marked fresh, so the sensor does not queue a
second run for the same staleness event.
"""

from __future__ import annotations

from datetime import UTC, datetime
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
      # Stamped before the source is read so a write that lands during the
      # materialization keeps the graph stale (mark_fresh compares).
      started_at = datetime.now(UTC)
      materializer = ExtensionsMaterializer()
      result = await materializer.materialize(
        graph_id=self.graph_id,
        rebuild=rebuild,
      )

      if result.status != "success":
        # 'partial' counts as failure: blue/green did not swap, so the old
        # generation still serves and the graph must stay stale for a retry.
        logger.error(
          f"Extensions materialization {result.status} for "
          f"{self.graph_id}: {result.errors}"
        )
        await self.report_progress(f"Materialization {result.status}.", percent=100)
        return {
          "graph_id": self.graph_id,
          "status": result.status,
          "errors": result.errors,
          "duration_ms": result.duration_ms,
          "execution_time_ms": result.duration_ms,
        }

      await self.report_progress("Marking graph fresh...", percent=95)

      graph = db.query(Graph).filter(Graph.graph_id == self.graph_id).first()
      if graph and not graph.mark_fresh(session=db, started_at=started_at):
        logger.info(
          f"{self.graph_id} was written during the materialization; "
          "leaving it stale for the next sweep"
        )

      logger.info(
        f"Extensions materialization complete for {self.graph_id}: "
        f"{len(result.tables_materialized)} tables, "
        f"{result.total_rows} rows, "
        f"{result.duration_ms:.0f}ms"
      )

      from robosystems.dagster.reporting import report_asset_materialization

      await report_asset_materialization(
        asset_key="user_graph_extensions_materialized",
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
        # Alias read by older SDK clients.
        "execution_time_ms": result.duration_ms,
        "errors": result.errors,
      }

    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass
      self.release_lock(lock_key)
