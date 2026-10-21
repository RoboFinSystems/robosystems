"""Worker task for backfill-plan-history, which recloses up to 24 months and
outlasts the MCP tool budget as one close can.

Refusals are returned as results with an ``error`` key; only faults raise.
"""

from __future__ import annotations

from typing import Any

from robosystems.worker.tasks import register_task
from robosystems.worker.tasks.base import BaseTask


def backfill_payload(
  graph_id: str, params: dict[str, Any], actor_id: str
) -> dict[str, Any]:
  """Run the backfill and shape what a caller reads back: the outcome per
  month and what remains, or the refusal."""
  from sqlalchemy.exc import SQLAlchemyError

  from robosystems.db.extensions import extensions_session
  from robosystems.db.platform import platform_session
  from robosystems.middleware.mcp.tools._errors import database_failure
  from robosystems.models.api.extensions.fiscal_calendar import (
    BackfillPlanHistoryRequest,
  )
  from robosystems.operations.roboledger.commands.fiscal_calendar import (
    BackfillPreconditionError,
    backfill_plan_history,
  )
  from robosystems.operations.roboledger.entity_scope import find_entity_id
  from robosystems.operations.roboledger.fiscal_calendar import (
    FiscalCalendarError,
    FiscalCalendarService,
  )
  from robosystems.operations.roboledger.fiscal_calendar.close_service import (
    PeriodCloseService,
  )
  from robosystems.operations.roboledger.reads.fiscal_calendar import (
    entity_sync_state,
  )

  body = BackfillPlanHistoryRequest.model_validate(params)
  service = FiscalCalendarService()
  try:
    with extensions_session(graph_id) as session, platform_session() as platform_db:
      result = backfill_plan_history(
        session,
        platform_db,
        graph_id,
        body,
        actor_id=actor_id,
        service=service,
        close_service=PeriodCloseService(service),
        actor_type="agent",
        entity_id=body.entity_id,
      )
      calendar = result.fiscal_calendar.model_dump(mode="json")
      has_sync, _ = entity_sync_state(
        session, platform_db, graph_id, find_entity_id(session, body.entity_id)
      )
      calendar["has_sync_connection"] = has_sync
      return {
        "earliest_available_period": result.earliest_available_period,
        "effective_start_period": result.effective_start_period,
        "closed_through": result.closed_through,
        "period_rows_created": result.period_rows_created,
        "processed": [outcome.model_dump(mode="json") for outcome in result.processed],
        "remaining_periods": result.remaining_periods,
        "fiscal_calendar": calendar,
      }
  except BackfillPreconditionError as exc:
    return {"error": exc.code, "message": str(exc)}
  except FiscalCalendarError as exc:
    return {"error": "calendar_error", "message": str(exc)}
  except SQLAlchemyError as exc:
    return database_failure("backfill-plan-history", exc)


@register_task("plan_history_backfill")
class PlanHistoryBackfillTask(BaseTask):
  """Reclose the months behind the close boundary on the worker."""

  async def execute(self) -> dict[str, Any]:
    from robosystems.middleware.sse.event_storage import EventType

    await self.manager.event_storage.store_event(
      self.task_id,
      EventType.OPERATION_STARTED,
      {"operation_type": "plan_history_backfill", "graph_id": self.graph_id},
    )
    await self.report_progress("Compiling statement history…", percent=5)
    result = await self.run_blocking(self._run)
    await self.report_progress("Statement history compiled.", percent=100)
    return {**result, "operation_id": self.task_id}

  def _run(self) -> dict[str, Any]:
    if self.graph_id is None:
      raise ValueError("plan_history_backfill requires a graph_id")
    params = dict(self.params)
    actor_id = params.pop("actor_id", None) or self.user_id or f"mcp:{self.graph_id}"
    return backfill_payload(self.graph_id, params, str(actor_id))
