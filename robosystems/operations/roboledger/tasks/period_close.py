"""Worker task for period close, which can outlast the MCP tool budget.

The fence wait is the task's budget, so a duplicate dispatch queues behind
the close it duplicates and returns that close's receipt. Refusals are
returned as results (see ``close_outcomes``); only faults raise.
"""

from __future__ import annotations

from typing import Any

from robosystems.logger import get_logger
from robosystems.worker.tasks import register_task
from robosystems.worker.tasks.base import BaseTask

logger = get_logger(__name__)


@register_task("period_close")
class PeriodCloseTask(BaseTask):
  """Close a fiscal period on the worker, reporting the outcome either way."""

  async def execute(self) -> dict[str, Any]:
    from robosystems.middleware.sse.event_storage import EventType

    period = self.params["period"]

    # Nothing else moves the operation off PENDING, and the tool tells
    # "queued" apart from "running, do not retry".
    await self.manager.event_storage.store_event(
      self.task_id,
      EventType.OPERATION_STARTED,
      {
        "operation_type": "period_close",
        "graph_id": self.graph_id,
        "period": period,
      },
    )
    await self.report_progress(f"Closing {period}…", percent=5)

    # Off the event loop: the close blocks on QuickBooks. The budget can't
    # cancel the thread, so run_blocking waits it out rather than report a
    # landing close as failed.
    result = await self.run_blocking(self._run_close)

    outcome = result.get("outcome")
    await self.report_progress(
      f"Close {'completed' if outcome == 'closed' else 'refused'} for {period}.",
      percent=100,
    )
    return result

  def _run_close(self) -> dict[str, Any]:
    from robosystems.db.extensions import extensions_session
    from robosystems.db.platform import platform_session
    from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
    from robosystems.operations.roboledger.commands.fiscal_calendar import (
      close_period as cmd_close_period,
    )
    from robosystems.operations.roboledger.fiscal_calendar import (
      FiscalCalendarService,
      PeriodAlreadyClosedError,
    )
    from robosystems.operations.roboledger.fiscal_calendar.close_outcomes import (
      CLOSE_DOMAIN_ERRORS,
      close_error_payload,
      close_success_payload,
    )
    from robosystems.operations.roboledger.fiscal_calendar.close_service import (
      PeriodCloseService,
    )
    from robosystems.operations.roboledger.reads.fiscal_calendar import qb_sync_state

    period = self.params["period"]
    graph_id = self.graph_id
    if graph_id is None:
      raise ValueError("period_close requires a graph_id")

    service = FiscalCalendarService()
    with platform_session() as platform_db, extensions_session(graph_id) as session:
      try:
        result = cmd_close_period(
          session,
          platform_db,
          graph_id,
          period,
          actor_id=self.params.get("actor_id") or self.user_id,
          allow_stale_sync=bool(self.params.get("allow_stale_sync", False)),
          note=self.params.get("note"),
          service=service,
          close_service=PeriodCloseService(service),
          actor_type=self.params.get("actor_type", "agent"),
          allow_stranded_obligations=bool(
            self.params.get("allow_stranded_obligations", False)
          ),
          allow_reconciling_items=bool(
            self.params.get("allow_reconciling_items", False)
          ),
          allow_unposted_source_events=bool(
            self.params.get("allow_unposted_source_events", False)
          ),
          # Usually what holds the fence is another close of the same
          # period; waiting it out yields "already closed" with a receipt.
          fence_wait_ms=self.budget_seconds * 1000,
        )
      except CLOSE_DOMAIN_ERRORS as exc:
        payload = close_error_payload(exc, period=period)
        if payload is None:  # pragma: no cover - CLOSE_DOMAIN_ERRORS covers these
          raise
        payload["outcome"] = "rejected"
        payload["operation_id"] = self.task_id
        # The stalled-task reaper can re-queue a close that already landed;
        # hand back its receipt rather than report success as a refusal.
        if _is_already_closed(exc, PeriodAlreadyClosedError):
          receipt = (
            session.query(FiscalPeriod.close_receipt)
            .filter(
              FiscalPeriod.graph_id == graph_id,
              FiscalPeriod.name == period,
            )
            .scalar()
          )
          if receipt:
            payload["close_receipt"] = receipt
        # Returning exits the `with` cleanly, which would COMMIT; a stamp
        # failure raises after the period was already flipped to closed.
        # WritebackFailed's markers were committed earlier and survive.
        session.rollback()
        logger.info(
          "period_close refused for %s %s: %s",
          graph_id,
          period,
          payload.get("error"),
        )
        return payload

      has_sync, _last_sync_at = qb_sync_state(platform_db, graph_id)

    return {
      **close_success_payload(result, has_sync_connection=has_sync),
      "outcome": "closed",
      "operation_id": self.task_id,
    }


def _is_already_closed(exc: Exception, already_closed_type: type[Exception]) -> bool:
  """Whether this refusal means the period is closed rather than uncloseable."""
  if isinstance(exc, already_closed_type):
    return True
  blockers = getattr(getattr(exc, "gate", None), "blockers", None) or []
  return "period_already_closed" in blockers
