"""Worker task for period close.

The close runs ~30 seconds on a real month — most of it one QuickBooks
round-trip per draft — against a 25-second tool budget on the MCP surface.
It always *completed*; the operator just stopped hearing about it, and then
had to know not to retry a write that had already landed.

Running it here decouples the work from the listener. The close itself is
unchanged: same command, same exclusive period fence, same mid-flow commit
of the publish markers. What changes is who waits.

**A refused close is a result, not a failure.** The consumer reduces any
raised exception to a plain string, so a gate rejection that propagated
would reach the operator as text with its blockers stripped — the one part
they need. Domain outcomes are therefore returned (shaped by
``close_outcomes``), and only genuine faults are left to raise.
"""

from __future__ import annotations

import asyncio
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

    # Nothing else moves a worker operation off PENDING, and the tool's
    # handback reads very differently for "queued behind a busy worker"
    # than for "running now, do not retry".
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

    # The close is synchronous and blocks on QuickBooks, so it cannot run on
    # the event loop the consumer uses to emit progress.
    result = await asyncio.to_thread(self._run_close)

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
    assert graph_id is not None  # every close is graph-scoped

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
        )
      except CLOSE_DOMAIN_ERRORS as exc:
        payload = close_error_payload(exc, period=period)
        if payload is None:  # pragma: no cover - CLOSE_DOMAIN_ERRORS covers these
          raise
        payload["outcome"] = "rejected"
        payload["operation_id"] = self.task_id
        # A close that already landed can be re-queued by the stalled-task
        # reaper, which measures age from enqueue rather than from start.
        # Reporting only "already closed" would describe a successful close
        # as a refusal, so hand back the receipt it wrote.
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
