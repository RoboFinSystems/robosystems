"""What a close says about itself, in one shape.

The close has two callers that must describe it identically: the MCP tool,
and the worker task the tool dispatches to. Their outcomes cannot be shaped
independently — the tool returns whatever the task produced, so a divergence
would mean the agent sees a different close depending on how fast the worker
answered.

It also settles a distinction the worker forces. A gate rejection ("this
period has pending obligations") is an *outcome*, not a fault: it is the
close correctly declining, and the operator needs the blockers to act on.
The worker turns any raised exception into a plain string, so a rejection
that propagated would arrive as `"CloseGateFailed(...)"` with the blockers
gone. Everything a caller might reasonably act on is therefore shaped into a
result here, and only genuine faults are left to raise.
"""

from __future__ import annotations

from typing import Any

from robosystems.models.api.extensions.fiscal_calendar import ClosePeriodResponse
from robosystems.operations.locking import RowLockedError
from robosystems.operations.roboledger.fiscal_calendar import (
  CloseGateFailed,
  FiscalCalendarError,
  PeriodAlreadyClosedError,
  PeriodNotFoundError,
  UnbalancedLedgerError,
)
from robosystems.operations.roboledger.fiscal_calendar.close_service import (
  WritebackFailed,
)
from robosystems.operations.roboledger.reports.statement_sets import StatementStampError

# The exceptions that describe a close rather than a fault. Each maps to a
# structured payload below; anything outside this tuple is a bug or an
# infrastructure failure and should keep propagating.
CLOSE_DOMAIN_ERRORS: tuple[type[Exception], ...] = (
  CloseGateFailed,
  PeriodNotFoundError,
  PeriodAlreadyClosedError,
  RowLockedError,
  UnbalancedLedgerError,
  WritebackFailed,
  StatementStampError,
  FiscalCalendarError,
)


def close_success_payload(
  result: ClosePeriodResponse, *, has_sync_connection: bool
) -> dict[str, Any]:
  """The receipt an agent reads after a close lands."""
  fiscal_calendar = result.fiscal_calendar.model_dump(mode="json")
  # Not on the Pydantic response, but the agent needs it to reason about
  # the sync gate on the next period.
  fiscal_calendar["has_sync_connection"] = has_sync_connection
  return {
    "period": result.period,
    "entries_posted": result.entries_posted,
    "entries_published_to_qb": result.entries_published_to_qb,
    "entries_posted_locally": result.entries_posted_locally,
    "target_auto_advanced": result.target_auto_advanced,
    "fiscal_calendar": fiscal_calendar,
    # Rule outcomes from the auto-run on close, pairing with the REST
    # response so agents and REST consumers see the same surface.
    "rule_summary": result.rule_summary,
    "evaluated_structure_ids": list(result.evaluated_structure_ids),
    # Canonical statement stamping — without these keys an agent never
    # learns that the close persisted the month's statements.
    "statements_stamped": result.statements_stamped,
    "statement_stamp_note": result.statement_stamp_note,
    "stamped_statement_sets": dict(result.stamped_statement_sets),
    "statement_rule_summary": result.statement_rule_summary,
  }


def close_error_payload(exc: Exception, *, period: str) -> dict[str, Any] | None:
  """Describe a refused close, or return None if this is not one.

  None means the exception is a fault rather than an outcome, and the
  caller should let it propagate.
  """
  if isinstance(exc, CloseGateFailed):
    return _gate_payload(exc, period=period)
  if isinstance(exc, PeriodNotFoundError):
    return {"error": "period_not_found", "message": str(exc)}
  if isinstance(exc, PeriodAlreadyClosedError):
    return {"error": "already_closed", "message": str(exc)}
  if isinstance(exc, RowLockedError):
    return {"error": "row_locked", "message": str(exc)}
  if isinstance(exc, UnbalancedLedgerError):
    return {
      "error": "unbalanced",
      "message": (
        f"Balance sheet equation broken for period {period!r}: "
        f"debits={exc.total_debit} credits={exc.total_credit}. "
        "Review the ledger before closing."
      ),
    }
  if isinstance(exc, WritebackFailed):
    # The publish markers are committed before this raises, so a retry
    # does not re-send what already reached QuickBooks.
    return {
      "error": "write_back_failed",
      "message": (
        f"{exc} The entries that did publish are recorded as published; "
        "fix the rejected ones and re-run close-period."
      ),
      "failed_events": list(getattr(exc, "failed_events", []) or []),
    }
  if isinstance(exc, StatementStampError):
    return {
      "error": "statement_stamp_failed",
      "message": (
        f"{exc} The close rolled back — nothing was committed. Fix the "
        "mapping/reporting configuration and re-run close-period."
      ),
    }
  if isinstance(exc, FiscalCalendarError):
    return {"error": "calendar_error", "message": str(exc)}
  return None


def _gate_payload(exc: CloseGateFailed, *, period: str) -> dict[str, Any]:
  if exc.no_calendar:
    return {
      "error": "calendar_not_initialized",
      "message": "Fiscal calendar not initialized for this graph.",
    }
  payload: dict[str, Any] = {
    "error": "not_closeable",
    "message": f"Cannot close period {period!r}.",
    "blockers": exc.blockers,
  }
  # Each detail block rides only when its blocker fired, so the payload
  # names what to go and fix rather than making the agent ask again.
  if exc.gate.pending_obligation_count:
    payload["pending_obligation_count"] = exc.gate.pending_obligation_count
    payload["pending_obligation_sample"] = [
      _obligation(detail) for detail in exc.gate.pending_obligation_sample
    ]
    payload["earliest_pending_period"] = exc.gate.earliest_pending_period
  if exc.gate.stranded_obligation_count:
    payload["stranded_obligation_count"] = exc.gate.stranded_obligation_count
    payload["stranded_obligation_sample"] = [
      _obligation(detail) for detail in exc.gate.stranded_obligation_sample
    ]
  if exc.gate.reconciling_item_count:
    payload["reconciling_item_count"] = exc.gate.reconciling_item_count
    payload["reconciling_item_sample"] = list(exc.gate.reconciling_item_sample)
  if exc.gate.sync_stale_days is not None:
    payload["sync_stale_days"] = exc.gate.sync_stale_days
  return payload


def _obligation(detail: Any) -> dict[str, Any]:
  return {
    "event_id": detail.event_id,
    "schedule_id": detail.schedule_id,
    "schedule_name": detail.schedule_name,
    "period": detail.period,
  }
