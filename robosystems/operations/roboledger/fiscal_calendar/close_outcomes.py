"""The one payload shape for a close, shared by the MCP tool and its worker task.

A refused close (e.g. a gate rejection) is an outcome, not a fault: the
worker stringifies raised exceptions, which would lose the blockers, so
refusals are shaped into payloads here and only genuine faults raise.
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

# Refusals, not faults; anything else should propagate.
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
    "rule_summary": result.rule_summary,
    "evaluated_structure_ids": list(result.evaluated_structure_ids),
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
