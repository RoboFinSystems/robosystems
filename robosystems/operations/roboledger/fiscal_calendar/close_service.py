"""PeriodCloseService — the single source of truth for month-end close.

Used by both the REST router and the MCP tool. Encapsulates the full
close flow (gate, draft-to-posted transition, BS balance check, calendar
advance/reclose) behind one service method so bug fixes and behavior
changes land in one place instead of two.

This module does NOT handle HTTP/MCP response shaping — callers translate
the domain exceptions defined here into their respective error formats.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod

from .periods import period_date_range
from .service import (
  CloseableGateResult,
  FiscalCalendarService,
)

# ────────────────────────────────────────────────────────────────────────────
# Errors
# ────────────────────────────────────────────────────────────────────────────


class PeriodCloseError(Exception):
  """Base class for all period-close failures."""


class CloseGateFailed(PeriodCloseError):
  """Raised when the closeable gate rejects the request.

  Carries the full :class:`CloseableGateResult` so callers can surface
  the enriched payload (``pending_obligation_count``,
  ``pending_obligation_sample``, ``earliest_pending_period``,
  ``sync_stale_days``) without re-fetching the calendar. ``blockers``
  / ``no_calendar`` are kept as direct attributes for the common
  branches; full detail lives on ``gate``.
  """

  def __init__(self, gate: CloseableGateResult):
    super().__init__(f"Cannot close period: blockers={gate.blockers}")
    self.gate = gate
    self.blockers = gate.blockers
    self.no_calendar = CloseableGateResult.NO_CALENDAR in gate.blockers


class PeriodNotFoundError(PeriodCloseError):
  """Raised when no FiscalPeriod row exists for the target period."""

  def __init__(self, period: str):
    super().__init__(f"Fiscal period {period!r} not found.")
    self.period = period


class UnbalancedLedgerError(PeriodCloseError):
  """Raised when debits != credits for the period being closed.

  Detected as a pre-flight check against the combined draft + posted
  line items — we never mutate state when the ledger is unbalanced,
  so no rollback is needed.
  """

  def __init__(self, total_debit: int, total_credit: int):
    super().__init__(
      f"Balance sheet equation broken: debits={total_debit} "
      f"credits={total_credit} diff={total_debit - total_credit}"
    )
    self.total_debit = total_debit
    self.total_credit = total_credit


# ────────────────────────────────────────────────────────────────────────────
# Result
# ────────────────────────────────────────────────────────────────────────────


@dataclass
class PeriodCloseResult:
  """Outcome of a successful `PeriodCloseService.close()` call."""

  period: str
  entries_posted: int
  target_auto_advanced: bool
  calendar: FiscalCalendar
  was_reclose: bool
  # §3.8 auto-run rules on close. None if no schedules with facts in the
  # period had rules attached. Otherwise a dict tallying outcomes:
  # {"pass": int, "fail": int, "error": int, "skipped": int}.
  rule_summary: dict[str, int] | None = None
  # ids of schedule Structures whose rules were evaluated. Empty when no
  # schedule structures had facts in the closed period.
  evaluated_structure_ids: tuple[str, ...] = ()


# ────────────────────────────────────────────────────────────────────────────
# Service
# ────────────────────────────────────────────────────────────────────────────


class PeriodCloseService:
  """Execute the full period close flow in one atomic operation.

  The service runs, in order:

  1. **Closeable gate check** — sequence, period_complete, sync_current.
  2. **BS balance pre-flight** — SUM(debits) vs SUM(credits) across
     `draft` + `posted` entries in the period. Raises
     `UnbalancedLedgerError` BEFORE mutating state, so we never need a
     rollback after flipping drafts to posted.
  3. **Draft → posted transition** — all draft entries whose posting_date
     falls in the period are bulk-updated to `posted`.
  4. **FiscalPeriod transition** — `status='closed'`, `closed_at=now`,
     `closed_by=actor_id`.
  5. **Calendar advance or reclose** — advance_closed_through if this is
     a first-close or latest-reopen reclose, otherwise record_reclose.

  Callers commit the session. All failures raise domain exceptions that
  the REST / MCP caller translates into its native error format.
  """

  def __init__(self, fcs: FiscalCalendarService | None = None):
    self._fcs = fcs or FiscalCalendarService()

  def close(
    self,
    session: Session,
    graph_id: str,
    period: str,
    *,
    actor_id: str,
    actor_type: str = "user",
    has_sync_connection: bool,
    last_sync_at: datetime | None,
    allow_stale_sync: bool = False,
    note: str | None = None,
  ) -> PeriodCloseResult:
    """Close `period` atomically. See class docstring for the full flow."""
    # 1. Gate
    gate = self._fcs.closeable_gate(
      session,
      graph_id,
      period,
      has_sync_connection=has_sync_connection,
      last_sync_at=last_sync_at,
      allow_stale_sync=allow_stale_sync,
    )
    if not gate.is_closeable:
      raise CloseGateFailed(gate)

    period_start, period_end = period_date_range(period)

    # 2. Pre-flight BS balance check. Run against draft + posted together
    # so we know the ledger will still balance after the transition — no
    # state is mutated yet, so a mismatch just raises cleanly.
    self._preflight_bs_check(session, period_start, period_end)

    # Find the FiscalPeriod row before mutating anything so we can detect
    # the re-close path (status='closing' means a prior reopen).
    fp = (
      session.query(FiscalPeriod)
      .filter(FiscalPeriod.graph_id == graph_id, FiscalPeriod.name == period)
      .one_or_none()
    )
    if fp is None:
      raise PeriodNotFoundError(period)
    is_reclose = fp.status == "closing"

    # 3. Draft → posted
    now = datetime.now(UTC)
    entries_posted = (
      session.query(Entry)
      .filter(
        Entry.posting_date >= period_start,
        Entry.posting_date <= period_end,
        Entry.status == "draft",
      )
      .update(
        {Entry.status: "posted", Entry.posted_at: now},
        synchronize_session=False,
      )
    )
    session.flush()

    # 4. FiscalPeriod transition
    fp.status = "closed"
    fp.closed_at = now
    fp.closed_by = actor_id
    session.flush()

    # 5. Advance / reclose. Capture target before so we can report
    # auto-advance to the caller.
    cal_before = self._fcs.get(session, graph_id)
    target_before = cal_before.close_target_period if cal_before else None

    # Build an audit-friendly note. When the sync gate was overridden, we
    # record that explicitly so compliance audits can flag the close.
    effective_note = self._audit_note(
      note,
      allow_stale_sync=allow_stale_sync and has_sync_connection,
    )

    # Route: latest-reopen vs older-reopen reclose vs normal advance
    if is_reclose and not self._fcs.is_latest_sequential_close(
      session,
      graph_id,
      cal_before,
      period,  # type: ignore[arg-type]
    ):
      calendar = self._fcs.record_reclose(
        session,
        graph_id,
        period,
        actor_id=actor_id,
        actor_type=actor_type,
        note=effective_note,
      )
    else:
      calendar = self._fcs.advance_closed_through(
        session,
        graph_id,
        period,
        actor_id=actor_id,
        actor_type=actor_type,
        note=effective_note,
      )

    target_auto_advanced = (
      target_before != calendar.close_target_period
      and calendar.close_target_period is not None
    )

    # 6. §3.8 — Auto-run rules on schedules with facts in the closing
    # period. Rule failures are isolated from the close result: the close
    # succeeds even if a rule errors, and the failure surfaces only in
    # rule_summary / verification_results for downstream inspection. This
    # keeps the close path as the "single source of truth" while letting
    # the validation panel accumulate fresh results without an explicit
    # `POST /evaluate-rules` call.
    rule_summary, evaluated_ids = self._evaluate_schedule_rules_in_period(
      session,
      period_start=period_start,
      period_end=period_end,
      actor_id=actor_id,
    )

    logger.info(
      f"Period {period} closed for graph {graph_id}: "
      f"entries_posted={entries_posted} reclose={is_reclose} "
      f"target_auto_advanced={target_auto_advanced} "
      f"rule_summary={rule_summary}"
    )

    return PeriodCloseResult(
      period=period,
      entries_posted=entries_posted,
      target_auto_advanced=target_auto_advanced,
      calendar=calendar,
      was_reclose=is_reclose,
      rule_summary=rule_summary,
      evaluated_structure_ids=evaluated_ids,
    )

  # ── Private helpers ────────────────────────────────────────────────────

  def _preflight_bs_check(
    self,
    session: Session,
    period_start,
    period_end,
  ) -> None:
    """Pre-flight check: does debit total equal credit total for the period?

    Runs across BOTH draft and posted entries — the draft entries are
    about to be posted, so they need to balance for the close to succeed.
    Raises `UnbalancedLedgerError` if not. Because this runs before any
    mutation, there's no state to roll back.
    """
    row = session.execute(
      text("""
        SELECT
          COALESCE(SUM(li.debit_amount), 0)  AS total_debit,
          COALESCE(SUM(li.credit_amount), 0) AS total_credit
        FROM line_items li
        JOIN entries e ON e.id = li.entry_id
        WHERE e.posting_date >= :period_start
          AND e.posting_date <= :period_end
          AND e.status IN ('draft', 'posted')
      """),
      {"period_start": period_start, "period_end": period_end},
    ).fetchone()
    total_debit = int(row.total_debit) if row else 0
    total_credit = int(row.total_credit) if row else 0
    if total_debit != total_credit:
      raise UnbalancedLedgerError(total_debit, total_credit)

  def _evaluate_schedule_rules_in_period(
    self,
    session: Session,
    *,
    period_start,
    period_end,
    actor_id: str,
  ) -> tuple[dict[str, int] | None, tuple[str, ...]]:
    """Run the rule engine for every schedule Structure with facts in the
    closing period. Returns (rule_summary, evaluated_structure_ids).

    Failures from individual rule evaluations are caught and logged —
    they cannot break the close path, which has already succeeded by
    the time this is called. The rule engine itself already converts
    binding/dispatch failures into ``VerificationResult`` rows with
    ``status='error'``; this wrapper guards against an outright engine
    exception (e.g., schema-level issue) for robustness.
    """
    # Lazy import — keeps the close service free of information-block
    # dependencies at module import time.
    from robosystems.operations.information_block.rules.engine import (
      evaluate_rules_for_structure,
    )

    # Unqualified table names — relies on `extensions_session(graph_id)`
    # having SET the tenant's search_path before this method runs. The
    # close service is only called from that session context (REST
    # handler + MCP tool both go through `cmd_close_period`); any future
    # refactor that bypasses the tenant session must qualify these
    # tables explicitly to avoid silently querying the wrong schema.
    structure_ids = (
      session.execute(
        text(
          """
          SELECT DISTINCT s.id
          FROM structures s
          JOIN facts f ON f.structure_id = s.id
          WHERE s.block_type = 'schedule'
            AND f.fact_scope = 'in_scope'
            AND f.period_end >= :period_start
            AND f.period_end <= :period_end
          """
        ),
        {"period_start": period_start, "period_end": period_end},
      )
      .scalars()
      .all()
    )

    if not structure_ids:
      return None, ()

    tally: dict[str, int] = {"pass": 0, "fail": 0, "error": 0, "skipped": 0}
    for sid in structure_ids:
      try:
        rows = evaluate_rules_for_structure(
          session,
          sid,
          period_start=period_start,
          period_end=period_end,
          created_by=actor_id,
        )
      except Exception as exc:
        logger.warning(f"Rule eval failed for structure {sid} during close: {exc}")
        continue
      for row in rows:
        tally[row.status] = tally.get(row.status, 0) + 1

    return tally, tuple(structure_ids)

  @staticmethod
  def _audit_note(note: str | None, *, allow_stale_sync: bool) -> str | None:
    """Annotate the audit note when the sync gate was overridden.

    This ensures the `period_closed` event reflects that a human asserted
    "the data is complete despite the stale sync," which matters for
    compliance review.
    """
    if not allow_stale_sync:
      return note
    suffix = "[sync gate overridden — allow_stale_sync=true]"
    if note:
      return f"{note} {suffix}"
    return suffix


__all__ = [
  "CloseGateFailed",
  "PeriodCloseError",
  "PeriodCloseResult",
  "PeriodCloseService",
  "PeriodNotFoundError",
  "UnbalancedLedgerError",
]
