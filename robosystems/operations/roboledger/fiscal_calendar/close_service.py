"""PeriodCloseService: the month-end close flow shared by REST and MCP.

Callers translate the domain exceptions here into their own error formats.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import or_, text
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.locking import bounded_lock_wait

from .periods import period_date_range
from .qb_writeback import WRITEBACK_EXCLUDED_EVENT_STATUSES
from .service import (
  CloseableGateResult,
  FiscalCalendarService,
)

if TYPE_CHECKING:
  from collections.abc import Callable

  from robosystems.operations.roboledger.reports.statement_sets import (
    StatementStampResult,
  )


class PeriodCloseError(Exception):
  """Base class for period-close failures."""


class CloseGateFailed(PeriodCloseError):
  """The closeable gate rejected the close; detail lives on ``gate``."""

  def __init__(self, gate: CloseableGateResult):
    super().__init__(f"Cannot close period: blockers={gate.blockers}")
    self.gate = gate
    self.blockers = gate.blockers
    self.no_calendar = CloseableGateResult.NO_CALENDAR in gate.blockers


class PeriodNotFoundError(PeriodCloseError):
  def __init__(self, period: str):
    super().__init__(f"Fiscal period {period!r} not found.")
    self.period = period


class PeriodAlreadyClosedError(PeriodCloseError):
  """The period was found closed at the post-publish row-lock revalidation."""

  def __init__(self, period: str):
    super().__init__(f"Fiscal period {period!r} is already closed.")
    self.period = period


class UnbalancedLedgerError(PeriodCloseError):
  """Debits != credits across the period's draft + posted entries."""

  def __init__(self, total_debit: int, total_credit: int):
    super().__init__(
      f"Balance sheet equation broken: debits={total_debit} "
      f"credits={total_credit} diff={total_debit - total_credit}"
    )
    self.total_debit = total_debit
    self.total_credit = total_credit


class WritebackFailed(PeriodCloseError):
  """One or more in-period drafts failed to publish to QuickBooks.

  Raised before any close state mutates; the markers for drafts that did
  reach QB are already committed, so a retry won't re-publish them.
  """

  def __init__(self, failed_events: list[dict]):
    super().__init__(
      f"Cannot close: {len(failed_events)} draft(s) failed to publish to "
      f"QuickBooks. Fix the offending entries and retry the close."
    )
    self.failed_events = failed_events


@dataclass
class PeriodCloseResult:
  """Outcome of a successful close.

  ``entries_posted`` is the total across the QB pre-publish step and the
  local draft→posted pass; the split is on the two fields below.
  """

  period: str
  entries_posted: int
  target_auto_advanced: bool
  calendar: FiscalCalendar
  was_reclose: bool
  entries_published_to_qb: int = 0
  entries_posted_locally: int = 0
  # {"pass", "fail", "error", "skipped"} counts; None when no schedule
  # had facts in the period.
  rule_summary: dict[str, int] | None = None
  evaluated_structure_ids: tuple[str, ...] = ()
  # False with a note when the tenant hasn't set up reporting.
  statements_stamped: bool = False
  statement_stamp_note: str | None = None
  stamped_statement_sets: dict[str, str] = dataclass_field(default_factory=dict)
  statement_rule_summary: dict[str, int] | None = None


# Bump when the receipt's shape changes so a reader can tell an old receipt
# from a new one rather than inferring it from which keys are present.
CLOSE_RECEIPT_VERSION = 1


def _build_close_receipt(
  result: PeriodCloseResult,
  *,
  actor_id: str,
  actor_type: str,
  closed_at: datetime,
) -> dict:
  """Project a close result to the JSON receipt stored on the period.

  Scalars only: `calendar` is a live ORM object and would drift after close.
  """
  return {
    "version": CLOSE_RECEIPT_VERSION,
    "period": result.period,
    "closed_at": closed_at.isoformat(),
    "closed_by": actor_id,
    "actor_type": actor_type,
    "was_reclose": result.was_reclose,
    "entries_posted": result.entries_posted,
    "entries_published_to_qb": result.entries_published_to_qb,
    "entries_posted_locally": result.entries_posted_locally,
    "target_auto_advanced": result.target_auto_advanced,
    "rule_summary": result.rule_summary,
    "evaluated_structure_ids": list(result.evaluated_structure_ids),
    "statements_stamped": result.statements_stamped,
    "statement_stamp_note": result.statement_stamp_note,
    "stamped_statement_sets": dict(result.stamped_statement_sets),
    "statement_rule_summary": result.statement_rule_summary,
  }


class PeriodCloseService:
  """Run a period close as one transaction, which the caller commits.

  The one exception is the QB pre-publish step, which commits its own
  qb_external_id markers: they record external writes that must survive a
  failed close. ``statement_stamper`` is injectable for tests.
  """

  def __init__(
    self,
    fcs: FiscalCalendarService | None = None,
    statement_stamper: Callable[..., StatementStampResult] | None = None,
  ):
    self._fcs = fcs or FiscalCalendarService()
    self._statement_stamper = statement_stamper

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
    allow_stranded_obligations: bool = False,
    allow_reconciling_items: bool = False,
    allow_unposted_source_events: bool = False,
    note: str | None = None,
  ) -> PeriodCloseResult:
    gate = self._fcs.closeable_gate(
      session,
      graph_id,
      period,
      has_sync_connection=has_sync_connection,
      last_sync_at=last_sync_at,
      allow_stale_sync=allow_stale_sync,
      allow_stranded_obligations=allow_stranded_obligations,
      allow_reconciling_items=allow_reconciling_items,
      allow_unposted_source_events=allow_unposted_source_events,
    )
    if not gate.is_closeable:
      raise CloseGateFailed(gate)

    # Only a QuickBooks sync seeds rows past the setup month, so a native or
    # bank-feed ledger reaches its next closeable period with no row for it.
    self._fcs.ensure_fiscal_periods(
      session, graph_id, start_period=period, end_period=period
    )

    period_start, period_end = period_date_range(period)

    # Draft + posted together, before anything mutates.
    self._preflight_bs_check(session, period_start, period_end)

    published_to_qb = self._publish_drafts_to_qb(
      session, graph_id, period_start, period_end, actor_id=actor_id
    )

    # The caller's session-scoped period fence serializes the whole close
    # (a FOR UPDATE would not survive the publish's commit). This row lock
    # guards against anyone who skipped the fence, and the status recheck
    # stops a losing closer from stamping again.
    session.flush()
    with bounded_lock_wait(
      session,
      f"Period {period} is being closed or reopened by another process. "
      "Retry in a moment.",
    ):
      fp = (
        session.query(FiscalPeriod)
        .filter(FiscalPeriod.graph_id == graph_id, FiscalPeriod.name == period)
        .populate_existing()
        .with_for_update()
        .one_or_none()
      )
    if fp is None:
      raise PeriodNotFoundError(period)
    if fp.status == "closed":
      raise PeriodAlreadyClosedError(period)
    is_reclose = fp.status == "closing"

    # Drafts the pre-publish step published are already posted.
    now = datetime.now(UTC)
    retracted_event_ids = session.query(Event.id).filter(
      Event.status.in_(WRITEBACK_EXCLUDED_EVENT_STATUSES)
    )
    posted_locally = (
      session.query(Entry)
      .filter(
        Entry.posting_date >= period_start,
        Entry.posting_date <= period_end,
        Entry.status == "draft",
        or_(
          Entry.triggered_by_event_id.is_(None),
          ~Entry.triggered_by_event_id.in_(retracted_event_ids),
        ),
      )
      .update(
        {Entry.status: "posted", Entry.posted_at: now},
        synchronize_session=False,
      )
    )
    entries_posted = posted_locally + published_to_qb
    session.flush()

    # Must run before this period flips to closed: on a never-closed
    # calendar the sequence check resolves the expected close from the
    # earliest non-closed FiscalPeriod, which has to still be this one.
    cal_before = self._fcs.get(session, graph_id)
    target_before = cal_before.close_target_period if cal_before else None

    effective_note = self._audit_note(
      note,
      allow_stale_sync=allow_stale_sync and has_sync_connection,
      stranded_overridden_count=(
        gate.stranded_obligation_count if allow_stranded_obligations else 0
      ),
      reconciling_overridden_count=(
        gate.reconciling_item_count if allow_reconciling_items else 0
      ),
      unposted_overridden_count=(
        gate.unposted_source_event_count if allow_unposted_source_events else 0
      ),
    )

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

    fp.status = "closed"
    fp.closed_at = now
    fp.closed_by = actor_id
    session.flush()

    # Replace semantics make this idempotent across reclose and retry.
    # StatementStampError rolls back the whole close rather than leave a
    # closed month with no canonical statements. Runs after draft→posted
    # because the pivot reads posted entries only.
    stamp = self._stamp_statement_sets(
      session,
      graph_id=graph_id,
      period_start=period_start,
      period_end=period_end,
      actor_id=actor_id,
    )

    # Rule failures never fail the close; they surface in rule_summary.
    rule_summary, evaluated_ids = self._evaluate_schedule_rules_in_period(
      session,
      period_start=period_start,
      period_end=period_end,
      actor_id=actor_id,
    )

    logger.info(
      f"Period {period} closed for graph {graph_id}: "
      f"entries_posted={entries_posted} "
      f"(published_to_qb={published_to_qb} posted_locally={posted_locally}) "
      f"reclose={is_reclose} "
      f"target_auto_advanced={target_auto_advanced} "
      f"statements_stamped={stamp.stamped} "
      f"stamp_note={stamp.note} "
      f"rule_summary={rule_summary}"
    )

    result = PeriodCloseResult(
      period=period,
      entries_posted=entries_posted,
      target_auto_advanced=target_auto_advanced,
      calendar=calendar,
      was_reclose=is_reclose,
      entries_published_to_qb=published_to_qb,
      entries_posted_locally=posted_locally,
      rule_summary=rule_summary,
      evaluated_structure_ids=evaluated_ids,
      statements_stamped=stamp.stamped,
      statement_stamp_note=stamp.note,
      stamped_statement_sets=stamp.fact_set_ids,
      statement_rule_summary=stamp.rule_summary,
    )

    # Same transaction as the status flip, so a close and its receipt
    # commit or roll back together.
    fp.close_receipt = _build_close_receipt(
      result, actor_id=actor_id, actor_type=actor_type, closed_at=now
    )
    session.flush()

    return result

  def _stamp_statement_sets(
    self,
    session: Session,
    *,
    graph_id: str,
    period_start,
    period_end,
    actor_id: str,
  ) -> StatementStampResult:
    stamper = self._statement_stamper
    if stamper is None:
      from robosystems.operations.roboledger.reports.statement_sets import (
        stamp_canonical_statement_sets,
      )

      stamper = stamp_canonical_statement_sets
    return stamper(
      session,
      graph_id=graph_id,
      period_start=period_start,
      period_end=period_end,
      actor_id=actor_id,
    )

  def _publish_drafts_to_qb(
    self,
    session: Session,
    graph_id: str,
    period_start,
    period_end,
    *,
    actor_id: str,
  ) -> int:
    """Publish in-period RoboLedger-originated drafts to QuickBooks.

    Only for graphs with a write-back QB connection. Each publish also posts
    its draft; returns the count published. Every rejection is collected,
    then `WritebackFailed` is raised before the close mutates anything.

    Deliberately not atomic with the close: the `qb_external_id` markers
    (the only dedupe key) are committed here so a failed close can't roll
    them back and have the retry publish duplicates into QuickBooks.
    """
    from robosystems.database import SessionFactory as _PlatformSessionFactory
    from robosystems.models.api.event_block import ExecuteEventBlockRequest
    from robosystems.operations.event_block.commands import execute_event_block

    from .qb_writeback import (
      resolve_writeback_connection,
      select_writeback_eligible_entries,
    )

    # Shared with the outbox read (`list_period_drafts`) so its preview
    # matches this write.
    with _PlatformSessionFactory() as platform_session:
      writeback = resolve_writeback_connection(platform_session, graph_id)
    if writeback is None:
      logger.debug(
        f"No qb_authoritative QB connection on graph {graph_id}; "
        f"skipping close-period pre-publish step"
      )
      return 0
    qb_connection_id = writeback.connection_id

    drafts_to_publish = select_writeback_eligible_entries(
      session, period_start, period_end
    )

    if not drafts_to_publish:
      logger.debug(
        f"Graph {graph_id}: no RL-originated drafts in period to publish "
        f"({period_start} → {period_end})"
      )
      return 0

    logger.info(
      f"Graph {graph_id}: pre-publishing {len(drafts_to_publish)} draft "
      f"event(s) to QB connection {qb_connection_id} before close"
    )

    # Each publish is a synchronous QB round-trip inside the open transaction.
    if len(drafts_to_publish) > 5:
      logger.warning(
        f"Graph {graph_id}: pre-publishing {len(drafts_to_publish)} drafts in "
        f"sequence — extensions transaction held open ~{len(drafts_to_publish) * 2}s "
        f"during the close. Consider batching the period (close in smaller "
        f"windows)."
      )

    # Collect every failure rather than failing fast. One QB client for the
    # run, so the token refreshes once rather than once per entry.
    failed_events: list[dict] = []
    qb_clients: dict[str, Any] = {}
    for entry, event in drafts_to_publish:
      try:
        # Savepoint: a database error (e.g. a lock-wait timeout) would
        # otherwise abort the transaction and take down the marker commit
        # below, leaving QB entries the ledger has no record of sending.
        with session.begin_nested():
          result = execute_event_block(
            session,
            ExecuteEventBlockRequest(
              event_id=str(event.id),
              connection_id=qb_connection_id,
            ),
            created_by=actor_id,
            graph_id=graph_id,
            acquire_period_fence=False,
            entry_ids=[str(entry.id)],
            qb_clients=qb_clients,
          )
        if result.qb_error is not None:
          failed_events.append(
            {
              "event_id": str(event.id),
              "entry_id": str(entry.id),
              "memo": entry.memo,
              "posting_date": str(entry.posting_date),
              "qb_error": result.qb_error,
            }
          )
      except Exception as e:
        failed_events.append(
          {
            "event_id": str(event.id),
            "entry_id": str(entry.id),
            "memo": entry.memo,
            "posting_date": str(entry.posting_date),
            "qb_error": {"code": type(e).__name__, "message": str(e)},
          }
        )

    # Durability boundary for the qb_external_id markers. The session stays
    # tenant-scoped across it: `extensions_session` re-binds search_path on
    # every transaction.
    session.commit()

    if failed_events:
      raise WritebackFailed(failed_events)

    return len(drafts_to_publish)

  def _preflight_bs_check(
    self,
    session: Session,
    period_start,
    period_end,
  ) -> None:
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
    """Evaluate rules on every schedule with facts in the period.

    Returns (rule_summary, evaluated_structure_ids). An engine exception is
    logged and skipped; it never fails the close.
    """
    from robosystems.operations.information_block.rules.engine import (
      evaluate_rules_for_structure,
    )

    # Unqualified table names: relies on the tenant search_path.
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
        # Savepoint: a database error in rule writes must not abort the
        # close's transaction after the QB markers committed.
        with session.begin_nested():
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
  def _audit_note(
    note: str | None,
    *,
    allow_stale_sync: bool,
    stranded_overridden_count: int = 0,
    reconciling_overridden_count: int = 0,
    unposted_overridden_count: int = 0,
  ) -> str | None:
    """Append a marker to the audit note for each overridden close gate."""
    suffixes: list[str] = []
    if allow_stale_sync:
      suffixes.append("[sync gate overridden — allow_stale_sync=true]")
    if stranded_overridden_count > 0:
      suffixes.append(
        "[stranded-obligation gate overridden — "
        f"{stranded_overridden_count} undrafted obligation(s) omitted]"
      )
    if reconciling_overridden_count > 0:
      suffixes.append(
        "[reconciling-item gate overridden — "
        f"{reconciling_overridden_count} unresolved item(s) left undecided]"
      )
    if unposted_overridden_count > 0:
      suffixes.append(
        "[unposted-source-event gate overridden — "
        f"{unposted_overridden_count} uncommitted event(s) left out of the period]"
      )
    if not suffixes:
      return note
    suffix = " ".join(suffixes)
    if note:
      return f"{note} {suffix}"
    return suffix


__all__ = [
  "CloseGateFailed",
  "PeriodAlreadyClosedError",
  "PeriodCloseError",
  "PeriodCloseResult",
  "PeriodCloseService",
  "PeriodNotFoundError",
  "UnbalancedLedgerError",
  "WritebackFailed",
]
