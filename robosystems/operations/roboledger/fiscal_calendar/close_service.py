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
from dataclasses import field as dataclass_field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.locking import bounded_lock_wait

from .periods import period_date_range
from .service import (
  CloseableGateResult,
  FiscalCalendarService,
)

if TYPE_CHECKING:
  from collections.abc import Callable

  from robosystems.operations.roboledger.reports.statement_sets import (
    StatementStampResult,
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


class PeriodAlreadyClosedError(PeriodCloseError):
  """Raised when the period is already closed at the post-publish revalidation.

  The exclusive period fence is supposed to stop a second closer before
  this, but the database close still re-locks the FiscalPeriod row after
  the QuickBooks commit and refuses `status='closed'`. Without that
  check a closer that skipped the fence would stamp again.
  """

  def __init__(self, period: str):
    super().__init__(f"Fiscal period {period!r} is already closed.")
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


class WritebackFailed(PeriodCloseError):
  """Raised when the close-period pre-publish step fails to write one or
  more in-period drafts to QuickBooks.

  Atomic: a single QB rejection rolls back the entire close — no
  half-published periods. The exception carries the offending event
  IDs and their per-event error payloads so the operator can fix the
  underlying issue (mapping, balance, closed-in-QB-already) and retry.
  """

  def __init__(self, failed_events: list[dict]):
    super().__init__(
      f"Cannot close: {len(failed_events)} draft(s) failed to publish to "
      f"QuickBooks. Fix the offending entries and retry the close."
    )
    self.failed_events = failed_events


# ────────────────────────────────────────────────────────────────────────────
# Result
# ────────────────────────────────────────────────────────────────────────────


@dataclass
class PeriodCloseResult:
  """Outcome of a successful `PeriodCloseService.close()` call.

  ``entries_posted`` is the TOTAL entries the close transitioned to
  posted, across both post paths: the QB pre-publish step (which promotes
  each published draft immediately) and the bulk draft→posted transition
  that follows. Counting only the bulk path would under-report a
  fully-published period as 0. The split rides on
  ``entries_published_to_qb`` / ``entries_posted_locally``.
  """

  period: str
  entries_posted: int
  target_auto_advanced: bool
  calendar: FiscalCalendar
  was_reclose: bool
  # The two post paths behind entries_posted.
  entries_published_to_qb: int = 0
  entries_posted_locally: int = 0
  # Auto-run rules on close. None if no schedules with facts in the
  # period had rules attached. Otherwise a dict tallying outcomes:
  # {"pass": int, "fail": int, "error": int, "skipped": int}.
  rule_summary: dict[str, int] | None = None
  # ids of schedule Structures whose rules were evaluated. Empty when no
  # schedule structures had facts in the closed period.
  evaluated_structure_ids: tuple[str, ...] = ()
  # Canonical statement stamping (close-time pivot). False + note when
  # the tenant hasn't set up reporting (soft-skip); True with the minted
  # structure_id -> fact_set_id map otherwise. A stamp failure on a
  # reporting-configured tenant raises StatementStampError instead —
  # the close rolls back rather than holing the statement series.
  statements_stamped: bool = False
  statement_stamp_note: str | None = None
  stamped_statement_sets: dict[str, str] = dataclass_field(default_factory=dict)
  statement_rule_summary: dict[str, int] | None = None


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
  4. **Calendar advance or reclose** — advance_closed_through if this is
     a first-close or latest-reopen reclose, otherwise record_reclose.
     Runs BEFORE the FiscalPeriod flip: the never-closed
     (`closed_through IS NULL`) sequence check resolves the expected
     close from the earliest non-closed FiscalPeriod, which must still
     be the period being closed.
  5. **FiscalPeriod transition** — `status='closed'`, `closed_at=now`,
     `closed_by=actor_id`.
  5b. **Canonical statement stamping** — pivot the posted ledger and
     stamp the period's statement FactSets (`report_id NULL`), replacing
     any prior canonical sets for the window (reclose/retry idempotency).
     Soft-skips when the tenant hasn't set up reporting; raises
     `StatementStampError` (rolling back the close) when reporting is
     configured but the pivot fails. Runs after the draft→posted
     transition because the pivot reads posted entries only.

  Callers commit the session. One exception: the QB pre-publish step
  (2b, before any close mutation) commits its own qb_external_id
  markers — they record external writes that already happened and must
  survive a failed close (see `_publish_drafts_to_qb`). All failures
  raise domain exceptions that the REST / MCP caller translates into
  its native error format.

  ``statement_stamper`` is injectable for tests; the default resolves
  :func:`stamp_canonical_statement_sets` lazily so this module stays
  free of information-block imports at module load.
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
      allow_stranded_obligations=allow_stranded_obligations,
    )
    if not gate.is_closeable:
      raise CloseGateFailed(gate)

    period_start, period_end = period_date_range(period)

    # 2. Pre-flight BS balance check. Run against draft + posted together
    # so we know the ledger will still balance after the transition — no
    # state is mutated yet, so a mismatch just raises cleanly.
    self._preflight_bs_check(session, period_start, period_end)

    # 2b. Pre-publish step. For graphs with a QB connection in
    # `write_policy='qb_authoritative'` / `'hybrid'`, batch-publish every
    # in-period draft Entry whose triggering Event has
    # `source IN ('schedule', 'manual')`. Any QB rejection raises
    # `WritebackFailed` before the draft→posted transition below, so the
    # close itself never half-runs — but the step COMMITS the
    # qb_external_id markers for entries that did reach QB (see its
    # docstring), so a failed close never re-publishes them on retry.
    published_to_qb = self._publish_drafts_to_qb(
      session, graph_id, period_start, period_end, actor_id=actor_id
    )

    # The exclusive period fence (taken by `close_period` / `reopen_period`
    # around this call and their commit) is what serializes the whole
    # operation, including the QB publish above. That fence is
    # session-scoped on a dedicated connection because the publish
    # **commits** — a FOR UPDATE taken before it would be released.
    #
    # The row lock here is the second half: it serializes the database
    # close against anyone who skipped the fence, and it revalidates
    # status so a loser cannot treat `closed` as a normal first-close
    # and stamp again. Writers participate via the shared side of the
    # same fence in `assert_period_not_closed`.
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

    # 3. Draft → posted. The pre-publish step already promoted every
    # draft it published, so this bulk pass covers only what remains
    # (local-only drafts); the receipt's entries_posted is the sum of
    # both paths.
    now = datetime.now(UTC)
    posted_locally = (
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
    entries_posted = posted_locally + published_to_qb
    session.flush()

    # 4. Advance / reclose. Capture target before so we can report
    # auto-advance to the caller. This runs BEFORE the FiscalPeriod
    # transition below: on a never-closed calendar (closed_through IS
    # NULL) both `advance_closed_through`'s sequence check and
    # `is_latest_sequential_close` resolve the expected next close from
    # the earliest NON-closed FiscalPeriod — flipping this period's
    # status first would make that lookup land on the FOLLOWING month
    # and reject (or mis-route) every first close.
    cal_before = self._fcs.get(session, graph_id)
    target_before = cal_before.close_target_period if cal_before else None

    # Build an audit-friendly note. When the sync gate was overridden, we
    # record that explicitly so compliance audits can flag the close.
    effective_note = self._audit_note(
      note,
      allow_stale_sync=allow_stale_sync and has_sync_connection,
      stranded_overridden_count=(
        gate.stranded_obligation_count if allow_stranded_obligations else 0
      ),
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

    # 5. FiscalPeriod transition
    fp.status = "closed"
    fp.closed_at = now
    fp.closed_by = actor_id
    session.flush()

    # 5b. Canonical statement stamping — the close IS the act that
    # persists the month's statements. Replace semantics inside the
    # stamper make this idempotent for first-close, reclose, and
    # retry-after-failure; StatementStampError propagates so the whole
    # close (including the transitions above) rolls back rather than
    # leaving a closed month with no canonical sets.
    stamp = self._stamp_statement_sets(
      session,
      graph_id=graph_id,
      period_start=period_start,
      period_end=period_end,
      actor_id=actor_id,
    )

    # 6. Auto-run rules on schedules with facts in the closing period.
    # Rule failures are isolated from the close result: the close succeeds
    # even if a rule errors, and the failure surfaces only in rule_summary
    # / verification_results for downstream inspection. This keeps the
    # close path as the "single source of truth" while letting the
    # validation panel accumulate fresh results without an explicit
    # `POST /evaluate-rules` call.
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

    return PeriodCloseResult(
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

  # ── Private helpers ────────────────────────────────────────────────────

  def _stamp_statement_sets(
    self,
    session: Session,
    *,
    graph_id: str,
    period_start,
    period_end,
    actor_id: str,
  ) -> StatementStampResult:
    """Run the canonical statement stamper (injected or lazily resolved).

    Lazy import mirrors `_evaluate_schedule_rules_in_period` — the close
    service stays free of information-block/report dependencies at module
    import time, and mocked-session tests inject a stub stamper instead
    of exercising the pivot.
    """
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
    """Close-period pre-publish step. Returns the count published.

    For graphs with at least one QB connection in
    ``write_policy='qb_authoritative'`` / ``'hybrid'``, batch-publish
    every in-period draft Entry whose triggering Event has
    ``source IN ('schedule', 'manual')`` to that QB connection. Each
    successful publish also promotes its draft to posted, so the
    returned count feeds the close receipt's ``entries_posted`` total
    (the bulk transition that follows no longer sees these drafts).

    Any per-event QB rejection collects into a list and raises
    `WritebackFailed` BEFORE the close's draft→posted transition, so no
    close state mutates on failure. The operator fixes the offending
    entries (mapping issue, balance error, period closed in QB) and
    retries.

    NOT atomic with the close, deliberately: each successful publish
    stamps `Event.metadata['qb_external_id']` — the only dedupe marker —
    and those markers are COMMITTED at the end of this step, before any
    failure can roll them back. A QB JournalEntry is an external write
    that already happened; losing its marker to a rollback would make
    the retried close re-publish the same drafts as duplicates in the
    customer's QuickBooks. Retries skip already-marked events via
    `select_writeback_eligible_entries`.

    Skipped silently when:
    - No QB connection on the graph has qb_authoritative / hybrid policy
      (operator hasn't opted into write-back).
    - Period has no draft entries from RL-originated sources (no work
      to do — handler-approved drafts already wrote-back, or it's a
      native-only graph).
    """
    from robosystems.database import SessionFactory as _PlatformSessionFactory
    from robosystems.models.api.event_block import ExecuteEventBlockRequest
    from robosystems.operations.event_block.commands import execute_event_block

    from .qb_writeback import (
      resolve_writeback_connection,
      select_writeback_eligible_entries,
    )

    # Resolve the QB connection close publishes to. Shared with the
    # outbox read (`list_period_drafts`) so the preview of "what will
    # publish" can't drift from this actual write.
    with _PlatformSessionFactory() as platform_session:
      writeback = resolve_writeback_connection(platform_session, graph_id)
    if writeback is None:
      logger.debug(
        f"No qb_authoritative QB connection on graph {graph_id}; "
        f"skipping close-period pre-publish step"
      )
      return 0
    qb_connection_id = writeback.connection_id

    # In-period draft entries from RL-originated events not already in QB
    # (same predicate the outbox read previews — see qb_writeback.py).
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

    # Heads-up: each publish does a synchronous QB API round-trip
    # (~1-3s) inside the open extensions session. A large batch holds
    # the extensions transaction open + consumes connection-pool
    # capacity for the duration. Visible batches are flagged below so
    # the operational signal is in the logs.
    if len(drafts_to_publish) > 5:
      logger.warning(
        f"Graph {graph_id}: pre-publishing {len(drafts_to_publish)} drafts in "
        f"sequence — extensions transaction held open ~{len(drafts_to_publish) * 2}s "
        f"during the close. Consider batching the period (close in smaller "
        f"windows)."
      )

    # Publish each, collecting failures rather than failing fast — the
    # operator wants to see ALL offenders, not the first.
    failed_events: list[dict] = []
    for entry, event in drafts_to_publish:
      try:
        # Each publish gets its own SAVEPOINT. Without one, a *database* error
        # here — `execute_event_block` bounds its lock wait, so a timeout
        # surfaces as RowLockedError over an aborted transaction — poisons the
        # whole transaction. The loop would carry on collecting failures, every
        # later statement would fail on the aborted transaction, and the
        # `session.commit()` below would take the qb_external_id markers for
        # entries that DID reach QuickBooks down with it. QuickBooks would hold
        # journal entries the ledger has no record of sending, and the retried
        # close would publish them again once QB's RequestId window expired —
        # the precise duplication this function's commit exists to prevent.
        with session.begin_nested():
          result = execute_event_block(
            session,
            ExecuteEventBlockRequest(
              event_id=str(event.id),
              connection_id=qb_connection_id,
            ),
            created_by=actor_id,
          )
        if result.status == "pending":
          # QB rejected — collect for the batch error.
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
        # Unexpected (auth, network, ValueError). Capture and continue
        # so the operator sees the full failure surface.
        failed_events.append(
          {
            "event_id": str(event.id),
            "entry_id": str(entry.id),
            "memo": entry.memo,
            "posting_date": str(entry.posting_date),
            "qb_error": {"code": type(e).__name__, "message": str(e)},
          }
        )

    # Durability boundary: the qb_external_id markers written by the loop
    # record JournalEntries that now exist in QuickBooks, so they must
    # survive whatever happens to the rest of the close. This is the
    # close's first mutation point, and the tenant search_path is a plain
    # (non-LOCAL) SET that survives commit. Without this commit, a later
    # failure — the WritebackFailed below, a StatementStampError, a
    # failed final commit — would roll the markers back while the QB
    # writes stand, and the retried close would re-publish the same
    # drafts into QuickBooks as duplicates.
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
    the time this is called. The rule engine itself converts
    binding/dispatch failures into ``VerificationResult`` rows with
    ``status='error'``; this wrapper only guards against an outright
    engine exception.
    """
    # Lazy import — keeps the close service free of information-block
    # dependencies at module import time.
    from robosystems.operations.information_block.rules.engine import (
      evaluate_rules_for_structure,
    )

    # Unqualified table names — relies on `extensions_session(graph_id)`
    # having SET the tenant's search_path before this method runs. The
    # close service is only called from that session context (REST
    # handler + MCP tool both go through `cmd_close_period`); any
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
  def _audit_note(
    note: str | None,
    *,
    allow_stale_sync: bool,
    stranded_overridden_count: int = 0,
  ) -> str | None:
    """Annotate the audit note when a close gate was overridden.

    This ensures the `period_closed` event reflects that a human asserted
    "the data is complete despite the stale sync" — or knowingly closed
    over undrafted obligations — which matters for compliance review.
    """
    suffixes: list[str] = []
    if allow_stale_sync:
      suffixes.append("[sync gate overridden — allow_stale_sync=true]")
    if stranded_overridden_count > 0:
      suffixes.append(
        "[stranded-obligation gate overridden — "
        f"{stranded_overridden_count} undrafted obligation(s) omitted]"
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
