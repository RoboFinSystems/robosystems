"""Period close / reopen endpoints.

`POST /periods/{period}/close` is the **final commit action** of the
month-end close workflow. In v1 (no QB writeback) it is synchronous:

  1. Validate closeable gates (sequence, period_complete, sync_current)
  2. Bulk-transition all draft entries with posting_date in the period
     to status='posted'
  3. Validate the BS equation balances for the period
  4. Transition FiscalPeriod → closed
  5. Advance closed_through; auto-advance close_target if reached
  6. Emit 'period_closed' audit event

When QB writeback lands, a writeback phase runs between (2) and (3), and the
entire operation becomes async.
"""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.db.extensions import extensions_session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.extensions.fiscal_calendar import (
  ClosePeriodRequest,
  ClosePeriodResponse,
  DraftEntryResponse,
  DraftLineItem,
  FiscalCalendarResponse,
  PeriodDraftsResponse,
  ReopenPeriodRequest,
)
from robosystems.models.core import User
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.fiscal_calendar import (
  FiscalCalendarService,
  next_period,
  parse_period,
  period_date_range,
)
from robosystems.operations.fiscal_calendar.service import (
  AdvanceSequenceError,
  CloseableGateResult,
  FiscalCalendarError,
)

# Reuse the response builder + sync state helper to avoid duplication
from robosystems.routers.ledger.fiscal_calendar import (
  _build_response,
  _get_qb_sync_state,
)

router = APIRouter()
_svc = FiscalCalendarService()


# ── Helpers ────────────────────────────────────────────────────────────────


PERIOD_PATH_PATTERN = r"^\d{4}-(0[1-9]|1[0-2])$"


def _ledger_404():
  return HTTPException(
    status_code=404,
    detail="Ledger not initialized. Connect a data source first.",
  )


# _get_qb_sync_state is imported from fiscal_calendar.py — it returns
# (has_connection, last_sync_at) so we can distinguish "no QB" from
# "QB connection exists but has never synced" (a blocking condition).


def _validate_bs_equation(session, graph_id: str, period_start, period_end) -> None:
  """Check that debits equal credits for all line items in the period.

  Raises HTTPException(422) if out of balance.
  """
  # Query total debits and credits across all line items whose parent entry
  # has a posting_date in the period. Works on the tenant schema.
  result = session.execute(
    text("""
      SELECT
        COALESCE(SUM(li.debit_amount), 0)  AS total_debit,
        COALESCE(SUM(li.credit_amount), 0) AS total_credit
      FROM line_items li
      JOIN entries e ON e.id = li.entry_id
      WHERE e.posting_date >= :period_start
        AND e.posting_date <= :period_end
        AND e.status = 'posted'
    """),
    {"period_start": period_start, "period_end": period_end},
  ).fetchone()

  total_debit = int(result.total_debit) if result else 0
  total_credit = int(result.total_credit) if result else 0
  if total_debit != total_credit:
    raise HTTPException(
      status_code=422,
      detail=(
        f"Balance sheet equation broken for this period: "
        f"total debits={total_debit} total credits={total_credit}. "
        f"Difference={total_debit - total_credit}. "
        f"Review the ledger before closing."
      ),
    )


# ── Endpoints ──────────────────────────────────────────────────────────────


@router.post(
  "/periods/{period}/close",
  response_model=ClosePeriodResponse,
  operation_id="closeFiscalPeriod",
  summary="Close Fiscal Period",
  tags=["Ledger"],
)
async def close_period(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  period: str = Path(
    ..., pattern=PERIOD_PATH_PATTERN, description="Target period in YYYY-MM format"
  ),
  body: ClosePeriodRequest = ClosePeriodRequest(),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  platform_db: Session = Depends(get_db_session),
):
  """Close a fiscal period — the final commit action.

  In a single transaction:
  1. Validates closeable gates
  2. Transitions all draft entries in the period to 'posted'
  3. Verifies BS equation balances
  4. Transitions the FiscalPeriod to 'closed'
  5. Advances closed_through; auto-advances close_target if reached

  This is synchronous in v1. With QB writeback (future) it will become async.
  """
  try:
    parse_period(period)  # validate shape early (redundant with path regex)
  except ValueError as e:
    raise HTTPException(status_code=422, detail=str(e))

  try:
    with extensions_session(graph_id) as session:
      # Gate check
      has_sync, last_sync_at = _get_qb_sync_state(platform_db, graph_id)
      gate = _svc.closeable_gate(
        session,
        graph_id,
        period,
        has_sync_connection=has_sync,
        last_sync_at=last_sync_at,
        allow_stale_sync=body.allow_stale_sync,
      )
      if not gate.is_closeable:
        if CloseableGateResult.NO_CALENDAR in gate.blockers:
          raise HTTPException(
            status_code=404,
            detail=(
              "Fiscal calendar not initialized. "
              "Call POST /v1/ledger/{graph_id}/initialize first."
            ),
          )
        raise HTTPException(
          status_code=422,
          detail={
            "message": f"Cannot close period {period!r}.",
            "blockers": gate.blockers,
          },
        )

      period_start, period_end = period_date_range(period)

      # Detect re-close of a previously reopened period BEFORE mutating state,
      # so we know whether to advance the closed_through cursor at the end.
      fp = (
        session.query(FiscalPeriod)
        .filter(FiscalPeriod.graph_id == graph_id, FiscalPeriod.name == period)
        .one_or_none()
      )
      if fp is None:
        raise HTTPException(
          status_code=404, detail=f"Fiscal period {period!r} not found."
        )
      is_reclose = fp.status == "closing"

      # Transition drafts to posted
      now = datetime.now(UTC)
      updated = (
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

      # Balance check on the now-posted ledger for this period
      _validate_bs_equation(session, graph_id, period_start, period_end)

      # Transition the FiscalPeriod itself
      fp.status = "closed"
      fp.closed_at = now
      fp.closed_by = current_user.id
      session.flush()

      cal_before = _svc.get(session, graph_id)
      target_before = cal_before.close_target_period  # type: ignore[union-attr]

      # Re-close routing:
      # - "Latest reopen" → reopen retreated closed_through, so this re-close
      #   must advance the pointer back to `period` exactly as a normal close
      #   does. Detection: period == next_period(current closed_through).
      # - "Older reopen" → reopen did NOT touch closed_through, so this
      #   re-close must NOT touch it either. We just record the close event
      #   without moving the cursor.
      if is_reclose:
        latest_reopen = (
          cal_before.closed_through_period is not None  # type: ignore[union-attr]
          and next_period(cal_before.closed_through_period) == period  # type: ignore[union-attr]
        ) or (
          cal_before.closed_through_period is None  # type: ignore[union-attr]
          and _svc._earliest_open_period(session, graph_id) == period
        )
      else:
        latest_reopen = False

      if is_reclose and not latest_reopen:
        # Older reopen — pointer unchanged, just audit the re-close.
        calendar = _svc.record_reclose(
          session,
          graph_id,
          period,
          actor_id=current_user.id,
          actor_type="user",
          note=body.note,
        )
      else:
        # Normal advance path — also handles latest-reopen re-close.
        calendar = _svc.advance_closed_through(
          session,
          graph_id,
          period,
          actor_id=current_user.id,
          actor_type="user",
          note=body.note,
        )
      target_auto_advanced = (
        target_before != calendar.close_target_period
        and calendar.close_target_period is not None
      )

      session.commit()

      return ClosePeriodResponse(
        fiscal_calendar=_build_response(
          session, graph_id, calendar, has_sync, last_sync_at
        ),
        period=period,
        entries_posted=updated,
        target_auto_advanced=target_auto_advanced,
      )

  except HTTPException:
    raise
  except AdvanceSequenceError as e:
    # Shouldn't normally reach here (gate check catches it) but just in case
    raise HTTPException(status_code=422, detail=str(e))
  except FiscalCalendarError as e:
    raise HTTPException(status_code=404, detail=str(e))
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"close_period failed: {e}")
    raise


@router.post(
  "/periods/{period}/reopen",
  response_model=FiscalCalendarResponse,
  operation_id="reopenFiscalPeriod",
  summary="Reopen Fiscal Period",
  tags=["Ledger"],
)
async def reopen_period(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  period: str = Path(
    ..., pattern=PERIOD_PATH_PATTERN, description="Period to reopen (YYYY-MM)"
  ),
  body: ReopenPeriodRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  platform_db: Session = Depends(get_db_session),
):
  """Reopen a closed fiscal period.

  Transitions the period status from 'closed' to 'closing' (drafts may still
  exist for this period after reopening). If the reopened period is the
  current `closed_through`, decrements the pointer. Requires a non-empty
  `reason` for the audit log. Does NOT modify `close_target` — that's a
  separate user decision.

  Posted entries in the reopened period stay posted. The user can then post
  additional adjustments, review, and close the period again.
  """
  try:
    with extensions_session(graph_id) as session:
      fp = (
        session.query(FiscalPeriod)
        .filter(FiscalPeriod.graph_id == graph_id, FiscalPeriod.name == period)
        .one_or_none()
      )
      if fp is None:
        raise HTTPException(
          status_code=404, detail=f"Fiscal period {period!r} not found."
        )
      if fp.status != "closed":
        raise HTTPException(
          status_code=422,
          detail=f"Period {period!r} is not closed (status={fp.status!r}).",
        )

      fp.status = "closing"
      fp.closed_at = None
      fp.closed_by = None
      session.flush()

      calendar = _svc.retreat_closed_through(
        session,
        graph_id,
        period,
        reason=body.reason,
        actor_id=current_user.id,
        actor_type="user",
        note=body.note,
      )

      session.commit()

      has_sync, last_sync_at = _get_qb_sync_state(platform_db, graph_id)
      return _build_response(session, graph_id, calendar, has_sync, last_sync_at)

  except HTTPException:
    raise
  except FiscalCalendarError as e:
    raise HTTPException(status_code=404, detail=str(e))
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"reopen_period failed: {e}")
    raise


@router.get(
  "/periods/{period}/drafts",
  response_model=PeriodDraftsResponse,
  operation_id="listPeriodDrafts",
  summary="List Draft Entries For Review",
  tags=["Ledger"],
)
async def list_period_drafts(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  period: str = Path(
    ..., pattern=PERIOD_PATH_PATTERN, description="Period in YYYY-MM format"
  ),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """List all draft entries in a fiscal period for review before close.

  Returns every draft entry whose `posting_date` falls within the period,
  fully expanded with line items, element names/codes, source schedule
  structure name, and per-entry balance check.

  Use this to review exactly what `close-period` will commit. Typical flow:

  1. Draft entries via `create-closing-entry` (one per schedule)
  2. Call this endpoint to review the full set
  3. Call `POST /periods/{period}/close` to atomically post + close

  This is a pure read — no side effects. It can be called repeatedly.
  """
  try:
    parse_period(period)
  except ValueError as e:
    raise HTTPException(status_code=422, detail=str(e))

  try:
    period_start, period_end = period_date_range(period)
    with extensions_session(graph_id) as session:
      # Pull draft entries + aggregated line items in a single query.
      # We fetch rows and group by entry_id to avoid N+1.
      rows = session.execute(
        text("""
          SELECT
            e.id               AS entry_id,
            e.posting_date     AS posting_date,
            e.type             AS entry_type,
            e.memo             AS memo,
            e.provenance       AS provenance,
            e.source_structure_id AS source_structure_id,
            s.name             AS source_structure_name,
            li.id              AS line_item_id,
            li.element_id      AS element_id,
            el.code            AS element_code,
            el.name            AS element_name,
            li.debit_amount    AS debit_amount,
            li.credit_amount   AS credit_amount,
            li.description     AS line_description
          FROM entries e
          LEFT JOIN structures s ON s.id = e.source_structure_id
          JOIN line_items li ON li.entry_id = e.id
          JOIN elements el ON el.id = li.element_id
          WHERE e.posting_date >= :period_start
            AND e.posting_date <= :period_end
            AND e.status = 'draft'
          ORDER BY e.posting_date, s.name NULLS LAST, e.id, li.line_order, li.id
        """),
        {"period_start": period_start, "period_end": period_end},
      ).fetchall()

      # Group line items under their parent entry
      by_entry: dict[str, dict] = {}
      for row in rows:
        entry_id = row.entry_id
        if entry_id not in by_entry:
          by_entry[entry_id] = {
            "entry_id": entry_id,
            "posting_date": row.posting_date,
            "type": row.entry_type,
            "memo": row.memo,
            "provenance": row.provenance,
            "source_structure_id": row.source_structure_id,
            "source_structure_name": row.source_structure_name,
            "line_items": [],
          }
        by_entry[entry_id]["line_items"].append(
          DraftLineItem(
            line_item_id=row.line_item_id,
            element_id=row.element_id,
            element_code=row.element_code,
            element_name=row.element_name,
            debit_amount=int(row.debit_amount or 0),
            credit_amount=int(row.credit_amount or 0),
            description=row.line_description,
          )
        )

      drafts: list[DraftEntryResponse] = []
      total_debit = 0
      total_credit = 0
      all_balanced = True
      for entry_data in by_entry.values():
        line_items = entry_data["line_items"]
        entry_debit = sum(li.debit_amount for li in line_items)
        entry_credit = sum(li.credit_amount for li in line_items)
        balanced = entry_debit == entry_credit
        if not balanced:
          all_balanced = False
        total_debit += entry_debit
        total_credit += entry_credit
        drafts.append(
          DraftEntryResponse(
            entry_id=entry_data["entry_id"],
            posting_date=entry_data["posting_date"],
            type=entry_data["type"],
            memo=entry_data["memo"],
            provenance=entry_data["provenance"],
            source_structure_id=entry_data["source_structure_id"],
            source_structure_name=entry_data["source_structure_name"],
            line_items=line_items,
            total_debit=entry_debit,
            total_credit=entry_credit,
            balanced=balanced,
          )
        )

      return PeriodDraftsResponse(
        period=period,
        period_start=period_start,
        period_end=period_end,
        draft_count=len(drafts),
        total_debit=total_debit,
        total_credit=total_credit,
        all_balanced=all_balanced,
        drafts=drafts,
      )

  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"list_period_drafts failed: {e}")
    raise
