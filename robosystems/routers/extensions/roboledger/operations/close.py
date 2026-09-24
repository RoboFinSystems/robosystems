"""Month-end close and the schedules that drive it.

Schedule operations live here because promoting matured obligations is how a
schedule-driven close completes in a single session.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Path
from pydantic import ConfigDict, Field
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.db.extensions import extensions_session
from robosystems.middleware.extensions import OperationSpec
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.operations import (
  IdempotencyCache,
  OperationEnvelope,
  get_idempotency_cache,
)
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.models.api.common import OPERATION_ERROR_RESPONSES
from robosystems.models.api.extensions.fiscal_calendar import (
  BackfillPlanHistoryRequest,
  BackfillPlanHistoryResponse,
  ClosePeriodRequest,
  ClosePeriodResponse,
  FiscalCalendarResponse,
  PendingObligationDetailResponse,
  ReopenPeriodRequest,
  SetCloseTargetRequest,
)
from robosystems.models.api.extensions.schedules import (
  PromoteObligationsRequest,
  PromoteObligationsResponse,
  RebuildScheduleRequest,
  ScheduleCreatedResponse,
  TerminateScheduleRequest,
  TerminateScheduleResponse,
)
from robosystems.models.core import User
from robosystems.operations.locking import RowLockedError
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  BackfillPreconditionError,
  PeriodNotClosedError,
  PeriodNotFoundInLedgerError,
  ReopenOrderError,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  backfill_plan_history as cmd_backfill_plan_history,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  close_period as cmd_close_period,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  reopen_period as cmd_reopen_period,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  set_close_target as cmd_set_close_target,
)
from robosystems.operations.roboledger.commands.schedules import ScheduleNotFoundError
from robosystems.operations.roboledger.commands.schedules import (
  promote_obligations as cmd_promote_obligations,
)
from robosystems.operations.roboledger.commands.schedules import (
  rebuild_schedule as cmd_rebuild_schedule,
)
from robosystems.operations.roboledger.commands.schedules import (
  terminate_schedule as cmd_terminate_schedule,
)
from robosystems.operations.roboledger.fiscal_calendar import (
  CloseGateFailed,
  FiscalCalendarError,
  PeriodAlreadyClosedError,
  PeriodNotFoundError,
  UnbalancedLedgerError,
  parse_period,
)
from robosystems.operations.roboledger.fiscal_calendar.close_service import (
  WritebackFailed,
)
from robosystems.operations.roboledger.fiscal_calendar.service import (
  InvalidCloseTargetError,
)
from robosystems.operations.roboledger.reports.statement_sets import StatementStampError
from robosystems.routers.extensions.roboledger._common import (
  _RATE_LIMIT,
  _close_svc,
  _ctx,
  _dispatch,
  _fiscal_svc,
  _require_roboledger_write,
  make_registrar,
)

router = APIRouter()

_OP_TAG = "RoboLedger: Fiscal Close"
_registrar = make_registrar(router, _OP_TAG)


class SetCloseTargetOperation(SetCloseTargetRequest):
  pass  # `period` already in body


class ClosePeriodOperation(ClosePeriodRequest):
  """Close a single fiscal period. Carries the YYYY-MM `period` in the
  request body alongside the close-time options inherited from
  :class:`ClosePeriodRequest`.
  """

  period: str = Field(
    ...,
    pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
    description=(
      "Period to close, in YYYY-MM. Must be exactly `closed_through + 1` "
      "— close runs sequentially."
    ),
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"period": "2026-03"},
        {
          "period": "2026-03",
          "allow_stale_sync": True,
          "note": "QB sync down; data manually verified.",
        },
      ]
    },
  )


class ReopenPeriodOperation(ReopenPeriodRequest):
  """Reopen a closed fiscal period."""

  period: str = Field(
    ...,
    pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
    description=(
      "Period to reopen, in YYYY-MM. Any closed period may be reopened. "
      "Reopening the current `closed_through` retreats it by one month; "
      "reopening an earlier period leaves `closed_through` where it is "
      "(a prior-period adjustment), and its re-close restores the period "
      "without moving the pointer."
    ),
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "period": "2026-03",
          "reason": "Discovered late vendor invoice — needs to land in March.",
        },
      ]
    },
  )


class BackfillPlanHistoryOperation(BackfillPlanHistoryRequest):
  """Compile monthly statement history behind the close boundary."""

  pass  # range and chunking already in body


# ── Close Workflow ───────────────────────────────────────────────────────────


@router.post(
  "/set-close-target",
  response_model=OperationEnvelope[FiscalCalendarResponse],
  operation_id="setCloseTarget",
  summary="Set Close Target",
  description=(
    "Set the user-controlled goal period for closing (`close_target`). "
    "Format: YYYY-MM. Distinct from `closed_through` (what's actually "
    "locked) — setting a target doesn't close anything; call "
    "`close-period` for that. The catch-up sequence between "
    "`closed_through` and this target appears on the response's "
    "`fiscal_calendar.catch_up_sequence`."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/set-close-target",
  method="POST",
  business_event_type="ledger_set_close_target",
)
async def set_close_target_op(
  body: SetCloseTargetOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(_require_roboledger_write),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  platform_db: Session = Depends(get_db_session),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="set-close-target",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        return cmd_set_close_target(
          session,
          platform_db,
          graph_id,
          body.period,
          actor_id=str(user.id),
          note=body.note,
          service=_fiscal_svc,
        )
    except InvalidCloseTargetError as e:
      raise HTTPException(status_code=422, detail=str(e))
    except FiscalCalendarError as e:
      raise HTTPException(status_code=404, detail=str(e))
    except RowLockedError as e:
      # A close in flight holds the calendar row; retryable, like its siblings.
      raise HTTPException(status_code=409, detail=str(e))

  return await _dispatch(ctx, _runner, cache)


# Ledger writes (closing entries, reversals, schedule truncation, disposals)
# go through create-event-block with Python-registered event types.


@router.post(
  "/close-period",
  response_model=OperationEnvelope[ClosePeriodResponse],
  operation_id="closePeriod",
  summary="Close Fiscal Period",
  description=(
    "Lock a single fiscal period. Posts draft entries, runs the "
    "balance-sheet equation check, advances `closed_through` by one, "
    "auto-advances `close_target` if this close caught up to it, and "
    "stamps the period's canonical statement FactSets from the posted "
    "ledger (`statements_stamped` / `stamped_statement_sets` in the "
    "response; soft-skipped with `statement_stamp_note` when reporting "
    "isn't set up). Period must be exactly `closed_through + 1` — "
    "sequence violations return 422 with structured `blockers`. Common "
    "blockers: `sync_stale` (override with `allow_stale_sync=true` "
    "after manual verification), `period_incomplete` (draft entries "
    "unbalanced), `sequence_violation` (out-of-order)."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/close-period",
  method="POST",
  business_event_type="ledger_close_period",
)
async def close_period_op(
  body: ClosePeriodOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(_require_roboledger_write),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  platform_db: Session = Depends(get_db_session),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="close-period",
    idempotency_key=idempotency_key,
    body=body,
  )

  try:
    parse_period(body.period)
  except ValueError as e:
    raise HTTPException(status_code=422, detail=str(e))

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        return cmd_close_period(
          session,
          platform_db,
          graph_id,
          body.period,
          actor_id=str(user.id),
          allow_stale_sync=body.allow_stale_sync,
          note=body.note,
          service=_fiscal_svc,
          close_service=_close_svc,
          allow_stranded_obligations=body.allow_stranded_obligations,
          allow_reconciling_items=body.allow_reconciling_items,
        )
    except CloseGateFailed as e:
      if e.no_calendar:
        raise HTTPException(
          status_code=404,
          detail=(
            "Fiscal calendar not initialized. Call /operations/initialize first."
          ),
        )
      detail: dict = {
        "message": f"Cannot close period {body.period!r}.",
        "blockers": e.blockers,
      }
      if e.gate.pending_obligation_count:
        detail["pending_obligation_count"] = e.gate.pending_obligation_count
        # Via the response model so the shape tracks the calendar read.
        detail["pending_obligation_sample"] = [
          PendingObligationDetailResponse(
            event_id=d.event_id,
            schedule_id=d.schedule_id,
            schedule_name=d.schedule_name,
            period=d.period,
          ).model_dump()
          for d in e.gate.pending_obligation_sample
        ]
        detail["earliest_pending_period"] = e.gate.earliest_pending_period
      if e.gate.stranded_obligation_count:
        detail["stranded_obligation_count"] = e.gate.stranded_obligation_count
        detail["stranded_obligation_sample"] = [
          PendingObligationDetailResponse(
            event_id=d.event_id,
            schedule_id=d.schedule_id,
            schedule_name=d.schedule_name,
            period=d.period,
          ).model_dump()
          for d in e.gate.stranded_obligation_sample
        ]
      if e.gate.reconciling_item_count:
        detail["reconciling_item_count"] = e.gate.reconciling_item_count
        detail["reconciling_item_sample"] = list(e.gate.reconciling_item_sample)
      if e.gate.sync_stale_days is not None:
        detail["sync_stale_days"] = e.gate.sync_stale_days
      raise HTTPException(status_code=422, detail=detail)
    except PeriodNotFoundError as e:
      raise HTTPException(status_code=404, detail=str(e))
    except PeriodAlreadyClosedError as e:
      raise HTTPException(status_code=409, detail=str(e))
    except RowLockedError as e:
      raise HTTPException(status_code=409, detail=str(e))
    except UnbalancedLedgerError as e:
      raise HTTPException(
        status_code=422,
        detail=(
          f"Balance sheet equation broken for this period: "
          f"total debits={e.total_debit} total credits={e.total_credit}. "
          f"Difference={e.total_debit - e.total_credit}. "
          f"Review the ledger before closing."
        ),
      )
    except WritebackFailed as e:
      # Surface which drafts QB rejected so the operator can fix and retry.
      raise HTTPException(
        status_code=422,
        detail={
          "message": str(e),
          "failed_events": e.failed_events,
          "code": "WRITE_BACK_FAILED",
        },
      )
    except StatementStampError as e:
      # Rolled back, nothing committed: the pivot couldn't stamp the period's
      # statement sets. Fix mapping/style and re-run.
      raise HTTPException(
        status_code=422,
        detail={
          "message": str(e),
          "code": "STATEMENT_STAMP_FAILED",
        },
      )
    except FiscalCalendarError as e:
      raise HTTPException(status_code=404, detail=str(e))

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/reopen-period",
  response_model=OperationEnvelope[FiscalCalendarResponse],
  operation_id="reopenPeriod",
  summary="Reopen Fiscal Period",
  description=(
    "Reopen a closed period for adjustment. Only the latest closed period "
    "(`closed_through`) can be reopened; it decrements by one and the "
    "period's entries become writable again. To reach an earlier month, "
    "reopen latest-first down to it, then re-close forward — an "
    "out-of-order reopen is refused (422) with the ordered list, because "
    "every later closed month carries statements stamped from the earlier "
    "month's numbers. Retracts the month's canonical statement FactSets "
    "(a reopened month is no longer a closed assertion; re-closing "
    "restamps them). The required `reason` is captured in the audit log. "
    "Use sparingly — reopen invalidates downstream artifacts that trusted "
    "the closed state (reports, shared filings)."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/reopen-period",
  method="POST",
  business_event_type="ledger_reopen_period",
)
async def reopen_period_op(
  body: ReopenPeriodOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(_require_roboledger_write),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  platform_db: Session = Depends(get_db_session),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="reopen-period",
    idempotency_key=idempotency_key,
    body=body,
  )

  try:
    parse_period(body.period)
  except ValueError as e:
    raise HTTPException(status_code=422, detail=str(e))

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        # The envelope carries the refreshed calendar; the retraction count
        # surfaces on the MCP tool result only.
        return cmd_reopen_period(
          session,
          platform_db,
          graph_id,
          body.period,
          actor_id=str(user.id),
          reason=body.reason,
          note=body.note,
          service=_fiscal_svc,
        ).fiscal_calendar
    except RowLockedError as e:
      # A concurrent writer holds the rows; retryable, same 409 as the registrar.
      raise HTTPException(status_code=409, detail=str(e))
    except PeriodNotFoundInLedgerError:
      raise HTTPException(
        status_code=404, detail=f"Fiscal period {body.period!r} not found."
      )
    except PeriodNotClosedError as e:
      raise HTTPException(status_code=422, detail=str(e))
    except ReopenOrderError as e:
      raise HTTPException(status_code=422, detail=str(e))
    except FiscalCalendarError as e:
      raise HTTPException(status_code=404, detail=str(e))

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/backfill-plan-history",
  response_model=OperationEnvelope[BackfillPlanHistoryResponse],
  operation_id="backfillPlanHistory",
  summary="Backfill Plan History",
  description=(
    "Compile monthly statement history behind the close boundary — the "
    "plan's historical columns. Seeds any missing FiscalPeriod rows "
    "(baseline-closed) back to the clamped `start_period`, then "
    "restamps each month lacking canonical statement FactSets by "
    "running the real reopen → reclose cycle (balance validation, "
    "statement rules, and audit events per month). Chunked: at most "
    "`max_periods` months per call, oldest first — loop until "
    "`remaining_periods` comes back empty. Idempotent: already-stamped "
    "months are never touched. Months holding draft entries are "
    "skipped, never posted. `start_period` is clamped to the earliest "
    "month with ledger data, so deep-history tenants only backfill "
    "what actually exists."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/backfill-plan-history",
  method="POST",
  business_event_type="ledger_backfill_plan_history",
)
async def backfill_plan_history_op(
  body: BackfillPlanHistoryOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(_require_roboledger_write),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  platform_db: Session = Depends(get_db_session),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="backfill-plan-history",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        return cmd_backfill_plan_history(
          session,
          platform_db,
          graph_id,
          body,
          actor_id=str(user.id),
          service=_fiscal_svc,
          close_service=_close_svc,
        )
    except RowLockedError as e:
      # The internal reopen locks the period; another writer may hold it.
      raise HTTPException(status_code=409, detail=str(e))
    except BackfillPreconditionError as e:
      raise HTTPException(
        status_code=422,
        detail={"message": str(e), "code": e.code},
      )
    except FiscalCalendarError as e:
      raise HTTPException(status_code=404, detail=str(e))

  return await _dispatch(ctx, _runner, cache)


# The `scheduled_obligation_promoter` sensor's sweep, on demand, so a close can
# finish in one session. Idempotent.
promote_obligations_op = _registrar.register(
  OperationSpec(
    name="promote-obligations",
    summary="Promote Due Schedule Obligations",
    description=(
      "Promote matured pending schedule obligations (schedule_entry_due "
      "events whose period boundary has passed) to 'classified', and — when "
      "dispatch_handlers=true (default) — draft their closing entries in the "
      "same transaction. Also reaches stranded obligations: events already "
      "'classified' (by an earlier flip-only sweep) whose closing entry was "
      "never drafted are dispatched in the same pass, and reported via "
      "stranded_count. This is the on-demand form of the background "
      "obligation-promotion sweep; run it before close-period when a schedule "
      "was just created or when you can't wait for the Dagster sensor. "
      "Idempotent: re-running skips already-classified obligations and "
      "reconciles to existing drafts."
    ),
    command=cmd_promote_obligations,
    request_model=PromoteObligationsRequest,
    result_type=PromoteObligationsResponse,
    # The background sweep holds the candidate rows' locks. Retryable.
    error_map={ValueError: 422, RowLockedError: 409},
    mark_stale_reason="obligations_promoted",
  )
)


rebuild_schedule_op = _registrar.register(
  OperationSpec(
    name="rebuild-schedule",
    summary="Rebuild Schedule In Place",
    description=(
      "Re-run the schedule generator in place on an existing schedule. "
      "Atomic alternative to delete-then-recreate (which orphans pending "
      "obligations): preserves the structure id + element associations + "
      "taxonomy, voids the old pending obligation chain, deletes the old "
      "facts and SumEquals rules, and regenerates fresh forward facts + a "
      "fresh obligation chain from the schedule's stored definition "
      "(entry_template / schedule_metadata / monthly_amount / period "
      "bounds). The historical-vs-in-scope split is re-derived from the "
      "CURRENT fiscal calendar closed_through. Use this to pick up a fixed "
      "generator (e.g. the roll-forward direction fix) without orphaning "
      "obligations."
    ),
    command=cmd_rebuild_schedule,
    request_model=RebuildScheduleRequest,
    result_type=ScheduleCreatedResponse,
    error_map={
      ScheduleNotFoundError: 404,
      # The rebuild voids the schedule's pending obligations, which the
      # promotion sweep may be holding. Retryable.
      RowLockedError: 409,
      ValueError: 422,
    },
    mark_stale_reason="schedule_rebuilt",
  )
)


terminate_schedule_op = _registrar.register(
  OperationSpec(
    name="terminate-schedule",
    summary="Terminate Schedule Early",
    description=(
      "End a schedule early at a month-end cutoff without booking any "
      "entry. In one transaction: deletes forward facts past the cutoff "
      "(refusing when posted entries exist past it; stale drafts past it "
      "are deleted), voids the remaining obligation chain past the cutoff "
      "(pending and classified rows), and rewrites the SumEquals rule to "
      "prove the truncated curve. History at or before the cutoff is "
      "untouched, so open months the schedule still covers close "
      "normally. Use this when the termination's GL effect is already "
      "booked (an asset transferred via a manual entry, a prepaid "
      "refunded in the source system) or none is wanted; when the "
      "derecognition entry still needs to be booked, use "
      "create-event-block(event_type='asset_disposed') instead — the "
      "disposal handler posts it atomically with the same obligation "
      "void. Run BEFORE promote-obligations at close so terminated "
      "periods are never drafted."
    ),
    command=cmd_terminate_schedule,
    request_model=TerminateScheduleRequest,
    result_type=TerminateScheduleResponse,
    error_map={
      ScheduleNotFoundError: 404,
      # The void locks the rows the promotion sweep holds. Retryable.
      RowLockedError: 409,
      ValueError: 422,
    },
    mark_stale_reason="schedule_terminated",
  )
)
