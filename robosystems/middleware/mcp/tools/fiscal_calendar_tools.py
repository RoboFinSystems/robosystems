"""Fiscal calendar MCP tools for AI accounting close workflows.

Four tools that expose the fiscal calendar state machine to Claude:

1. get-fiscal-calendar — read current state (closed_through, close_target,
   gap, closeable_now, blockers)
2. close-period — the final commit action: atomically posts all drafts in
   the period, marks the period closed, advances closed_through, auto-advances
   close_target when reached
3. reopen-period — undo a prior close. Requires a reason for the audit log.
4. backfill-plan-history — compile monthly statement history behind the
   close boundary (chunked reopen → reclose restamps, feeding the plan's
   historical columns).

Initialize and set-close-target are deliberately NOT exposed as MCP tools:
initialize is a one-time onboarding operation done via the UI, and
set-close-target is a configuration action that normal close workflows
don't need (auto-advance handles it). Both are still available via REST.

All tools route through `operations/roboledger/{reads,commands}/
fiscal_calendar.py` so MCP, GraphQL, and the REST operation surface
share one source of truth for both behavior and wire shape.
"""

import asyncio
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from robosystems.db.extensions import extensions_session
from robosystems.db.platform import platform_session as _platform_session
from robosystems.logger import logger
from robosystems.middleware.mcp.tools._gate import (
  MCPExtensionGateError,
  require_graph_extension_mcp,
)
from robosystems.middleware.operations import run_off_loop
from robosystems.models.api.extensions.fiscal_calendar import (
  BackfillPlanHistoryRequest,
)
from robosystems.operations.locking import RowLockedError
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  BackfillPreconditionError,
  PeriodNotClosedError,
  PeriodNotFoundInLedgerError,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  backfill_plan_history as ops_backfill_plan_history,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  reopen_period as ops_reopen_period,
)
from robosystems.operations.roboledger.fiscal_calendar import (
  CloseGateFailed,
  FiscalCalendarError,
  FiscalCalendarService,
)
from robosystems.operations.roboledger.fiscal_calendar.close_outcomes import (
  close_error_payload,
)
from robosystems.operations.roboledger.fiscal_calendar.close_service import (
  PeriodCloseService,
)
from robosystems.operations.roboledger.reads.fiscal_calendar import (
  build_fiscal_calendar_response,
  qb_sync_state,
)

from ._errors import database_failure


def _calendar_dict(session, graph_id: str, calendar, service) -> dict[str, Any]:
  """Build the MCP wire shape for the fiscal calendar.

  Wraps `build_fiscal_calendar_response` (the shared ops-layer assembler)
  and tacks on `has_sync_connection` — the only field the MCP tool
  surfaced that isn't on the Pydantic response model. Everything else
  flows through `model_dump(mode="json")` so date/time fields serialize
  as ISO-8601 strings, matching the original handcrafted shape.
  """
  with _platform_session() as platform_db:
    has_sync, last_sync_at = qb_sync_state(platform_db, graph_id)
  response = build_fiscal_calendar_response(
    session, graph_id, calendar, has_sync, last_sync_at, service
  )
  payload = response.model_dump(mode="json")
  payload["has_sync_connection"] = has_sync
  return payload


# ────────────────────────────────────────────────────────────────────────────
# get-fiscal-calendar
# ────────────────────────────────────────────────────────────────────────────


class GetFiscalCalendarTool:
  """Read the current fiscal calendar state for a graph."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-fiscal-calendar",
      "description": """Get the current fiscal calendar state — what period is next to close, what's blocking it.

**NEW TO THE CLOSE? Call `get-close-playbook` first** — it lays out the full tool sequence, the schedule-setup decisions, and the gotchas this tool's output assumes you know.

**WHEN TO USE:**
- At the START of every close session — check what period you're working on
- Before calling close-period — verify it will succeed (check `closeable_now`)
- To check catch-up state when a user is behind ("close the books — I'm 3 months behind")

**RETURNS:**
- `closed_through`: latest period actually closed (YYYY-MM) or null if never closed
- `close_target`: the period the user wants closed through (YYYY-MM)
- `gap_periods`: number of periods between closed_through and close_target
- `catch_up_sequence`: ordered list of periods a close run would process
- `closeable_now`: true if the next period can be closed right now
- `blockers`: list of blocker codes if closeable_now is false:
  - `sequence_violation`: trying to close a non-sequential period
  - `period_incomplete`: the month hasn't ended yet (current month)
  - `sync_stale`: QB sync is older than the period end
  - `calendar_not_initialized`: fiscal calendar not yet set up
  - `pending_obligations`: matured schedule obligations not yet promoted
    (count + sample ride on the response)
  - `stranded_obligations`: matured obligations already classified but
    with no drafted closing entry — adjusting entries a close would
    silently omit (count + sample ride on the response)
  - `reconciling_items`: posted events whose source payload changed
    afterwards and that nobody has dispositioned — resolve each with
    resolve-reconciling-item (count + sample ride on the response)
- `last_sync_at`: most recent QB sync timestamp (null if no QB connection)
- `periods`: list of all fiscal period rows with status

**WORKFLOW:**
1. Call this first to orient yourself
2. If `closeable_now` is true, proceed with drafting
3. If blocked by `sync_stale`, tell user to sync QB first
4. If blocked by `period_incomplete`, tell user the month hasn't ended
5. If blocked by `pending_obligations` or `stranded_obligations`, run
   promote-obligations (dispatch_handlers=true) — it reaches both — then
   re-check
5b. If blocked by `reconciling_items`, work each with
   preview-reconciling-item then resolve-reconciling-item, then re-check
6. If `gap_periods > 1`, the user is behind — acknowledge and plan a catch-up

**NOTES:** Read-only — safe to call repeatedly, no side effects.""",
      "inputSchema": {"type": "object", "properties": {}, "required": []},
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    svc = FiscalCalendarService()

    try:
      with extensions_session(graph_id) as session:
        calendar = svc.get(session, graph_id)
        if calendar is None:
          return {
            "error": "calendar_not_initialized",
            "message": (
              f"Fiscal calendar not initialized for graph {graph_id}. "
              "Use the initialize-ledger REST endpoint or UI to set it up first."
            ),
          }
        return _calendar_dict(session, graph_id, calendar, svc)
    except SQLAlchemyError as exc:
      return database_failure("get-fiscal-calendar", exc)
    except Exception as exc:
      logger.warning(f"get-fiscal-calendar failed: {exc}")
      return {"error": str(exc)}


# ────────────────────────────────────────────────────────────────────────────
# close-period
# ────────────────────────────────────────────────────────────────────────────


class ClosePeriodTool:
  """Close a fiscal period — the final commit action of the close workflow."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "close-period",
      "description": """Close a fiscal period — the FINAL commit action.

**WHEN TO USE:**
- After all closing entries are drafted AND reviewed via list-period-drafts
- After the user explicitly approves the drafts
- NEVER without calling list-period-drafts first and summarizing to the user

**WHAT IT DOES (atomic):**
1. Validates closeable gates (sequence, period complete, sync current)
2. Bulk-transitions all draft entries in the period to status='posted'
3. Validates the BS equation balances for the period
4. Transitions the FiscalPeriod from open → closed
5. Advances closed_through; auto-advances close_target if reached
6. Pivots the posted ledger and stamps the period's canonical statement
   FactSets (balance sheet / income statement / cash flow) — closing IS
   the act that persists the month's statements. Re-closing replaces
   them. Soft-skipped (statements_stamped=false with a note) when the
   tenant has no CoA mapping yet.
7. Auto-runs the rule engine for every schedule Structure with facts in
   the closed period, and the statement rule corpus against the stamped
   sets. Rule outcomes ride on the response as `rule_summary` /
   `evaluated_structure_ids` / `statement_rule_summary` so you can
   report which schedules and statements passed / failed to the user.
8. Emits a period_closed audit event

**PARAMETERS:**
- period (required): YYYY-MM format (e.g., "2026-03")
- allow_stale_sync (optional): override the sync-current gate. Only use
  when the user has explicitly verified that QB data is complete despite
  a stale sync timestamp.
- allow_stranded_obligations (optional): override the stranded-obligation
  gate — close even though matured classified obligations have no drafted
  closing entry, knowingly omitting those adjusting entries. Prefer
  promote-obligations (dispatch_handlers=true) or voiding them instead.
- allow_reconciling_items (optional): override the reconciling-item gate —
  close even though posted events in the period are still flagged as
  changed in the source system, leaving those differences undecided.
  Prefer resolve-reconciling-item on each first.

**RETURNS:**
Two shapes. Usually the receipt below, returned once the close lands. If the
close is still running when this call's budget runs out, you instead get
`status: "in_progress"` with an `operation_id`, `worker_started`, and
instructions — see WORKFLOW. That is not a failure and not a timeout: the
close is running on the worker and is atomic.

The receipt:
- period: the period that was closed
- entries_posted: TOTAL drafts transitioned to posted, across both post
  paths; entries_published_to_qb / entries_posted_locally carry the
  split (drafts published to QuickBooks are promoted at publish time,
  before the local bulk transition)
- target_auto_advanced: true if close_target moved forward after this close
- fiscal_calendar: updated calendar state (same shape as get-fiscal-calendar)
- rule_summary: aggregated rule-eval tally across every schedule Structure
  with facts in the closed period — keys: pass / fail / error / skipped.
  null when no schedules had facts in the period.
- evaluated_structure_ids: ids of schedule Structures whose rules were
  evaluated. Pairs with rule_summary.
- statements_stamped / statement_stamp_note: whether the close stamped
  the period's canonical statement FactSets (note carries the soft-skip
  reason, e.g. no_coa_mapping).
- stamped_statement_sets: structure_id -> fact_set_id for the minted
  canonical sets — use get-information-block to render them.
- statement_rule_summary: verification tally across the stamped
  statements (pass / fail / error / skipped); null when no rules exist.

**GUARDS (422 errors):**
- Cannot close out of sequence
- Cannot close the current (still-open) month
- Cannot close with a stale QB sync unless allow_stale_sync=true
- Cannot close over stranded obligations (matured, classified, but no
  drafted entry) unless allow_stranded_obligations=true — run
  promote-obligations (dispatch_handlers=true) to draft them first
- Cannot close over unresolved reconciling items (posted events whose
  source payload changed afterwards) unless allow_reconciling_items=true —
  preview-reconciling-item then resolve-reconciling-item on each first
- Cannot close if BS equation doesn't balance for the period

**WORKFLOW:**
1. Call close-period once. It dispatches the close to the worker and waits
   ~18s for it.
2. If you get the receipt, you are done — read entries_published_to_qb and
   statements_stamped to summarize for the user.
3. If you get `status: "in_progress"`, do NOT call close-period again. The
   close is already running and re-dispatching risks nothing useful. Poll
   get-period-close-status (period_status flips to closed, close_receipt
   fills in) or get-fiscal-calendar (has_close_receipt on the period row)
   every ~10s until it lands, then summarize from the receipt.
4. `worker_started: false` means the close is queued behind a busy worker
   rather than running yet. Same instruction — it will run.

**NOTES:**
- Posted entries cannot be re-drafted — use reopen-period to undo
- Manual entries (drafted via create-event-block event_type='journal_entry_recorded') are posted alongside schedule drafts
- Which drafts write back to QuickBooks follows the event's source (schedule/manual publish; system posts locally), unless metadata.publish_to_source overrides it per entry. list-period-drafts previews the split before you close
- After close, close_target auto-advances to the next period
- The close runs on the worker, so a slow QuickBooks publish can outlast
  this call. The work is unaffected by that — only who is waiting for it
- An identical close re-dispatched within 30s returns the same operation
  rather than starting a second one (`deduplicated: true`)""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "period": {
            "type": "string",
            "description": "Fiscal period in YYYY-MM format",
            "pattern": r"^\d{4}-(0[1-9]|1[0-2])$",
          },
          "allow_stale_sync": {
            "type": "boolean",
            "description": (
              "Override the sync-current gate (default false). Only set "
              "true when the user has verified QB data is complete."
            ),
          },
          "allow_stranded_obligations": {
            "type": "boolean",
            "description": (
              "Override the stranded-obligation gate (default false). "
              "Closes over matured classified obligations that have no "
              "drafted closing entry, knowingly omitting them; the "
              "override is recorded in the close audit note."
            ),
          },
          "allow_reconciling_items": {
            "type": "boolean",
            "description": (
              "Override the reconciling-item gate (default false). Closes "
              "over posted events still flagged as changed in the source "
              "system, leaving those differences undecided; the override "
              "is recorded in the close audit note."
            ),
          },
          "note": {
            "type": "string",
            "description": "Optional note captured in the audit event",
          },
        },
        "required": ["period"],
      },
    }

  # Polling budget for the dispatched close. The server cuts a tool call at
  # 25s, so this has to land inside that with room for the dispatch itself;
  # what is left over is the handback the operator reads.
  POLL_INTERVAL_S = 1.0
  POLL_BUDGET_S = 18.0

  async def execute(self, arguments: dict[str, Any]) -> Any:
    from robosystems.middleware.sse.event_storage import (
      OperationStatus,
      get_event_storage,
    )
    from robosystems.worker.client import enqueue_task

    graph_id = self.client.graph_id
    period = arguments["period"]

    gate = await run_off_loop(self._gate_check, arguments)
    if gate is not None:
      return gate

    # A real user id, not the graph-scoped sentinel the synchronous path
    # could fall back to: the operation is readable at
    # /v1/operations/{id}/status only by the user it was created for, so a
    # sentinel would enqueue work nobody could then look up.
    user_id = getattr(self.client, "user_id", None)
    if not user_id:
      return {
        "error": "authentication_required",
        "message": "User context required to close a period.",
      }

    try:
      dispatch = await enqueue_task(
        task_type="period_close",
        graph_id=graph_id,
        user_id=str(user_id),
        params={
          "period": period,
          "actor_id": str(user_id),
          "actor_type": "agent",
          "allow_stale_sync": bool(arguments.get("allow_stale_sync", False)),
          "allow_stranded_obligations": bool(
            arguments.get("allow_stranded_obligations", False)
          ),
          "allow_reconciling_items": bool(
            arguments.get("allow_reconciling_items", False)
          ),
          "note": arguments.get("note"),
        },
      )
    except Exception as exc:
      # Nothing was queued, which makes this the one failure here that is
      # safe to retry — and worth saying so, because every other message
      # this tool returns says the opposite.
      logger.warning("close-period could not dispatch: %s", exc, exc_info=True)
      return {
        "error": "dispatch_failed",
        "period": period,
        "message": (
          "The close could not be handed to the worker, so it has NOT "
          "started and nothing was written. Retry; if it keeps failing, "
          "the task queue is unavailable."
        ),
      }
    operation_id = dispatch["operation_id"]

    storage = get_event_storage()
    # Measured against the clock rather than by summing the intended sleeps:
    # the budget is "how long the caller has been waiting", and an interval
    # that rounds to nothing must still exhaust it rather than spin.
    loop = asyncio.get_running_loop()
    deadline = loop.time() + self.POLL_BUDGET_S
    started_at = loop.time()
    status = None
    while loop.time() < deadline:
      try:
        metadata = await storage.get_operation_metadata(operation_id)
      except Exception as exc:
        # The close is already running. Losing sight of it is a reporting
        # problem, not a reason to suggest doing it again — fall through to
        # the handback, which sends the operator to the period itself.
        logger.warning(
          "close-period lost track of operation %s: %s",
          operation_id,
          exc,
          exc_info=True,
        )
        break
      status = metadata.status if metadata else None
      if metadata and status == OperationStatus.COMPLETED:
        # The task already shaped this — return it as it stands so the
        # receipt reads the same whether the worker was fast or slow.
        return metadata.result_data or {}
      if metadata and status == OperationStatus.FAILED:
        return {
          "error": "close_failed",
          "operation_id": operation_id,
          "period": period,
          "message": (
            f"{metadata.error_message or 'The close failed.'} If it ran out of "
            "time rather than erroring, the close may still have landed — "
            "check get-period-close-status before retrying."
          ),
        }
      if metadata and status == OperationStatus.CANCELLED:
        return {
          "error": "cancelled",
          "operation_id": operation_id,
          "period": period,
          "message": "The close was cancelled.",
        }
      await asyncio.sleep(self.POLL_INTERVAL_S)

    started = status == OperationStatus.RUNNING
    return {
      "status": "in_progress",
      "operation_id": operation_id,
      "period": period,
      "worker_started": started,
      "deduplicated": bool(dispatch.get("deduplicated", False)),
      "waited_seconds": round(loop.time() - started_at, 1),
      "message": (
        (
          f"The close of {period} is running on the worker and is atomic — "
          "NEVER call close-period again for this period. "
        )
        if started
        else (
          f"The close of {period} is queued and has not started yet (the "
          "worker is busy or scaling). It will run — do not re-dispatch. "
        )
      )
      + "Poll get-period-close-status (period_status and close_receipt) or "
      "get-fiscal-calendar (has_close_receipt on the period) every ~10s "
      "until it lands.",
    }

  def _gate_check(self, arguments: dict[str, Any]) -> Any:
    """Answer an uncloseable period without burning a worker slot.

    The task re-runs this gate under the period fence, which is the
    authoritative check; this one only spares the operator a dispatch and a
    poll to be told something already knowable. A period that becomes
    uncloseable in between is caught there and comes back as a refusal.
    """
    graph_id = self.client.graph_id
    try:
      require_graph_extension_mcp("roboledger", graph_id)
    except MCPExtensionGateError as exc:
      return {"error": exc.code, "message": exc.message}

    period = arguments["period"]
    try:
      with extensions_session(graph_id) as session, _platform_session() as platform_db:
        has_sync, last_sync_at = qb_sync_state(platform_db, graph_id)
        gate = FiscalCalendarService().closeable_gate(
          session,
          graph_id,
          period,
          has_sync_connection=has_sync,
          last_sync_at=last_sync_at,
          allow_stale_sync=bool(arguments.get("allow_stale_sync", False)),
          allow_stranded_obligations=bool(
            arguments.get("allow_stranded_obligations", False)
          ),
          allow_reconciling_items=bool(arguments.get("allow_reconciling_items", False)),
        )
        if gate.is_closeable:
          return None
        return close_error_payload(CloseGateFailed(gate), period=period)
    except SQLAlchemyError as exc:
      return database_failure("close-period", exc)
    except FiscalCalendarError as exc:
      return {"error": "calendar_error", "message": str(exc)}


# ────────────────────────────────────────────────────────────────────────────
# reopen-period
# ────────────────────────────────────────────────────────────────────────────


class ReopenPeriodTool:
  """Reopen a closed fiscal period. Requires a reason for the audit log."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "reopen-period",
      "description": """Reopen a closed fiscal period so adjustments can be made.

**WHEN TO USE:**
- A missed entry was discovered after a period was closed
- A correcting entry is needed for a prior period adjustment
- The user wants to re-run the close workflow for a closed period

**NOTES:**
1. Transitions FiscalPeriod from 'closed' → 'closing' (drafts may still exist)
2. If this was the latest closed period, decrements closed_through
3. Retracts the month's canonical statement FactSets (a reopened month is
   no longer a closed assertion; re-closing restamps them fresh)
4. Does NOT modify close_target — that's a separate user decision
5. Does NOT modify existing posted entries — they stay posted
6. Emits a period_reopened audit event with the required reason
- Period must be in 'closed' status (422 otherwise)
- Reason must be non-empty (422 otherwise)
- Posted entries stay posted. To "undo" a posted entry, create a reversing
  entry via create-event-block(event_type='journal_entry_recorded',
  metadata.type='reversing'), or reopen + correct + re-close.
- Reopening older periods (not the most recently closed) is allowed but
  does not decrement closed_through.

**PARAMETERS:**
- period (required): YYYY-MM format
- reason (required): Why the reopen is needed — captured in the audit log

**RETURNS:**
- Updated fiscal_calendar state
- statement_sets_retracted: how many canonical statement FactSets the
  reopen deleted (0 for months whose statement sets were never stamped)
""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "period": {
            "type": "string",
            "description": "Period to reopen in YYYY-MM format",
            "pattern": r"^\d{4}-(0[1-9]|1[0-2])$",
          },
          "reason": {
            "type": "string",
            "description": "Required reason for the reopen (captured in audit log)",
            "minLength": 1,
          },
          "note": {
            "type": "string",
            "description": "Optional additional note",
          },
        },
        "required": ["period", "reason"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id

    try:
      require_graph_extension_mcp("roboledger", graph_id)
    except MCPExtensionGateError as exc:
      return {"error": exc.code, "message": exc.message}

    period = arguments["period"]
    reason = arguments.get("reason", "").strip()
    note = arguments.get("note")

    if not reason:
      return {
        "error": "missing_reason",
        "message": "Reopen requires a non-empty reason.",
      }

    actor_id = getattr(self.client, "user_id", None) or f"mcp:{graph_id}"
    svc = FiscalCalendarService()

    try:
      with extensions_session(graph_id) as session, _platform_session() as platform_db:
        try:
          result = ops_reopen_period(
            session,
            platform_db,
            graph_id,
            period,
            actor_id=actor_id,
            reason=reason,
            note=note,
            service=svc,
            actor_type="agent",
          )
        except PeriodNotFoundInLedgerError:
          return {
            "error": "period_not_found",
            "message": f"Fiscal period {period!r} not found.",
          }
        except PeriodNotClosedError as exc:
          return {
            "error": "not_closed",
            "message": f"Period {period!r} is not closed (status={exc.status!r}).",
          }
        except RowLockedError as exc:
          return {"error": "row_locked", "message": str(exc)}
        fc_payload = result.fiscal_calendar.model_dump(mode="json")
        has_sync, _ = qb_sync_state(platform_db, graph_id)
        fc_payload["has_sync_connection"] = has_sync
        return {
          "period": period,
          "reason": reason,
          "fiscal_calendar": fc_payload,
          # Canonical statement sets deleted by the reopen (a reopened
          # month is no longer a closed assertion; re-closing restamps).
          "statement_sets_retracted": result.statement_sets_retracted,
        }
    except FiscalCalendarError as exc:
      return {"error": "calendar_error", "message": str(exc)}
    except SQLAlchemyError as exc:
      return database_failure("reopen-period", exc)
    except Exception as exc:
      logger.warning(f"reopen-period failed: {exc}")
      return {"error": str(exc)}


# ────────────────────────────────────────────────────────────────────────────
# backfill-plan-history
# ────────────────────────────────────────────────────────────────────────────


class BackfillPlanHistoryTool:
  """Compile monthly statement history behind the close boundary."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "backfill-plan-history",
      "description": """Compile monthly statement history behind the close boundary — the plan's historical columns.

**WHEN TO USE:**
- The Plan page shows only annual (or missing) columns because historical
  months have no stamped statement sets — either never stamped, or
  baseline-closed at calendar initialization and never really closed
- A tenant with deep ledger history (QB sync) wants monthly statement
  columns further back than the calendar currently covers
- After onboarding: CoA mapping is done and the user wants their history
  compiled into the monthly statement series

**WHAT IT DOES (per call, oldest month first):**
1. Finds the earliest month with ledger data (the hard floor — a request
   can never reach past real data)
2. Seeds any missing FiscalPeriod rows back to the clamped start
   (baseline-closed)
3. For each month in range that lacks canonical statement FactSets, runs
   the REAL reopen → reclose cycle: balance validation, statement
   stamping, statement rules, and audit events — identical to a manual
   restamp
4. Stops after max_periods months and reports the rest in
   remaining_periods — call again to continue (chunked, resumable)

**NOTES:**
- Months that already have canonical statement sets are never touched
  (unless restamp=true — the deliberate healing pass that re-derives them)
- Months holding draft entries are SKIPPED, never posted — the backfill
  refuses to commit ledger changes nobody reviewed (resolve via
  list-period-drafts + close-period, then re-run)
- A failed reclose halts the run (no holes in the series); the failure
  rides that month's outcome
- The open month and close_target are never touched

**PARAMETERS:**
- start_period (optional): YYYY-MM to backfill from. Defaults to the
  earliest month with ledger data; clamped there when set earlier.
- max_periods (optional, default 12, max 24): months to restamp this call.
- allow_stale_sync (optional): override the sync gate on each reclose —
  rarely needed since historical months predate the last sync.
- allow_stranded_obligations (optional): override the stranded-obligation
  gate on each reclose — only when a matured classified obligation with
  no drafted entry sits inside the backfill window.
- allow_reconciling_items (optional): override the reconciling-item gate
  on each reclose — only when an event inside the backfill window is
  still flagged as changed upstream and you have decided not to resolve
  it first.
- restamp (optional, default false): also re-derive months that already
  have canonical sets — the healing pass after an engine improvement.
  Advance start_period between chunks (a restamp run is not
  self-resuming).
- note (optional): attached to each close audit event.

**RETURNS:**
- earliest_available_period / effective_start_period / closed_through:
  the resolved range
- period_rows_created: FiscalPeriod rows seeded for uncovered months
- processed: per-month outcomes (stamped | skipped_drafts | failed) with
  statement stamp + rule details
- remaining_periods: months still needing stamps — LOOP UNTIL EMPTY
- fiscal_calendar: refreshed calendar state

**WORKFLOW:**
1. Call with defaults (or a start_period the user chose)
2. Report per-month outcomes to the user
3. If remaining_periods is non-empty, call again
4. Verify in the Plan page / statement series when done""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "start_period": {
            "type": "string",
            "description": (
              "YYYY-MM to backfill from (default: earliest month with "
              "ledger data; clamped to it when earlier)"
            ),
            "pattern": r"^\d{4}-(0[1-9]|1[0-2])$",
          },
          "max_periods": {
            "type": "integer",
            "description": "Months to restamp in this call (default 12, max 24)",
            "minimum": 1,
            "maximum": 24,
          },
          "allow_stale_sync": {
            "type": "boolean",
            "description": (
              "Override the sync-currency gate on each reclose (default "
              "false). Rarely needed — historical months predate the last "
              "sync in the normal case."
            ),
          },
          "allow_reconciling_items": {
            "type": "boolean",
            "description": (
              "Override the reconciling-item gate on each reclose "
              "(default false). Only needed when an event inside the "
              "backfill window is still flagged as changed upstream."
            ),
          },
          "allow_stranded_obligations": {
            "type": "boolean",
            "description": (
              "Override the stranded-obligation gate on each reclose "
              "(default false). Only needed when a matured classified "
              "obligation without a drafted entry sits inside the "
              "backfill window."
            ),
          },
          "restamp": {
            "type": "boolean",
            "description": (
              "Also re-derive months that ALREADY have canonical statement "
              "sets (default false). The healing pass after an engine "
              "improvement changes what a stamp produces. NOT self-resuming "
              "— every month in range stays a candidate, so advance "
              "start_period between chunks instead of looping on "
              "remaining_periods."
            ),
          },
          "note": {
            "type": "string",
            "description": "Optional note attached to each close audit event",
          },
        },
        "required": [],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id

    try:
      require_graph_extension_mcp("roboledger", graph_id)
    except MCPExtensionGateError as exc:
      return {"error": exc.code, "message": exc.message}

    body = BackfillPlanHistoryRequest(
      start_period=arguments.get("start_period"),
      max_periods=int(arguments.get("max_periods", 12)),
      allow_stale_sync=bool(arguments.get("allow_stale_sync", False)),
      allow_stranded_obligations=bool(
        arguments.get("allow_stranded_obligations", False)
      ),
      allow_reconciling_items=bool(arguments.get("allow_reconciling_items", False)),
      restamp=bool(arguments.get("restamp", False)),
      note=arguments.get("note"),
    )
    actor_id = getattr(self.client, "user_id", None) or f"mcp:{graph_id}"

    svc = FiscalCalendarService()
    close_svc = PeriodCloseService(svc)

    try:
      with extensions_session(graph_id) as session, _platform_session() as platform_db:
        result = ops_backfill_plan_history(
          session,
          platform_db,
          graph_id,
          body,
          actor_id=actor_id,
          service=svc,
          close_service=close_svc,
          actor_type="agent",
        )
        fc_payload = result.fiscal_calendar.model_dump(mode="json")
        has_sync, _ = qb_sync_state(platform_db, graph_id)
        fc_payload["has_sync_connection"] = has_sync
        return {
          "earliest_available_period": result.earliest_available_period,
          "effective_start_period": result.effective_start_period,
          "closed_through": result.closed_through,
          "period_rows_created": result.period_rows_created,
          "processed": [
            outcome.model_dump(mode="json") for outcome in result.processed
          ],
          "remaining_periods": result.remaining_periods,
          "fiscal_calendar": fc_payload,
        }
    except BackfillPreconditionError as exc:
      return {"error": exc.code, "message": str(exc)}
    except FiscalCalendarError as exc:
      return {"error": "calendar_error", "message": str(exc)}
    except SQLAlchemyError as exc:
      return database_failure("backfill-plan-history", exc)
    except Exception as exc:
      logger.warning(f"backfill-plan-history failed: {exc}")
      return {"error": str(exc)}
