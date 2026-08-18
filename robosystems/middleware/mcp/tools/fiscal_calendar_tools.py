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

from typing import Any

from robosystems.db.extensions import extensions_session
from robosystems.db.platform import platform_session as _platform_session
from robosystems.logger import logger
from robosystems.middleware.mcp.tools._gate import (
  MCPExtensionGateError,
  require_graph_extension_mcp,
)
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
  close_period as ops_close_period,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  reopen_period as ops_reopen_period,
)
from robosystems.operations.roboledger.fiscal_calendar import (
  CloseGateFailed,
  FiscalCalendarError,
  FiscalCalendarService,
  PeriodAlreadyClosedError,
  PeriodNotFoundError,
  UnbalancedLedgerError,
)
from robosystems.operations.roboledger.fiscal_calendar.close_service import (
  PeriodCloseService,
)
from robosystems.operations.roboledger.reads.fiscal_calendar import (
  build_fiscal_calendar_response,
  qb_sync_state,
)
from robosystems.operations.roboledger.reports.statement_sets import (
  StatementStampError,
)


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
6. If `gap_periods > 1`, the user is behind — acknowledge and plan a catch-up

**READ-ONLY**: safe to call repeatedly, no side effects.""",
      "inputSchema": {"type": "object", "properties": {}, "required": []},
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
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

**RETURNS:**
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
- Cannot close if BS equation doesn't balance for the period

**NOTES:**
- Posted entries cannot be re-drafted — use reopen-period to undo
- Manual entries (drafted via create-event-block event_type='journal_entry_recorded') are posted alongside schedule drafts
- After close, close_target auto-advances to the next period""",
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
          "note": {
            "type": "string",
            "description": "Optional note captured in the audit event",
          },
        },
        "required": ["period"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id

    try:
      require_graph_extension_mcp("roboledger", graph_id)
    except MCPExtensionGateError as exc:
      return {"error": exc.code, "message": exc.message}

    period = arguments["period"]
    allow_stale_sync = bool(arguments.get("allow_stale_sync", False))
    allow_stranded_obligations = bool(
      arguments.get("allow_stranded_obligations", False)
    )
    note = arguments.get("note")

    # Best-effort user identity from the graph client context; fall back to
    # a graph-scoped sentinel so audit logs stay traceable to the tenant.
    actor_id = getattr(self.client, "user_id", None) or f"mcp:{graph_id}"

    svc = FiscalCalendarService()
    close_svc = PeriodCloseService()

    try:
      with extensions_session(graph_id) as session, _platform_session() as platform_db:
        result = ops_close_period(
          session,
          platform_db,
          graph_id,
          period,
          actor_id=actor_id,
          allow_stale_sync=allow_stale_sync,
          note=note,
          service=svc,
          close_service=close_svc,
          actor_type="agent",
          allow_stranded_obligations=allow_stranded_obligations,
        )
        # ops_close_period commits the session internally.
        fc_payload = result.fiscal_calendar.model_dump(mode="json")
        # Re-derive `has_sync_connection` so the MCP wire shape stays the
        # same as before the refactor (the Pydantic response doesn't
        # carry it).
        has_sync, _ = qb_sync_state(platform_db, graph_id)
        fc_payload["has_sync_connection"] = has_sync
        return {
          "period": result.period,
          "entries_posted": result.entries_posted,
          "entries_published_to_qb": result.entries_published_to_qb,
          "entries_posted_locally": result.entries_posted_locally,
          "target_auto_advanced": result.target_auto_advanced,
          "fiscal_calendar": fc_payload,
          # Rule eval outcomes from the auto-run on close. Pairs with
          # the REST `ClosePeriodResponse` shape so agents and REST
          # consumers see the same surface.
          "rule_summary": result.rule_summary,
          "evaluated_structure_ids": list(result.evaluated_structure_ids),
          # Canonical statement stamping (the close-time pivot). Shaped
          # by hand here — without these keys agents never see that the
          # close persisted the month's statements.
          "statements_stamped": result.statements_stamped,
          "statement_stamp_note": result.statement_stamp_note,
          "stamped_statement_sets": dict(result.stamped_statement_sets),
          "statement_rule_summary": result.statement_rule_summary,
        }
    except CloseGateFailed as exc:
      if exc.no_calendar:
        return {
          "error": "calendar_not_initialized",
          "message": "Fiscal calendar not initialized for this graph.",
        }
      payload: dict = {
        "error": "not_closeable",
        "message": f"Cannot close period {period!r}.",
        "blockers": exc.blockers,
      }
      if exc.gate.pending_obligation_count:
        payload["pending_obligation_count"] = exc.gate.pending_obligation_count
        payload["pending_obligation_sample"] = [
          {
            "event_id": d.event_id,
            "schedule_id": d.schedule_id,
            "schedule_name": d.schedule_name,
            "period": d.period,
          }
          for d in exc.gate.pending_obligation_sample
        ]
        payload["earliest_pending_period"] = exc.gate.earliest_pending_period
      if exc.gate.stranded_obligation_count:
        payload["stranded_obligation_count"] = exc.gate.stranded_obligation_count
        payload["stranded_obligation_sample"] = [
          {
            "event_id": d.event_id,
            "schedule_id": d.schedule_id,
            "schedule_name": d.schedule_name,
            "period": d.period,
          }
          for d in exc.gate.stranded_obligation_sample
        ]
      if exc.gate.sync_stale_days is not None:
        payload["sync_stale_days"] = exc.gate.sync_stale_days
      return payload
    except PeriodNotFoundError as exc:
      return {"error": "period_not_found", "message": str(exc)}
    except PeriodAlreadyClosedError as exc:
      return {"error": "already_closed", "message": str(exc)}
    except RowLockedError as exc:
      return {"error": "row_locked", "message": str(exc)}
    except UnbalancedLedgerError as exc:
      return {
        "error": "unbalanced",
        "message": (
          f"Balance sheet equation broken for period {period!r}: "
          f"debits={exc.total_debit} credits={exc.total_credit}. "
          "Review the ledger before closing."
        ),
      }
    except StatementStampError as exc:
      return {
        "error": "statement_stamp_failed",
        "message": (
          f"{exc} The close rolled back — nothing was committed. Fix the "
          "mapping/reporting configuration and re-run close-period."
        ),
      }
    except FiscalCalendarError as exc:
      return {"error": "calendar_error", "message": str(exc)}
    except Exception as exc:
      logger.warning(f"close-period failed: {exc}")
      return {"error": str(exc)}


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

**WHAT IT DOES:**
1. Transitions FiscalPeriod from 'closed' → 'closing' (drafts may still exist)
2. If this was the latest closed period, decrements closed_through
3. Retracts the month's canonical statement FactSets (a reopened month is
   no longer a closed assertion; re-closing restamps them fresh)
4. Does NOT modify close_target — that's a separate user decision
5. Does NOT modify existing posted entries — they stay posted
6. Emits a period_reopened audit event with the required reason

**PARAMETERS:**
- period (required): YYYY-MM format
- reason (required): Why the reopen is needed — captured in the audit log

**RETURNS:**
- Updated fiscal_calendar state
- statement_sets_retracted: how many canonical statement FactSets the
  reopen deleted (0 for months closed before close-time stamping existed)

**GUARDS:**
- Period must be in 'closed' status (422 otherwise)
- Reason must be non-empty (422 otherwise)

**NOTES:**
- Posted entries stay posted. To "undo" a posted entry, create a reversing
  entry via create-event-block(event_type='journal_entry_recorded',
  metadata.type='reversing'), or reopen + correct + re-close.
- Reopening older periods (not the most recently closed) is allowed but
  does not decrement closed_through.""",
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
  months were closed before close-time statement stamping existed, or were
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

**IDEMPOTENT / SAFE:**
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
    except Exception as exc:
      logger.warning(f"backfill-plan-history failed: {exc}")
      return {"error": str(exc)}
