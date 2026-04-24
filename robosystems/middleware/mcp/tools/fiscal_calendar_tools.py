"""Fiscal calendar MCP tools for AI accounting close workflows.

Three tools that expose the fiscal calendar state machine to Claude:

1. get-fiscal-calendar — read current state (closed_through, close_target,
   gap, closeable_now, blockers)
2. close-period — the final commit action: atomically posts all drafts in
   the period, marks the period closed, advances closed_through, auto-advances
   close_target when reached
3. reopen-period — undo a prior close. Requires a reason for the audit log.

Initialize and set-close-target are deliberately NOT exposed as MCP tools:
initialize is a one-time onboarding operation done via the UI, and
set-close-target is a configuration action that normal close workflows
don't need (auto-advance handles it). Both are still available via REST.

All three tools route through `operations/roboledger/{reads,commands}/
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
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  PeriodNotClosedError,
  PeriodNotFoundInLedgerError,
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
- `last_sync_at`: most recent QB sync timestamp (null if no QB connection)
- `periods`: list of all fiscal period rows with status

**WORKFLOW:**
1. Call this first to orient yourself
2. If `closeable_now` is true, proceed with drafting
3. If blocked by `sync_stale`, tell user to sync QB first
4. If blocked by `period_incomplete`, tell user the month hasn't ended
5. If `gap_periods > 1`, the user is behind — acknowledge and plan a catch-up

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
6. Emits a period_closed audit event

**PARAMETERS:**
- period (required): YYYY-MM format (e.g., "2026-03")
- allow_stale_sync (optional): override the sync-current gate. Only use
  when the user has explicitly verified that QB data is complete despite
  a stale sync timestamp.

**RETURNS:**
- period: the period that was closed
- entries_posted: number of drafts transitioned to posted
- target_auto_advanced: true if close_target moved forward after this close
- fiscal_calendar: updated calendar state (same shape as get-fiscal-calendar)

**GUARDS (422 errors):**
- Cannot close out of sequence
- Cannot close the current (still-open) month
- Cannot close with a stale QB sync unless allow_stale_sync=true
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
          "target_auto_advanced": result.target_auto_advanced,
          "fiscal_calendar": fc_payload,
        }
    except CloseGateFailed as exc:
      if exc.no_calendar:
        return {
          "error": "calendar_not_initialized",
          "message": "Fiscal calendar not initialized for this graph.",
        }
      return {
        "error": "not_closeable",
        "message": f"Cannot close period {period!r}.",
        "blockers": exc.blockers,
      }
    except PeriodNotFoundError as exc:
      return {"error": "period_not_found", "message": str(exc)}
    except UnbalancedLedgerError as exc:
      return {
        "error": "unbalanced",
        "message": (
          f"Balance sheet equation broken for period {period!r}: "
          f"debits={exc.total_debit} credits={exc.total_credit}. "
          "Review the ledger before closing."
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
3. Does NOT modify close_target — that's a separate user decision
4. Does NOT modify existing posted entries — they stay posted
5. Emits a period_reopened audit event with the required reason

**PARAMETERS:**
- period (required): YYYY-MM format
- reason (required): Why the reopen is needed — captured in the audit log

**RETURNS:**
- Updated fiscal_calendar state

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
          fc_response = ops_reopen_period(
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
        fc_payload = fc_response.model_dump(mode="json")
        has_sync, _ = qb_sync_state(platform_db, graph_id)
        fc_payload["has_sync_connection"] = has_sync
        return {
          "period": period,
          "reason": reason,
          "fiscal_calendar": fc_payload,
        }
    except FiscalCalendarError as exc:
      return {"error": "calendar_error", "message": str(exc)}
    except Exception as exc:
      logger.warning(f"reopen-period failed: {exc}")
      return {"error": str(exc)}
