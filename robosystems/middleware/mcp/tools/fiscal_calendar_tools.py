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
"""

from datetime import datetime
from typing import Any

from robosystems.logger import logger


def _build_response_payload(session, graph_id: str, calendar) -> dict[str, Any]:
  """Assemble a fiscal-calendar state dict identical to the REST response.

  Keeps the response shape consistent with GET /v1/ledger/{graph_id}/fiscal-calendar
  so Claude sees the same fields whether it called via MCP or REST.
  """
  from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
  from robosystems.operations.roboledger.fiscal_calendar import FiscalCalendarService

  svc = FiscalCalendarService()
  periods = (
    session.query(FiscalPeriod)
    .filter(FiscalPeriod.graph_id == graph_id)
    .order_by(FiscalPeriod.start_date)
    .all()
  )

  catch_up = svc.catch_up_sequence(calendar, session=session, graph_id=graph_id)
  next_to_close = catch_up[0] if catch_up else None
  gate = None
  has_sync, last_sync_at = _get_qb_sync_state(graph_id)
  if next_to_close is not None:
    gate = svc.closeable_gate(
      session,
      graph_id,
      next_to_close,
      has_sync_connection=has_sync,
      last_sync_at=last_sync_at,
    )

  return {
    "graph_id": graph_id,
    "fiscal_year_start_month": calendar.fiscal_year_start_month,
    "closed_through": calendar.closed_through_period,
    "close_target": calendar.close_target_period,
    "gap_periods": len(catch_up),
    "catch_up_sequence": catch_up,
    "closeable_now": gate.is_closeable if gate else False,
    "blockers": gate.blockers if gate else [],
    "last_close_at": calendar.last_close_at.isoformat()
    if calendar.last_close_at
    else None,
    "initialized_at": calendar.initialized_at.isoformat()
    if calendar.initialized_at
    else None,
    "has_sync_connection": has_sync,
    "last_sync_at": last_sync_at.isoformat() if last_sync_at else None,
    "periods": [
      {
        "name": p.name,
        "start_date": str(p.start_date),
        "end_date": str(p.end_date),
        "status": p.status,
        "closed_at": p.closed_at.isoformat() if p.closed_at else None,
      }
      for p in periods
    ],
  }


def _get_qb_sync_state(graph_id: str) -> tuple[bool, datetime | None]:
  """Look up QB connection state from the platform DB.

  Returns (has_connection, last_sync_at). A connection that exists but has
  never synced returns (True, None) — the close gate treats this as stale
  and blocks. No connection at all returns (False, None) which passes the gate.
  """
  from robosystems.database import get_db_session
  from robosystems.models.core.connection.connection import Connection

  db_gen = get_db_session()
  db = next(db_gen)
  try:
    # A graph can have multiple QB connection rows (disconnected/old/new).
    # Prefer a currently-connected one; fall back to the most recently updated.
    # `.first()` (not `.one_or_none()`) avoids a MultipleResultsFound crash.
    conn = (
      db.query(Connection)
      .filter(Connection.graph_id == graph_id, Connection.provider == "quickbooks")
      .order_by(
        (Connection.status == "connected").desc(),
        Connection.updated_at.desc(),
      )
      .first()
    )
    if conn is None:
      return (False, None)
    return (True, conn.last_sync)
  finally:
    db_gen.close()


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
    from robosystems.db.extensions import extensions_session
    from robosystems.operations.roboledger.fiscal_calendar import FiscalCalendarService

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
        return _build_response_payload(session, graph_id, calendar)
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
- Manual entries (from create-manual-closing-entry) are posted alongside schedule drafts
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
    from robosystems.db.extensions import extensions_session
    from robosystems.operations.roboledger.fiscal_calendar import (
      CloseGateFailed,
      PeriodCloseService,
      PeriodNotFoundError,
      UnbalancedLedgerError,
    )
    from robosystems.operations.roboledger.fiscal_calendar.service import (
      FiscalCalendarError,
    )

    graph_id = self.client.graph_id
    period = arguments["period"]
    allow_stale_sync = bool(arguments.get("allow_stale_sync", False))
    note = arguments.get("note")

    # Best-effort user identity from the graph client context; fall back to
    # a graph-scoped sentinel so audit logs stay traceable to the tenant.
    # Matches the format used by `ReopenPeriodTool` below.
    actor_id = getattr(self.client, "user_id", None) or f"mcp:{graph_id}"

    close_svc = PeriodCloseService()

    try:
      with extensions_session(graph_id) as session:
        has_sync, last_sync_at = _get_qb_sync_state(graph_id)
        result = close_svc.close(
          session,
          graph_id,
          period,
          actor_id=actor_id,
          actor_type="agent",
          has_sync_connection=has_sync,
          last_sync_at=last_sync_at,
          allow_stale_sync=allow_stale_sync,
          note=note,
        )
        session.commit()

        return {
          "period": result.period,
          "entries_posted": result.entries_posted,
          "target_auto_advanced": result.target_auto_advanced,
          "fiscal_calendar": _build_response_payload(
            session, graph_id, result.calendar
          ),
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
  entry via create-manual-closing-entry, or reopen + correct + re-close.
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
    from robosystems.db.extensions import extensions_session
    from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
    from robosystems.operations.roboledger.fiscal_calendar import FiscalCalendarService
    from robosystems.operations.roboledger.fiscal_calendar.service import (
      FiscalCalendarError,
    )

    graph_id = self.client.graph_id
    period = arguments["period"]
    reason = arguments.get("reason", "").strip()
    note = arguments.get("note")

    if not reason:
      return {
        "error": "missing_reason",
        "message": "Reopen requires a non-empty reason.",
      }

    # Best-effort user identity from the graph client context; fall back to
    # a graph-scoped sentinel so audit logs stay traceable to the tenant.
    # Matches the pattern used by `ClosePeriodTool` above.
    actor_id = getattr(self.client, "user_id", None) or f"mcp:{graph_id}"

    svc = FiscalCalendarService()

    try:
      with extensions_session(graph_id) as session:
        fp = (
          session.query(FiscalPeriod)
          .filter(FiscalPeriod.graph_id == graph_id, FiscalPeriod.name == period)
          .one_or_none()
        )
        if fp is None:
          return {
            "error": "period_not_found",
            "message": f"Fiscal period {period!r} not found.",
          }
        if fp.status != "closed":
          return {
            "error": "not_closed",
            "message": f"Period {period!r} is not closed (status={fp.status!r}).",
          }

        fp.status = "closing"
        fp.closed_at = None
        fp.closed_by = None
        session.flush()

        calendar = svc.retreat_closed_through(
          session,
          graph_id,
          period,
          reason=reason,
          actor_id=actor_id,
          actor_type="agent",
          note=note,
        )
        session.commit()

        return {
          "period": period,
          "reason": reason,
          "fiscal_calendar": _build_response_payload(session, graph_id, calendar),
        }
    except FiscalCalendarError as exc:
      return {"error": "calendar_error", "message": str(exc)}
    except Exception as exc:
      logger.warning(f"reopen-period failed: {exc}")
      return {"error": str(exc)}
