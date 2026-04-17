"""Schedule MCP read tools for AI accounting close workflows.

Four read-side tools that complement the registrar-generated schedule
writes (`create-schedule`, `truncate-schedule`, `create-closing-entry`,
`create-manual-closing-entry`, `update-schedule`, `delete-schedule`):

1. list-schedule-structures: List active schedules with entry templates
2. get-schedule-facts: Get fact values for a schedule by period
3. get-period-close-status: Overview of what's done vs pending for a period
4. list-period-drafts: Review all draft entries for a period before close

Reads stay hand-written because they reshape the response for the
agent-friendly MCP wire format (e.g., folding totals into summary blocks).
All four route through `operations/roboledger/{reads}/schedules.py` and
`reads/period_drafts.py` so MCP, GraphQL, and REST read surfaces share
one source of truth.
"""

from datetime import date
from typing import Any

from robosystems.db.extensions import extensions_session
from robosystems.logger import logger
from robosystems.operations.roboledger.reads.period_drafts import list_period_drafts
from robosystems.operations.roboledger.reads.schedules import (
  get_period_close_status as ops_get_period_close_status,
)
from robosystems.operations.roboledger.reads.schedules import (
  get_schedule_facts as ops_get_schedule_facts,
)
from robosystems.operations.roboledger.reads.schedules import (
  list_schedules as ops_list_schedules,
)
from robosystems.operations.roboledger.schedules import ScheduleService

# ────────────────────────────────────────────────────────────────────────────
# list-schedule-structures
# ────────────────────────────────────────────────────────────────────────────


class ListScheduleStructuresTool:
  """List all active schedule structures with summary info."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "list-schedule-structures",
      "description": """List all active schedule structures for this graph.

**WHEN TO USE:**
- At the start of a month-end close to see what schedules exist
- To check which schedules have pending closing entries
- To understand the depreciation, amortization, and accrual schedules

**RETURNS:**
- Schedule name, taxonomy, entry template (debit/credit elements), metadata
- Total periods and how many already have closing entries

**WORKFLOW:**
1. Call this to see all schedules
2. Use get-period-close-status to check what's pending for a specific period
3. Use create-closing-entry to draft entries for pending schedules""",
      "inputSchema": {
        "type": "object",
        "properties": {},
        "required": [],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id

    try:
      with extensions_session(graph_id) as session:
        response = ops_list_schedules(session, ScheduleService())
        return {
          "schedule_count": len(response.schedules),
          "schedules": [s.model_dump(mode="json") for s in response.schedules],
        }
    except Exception as exc:
      logger.warning(f"list-schedule-structures failed: {exc}")
      return {"error": str(exc)}


# ────────────────────────────────────────────────────────────────────────────
# get-schedule-facts
# ────────────────────────────────────────────────────────────────────────────


class GetScheduleFactsTool:
  """Get facts for a schedule, optionally filtered by period."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-schedule-facts",
      "description": """Get the in-scope fact values for a schedule structure.

**WHEN TO USE:**
- To see the planned amounts for a specific schedule (e.g., monthly depreciation)
- To verify the amount before creating a closing entry
- To view the schedule across the in-scope reporting window

**NOTE:**
Only facts flagged as ``fact_scope='in_scope'`` are returned. Historical
facts (those that fell into an opening-balance window at schedule creation
time via ``closed_through``) are hidden, since they've already been
reflected in the ledger and shouldn't generate new closing entries.

**PARAMETERS:**
- structure_id (required): The schedule structure ID
- period_start / period_end (optional): Filter to a specific period

**RETURNS:**
- List of in-scope facts with element name, value (dollars), and period dates""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "structure_id": {
            "type": "string",
            "description": "Schedule structure ID",
          },
          "period_start": {
            "type": "string",
            "description": "Filter: period start date (YYYY-MM-DD)",
          },
          "period_end": {
            "type": "string",
            "description": "Filter: period end date (YYYY-MM-DD)",
          },
        },
        "required": ["structure_id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    structure_id = arguments["structure_id"]

    period_start = (
      date.fromisoformat(arguments["period_start"])
      if arguments.get("period_start")
      else None
    )
    period_end = (
      date.fromisoformat(arguments["period_end"])
      if arguments.get("period_end")
      else None
    )

    try:
      with extensions_session(graph_id) as session:
        response = ops_get_schedule_facts(
          session, ScheduleService(), structure_id, period_start, period_end
        )
        return {
          "structure_id": response.structure_id,
          "fact_count": len(response.facts),
          "facts": [f.model_dump(mode="json") for f in response.facts],
        }
    except Exception as exc:
      logger.warning(f"get-schedule-facts failed: {exc}")
      return {"error": str(exc)}


# ────────────────────────────────────────────────────────────────────────────
# get-period-close-status
# ────────────────────────────────────────────────────────────────────────────


class GetPeriodCloseStatusTool:
  """Overview of schedule close progress for a fiscal period."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-period-close-status",
      "description": """Get an overview of what schedule-derived work has been done for a fiscal period.

**WHEN TO USE:**
- At the start of a close session to see what's pending
- After creating entries to verify progress
- To generate a close summary for the user

**PARAMETERS:**
- period_start / period_end (required): The fiscal period dates

**RETURNS:**
- Period status (open/closed)
- List of schedules with their status (pending/drafted/posted) and amounts
- Count of draft and posted entries""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "period_start": {
            "type": "string",
            "description": "Fiscal period start date (YYYY-MM-DD)",
          },
          "period_end": {
            "type": "string",
            "description": "Fiscal period end date (YYYY-MM-DD)",
          },
        },
        "required": ["period_start", "period_end"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    period_start = date.fromisoformat(arguments["period_start"])
    period_end = date.fromisoformat(arguments["period_end"])

    try:
      with extensions_session(graph_id) as session:
        response = ops_get_period_close_status(
          session, ScheduleService(), period_start, period_end
        )
        return {
          "fiscal_period_start": response.fiscal_period_start.isoformat(),
          "fiscal_period_end": response.fiscal_period_end.isoformat(),
          "period_status": response.period_status,
          "schedules": {
            "total": len(response.schedules),
            "pending": sum(1 for s in response.schedules if s.status == "pending"),
            "drafted": response.total_draft,
            "posted": response.total_posted,
            "details": [s.model_dump(mode="json") for s in response.schedules],
          },
        }
    except Exception as exc:
      logger.warning(f"get-period-close-status failed: {exc}")
      return {"error": str(exc)}


# ────────────────────────────────────────────────────────────────────────────
# list-period-drafts
# ────────────────────────────────────────────────────────────────────────────


class ListPeriodDraftsTool:
  """List all draft entries in a fiscal period for review before close."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "list-period-drafts",
      "description": """List all draft closing entries in a fiscal period with full line item detail.

**WHEN TO USE:**
- After drafting closing entries, BEFORE calling close-period
- When the user asks "what's pending" or "show me the drafts"
- To review exactly what will be committed on close

**WORKFLOW:**
1. Draft entries via create-closing-entry (one per schedule)
2. Use this tool to review every draft with DR/CR detail
3. Summarize to the user — total debits/credits, balance check, per-schedule amounts
4. On user approval, call the close endpoint to commit + close atomically

**PARAMETERS:**
- period: YYYY-MM format (e.g., "2026-03")

**RETURNS:**
- draft_count, total_debit, total_credit, all_balanced
- drafts: full list with entry_id, posting_date, memo, source schedule name, line items (element name/code, debit/credit in cents), per-entry balance check

**NOTES:**
- Read-only — no side effects, safe to call repeatedly
- Returns an empty list if no drafts exist for the period
- Line amounts are in cents (divide by 100 for dollar display)""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "period": {
            "type": "string",
            "description": "Fiscal period in YYYY-MM format",
            "pattern": r"^\d{4}-(0[1-9]|1[0-2])$",
          },
        },
        "required": ["period"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    period = arguments["period"]

    try:
      with extensions_session(graph_id) as session:
        response = list_period_drafts(session, period)
        return response.model_dump(mode="json")
    except Exception as exc:
      logger.warning(f"list-period-drafts failed: {exc}")
      return {"error": str(exc)}
