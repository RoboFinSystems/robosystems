"""Period-workflow MCP read tools.

Two read-side tools covering the period-close workflow that spans
multiple Information Blocks:

1. get-period-close-status: Overview of what's done vs pending for a period
2. list-period-drafts: Review all draft entries for a period before close

Schedule-specific reads (``list-schedule-structures``,
``get-schedule-facts``) were retired in favour of the generic
Information Block reads — see ``information_block_tools.py``.
Schedule envelopes now surface through ``list-information-blocks``
with ``blockType="schedule"`` and ``get-information-block``. This
module stays scoped to tools that operate across blocks rather than
within one.

Writes (``create-schedule``, ``update-schedule``, ``delete-schedule``,
``truncate-schedule``) are registrar-generated from the roboledger
``OperationSpec`` declarations. Closing-entry drafting (both
schedule-derived and free-form manual) runs through
``create-event-block(event_type='schedule_entry_due' |
'manual_adjustment')``. The unified ``create-information-block`` /
``update-information-block`` / ``delete-information-block`` operations
dispatch the same underlying schedule commands via the block-type
registry.
"""

from datetime import date
from typing import Any

from robosystems.db.extensions import extensions_session
from robosystems.logger import logger
from robosystems.operations.roboledger.reads.period_drafts import list_period_drafts
from robosystems.operations.roboledger.reads.schedules import (
  get_period_close_status as ops_get_period_close_status,
)
from robosystems.operations.roboledger.schedules import ScheduleService

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
