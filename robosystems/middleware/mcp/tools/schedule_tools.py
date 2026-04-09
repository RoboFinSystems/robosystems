"""Schedule MCP tools for AI accounting close workflows.

Five tools for schedule management:
1. create-schedule: Create a new schedule with pre-generated monthly facts
2. list-schedule-structures: List active schedules with entry templates
3. get-schedule-facts: Get fact values for a schedule by period
4. get-period-close-status: Overview of what's done vs pending for a period
5. create-closing-entry: Draft a closing entry from schedule facts
"""

from typing import Any

from robosystems.logger import logger


class CreateScheduleTool:
  """Create a new depreciation/amortization/accrual schedule."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "create-schedule",
      "description": """Create a new schedule with pre-generated monthly facts.

**WHEN TO USE:**
- When the user asks to set up depreciation for an asset
- When the user wants to create an amortization or accrual schedule
- When setting up recurring monthly entries that follow a straight-line pattern

**WORKFLOW:**
1. Identify the relevant elements (e.g., Depreciation Expense and Accumulated Depreciation)
   - Use resolve-element to find the correct element IDs
2. Determine the schedule parameters from the user (amount, start/end dates, useful life)
3. Call this tool to create the schedule
4. The schedule pre-generates facts for each monthly period
5. Use create-closing-entry each month to draft journal entries from the schedule

**PARAMETERS:**
- name: Descriptive name (e.g., "Office Furniture Depreciation")
- element_ids: The element IDs involved (debit + credit elements)
- period_start / period_end: Date range for the schedule
- monthly_amount: Amount per month in cents (e.g., 41667 for $416.67)
- debit_element_id / credit_element_id: Elements for the closing entry template
- entry_type: Usually "closing" (default)
- memo_template: Template with {structure_name} placeholder
- Optional metadata: method, original_amount, residual_value, useful_life_months, asset_element_id

**RETURNS:**
- structure_id, name, taxonomy_id, message""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "name": {
            "type": "string",
            "description": "Schedule name (e.g., 'Office Furniture Depreciation')",
          },
          "element_ids": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Element IDs to include (debit + credit elements)",
          },
          "period_start": {
            "type": "string",
            "description": "First period start date (YYYY-MM-DD)",
          },
          "period_end": {
            "type": "string",
            "description": "Last period end date (YYYY-MM-DD)",
          },
          "monthly_amount": {
            "type": "integer",
            "description": "Monthly amount in cents (e.g., 41667 for $416.67)",
          },
          "debit_element_id": {
            "type": "string",
            "description": "Element to debit (e.g., Depreciation Expense)",
          },
          "credit_element_id": {
            "type": "string",
            "description": "Element to credit (e.g., Accumulated Depreciation)",
          },
          "entry_type": {
            "type": "string",
            "description": "Entry type for generated entries (default: 'closing')",
          },
          "memo_template": {
            "type": "string",
            "description": "Memo template — {structure_name} is replaced with schedule name",
          },
          "method": {
            "type": "string",
            "description": "Calculation method (default: 'straight_line')",
          },
          "original_amount": {
            "type": "integer",
            "description": "Cost basis in cents",
          },
          "residual_value": {
            "type": "integer",
            "description": "Salvage value in cents (default: 0)",
          },
          "useful_life_months": {
            "type": "integer",
            "description": "Useful life in months",
          },
          "asset_element_id": {
            "type": "string",
            "description": "BS asset element for net book value tracking",
          },
          "auto_reverse": {
            "type": "boolean",
            "description": "If true, closing entries auto-generate a reversing entry on the first day of the next period (default: false). Use for accruals that need to reverse.",
          },
        },
        "required": [
          "name",
          "element_ids",
          "period_start",
          "period_end",
          "monthly_amount",
          "debit_element_id",
          "credit_element_id",
        ],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    from datetime import date

    from robosystems.db.extensions import extensions_session
    from robosystems.operations.schedules import ScheduleService
    from robosystems.operations.schedules.service import EntryTemplate, ScheduleMetadata

    graph_id = self.client.graph_id
    svc = ScheduleService()

    entry_template = EntryTemplate(
      debit_element_id=arguments["debit_element_id"],
      credit_element_id=arguments["credit_element_id"],
      entry_type=arguments.get("entry_type", "closing"),
      memo_template=arguments.get("memo_template", "Monthly {structure_name}"),
      auto_reverse=arguments.get("auto_reverse", False),
    )

    schedule_metadata = None
    if any(
      arguments.get(k) is not None
      for k in (
        "method",
        "original_amount",
        "residual_value",
        "useful_life_months",
        "asset_element_id",
      )
    ):
      schedule_metadata = ScheduleMetadata(
        method=arguments.get("method", "straight_line"),
        original_amount=arguments.get("original_amount", 0),
        residual_value=arguments.get("residual_value", 0),
        useful_life_months=arguments.get("useful_life_months", 0),
        asset_element_id=arguments.get("asset_element_id"),
      )

    try:
      with extensions_session(graph_id) as session:
        structure = svc.create_schedule(
          session,
          name=arguments["name"],
          taxonomy_id=None,
          element_ids=arguments["element_ids"],
          period_start=date.fromisoformat(arguments["period_start"]),
          period_end=date.fromisoformat(arguments["period_end"]),
          monthly_amount=arguments["monthly_amount"],
          entry_template=entry_template,
          schedule_metadata=schedule_metadata,
          created_by=f"mcp:{graph_id}",
        )
        session.commit()

        return {
          "success": True,
          "structure_id": structure.id,
          "name": structure.name,
          "taxonomy_id": structure.taxonomy_id,
          "message": f"Schedule '{arguments['name']}' created successfully.",
        }
    except Exception as exc:
      logger.warning("create-schedule failed: %s", exc)
      return {"error": str(exc)}


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
    from robosystems.db.extensions import extensions_session
    from robosystems.operations.schedules import ScheduleService

    graph_id = self.client.graph_id
    svc = ScheduleService()

    try:
      with extensions_session(graph_id) as session:
        summaries = svc.list_schedules(session)
        return {
          "schedule_count": len(summaries),
          "schedules": [
            {
              "structure_id": s.structure_id,
              "name": s.name,
              "taxonomy_name": s.taxonomy_name,
              "entry_template": s.entry_template,
              "schedule_metadata": s.schedule_metadata,
              "total_periods": s.total_periods,
              "periods_with_entries": s.periods_with_entries,
            }
            for s in summaries
          ],
        }
    except Exception as exc:
      logger.warning(f"list-schedule-structures failed: {exc}")
      return {"error": str(exc)}


class GetScheduleFactsTool:
  """Get facts for a schedule, optionally filtered by period."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-schedule-facts",
      "description": """Get the fact values for a schedule structure.

**WHEN TO USE:**
- To see the planned amounts for a specific schedule (e.g., monthly depreciation)
- To verify the amount before creating a closing entry
- To view the full schedule across all periods

**PARAMETERS:**
- structure_id (required): The schedule structure ID
- period_start / period_end (optional): Filter to a specific period

**RETURNS:**
- List of facts with element name, value (dollars), and period dates""",
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
    from datetime import date

    from robosystems.db.extensions import extensions_session
    from robosystems.operations.schedules import ScheduleService

    graph_id = self.client.graph_id
    svc = ScheduleService()
    structure_id = arguments["structure_id"]

    period_start = None
    period_end = None
    if arguments.get("period_start"):
      period_start = date.fromisoformat(arguments["period_start"])
    if arguments.get("period_end"):
      period_end = date.fromisoformat(arguments["period_end"])

    try:
      with extensions_session(graph_id) as session:
        facts = svc.get_schedule_facts(session, structure_id, period_start, period_end)
        return {
          "structure_id": structure_id,
          "fact_count": len(facts),
          "facts": [
            {
              "element_id": f.element_id,
              "element_name": f.element_name,
              "value": f.value,
              "period_start": str(f.period_start),
              "period_end": str(f.period_end),
            }
            for f in facts
          ],
        }
    except Exception as exc:
      logger.warning(f"get-schedule-facts failed: {exc}")
      return {"error": str(exc)}


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
    from datetime import date

    from robosystems.db.extensions import extensions_session
    from robosystems.operations.schedules import ScheduleService

    graph_id = self.client.graph_id
    svc = ScheduleService()
    period_start = date.fromisoformat(arguments["period_start"])
    period_end = date.fromisoformat(arguments["period_end"])

    try:
      with extensions_session(graph_id) as session:
        status = svc.get_period_close_status(session, period_start, period_end)
        return {
          "fiscal_period_start": str(status.fiscal_period_start),
          "fiscal_period_end": str(status.fiscal_period_end),
          "period_status": status.period_status,
          "schedules": {
            "total": len(status.schedules),
            "pending": sum(1 for s in status.schedules if s.status == "pending"),
            "drafted": status.total_draft,
            "posted": status.total_posted,
            "details": [
              {
                "structure_id": s.structure_id,
                "structure_name": s.structure_name,
                "amount": s.amount,
                "status": s.status,
                "entry_id": s.entry_id,
                "reversal_entry_id": s.reversal_entry_id,
                "reversal_status": s.reversal_status,
              }
              for s in status.schedules
            ],
          },
        }
    except Exception as exc:
      logger.warning(f"get-period-close-status failed: {exc}")
      return {"error": str(exc)}


class CreateClosingEntryTool:
  """Create a draft closing entry from a schedule's facts for a period."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "create-closing-entry",
      "description": """Create a draft closing entry from a schedule structure's facts for a given period.

**WHEN TO USE:**
- During month-end close, for each pending schedule
- After verifying the schedule amount with get-schedule-facts

**BEHAVIOR:**
1. Looks up the schedule's entry template (debit/credit elements)
2. Finds the fact for the debit element in the specified period
3. Creates a draft Entry with balanced DR/CR line items
4. Sets source_structure_id for audit provenance

**GUARDS:**
- Fails if an entry already exists for this schedule + period (idempotent)
- Creates draft only — never auto-posts
- Fails if no fact exists for the requested period

**PARAMETERS:**
- structure_id (required): Schedule structure ID
- posting_date (required): Date for the journal entry
- period_start / period_end (required): The fiscal period
- memo (optional): Override the template memo""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "structure_id": {
            "type": "string",
            "description": "Schedule structure ID",
          },
          "posting_date": {
            "type": "string",
            "description": "Posting date (YYYY-MM-DD), typically the last day of the period",
          },
          "period_start": {
            "type": "string",
            "description": "Fiscal period start date (YYYY-MM-DD)",
          },
          "period_end": {
            "type": "string",
            "description": "Fiscal period end date (YYYY-MM-DD)",
          },
          "memo": {
            "type": "string",
            "description": "Optional memo override (default: uses schedule template)",
          },
        },
        "required": ["structure_id", "posting_date", "period_start", "period_end"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    from datetime import date

    from robosystems.db.extensions import extensions_session
    from robosystems.operations.schedules import ScheduleService

    graph_id = self.client.graph_id
    svc = ScheduleService()

    structure_id = arguments["structure_id"]
    posting_date = date.fromisoformat(arguments["posting_date"])
    period_start = date.fromisoformat(arguments["period_start"])
    period_end = date.fromisoformat(arguments["period_end"])
    memo = arguments.get("memo")

    try:
      with extensions_session(graph_id) as session:
        result = svc.create_closing_entry(
          session,
          structure_id=structure_id,
          posting_date=posting_date,
          period_start=period_start,
          period_end=period_end,
          created_by=f"mcp:{graph_id}",
          memo=memo,
        )
        session.commit()

        response = {
          "entry_id": result.entry_id,
          "status": result.status,
          "posting_date": str(result.posting_date),
          "memo": result.memo,
          "line_items": [
            {
              "element_id": result.debit_element_id,
              "debit": result.amount,
              "credit": 0,
            },
            {
              "element_id": result.credit_element_id,
              "debit": 0,
              "credit": result.amount,
            },
          ],
        }

        if result.reversal:
          response["reversal"] = {
            "entry_id": result.reversal.entry_id,
            "status": result.reversal.status,
            "posting_date": str(result.reversal.posting_date),
            "memo": result.reversal.memo,
            "line_items": [
              {
                "element_id": result.reversal.debit_element_id,
                "debit": result.reversal.amount,
                "credit": 0,
              },
              {
                "element_id": result.reversal.credit_element_id,
                "debit": 0,
                "credit": result.reversal.amount,
              },
            ],
          }

        return response
    except ValueError as exc:
      return {"error": str(exc)}
    except Exception as exc:
      logger.warning(f"create-closing-entry failed: {exc}")
      return {"error": str(exc)}
