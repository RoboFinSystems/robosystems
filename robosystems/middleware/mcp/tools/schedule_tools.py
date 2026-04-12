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
          "closed_through": {
            "type": "string",
            "description": "Optional YYYY-MM-DD. Facts with period_end ≤ this date are flagged as historical (not acted on by the close workflow). Used during initial ledger setup to create schedules whose early facts have already been captured elsewhere.",
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
    from robosystems.operations.roboledger.schedules import ScheduleService
    from robosystems.operations.roboledger.schedules.service import (
      EntryTemplate,
      ScheduleMetadata,
    )

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

    closed_through_arg = arguments.get("closed_through")
    closed_through = (
      date.fromisoformat(closed_through_arg) if closed_through_arg else None
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
          closed_through=closed_through,
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
    from robosystems.operations.roboledger.schedules import ScheduleService

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
    from datetime import date

    from robosystems.db.extensions import extensions_session
    from robosystems.operations.roboledger.schedules import ScheduleService

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
    from robosystems.operations.roboledger.schedules import ScheduleService

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
    from robosystems.operations.roboledger.schedules import ScheduleService

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

        response: dict[str, Any] = {
          "outcome": result.outcome,
          "entry_id": result.entry_id,
          "status": result.status,
          "memo": result.memo,
          "reason": result.reason,
        }
        if result.posting_date is not None:
          response["posting_date"] = str(result.posting_date)
        if result.amount is not None:
          response["line_items"] = [
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
          ]

        if result.reversal:
          response["reversal"] = {
            "outcome": result.reversal.outcome,
            "entry_id": result.reversal.entry_id,
            "status": result.reversal.status,
            "posting_date": str(result.reversal.posting_date)
            if result.reversal.posting_date
            else None,
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
            ]
            if result.reversal.amount is not None
            else None,
          }

        return response
    except ValueError as exc:
      return {"error": str(exc)}
    except Exception as exc:
      logger.warning(f"create-closing-entry failed: {exc}")
      return {"error": str(exc)}


class CreateManualClosingEntryTool:
  """Create a one-off draft closing entry not derived from a schedule."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "create-manual-closing-entry",
      "description": """Create a manual draft closing entry for a one-off business event.

**WHEN TO USE:**
- An asset is sold or disposed of (removal + gain/loss on sale)
- A prepaid is cancelled with refund (removal + cash)
- A correcting entry for a prior period adjustment
- Any closing-period entry that isn't derived from a schedule
- The entry has any number of line items (often 3, 4, or more)

**WORKFLOW:**
1. User describes the business event
2. Determine the correct DR/CR lines — may involve multiple accounts
3. Call this tool with all line items
4. Entry is drafted; review with list-period-drafts
5. Posted atomically when close-period runs

**EXAMPLE — Asset disposal**:
Sold a $4,800 computer for $3,000 cash, $1,866.62 accumulated depreciation on the books.
```
line_items = [
  {"element_id": "elem_cash", "debit_amount": 300000},        # $3,000
  {"element_id": "elem_accum_depr", "debit_amount": 186662},  # $1,866.62
  {"element_id": "elem_computer", "credit_amount": 480000},   # $4,800
  {"element_id": "elem_gain_on_sale", "credit_amount": 6662}  # $66.62 gain
]
```

**PARAMETERS:**
- posting_date (YYYY-MM-DD): when the entry posts
- memo (required): narrative describing the event for audit
- line_items: list of {element_id, debit_amount, credit_amount, description}
  - Amounts in CENTS
  - Exactly one of debit_amount / credit_amount per line must be > 0
  - Totals must balance (sum debits == sum credits)
- entry_type: 'closing' (default), 'adjusting', or 'standard'

**GUARDS:**
- Line items must balance (422 if not)
- At least one line required
- Non-empty memo required
- provenance set to 'manual_entry' and source_structure_id is null

**NOTES:**
- Entries flow through the same review/close pipeline as schedule-derived drafts
- list-period-drafts will show this alongside schedule drafts (source_structure_name will be None)
- close-period commits this along with schedule drafts atomically""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "posting_date": {
            "type": "string",
            "description": "Posting date in YYYY-MM-DD format",
          },
          "memo": {
            "type": "string",
            "description": "Narrative describing the business event (for audit)",
            "minLength": 1,
          },
          "line_items": {
            "type": "array",
            "minItems": 1,
            "items": {
              "type": "object",
              "properties": {
                "element_id": {"type": "string"},
                "debit_amount": {"type": "integer", "minimum": 0},
                "credit_amount": {"type": "integer", "minimum": 0},
                "description": {"type": "string"},
              },
              "required": ["element_id"],
            },
            "description": "Line items — amounts in CENTS, must balance",
          },
          "entry_type": {
            "type": "string",
            "description": "Entry type (default: 'closing')",
          },
        },
        "required": ["posting_date", "memo", "line_items"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    from datetime import date

    from robosystems.db.extensions import extensions_session
    from robosystems.operations.roboledger.schedules import ScheduleService

    graph_id = self.client.graph_id
    svc = ScheduleService()

    try:
      posting_date = date.fromisoformat(arguments["posting_date"])
      with extensions_session(graph_id) as session:
        result = svc.create_manual_closing_entry(
          session,
          posting_date=posting_date,
          line_items=arguments["line_items"],
          memo=arguments["memo"],
          entry_type=arguments.get("entry_type", "closing"),
          created_by=f"mcp:{graph_id}",
        )
        session.commit()

        return {
          "outcome": result.outcome,
          "entry_id": result.entry_id,
          "status": result.status,
          "posting_date": str(result.posting_date) if result.posting_date else None,
          "memo": result.memo,
          "total_amount": result.amount,
          "line_items_count": len(arguments["line_items"]),
        }
    except ValueError as exc:
      return {"error": str(exc)}
    except Exception as exc:
      logger.warning(f"create-manual-closing-entry failed: {exc}")
      return {"error": str(exc)}


class TruncateScheduleTool:
  """End a schedule early (e.g., asset sold mid-life)."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "truncate-schedule",
      "description": """End a schedule early — delete all future facts after a new end date.

**WHEN TO USE:**
- An asset is sold or disposed of partway through its depreciation
- A prepaid expense is cancelled and refunded
- A contract/commitment is terminated early
- Any event that shortens a schedule's lifespan

**WORKFLOW:**
1. User describes the event ("I sold the computer on March 15")
2. Determine the correct new end date (last period that should still recognize expense)
3. Call this tool to truncate the schedule
4. Any stale draft entries for periods beyond the new end date are automatically deleted
5. Create a manual closing entry to record the disposal itself (gain/loss, asset removal)

**PARAMETERS:**
- structure_id (required): The schedule to truncate
- new_end_date (required): YYYY-MM-DD — **MUST be the last day of a month**
  (e.g., 2026-03-31, 2026-02-28). Schedule facts are whole-month rows, so
  mid-month dates are ambiguous and rejected.
- reason (required): Captured in audit log

**MID-MONTH EVENTS (IMPORTANT):**
When the real-world event happens mid-month (e.g., asset sold March 15), you
choose which month-end to use based on accounting convention:
- **Drop March entirely** → truncate to 2026-02-28 (prior month-end).
  Zero March depreciation recognized.
- **Keep full March** → truncate to 2026-03-31 (current month-end).
  Full March depreciation recognized.
Then book any prorated adjustment as a manual closing entry if needed.

**RETURNS:**
- facts_deleted: count of future facts removed
- new_end_date: confirmed

**GUARDS:**
- new_end_date MUST be the last day of its month (422 otherwise)
- Cannot truncate if posted entries exist for periods after new_end_date (reopen first)
- Cannot set new_end_date earlier than the schedule's first fact (deactivate instead)
- Historical facts (already posted prior to the new end date) are preserved

**NOTES:**
- Draft entries for now-deleted periods are deleted automatically
- Historical facts (fact_scope='historical') are untouched — they're already recorded
- The truncation is logged to the schedule's metadata for audit""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "structure_id": {
            "type": "string",
            "description": "Schedule structure ID",
          },
          "new_end_date": {
            "type": "string",
            "description": "New last-covered date in YYYY-MM-DD format",
          },
          "reason": {
            "type": "string",
            "description": "Reason for the truncation (required for audit)",
            "minLength": 1,
          },
        },
        "required": ["structure_id", "new_end_date", "reason"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    from datetime import date

    from robosystems.db.extensions import extensions_session
    from robosystems.operations.roboledger.schedules import ScheduleService

    graph_id = self.client.graph_id
    svc = ScheduleService()

    try:
      new_end = date.fromisoformat(arguments["new_end_date"])
      with extensions_session(graph_id) as session:
        result = svc.truncate_schedule(
          session,
          structure_id=arguments["structure_id"],
          new_end_date=new_end,
          reason=arguments["reason"],
          updated_by=f"mcp:{graph_id}",
        )
        session.commit()
        return {
          "success": True,
          "structure_id": result["structure_id"],
          "new_end_date": str(result["new_end_date"]),
          "facts_deleted": result["facts_deleted"],
          "reason": result["reason"],
        }
    except ValueError as exc:
      return {"error": str(exc)}
    except Exception as exc:
      logger.warning(f"truncate-schedule failed: {exc}")
      return {"error": str(exc)}


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
    from calendar import monthrange
    from datetime import date

    from sqlalchemy import text

    from robosystems.db.extensions import extensions_session

    graph_id = self.client.graph_id
    period = arguments["period"]

    try:
      year, month = int(period[:4]), int(period[5:7])
      period_start = date(year, month, 1)
      period_end = date(year, month, monthrange(year, month)[1])

      with extensions_session(graph_id) as session:
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

        by_entry: dict[str, dict] = {}
        for row in rows:
          eid = row.entry_id
          if eid not in by_entry:
            by_entry[eid] = {
              "entry_id": eid,
              "posting_date": str(row.posting_date),
              "type": row.entry_type,
              "memo": row.memo,
              "provenance": row.provenance,
              "source_structure_id": row.source_structure_id,
              "source_structure_name": row.source_structure_name,
              "line_items": [],
            }
          by_entry[eid]["line_items"].append(
            {
              "line_item_id": row.line_item_id,
              "element_id": row.element_id,
              "element_code": row.element_code,
              "element_name": row.element_name,
              "debit_amount": int(row.debit_amount or 0),
              "credit_amount": int(row.credit_amount or 0),
              "description": row.line_description,
            }
          )

        drafts_out = []
        total_debit = 0
        total_credit = 0
        all_balanced = True
        for data in by_entry.values():
          entry_debit = sum(li["debit_amount"] for li in data["line_items"])
          entry_credit = sum(li["credit_amount"] for li in data["line_items"])
          balanced = entry_debit == entry_credit
          if not balanced:
            all_balanced = False
          total_debit += entry_debit
          total_credit += entry_credit
          data["total_debit"] = entry_debit
          data["total_credit"] = entry_credit
          data["balanced"] = balanced
          drafts_out.append(data)

        return {
          "period": period,
          "period_start": str(period_start),
          "period_end": str(period_end),
          "draft_count": len(drafts_out),
          "total_debit": total_debit,
          "total_credit": total_credit,
          "all_balanced": all_balanced,
          "drafts": drafts_out,
        }
    except Exception as exc:
      logger.warning(f"list-period-drafts failed: {exc}")
      return {"error": str(exc)}
