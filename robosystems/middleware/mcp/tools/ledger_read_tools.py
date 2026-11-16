"""Ledger reads that each answer one named question: an account's register,
the flows a line may carry, a month's classification outcomes, the open
items a payment may settle, and the group's bank accounts.

Each is the GraphQL field of the same name as a tool of its own, delegating
to the operations function that field's resolver calls.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from robosystems.db.extensions import extensions_session
from robosystems.logger import logger
from robosystems.middleware.operations import run_off_loop
from robosystems.models.api.extensions.dimensions import UNALLOCATED
from robosystems.operations.roboledger.entity_scope import EntityNotInGraphError
from robosystems.operations.roboledger.fiscal_calendar import parse_period
from robosystems.operations.roboledger.line_dimensions import LineSlice
from robosystems.operations.roboledger.reads import account_register as reads_register
from robosystems.operations.roboledger.reads import ar_ap as reads_ar_ap
from robosystems.operations.roboledger.reads import bank_accounts as reads_bank_accounts
from robosystems.operations.roboledger.reads import (
  classification as reads_classification,
)
from robosystems.operations.roboledger.reads import taxonomies as reads_taxonomies

from ._errors import database_failure
from .constants import ENTITY_ID_ARGUMENT


def _invalid(message: str) -> dict[str, Any]:
  return {"error": "invalid_arguments", "message": message}


def _unexpected(tool_name: str) -> dict[str, Any]:
  """A fault that is not the caller's: the detail goes to the log, never to
  the client."""
  logger.warning("MCP tool %s failed", tool_name, exc_info=True)
  return {"error": "command_failed", "message": f"{tool_name} failed"}


def _page_size(arguments: dict[str, Any]) -> int:
  """`arguments["limit"]`, held to the range its schema declares."""
  value = arguments.get("limit")
  if value is None:
    return reads_register.DEFAULT_PAGE_SIZE
  try:
    if isinstance(value, bool) or (isinstance(value, float) and not value.is_integer()):
      raise ValueError
    limit = int(value)
  except TypeError, ValueError:
    limit = 0
  if not 1 <= limit <= reads_register.MAX_PAGE_SIZE:
    raise ValueError(f"limit must be between 1 and {reads_register.MAX_PAGE_SIZE}")
  return limit


def _optional_date(arguments: dict[str, Any], key: str) -> date | None:
  """`arguments[key]` as a date; ValueError names the argument."""
  value = arguments.get(key)
  if not value:
    return None
  try:
    return date.fromisoformat(value)
  except TypeError, ValueError:
    raise ValueError(f"{key} must be YYYY-MM-DD") from None


class GetAccountRegisterTool:
  """One account's posted lines over a window, each with the balance it leaves."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-account-register",
      "description": """One account's posted lines, oldest first, each with the balance it leaves and the entry and event behind it.

**WHEN TO USE:**
- "Why is this account's balance what it is?": the lines that make it up
- After `live-financial-statement(include_sources=true)` names the accounts behind a figure: open one of them here
- Checking what posted to an account in a month before closing it

**PARAMETERS:**
- account_id (required): the chart account (element id)
- start_date / end_date (optional, YYYY-MM-DD): the window, both inclusive; omit for the first or last line
- entity_id (optional): omit for the group parent
- dimension_type + dimension_member_id (optional, together): only the lines that carry that member; `unallocated` for the lines no member of the axis tags
- flow_element_id (optional): only the lines tagged to that cash flow or equity row
- include_drafts (optional, default false): also return the window's draft lines, outside every balance
- after (optional): the `next_cursor` of the page before
- limit (optional, default 200): lines per page, 1 to 1000; anything else is refused

**RETURNS:**
`account`, the window, `opening_balance`, `total_debits`, `total_credits`, `closing_balance`, `line_count`, `page_opening_balance` and `lines`: each with its entry, posting date, memo, debit, credit, the balance after it, and the event that wrote it. Amounts are in dollars, in the account's natural sign. `next_cursor` is set while more lines follow. An income or expense account's opening counts from the fiscal year's start.

**RELATED TOOLS:**
- live-financial-statement — the statement whose figure the account feeds
- get-event-block — the event behind a line""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "account_id": {
            "type": "string",
            "description": "The chart account (element id).",
          },
          "start_date": {
            "type": "string",
            "description": "First day of the window, YYYY-MM-DD. Omit from the first line.",
          },
          "end_date": {
            "type": "string",
            "description": "Last day of the window, YYYY-MM-DD. Omit through the last line.",
          },
          "entity_id": ENTITY_ID_ARGUMENT,
          "dimension_type": {
            "type": "string",
            "description": "A management dimension axis, with dimension_member_id.",
          },
          "dimension_member_id": {
            "type": "string",
            "description": "The member whose lines to read, or `unallocated`.",
          },
          "flow_element_id": {
            "type": "string",
            "description": "Only the lines tagged to this cash flow or equity row.",
          },
          "include_drafts": {"type": "boolean", "default": False},
          "after": {
            "type": "string",
            "description": "The `next_cursor` of the page before.",
          },
          "limit": {
            "type": "integer",
            "minimum": 1,
            "maximum": reads_register.MAX_PAGE_SIZE,
            "default": reads_register.DEFAULT_PAGE_SIZE,
          },
        },
        "required": ["account_id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    account_id = arguments.get("account_id")
    if not isinstance(account_id, str) or not account_id:
      return _invalid("account_id is required")
    dimension_type = arguments.get("dimension_type") or None
    member_id = arguments.get("dimension_member_id") or None
    if (dimension_type is None) != (member_id is None):
      return _invalid(
        "dimension_type and dimension_member_id go together: the axis, and the "
        "member (or `unallocated`) whose lines to read"
      )
    line_slice = (
      LineSlice(dimension_type, None if member_id == UNALLOCATED else member_id)
      if dimension_type
      else None
    )
    try:
      start_date = _optional_date(arguments, "start_date")
      end_date = _optional_date(arguments, "end_date")
      limit = _page_size(arguments)
    except ValueError as exc:
      return _invalid(str(exc))

    try:
      with extensions_session(self.client.graph_id) as session:
        return reads_register.get_account_register(
          session,
          account_id,
          start_date=start_date,
          end_date=end_date,
          entity_id=arguments.get("entity_id") or None,
          line_slice=line_slice,
          flow_element_id=arguments.get("flow_element_id") or None,
          include_drafts=bool(arguments.get("include_drafts", False)),
          after=arguments.get("after") or None,
          limit=limit,
        ).model_dump(mode="json")
    except EntityNotInGraphError as exc:
      return {"error": "entity_not_found", "message": str(exc)}
    except reads_register.AccountNotFoundError as exc:
      return {"error": "not_found", "message": str(exc)}
    except reads_register.InvalidCursorError as exc:
      return _invalid(str(exc))
    except SQLAlchemyError as exc:
      return database_failure("get-account-register", exc)
    except Exception:
      return _unexpected("get-account-register")


class ListFlowCandidatesTool:
  """The cash flow rows a line may be tagged with."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "list-flow-candidates",
      "description": """The cash flow rows a ledger line may be tagged with: the flow that overrides its account's default on the cash flow and equity statements.

**WHEN TO USE:**
- Before setting a bank line's `classified_flow_qname` or a journal-entry line's `flow_qname`: a loan drawn or repaid, capital put in or taken out, an asset bought or sold
- To see which investing and financing rows this company's cash flow statement offers

**PARAMETERS:**
- entity_id (optional): the entity whose statement the rows come from; omit for the group parent

**RETURNS:**
`flows`: each row's `element_id`, `qname` (the value the line takes), `name` as the statement labels it, and `activity` (`operating`, `investing` or `financing`), by section then name. These are the rows the write path accepts, so a flow picked here is never refused.

**RELATED TOOLS:**
- update-event-block — `classified_flow_qname` on a bank line
- update-journal-entry — `line_flows` on a posted entry's lines""",
      "inputSchema": {
        "type": "object",
        "properties": {"entity_id": ENTITY_ID_ARGUMENT},
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    entity_id = arguments.get("entity_id") or None
    try:
      with extensions_session(self.client.graph_id) as session:
        flows = reads_taxonomies.list_entity_flow_candidates(session, entity_id)
        return {
          "entity_id": entity_id,
          "flows": [flow.model_dump(mode="json") for flow in flows],
        }
    except EntityNotInGraphError as exc:
      return {"error": "entity_not_found", "message": str(exc)}
    except SQLAlchemyError as exc:
      return database_failure("list-flow-candidates", exc)
    except Exception:
      return _unexpected("list-flow-candidates")


class GetClassificationStatsTool:
  """How a month's bank lines were classified, per suggestion source."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-classification-stats",
      "description": """How one month's bank-feed lines were classified: taken as suggested, overridden, split, or chosen with no suggestion, and the share that needed a person's choice.

**WHEN TO USE:**
- "How much of the month did the suggestions get right?" after an `accept-suggestions` run
- Month-end review of the inbox: what is still open, and where suggestions were overridden

**PARAMETERS:**
- period (required): the month, `YYYY-MM`, over the lines' own dates
- entity_id (optional): omit for the group parent

**RETURNS:**
`lines` (committed lines that recorded an outcome), `accepted`, `overridden`, `unsuggested`, `split`, `unstamped` (committed before outcomes were recorded, outside the share), `human_choice_share` (null when no line recorded an outcome), `open_lines` (still captured or classified), and `by_source`: the same counts per suggestion source (such as `agent_default`, `open_item`, or `tier0` for the bank's own category; `none` for lines with no suggestion), largest first.

**RELATED TOOLS:**
- list-event-blocks — the lines themselves
- accept-suggestions — take the open suggestions in bulk""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "period": {
            "type": "string",
            "description": "The month, YYYY-MM.",
          },
          "entity_id": ENTITY_ID_ARGUMENT,
        },
        "required": ["period"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    period = arguments.get("period")
    try:
      parse_period(period if isinstance(period, str) else "")
    except ValueError as exc:
      return _invalid(str(exc))
    try:
      with extensions_session(self.client.graph_id) as session:
        return reads_classification.classification_stats(
          session, period, arguments.get("entity_id") or None
        ).model_dump(mode="json")
    except EntityNotInGraphError as exc:
      return {"error": "entity_not_found", "message": str(exc)}
    except SQLAlchemyError as exc:
      return database_failure("get-classification-stats", exc)
    except Exception:
      return _unexpected("get-classification-stats")


class ListDischargeCandidatesTool:
  """The open invoices or bills a payment or bank line may settle."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "list-discharge-candidates",
      "description": """The open invoices or bills a payment or bank line may settle, best match first.

**WHEN TO USE:**
- Before committing a bank line that pays a bill or collects an invoice: a person picks the items it settles from these
- "Which invoice does this deposit pay?"

**PARAMETERS:**
- event_id (required): the payment or bank line

**RETURNS:**
`event_id`, `kind` (`receivables` for money in, `payables` for money out), `amount_cents`, `classified_to_control` (whether the line is classified to a receivables or payables account), and `candidates`: the open items of the line's own entity and counterparty at its date, an exact amount match first, then the oldest due date, each with its `open_cents`, due date and days past due.

**NOTES:**
- Settling is a person's choice: record it on the commit with `discharge_allocations: [{obligation_event_id, amount}]` on `update-event-block`, several for a split. A line with no pick commits as it is, and its bill stays open

**RELATED TOOLS:**
- update-event-block — commit the line with its settlement
- aged-balances — every open item, aged""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "event_id": {
            "type": "string",
            "description": "The payment or bank line (event id).",
          },
        },
        "required": ["event_id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    event_id = arguments.get("event_id")
    if not isinstance(event_id, str) or not event_id:
      return _invalid("event_id is required")
    try:
      with extensions_session(self.client.graph_id) as session:
        response = reads_ar_ap.list_discharge_candidates(session, event_id)
        if response is None:
          return {"error": "not_found", "message": f"Event {event_id!r} not found."}
        return response.model_dump(mode="json")
    except SQLAlchemyError as exc:
      return database_failure("list-discharge-candidates", exc)
    except Exception:
      return _unexpected("list-discharge-candidates")


class ListBankAccountsTool:
  """The group's bank and card accounts, with what writes to each."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "list-bank-accounts",
      "description": """The group's bank and card accounts: every chart account a bank feed books to, or that a source system types as a bank or card account, with the entity whose chart it is in and the health of the connection that writes to it.

**WHEN TO USE:**
- "Which bank accounts do we have, and which feed writes to each?"
- Before `link-bank-account`: find the chart account a feed account should book to
- Before `record-statement-balance`: find the account a statement covers

**PARAMETERS:**
- entity_id (optional): one entity's accounts; omit for every entity in the group

**RETURNS:**
`accounts`, by code then name: each account's `id`, `code`, `name`, `kind` (`bank` or `credit`), `entity_id` and `entity_name`, `source` (`plaid`, `mercury`, `quickbooks`, or null for one kept by hand), `connection_id`, `feed_account_id` (the provider's id, which `link-bank-account` takes), the institution, and the connection's status and last sync. `total` counts them.

**RELATED TOOLS:**
- link-bank-account — point a feed account at a chart account
- get-graph-sync-status — each connection's last sync in full""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "entity_id": {
            "type": "string",
            "description": "One entity's accounts, by id. Omit for every entity.",
          },
        },
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    health = reads_bank_accounts.load_connection_health(graph_id)
    try:
      with extensions_session(graph_id) as session:
        return reads_bank_accounts.list_bank_accounts(
          session,
          entity_id=arguments.get("entity_id") or None,
          connections=health,
        ).model_dump(mode="json")
    except EntityNotInGraphError as exc:
      return {"error": "entity_not_found", "message": str(exc)}
    except SQLAlchemyError as exc:
      return database_failure("list-bank-accounts", exc)
    except Exception:
      return _unexpected("list-bank-accounts")


__all__ = [
  "GetAccountRegisterTool",
  "GetClassificationStatsTool",
  "ListBankAccountsTool",
  "ListDischargeCandidatesTool",
  "ListFlowCandidatesTool",
]
