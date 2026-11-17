"""Ledger reads that each answer one named question from the live ledger:
the chart, the journal, transactions, trial balances, an account's register,
reconciliations, the flows a line may carry, a month's classification
outcomes, the open items a payment may settle, and the group's bank accounts.

Each is the GraphQL field of the same name as a tool of its own, delegating
to the operations function that field's resolver calls.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from robosystems.db.extensions import extensions_session
from robosystems.middleware.operations import run_off_loop
from robosystems.models.api.extensions.dimensions import UNALLOCATED
from robosystems.operations.roboledger.entity_scope import EntityNotInGraphError
from robosystems.operations.roboledger.fiscal_calendar import parse_period
from robosystems.operations.roboledger.line_dimensions import LineSlice
from robosystems.operations.roboledger.reads import account_register as reads_register
from robosystems.operations.roboledger.reads import accounts as reads_accounts
from robosystems.operations.roboledger.reads import ar_ap as reads_ar_ap
from robosystems.operations.roboledger.reads import bank_accounts as reads_bank_accounts
from robosystems.operations.roboledger.reads import (
  classification as reads_classification,
)
from robosystems.operations.roboledger.reads import (
  journal_entries as reads_journal_entries,
)
from robosystems.operations.roboledger.reads import (
  reconciliations as reads_reconciliations,
)
from robosystems.operations.roboledger.reads import taxonomies as reads_taxonomies
from robosystems.operations.roboledger.reads import transactions as reads_transactions
from robosystems.operations.roboledger.reads import (
  trial_balance as reads_trial_balance,
)

from ._arguments import bounded_int as _bounded_int
from ._arguments import invalid as _invalid
from ._arguments import optional_date as _optional_date
from ._arguments import unexpected as _unexpected
from ._errors import database_failure
from .constants import ENTITY_ID_ARGUMENT

# The bounds the GraphQL list fields apply (`resolve_pagination`).
_DEFAULT_LIMIT = 100
_MAX_LIMIT = 1000
_MAX_OFFSET = 1_000_000

_LIMIT = {"type": "integer", "minimum": 1, "maximum": _MAX_LIMIT, "default": 100}
_OFFSET = {"type": "integer", "minimum": 0, "maximum": _MAX_OFFSET, "default": 0}
_DATE = {"type": "string", "description": "YYYY-MM-DD."}


def _page_size(arguments: dict[str, Any]) -> int:
  return _bounded_int(
    arguments,
    "limit",
    default=reads_register.DEFAULT_PAGE_SIZE,
    low=1,
    high=reads_register.MAX_PAGE_SIZE,
  )


def _pagination(arguments: dict[str, Any]) -> tuple[int, int]:
  """`limit` and `offset` with the bounds the GraphQL lists apply."""
  limit = _bounded_int(
    arguments, "limit", default=_DEFAULT_LIMIT, low=1, high=_MAX_LIMIT
  )
  offset = _bounded_int(arguments, "offset", default=0, low=0, high=_MAX_OFFSET)
  return limit, offset


def _line_slice(arguments: dict[str, Any]) -> LineSlice | None:
  """The management slice a read takes from its dimension pair."""
  dimension_type = arguments.get("dimension_type") or None
  member_id = arguments.get("dimension_member_id") or None
  if (dimension_type is None) != (member_id is None):
    raise ValueError(
      "dimension_type and dimension_member_id go together: the axis, and the "
      "member (or `unallocated`) whose lines to read"
    )
  if dimension_type is None:
    return None
  return LineSlice(dimension_type, None if member_id == UNALLOCATED else member_id)


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
    try:
      line_slice = _line_slice(arguments)
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


class ListAccountsTool:
  """One entity's chart of accounts, paginated."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "list-accounts",
      "description": """One entity's chart of accounts as it stands in the books, by code: each account's id, code, name and classification.

**WHEN TO USE:**
- Finding an account's id before a journal entry, a mapping, a register read or a schedule
- "What accounts do we have?", or every expense account (`classification='expense'`)

**PARAMETERS:**
- classification (optional): `asset`, `liability`, `equity`, `revenue`, `expense`, `gain` or `loss`
- is_active (optional): true for active accounts only, false for retired ones; omit for both
- entity_id (optional): omit for the group parent
- limit (optional, default 100, 1 to 1000) and offset (optional, default 0): one page

**RETURNS:**
`accounts`: each with `id`, `code`, `name`, `description`, `trait` (its classification), `sub_classification`, `balance_type`, `parent_id` and `depth` (the chart's hierarchy), `currency`, `is_active`, `is_placeholder`, `account_type`, and the source system's `external_id` and `external_source`. `pagination` gives the total and whether more follow.

**RELATED TOOLS:**
- get-account-register — one account's lines
- get-trial-balance — every account's balance over a window""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "classification": {
            "type": "string",
            "description": "The statement classification, e.g. `asset` or `expense`.",
          },
          "is_active": {"type": "boolean"},
          "entity_id": ENTITY_ID_ARGUMENT,
          "limit": _LIMIT,
          "offset": _OFFSET,
        },
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    try:
      limit, offset = _pagination(arguments)
    except ValueError as exc:
      return _invalid(str(exc))
    is_active = arguments.get("is_active")
    try:
      with extensions_session(self.client.graph_id) as session:
        return reads_accounts.list_accounts(
          session,
          trait=arguments.get("classification") or None,
          is_active=None if is_active is None else bool(is_active),
          limit=limit,
          offset=offset,
          entity_id=arguments.get("entity_id") or None,
        ).model_dump(mode="json")
    except EntityNotInGraphError as exc:
      return {"error": "entity_not_found", "message": str(exc)}
    except SQLAlchemyError as exc:
      return database_failure("list-accounts", exc)
    except Exception:
      return _unexpected("list-accounts")


class ListJournalEntriesTool:
  """One entity's journal entries with their lines, newest first."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "list-journal-entries",
      "description": """One entity's journal entries with their line items, newest first, read from the live ledger.

**WHEN TO USE:**
- "What did the close post?" (`provenance='schedule_derived'`), "what drafts are waiting?" (`status='draft'`)
- Reviewing a month's entries before closing it, or the entries under one transaction (`transaction_id`)
- Confirming a write landed: the ledger, not the materialized graph, which lags it

**PARAMETERS:**
- start_date / end_date (optional, YYYY-MM-DD): the entry-date window, both inclusive
- status (optional): `draft`, `posted` or `reversed`
- type (optional): `standard`, `adjusting`, `closing` or `reversing`
- provenance (optional): what created the entry; `schedule_derived` is what the close posted
- transaction_id (optional): only the entries under one transaction (from `list-transactions`)
- entity_id (optional): omit for the group parent
- limit (optional, default 100, 1 to 1000) and offset (optional, default 0): one page of entries

**RETURNS:**
`entries`: each with `id`, `number`, `transaction_id` (null for a standalone entry), `type`, `status`, `posting_date`, `memo`, `provenance`, the schedule it came from, the event that wrote it, `reversal_of`, `line_items` (account, debit and credit amounts, description, flow and dimension tags), `total_debit`, `total_credit` and `balanced`. Amounts are in dollars. `pagination` counts entries.

**RELATED TOOLS:**
- update-journal-entry / delete-journal-entry — correct a draft
- get-account-register — one account's lines across entries""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "start_date": _DATE,
          "end_date": _DATE,
          "status": {"type": "string", "description": "draft, posted or reversed."},
          "type": {
            "type": "string",
            "description": "standard, adjusting, closing or reversing.",
          },
          "provenance": {
            "type": "string",
            "description": "What created the entry, e.g. schedule_derived.",
          },
          "transaction_id": {"type": "string"},
          "entity_id": ENTITY_ID_ARGUMENT,
          "limit": _LIMIT,
          "offset": _OFFSET,
        },
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    try:
      start_date = _optional_date(arguments, "start_date")
      end_date = _optional_date(arguments, "end_date")
      limit, offset = _pagination(arguments)
    except ValueError as exc:
      return _invalid(str(exc))
    try:
      with extensions_session(self.client.graph_id) as session:
        return reads_journal_entries.list_journal_entries(
          session,
          start_date=start_date,
          end_date=end_date,
          status=arguments.get("status") or None,
          type=arguments.get("type") or None,
          provenance=arguments.get("provenance") or None,
          transaction_id=arguments.get("transaction_id") or None,
          limit=limit,
          offset=offset,
          entity_id=arguments.get("entity_id") or None,
        ).model_dump(mode="json")
    except EntityNotInGraphError as exc:
      return {"error": "entity_not_found", "message": str(exc)}
    except SQLAlchemyError as exc:
      return database_failure("list-journal-entries", exc)
    except Exception:
      return _unexpected("list-journal-entries")


class ListTransactionsTool:
  """One entity's transactions, paginated."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "list-transactions",
      "description": """One entity's transactions as the ledger holds them: invoices, bills, payments, deposits and the rest a source system or the ledger recorded.

**WHEN TO USE:**
- Last month's invoices or bills (`type`), or every transaction in a window
- Finding the transaction behind an entry, then its entries with `list-journal-entries(transaction_id)`

**PARAMETERS:**
- type (optional): the transaction type, as its source names it
- start_date / end_date (optional, YYYY-MM-DD): the transaction-date window, both inclusive
- entity_id (optional): omit for the group parent
- limit (optional, default 100, 1 to 1000) and offset (optional, default 0): one page

**RETURNS:**
`transactions`: each with `id`, `number`, `type`, `category`, `amount`, `currency`, `date`, `due_date`, `merchant_name`, `reference_number`, `description`, `source` and `status`. Amounts are in dollars. `pagination` gives the total and whether more follow. A standalone journal entry (what the close and the handlers post) has no transaction: read those with `list-journal-entries`.

**RELATED TOOLS:**
- list-journal-entries — a transaction's entries and lines""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "type": {"type": "string", "description": "The transaction type."},
          "start_date": _DATE,
          "end_date": _DATE,
          "entity_id": ENTITY_ID_ARGUMENT,
          "limit": _LIMIT,
          "offset": _OFFSET,
        },
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    try:
      start_date = _optional_date(arguments, "start_date")
      end_date = _optional_date(arguments, "end_date")
      limit, offset = _pagination(arguments)
    except ValueError as exc:
      return _invalid(str(exc))
    try:
      with extensions_session(self.client.graph_id) as session:
        return reads_transactions.list_transactions(
          session,
          type=arguments.get("type") or None,
          start_date=start_date,
          end_date=end_date,
          limit=limit,
          offset=offset,
          entity_id=arguments.get("entity_id") or None,
        ).model_dump(mode="json")
    except EntityNotInGraphError as exc:
      return {"error": "entity_not_found", "message": str(exc)}
    except SQLAlchemyError as exc:
      return database_failure("list-transactions", exc)
    except Exception:
      return _unexpected("list-transactions")


class GetTrialBalanceTool:
  """One entity's trial balance over a window, by account."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-trial-balance",
      "description": """One entity's trial balance from the live ledger: each account's posted debits, credits and net over a window.

**WHEN TO USE:**
- Month-end review: does the ledger foot, and what does each account carry at the month's end?
- Checking a balance against a bank statement or the source system before reconciling
- One department's or project's figures: `dimension_type` with `dimension_member_id`

**PARAMETERS:**
- start_date / end_date (optional, YYYY-MM-DD): the posting window, both inclusive; omit the start for inception to date
- entity_id (optional): omit for the group parent
- dimension_type + dimension_member_id (optional, together): only the lines that carry that member; `unallocated` for the lines no member of the axis tags

**RETURNS:**
`rows`: each account's `account_id`, `account_code`, `account_name`, `trait`, `account_type`, `total_debits`, `total_credits` and `net_balance` (debit-positive), with `total_debits` and `total_credits` for the whole. Amounts are in dollars; only posted entries count. `sliced` is true for a dimension slice, which need not foot.

**RELATED TOOLS:**
- get-mapped-trial-balance — the same balances rolled up to reporting concepts
- get-account-register — the lines behind one account's balance""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "start_date": _DATE,
          "end_date": _DATE,
          "entity_id": ENTITY_ID_ARGUMENT,
          "dimension_type": {
            "type": "string",
            "description": "A management dimension axis, with dimension_member_id.",
          },
          "dimension_member_id": {
            "type": "string",
            "description": "The member whose lines to sum, or `unallocated`.",
          },
        },
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    try:
      line_slice = _line_slice(arguments)
      start_date = _optional_date(arguments, "start_date")
      end_date = _optional_date(arguments, "end_date")
    except ValueError as exc:
      return _invalid(str(exc))
    try:
      with extensions_session(self.client.graph_id) as session:
        return reads_trial_balance.get_trial_balance(
          session,
          start_date=start_date,
          end_date=end_date,
          entity_id=arguments.get("entity_id") or None,
          line_slice=line_slice,
        ).model_dump(mode="json")
    except EntityNotInGraphError as exc:
      return {"error": "entity_not_found", "message": str(exc)}
    except SQLAlchemyError as exc:
      return database_failure("get-trial-balance", exc)
    except Exception:
      return _unexpected("get-trial-balance")


class GetMappedTrialBalanceTool:
  """A trial balance rolled up to reporting concepts through a mapping."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-mapped-trial-balance",
      "description": """A trial balance rolled up to reporting concepts: each rs-gaap concept's posted debits, credits and net over a window, summed from the accounts a chart-of-accounts mapping sends to it.

**WHEN TO USE:**
- Checking a mapping: does each reporting concept carry what its accounts do?
- Reading the books at the level the statements present, before or without generating a report

**PARAMETERS:**
- mapping_id (required): the mapping structure (`list-mapping-structures` lists them, one per entity's chart)
- start_date / end_date (optional, YYYY-MM-DD): the posting window, both inclusive

**RETURNS:**
`mapping_id` and `rows`: each concept's `reporting_element_id`, `qname`, `reporting_name`, `trait`, `balance_type`, `total_debits`, `total_credits` and `net_balance`. Amounts are in dollars. An account the mapping does not reach is in no row: `get-unmapped-elements` lists those.

**RELATED TOOLS:**
- get-trial-balance — the same window by account
- live-financial-statement — the statements built from these concepts""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "mapping_id": {
            "type": "string",
            "description": "The mapping structure whose associations roll accounts up.",
          },
          "start_date": _DATE,
          "end_date": _DATE,
        },
        "required": ["mapping_id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    mapping_id = arguments.get("mapping_id")
    if not isinstance(mapping_id, str) or not mapping_id:
      return _invalid("mapping_id is required")
    try:
      start_date = _optional_date(arguments, "start_date")
      end_date = _optional_date(arguments, "end_date")
    except ValueError as exc:
      return _invalid(str(exc))
    try:
      with extensions_session(self.client.graph_id) as session:
        return reads_taxonomies.get_mapped_trial_balance(
          session, mapping_id, start_date=start_date, end_date=end_date
        ).model_dump(mode="json")
    except SQLAlchemyError as exc:
      return database_failure("get-mapped-trial-balance", exc)
    except Exception:
      return _unexpected("get-mapped-trial-balance")


class ListReconciliationsTool:
  """Every reconciliation's standing at a period end."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "list-reconciliations",
      "description": """Every reconciliation's standing at a period end for one entity, as last recorded: the ledger against the source system, an account against its statement or its schedules.

**WHEN TO USE:**
- Before closing a month: which reconciliations the close waits on, and which are not yet reconciled
- Before `sign-off-reconciliation`: the reconciliation's id and whether it reconciles
- Reading the standing without comparing again (`refresh-reconciliations` compares and records)

**PARAMETERS:**
- period (required): the month, `YYYY-MM`
- entity_id (optional): omit for the group parent

**RETURNS:**
`period`, `as_of` (its last day) and `reconciliations`: each with `structure_id`, `name`, `scope` (`ledger` or `account`), `method` (`source_ledger`, `schedule_register` or `statement`), the account for an account-scope one, `required_for_close`, `materiality`, `status` (`not_started`, `stale`, `unreconciled`, `reconciled` or `reviewed`), `unreconciled_difference`, and the balances or account counts of the last comparison. Amounts are in dollars.

**RELATED TOOLS:**
- refresh-reconciliations — compare again and record the result
- record-statement-balance — record the statement an account reconciles to
- sign-off-reconciliation — a reviewer's sign-off""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "period": {"type": "string", "description": "The month, YYYY-MM."},
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
        return reads_reconciliations.list_reconciliations(
          session, period, entity_id=arguments.get("entity_id") or None
        ).model_dump(mode="json")
    except EntityNotInGraphError as exc:
      return {"error": "entity_not_found", "message": str(exc)}
    except SQLAlchemyError as exc:
      return database_failure("list-reconciliations", exc)
    except Exception:
      return _unexpected("list-reconciliations")


__all__ = [
  "GetAccountRegisterTool",
  "GetClassificationStatsTool",
  "GetMappedTrialBalanceTool",
  "GetTrialBalanceTool",
  "ListAccountsTool",
  "ListBankAccountsTool",
  "ListDischargeCandidatesTool",
  "ListFlowCandidatesTool",
  "ListJournalEntriesTool",
  "ListReconciliationsTool",
  "ListTransactionsTool",
]
