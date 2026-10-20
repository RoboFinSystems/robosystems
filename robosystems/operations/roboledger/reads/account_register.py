"""Account register: one entity's landed lines on one account, oldest first,
each with the balance it leaves.

Lines are ordered by ``(posting_date, entry id, line_order, line id)``, and a
page is the lines after a cursor on that key. Each page's opening is summed
from the lines before its cursor, so the running balance is right on any page
read alone.
"""

from __future__ import annotations

import base64
import json
from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session

from robosystems.models.api.extensions import cents_to_dollars
from robosystems.models.api.extensions.account_register import (
  AccountRegisterResponse,
  RegisterAccount,
  RegisterLine,
)
from robosystems.models.extensions import Element
from robosystems.operations.library.reads.elements import efs_trait_by_element
from robosystems.operations.roboledger.entity_scope import find_entity_id
from robosystems.operations.roboledger.entry_status import landed_entry_bindparam
from robosystems.operations.roboledger.line_dimensions import (
  LINE_SLICE_SQL,
  LineSlice,
  line_dimensions,
  slice_params,
)
from robosystems.operations.roboledger.reads.fiscal_calendar import (
  get_fiscal_year_start_month,
)

DEFAULT_PAGE_SIZE = 200
MAX_PAGE_SIZE = 1000

# Income and expense close to retained earnings each fiscal year.
_TEMPORARY_TRAITS = frozenset({"revenue", "expense", "gain", "loss"})


class AccountNotFoundError(LookupError):
  """The account is not an element of this graph."""


class InvalidCursorError(ValueError):
  """The ``after`` cursor is not one this read issued."""


_LINES_WHERE = (
  """
  li.element_id = :account_id
  AND (:entity_id IS NULL OR e.entity_id = :entity_id)
  AND (CAST(:flow_id AS TEXT) IS NULL OR li.flow_element_id = :flow_id)
  AND """
  + LINE_SLICE_SQL
)

_AFTER_CURSOR = """(
  CAST(:c_line AS TEXT) IS NULL
  OR (e.posting_date, e.id, li.line_order, li.id)
     > (CAST(:c_date AS DATE), CAST(:c_entry AS TEXT),
        CAST(:c_order AS INTEGER), CAST(:c_line AS TEXT))
)"""

_IN_WINDOW = "(e.posting_date >= :start_date OR :start_date IS NULL)"

_TOTALS_SQL = text(
  f"""
  SELECT
    COALESCE(SUM(li.debit_amount - li.credit_amount)
      FILTER (WHERE e.posting_date < :start_date), 0) AS opening,
    COALESCE(SUM(li.debit_amount) FILTER (WHERE {_IN_WINDOW}), 0) AS debits,
    COALESCE(SUM(li.credit_amount) FILTER (WHERE {_IN_WINDOW}), 0) AS credits,
    COUNT(*) FILTER (WHERE {_IN_WINDOW}) AS line_count,
    COALESCE(SUM(li.debit_amount - li.credit_amount)
      FILTER (WHERE {_IN_WINDOW} AND NOT {_AFTER_CURSOR}), 0) AS carried
  FROM line_items li
  JOIN entries e ON e.id = li.entry_id
  WHERE e.status IN :landed_entry_statuses
    AND (e.posting_date >= :opening_from OR :opening_from IS NULL)
    AND (e.posting_date <= :end_date OR :end_date IS NULL)
    AND {_LINES_WHERE}
  """
).bindparams(landed_entry_bindparam())

_LINE_COLUMNS = """
  li.id AS line_id, e.id AS entry_id, e.number, e.posting_date, e.type,
  e.provenance, e.status, e.memo, li.description, li.debit_amount,
  li.credit_amount, li.line_order, li.flow_element_id,
  e.triggered_by_event_id, ev.event_type AS triggered_by_event_type,
  e.reversal_of, ev.agent_id, ag.name AS agent_name
"""

_LINE_JOINS = """
  FROM line_items li
  JOIN entries e ON e.id = li.entry_id
  LEFT JOIN events ev ON ev.id = e.triggered_by_event_id
  LEFT JOIN agents ag ON ag.id = ev.agent_id
"""

_PAGE_SQL = text(
  f"""
  SELECT {_LINE_COLUMNS}, rv.id AS reversed_by
  {_LINE_JOINS}
  LEFT JOIN entries rv
    ON rv.reversal_of = e.id AND rv.status IN :landed_entry_statuses
  WHERE e.status IN :landed_entry_statuses
    AND {_IN_WINDOW}
    AND (e.posting_date <= :end_date OR :end_date IS NULL)
    AND {_AFTER_CURSOR}
    AND {_LINES_WHERE}
  ORDER BY e.posting_date, e.id, li.line_order, li.id
  LIMIT :page_limit
  """
).bindparams(landed_entry_bindparam())

_DRAFTS_SQL = text(
  f"""
  SELECT {_LINE_COLUMNS}, NULL AS reversed_by
  {_LINE_JOINS}
  WHERE e.status = 'draft'
    AND {_IN_WINDOW}
    AND (e.posting_date <= :end_date OR :end_date IS NULL)
    AND {_LINES_WHERE}
  ORDER BY e.posting_date, e.id, li.line_order, li.id
  LIMIT :page_limit
  """
)


def _encode_cursor(
  posting_date: date, entry_id: str, line_order: int, line_id: str
) -> str:
  raw = json.dumps([posting_date.isoformat(), entry_id, line_order, line_id])
  return base64.urlsafe_b64encode(raw.encode()).decode()


def _decode_cursor(cursor: str | None) -> dict[str, object]:
  if not cursor:
    return {"c_date": None, "c_entry": None, "c_order": None, "c_line": None}
  try:
    posting, entry_id, line_order, line_id = json.loads(
      base64.urlsafe_b64decode(cursor.encode())
    )
    return {
      "c_date": date.fromisoformat(posting),
      "c_entry": str(entry_id),
      "c_order": int(line_order),
      "c_line": str(line_id),
    }
  except (ValueError, TypeError) as exc:
    raise InvalidCursorError("`after` is not a cursor this register issued.") from exc


def _fiscal_year_start(day: date, start_month: int) -> date:
  year = day.year if day.month >= start_month else day.year - 1
  return date(year, start_month, 1)


def _line(row, balance_cents: int | None, sign: int, dims) -> RegisterLine:
  return RegisterLine(
    line_id=row.line_id,
    entry_id=row.entry_id,
    entry_number=row.number,
    posting_date=row.posting_date,
    entry_type=row.type,
    provenance=row.provenance,
    status=row.status,
    memo=row.memo,
    description=row.description,
    debit=cents_to_dollars(row.debit_amount),
    credit=cents_to_dollars(row.credit_amount),
    balance=None if balance_cents is None else cents_to_dollars(sign * balance_cents),
    triggered_by_event_id=row.triggered_by_event_id,
    triggered_by_event_type=row.triggered_by_event_type,
    reversal_of=row.reversal_of,
    reversed_by=row.reversed_by,
    agent_id=row.agent_id,
    agent_name=row.agent_name,
    flow_element_id=row.flow_element_id,
    dimensions=dims.get(str(row.line_id), []),
  )


def get_account_register(
  session: Session,
  account_id: str,
  start_date: date | None = None,
  end_date: date | None = None,
  *,
  entity_id: str | None = None,
  line_slice: LineSlice | None = None,
  flow_element_id: str | None = None,
  include_drafts: bool = False,
  after: str | None = None,
  limit: int = DEFAULT_PAGE_SIZE,
) -> AccountRegisterResponse:
  """One entity's landed lines on one account over a window, oldest first.
  ``entity_id`` defaults to the group parent. ``line_slice`` and
  ``flow_element_id`` read only the lines carrying that member or flow row,
  and every balance is then the slice's. ``after`` is the ``next_cursor`` of
  the page before; drafts come with the first page only."""
  account = session.get(Element, account_id)
  if account is None:
    raise AccountNotFoundError(f"Account {account_id!r} not found in this graph.")
  limit = max(1, min(limit, MAX_PAGE_SIZE))

  trait = efs_trait_by_element(session, [account_id]).get(account_id)
  temporary = trait in _TEMPORARY_TRAITS
  opening_from = (
    _fiscal_year_start(start_date, get_fiscal_year_start_month(session))
    if temporary and start_date is not None
    else None
  )
  sign = -1 if account.balance_type == "credit" else 1
  scoped_entity_id = find_entity_id(session, entity_id)

  params: dict[str, object] = {
    "account_id": account_id,
    "entity_id": scoped_entity_id,
    "flow_id": flow_element_id,
    "start_date": start_date,
    "end_date": end_date,
    "opening_from": opening_from,
    "page_limit": limit + 1,
    **slice_params(line_slice),
    **_decode_cursor(after),
  }

  totals = session.execute(_TOTALS_SQL, params).one()
  opening = int(totals.opening)
  page_opening = opening + int(totals.carried)

  rows = session.execute(_PAGE_SQL, params).all()
  next_cursor = None
  if len(rows) > limit:
    rows = rows[:limit]
    last = rows[-1]
    next_cursor = _encode_cursor(
      last.posting_date, last.entry_id, last.line_order, last.line_id
    )

  draft_rows = (
    session.execute(_DRAFTS_SQL, params).all()
    if include_drafts and after is None
    else []
  )
  drafts_truncated = len(draft_rows) > limit
  draft_rows = draft_rows[:limit]
  dims = line_dimensions(session, [r.line_id for r in (*rows, *draft_rows)])

  running = page_opening
  lines: list[RegisterLine] = []
  for row in rows:
    running += int(row.debit_amount) - int(row.credit_amount)
    lines.append(_line(row, running, sign, dims))

  debits = int(totals.debits)
  credits = int(totals.credits)
  return AccountRegisterResponse(
    account=RegisterAccount(
      id=account.id,
      code=account.code,
      name=account.name,
      balance_type=account.balance_type,
      trait=trait,
      temporary=temporary,
    ),
    entity_id=scoped_entity_id,
    start_date=start_date,
    end_date=end_date,
    opening_from=opening_from,
    opening_balance=cents_to_dollars(sign * opening),
    total_debits=cents_to_dollars(debits),
    total_credits=cents_to_dollars(credits),
    closing_balance=cents_to_dollars(sign * (opening + debits - credits)),
    line_count=int(totals.line_count),
    page_opening_balance=cents_to_dollars(sign * page_opening),
    lines=lines,
    next_cursor=next_cursor,
    drafts=[_line(row, None, sign, dims) for row in draft_rows],
    drafts_truncated=drafts_truncated,
    sliced=line_slice is not None or flow_element_id is not None,
  )
