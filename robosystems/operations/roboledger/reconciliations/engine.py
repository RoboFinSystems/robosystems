"""Compare the ledger with an independent side, account by account.

The ledger side is always computed, never entered: a balance-sheet account's
landed balance to the period end, an income-statement account's from the start
of the fiscal year. Retained earnings also carries every earlier year's
result, because the ledger keeps that in the income and expense accounts
rather than closing them into equity.

An account-scope side is compared with the balance the period's close will
leave: landed entries, the drafts the close posts, and schedule entries not
yet drafted. That figure does not move as the close drafts and posts them, so
a comparison made before the close still describes the books after it.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, time

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from robosystems.models.api.extensions import cents_to_dollars
from robosystems.models.api.extensions.reconciliations import (
  ReconciliationComponent,
  ReconciliationPreviewResponse,
  ReconciliationRow,
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger import Entry, LineItem
from robosystems.operations.roboledger.fiscal_calendar import period_date_range
from robosystems.operations.roboledger.reads.trial_balance import (
  get_net_balances_cents,
)

from .resolvers import IndependentSide, ReconciliationWindow

_RETAINED_EARNINGS_SUB_TYPE = "RetainedEarnings"


def reconciliation_window(
  period: str, fiscal_year_start_month: int
) -> ReconciliationWindow:
  """The period's last day and the first day of the fiscal year holding it."""
  _, period_end = period_date_range(period)
  year = period_end.year
  if period_end.month < fiscal_year_start_month:
    year -= 1
  return ReconciliationWindow(
    period=period,
    period_end=period_end,
    fiscal_year_start=date(year, fiscal_year_start_month, 1),
  )


def _ledger_fingerprint(
  cumulative: dict[str, int], year_to_date: dict[str, int]
) -> str:
  lines = [
    f"{element_id}|{cumulative.get(element_id, 0)}|{year_to_date.get(element_id, 0)}"
    for element_id in sorted(set(cumulative) | set(year_to_date))
  ]
  return hashlib.sha256("\n".join(lines).encode()).hexdigest()[:32]


def ledger_digest(session: Session, window: ReconciliationWindow) -> str:
  """A fingerprint of every account's landed balance at the period end, and
  for the fiscal year to it. Any entry landing in, leaving or moving within
  the window changes it."""
  return _ledger_fingerprint(
    get_net_balances_cents(session, None, window.period_end),
    get_net_balances_cents(session, window.fiscal_year_start, window.period_end),
  )


def _ledger_balances(
  window: ReconciliationWindow,
  elements: dict[str, Element],
  *,
  cumulative: dict[str, int],
  year_to_date: dict[str, int],
) -> tuple[dict[str, int], list[str]]:
  """Each account's ledger balance at the period end, and any notes."""
  balances: dict[str, int] = {}
  prior_years_result = 0
  for element_id, element in elements.items():
    if element.period_type == "instant":
      balances[element_id] = cumulative.get(element_id, 0)
    else:
      balances[element_id] = year_to_date.get(element_id, 0)
      prior_years_result += cumulative.get(element_id, 0) - balances[element_id]

  notes: list[str] = []
  retained = [
    element_id
    for element_id, element in elements.items()
    if (element.metadata_ or {}).get("account_sub_type") == _RETAINED_EARNINGS_SUB_TYPE
  ]
  if len(retained) == 1:
    balances[retained[0]] += prior_years_result
  elif prior_years_result:
    reason = (
      f"{len(retained)} chart accounts are marked as retained earnings"
      if retained
      else "no chart account is marked as retained earnings"
    )
    notes.append(
      f"Earlier years' results could not be placed ({reason}), so retained "
      "earnings will not tie."
    )
  return balances, notes


def _draft_balances(session: Session, as_of: date) -> dict[str, int]:
  """Debits minus credits, by account, of the drafts a close will post."""
  # Function-level: the close service imports the gate, which reads
  # reconciliations.
  from robosystems.operations.roboledger.fiscal_calendar.close_service import (
    drafts_close_posts,
  )

  drafts = drafts_close_posts(session, date.min, as_of).with_entities(Entry.id)
  rows = session.execute(
    select(
      LineItem.element_id,
      func.coalesce(func.sum(LineItem.debit_amount), 0),
      func.coalesce(func.sum(LineItem.credit_amount), 0),
    )
    .where(LineItem.entry_id.in_(drafts))
    .group_by(LineItem.element_id)
  )
  return {
    str(element_id): int(debits) - int(credits) for element_id, debits, credits in rows
  }


def _undrafted_schedule_balances(session: Session, as_of: date) -> dict[str, int]:
  """Debits minus credits, by account, of matured schedule entries that have
  no entry yet. The close gate holds until they are drafted."""
  # One amount per obligation, read the way the schedule's own drafting
  # reads it, so a stray second fact for a period is not counted twice.
  rows = session.execute(
    text("""
      SELECT s.metadata->'entry_template'->>'debit_element_id' AS debit_id,
             s.metadata->'entry_template'->>'credit_element_id' AS credit_id,
             f.value
      FROM events ev
      JOIN structures s
        ON s.id = ev.metadata->>'schedule_id' AND s.block_type = 'schedule'
      JOIN LATERAL (
        SELECT value, period_start, period_end
        FROM facts
        WHERE structure_id = s.id
          AND element_id = s.metadata->'entry_template'->>'debit_element_id'
          AND period_type = 'duration'
          AND fact_scope = 'in_scope'
          AND period_start = (ev.metadata->>'period_start')::date
          AND period_end = (ev.metadata->>'period_end')::date
        ORDER BY id
        LIMIT 1
      ) f ON TRUE
      WHERE ev.event_type = 'schedule_entry_due'
        AND ev.status IN ('pending', 'classified')
        AND ev.occurred_at <= :as_of_end
        AND s.metadata->'entry_template'->>'credit_element_id' IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM entries e
          WHERE e.source_structure_id = s.id
            AND e.reversal_of IS NULL
            AND e.posting_date >= f.period_start
            AND e.posting_date <= f.period_end
        )
    """),
    {"as_of_end": datetime.combine(as_of, time.max, tzinfo=UTC)},
  )
  balances: dict[str, int] = {}
  for row in rows:
    cents = round(float(row.value) * 100)
    balances[row.debit_id] = balances.get(row.debit_id, 0) + cents
    balances[row.credit_id] = balances.get(row.credit_id, 0) - cents
  return balances


_ACCOUNT_METHOD_NOTES = {
  "schedule_register": (
    "Each account's balance is compared with what its schedules say it "
    "carries at {as_of}. A difference is a balance with no schedule behind "
    "it, or a scheduled amount the ledger does not hold."
  ),
  "statement": (
    "Each account's balance is compared with the ending balance of its "
    "statement, at the statement's own date. A difference is activity on "
    "one side the other does not have yet, or an error on either."
  ),
}


def _compute_account_scope(
  session: Session,
  *,
  window: ReconciliationWindow,
  side: IndependentSide,
  include_tied: bool,
) -> ReconciliationPreviewResponse:
  # Usually one date, the period's last day; a statement can end earlier.
  dates = {
    side.as_of.get(element_id, window.period_end)
    for element_id in side.covered_element_ids
  }
  landed = {d: get_net_balances_cents(session, None, d) for d in dates}
  awaiting_close = {
    d: (_draft_balances(session, d), _undrafted_schedule_balances(session, d))
    for d in dates
  }
  elements = {
    str(element.id): element
    for element in session.execute(
      select(Element).where(Element.id.in_(sorted(side.covered_element_ids)))
    ).scalars()
  }

  tied: list[ReconciliationRow] = []
  open_rows: list[ReconciliationRow] = []
  total_difference_cents = 0
  awaiting_close_cents = 0
  for element_id, element in elements.items():
    stated_at = side.as_of.get(element_id, window.period_end)
    drafts, undrafted = awaiting_close[stated_at]
    awaiting = drafts.get(element_id, 0) + undrafted.get(element_id, 0)
    awaiting_close_cents += abs(awaiting)
    ledger_cents = landed[stated_at].get(element_id, 0) + awaiting
    independent_cents = side.balances.get(element_id, 0)
    status = "tied" if ledger_cents == independent_cents else "different"
    row = _row(
      element,
      name=element.name,
      source_account_id=None,
      ledger=ledger_cents,
      independent=independent_cents,
      status=status,
    )
    row.as_of = stated_at if stated_at != window.period_end else None
    row.components = [
      ReconciliationComponent(
        name=component.name,
        amount=cents_to_dollars(component.amount_cents),
        structure_id=component.structure_id,
        event_id=component.event_id,
        document_id=component.document_id,
        note=component.note,
      )
      for component in side.components.get(element_id, [])
    ]
    (tied if status == "tied" else open_rows).append(row)
    total_difference_cents += abs(ledger_cents - independent_cents)

  open_rows.sort(key=lambda r: (-abs(r.difference), r.account_name))
  tied.sort(key=lambda r: (r.account_code or "", r.account_name))

  notes = [_ACCOUNT_METHOD_NOTES[side.method].format(as_of=window.period_end)]
  if awaiting_close_cents:
    notes.append(
      "The ledger side includes "
      f"{cents_to_dollars(awaiting_close_cents):,.2f} of drafts and schedule "
      "entries the close will post."
    )

  return ReconciliationPreviewResponse(
    period=window.period,
    as_of=window.period_end,
    fiscal_year_start=window.fiscal_year_start,
    method=side.method,
    source=side.source,
    report_basis=side.basis,
    last_sync_at=side.last_sync_at,
    accounts_compared=len(tied) + len(open_rows),
    accounts_tied=len(tied),
    accounts_different=len(open_rows),
    total_difference=cents_to_dollars(total_difference_cents),
    rows=open_rows + (tied if include_tied else []),
    notes=notes,
  )


def _row(
  element: Element | None,
  *,
  name: str,
  source_account_id: str | None,
  ledger: int,
  independent: int,
  status: str,
) -> ReconciliationRow:
  statement = None
  if element is not None:
    statement = (
      "balance_sheet" if element.period_type == "instant" else "income_statement"
    )
  return ReconciliationRow(
    element_id=str(element.id) if element is not None else None,
    account_code=element.code if element is not None else None,
    account_name=name,
    source_account_id=source_account_id,
    statement=statement,
    ledger_balance=cents_to_dollars(ledger),
    independent_balance=cents_to_dollars(independent),
    difference=cents_to_dollars(ledger - independent),
    status=status,
  )


def compute_reconciliations(
  session: Session,
  *,
  window: ReconciliationWindow,
  side: IndependentSide,
  include_tied: bool = False,
) -> ReconciliationPreviewResponse:
  """Compare at the period end: every account for a ledger-scope side, the
  accounts it covers for an account-scope one. Writes nothing."""
  if side.scope == "account":
    return _compute_account_scope(
      session, window=window, side=side, include_tied=include_tied
    )
  cumulative = get_net_balances_cents(session, None, window.period_end)
  year_to_date = get_net_balances_cents(
    session, window.fiscal_year_start, window.period_end
  )
  # Accounts with activity or known to the source; the library's concepts
  # share the table and are not chart accounts.
  in_play = set(cumulative) | set(side.balances) | side.covered_element_ids
  elements = {
    str(element.id): element
    for element in session.execute(
      select(Element).where(Element.id.in_(sorted(in_play)))
    ).scalars()
  }
  ledger, notes = _ledger_balances(
    window, elements, cumulative=cumulative, year_to_date=year_to_date
  )

  tied: list[ReconciliationRow] = []
  open_rows: list[ReconciliationRow] = []
  total_difference_cents = 0
  for element_id, element in elements.items():
    ledger_cents = ledger.get(element_id, 0)
    independent_cents = side.balances.get(element_id, 0)
    if not ledger_cents and not independent_cents:
      continue
    if element_id not in side.covered_element_ids:
      status = "not_in_source"
    elif ledger_cents == independent_cents:
      status = "tied"
    else:
      status = "different"
    row = _row(
      element,
      name=element.name,
      source_account_id=side.source_account_ids.get(element_id),
      ledger=ledger_cents,
      independent=independent_cents,
      status=status,
    )
    (tied if status == "tied" else open_rows).append(row)
    total_difference_cents += abs(ledger_cents - independent_cents)

  for account in side.unmatched:
    total_difference_cents += abs(account.amount_cents)
    open_rows.append(
      _row(
        None,
        name=account.name,
        source_account_id=account.source_account_id,
        ledger=0,
        independent=account.amount_cents,
        status="not_in_ledger",
      )
    )

  open_rows.sort(key=lambda r: (-abs(r.difference), r.account_name))
  tied.sort(key=lambda r: (r.account_code or "", r.account_name))

  notes.insert(
    0,
    "Balance-sheet accounts are compared cumulatively to "
    f"{window.period_end}; income and expense accounts from "
    f"{window.fiscal_year_start}. Retained earnings includes earlier years' "
    "results. Draft entries are on neither side.",
  )
  if side.last_sync_at is None or side.last_sync_at.date() < window.period_end:
    notes.append(
      "The last sync is older than the period end, so a difference may be "
      "activity not yet synced."
    )

  comparison = ReconciliationPreviewResponse(
    period=window.period,
    as_of=window.period_end,
    fiscal_year_start=window.fiscal_year_start,
    method=side.method,
    source=side.source,
    report_basis=side.basis,
    last_sync_at=side.last_sync_at,
    accounts_compared=len(tied) + len(open_rows),
    accounts_tied=len(tied),
    accounts_different=len(open_rows),
    total_difference=cents_to_dollars(total_difference_cents),
    rows=open_rows + (tied if include_tied else []),
    notes=notes,
  )
  comparison._ledger_digest = _ledger_fingerprint(cumulative, year_to_date)
  return comparison
