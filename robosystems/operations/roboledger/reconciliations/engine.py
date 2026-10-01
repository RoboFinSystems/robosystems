"""Compare the ledger with an independent side, account by account.

The ledger side is always computed, never entered: a balance-sheet account's
landed balance to the period end, an income-statement account's from the start
of the fiscal year. Retained earnings also carries every earlier year's
result, because the ledger keeps that in the income and expense accounts
rather than closing them into equity.
"""

from __future__ import annotations

from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions import cents_to_dollars
from robosystems.models.api.extensions.reconciliations import (
  ReconciliationPreviewResponse,
  ReconciliationRow,
)
from robosystems.models.extensions.element import Element
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
  """Compare every account at the period end. Writes nothing."""
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
