"""Trial balance read operations."""

from __future__ import annotations

from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session

from robosystems.models.api.extensions import cents_to_dollars
from robosystems.models.api.extensions.trial_balance import (
  TrialBalanceResponse,
  TrialBalanceRow,
)

_TRIAL_BALANCE_SQL = text("""
  SELECT a.id, a.code, a.name,
         t.identifier AS trait,
         a.metadata->>'account_type' AS account_type,
         COALESCE(SUM(li.debit_amount), 0) AS total_debits,
         COALESCE(SUM(li.credit_amount), 0) AS total_credits
  FROM elements a
  JOIN line_items li ON li.element_id = a.id
  JOIN entries e ON e.id = li.entry_id
  LEFT JOIN (
    SELECT et.element_id, tr.identifier
    FROM element_traits et
    JOIN traits tr ON tr.id = et.trait_id
    WHERE et.is_primary = TRUE
      AND tr.category = 'elementsOfFinancialStatements'
  ) t ON t.element_id = a.id
  WHERE e.status = 'posted'
    AND (e.posting_date >= :start_date OR :start_date IS NULL)
    AND (e.posting_date <= :end_date OR :end_date IS NULL)
  GROUP BY a.id, a.code, a.name, t.identifier, a.metadata->>'account_type'
  ORDER BY a.code
""")


def get_trial_balance(
  session: Session,
  start_date: date | None = None,
  end_date: date | None = None,
) -> TrialBalanceResponse:
  """Return the trial balance for posted entries in the given date range."""
  result = session.execute(
    _TRIAL_BALANCE_SQL, {"start_date": start_date, "end_date": end_date}
  )

  rows: list[TrialBalanceRow] = []
  grand_debits = 0.0
  grand_credits = 0.0

  for row in result:
    debits = cents_to_dollars(row.total_debits)
    credits = cents_to_dollars(row.total_credits)
    grand_debits += debits
    grand_credits += credits
    rows.append(
      TrialBalanceRow(
        account_id=row.id,
        account_code=row.code,
        account_name=row.name,
        trait=row.trait,
        account_type=row.account_type,
        total_debits=debits,
        total_credits=credits,
        net_balance=debits - credits,
      )
    )

  return TrialBalanceResponse(
    rows=rows,
    total_debits=grand_debits,
    total_credits=grand_credits,
  )
