"""Ledger trial balance endpoint."""

from datetime import date

from fastapi import APIRouter, Depends, Path, Query
from sqlalchemy import text

from robosystems.db.ledger import oltp_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import create_error_response
from robosystems.models.api.ledger import cents_to_dollars
from robosystems.models.api.ledger.trial_balance import (
  TrialBalanceResponse,
  TrialBalanceRow,
)
from robosystems.models.iam import User

router = APIRouter()


@router.get(
  "/trial-balance",
  response_model=TrialBalanceResponse,
  operation_id="getLedgerTrialBalance",
  summary="Trial Balance",
  tags=["Ledger"],
)
async def get_trial_balance(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  start_date: date | None = Query(None, description="Start date (inclusive)"),
  end_date: date | None = Query(None, description="End date (inclusive)"),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with oltp_session(graph_id) as session:
      result = session.execute(
        text("""
          SELECT a.id, a.code, a.name, a.classification,
                 COALESCE(SUM(li.debit_amount), 0) AS total_debits,
                 COALESCE(SUM(li.credit_amount), 0) AS total_credits
          FROM accounts a
          JOIN line_items li ON li.account_id = a.id
          JOIN entries e ON e.id = li.entry_id
          WHERE e.status = 'posted'
            AND (e.posting_date >= :start_date OR :start_date IS NULL)
            AND (e.posting_date <= :end_date OR :end_date IS NULL)
          GROUP BY a.id, a.code, a.name, a.classification
          ORDER BY a.code
        """),
        {"start_date": start_date, "end_date": end_date},
      )

      rows = []
      grand_debits = 0
      grand_credits = 0

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
            classification=row.classification,
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
  except ValueError:
    raise create_error_response(
      404, "Ledger not initialized. Connect a data source first."
    )
  except Exception as e:
    if "schema" in str(e).lower() or "does not exist" in str(e).lower():
      raise create_error_response(
        404, "Ledger not initialized. Connect a data source first."
      )
    raise
