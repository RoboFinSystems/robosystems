"""Closing book endpoints.

Provides the structure overview for the digital closing book viewer —
all structure categories in one call for the sidebar navigation.
"""

from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy import select, text
from sqlalchemy.exc import ProgrammingError

from robosystems.db.extensions import extensions_session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.extensions.closing_book import (
  ClosingBookCategory,
  ClosingBookItem,
  ClosingBookStructuresResponse,
)
from robosystems.models.core import User
from robosystems.models.extensions.roboledger import Report, Structure

router = APIRouter()


def _ledger_404():
  return HTTPException(
    status_code=404,
    detail="Ledger not initialized. Connect a data source first.",
  )


# Structure types that represent financial statements
_STATEMENT_TYPES = {
  "income_statement",
  "balance_sheet",
  "cash_flow_statement",
  "equity_statement",
}

# Display labels for statement types
_STATEMENT_LABELS = {
  "income_statement": "Income Statement",
  "balance_sheet": "Balance Sheet",
  "cash_flow_statement": "Cash Flow Statement",
  "equity_statement": "Statement of Changes in Equity",
}


@router.get(
  "/closing-book/structures",
  response_model=ClosingBookStructuresResponse,
  operation_id="getClosingBookStructures",
  summary="Closing Book Structures",
  tags=["Ledger"],
)
async def get_closing_book_structures(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Returns all structure categories for the closing book sidebar.

  Aggregates statements (from latest report), schedules, account rollups
  (from mapping structures), and trial balance availability into a single
  response for the viewer sidebar navigation.
  """
  try:
    with extensions_session(graph_id) as session:
      categories: list[ClosingBookCategory] = []

      # 1. Statements — from the most recent report's taxonomy structures
      latest_report = session.execute(
        select(Report)
        .where(Report.generation_status.in_(["complete", "published", "generating"]))
        .order_by(Report.created_at.desc())
        .limit(1)
      ).scalar_one_or_none()

      if latest_report:
        stmt_result = session.execute(
          text("""
            SELECT id, name, structure_type FROM structures
            WHERE taxonomy_id = :taxonomy_id
              AND structure_type IN ('income_statement', 'balance_sheet',
                                     'cash_flow_statement', 'equity_statement')
              AND is_active = true
            ORDER BY structure_type
          """),
          {"taxonomy_id": latest_report.taxonomy_id},
        )

        statement_items = [
          ClosingBookItem(
            id=r.id,
            name=_STATEMENT_LABELS.get(r.structure_type, r.name),
            item_type="statement",
            structure_type=r.structure_type,
            report_id=latest_report.id,
          )
          for r in stmt_result
        ]

        if statement_items:
          categories.append(
            ClosingBookCategory(label="Statements", items=statement_items)
          )

      # 2. Account Rollups — from mapping structures
      mappings = (
        session.execute(
          select(Structure)
          .where(
            Structure.structure_type == "coa_mapping",
            Structure.is_active.is_(True),
          )
          .order_by(Structure.name)
        )
        .scalars()
        .all()
      )

      if mappings:
        rollup_items = [
          ClosingBookItem(
            id=m.id,
            name=m.name,
            item_type="account_rollups",
          )
          for m in mappings
        ]
        categories.append(
          ClosingBookCategory(label="Account Rollups", items=rollup_items)
        )

      # 3. Schedules — active schedule structures
      schedules = (
        session.execute(
          select(Structure)
          .where(
            Structure.structure_type == "schedule",
            Structure.is_active.is_(True),
          )
          .order_by(Structure.name)
        )
        .scalars()
        .all()
      )

      if schedules:
        schedule_items = [
          ClosingBookItem(
            id=s.id,
            name=s.name,
            item_type="schedule",
            structure_type="schedule",
          )
          for s in schedules
        ]
        categories.append(ClosingBookCategory(label="Schedules", items=schedule_items))

      # 4. Trial Balance — always present if there are posted entries
      has_posted = session.execute(
        text("SELECT EXISTS(SELECT 1 FROM entries WHERE status = 'posted' LIMIT 1)")
      ).scalar()

      if has_posted:
        categories.append(
          ClosingBookCategory(
            label="Trial Balance",
            items=[
              ClosingBookItem(
                id="trial_balance",
                name="Trial Balance",
                item_type="trial_balance",
              ),
            ],
          )
        )

      # 5. Period Close — always present
      categories.append(
        ClosingBookCategory(
          label="Period Close",
          items=[
            ClosingBookItem(
              id="period_close",
              name="Current Period Status",
              item_type="period_close",
            ),
          ],
        )
      )

      return ClosingBookStructuresResponse(
        categories=categories,
        has_data=bool(has_posted),
      )

  except ValueError:
    raise _ledger_404()
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"Closing book structures failed: {e}")
    raise
