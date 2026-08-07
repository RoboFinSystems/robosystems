"""Closing book read operations.

Provides the structure overview for the digital closing book viewer —
all structure categories in one call for the sidebar navigation.
"""

from __future__ import annotations

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.closing_book import (
  ClosingBookCategory,
  ClosingBookItem,
  ClosingBookStructuresResponse,
)
from robosystems.models.extensions.roboledger import Report, Structure

# Structure types that represent financial statements. `cash_flow_statement`
# is absent because roboledger has no renderer for it; SEC XBRL cash-flow
# parsing is a separate path and is unaffected.
_STATEMENT_TYPES = {
  "income_statement",
  "balance_sheet",
  "equity_statement",
}

# Display labels for statement types
_STATEMENT_LABELS = {
  "income_statement": "Income Statement",
  "balance_sheet": "Balance Sheet",
  "equity_statement": "Statement of Changes in Equity",
}


def get_closing_book_structures(session: Session) -> ClosingBookStructuresResponse:
  """Aggregate closing-book categories for the sidebar navigation.

  Merges period-close status, statements (from the latest report),
  schedules, account rollups, and trial-balance availability into a
  single response.
  """
  categories: list[ClosingBookCategory] = []

  # 1. Period Close hub — always first so it's the operational home
  # for the close workflow. Frontend defaults to this item on load.
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

  # 2. Statements — from the most recent report's taxonomy structures
  latest_report = session.execute(
    select(Report)
    .where(Report.generation_status.in_(["complete", "published", "generating"]))
    .order_by(Report.created_at.desc())
    .limit(1)
  ).scalar_one_or_none()

  if latest_report:
    types_list = ", ".join(f"'{t}'" for t in _STATEMENT_TYPES)
    stmt_result = session.execute(
      text(f"""
        SELECT id, name, block_type FROM structures
        WHERE taxonomy_id = :taxonomy_id
          AND block_type IN ({types_list})
          AND is_active = true
        ORDER BY block_type
      """),
      {"taxonomy_id": latest_report.taxonomy_id},
    )

    statement_items = [
      ClosingBookItem(
        id=r.id,
        name=_STATEMENT_LABELS.get(r.block_type, r.name),
        item_type="statement",
        block_type=r.block_type,
        report_id=latest_report.id,
      )
      for r in stmt_result
    ]

    if statement_items:
      categories.append(ClosingBookCategory(label="Statements", items=statement_items))

  # 3. Account Rollups — from mapping structures
  mappings = (
    session.execute(
      select(Structure)
      .where(
        Structure.block_type == "coa_mapping",
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
    categories.append(ClosingBookCategory(label="Account Rollups", items=rollup_items))

  # 4. Schedules — active schedule structures
  schedules = (
    session.execute(
      select(Structure)
      .where(
        Structure.block_type == "schedule",
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
        block_type="schedule",
      )
      for s in schedules
    ]
    categories.append(ClosingBookCategory(label="Schedules", items=schedule_items))

  # 5. Trial Balance — always present if there are posted entries
  has_posted = session.execute(
    text("SELECT EXISTS(SELECT 1 FROM entries WHERE status = 'posted')")
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

  return ClosingBookStructuresResponse(
    categories=categories,
    has_data=bool(has_posted),
  )
