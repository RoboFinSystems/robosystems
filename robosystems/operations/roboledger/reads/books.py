"""What kind of books a graph keeps: the predicates behind the provider guard.
Native and synced ledgers never mix, so a bank feed needs a chart and no live
QuickBooks connection, and QuickBooks cannot take over native books.
"""

from __future__ import annotations

from sqlalchemy import exists, select
from sqlalchemy.orm import Session

from robosystems.models.extensions import Element, LineItem, Taxonomy

COA_TAXONOMY_TYPE = "chart_of_accounts"


def graph_has_chart(session: Session) -> bool:
  """True when the graph has an active ``chart_of_accounts`` taxonomy."""
  return bool(
    session.execute(
      select(
        exists().where(
          Taxonomy.taxonomy_type == COA_TAXONOMY_TYPE,
          Taxonomy.is_active.is_(True),
        )
      )
    ).scalar()
  )


def graph_has_native_line_items(session: Session, *, synced_source: str) -> bool:
  """True when posted line items sit on elements ``synced_source`` did not create.

  A severed tenant always has them (its elements became ``native``), which
  makes the QuickBooks → native lifecycle one-way.
  """
  return bool(
    session.execute(
      select(
        exists().where(
          LineItem.element_id == Element.id,
          Element.source != synced_source,
        )
      )
    ).scalar()
  )
