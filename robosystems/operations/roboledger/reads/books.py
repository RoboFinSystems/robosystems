"""What kind of books a graph keeps — the predicates behind the provider guard.

``specs/ledger/native-accounting-cutover.md`` §2: native and synced ledgers
never mix. A bank feed needs a chart to resolve against and cannot sit
beside a live QuickBooks connection; QuickBooks cannot become the source
of truth over books a tenant already keeps natively.
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

  A tenant whose only chart came from ``synced_source`` (and whose manual
  entries post to that chart) has none. A severed tenant has them by
  construction — its elements are ``native`` and carry the history — which
  is what makes the QuickBooks → native lifecycle one-way.
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
