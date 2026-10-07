"""What kind of books a graph keeps: the predicates behind the provider guard.
Native and synced ledgers never mix, so a bank feed needs a chart and no live
QuickBooks connection, and QuickBooks cannot take over native books.
"""

from __future__ import annotations

from sqlalchemy import exists, select
from sqlalchemy.orm import Session

from robosystems.models.extensions import Element, LineItem, Taxonomy

COA_TAXONOMY_TYPE = "chart_of_accounts"


def graph_has_chart(session: Session, entity_id: str | None = None) -> bool:
  """True when the entity (default the group parent, or any entity on a
  graph with none yet) has an active ``chart_of_accounts`` taxonomy."""
  from robosystems.operations.roboledger.entity_scope import find_entity_id
  from robosystems.operations.taxonomy_block.coa_mappings import entity_chart_id

  owner = find_entity_id(session, entity_id)
  if owner is not None:
    return entity_chart_id(session, owner) is not None
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


def graph_has_native_line_items(
  session: Session, *, synced_source: str, entity_id: str | None = None
) -> bool:
  """True when posted line items sit on elements ``synced_source`` did not
  create — in ``entity_id``'s books when one is named, anywhere otherwise.

  A severed tenant always has them (its elements became ``native``), which
  makes the QuickBooks → native lifecycle one-way. A subsidiary's own lines
  are not the parent's, so a synced ledger for the parent reads the
  parent's books only.
  """
  from robosystems.models.extensions.roboledger import Entry

  clauses = [LineItem.element_id == Element.id, Element.source != synced_source]
  if entity_id is not None:
    clauses += [LineItem.entry_id == Entry.id, Entry.entity_id == entity_id]
  return bool(session.execute(select(exists().where(*clauses))).scalar())


def entity_has_feed_account(session: Session, entity_id: str | None) -> bool:
  """True when a bank feed books to an account in ``entity_id``'s chart
  (default the group parent's): the link on ``Element.metadata.bank_feed``."""
  from robosystems.adapters.bank_feed.accounts import BANK_FEED_KEY
  from robosystems.operations.roboledger.reads.accounts import (
    account_scope,
    entity_accounts_clause,
  )

  scope = account_scope(session, entity_id)
  return bool(
    session.execute(
      select(
        exists().where(
          entity_accounts_clause(scope), Element.metadata_.has_key(BANK_FEED_KEY)
        )
      )
    ).scalar()
  )
