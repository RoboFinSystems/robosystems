"""Graph-side effects of a connection lifecycle event.

The sever half of the native accounting cutover
(``specs/ledger/native-accounting-cutover.md`` §3): when a QuickBooks
tenant goes native, the chart QuickBooks created becomes the tenant's own.
"""

from __future__ import annotations

from sqlalchemy import update
from sqlalchemy.orm import Session

from robosystems.models.extensions import Element

SEVERABLE_SOURCES: frozenset[str] = frozenset({"quickbooks"})


def sever_synced_chart(
  session: Session, connection_id: str, *, source: str = "quickbooks"
) -> int:
  """Stamp the elements a synced connection created as native-owned.

  One statement does three jobs. Nulling ``external_source`` /
  ``connection_id`` / ``external_id`` takes the rows out of the loader's
  upsert key (``idx_elements_upsert_key``), so a later sync of the same
  provider can neither overwrite nor re-adopt them and the trait heal never
  sees them. ``source='native'`` keeps them visible as chart accounts
  (``native`` is in ``COA_SOURCES``) and marks them editable. The qname is
  kept: facts, associations, line items and reports reference element ids,
  and a rename would reopen the adapter's qname-collision path.

  Returns the number of elements stamped.
  """
  if source not in SEVERABLE_SOURCES:
    raise ValueError(f"{source!r} connections cannot be severed")
  result = session.execute(
    update(Element)
    .where(
      Element.external_source == source,
      Element.connection_id == connection_id,
    )
    .values(
      source="native",
      external_source=None,
      connection_id=None,
      external_id=None,
    )
  )
  return int(result.rowcount or 0)
