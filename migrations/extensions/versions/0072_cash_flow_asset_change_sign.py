"""An asset's change on the cash flow is stated as the change in the balance.

Six operating cash flow rows stored the cash effect of a change in an asset:
a rise in receivables was a negative fact added to operating cash. The
concepts say the opposite (``IncreaseDecreaseInAccountsReceivable`` is the
increase), and every filer states the increase and subtracts it, so any
reader of the concept (an XBRL or holon consumer, xbrlkit, a fund's ingest)
saw a fall where receivables rose.

The committed packages now carry:

* ``rs-gaap-calculations``: the derivation arcs of receivables, inventory,
  prepaid expense, other current assets and amounts due from affiliates at
  weight +1 (the fact is the change in the balance), and their calculation
  arcs into ``NetCashProvidedByUsedInOperatingActivities`` at -1, with the
  reconciling leaf ``IncreaseDecreaseInOtherOperatingCapitalNet``;
* ``rs-gaap-rollup-rules``: the operating RollUp subtracts the six.

Statements read as before: a subtracted row is shown at its cash effect
under a negated label. Open periods take the new sign on their next read; a
closed month keeps what it stamped until it is re-stamped.

This migration brings ``public`` up to the committed source and fans the two
packages into every tenant through ``resync_library_into_tenant`` (the 0068
pattern). The arcs keep their ids, so the weights update in place.

Revision ID: 0072
Revises: 0071
Create Date: 2026-11-25
"""

from __future__ import annotations

from pathlib import Path

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0072"
down_revision = "0071"
branch_labels = None
depends_on = None

# tenant-fanout: not-required (data only; every tenant is reached through
# resync_library_into_tenant below)

_PINS = {
  "rs-gaap-calculations": "v1",
  "rs-gaap-rollup-rules": "v1",
}


def _seed_path(standard: str, version: str) -> Path:
  from robosystems.taxonomy.discovery import FRAMEWORKS_DIR

  return (
    FRAMEWORKS_DIR / "rs-gaap" / "packages" / standard / version / "taxonomy.jsonld"
  )


def _refresh_public(conn) -> None:
  """Bring ``public`` up to the committed JSON-LD for both packages."""
  from sqlalchemy.orm import Session as _Session

  from robosystems.operations.taxonomy_block.library_creator import (
    create_library_arcs,
    create_library_rules,
  )
  from robosystems.taxonomy import load_taxonomy_package

  packages = {
    standard: load_taxonomy_package(_seed_path(standard, version))
    for standard, version in _PINS.items()
  }
  session = _Session(bind=conn)
  try:
    counts = create_library_arcs(session, packages["rs-gaap-calculations"])
    session.flush()
    print(
      f"  [rs-gaap-calculations] arcs={counts['associations']} "
      f"skipped={counts['associations_skipped']} "
      f"retired={counts['associations_retired']}"
    )
    counts = create_library_rules(session, packages["rs-gaap-rollup-rules"])
    session.flush()
    print(f"  [rs-gaap-rollup-rules] rules={counts['rules']}")
  finally:
    session.close()


def upgrade() -> None:
  from robosystems.taxonomy.writer import SET_LIBRARY_RESYNC, resync_library_into_tenant

  conn = op.get_bind()

  # The immutability triggers reject UPDATE and DELETE on library-origin rows
  # without the bypass GUC; SET LOCAL covers the public refresh and the whole
  # tenant fan-out in this transaction.
  conn.execute(text(SET_LIBRARY_RESYNC))

  _refresh_public(conn)

  def _resync(conn, schema: str) -> None:
    if conn.execute(text(f"SELECT 1 FROM {schema}.elements LIMIT 1")).scalar() is None:
      print(f"  [cash flow asset sign -> {schema}] skipped: no elements provisioned")
      return
    stats = resync_library_into_tenant(conn, schema, pin=dict(_PINS))
    print(
      f"  [cash flow asset sign -> {schema}] associations={stats.associations} "
      f"rules={stats.rules}"
    )

  for_each_tenant_schema(conn, _resync)


def downgrade() -> None:
  """Intentionally a no-op.

  The upgrade brings library content up to the committed framework source.
  Rolling the schema back does not require rolling the taxonomy back; re-run
  the upgrade after any downgrade/upgrade cycle if content ever diverges.
  """
