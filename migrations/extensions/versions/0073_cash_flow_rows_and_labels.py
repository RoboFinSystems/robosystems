"""Other noncurrent assets and liabilities are operating; three investing rows.

A change in ``OtherAssetsNoncurrent`` defaulted to *Payments to acquire / Proceeds
from sale of other investments*, and one in ``OtherLiabilitiesNoncurrent`` to
long-term debt proceeds and repayments. What a small company books there is
mostly neither: a lease security deposit paid or refunded, a customer deposit
held past a year. Both now default to operating, as working-capital changes;
a borrowing or an investment booked to them takes its flow by tag. The mapping
operator's bucket for a noncurrent asset or liability it cannot place is these
two concepts, so a guessed account no longer reads as debt or an investment.

Three investing rows join tenants for a tagged line: notes receivable made and
collected, and insurance settlement proceeds.

The committed packages now carry:

* ``rs-gaap-calculations``: a derivation arc from each account to its operating
  change row (``IncreaseDecreaseInOtherNoncurrentAssets`` /
  ``...Liabilities``, weight +1) in place of the four flow arcs, which retire;
  calculation arcs for the two operating rows (assets at -1, liabilities at
  +1) and the three investing rows;
* ``rs-gaap-presentation``: the five rows on the three indirect cash flows;
* ``rs-gaap-rollup-rules``: the operating and investing RollUps, rebuilt.

Open periods read the new rows on their next read; a closed month keeps what it
stamped until it is re-stamped.

This migration brings ``public`` up to the committed source and fans the pinned
packages into every tenant through ``resync_library_into_tenant`` (the 0068
pattern); the resync's retire step removes the four old arcs.
``tenant-exclude/v1.json`` was regenerated so the five concepts copy.

Revision ID: 0073
Revises: 0072
Create Date: 2026-11-25
"""

from __future__ import annotations

from pathlib import Path

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0073"
down_revision = "0072"
branch_labels = None
depends_on = None

# tenant-fanout: not-required (data only; every tenant is reached through
# resync_library_into_tenant below)

# In load order. ``rs-gaap`` carries no change of its own: it is pinned so
# the tenant resync copies the concepts the regenerated keep-set now admits.
_PINS = {
  "rs-gaap": "v1",
  "rs-gaap-presentation": "v1",
  "rs-gaap-calculations": "v1",
  "rs-gaap-rollup-rules": "v1",
}

_ARC_PACKAGES = ("rs-gaap-presentation", "rs-gaap-calculations")


def _seed_path(standard: str, version: str) -> Path:
  from robosystems.taxonomy.discovery import FRAMEWORKS_DIR

  return (
    FRAMEWORKS_DIR / "rs-gaap" / "packages" / standard / version / "taxonomy.jsonld"
  )


def _refresh_public(conn) -> None:
  """Bring ``public`` up to the committed JSON-LD for every pinned package."""
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
    for standard in _ARC_PACKAGES:
      counts = create_library_arcs(session, packages[standard])
      session.flush()
      print(
        f"  [{standard}] arcs={counts['associations']} "
        f"skipped={counts['associations_skipped']} "
        f"retired={counts['associations_retired']}"
      )
    # Upserts on the rule id, so the rebuilt expressions and variable lists
    # replace the old ones in place.
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
    # The arcs resolve against elements; a schema never provisioned has none.
    if conn.execute(text(f"SELECT 1 FROM {schema}.elements LIMIT 1")).scalar() is None:
      print(f"  [cash flow rows -> {schema}] skipped: no elements provisioned")
      return
    stats = resync_library_into_tenant(conn, schema, pin=dict(_PINS))
    print(
      f"  [cash flow rows -> {schema}] elements={stats.elements} "
      f"structures={stats.structures} associations={stats.associations} "
      f"rules={stats.rules}"
    )

  for_each_tenant_schema(conn, _resync)


def downgrade() -> None:
  """Intentionally a no-op: library content tracks the committed source."""
