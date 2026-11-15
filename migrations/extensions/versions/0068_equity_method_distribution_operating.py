"""An equity-method distribution is an operating cash flow, by default.

A distribution received from an equity-method investee reduces the
investment, so it took the investment's default inflow,
``ProceedsFromSaleOfEquityMethodInvestments``: every routine distribution
read as a sale in investing. Under ASC 230-10-45-21D a distribution within
the investor's cumulative share of the investee's earnings is a return on
the investment, operating; only an excess, a return of it, is investing.

The committed packages now carry:

* ``rs-gaap-traits``: ``EquityMethodInvestmentDividendsOrDistributions``
  gains the operating activity and inflow traits, so the engine reads it
  gross from the cash lines as it does investing and financing flows;
* ``rs-gaap-presentation``: that concept in operating on the three cash
  flows, beside the share of income it follows, and
  ``ProceedsFromEquityMethodInvestmentDividendsOrDistributionsReturnOfCapital``
  in investing, for a line tagged as a return of capital;
* ``rs-gaap-calculations``: a calc arc for each, and the investment's
  inflow default flow re-pointed from the sale to the distribution (a sale
  of the investment is tagged with the sale row, which stays);
* ``rs-gaap-rollup-rules``: the operating and investing RollUps, rebuilt.

A statement with none of these facts renders exactly as before. A posted
distribution moves from investing to operating on the next read of an open
period; a closed month keeps what it stamped.

This migration brings ``public`` up to the committed source and fans the
pinned packages into every tenant through ``resync_library_into_tenant``
(the 0052 pattern). The re-pointed default flow is a new arc: the resync's
retire step removes the old one, in ``public`` and in every tenant.
``tenant-exclude/v1.json`` was regenerated so the two concepts copy.

Revision ID: 0068
Revises: 0067
Create Date: 2026-11-14
"""

from __future__ import annotations

from pathlib import Path

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0068"
down_revision = "0067"
branch_labels = None
depends_on = None

# tenant-fanout: not-required (data only; every tenant is reached through
# resync_library_into_tenant below)

# In load order. ``rs-gaap`` carries no change of its own: it is pinned so
# the tenant resync copies the concepts the regenerated keep-set now admits.
_PINS = {
  "rs-gaap": "v1",
  "rs-gaap-traits": "v1",
  "rs-gaap-presentation": "v1",
  "rs-gaap-calculations": "v1",
  "rs-gaap-rollup-rules": "v1",
}

_ARC_PACKAGES = ("rs-gaap-traits", "rs-gaap-presentation", "rs-gaap-calculations")


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
        f"retired={counts['associations_retired']} "
        f"traits={counts['trait_assignments']}"
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
    # The pins carry arcs and rules, not elements; a schema that was never
    # provisioned has nothing for them to resolve against.
    if conn.execute(text(f"SELECT 1 FROM {schema}.elements LIMIT 1")).scalar() is None:
      print(
        f"  [equity-method distribution -> {schema}] skipped: no elements provisioned"
      )
      return
    stats = resync_library_into_tenant(conn, schema, pin=dict(_PINS))
    print(
      f"  [equity-method distribution -> {schema}] elements={stats.elements} "
      f"structures={stats.structures} associations={stats.associations} "
      f"element_traits={stats.element_traits} rules={stats.rules}"
    )

  for_each_tenant_schema(conn, _resync)


def downgrade() -> None:
  """Intentionally a no-op.

  The upgrade brings library content up to the committed framework source;
  taking the rows back would strand any tenant account mapped to them.
  Rolling the schema back does not require rolling the taxonomy back; re-run
  the upgrade after any downgrade/upgrade cycle if content ever diverges.
  """
