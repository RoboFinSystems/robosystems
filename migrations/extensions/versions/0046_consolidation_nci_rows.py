"""Consolidation rows: noncontrolling interest, the parent's investment in
its subsidiaries, and the intercompany pair, in every Reporting Style.

A group with an outside owner in one of its members carries that owner's
share: noncontrolling interest in equity, income attributable to it on the
income statement, its distributions and contributions in financing. The
parent's standalone books carry the investment in each subsidiary and the
balances due from and to its affiliates. Every one of these concepts was in
the library and excluded from tenants, so none could be mapped or rendered
(spec ``consolidation`` §4.6, C0).

The committed packages now carry, additively — no arc moves, no new concept:

* ``rs-gaap-traits``: the expense EFS trait on
  ``NetIncomeLossAttributableToNoncontrollingInterest`` (a mapping candidate
  whose sign reaches net income), and the indirect-method reconciling-item
  trait on the two affiliate working-capital rows;
* ``rs-gaap-presentation``: on the three balance sheets, due from affiliates
  under current assets, the investment under noncurrent assets, due to
  affiliates under current liabilities, and noncontrolling interest beside
  stockholders' equity under the liabilities-and-equity total; the NCI share
  of income before net income on the income statement; on the three cash
  flow networks, the NCI share as an operating add-back, the two affiliate
  working-capital rows, the investment's purchase and divestiture in
  investing, and distributions to and contributions from noncontrolling
  interests in financing; on the three roll-forwards, noncontrolling
  interest as a second root with its share of income, contributions and
  distributions;
* ``rs-gaap-calculations``: a calc arc for each new row (net income subtracts
  the NCI share; the liabilities-and-equity total adds noncontrolling
  interest beside stockholders' equity), and a default flow for each new
  balance-sheet leaf;
* ``rs-gaap-rollup-rules``: the eight RollUps whose children grew, regenerated.

A statement with none of these facts renders exactly as before: every new
row is a leaf that drops when all-zero, and no subtotal's children moved.

This migration brings ``public`` up to the committed source and fans the
pinned packages into every tenant through ``resync_library_into_tenant``
(the 0045 pattern). ``tenant-exclude/v1.json`` was regenerated with this
change so the concepts copy. Tenant-authored arcs are never touched.

Revision ID: 0046
Revises: 0045
Create Date: 2026-10-15
"""

from __future__ import annotations

from pathlib import Path

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0046"
down_revision = "0045"
branch_labels = None
depends_on = None

# tenant-fanout: not-required (data only; every tenant is reached through
# resync_library_into_tenant below)

# In load order. ``rs-gaap`` carries no change of its own: it is pinned so
# the tenant resync copies the concepts the regenerated keep-set now admits
# (the element step selects from the pinned taxonomies). The trait
# assignments and arcs then resolve against them.
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
    # Upserts on the rule id, so the regenerated expressions and variable
    # lists replace the old ones in place.
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
      print(f"  [consolidation rows -> {schema}] skipped: no elements provisioned")
      return
    stats = resync_library_into_tenant(conn, schema, pin=dict(_PINS))
    print(
      f"  [consolidation rows -> {schema}] elements={stats.elements} "
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
