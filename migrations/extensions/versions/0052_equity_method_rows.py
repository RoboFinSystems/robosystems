"""Equity-method rows: the investment, its income, and its flows, in every
Reporting Style.

A group's associate — a member held at a fifth to a half, or any entity a
person sets to the equity method (``consolidation_method``) — is not summed
into the consolidated statements. The group carries one asset, the
investment, and one income line, its share of the associate's result; a
distribution received reduces the investment (spec ``consolidation`` §4.3
item 3, C1). Every concept was in the library and excluded from tenants,
so none could be mapped or rendered.

The committed packages now carry, additively — no arc moves, no new concept:

* ``rs-gaap-traits``: the gain EFS trait on
  ``IncomeLossFromEquityMethodInvestments`` (a mapping candidate whose sign
  reaches net income and the retained-earnings close, as the NCI share's
  does since 0046);
* ``rs-gaap-presentation``: ``EquityMethodInvestments`` under noncurrent
  assets on the three balance sheets, beside the investment in
  subsidiaries; ``IncomeLossFromEquityMethodInvestments`` in the
  nonoperating section of the income statement and, on the three cash
  flows, as a non-cash add-back in operating; the investment's purchase and
  sale in investing;
* ``rs-gaap-calculations``: a calc arc for each new row (the add-back at
  weight -1, the share of income being non-cash; distributions received
  arrive through the investment's default flows, as a return of the
  investment — the ``...NetOfDividendsOrDistributions`` reconciling item is
  not used), and the two default flows for the investment leaf;
* ``rs-gaap-rollup-rules``: the four RollUps whose children grew, rebuilt.

A statement with none of these facts renders exactly as before: every new
row is a leaf that drops when all-zero, and no subtotal's children moved.

This migration brings ``public`` up to the committed source and fans the
pinned packages into every tenant through ``resync_library_into_tenant``
(the 0046 pattern). ``tenant-exclude/v1.json`` was regenerated with this
change so the concepts copy. Tenant-authored arcs are never touched.

Revision ID: 0052
Revises: 0051
Create Date: 2026-10-17
"""

from __future__ import annotations

from pathlib import Path

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0052"
down_revision = "0051"
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
      print(f"  [equity-method rows -> {schema}] skipped: no elements provisioned")
      return
    stats = resync_library_into_tenant(conn, schema, pin=dict(_PINS))
    print(
      f"  [equity-method rows -> {schema}] elements={stats.elements} "
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
