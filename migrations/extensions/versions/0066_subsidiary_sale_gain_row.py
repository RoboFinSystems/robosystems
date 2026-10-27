"""The gain on selling a member's shares, on its own row.

The consolidation engine posts the group's gain or loss on a member sold out
of the group, and its reversal of the parent's gain on a sale that keeps
control, to one concept. It used ``GainLossOnDispositionOfAssets``, the one
disposal gain the income statement's calculation arcs carried; us-gaap
narrowed that concept to property, plant and equipment, so every statement
labelled a subsidiary sale as an equipment sale. The concept that says what
it is, ``GainOrLossOnSaleOfStockInSubsidiary``, was in the library and
excluded from tenants, with no row to reach net income.

The committed packages now carry, additively — no arc moves, no new concept:

* ``rs-gaap-presentation``: the row in the nonoperating section of both
  income statements and, on the three cash flows, removed in operating as
  non-cash beside the disposal gain;
* ``rs-gaap-calculations``: a calc arc for each (the add-back at weight -1);
* ``rs-gaap-rollup-rules``: the nonoperating and operating RollUps, rebuilt.

A statement with none of its facts renders exactly as before: the row is a
leaf that drops when all-zero, and no subtotal's children moved.

This migration brings ``public`` up to the committed source and fans the
pinned packages into every tenant through ``resync_library_into_tenant``
(the 0052 pattern). ``tenant-exclude/v1.json`` was regenerated with this
change so the concept copies. Tenant-authored arcs are never touched, so a
consolidation entity forked from the template before this keeps its
``4400`` account on the old concept; the engine names the missing account
the next time it needs it.

Revision ID: 0066
Revises: 0065
Create Date: 2026-10-27
"""

from __future__ import annotations

from pathlib import Path

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0066"
down_revision = "0065"
branch_labels = None
depends_on = None

# tenant-fanout: not-required (data only; every tenant is reached through
# resync_library_into_tenant below)

# In load order. ``rs-gaap`` carries no change of its own: it is pinned so
# the tenant resync copies the concept the regenerated keep-set now admits.
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
      print(f"  [subsidiary sale gain -> {schema}] skipped: no elements provisioned")
      return
    stats = resync_library_into_tenant(conn, schema, pin=dict(_PINS))
    print(
      f"  [subsidiary sale gain -> {schema}] elements={stats.elements} "
      f"structures={stats.structures} associations={stats.associations} "
      f"rules={stats.rules}"
    )

  for_each_tenant_schema(conn, _resync)


def downgrade() -> None:
  """Intentionally a no-op: library content tracks the committed source."""
