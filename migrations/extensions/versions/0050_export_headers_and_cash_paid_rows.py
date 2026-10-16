"""The roll-forward's header concepts and the cash flow's cash-paid rows.

The committed packages now carry:

* ``rs-gaap``: ``StatementOfStockholdersEquityAbstract`` and
  ``StatementOfPartnersCapitalAbstract``, the headers the export presents an
  equity roll-forward under (``serialization.bundle._apply_preferred_labels``);
  and ``InterestPaidNet`` and ``IncomeTaxesPaidNet``, the two most common
  supplemental cash flow rows in the SEC coverage pass. All under their
  us-gaap names.
* ``rs-gaap-presentation``: the two cash-paid rows in the supplemental section
  of ``CashFlow-indirect``, ``-LLC`` and ``-PART``.

No Style network presents the headers (the engine keeps the capital concept
as the parent of its movements), so they seed ``public`` only and stay out of
every tenant copy; the bundle declares the header it uses. The cash-paid rows
are presented, so ``tenant-exclude/v1.json`` (regenerated with this change)
copies them, and every tenant is resynced through
``resync_library_into_tenant`` (the 0045 pattern). Tenant-authored arcs are
never touched.

Revision ID: 0050
Revises: 0049
Create Date: 2026-10-16
"""

from __future__ import annotations

from pathlib import Path

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0050"
down_revision = "0049"
branch_labels = None
depends_on = None

# tenant-fanout: not-required (data only; every tenant is reached through
# resync_library_into_tenant below)

_PINS = {
  "rs-gaap": "v1",
  "rs-gaap-traits": "v1",
  "rs-gaap-presentation": "v1",
  "rs-gaap-calculations": "v1",
  "rs-gaap-rollup-rules": "v1",
}

_NEW_CONCEPTS = frozenset(
  {
    "rs-gaap:StatementOfStockholdersEquityAbstract",
    "rs-gaap:StatementOfPartnersCapitalAbstract",
    "rs-gaap:InterestPaidNet",
    "rs-gaap:IncomeTaxesPaidNet",
  }
)


def _seed_path(standard: str) -> Path:
  from robosystems.taxonomy.discovery import FRAMEWORKS_DIR

  return (
    FRAMEWORKS_DIR
    / "rs-gaap"
    / "packages"
    / standard
    / _PINS[standard]
    / "taxonomy.jsonld"
  )


def _refresh_public(conn) -> None:
  from sqlalchemy.orm import Session as _Session

  from robosystems.operations.taxonomy_block.library_creator import (
    create_library_arcs,
    create_library_taxonomy_elements,
  )
  from robosystems.taxonomy import load_taxonomy_package

  session = _Session(bind=conn)
  try:
    rs_gaap = load_taxonomy_package(_seed_path("rs-gaap"))
    only_new = rs_gaap.model_copy(
      update={"elements": [e for e in rs_gaap.elements if e.qname in _NEW_CONCEPTS]}
    )
    _taxonomy_id, counts = create_library_taxonomy_elements(session, only_new)
    session.flush()
    print(f"  [rs-gaap] elements={counts['elements']}")

    counts = create_library_arcs(
      session, load_taxonomy_package(_seed_path("rs-gaap-presentation"))
    )
    session.flush()
    print(
      f"  [rs-gaap-presentation] arcs={counts['associations']} "
      f"skipped={counts['associations_skipped']} "
      f"retired={counts['associations_retired']}"
    )
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
    stats = resync_library_into_tenant(conn, schema, pin=dict(_PINS))
    print(
      f"  [cash-paid rows -> {schema}] elements={stats.elements} "
      f"structures={stats.structures} associations={stats.associations} "
      f"element_traits={stats.element_traits} rules={stats.rules}"
    )

  for_each_tenant_schema(conn, _resync)


def downgrade() -> None:
  """Intentionally a no-op: library content tracks the committed source."""
