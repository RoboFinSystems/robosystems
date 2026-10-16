"""Equity roll-forward movement rows, in every Reporting Style.

The Statement of Changes in Equity presented cash flow concepts as its change
rows (``ProceedsFromIssuanceOfCommonStock``, ``PaymentsOfDividends``, …), so it
could only show the equity movements that moved cash: an owner who paid a
company liability directly contributed capital that appeared nowhere, and the
statement did not reach its closing balance. The roll-forward now has rows of
its own, filled from every equity line, cash or not (spec
``cash-flow-noncash-and-settlement`` G1, the equity half).

The committed packages now carry:

* ``rs-gaap``: seven concepts under their us-gaap names —
  ``StockIssuedDuringPeriodValueNewIssues``,
  ``StockRepurchasedDuringPeriodValue``, ``Dividends`` and
  ``StockholdersEquityOther`` for the corporate form,
  ``PartnersCapitalAccountContributions``,
  ``PartnersCapitalAccountDistributions`` and ``PartnersCapitalOther`` for the
  partnership and LLC forms;
* ``rs-gaap-presentation``: ``Equity-rollforward``, ``-PART`` and ``-LLC``
  present those rows beside net income and OCI, in place of the cash flow
  concepts (and of ``ShareBasedCompensation``, which now lands in other).

No calc arc moves and no balance-sheet or cash flow network changes. The new
concepts carry no statement-classification trait, so none is offered as a
mapping target.

This migration brings ``public`` up to the committed source and fans the
pinned packages into every tenant through ``resync_library_into_tenant`` (the
0045 pattern). The resync retires the replaced arcs in each tenant copy;
tenant-authored arcs are never touched.

Revision ID: 0047
Revises: 0046
Create Date: 2026-10-16
"""

from __future__ import annotations

from pathlib import Path

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0047"
down_revision = "0046"
branch_labels = None
depends_on = None

# tenant-fanout: not-required (data only; every tenant is reached through
# resync_library_into_tenant below)

# In load order. Elements seed before the arcs resolve them.
_PINS = {
  "rs-gaap": "v1",
  "rs-gaap-presentation": "v1",
}

_NEW_CONCEPTS = frozenset(
  {
    "rs-gaap:StockIssuedDuringPeriodValueNewIssues",
    "rs-gaap:StockRepurchasedDuringPeriodValue",
    "rs-gaap:Dividends",
    "rs-gaap:StockholdersEquityOther",
    "rs-gaap:PartnersCapitalAccountContributions",
    "rs-gaap:PartnersCapitalAccountDistributions",
    "rs-gaap:PartnersCapitalOther",
  }
)


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
    create_library_taxonomy_elements,
  )
  from robosystems.taxonomy import load_taxonomy_package

  packages = {
    standard: load_taxonomy_package(_seed_path(standard, version))
    for standard, version in _PINS.items()
  }
  session = _Session(bind=conn)
  try:
    # 1. The seven concepts. Every other element and structure already
    #    exists and is left alone.
    rs_gaap = packages["rs-gaap"]
    only_new = rs_gaap.model_copy(
      update={"elements": [e for e in rs_gaap.elements if e.qname in _NEW_CONCEPTS]}
    )
    _taxonomy_id, counts = create_library_taxonomy_elements(session, only_new)
    session.flush()
    print(f"  [rs-gaap] elements={counts['elements']}")

    # 2. The three roll-forwards' arcs: the new rows insert, the cash flow
    #    concepts they replace retire.
    counts = create_library_arcs(session, packages["rs-gaap-presentation"])
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
    # The arcs resolve against elements; a schema never provisioned has none.
    if conn.execute(text(f"SELECT 1 FROM {schema}.elements LIMIT 1")).scalar() is None:
      print(f"  [equity rows -> {schema}] skipped: no elements provisioned")
      return
    stats = resync_library_into_tenant(conn, schema, pin=dict(_PINS))
    print(
      f"  [equity rows -> {schema}] elements={stats.elements} "
      f"structures={stats.structures} associations={stats.associations}"
    )

  for_each_tenant_schema(conn, _resync)


def downgrade() -> None:
  """Intentionally a no-op.

  The upgrade brings library content up to the committed framework source;
  taking the rows back would put the equity statement back on cash flow
  concepts, unable to show a movement that moved no cash. Rolling the schema
  back does not require rolling the taxonomy back; re-run the upgrade after
  any downgrade/upgrade cycle if content ever diverges.
  """
