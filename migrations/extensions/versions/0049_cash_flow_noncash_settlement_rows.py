"""Cash flow: non-cash adjustments and the card's own working-capital row, in
every Reporting Style.

Spec ``cash-flow-noncash-and-settlement``. The committed packages now carry:

* ``rs-gaap``: ``CapitalExpendituresIncurredButNotYetPaid`` and
  ``NoncashOrPartNoncashAcquisitionValueOfAssetsAcquired1``, under their
  us-gaap names, and ``SupplementalCashFlowInformationAbstract``, the header
  they sit under;
* ``rs-gaap-presentation``: on ``CashFlow-indirect``, ``-LLC`` and ``-PART``,
  four operating adjustments — ``GainLossOnDispositionOfAssets``,
  ``AssetImpairmentCharges`` and ``GainsLossesOnExtinguishmentOfDebt`` (each
  presented from its income statement fact, as depreciation is) and
  ``OtherNoncashIncomeExpense`` (which the engine computes from entries that
  move no cash) — and ``IncreaseDecreaseInOtherCurrentLiabilities``; and a
  supplemental section, a second root that sums into nothing, carrying the
  two non-cash disclosures;
* ``rs-gaap-calculations``: a calc arc for each under operating cash, gains
  subtracted; the derivation of ``OtherLiabilitiesCurrent`` moves from the
  reconciling leaf ``IncreaseDecreaseInOtherOperatingCapitalNet`` to
  ``IncreaseDecreaseInOtherCurrentLiabilities``, so the reconciling leaf
  holds only the reconciling adjustment;
* ``rs-gaap-rollup-rules``: the operating-cash RollUp, whose children grew,
  regenerated.

A statement with none of these facts
renders as before, except that a change in other current liabilities now
has its own row instead of sitting in the reconciling adjustment.

This migration brings ``public`` up to the committed source and fans the
pinned packages into every tenant through ``resync_library_into_tenant``
(the 0045 pattern), which retires the replaced derivation arc in each tenant
copy. ``tenant-exclude/v1.json`` was regenerated with this change so the new
rows copy. Tenant-authored arcs are never touched.

Revision ID: 0049
Revises: 0048
Create Date: 2026-10-16
"""

from __future__ import annotations

from pathlib import Path

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0049"
down_revision = "0048"
branch_labels = None
depends_on = None

# tenant-fanout: not-required (data only; every tenant is reached through
# resync_library_into_tenant below)

# In load order. Elements and structures seed before arcs resolve them.
_PINS = {
  "rs-gaap": "v1",
  "rs-gaap-traits": "v1",
  "rs-gaap-presentation": "v1",
  "rs-gaap-calculations": "v1",
  "rs-gaap-rollup-rules": "v1",
}

_NEW_CONCEPTS = frozenset(
  {
    "rs-gaap:SupplementalCashFlowInformationAbstract",
    "rs-gaap:CapitalExpendituresIncurredButNotYetPaid",
    "rs-gaap:NoncashOrPartNoncashAcquisitionValueOfAssetsAcquired1",
  }
)

# Packages whose arcs changed.
_ARC_PACKAGES = ("rs-gaap-calculations", "rs-gaap-presentation")


def _seed_path(standard: str, version: str) -> Path:
  from robosystems.taxonomy.discovery import FRAMEWORKS_DIR

  return (
    FRAMEWORKS_DIR / "rs-gaap" / "packages" / standard / version / "taxonomy.jsonld"
  )


def _refresh_public(conn) -> None:
  """Bring ``public`` up to the committed JSON-LD for the changed packages."""
  from sqlalchemy.orm import Session as _Session

  from robosystems.operations.taxonomy_block.library_creator import (
    create_library_arcs,
    create_library_rules,
    create_library_taxonomy_elements,
  )
  from robosystems.taxonomy import load_taxonomy_package

  session = _Session(bind=conn)
  try:
    # 1. The new concepts. Every other element and structure already exists
    #    and is left alone.
    rs_gaap = load_taxonomy_package(_seed_path("rs-gaap", _PINS["rs-gaap"]))
    only_new = rs_gaap.model_copy(
      update={"elements": [e for e in rs_gaap.elements if e.qname in _NEW_CONCEPTS]}
    )
    _taxonomy_id, counts = create_library_taxonomy_elements(session, only_new)
    session.flush()
    print(f"  [rs-gaap] elements={counts['elements']}")

    # 2. Arcs; new ones insert and the replaced derivation arc retires.
    for standard in _ARC_PACKAGES:
      counts = create_library_arcs(
        session, load_taxonomy_package(_seed_path(standard, _PINS[standard]))
      )
      session.flush()
      print(
        f"  [{standard}] arcs={counts['associations']} "
        f"skipped={counts['associations_skipped']} "
        f"retired={counts['associations_retired']}"
      )

    # 3. Upserts on the rule id through the canonical variable writer, so the
    #    regenerated RollUp replaces the old one in place.
    counts = create_library_rules(
      session,
      load_taxonomy_package(
        _seed_path("rs-gaap-rollup-rules", _PINS["rs-gaap-rollup-rules"])
      ),
    )
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
    stats = resync_library_into_tenant(conn, schema, pin=dict(_PINS))
    print(
      f"  [cash flow rows -> {schema}] elements={stats.elements} "
      f"structures={stats.structures} associations={stats.associations} "
      f"element_traits={stats.element_traits} rules={stats.rules}"
    )

  for_each_tenant_schema(conn, _resync)


def downgrade() -> None:
  """Intentionally a no-op.

  The upgrade brings library content up to the committed framework source;
  rolling the schema back does not require rolling the taxonomy back. Re-run
  the upgrade after any downgrade/upgrade cycle if content ever diverges.
  """
