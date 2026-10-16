"""Coverage wave: share-based compensation and professional fees, in every
Reporting Style.

A coverage pass over 60 recent 10-Ks in the SEC repository (spec
``style-extensions`` §6) ranked the face-statement rows filers use that the
Styles do not present. Share-based compensation is the largest gap — on 67%
of the cash flows and 45% of the equity statements — and professional fees
is the most common operating expense line the income statement lacks.

The committed packages now carry:

* ``rs-gaap``: ``AdjustmentsToAdditionalPaidInCapitalSharebasedCompensation…``
  (the corporate roll-forward's row) and
  ``PartnersCapitalAccountUnitBasedCompensation`` (the partnership and LLC
  roll-forwards'), under their us-gaap names;
* ``rs-gaap-traits``: the expense EFS trait on ``ShareBasedCompensation``, so
  a stock-compensation expense account maps to it the way a depreciation
  account maps to ``DepreciationDepletionAndAmortization``;
* ``rs-gaap-presentation``: ``ShareBasedCompensation`` under operating
  expenses on ``IS-multistep`` and as an operating add-back on
  ``CashFlow-indirect``, ``-LLC`` and ``-PART``; ``ProfessionalFees`` under
  operating expenses; the share-based compensation movement on each equity
  roll-forward;
* ``rs-gaap-calculations``: a calc arc for each new income statement and cash
  flow row under its section subtotal;
* ``rs-gaap-rollup-rules``: the operating-expense and operating-cash RollUps,
  whose children grew, regenerated.

A statement with none of these facts renders as before: every new row is a
leaf that drops when all-zero, and no subtotal's existing children moved.

This migration brings ``public`` up to the committed source and fans the
pinned packages into every tenant through ``resync_library_into_tenant``
(the 0045 pattern). ``tenant-exclude/v1.json`` was regenerated with this
change so ``ProfessionalFees`` copies. Tenant-authored arcs are never
touched.

Revision ID: 0048
Revises: 0047
Create Date: 2026-10-16
"""

from __future__ import annotations

from pathlib import Path

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0048"
down_revision = "0047"
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
    "rs-gaap:AdjustmentsToAdditionalPaidInCapitalSharebasedCompensationRequisiteServicePeriodRecognitionValue",
    "rs-gaap:PartnersCapitalAccountUnitBasedCompensation",
  }
)

# Packages whose arcs or assignments changed; the others changed only in
# elements or rules.
_ARC_PACKAGES = ("rs-gaap-traits", "rs-gaap-calculations", "rs-gaap-presentation")


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
    create_library_taxonomy_elements,
  )
  from robosystems.taxonomy import load_taxonomy_package

  packages = {
    standard: load_taxonomy_package(_seed_path(standard, version))
    for standard, version in _PINS.items()
  }
  session = _Session(bind=conn)
  try:
    # 1. The two concepts. Every other element and structure already exists
    #    and is left alone.
    rs_gaap = packages["rs-gaap"]
    only_new = rs_gaap.model_copy(
      update={"elements": [e for e in rs_gaap.elements if e.qname in _NEW_CONCEPTS]}
    )
    _taxonomy_id, counts = create_library_taxonomy_elements(session, only_new)
    session.flush()
    print(f"  [rs-gaap] elements={counts['elements']}")

    # 2. Arcs and trait assignments; new ones insert, nothing here retires.
    for standard in _ARC_PACKAGES:
      counts = create_library_arcs(session, packages[standard])
      session.flush()
      print(
        f"  [{standard}] arcs={counts['associations']} "
        f"skipped={counts['associations_skipped']} "
        f"retired={counts['associations_retired']} "
        f"traits={counts['trait_assignments']}"
      )

    # 3. Upserts on the rule id through the canonical variable writer, so
    #    the two regenerated RollUps replace the old ones in place and every
    #    rule keeps the shape the engine reads.
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
    stats = resync_library_into_tenant(conn, schema, pin=dict(_PINS))
    print(
      f"  [coverage wave -> {schema}] elements={stats.elements} "
      f"structures={stats.structures} associations={stats.associations} "
      f"element_traits={stats.element_traits} rules={stats.rules}"
    )

  for_each_tenant_schema(conn, _resync)


def downgrade() -> None:
  """Intentionally a no-op.

  The upgrade brings library content up to the committed framework source;
  taking the rows back would strand any tenant account mapped to share-based
  compensation or professional fees. Rolling the schema back does not
  require rolling the taxonomy back; re-run the upgrade after any
  downgrade/upgrade cycle if content ever diverges.
  """
