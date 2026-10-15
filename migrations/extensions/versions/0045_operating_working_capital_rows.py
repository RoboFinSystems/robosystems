"""Operating rows for the declared working-capital exceptions.

The indirect cash flow statement's operating section derives its
working-capital rows from balance-sheet movements (``derivation`` arcs in
``rs-gaap-calculations``). Six balance-sheet leaves had no row and were
declared exceptions in ``tests/taxonomy/test_rs_gaap_flow_coverage.py``:
deferred revenue (current and noncurrent), other current assets, the
operating lease liability and the net deferred tax liability. Their movements
landed in the operating reconciling adjustment — the right section with the
wrong row (spec ``transaction-enrichment`` §13 A).

The committed packages now carry:

* ``rs-gaap``: ``IncreaseDecreaseInContractWithCustomerLiability`` and
  ``IncreaseDecreaseInOperatingLeaseLiability``, under their us-gaap names;
* ``rs-gaap-traits``: the indirect-method reconciling-item trait on those two,
  on ``IncreaseDecreaseInOtherCurrentAssets`` and on
  ``DeferredIncomeTaxExpenseBenefit``, beside their operating trait;
* ``rs-gaap-calculations``: a derivation from each of the six leaves to its
  row (asset up -1, liability up +1), and a calc arc from the operating
  subtotal to each of the four rows;
* ``rs-gaap-presentation``: the four rows on ``CashFlow-indirect``, ``-LLC``
  and ``-PART``, deferred tax beside depreciation and the others at the end of
  the working-capital block;
* ``rs-gaap-rollup-rules``: the operating RollUp enumerates the four new
  children (the shared builder's expression).

This migration brings ``public`` up to the committed source and fans the
pinned packages into every tenant through ``resync_library_into_tenant``
(the 0044 pattern). ``tenant-exclude/v1.json`` was regenerated with this
change so the two new concepts copy; nothing moves between Styles, so no
composition row is repointed. Tenant-authored arcs are never touched.

Revision ID: 0045
Revises: 0044
Create Date: 2026-10-15
"""

from __future__ import annotations

import json
from pathlib import Path

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0045"
down_revision = "0044"
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
    "rs-gaap:IncreaseDecreaseInContractWithCustomerLiability",
    "rs-gaap:IncreaseDecreaseInOperatingLeaseLiability",
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
    for standard, package in packages.items():
      only_new = package.model_copy(
        update={"elements": [e for e in package.elements if e.qname in _NEW_CONCEPTS]}
      )
      _taxonomy_id, counts = create_library_taxonomy_elements(session, only_new)
      session.flush()
      print(
        f"  [{standard}] elements={counts['elements']} "
        f"structures={counts['structures']}"
      )

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
  finally:
    session.close()

  _update_public_rules(conn, packages["rs-gaap-rollup-rules"])


def _update_public_rules(conn, package) -> None:
  """Rewrite the rollup rules' expression and variables in place (rule ids
  key on ``standard:local_id``, which the regeneration preserved)."""
  from robosystems.operations.taxonomy_block.library_creator import _rule_id

  updated = 0
  for rule in package.rules:
    result = conn.execute(
      text("""
        UPDATE public.rules
           SET rule_expression = :expr,
               rule_variables  = CAST(:vars AS jsonb)
         WHERE id = :id
           AND (rule_expression IS DISTINCT FROM :expr
                OR rule_variables IS DISTINCT FROM CAST(:vars AS jsonb))
      """),
      {
        "id": _rule_id(package.standard, rule.id),
        "expr": rule.rule_expression,
        "vars": json.dumps(
          [
            {"variableName": v.variable_name, "variableQname": v.variable_qname}
            for v in rule.rule_variables
          ]
        ),
      },
    )
    updated += result.rowcount or 0
  print(f"  [{package.standard}] rules updated in public={updated}")


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
      f"  [working-capital rows -> {schema}] elements={stats.elements} "
      f"structures={stats.structures} associations={stats.associations} "
      f"element_traits={stats.element_traits} rules={stats.rules}"
    )

  for_each_tenant_schema(conn, _resync)


def downgrade() -> None:
  """Intentionally a no-op.

  The upgrade brings library content up to the committed framework source;
  taking the rows back would put deferred revenue and the lease liability
  back into the operating adjustment. Rolling the schema back does not
  require rolling the taxonomy back; re-run the upgrade after any
  downgrade/upgrade cycle if content ever diverges.
  """
