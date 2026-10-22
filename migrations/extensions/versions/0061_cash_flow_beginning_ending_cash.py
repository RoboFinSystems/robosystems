"""Beginning and ending cash on the cash flow statement.

Every cash flow ended at *net change in cash* and never reconciled on the
page to the balance sheet's cash. ASC 230 wants the statement to open and
close on cash, so each indirect network now rolls forward: the cash total
above net change, rendered as beginning and ending rows the way the equity
statement renders its capital.

The committed packages now carry:

* ``rs-gaap``: ``CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents``,
  the total the statement opens and closes on (ASU 2016-18), and
  ``StatementOfCashFlowsAbstract``, the header the export presents the
  roll-forward under (``serialization.bundle._apply_preferred_labels``). Both
  under their us-gaap names. The total carries no statement-classification
  trait, so it is never offered as a mapping target: it is derived from the
  balance sheet's cash rows.
* ``rs-gaap-calculations``: the total sums ``CashAndCashEquivalentsAtCarryingValue``
  and ``RestrictedCashAndInvestmentsCurrent``, the two cash rows every
  balance-sheet network presents, so ``_emit_subtotal_facts`` states it at
  each column's end.
* ``rs-gaap-presentation``: the total is the root of ``CashFlow-indirect``,
  ``-LLC`` and ``-PART``, with net change beneath it.
* ``rs-gaap-rollup-rules``: the total's roll-up rule, as every calc parent
  carries one.

This migration brings ``public`` up to the committed source and fans the
pinned packages into every tenant through ``resync_library_into_tenant`` (the
0045 pattern). The total is presented, so ``tenant-exclude/v1.json``
(regenerated with this change) copies it. Tenant-authored arcs are never
touched.

Revision ID: 0061
Revises: 0060
Create Date: 2026-10-22
"""

from __future__ import annotations

from pathlib import Path

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0061"
down_revision = "0060"
branch_labels = None
depends_on = None

# tenant-fanout: not-required (data only; every tenant is reached through
# resync_library_into_tenant below)

_PINS = {
  "rs-gaap": "v1",
  "rs-gaap-presentation": "v1",
  "rs-gaap-calculations": "v1",
  "rs-gaap-rollup-rules": "v1",
}

_NEW_CONCEPTS = frozenset(
  {
    "rs-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    "rs-gaap:StatementOfCashFlowsAbstract",
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
    create_library_rules,
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

    for standard in ("rs-gaap-presentation", "rs-gaap-calculations"):
      counts = create_library_arcs(session, load_taxonomy_package(_seed_path(standard)))
      session.flush()
      print(
        f"  [{standard}] arcs={counts['associations']} "
        f"skipped={counts['associations_skipped']} "
        f"retired={counts['associations_retired']}"
      )

    # The total's roll-up rule, beside the other calc parents'.
    counts = create_library_rules(
      session, load_taxonomy_package(_seed_path("rs-gaap-rollup-rules"))
    )
    session.flush()
    print(
      f"  [rs-gaap-rollup-rules] rules={counts['rules']} skipped={counts['rules_skipped']}"
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
      print(f"  [cash roll-forward -> {schema}] skipped: no elements provisioned")
      return
    stats = resync_library_into_tenant(conn, schema, pin=dict(_PINS))
    print(
      f"  [cash roll-forward -> {schema}] elements={stats.elements} "
      f"structures={stats.structures} associations={stats.associations} "
      f"element_traits={stats.element_traits} rules={stats.rules}"
    )

  for_each_tenant_schema(conn, _resync)


def downgrade() -> None:
  """Intentionally a no-op: library content tracks the committed source."""
