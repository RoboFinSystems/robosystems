"""The noncontrolling interest's roll-forward on the equity statement.

A group with an outside owner carries the owner's share of a subsidiary as
noncontrolling interest, and the consolidated statement of changes in equity
rolls it forward beside the parent's equity: its share of income, the
outside owners' contributions, and the distributions paid to them (spec
``consolidation`` §4.4, C1). The committed packages now carry:

* ``rs-gaap``: ``NoncontrollingInterestIncreaseFromSubsidiaryEquityIssuance``
  and ``MinorityInterestDecreaseFromDistributionsToNoncontrollingInterestHolders``,
  under their us-gaap names — the two movements the SEC coverage pass ranks
  first after the share of income;
* ``rs-gaap-presentation``: ``MinorityInterest`` as a second root of the three
  roll-forward networks (``Equity-rollforward``, ``-LLC``, ``-PART``), its
  change rows the share of income
  (``NetIncomeLossAttributableToNoncontrollingInterest``) and the two
  movements.

A statement with no noncontrolling interest renders exactly as before: the
renderer opens a second roll-forward only when the interest has a balance or
a movement. ``tenant-exclude/v1.json`` was regenerated with this change so the
two movements copy, and every tenant is resynced through
``resync_library_into_tenant`` (the 0050 pattern). Tenant-authored arcs are
never touched.

Revision ID: 0053
Revises: 0052
Create Date: 2026-10-17
"""

from __future__ import annotations

from pathlib import Path

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0053"
down_revision = "0052"
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
    "rs-gaap:NoncontrollingInterestIncreaseFromSubsidiaryEquityIssuance",
    "rs-gaap:MinorityInterestDecreaseFromDistributionsToNoncontrollingInterestHolders",
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
    # A schema that was never provisioned has nothing for the arcs to resolve
    # against.
    if conn.execute(text(f"SELECT 1 FROM {schema}.elements LIMIT 1")).scalar() is None:
      print(f"  [noncontrolling-interest rows -> {schema}] skipped: no elements")
      return
    stats = resync_library_into_tenant(conn, schema, pin=dict(_PINS))
    print(
      f"  [noncontrolling-interest rows -> {schema}] elements={stats.elements} "
      f"structures={stats.structures} associations={stats.associations} "
      f"element_traits={stats.element_traits} rules={stats.rules}"
    )

  for_each_tenant_schema(conn, _resync)


def downgrade() -> None:
  """Intentionally a no-op: library content tracks the committed source."""
