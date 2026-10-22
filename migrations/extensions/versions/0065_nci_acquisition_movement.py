"""The noncontrolling interest's acquisition row on the equity statement.

A member bought with outside owners brings their interest into the group
with it: the engine's investment elimination in the month it joins credits
noncontrolling interest with their share of the equity it brought, untagged
since no cash moved, and the equity statement's roll-forward of that
interest had no row for the movement, so the group could not close the
month a member was acquired in (spec ``group-lifecycle`` Phase 4, found on
the exemplar). 0064 added the row for the interest leaving; this is the row
for it arriving.

The committed packages now carry:

* ``rs-gaap``: ``NoncontrollingInterestIncreaseFromBusinessCombination``,
  under its us-gaap name.
* ``rs-gaap-presentation``: the row under ``MinorityInterest`` in each of the
  three equity roll-forward networks (corporate, LLC, partnership), after
  the contributions row.

This migration brings ``public`` up to the committed source and fans the
pinned packages into every tenant through ``resync_library_into_tenant``
(the 0061 pattern). The row is presented, so it is in the working set
``tenant-exclude/v1.json`` keeps. Tenant-authored arcs are never touched.

Revision ID: 0065
Revises: 0064
Create Date: 2026-10-22
"""

from __future__ import annotations

from pathlib import Path

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0065"
down_revision = "0064"
branch_labels = None
depends_on = None

# tenant-fanout: not-required (data only; every tenant is reached through
# resync_library_into_tenant below)

_PINS = {
  "rs-gaap": "v1",
  "rs-gaap-presentation": "v1",
}

_NEW_CONCEPTS = frozenset(
  {"rs-gaap:NoncontrollingInterestIncreaseFromBusinessCombination"}
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
    # The arcs resolve against elements; a schema never provisioned has none.
    if conn.execute(text(f"SELECT 1 FROM {schema}.elements LIMIT 1")).scalar() is None:
      print(f"  [nci acquisition -> {schema}] skipped: no elements provisioned")
      return
    stats = resync_library_into_tenant(conn, schema, pin=dict(_PINS))
    print(
      f"  [nci acquisition -> {schema}] elements={stats.elements} "
      f"structures={stats.structures} associations={stats.associations}"
    )

  for_each_tenant_schema(conn, _resync)


def downgrade() -> None:
  """Intentionally a no-op: library content tracks the committed source."""
