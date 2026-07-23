"""metrics M-2 — reseed the grown rs-metric catalog + element item_type

The M-2 grammar (avg() + DuPont composition + unit/format families) grows
the rs-metric/v1 package in place: five new concepts (ReturnOnEquity,
NetProfitMargin, AssetTurnover, EquityMultiplier, ReturnOnEquityDuPont),
five presentation arcs (orders 6-10), five Derive rules, and an
``itemType`` format family on every catalog concept (monetary | ratio |
percent | multiple | days) that ``compute-metrics`` maps to fact units
and viewers map to display formats.

No CHECK changes: 0022 already admits ``'metric'`` FactSets, ``'Derive'``
rules, and the ``'rs-metric'`` element source, and ``elements.item_type``
is an existing untyped column (open vocabulary by design). The work is
pure content: re-run the 0022 seed passes against the updated artifact —
``create_library_taxonomy_elements`` upserts carry ``item_type`` onto the
original five concepts and insert the new five; arcs and rules follow —
then fan the deltas into every tenant via ``resync_library_into_tenant``
(elements and rules update in place, new rows insert additively) under
the 0016 ``SET LOCAL robosystems.library_resync`` GUC.

Fresh deployments get all of this from 0002 + provisioning against the
updated artifact; this migration is the backfill path for existing
databases. Deploy-coupled with the loader/writer code that reads and
copies ``item_type`` — it executes that code at migrate time.

The downgrade deletes the five new concepts' content (facts, rules,
arcs, labels/references/traits, elements) per schema and nulls
``item_type`` on the remaining rs-metric elements. It fails if
tenant-authored content references the new concepts — data the operator
must triage, not silently drop.

Revision ID: 0023
Revises: 0022
Create Date: 2026-07-23

"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0023"
down_revision = "0022"
branch_labels = None
depends_on = None

_PACKAGE_STANDARD = "rs-metric"
_PACKAGE_VERSION = "v1"

_NEW_QNAMES = (
  "rs-metric:ReturnOnEquity",
  "rs-metric:NetProfitMargin",
  "rs-metric:AssetTurnover",
  "rs-metric:EquityMultiplier",
  "rs-metric:ReturnOnEquityDuPont",
)


def _seed_path():
  from robosystems.taxonomy.discovery import FRAMEWORKS_DIR

  return (
    FRAMEWORKS_DIR
    / "rs-gaap"
    / "packages"
    / _PACKAGE_STANDARD
    / _PACKAGE_VERSION
    / "taxonomy.jsonld"
  )


def _reseed_public_library(conn) -> None:
  """Re-run the 0022 seed passes — upserts grow the catalog in place."""
  from sqlalchemy.orm import Session as _Session

  from robosystems.operations.taxonomy_block.library_creator import (
    create_library_arcs,
    create_library_rules,
    create_library_taxonomy_elements,
    prune_empty_default_structures,
  )
  from robosystems.taxonomy import load_taxonomy_package

  package = load_taxonomy_package(_seed_path())
  session = _Session(bind=conn)
  try:
    _, counts = create_library_taxonomy_elements(session, package)
    session.flush()
    print(
      f"  [rs-metric M-2 pass 1] elements={counts['elements']} "
      f"structures={counts['structures']}"
    )
    counts = create_library_arcs(session, package)
    session.flush()
    print(
      f"  [rs-metric M-2 pass 2] associations={counts['associations']} "
      f"(skipped={counts['associations_skipped']})"
    )
    counts = create_library_rules(session, package)
    session.flush()
    print(
      f"  [rs-metric M-2 pass 3] rules={counts['rules']} "
      f"(skipped={counts['rules_skipped']})"
    )
    pruned = prune_empty_default_structures(session)
    session.flush()
    if pruned:
      print(f"  [rs-metric M-2 pass 4] pruned {pruned} empty default structure(s)")
  finally:
    session.close()


def upgrade() -> None:
  from robosystems.taxonomy.writer import SET_LIBRARY_RESYNC, resync_library_into_tenant

  conn = op.get_bind()

  # 1. Grow the public catalog (upserts: item_type onto the original five,
  #    the new five concepts/arcs/rules inserted).
  _reseed_public_library(conn)

  # 2. Fan the deltas into every existing tenant schema. SET LOCAL is
  #    transaction-scoped — it covers the whole fan-out and vanishes on
  #    commit. Mandatory: the new arcs INSERT into a library-seeded
  #    structure, which the 0016 immutability triggers reject otherwise.
  conn.execute(text(SET_LIBRARY_RESYNC))

  def _copy(conn, schema: str) -> None:
    stats = resync_library_into_tenant(
      conn, schema, pin={_PACKAGE_STANDARD: _PACKAGE_VERSION}
    )
    print(
      f"  [rs-metric M-2 → {schema}] elements={stats.elements} "
      f"associations={stats.associations} rules={stats.rules}"
    )

  for_each_tenant_schema(conn, _copy)


def _delete_new_concept_content(conn, schema: str) -> None:
  """Delete the five M-2 concepts' content from one schema, FK-safe."""
  from robosystems.utils.uuid import generate_deterministic_uuid

  taxonomy_id = generate_deterministic_uuid(
    f"{_PACKAGE_STANDARD}:{_PACKAGE_VERSION}", namespace="taxonomy"
  )
  s = schema
  params = {"tid": taxonomy_id, "qnames": list(_NEW_QNAMES)}
  statements = [
    # verification_results FK rules NOT NULL with no ON DELETE.
    f"DELETE FROM {s}.verification_results WHERE rule_id IN "
    f"(SELECT r.id FROM {s}.rules r JOIN {s}.elements e "
    f"ON r.target_element_id = e.id "
    f"WHERE e.taxonomy_id = :tid AND e.qname = ANY(:qnames))",
    f"DELETE FROM {s}.facts WHERE element_id IN "
    f"(SELECT id FROM {s}.elements WHERE taxonomy_id = :tid "
    f"AND qname = ANY(:qnames))",
    f"DELETE FROM {s}.rules WHERE target_element_id IN "
    f"(SELECT id FROM {s}.elements WHERE taxonomy_id = :tid "
    f"AND qname = ANY(:qnames))",
    f"DELETE FROM {s}.associations WHERE to_element_id IN "
    f"(SELECT id FROM {s}.elements WHERE taxonomy_id = :tid "
    f"AND qname = ANY(:qnames))",
    f"DELETE FROM {s}.element_traits WHERE element_id IN "
    f"(SELECT id FROM {s}.elements WHERE taxonomy_id = :tid "
    f"AND qname = ANY(:qnames))",
    f"DELETE FROM {s}.element_labels WHERE element_id IN "
    f"(SELECT id FROM {s}.elements WHERE taxonomy_id = :tid "
    f"AND qname = ANY(:qnames))",
    f"DELETE FROM {s}.element_references WHERE element_id IN "
    f"(SELECT id FROM {s}.elements WHERE taxonomy_id = :tid "
    f"AND qname = ANY(:qnames))",
    f"DELETE FROM {s}.elements WHERE taxonomy_id = :tid AND qname = ANY(:qnames)",
    f"UPDATE {s}.elements SET item_type = NULL WHERE taxonomy_id = :tid",
  ]
  for stmt in statements:
    conn.execute(text(stmt), params)


def downgrade() -> None:
  from robosystems.taxonomy.writer import SET_LIBRARY_RESYNC

  conn = op.get_bind()

  # Library-row deletes in tenant schemas need the immutability bypass.
  conn.execute(text(SET_LIBRARY_RESYNC))

  def _delete(conn, schema: str) -> None:
    _delete_new_concept_content(conn, schema)

  for_each_tenant_schema(conn, _delete)
  _delete_new_concept_content(conn, "public")
