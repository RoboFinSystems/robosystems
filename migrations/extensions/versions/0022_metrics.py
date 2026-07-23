"""metrics — seed the rs-metric catalog package + Derive rules + metric FactSets

The Metrics MVP (M-1) needs three vocabulary widenings plus a new library
package in deployments whose library was seeded before rs-metric existed:

- ``fact_sets``: widen ``check_fact_set_type`` to admit ``'metric'``
  (standing computed-metric time series — one FactSet per
  (structure, entity, period_end), filled by compute-metrics).
- ``rules``: widen ``check_rule_pattern`` to admit ``'Derive'`` (rules that
  COMPUTE a value from bound facts rather than verify one).
- ``elements``: widen ``check_element_source`` to admit ``'rs-metric'``
  (the metric catalog's namespace prefix, following the disclosures /
  checklist / styles source-per-package convention from 0007).

Then the content: seed ``frameworks/rs-gaap/packages/rs-metric/v1`` into the
public library (0002's pass structure, one package), mirror the manifest's
new ``framework_packages`` junction row (0007's deterministic-uuid upsert),
and fan the package into every existing tenant schema via
``resync_library_into_tenant`` with a single-package pin. The resync runs
under ``SET LOCAL robosystems.library_resync = 'on'`` (0016) — the new
package's arcs INSERT into a library-seeded structure, which the
immutability triggers reject otherwise.

Fresh deployments get all of this from 0002 + provisioning (the manifest now
lists rs-metric; ``_widen_library_checks`` and the models carry the widened
vocabulary), so this migration is the backfill path for existing databases.

Note: the downgrade deletes rs-metric content (public + tenants, including
any computed metric FactSets) and re-narrows the constraints. It will fail
if tenant-authored content references rs-metric elements (e.g. custom arcs
into the catalog). Expected once the values are in use.

Revision ID: 0022
Revises: 0021
Create Date: 2026-07-19

"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0022"
down_revision = "0021"
branch_labels = None
depends_on = None

_PACKAGE_STANDARD = "rs-metric"
_PACKAGE_VERSION = "v1"
_FRAMEWORK = "rs-gaap"
_FRAMEWORK_VERSION = "v1"
_PACKAGE_ORDINAL = 14

_FACTSET_WIDENED = (
  "factset_type IN ('report', 'schedule', 'custom', 'disclosure', 'metric')"
)
_FACTSET_ORIGINAL = "factset_type IN ('report', 'schedule', 'custom', 'disclosure')"

_RULE_PATTERN_WIDENED = (
  "rule_pattern IS NULL OR rule_pattern IN ("
  "'Adjustment', 'CoExists', 'Derive', 'EqualTo', 'Exists', 'GreaterThan', "
  "'GreaterThanOrEqualToZero', 'LessThan', 'RollForward', 'RollUp', "
  "'SumEquals', 'Variance'"
  ")"
)
_RULE_PATTERN_ORIGINAL = (
  "rule_pattern IS NULL OR rule_pattern IN ("
  "'Adjustment', 'CoExists', 'EqualTo', 'Exists', 'GreaterThan', "
  "'GreaterThanOrEqualToZero', 'LessThan', 'RollForward', 'RollUp', "
  "'SumEquals', 'Variance'"
  ")"
)

_SOURCE_WIDENED = (
  "source IN ("
  "'fac', 'rs-gaap', 'us-gaap', 'ifrs', "
  "'quickbooks', 'xero', 'plaid', 'native', 'import', 'system', "
  # 'rs-driver' — fresh databases seed the forecast lever catalog at
  # 0002 (dynamic manifest), so the rows exist when this DROP+ADD
  # re-validates them; deployed databases get the value at 0024.
  "'disclosures', 'checklist', 'styles', 'cm', 'rs-metric', 'rs-driver'"
  ")"
)
_SOURCE_ORIGINAL = (
  "source IN ("
  "'fac', 'rs-gaap', 'us-gaap', 'ifrs', "
  "'quickbooks', 'xero', 'plaid', 'native', 'import', 'system', "
  "'disclosures', 'checklist', 'styles', 'cm'"
  ")"
)


def _widen(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.add_check("fact_sets", "check_fact_set_type", _FACTSET_WIDENED)
  t.add_check("rules", "check_rule_pattern", _RULE_PATTERN_WIDENED)
  t.add_check("elements", "check_element_source", _SOURCE_WIDENED)


def _narrow(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.add_check("fact_sets", "check_fact_set_type", _FACTSET_ORIGINAL)
  t.add_check("rules", "check_rule_pattern", _RULE_PATTERN_ORIGINAL)
  t.add_check("elements", "check_element_source", _SOURCE_ORIGINAL)


def _seed_path():
  from robosystems.taxonomy.discovery import FRAMEWORKS_DIR

  return (
    FRAMEWORKS_DIR
    / _FRAMEWORK
    / "packages"
    / _PACKAGE_STANDARD
    / _PACKAGE_VERSION
    / "taxonomy.jsonld"
  )


def _seed_public_library(conn) -> None:
  """Load rs-metric into the public library — 0002's three passes, one package."""
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
      f"  [rs-metric pass 1] elements={counts['elements']} "
      f"structures={counts['structures']}"
    )
    counts = create_library_arcs(session, package)
    session.flush()
    print(
      f"  [rs-metric pass 2] associations={counts['associations']} "
      f"(skipped={counts['associations_skipped']})"
    )
    counts = create_library_rules(session, package)
    session.flush()
    print(
      f"  [rs-metric pass 3] rules={counts['rules']} "
      f"(skipped={counts['rules_skipped']})"
    )
    # The auto-created "rs-metric — default structure" fallback ends up
    # empty (every arc routes to the named Key Financial Metrics role);
    # prune it so it isn't copied into every tenant. Same name-scoped
    # helper 0002 uses — other packages' defaults are already gone.
    pruned = prune_empty_default_structures(session)
    session.flush()
    print(f"  [rs-metric pass 4] pruned {pruned} empty default structure(s)")
  finally:
    session.close()


def _upsert_framework_package_row(conn) -> None:
  """Mirror the manifest's new packages[] entry — 0007's junction upsert."""
  from datetime import UTC, datetime

  from robosystems.utils.uuid import generate_deterministic_uuid

  fid = generate_deterministic_uuid(
    f"{_FRAMEWORK}:{_FRAMEWORK_VERSION}", namespace="framework"
  )
  fpid = generate_deterministic_uuid(
    f"{fid}:{_PACKAGE_STANDARD}:{_PACKAGE_VERSION}", namespace="framework_package"
  )
  conn.execute(
    text("""
      INSERT INTO public.framework_packages (
        id, framework_id, package_standard, package_version,
        ordinal, is_required, metadata, created_at, created_by
      ) VALUES (
        :id, :fid, :std, :ver, :ordinal, true,
        '{}'::jsonb, :now, 'library-seeder'
      )
      ON CONFLICT (id) DO UPDATE SET
        ordinal     = EXCLUDED.ordinal,
        is_required = EXCLUDED.is_required
    """),
    {
      "id": fpid,
      "fid": fid,
      "std": _PACKAGE_STANDARD,
      "ver": _PACKAGE_VERSION,
      "ordinal": _PACKAGE_ORDINAL,
      "now": datetime.now(UTC),
    },
  )


def upgrade() -> None:
  from robosystems.taxonomy.writer import SET_LIBRARY_RESYNC, resync_library_into_tenant

  conn = op.get_bind()

  # 1. Vocabulary widenings — must precede the seed so rs-metric rows insert.
  _widen(conn, "public")
  for_each_tenant_schema(conn, _widen)

  # 2. Public library seed + manifest junction row.
  _seed_public_library(conn)
  _upsert_framework_package_row(conn)

  # 3. Fan the new package into every existing tenant schema. SET LOCAL is
  #    transaction-scoped — it covers the whole fan-out and vanishes on
  #    commit. Mandatory: the package's arcs INSERT into a library-seeded
  #    structure, which the 0016 immutability triggers reject otherwise
  #    (resync_library_into_tenant asserts the GUC up front).
  conn.execute(text(SET_LIBRARY_RESYNC))

  def _copy(conn, schema: str) -> None:
    stats = resync_library_into_tenant(
      conn, schema, pin={_PACKAGE_STANDARD: _PACKAGE_VERSION}
    )
    print(
      f"  [rs-metric → {schema}] elements={stats.elements} "
      f"structures={stats.structures} associations={stats.associations} "
      f"rules={stats.rules}"
    )

  for_each_tenant_schema(conn, _copy)


def _delete_package_content(conn, schema: str) -> None:
  """Delete rs-metric content from one schema in FK-safe order.

  Fails (by design) if tenant-authored content still references rs-metric
  elements in ways not covered here — that's data the operator must
  triage, not silently drop.
  """
  from robosystems.utils.uuid import generate_deterministic_uuid

  taxonomy_id = generate_deterministic_uuid(
    f"{_PACKAGE_STANDARD}:{_PACKAGE_VERSION}", namespace="taxonomy"
  )
  s = schema
  params = {"tid": taxonomy_id}
  statements = [
    # verification_results FK rules NOT NULL with no ON DELETE.
    f"DELETE FROM {s}.verification_results WHERE rule_id IN "
    f"(SELECT id FROM {s}.rules WHERE taxonomy_id = :tid)",
    f"DELETE FROM {s}.rules WHERE taxonomy_id = :tid",
    # Metric FactSets reference the catalog structure; facts cascade.
    f"DELETE FROM {s}.fact_sets WHERE structure_id IN "
    f"(SELECT id FROM {s}.structures WHERE taxonomy_id = :tid)",
    f"DELETE FROM {s}.associations WHERE structure_id IN "
    f"(SELECT id FROM {s}.structures WHERE taxonomy_id = :tid)",
    f"DELETE FROM {s}.structures WHERE taxonomy_id = :tid",
    f"DELETE FROM {s}.element_traits WHERE element_id IN "
    f"(SELECT id FROM {s}.elements WHERE taxonomy_id = :tid)",
    f"DELETE FROM {s}.element_labels WHERE element_id IN "
    f"(SELECT id FROM {s}.elements WHERE taxonomy_id = :tid)",
    f"DELETE FROM {s}.element_references WHERE element_id IN "
    f"(SELECT id FROM {s}.elements WHERE taxonomy_id = :tid)",
    f"DELETE FROM {s}.elements WHERE taxonomy_id = :tid",
    f"DELETE FROM {s}.taxonomies WHERE id = :tid",
  ]
  for stmt in statements:
    conn.execute(text(stmt), params)


def downgrade() -> None:
  from robosystems.taxonomy.writer import SET_LIBRARY_RESYNC
  from robosystems.utils.uuid import generate_deterministic_uuid

  conn = op.get_bind()

  # Library-row deletes in tenant schemas need the immutability bypass.
  conn.execute(text(SET_LIBRARY_RESYNC))

  def _delete(conn, schema: str) -> None:
    _delete_package_content(conn, schema)

  for_each_tenant_schema(conn, _delete)
  _delete_package_content(conn, "public")

  fid = generate_deterministic_uuid(
    f"{_FRAMEWORK}:{_FRAMEWORK_VERSION}", namespace="framework"
  )
  fpid = generate_deterministic_uuid(
    f"{fid}:{_PACKAGE_STANDARD}:{_PACKAGE_VERSION}", namespace="framework_package"
  )
  conn.execute(
    text("DELETE FROM public.framework_packages WHERE id = :id"), {"id": fpid}
  )

  # Re-narrow the vocabulary — fails if 'metric' FactSets or 'Derive'
  # rules remain outside the deleted package (tenant-authored), which is
  # data the operator must triage first.
  _narrow(conn, "public")
  for_each_tenant_schema(conn, _narrow)
