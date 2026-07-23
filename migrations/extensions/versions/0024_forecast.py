"""forecast — scenario axis on fact_sets + the rs-driver lever catalog

The Forecast MVP (F-1) needs one schema change, two vocabulary widenings,
and a new library package in deployments seeded before rs-driver existed:

- ``fact_sets``: add ``scenario_id`` (nullable, NULL = actuals) — the
  scenario axis. Non-NULL points at the owning ``forecast`` Structure
  (the block IS the scenario) with ON DELETE CASCADE so deleting the
  block removes its whole scenario slice. Partial index for the
  scenario-slice reads.
- ``structures``: widen ``check_block_type`` to admit ``'forecast'``
  (the authored scenario container: lever assertions + scenario
  identity; derived forward facts land in the EXISTING statement/metric
  block types stamped with ``scenario_id``).
- ``elements``: widen ``check_element_source`` to admit ``'rs-driver'``
  (the lever catalog's namespace prefix, following the rs-metric
  source-per-package convention from 0022).

Then the content: seed ``frameworks/rs-gaap/packages/rs-driver/v1`` into
the public library (0022's pass structure), mirror the manifest's new
``framework_packages`` junction row, and fan the package into every
existing tenant schema via ``resync_library_into_tenant`` under
``SET LOCAL robosystems.library_resync = 'on'`` (0016).

Fresh deployments get all of this from 0002 + provisioning (the manifest
now lists rs-driver; ``_widen_library_checks`` and the models carry the
widened vocabulary), so this migration is the backfill path for existing
databases. Deploy-coupled with the scenario-aware reader code: a scenario
FactSet written before readers filter on ``scenario_id`` would win the
latest-FactSet envelope reads (they order by ``period_end DESC``).

Note: the downgrade deletes every scenario FactSet and forecast block,
deletes rs-driver content (public + tenants), and re-narrows the
constraints. It will fail if tenant-authored content references
rs-driver elements outside those paths — data the operator must triage.

Revision ID: 0024
Revises: 0023
Create Date: 2026-07-23

"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0024"
down_revision = "0023"
branch_labels = None
depends_on = None

_PACKAGE_STANDARD = "rs-driver"
_PACKAGE_VERSION = "v1"
_FRAMEWORK = "rs-gaap"
_FRAMEWORK_VERSION = "v1"
_PACKAGE_ORDINAL = 15

_BLOCK_TYPE_WIDENED = (
  "block_type IN ("
  "'income_statement', 'balance_sheet', "
  "'cash_flow_statement', 'equity_statement', "
  "'comprehensive_income', "
  "'schedule', 'rollforward', 'reconciliation', 'policy', 'metric', "
  "'forecast', "
  "'chart_of_accounts', 'coa_mapping', "
  "'validation_rules', 'regulatory_disclosure', 'taxonomy_mapping', "
  "'reporting_style', "
  "'custom'"
  ")"
)
_BLOCK_TYPE_ORIGINAL = (
  "block_type IN ("
  "'income_statement', 'balance_sheet', "
  "'cash_flow_statement', 'equity_statement', "
  "'comprehensive_income', "
  "'schedule', 'rollforward', 'reconciliation', 'policy', 'metric', "
  "'chart_of_accounts', 'coa_mapping', "
  "'validation_rules', 'regulatory_disclosure', 'taxonomy_mapping', "
  "'reporting_style', "
  "'custom'"
  ")"
)

_SOURCE_WIDENED = (
  "source IN ("
  "'fac', 'rs-gaap', 'us-gaap', 'ifrs', "
  "'quickbooks', 'xero', 'plaid', 'native', 'import', 'system', "
  "'disclosures', 'checklist', 'styles', 'cm', 'rs-metric', 'rs-driver'"
  ")"
)
_SOURCE_ORIGINAL = (
  "source IN ("
  "'fac', 'rs-gaap', 'us-gaap', 'ifrs', "
  "'quickbooks', 'xero', 'plaid', 'native', 'import', 'system', "
  "'disclosures', 'checklist', 'styles', 'cm', 'rs-metric'"
  ")"
)


def _add_scenario_axis(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.add_column("fact_sets", "scenario_id", "TEXT")
  t.add_foreign_key(
    "fact_sets",
    "fk_fact_sets_scenario",
    ["scenario_id"],
    "structures",
    ["id"],
    on_delete="CASCADE",
  )
  t.create_index(
    "idx_fact_sets_scenario",
    "fact_sets",
    ["scenario_id"],
    where="scenario_id IS NOT NULL",
  )


def _drop_scenario_axis(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.drop_index("idx_fact_sets_scenario")
  t.drop_constraint("fact_sets", "fk_fact_sets_scenario")
  t.drop_column("fact_sets", "scenario_id")


def _widen(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.add_check("structures", "check_block_type", _BLOCK_TYPE_WIDENED)
  t.add_check("elements", "check_element_source", _SOURCE_WIDENED)


def _narrow(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.add_check("structures", "check_block_type", _BLOCK_TYPE_ORIGINAL)
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
  """Load rs-driver into the public library — 0022's three passes, one package."""
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
      f"  [rs-driver pass 1] elements={counts['elements']} "
      f"structures={counts['structures']}"
    )
    counts = create_library_arcs(session, package)
    session.flush()
    print(
      f"  [rs-driver pass 2] associations={counts['associations']} "
      f"(skipped={counts['associations_skipped']})"
    )
    counts = create_library_rules(session, package)
    session.flush()
    print(
      f"  [rs-driver pass 3] rules={counts['rules']} "
      f"(skipped={counts['rules_skipped']})"
    )
    pruned = prune_empty_default_structures(session)
    session.flush()
    print(f"  [rs-driver pass 4] pruned {pruned} empty default structure(s)")
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

  # 1. Scenario axis + vocabulary widenings — must precede the seed so
  #    rs-driver rows insert and forecast blocks can be authored.
  _add_scenario_axis(conn, "public")
  _widen(conn, "public")

  def _schema_ddl(conn, schema: str) -> None:
    _add_scenario_axis(conn, schema)
    _widen(conn, schema)

  for_each_tenant_schema(conn, _schema_ddl)

  # 2. Public library seed + manifest junction row.
  _seed_public_library(conn)
  _upsert_framework_package_row(conn)

  # 3. Fan the new package into every existing tenant schema. SET LOCAL is
  #    transaction-scoped — it covers the whole fan-out and vanishes on
  #    commit. Mandatory: the package's arcs INSERT into a library-seeded
  #    structure, which the 0016 immutability triggers reject otherwise.
  conn.execute(text(SET_LIBRARY_RESYNC))

  def _copy(conn, schema: str) -> None:
    stats = resync_library_into_tenant(
      conn, schema, pin={_PACKAGE_STANDARD: _PACKAGE_VERSION}
    )
    print(
      f"  [rs-driver → {schema}] elements={stats.elements} "
      f"structures={stats.structures} associations={stats.associations} "
      f"rules={stats.rules}"
    )

  for_each_tenant_schema(conn, _copy)


def _delete_scenario_content(conn, schema: str) -> None:
  """Delete every scenario slice + forecast block from one schema.

  Scenario FactSets (facts cascade) must go before forecast Structures —
  the lever set and every emitted month reference the forecast id via
  ``scenario_id``, and ``fact_sets.structure_id`` has no ON DELETE.
  """
  s = schema
  conn.execute(text(f"DELETE FROM {s}.fact_sets WHERE scenario_id IS NOT NULL"))
  conn.execute(text(f"DELETE FROM {s}.structures WHERE block_type = 'forecast'"))


def _delete_package_content(conn, schema: str) -> None:
  """Delete rs-driver content from one schema in FK-safe order."""
  from robosystems.utils.uuid import generate_deterministic_uuid

  taxonomy_id = generate_deterministic_uuid(
    f"{_PACKAGE_STANDARD}:{_PACKAGE_VERSION}", namespace="taxonomy"
  )
  s = schema
  params = {"tid": taxonomy_id}
  statements = [
    f"DELETE FROM {s}.verification_results WHERE rule_id IN "
    f"(SELECT id FROM {s}.rules WHERE taxonomy_id = :tid)",
    f"DELETE FROM {s}.rules WHERE taxonomy_id = :tid",
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
    _delete_scenario_content(conn, schema)
    _delete_package_content(conn, schema)
    _drop_scenario_axis(conn, schema)

  for_each_tenant_schema(conn, _delete)
  _delete_scenario_content(conn, "public")
  _delete_package_content(conn, "public")
  _drop_scenario_axis(conn, "public")

  fid = generate_deterministic_uuid(
    f"{_FRAMEWORK}:{_FRAMEWORK_VERSION}", namespace="framework"
  )
  fpid = generate_deterministic_uuid(
    f"{fid}:{_PACKAGE_STANDARD}:{_PACKAGE_VERSION}", namespace="framework_package"
  )
  conn.execute(
    text("DELETE FROM public.framework_packages WHERE id = :id"), {"id": fpid}
  )

  # Re-narrow the vocabulary — fails if 'forecast' structures or
  # 'rs-driver' elements remain outside the deleted content
  # (tenant-authored), which is data the operator must triage first.
  _narrow(conn, "public")
  for_each_tenant_schema(conn, _narrow)
