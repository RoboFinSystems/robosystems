"""Reporting Style — pick-one-at-provision composition layer.

Phase 1 of roadmap §3.2: every entity graph picks a Reporting Style
(Charlie Hoffman's term) at provision; the picker reads it deterministically
to resolve which Network renders each statement type.

This migration is additive:

1. Widen ``structures.check_block_type`` to admit ``'reporting_style'``
   (public + every existing tenant schema). Replaces the 0007 widen.
2. Create ``public.reporting_style_networks`` — the typed composition
   table ``(reporting_style_id, statement_type) → network_id``. Mirrors
   into every tenant schema so ``provision_tenant_schema`` (which uses
   ``CREATE SCHEMA IF NOT EXISTS`` + ``metadata.create_all``) lands the
   table for fresh tenants and existing tenants get it via the loop.
3. Promote the three placeholder Style Structures from
   ``block_type='custom'`` → ``'reporting_style'`` in **public only**.
   Existing tenant rows keep their legacy ``'custom'`` type — the picker
   doesn't filter on the Style's block_type, and the immutability
   trigger blocks tenant-scope UPDATE on library-seeded rows. Newly
   re-provisioned tenants pick up the corrected type from public.
4. Seed every declared Style's composition into
   ``public.reporting_style_networks``, read data-driven from the
   reporting-styles package's ``reportingStyleNetworks`` declarations,
   and stamp each Style's 4-segment ``reportingStyleCode`` into
   ``structures.metadata``. Adding a preset is pure package content — a
   ``reset-local`` re-runs this and picks it up with no new migration.

No prod backfill (Reporting Style hasn't shipped): the platform-side
``graphs.reporting_style_id`` lands in a sibling migration; local dev
demos re-provision to pick up the composition in tenant schemas.

Revision ID: 0008
Revises: 0007
Create Date: 2026-05-13
"""

from __future__ import annotations

import json
from pathlib import Path

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

from robosystems.utils.uuid import generate_deterministic_uuid

# revision identifiers, used by Alembic.
revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


# ── Style compositions are read from the reporting-styles package ─────────
# at migration time (data-driven). The package declares each Style's
# 4-segment ``reportingStyleCode`` and its ``reportingStyleNetworks``
# composition (statement_type → network roleUri); the seeder resolves
# every roleUri to its deterministic Structure id. Adding a new preset is
# therefore pure package content — ``reset-local`` re-runs this migration
# and picks it up with no new migration.

_STYLE_ROLE_PREFIX = "https://robosystems.ai/seattle/cm-roles/roles/styles/"


def _structure_id(role_uri: str) -> str:
  return generate_deterministic_uuid(role_uri, namespace="structure")


def _read_reporting_styles_package() -> list[dict]:
  """Load the rs-gaap-reporting-styles package ``@graph`` nodes."""
  from robosystems.taxonomy.discovery import framework_root, package_path

  pkg = package_path(
    "rs-gaap-reporting-styles", "v1", framework_root("rs-gaap") / "packages"
  )
  return json.loads(Path(pkg).read_text()).get("@graph", [])


def _declared_styles() -> list[dict]:
  """Every Style that declares a composition.

  Returns ``[{"role_uri", "code", "networks": [(stmt, net_role), ...]}]``.
  Composition-less placeholders (e.g. Banking) are skipped so they stay
  non-selectable until a composition is authored.
  """
  styles: list[dict] = []
  for node in _read_reporting_styles_package():
    networks = node.get("reportingStyleNetworks")
    role_uri = node.get("roleUri")
    if not networks or not isinstance(role_uri, str):
      continue
    styles.append(
      {
        "role_uri": role_uri,
        "code": node.get("reportingStyleCode"),
        "networks": [(n["statementType"], n["networkRoleUri"]) for n in networks],
      }
    )
  return styles


def _all_style_role_uris() -> list[str]:
  """Every Style Structure roleUri in the package (composed or not)."""
  return [
    n["roleUri"]
    for n in _read_reporting_styles_package()
    if isinstance(n.get("roleUri"), str) and n["roleUri"].startswith(_STYLE_ROLE_PREFIX)
  ]


# ── block_type CHECK widen (admit 'reporting_style') ──────────────────


_WIDENED_BLOCK_TYPE_CHECK = (
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
# Same set as 0007's widen — restored on downgrade. Represents the
# CHECK constraint state *immediately before* this migration runs
# (i.e., post-0007). Uses 'regulatory_disclosure' because the rename
# landed in 0002 (rewritten in place during the 2026-05-15 vocabulary
# alignment); the old 'disclosure' value never existed in a deployed
# DB.
_PRIOR_BLOCK_TYPE_CHECK = (
  "block_type IN ("
  "'income_statement', 'balance_sheet', "
  "'cash_flow_statement', 'equity_statement', "
  "'comprehensive_income', "
  "'schedule', 'rollforward', 'reconciliation', 'policy', 'metric', "
  "'chart_of_accounts', 'coa_mapping', "
  "'validation_rules', 'regulatory_disclosure', 'taxonomy_mapping', "
  "'custom'"
  ")"
)


def _widen_block_type_check(conn, schema: str) -> None:
  table = "public.structures" if schema == "public" else f'"{schema}".structures'
  conn.execute(text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS check_block_type"))
  conn.execute(
    text(
      f"ALTER TABLE {table} ADD CONSTRAINT check_block_type "
      f"CHECK ({_WIDENED_BLOCK_TYPE_CHECK})"
    )
  )


def _restore_block_type_check(conn, schema: str) -> None:
  table = "public.structures" if schema == "public" else f'"{schema}".structures'
  conn.execute(text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS check_block_type"))
  conn.execute(
    text(
      f"ALTER TABLE {table} ADD CONSTRAINT check_block_type "
      f"CHECK ({_PRIOR_BLOCK_TYPE_CHECK})"
    )
  )


# ── reporting_style_networks table — tenant-side CREATE ───────────────────


_RSN_DDL = """
  CREATE TABLE IF NOT EXISTS {table} (
    reporting_style_id VARCHAR NOT NULL,
    statement_type     VARCHAR NOT NULL,
    network_id         VARCHAR NOT NULL,
    created_at         TIMESTAMP NOT NULL DEFAULT now(),
    created_by         VARCHAR NOT NULL DEFAULT 'library-seeder',
    CONSTRAINT pk_reporting_style_networks
      PRIMARY KEY (reporting_style_id, statement_type),
    CONSTRAINT check_statement_type CHECK (
      statement_type IN (
        'balance_sheet', 'income_statement',
        'cash_flow_statement', 'equity_statement',
        'comprehensive_income'
      )
    )
  )
"""


def _create_rsn_in_tenant(conn, schema: str) -> None:
  conn.execute(text(_RSN_DDL.format(table=f'"{schema}".reporting_style_networks')))


def _drop_rsn_in_tenant(conn, schema: str) -> None:
  conn.execute(text(f'DROP TABLE IF EXISTS "{schema}".reporting_style_networks'))


# ── Style composition rows (data-driven from the package) ─────────────────


def _seed_style_compositions(conn, schema: str) -> None:
  """Seed every declared Style's composition into ``{schema}.reporting_style_networks``.

  Reads each Style's ``reportingStyleNetworks`` from the reporting-styles
  package and resolves both the Style roleUri and each network roleUri to
  its deterministic Structure id. Used for both ``public`` (the library
  template) and every existing tenant schema. Reporting Style hasn't
  shipped, so this backfill is safe across all tenants — there's no live
  composition to overwrite. ``ON CONFLICT DO NOTHING`` keeps it idempotent.
  """
  table = (
    "public.reporting_style_networks"
    if schema == "public"
    else f'"{schema}".reporting_style_networks'
  )
  for style in _declared_styles():
    style_id = _structure_id(style["role_uri"])
    for statement_type, network_role in style["networks"]:
      conn.execute(
        text(
          f"""
          INSERT INTO {table} (
            reporting_style_id, statement_type, network_id, created_by
          ) VALUES (:style, :stmt, :network, 'library-seeder')
          ON CONFLICT (reporting_style_id, statement_type) DO NOTHING
          """
        ),
        {
          "style": style_id,
          "stmt": statement_type,
          "network": _structure_id(network_role),
        },
      )


def _stamp_style_codes(conn) -> None:
  """Stamp ``reportingStyleCode`` into ``public.structures.metadata`` for
  every declared Style. Tenant rows pick it up via the library copy on
  re-provision (the immutability trigger blocks tenant-scope UPDATE)."""
  for style in _declared_styles():
    if not style["code"]:
      continue
    conn.execute(
      text(
        """
        UPDATE public.structures
        SET metadata = jsonb_set(
          COALESCE(metadata, '{}'::jsonb),
          '{reporting_style_code}',
          to_jsonb(CAST(:code AS text)),
          true
        )
        WHERE id = :sid
        """
      ),
      {"sid": _structure_id(style["role_uri"]), "code": style["code"]},
    )


def _promote_style_structures(conn) -> None:
  """Flip every Style Structure in PUBLIC from 'custom' →
  'reporting_style'. Tenant rows are left untouched: the library
  immutability trigger blocks tenant-scope UPDATE, and the picker
  doesn't care about the Style's block_type. Fresh tenants pick
  up the corrected type when ``copy_library_into_tenant`` re-mirrors
  rows from public after this migration runs.
  """
  ids = [_structure_id(r) for r in _all_style_role_uris()]
  conn.execute(
    text(
      """
      UPDATE public.structures
      SET block_type = 'reporting_style'
      WHERE id = ANY(:ids) AND block_type = 'custom'
      """
    ),
    {"ids": ids},
  )


def _restore_style_structures(conn) -> None:
  ids = [_structure_id(r) for r in _all_style_role_uris()]
  conn.execute(
    text(
      """
      UPDATE public.structures
      SET block_type = 'custom'
      WHERE id = ANY(:ids) AND block_type = 'reporting_style'
      """
    ),
    {"ids": ids},
  )


def upgrade() -> None:
  conn = op.get_bind()
  from migrations.extensions.helpers import for_each_tenant_schema

  # 1. Widen block_type CHECK in public + every tenant schema.
  _widen_block_type_check(conn, "public")
  for_each_tenant_schema(conn, _widen_block_type_check)

  # 2. Create reporting_style_networks table in public (Alembic op).
  op.create_table(
    "reporting_style_networks",
    sa.Column("reporting_style_id", sa.String(), nullable=False),
    sa.Column("statement_type", sa.String(), nullable=False),
    sa.Column("network_id", sa.String(), nullable=False),
    sa.Column(
      "created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.Column(
      "created_by",
      sa.String(),
      nullable=False,
      server_default="library-seeder",
    ),
    sa.CheckConstraint(
      "statement_type IN ("
      "'balance_sheet', 'income_statement', "
      "'cash_flow_statement', 'equity_statement', "
      "'comprehensive_income'"
      ")",
      name="check_statement_type",
    ),
    sa.PrimaryKeyConstraint(
      "reporting_style_id", "statement_type", name="pk_reporting_style_networks"
    ),
  )

  # 3. Create reporting_style_networks in every existing tenant schema.
  for_each_tenant_schema(conn, _create_rsn_in_tenant)

  # 4. Promote every Style Structure in public from 'custom' →
  # 'reporting_style' and stamp its 4-segment reportingStyleCode into
  # structures.metadata. Tenant rows are left alone (immutability trigger).
  _promote_style_structures(conn)
  _stamp_style_codes(conn)

  # 5. Seed each declared Style's composition into public AND every
  # existing tenant. Data-driven from the reporting-styles package, so a
  # new preset is pure content (reset-local re-runs this). Backfilling
  # tenants is safe here because Reporting Style hasn't shipped — there's
  # no live composition to overwrite. Production tenants don't exist yet;
  # local dev tenants need the rows for the picker to resolve a Network.
  _seed_style_compositions(conn, "public")
  for_each_tenant_schema(conn, _seed_style_compositions)


def downgrade() -> None:
  conn = op.get_bind()
  from migrations.extensions.helpers import for_each_tenant_schema

  # Reverse step 4 + 5: restore Style block_types + drop composition rows.
  conn.execute(text("TRUNCATE TABLE public.reporting_style_networks"))
  _restore_style_structures(conn)

  # Drop the table in every tenant schema, then in public.
  for_each_tenant_schema(conn, _drop_rsn_in_tenant)
  op.drop_table("reporting_style_networks")

  # Reverse the CHECK widen.
  _restore_block_type_check(conn, "public")
  for_each_tenant_schema(conn, _restore_block_type_check)
