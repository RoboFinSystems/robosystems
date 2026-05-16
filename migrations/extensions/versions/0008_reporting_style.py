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
4. Seed the Default Style's composition (Balance Sheet / Multi-step IS /
   Indirect CF / Equity Roll Forward) into ``public.reporting_style_networks``.

No prod backfill (Reporting Style hasn't shipped): the platform-side
``graphs.reporting_style_id`` lands in a sibling migration; local dev
demos re-provision to pick up the composition in tenant schemas.

Revision ID: 0008
Revises: 0007
Create Date: 2026-05-13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

from robosystems.utils.uuid import generate_deterministic_uuid

# revision identifiers, used by Alembic.
revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


# ── Reporting Style + Network role URIs (deterministic, library-seeded) ──

_DEFAULT_STYLE_ROLE = "https://robosystems.ai/seattle/cm-roles/roles/styles/Default"
_SMALL_PRIVATE_STYLE_ROLE = (
  "https://robosystems.ai/seattle/cm-roles/roles/styles/SmallPrivateCompany"
)
_BANKING_STYLE_ROLE = "https://robosystems.ai/seattle/cm-roles/roles/styles/Banking"

_BS_NETWORK_ROLE = (
  "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/BS-classified"
)
_IS_NETWORK_ROLE = (
  "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/IS-multistep"
)
_CF_NETWORK_ROLE = (
  "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/CashFlow-indirect"
)
_SE_NETWORK_ROLE = "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/Equity-rollforward"


def _structure_id(role_uri: str) -> str:
  return generate_deterministic_uuid(role_uri, namespace="structure")


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


# ── Default Style composition rows (seeded in public only) ────────────────


_DEFAULT_STYLE_COMPOSITION = [
  # (statement_type, network_role_uri)
  ("balance_sheet", _BS_NETWORK_ROLE),
  ("income_statement", _IS_NETWORK_ROLE),
  ("cash_flow_statement", _CF_NETWORK_ROLE),
  ("equity_statement", _SE_NETWORK_ROLE),
]


def _seed_default_style_composition(conn, schema: str) -> None:
  """Seed the Default Style's 4 composition rows into ``{schema}.reporting_style_networks``.

  Used for both ``public`` (the library template) and every existing
  tenant schema. Reporting Style hasn't shipped, so this backfill is
  safe across all tenants — there's no live composition to overwrite.
  ``ON CONFLICT DO NOTHING`` keeps it idempotent.
  """
  default_style_id = _structure_id(_DEFAULT_STYLE_ROLE)
  table = (
    "public.reporting_style_networks"
    if schema == "public"
    else f'"{schema}".reporting_style_networks'
  )
  for statement_type, role in _DEFAULT_STYLE_COMPOSITION:
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
        "style": default_style_id,
        "stmt": statement_type,
        "network": _structure_id(role),
      },
    )


def _promote_style_structures(conn) -> None:
  """Flip the 3 placeholder Style Structures in PUBLIC from 'custom' →
  'reporting_style'. Tenant rows are left untouched: the library
  immutability trigger blocks tenant-scope UPDATE, and the picker
  doesn't care about the Style's block_type. Fresh tenants pick
  up the corrected type when ``copy_library_into_tenant`` re-mirrors
  rows from public after this migration runs.
  """
  ids = [
    _structure_id(_DEFAULT_STYLE_ROLE),
    _structure_id(_SMALL_PRIVATE_STYLE_ROLE),
    _structure_id(_BANKING_STYLE_ROLE),
  ]
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
  ids = [
    _structure_id(_DEFAULT_STYLE_ROLE),
    _structure_id(_SMALL_PRIVATE_STYLE_ROLE),
    _structure_id(_BANKING_STYLE_ROLE),
  ]
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

  # 4. Promote 3 Style Structures in public from 'custom' →
  # 'reporting_style'. Tenant rows are left alone (immutability trigger).
  _promote_style_structures(conn)

  # 5. Seed Default Style's composition (BS / IS / CF / SE) into public
  # AND every existing tenant. Backfilling tenants is safe here because
  # Reporting Style hasn't shipped — there's no live composition to
  # overwrite. Production tenants don't exist yet; local dev tenants
  # need the rows for the picker to resolve a Network.
  _seed_default_style_composition(conn, "public")
  for_each_tenant_schema(conn, _seed_default_style_composition)


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
