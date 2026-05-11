"""Widen the structures.structure_type CHECK to admit 'comprehensive_income'.

The rs-gaap-disclosures package's ``disclosures:StatementOfComprehensiveIncome``
entry previously carried ``structureType: income_statement`` so the renderer
treated it as a duplicate Income Statement. This migration:

1. Widens ``structures.structure_type`` CHECK in ``public`` and every existing
   tenant schema to admit ``'comprehensive_income'``.
2. The subsequent taxonomy re-seed reads the updated JSON-LD and upserts the
   disclosure row with the new ``structureType``.

Pure CHECK-constraint widening — no table additions, no data migration. UUID5
keying keeps the disclosure row stable across the upsert; only the
``structure_type`` column changes.

Revision: 0008
Down-revision: 0007
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


_WIDENED_STRUCTURE_TYPE_CHECK = (
  "structure_type IN ("
  "'income_statement', 'balance_sheet', "
  "'cash_flow_statement', 'equity_statement', "
  "'comprehensive_income', "
  "'schedule', 'rollforward', 'reconciliation', 'policy', 'metric', "
  "'chart_of_accounts', 'coa_mapping', "
  "'validation_rules', 'disclosure', 'taxonomy_mapping', "
  "'custom'"
  ")"
)
_PRIOR_STRUCTURE_TYPE_CHECK = (
  "structure_type IN ("
  "'income_statement', 'balance_sheet', "
  "'cash_flow_statement', 'equity_statement', "
  "'schedule', 'rollforward', 'reconciliation', 'policy', 'metric', "
  "'chart_of_accounts', 'coa_mapping', "
  "'validation_rules', 'disclosure', 'taxonomy_mapping', "
  "'custom'"
  ")"
)


def _widen(conn, schema: str) -> None:
  if schema == "public":
    table = "public.structures"
  else:
    table = f'"{schema}".structures'
  conn.execute(
    text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS check_structure_type")
  )
  conn.execute(
    text(
      f"ALTER TABLE {table} ADD CONSTRAINT check_structure_type "
      f"CHECK ({_WIDENED_STRUCTURE_TYPE_CHECK})"
    )
  )


def _restore(conn, schema: str) -> None:
  if schema == "public":
    table = "public.structures"
  else:
    table = f'"{schema}".structures'
  conn.execute(
    text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS check_structure_type")
  )
  conn.execute(
    text(
      f"ALTER TABLE {table} ADD CONSTRAINT check_structure_type "
      f"CHECK ({_PRIOR_STRUCTURE_TYPE_CHECK})"
    )
  )


def upgrade() -> None:
  conn = op.get_bind()
  from migrations.extensions.helpers import for_each_tenant_schema

  _widen(conn, "public")
  for_each_tenant_schema(conn, _widen)


def downgrade() -> None:
  conn = op.get_bind()
  from migrations.extensions.helpers import for_each_tenant_schema

  _restore(conn, "public")
  for_each_tenant_schema(conn, _restore)
