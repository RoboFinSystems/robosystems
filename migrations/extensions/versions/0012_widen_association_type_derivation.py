"""Widen associations.association_type CHECK to include 'derivation'.

Adds 'derivation' as a permitted ``association_type`` so the
rs-gaap-calculations package can carry arcs that map balance-sheet
leaves to their cash-flow "default change tag" counterparts (e.g.
``rs-gaap:ReceivablesNetCurrent`` →
``rs-gaap:IncreaseDecreaseInAccountsReceivable``).

These arcs let the renderer synthesize CF facts from period-over-period
BS deltas without manual authoring — see
``operations/roboledger/reports/fact_grid.py::_derive_cash_flow_facts``
and ``information-block.md §4.5``.

Applies the widen to ``public`` and every existing tenant schema.

Revision ID: 0012
Revises: 0011
Create Date: 2026-05-14
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None


_WIDENED_ASSOCIATION_TYPE_CHECK = (
  "association_type IN ("
  "'presentation', 'calculation', 'mapping', "
  "'equivalence', 'general-special', 'essence-alias', "
  "'definition', 'derivation'"
  ")"
)
_PRIOR_ASSOCIATION_TYPE_CHECK = (
  "association_type IN ("
  "'presentation', 'calculation', 'mapping', "
  "'equivalence', 'general-special', 'essence-alias', "
  "'definition'"
  ")"
)


def _widen(conn, schema: str) -> None:
  table = "public.associations" if schema == "public" else f'"{schema}".associations'
  conn.execute(
    text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS check_association_type")
  )
  conn.execute(
    text(
      f"ALTER TABLE {table} ADD CONSTRAINT check_association_type "
      f"CHECK ({_WIDENED_ASSOCIATION_TYPE_CHECK})"
    )
  )


def _restore(conn, schema: str) -> None:
  table = "public.associations" if schema == "public" else f'"{schema}".associations'
  conn.execute(
    text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS check_association_type")
  )
  conn.execute(
    text(
      f"ALTER TABLE {table} ADD CONSTRAINT check_association_type "
      f"CHECK ({_PRIOR_ASSOCIATION_TYPE_CHECK})"
    )
  )


def upgrade() -> None:
  from migrations.extensions.helpers import for_each_tenant_schema

  conn = op.get_bind()
  _widen(conn, "public")
  for_each_tenant_schema(conn, _widen)


def downgrade() -> None:
  from migrations.extensions.helpers import for_each_tenant_schema

  conn = op.get_bind()
  for_each_tenant_schema(conn, _restore)
  _restore(conn, "public")
