"""Retire the balance sheet identity rule and its false failures.

``Assets = Liabilities + StockholdersEquity`` failed every month of a group with
outside owners by exactly its noncontrolling interest, on books that balance:
that equity sits beside ``StockholdersEquity``. ``Assets = LiabilitiesAndEquity``
with the rollup of that total proves the identity for every statement.

The net income rollup failed the same months on the cash flow and equity
statements, which carry the outside owners' share without continuing
operations. The evaluator now skips it there, so what it wrote before is false.

Closed months' receipts keep the counts they recorded.

Revision ID: 0067
Revises: 0066
Create Date: 2026-11-03
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0067"
down_revision = "0066"
branch_labels = None
depends_on = None

# tenant-fanout: not-required (data only; every tenant is reached below)

# _rule_id("rs-gaap-rules", "rs-gaap-rule-bs-identity")
_BS_IDENTITY_RULE = "a1760111-71a2-5bfb-bb53-c5fdf5264d33"
# _rule_id("rs-gaap-rollup-rules", "rs-gaap-rollup-is-netincome")
_NET_INCOME_ROLLUP = "4fdf61ff-ec5d-5cb2-8bcd-aa584ad2e768"


def _clean(conn, schema: str) -> None:
  conn.execute(
    text(f"DELETE FROM {schema}.verification_results WHERE rule_id = :rule"),
    {"rule": _BS_IDENTITY_RULE},
  )
  conn.execute(
    text(f"DELETE FROM {schema}.rules WHERE id = :rule"),
    {"rule": _BS_IDENTITY_RULE},
  )
  conn.execute(
    text(
      f"""
      DELETE FROM {schema}.verification_results v
      USING {schema}.structures s
      WHERE s.id = v.structure_id
        AND v.rule_id = :rule
        AND v.status = 'fail'
        AND s.block_type IN ('cash_flow_statement', 'equity_statement')
      """
    ),
    {"rule": _NET_INCOME_ROLLUP},
  )


def upgrade() -> None:
  from robosystems.taxonomy.writer import SET_LIBRARY_RESYNC

  conn = op.get_bind()
  # The immutability triggers reject DELETE on library-origin rows without the
  # bypass GUC; SET LOCAL covers the public schema and the tenant fan-out.
  conn.execute(text(SET_LIBRARY_RESYNC))
  _clean(conn, "public")
  for_each_tenant_schema(conn, _clean)


def downgrade() -> None:
  """Intentionally a no-op: the rule and the results were wrong, and the framework
  source no longer carries the rule."""
