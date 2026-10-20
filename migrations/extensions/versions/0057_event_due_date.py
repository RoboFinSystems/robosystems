"""``events.due_date``: when an obligation falls due.

The aged receivables and payables bucket each open invoice and bill by how
far past due it is, and nothing held a due date: ``transactions.due_date``
was written by nothing, and QuickBooks' ``DueDate`` never left the extract
(spec ``ar-ap-aging`` §3). The obligation is the event, so the date goes
there, nullable, null on everything that is not an invoice issued or a bill
received.

Autogenerate sees the column on ``public`` but not the tenant schemas, so
the body is written by hand, in ``public`` and in every tenant.

Revision ID: 0057
Revises: 0056
Create Date: 2026-10-20
"""

from __future__ import annotations

from alembic import op
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

revision = "0057"
down_revision = "0056"
branch_labels = None
depends_on = None


def _expand(conn: Connection, schema: str) -> None:
  TenantOps(conn, schema).add_column("events", "due_date", "DATE")


def _contract(conn: Connection, schema: str) -> None:
  TenantOps(conn, schema).drop_column("events", "due_date")


def upgrade() -> None:
  conn = op.get_bind()
  _expand(conn, "public")
  for_each_tenant_schema(conn, _expand)


def downgrade() -> None:
  conn = op.get_bind()
  _contract(conn, "public")
  for_each_tenant_schema(conn, _contract)
