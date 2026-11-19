"""A report says how it came to be in the graph: ``authored`` from the graph's
own books, ``shared`` in from another graph, or ``uploaded`` as a file.

Until now "received" was spelled ``source_graph_id IS NOT NULL``, which a
report that arrives as a file cannot say. One NOT NULL column with a CHECK, in
``public`` and every tenant, backfilled from that old spelling.

Autogenerate sees the column on ``public`` but neither the CHECK nor the
tenants, so the body is written in the tenant style.

Revision ID: 0069
Revises: 0068
Create Date: 2026-11-18
"""

from __future__ import annotations

from alembic import op
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

revision = "0069"
down_revision = "0068"
branch_labels = None
depends_on = None

_CHECK = "check_report_origin"


def _expand(conn: Connection, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.add_column("reports", "origin", "VARCHAR", nullable=False, default="'authored'")
  t.execute(
    "UPDATE {s}.reports SET origin = 'shared' "
    "WHERE source_graph_id IS NOT NULL AND origin = 'authored'"
  )
  t.add_check("reports", _CHECK, "origin IN ('authored', 'shared', 'uploaded')")


def _contract(conn: Connection, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.drop_constraint("reports", _CHECK)
  t.drop_column("reports", "origin")


def upgrade() -> None:
  conn = op.get_bind()
  _expand(conn, "public")
  for_each_tenant_schema(conn, _expand)


def downgrade() -> None:
  conn = op.get_bind()
  for_each_tenant_schema(conn, _contract)
  _contract(conn, "public")
