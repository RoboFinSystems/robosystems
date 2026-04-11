"""add entry provenance column

Tracks where each entry originated: source_sync (QuickBooks),
ai_generated (CloseAgent), manual_entry (UI), schedule_derived
(from schedule facts), system_computed (platform operations).

Revision ID: 0002
Revises: 0001
Create Date: 2026-04-10
"""

import sqlalchemy as sa
from alembic import op

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
  # Public schema
  op.add_column("entries", sa.Column("provenance", sa.String(), nullable=True))

  # Tenant schemas
  conn = op.get_bind()

  def apply(conn, schema):
    t = TenantOps(conn, schema)
    t.add_column("entries", "provenance", "VARCHAR")

  for_each_tenant_schema(conn, apply)


def downgrade() -> None:
  # Public schema
  op.drop_column("entries", "provenance")

  # Tenant schemas
  conn = op.get_bind()

  def apply(conn, schema):
    t = TenantOps(conn, schema)
    t.drop_column("entries", "provenance")

  for_each_tenant_schema(conn, apply)
