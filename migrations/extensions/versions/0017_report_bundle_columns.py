"""add bundle_url and generation_count to reports

Two additive, nullable-or-defaulted columns on ``reports`` to support the
serialization MVP capstone:

* ``generation_count`` — monotonically incremented on each (re)generation
  so each Report's exported bundle stays addressable per-version in object
  storage. NOT NULL with server default 0; existing rows pick up 0 and
  step forward on next regenerate.
* ``bundle_url`` — S3 URI of the latest exported JSON-LD bundle. Null for
  pre-feature Reports (no backfill — re-publishing produces a bundle).

Fresh tenants get the columns via ``metadata.create_all`` at provision;
existing tenant schemas (and public) get them here via the TenantOps
fan-out pattern (matches 0015).

Revision ID: 0017
Revises: 0016
Create Date: 2026-05-28

"""

from __future__ import annotations

from alembic import op

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0017"
down_revision = "0016"
branch_labels = None
depends_on = None


def _add_bundle_columns(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.add_column(
    "reports",
    "generation_count",
    "INTEGER",
    nullable=False,
    default="0",
  )
  t.add_column("reports", "bundle_url", "VARCHAR", nullable=True)


def _drop_bundle_columns(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.drop_column("reports", "bundle_url")
  t.drop_column("reports", "generation_count")


def upgrade() -> None:
  conn = op.get_bind()
  _add_bundle_columns(conn, "public")
  for_each_tenant_schema(conn, _add_bundle_columns)


def downgrade() -> None:
  conn = op.get_bind()
  _drop_bundle_columns(conn, "public")
  for_each_tenant_schema(conn, _drop_bundle_columns)
