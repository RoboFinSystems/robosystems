"""The industry classification vocabulary, as ``public.industries``.

One copy in ``public`` that every tenant reads; nothing is added to a tenant
schema. Seeded from ``frameworks/industries/industries.csv`` as it stands when
this runs. A later change to that file reaches an existing database through a
new migration that calls ``seed_industries`` again.

Autogenerate also proposed unrelated drift on other tables; only the new table
is kept.

Revision ID: 0070
Revises: 0069
Create Date: 2026-11-23
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from robosystems.taxonomy.industries import seed_industries

revision = "0070"
down_revision = "0069"
branch_labels = None
depends_on = None


def upgrade() -> None:
  op.create_table(
    "industries",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("parent_id", sa.String(), nullable=True),
    sa.Column("level", sa.String(), nullable=False),
    sa.Column("label", sa.String(), nullable=False),
    sa.Column("carried", sa.Boolean(), nullable=False),
    sa.Column("replaced_by", sa.String(), nullable=True),
    sa.CheckConstraint(
      "level IN ('sector', 'subsector', 'industry', 'sub-industry')",
      name="check_industry_level",
    ),
    sa.PrimaryKeyConstraint("id"),
    schema="public",
  )
  op.create_index(
    "idx_industries_parent", "industries", ["parent_id"], unique=False, schema="public"
  )
  seed_industries(op.get_bind())


def downgrade() -> None:
  op.drop_index("idx_industries_parent", table_name="industries", schema="public")
  op.drop_table("industries", schema="public")
