"""Phase zeta — fact_sets table.

Data-model landing for the FactSet concept (spec §5.3). A FactSet is a
period-specific instantiation of a Structure — one row groups the facts
produced for that Structure in that period. Replaces the implicit
``report_id``-carries-period / free-string ``fact_set_id`` split with a
typed parent row.

This migration ships only the table. Backfill of existing rows, the
``create_report`` / ``create_schedule`` updates that stamp FactSet rows
on write, and the eventual ``report_id`` retirement are expand-pass
work. Indexes on ``structure_id``, ``period_start/end``, ``entity_id``,
and ``report_id`` match the query patterns the envelope builder will use
once the Phase ζ dimensional reads land.

Revision ID: 0003
Revises: 0002
Create Date: 2026-04-21
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from migrations.extensions.helpers import for_each_tenant_schema

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


_FACT_SET_TYPE_CHECK = "factset_type IN ('report', 'schedule', 'custom')"


def _create_fact_sets_in_tenant(conn, schema: str) -> None:
  conn.execute(
    text(f"""
      CREATE TABLE IF NOT EXISTS {schema}.fact_sets (
        id VARCHAR PRIMARY KEY,
        structure_id VARCHAR REFERENCES {schema}.structures(id),
        period_start DATE,
        period_end DATE NOT NULL,
        factset_type VARCHAR NOT NULL DEFAULT 'report',
        entity_id VARCHAR NOT NULL,
        report_id VARCHAR,
        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        created_by VARCHAR NOT NULL DEFAULT 'system',
        CONSTRAINT check_{schema}_fact_set_type
          CHECK ({_FACT_SET_TYPE_CHECK})
      )
    """)
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_fact_sets_structure "
      f"ON {schema}.fact_sets (structure_id)"
    )
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_fact_sets_period "
      f"ON {schema}.fact_sets (period_start, period_end)"
    )
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_fact_sets_entity "
      f"ON {schema}.fact_sets (entity_id)"
    )
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_fact_sets_report "
      f"ON {schema}.fact_sets (report_id)"
    )
  )


def _drop_fact_sets_in_tenant(conn, schema: str) -> None:
  conn.execute(text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_fact_sets_report"))
  conn.execute(text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_fact_sets_entity"))
  conn.execute(text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_fact_sets_period"))
  conn.execute(text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_fact_sets_structure"))
  conn.execute(text(f"DROP TABLE IF EXISTS {schema}.fact_sets"))


def upgrade() -> None:
  op.create_table(
    "fact_sets",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("structure_id", sa.String(), nullable=True),
    sa.Column("period_start", sa.Date(), nullable=True),
    sa.Column("period_end", sa.Date(), nullable=False),
    sa.Column("factset_type", sa.String(), nullable=False, server_default="report"),
    sa.Column("entity_id", sa.String(), nullable=False),
    sa.Column("report_id", sa.String(), nullable=True),
    sa.Column(
      "metadata",
      postgresql.JSONB(astext_type=sa.Text()),
      nullable=False,
      server_default=sa.text("'{}'::jsonb"),
    ),
    sa.Column(
      "created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.Column(
      "updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.Column("created_by", sa.String(), nullable=False, server_default="system"),
    sa.ForeignKeyConstraint(["structure_id"], ["structures.id"]),
    sa.PrimaryKeyConstraint("id"),
    sa.CheckConstraint(_FACT_SET_TYPE_CHECK, name="check_fact_set_type"),
  )
  op.create_index("idx_fact_sets_structure", "fact_sets", ["structure_id"])
  op.create_index("idx_fact_sets_period", "fact_sets", ["period_start", "period_end"])
  op.create_index("idx_fact_sets_entity", "fact_sets", ["entity_id"])
  op.create_index("idx_fact_sets_report", "fact_sets", ["report_id"])

  conn = op.get_bind()
  for_each_tenant_schema(conn, _create_fact_sets_in_tenant)


def downgrade() -> None:
  conn = op.get_bind()
  for_each_tenant_schema(conn, _drop_fact_sets_in_tenant)

  op.drop_index("idx_fact_sets_report", table_name="fact_sets")
  op.drop_index("idx_fact_sets_entity", table_name="fact_sets")
  op.drop_index("idx_fact_sets_period", table_name="fact_sets")
  op.drop_index("idx_fact_sets_structure", table_name="fact_sets")
  op.drop_table("fact_sets")
