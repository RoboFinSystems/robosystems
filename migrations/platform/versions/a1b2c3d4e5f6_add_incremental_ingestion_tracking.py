"""add incremental ingestion tracking

Revision ID: a1b2c3d4e5f6
Revises: c4cc411768f2
Create Date: 2025-11-19 23:30:00.000000

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "c4cc411768f2"
branch_labels = None
depends_on = None


def upgrade() -> None:
  # GraphFile: Add file tracking columns for v2 incremental ingestion
  op.add_column(
    "graph_files",
    sa.Column("duckdb_status", sa.String(), nullable=False, server_default="pending"),
  )
  op.add_column(
    "graph_files", sa.Column("duckdb_row_count", sa.Integer(), nullable=True)
  )
  op.add_column(
    "graph_files",
    sa.Column("duckdb_staged_at", sa.DateTime(timezone=True), nullable=True),
  )
  op.add_column(
    "graph_files",
    sa.Column("graph_status", sa.String(), nullable=False, server_default="pending"),
  )
  op.add_column(
    "graph_files",
    sa.Column("graph_ingested_at", sa.DateTime(timezone=True), nullable=True),
  )
  op.add_column("graph_files", sa.Column("celery_task_id", sa.String(), nullable=True))

  # Remove server_default now that existing rows have values
  op.alter_column("graph_files", "duckdb_status", server_default=None)
  op.alter_column("graph_files", "graph_status", server_default=None)

  op.create_index(
    "idx_graph_files_duckdb_status", "graph_files", ["duckdb_status"], unique=False
  )
  op.create_index(
    "idx_graph_files_graph_status", "graph_files", ["graph_status"], unique=False
  )

  # Graph: Add staleness tracking for v2 incremental ingestion
  op.add_column(
    "graphs",
    sa.Column("graph_stale", sa.Boolean(), nullable=False, server_default="false"),
  )
  op.add_column("graphs", sa.Column("graph_stale_reason", sa.String(), nullable=True))
  op.add_column("graphs", sa.Column("graph_stale_at", sa.DateTime(), nullable=True))
  op.alter_column("graphs", "graph_stale", server_default=None)
  op.create_index("idx_graphs_stale", "graphs", ["graph_stale"], unique=False)

  # Graph usage table: Rename indexes (unrelated cleanup)
  op.drop_index(op.f("ix_graph_usage_tracking_billing_day"), table_name="graph_usage")
  op.drop_index(op.f("ix_graph_usage_tracking_billing_month"), table_name="graph_usage")
  op.drop_index(op.f("ix_graph_usage_tracking_billing_year"), table_name="graph_usage")
  op.drop_index(op.f("ix_graph_usage_tracking_event_type"), table_name="graph_usage")
  op.drop_index(op.f("ix_graph_usage_tracking_graph_id"), table_name="graph_usage")
  op.drop_index(op.f("ix_graph_usage_tracking_graph_tier"), table_name="graph_usage")
  op.drop_index(op.f("ix_graph_usage_tracking_recorded_at"), table_name="graph_usage")
  op.drop_index(op.f("ix_graph_usage_tracking_user_id"), table_name="graph_usage")
  op.create_index(
    op.f("ix_graph_usage_billing_day"), "graph_usage", ["billing_day"], unique=False
  )
  op.create_index(
    op.f("ix_graph_usage_billing_month"), "graph_usage", ["billing_month"], unique=False
  )
  op.create_index(
    op.f("ix_graph_usage_billing_year"), "graph_usage", ["billing_year"], unique=False
  )
  op.create_index(
    op.f("ix_graph_usage_event_type"), "graph_usage", ["event_type"], unique=False
  )
  op.create_index(
    op.f("ix_graph_usage_graph_id"), "graph_usage", ["graph_id"], unique=False
  )
  op.create_index(
    op.f("ix_graph_usage_graph_tier"), "graph_usage", ["graph_tier"], unique=False
  )
  op.create_index(
    op.f("ix_graph_usage_recorded_at"), "graph_usage", ["recorded_at"], unique=False
  )
  op.create_index(
    op.f("ix_graph_usage_user_id"), "graph_usage", ["user_id"], unique=False
  )


def downgrade() -> None:
  # Graph usage table: Restore old indexes
  op.drop_index(op.f("ix_graph_usage_user_id"), table_name="graph_usage")
  op.drop_index(op.f("ix_graph_usage_recorded_at"), table_name="graph_usage")
  op.drop_index(op.f("ix_graph_usage_graph_tier"), table_name="graph_usage")
  op.drop_index(op.f("ix_graph_usage_graph_id"), table_name="graph_usage")
  op.drop_index(op.f("ix_graph_usage_event_type"), table_name="graph_usage")
  op.drop_index(op.f("ix_graph_usage_billing_year"), table_name="graph_usage")
  op.drop_index(op.f("ix_graph_usage_billing_month"), table_name="graph_usage")
  op.drop_index(op.f("ix_graph_usage_billing_day"), table_name="graph_usage")
  op.create_index(
    op.f("ix_graph_usage_tracking_user_id"), "graph_usage", ["user_id"], unique=False
  )
  op.create_index(
    op.f("ix_graph_usage_tracking_recorded_at"),
    "graph_usage",
    ["recorded_at"],
    unique=False,
  )
  op.create_index(
    op.f("ix_graph_usage_tracking_graph_tier"),
    "graph_usage",
    ["graph_tier"],
    unique=False,
  )
  op.create_index(
    op.f("ix_graph_usage_tracking_graph_id"), "graph_usage", ["graph_id"], unique=False
  )
  op.create_index(
    op.f("ix_graph_usage_tracking_event_type"),
    "graph_usage",
    ["event_type"],
    unique=False,
  )
  op.create_index(
    op.f("ix_graph_usage_tracking_billing_year"),
    "graph_usage",
    ["billing_year"],
    unique=False,
  )
  op.create_index(
    op.f("ix_graph_usage_tracking_billing_month"),
    "graph_usage",
    ["billing_month"],
    unique=False,
  )
  op.create_index(
    op.f("ix_graph_usage_tracking_billing_day"),
    "graph_usage",
    ["billing_day"],
    unique=False,
  )

  # Graph: Remove staleness tracking
  op.drop_index("idx_graphs_stale", table_name="graphs")

  # Restore server_default before dropping column (for completeness, though column will be dropped)
  op.alter_column("graphs", "graph_stale", server_default="false")

  op.drop_column("graphs", "graph_stale_at")
  op.drop_column("graphs", "graph_stale_reason")
  op.drop_column("graphs", "graph_stale")

  # GraphFile: Remove file tracking columns
  op.drop_index("idx_graph_files_graph_status", table_name="graph_files")
  op.drop_index("idx_graph_files_duckdb_status", table_name="graph_files")

  # Restore server_default before dropping columns (for completeness, though columns will be dropped)
  op.alter_column("graph_files", "duckdb_status", server_default="pending")
  op.alter_column("graph_files", "graph_status", server_default="pending")

  op.drop_column("graph_files", "celery_task_id")
  op.drop_column("graph_files", "graph_ingested_at")
  op.drop_column("graph_files", "graph_status")
  op.drop_column("graph_files", "duckdb_staged_at")
  op.drop_column("graph_files", "duckdb_row_count")
  op.drop_column("graph_files", "duckdb_status")
