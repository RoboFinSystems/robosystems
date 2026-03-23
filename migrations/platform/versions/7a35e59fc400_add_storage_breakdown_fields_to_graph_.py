"""Add storage breakdown fields and rename table to graph_usage

Revision ID: 7a35e59fc400
Revises: db62e86988de
Create Date: 2025-11-06 22:10:49.231560

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "7a35e59fc400"
down_revision = "db62e86988de"
branch_labels = None
depends_on = None


def upgrade() -> None:
  op.add_column(
    "graph_usage_tracking", sa.Column("files_storage_gb", sa.Float(), nullable=True)
  )
  op.add_column(
    "graph_usage_tracking", sa.Column("tables_storage_gb", sa.Float(), nullable=True)
  )
  op.add_column(
    "graph_usage_tracking", sa.Column("graphs_storage_gb", sa.Float(), nullable=True)
  )
  op.add_column(
    "graph_usage_tracking", sa.Column("subgraphs_storage_gb", sa.Float(), nullable=True)
  )

  op.rename_table("graph_usage_tracking", "graph_usage")


def downgrade() -> None:
  op.rename_table("graph_usage", "graph_usage_tracking")

  op.drop_column("graph_usage_tracking", "subgraphs_storage_gb")
  op.drop_column("graph_usage_tracking", "graphs_storage_gb")
  op.drop_column("graph_usage_tracking", "tables_storage_gb")
  op.drop_column("graph_usage_tracking", "files_storage_gb")
