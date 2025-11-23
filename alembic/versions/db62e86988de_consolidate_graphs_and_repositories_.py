"""consolidate_graphs_and_repositories_unified_infrastructure

Revision ID: db62e86988de
Revises: ebb9b6f57088
Create Date: 2025-11-06 13:53:31.745184

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "db62e86988de"
down_revision = "ebb9b6f57088"
branch_labels = None
depends_on = None


def upgrade() -> None:
  # Add repository support columns to graphs table
  op.add_column(
    "graphs",
    sa.Column(
      "is_repository", sa.Boolean(), nullable=False, server_default=sa.text("false")
    ),
  )
  op.add_column("graphs", sa.Column("repository_type", sa.String(), nullable=True))
  op.add_column("graphs", sa.Column("data_source_type", sa.String(), nullable=True))
  op.add_column("graphs", sa.Column("data_source_url", sa.String(), nullable=True))
  op.add_column("graphs", sa.Column("last_sync_at", sa.DateTime(), nullable=True))
  op.add_column("graphs", sa.Column("sync_status", sa.String(), nullable=True))
  op.add_column("graphs", sa.Column("sync_frequency", sa.String(), nullable=True))
  op.add_column("graphs", sa.Column("sync_error_message", sa.String(), nullable=True))

  # Create indexes for repository columns
  op.create_index("idx_graphs_is_repository", "graphs", ["is_repository"], unique=False)
  op.create_index(
    "idx_graphs_repository_type", "graphs", ["repository_type"], unique=False
  )

  # Update check constraint to allow 'repository' type
  op.drop_constraint("check_graph_type", "graphs", type_="check")
  op.create_check_constraint(
    "check_graph_type", "graphs", "graph_type IN ('generic', 'entity', 'repository')"
  )

  # Add foreign key from user_repository to graphs
  op.create_foreign_key(
    "fk_user_repository_graph_id",
    "user_repository",
    "graphs",
    ["repository_name"],
    ["graph_id"],
    ondelete="RESTRICT",
  )

  # Drop redundant infrastructure columns from user_repository
  op.drop_column("user_repository", "read_preference")
  op.drop_column("user_repository", "graph_cluster_region")
  op.drop_column("user_repository", "graph_instance_id")
  op.drop_column("user_repository", "instance_tier")


def downgrade() -> None:
  # Restore infrastructure columns to user_repository
  op.add_column(
    "user_repository",
    sa.Column(
      "instance_tier",
      sa.VARCHAR(),
      server_default="ladybug-shared",
      autoincrement=False,
      nullable=False,
    ),
  )
  op.add_column(
    "user_repository",
    sa.Column(
      "graph_instance_id",
      sa.VARCHAR(),
      server_default="default",
      autoincrement=False,
      nullable=False,
    ),
  )
  op.add_column(
    "user_repository",
    sa.Column("graph_cluster_region", sa.VARCHAR(), autoincrement=False, nullable=True),
  )
  op.add_column(
    "user_repository",
    sa.Column(
      "read_preference",
      sa.VARCHAR(),
      server_default="primary",
      autoincrement=False,
      nullable=False,
    ),
  )

  # Drop foreign key constraint
  op.drop_constraint(
    "fk_user_repository_graph_id", "user_repository", type_="foreignkey"
  )

  # Revert check constraint to original
  op.drop_constraint("check_graph_type", "graphs", type_="check")
  op.create_check_constraint(
    "check_graph_type", "graphs", "graph_type IN ('generic', 'entity')"
  )

  # Drop indexes
  op.drop_index("idx_graphs_repository_type", table_name="graphs")
  op.drop_index("idx_graphs_is_repository", table_name="graphs")

  # Drop repository columns
  op.drop_column("graphs", "sync_error_message")
  op.drop_column("graphs", "sync_frequency")
  op.drop_column("graphs", "sync_status")
  op.drop_column("graphs", "last_sync_at")
  op.drop_column("graphs", "data_source_url")
  op.drop_column("graphs", "data_source_type")
  op.drop_column("graphs", "repository_type")
  op.drop_column("graphs", "is_repository")
