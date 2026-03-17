"""rename celery_task_id to operation_id

Revision ID: 7bbb8648030f
Revises: a1b2c3d4e5f6
Create Date: 2025-12-18 23:27:18.962130

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "7bbb8648030f"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
  # Rename column to reflect Dagster migration (preserves existing data)
  op.alter_column("graph_files", "celery_task_id", new_column_name="operation_id")


def downgrade() -> None:
  op.alter_column("graph_files", "operation_id", new_column_name="celery_task_id")
