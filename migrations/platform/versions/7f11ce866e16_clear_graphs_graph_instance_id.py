"""clear graphs graph_instance_id

Revision ID: 7f11ce866e16
Revises: 6ef007fe2091
Create Date: 2026-10-05 11:37:26.093355

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "7f11ce866e16"
down_revision = "6ef007fe2091"
branch_labels = None
depends_on = None


def upgrade() -> None:
  # No schema change: the model stopped mapping the column. Clear the stale
  # instance ids it still holds; nothing reads them, and DynamoDB is where
  # a graph's instance lives. The column is dropped in the next release.
  op.execute(
    "UPDATE graphs SET graph_instance_id = NULL WHERE graph_instance_id IS NOT NULL"
  )


def downgrade() -> None:
  # The cleared values were stale and are not restored.
  pass
