"""drop subgraph scoped graph_user rows

Revision ID: a40f29cc6fe7
Revises: 7fd78cf16c54
Create Date: 2026-08-15 21:43:34.702797

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "a40f29cc6fe7"
down_revision = "7fd78cf16c54"
branch_labels = None
depends_on = None


def upgrade() -> None:
  # Data-only backfill; autogenerate has nothing to detect.
  #
  # Subgraph creation used to write a GraphUser row on the subgraph id for the
  # creator. Authorization never read it (GraphUser.get_effective_role resolves
  # a subgraph to its parent grant), and member removal never deleted it, so it
  # outlived the parent grant. Creation no longer writes it and the graph list
  # derives subgraphs from the parent grant, leaving these rows with no reader.
  op.execute(
    "DELETE FROM graph_users "
    "WHERE graph_id IN (SELECT graph_id FROM graphs WHERE is_subgraph IS TRUE)"
  )


def downgrade() -> None:
  # The rows carried no information the parent grant does not; nothing to
  # restore.
  pass
