"""connections keep books per entity

Revision ID: 4ed2e81c0abb
Revises: cbef551e7288
Create Date: 2026-11-14 16:57:44.126124

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "4ed2e81c0abb"
down_revision = "cbef551e7288"
branch_labels = None
depends_on = None


def upgrade() -> None:
  op.add_column("connections", sa.Column("entity_id", sa.String(), nullable=True))
  op.add_column(
    "connections",
    sa.Column("keeps_books", sa.Boolean(), server_default="false", nullable=False),
  )
  # A synced ledger keeps the group parent's books, and the parent's id is
  # `entity_<graph_id>` by construction (graph creation makes that row).
  op.execute(
    "UPDATE connections SET entity_id = 'entity_' || graph_id "
    "WHERE provider = 'quickbooks'"
  )
  # The keeper is the row the predicates already chose: the newest live one.
  op.execute(
    """
        UPDATE connections SET keeps_books = true
        WHERE id IN (
          SELECT DISTINCT ON (graph_id) id
          FROM connections
          WHERE provider = 'quickbooks'
            AND deleted_at IS NULL
            AND status NOT IN ('pending_oauth', 'disconnected', 'severed')
          ORDER BY graph_id, created_at DESC
        )
        """
  )
  op.create_index(
    "uq_connections_books_keeper",
    "connections",
    ["graph_id", "entity_id"],
    unique=True,
    postgresql_where="keeps_books AND deleted_at IS NULL",
  )


def downgrade() -> None:
  op.drop_index(
    "uq_connections_books_keeper",
    table_name="connections",
    postgresql_where="keeps_books AND deleted_at IS NULL",
  )
  op.drop_column("connections", "keeps_books")
  op.drop_column("connections", "entity_id")
  # ### end Alembic commands ###
