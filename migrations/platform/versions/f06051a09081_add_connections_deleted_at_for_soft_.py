"""Add connections.deleted_at for soft-delete + reuse-on-re-OAuth (B6).

Phase 3 Package B per `quickbooks-adapter.md` §4.6.2:

- `connections.deleted_at` — nullable timestamp. When set, the row is
  hidden from default lookups (`get_by_id`, `get_by_graph_and_provider`,
  `get_all_for_graph`, `list_filtered`) and the user-facing API surface
  treats the connection as gone.
- `idx_connections_soft_deleted_realm` — partial index supporting the
  reuse-on-re-OAuth query: when a user reconnects to the same QB realm
  after deleting a connection, the OAuth callback finds the prior
  soft-deleted row via `(graph_id, provider, realm_id)` and revives it
  in place. Preserves `connection_id` so the tenant-side
  events/agents/elements scoped to it stay attached.

Replaces the old hard-delete behavior which orphaned all
connection_id-scoped tenant rows.

Revision ID: f06051a09081
Revises: 17c5e1e50d67
Create Date: 2026-05-18

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f06051a09081"
down_revision = "17c5e1e50d67"
branch_labels = None
depends_on = None


def upgrade() -> None:
  op.add_column(
    "connections",
    sa.Column("deleted_at", sa.DateTime(), nullable=True),
  )
  op.create_index(
    "idx_connections_soft_deleted_realm",
    "connections",
    ["graph_id", "provider", "realm_id"],
    unique=False,
    postgresql_where="deleted_at IS NOT NULL",
  )


def downgrade() -> None:
  op.drop_index(
    "idx_connections_soft_deleted_realm",
    table_name="connections",
    postgresql_where="deleted_at IS NOT NULL",
  )
  op.drop_column("connections", "deleted_at")
