"""Add connections.deleted_at + write_policy.

Two cohesive additions to the `connections` table, consolidated into a
single migration since they ship in the same branch and both shape the
connection-lifecycle story:

1. **`deleted_at`** — nullable timestamp. When set, the row is hidden
   from default lookups (`get_by_id`, `get_by_graph_and_provider`,
   `get_all_for_graph`, `list_filtered`) and the user-facing API surface
   treats the connection as gone. Replaces the old hard-delete path which
   orphaned all connection_id-scoped tenant rows.

2. **`idx_connections_soft_deleted_realm`** (reuse-on-re-OAuth)
   — partial index supporting the OAuth callback's reuse query: a
   re-OAuth to the same QB realm finds the prior soft-deleted row via
   `(graph_id, provider, realm_id)` and revives it in place.

3. **`write_policy`** — per-connection authorization flag
   governing whether RoboSystems-originated events publish to the
   source-of-truth system. Replaces the loader's legacy
   `_SOURCE_AUTO_COMMITS` hardcode (`"quickbooks": True`) with a
   per-row truth. Values: `'native'` (RoboSystems is source-of-truth;
   no outbound publish), `'qb_authoritative'` (QB is source-of-truth;
   RL events publish to QB and local GL is draft until QB accepts),
   `'hybrid'` (qb_authoritative with exception-flagging — v1 ships
   only native + qb_authoritative).

4. **Behavior-preserving backfill**: existing QuickBooks connections
   were auto-committing under the legacy `_SOURCE_AUTO_COMMITS`
   hardcode. Without the UPDATE below the new per-connection
   `write_policy='native'` default would silently flip their inbound
   sync to inbox-review mode — a regression. Backfill to
   `'qb_authoritative'` to preserve behavior; new QB connections also
   default to `'qb_authoritative'` via
   `ConnectionService.create_connection`.

Revision ID: f06051a09081
Revises: 17c5e1e50d67
Create Date: 2026-05-18 (deleted_at) / 2026-05-19 (write_policy consolidation)

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f06051a09081"
down_revision = "17c5e1e50d67"
branch_labels = None
depends_on = None


def upgrade() -> None:
  # Soft-delete primitives
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

  # write_policy authorization flag
  op.add_column(
    "connections",
    sa.Column(
      "write_policy",
      sa.String(),
      server_default="native",
      nullable=False,
    ),
  )
  # Behavior-preserving backfill for existing QB connections.
  op.execute(
    "UPDATE connections "
    "SET write_policy = 'qb_authoritative' "
    "WHERE provider = 'quickbooks'"
  )


def downgrade() -> None:
  op.drop_column("connections", "write_policy")
  op.drop_index(
    "idx_connections_soft_deleted_realm",
    table_name="connections",
    postgresql_where="deleted_at IS NOT NULL",
  )
  op.drop_column("connections", "deleted_at")
