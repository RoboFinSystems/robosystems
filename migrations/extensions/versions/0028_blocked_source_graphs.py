"""blocked_source_graphs — the recipient's deny list for cross-graph shares

Cross-graph report sharing is capability-style: whoever holds a subscriber's
``graph_id`` can copy a published report into that subscriber's tenant schema.
The model only holds up if the recipient can refuse, and until now there was no
way to. This table is that refusal — ``share-report`` consults it in the target
schema before writing anything.

It is a tenant table: the block is the recipient's own state about their own
graph, needing no cross-tenant coordination, and it sits alongside
``report_shares`` and ``publish_lists``. Fresh deployments get it from
``create_tenant_schema``'s ``create_all`` over the model metadata; this
migration is the backfill path for schemas that already exist.

Revision ID: 0028
Revises: 0027
Create Date: 2026-08-09

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import Connection

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0028"
down_revision = "0027"
branch_labels = None
depends_on = None

_TABLE = "blocked_source_graphs"
_INDEX = "idx_blocked_source_graphs_source"
_UNIQUE = "uq_blocked_source_graphs_source"

_TENANT_DDL = f"""
  id TEXT PRIMARY KEY,
  source_graph_id TEXT NOT NULL,
  blocked_by TEXT NOT NULL,
  blocked_at TIMESTAMP NOT NULL DEFAULT now(),
  reason TEXT,
  CONSTRAINT {_UNIQUE} UNIQUE (source_graph_id)
"""


def _create_in_tenant(conn: Connection, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.create_table(_TABLE, _TENANT_DDL)
  t.create_index(_INDEX, _TABLE, ["source_graph_id"])


def _drop_in_tenant(conn: Connection, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.drop_index(_INDEX)
  t.drop_table(_TABLE)


def upgrade() -> None:
  op.create_table(
    _TABLE,
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("source_graph_id", sa.String(), nullable=False),
    sa.Column("blocked_by", sa.String(), nullable=False),
    sa.Column(
      "blocked_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.Column("reason", sa.String(), nullable=True),
    sa.PrimaryKeyConstraint("id"),
    sa.UniqueConstraint("source_graph_id", name=_UNIQUE),
  )
  op.create_index(_INDEX, _TABLE, ["source_graph_id"], unique=False)

  for_each_tenant_schema(op.get_bind(), _create_in_tenant)


def downgrade() -> None:
  for_each_tenant_schema(op.get_bind(), _drop_in_tenant)

  op.drop_index(_INDEX, table_name=_TABLE)
  op.drop_table(_TABLE)
