"""Consolidation method, role and membership interval on entities.

A group's consolidated statements need to know how each member is treated
(spec ``consolidation`` §4.1 to §4.2, C1): ``consolidation_method`` — ``full``
sums the member's books and carries an outside owner's share as
noncontrolling interest, ``equity`` collapses it to one investment and one
income line, ``cost`` carries an investment, ``none`` is the group parent,
the consolidation entity and a linked counterparty; ``entity_role`` —
``operating`` keeps a company's books, ``consolidation`` holds the
consolidating entries that only the consolidated statements read; and the
membership interval ``joined_on`` / ``left_on``, by month, both null by
default: null ``joined_on`` is a member since the beginning, null ``left_on``
a member still. Not the creation month — a row created after its calendar
starts (RFS in Harbinger, every Driftline member on a demo reset) would drop
out of its own history.

Per tenant schema: the four columns, then the backfill. Every own entity
except the group parent takes the method its recorded share implies — half
or more ``full``, a fifth to a half ``equity``, less ``cost``, unrecorded
``full`` (the same rule as ``commands.entity.derived_consolidation_method``,
and the same set the combined view summed before the method existed) — so
no graph's combined read moves. A consolidation entity made before the role
existed (the demo group's) is typed by ``update-entity`` on its next run;
until then it is a ``full`` member and summed in, as it was.

Fresh tenants get the columns from the model.

Revision ID: 0051
Revises: 0050
Create Date: 2026-10-17
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

revision = "0051"
down_revision = "0050"
branch_labels = None
depends_on = None

# column -> (SQL type, SQL default, nullable)
COLUMNS: dict[str, tuple[str, str | None, bool]] = {
  "consolidation_method": ("VARCHAR", "'none'", False),
  "entity_role": ("VARCHAR", "'operating'", False),
  "joined_on": ("DATE", None, True),
  "left_on": ("DATE", None, True),
}


def _group_parent(conn: Connection, schema: str) -> str | None:
  return conn.execute(
    text(
      f"""
      SELECT id FROM "{schema}".entities
      WHERE is_parent = true AND source <> 'linked'
      ORDER BY created_at ASC LIMIT 1
      """
    )
  ).scalar_one_or_none()


def _backfill(conn: Connection, schema: str) -> None:
  parent_id = _group_parent(conn, schema)
  if parent_id is None:
    return
  conn.execute(
    text(
      f"""
      UPDATE "{schema}".entities SET consolidation_method = CASE
        WHEN ownership_pct IS NULL OR ownership_pct >= 50 THEN 'full'
        WHEN ownership_pct >= 20 THEN 'equity'
        ELSE 'cost'
      END
      WHERE source <> 'linked' AND id <> :parent
        AND consolidation_method = 'none' AND entity_role = 'operating'
      """
    ),
    {"parent": parent_id},
  )


def _add(conn: Connection, schema: str) -> None:
  t = TenantOps(conn, schema)
  for column, (col_type, default, nullable) in COLUMNS.items():
    t.add_column("entities", column, col_type, nullable=nullable, default=default)
  _backfill(conn, schema)


def _drop(conn: Connection, schema: str) -> None:
  t = TenantOps(conn, schema)
  for column in COLUMNS:
    t.drop_column("entities", column)


def upgrade() -> None:
  op.add_column(
    "entities",
    sa.Column(
      "consolidation_method", sa.String(), nullable=False, server_default="none"
    ),
  )
  op.add_column(
    "entities",
    sa.Column("entity_role", sa.String(), nullable=False, server_default="operating"),
  )
  op.add_column("entities", sa.Column("joined_on", sa.Date(), nullable=True))
  op.add_column("entities", sa.Column("left_on", sa.Date(), nullable=True))
  for_each_tenant_schema(op.get_bind(), _add)


def downgrade() -> None:
  for_each_tenant_schema(op.get_bind(), _drop)
  for column in reversed(COLUMNS):
    op.drop_column("entities", column)
