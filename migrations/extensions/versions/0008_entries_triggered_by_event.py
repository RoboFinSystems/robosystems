"""Phase 4b — add entries.triggered_by_event_id for event audit chain.

Adds a parallel audit column on `entries` to match `transactions.triggered_by_event_id`
(Phase 1). Needed because `create_manual_closing_entry` creates an Entry with
`transaction_id=None` — closing entries have no Transaction parent, so the Phase 1
column on transactions doesn't cover them.

The `asset_disposed` event handler (and future `schedule_entry_due` handler in
Phase 4a) sets this column so every GL entry produced by an event links back to
its originating event.

Revision ID: 0008
Revises: 0007
Create Date: 2026-04-24
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def _upgrade_in_tenant(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.add_column("entries", "triggered_by_event_id", "VARCHAR")
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_entries_triggered_by_event "
      f"ON {schema}.entries (triggered_by_event_id) "
      f"WHERE triggered_by_event_id IS NOT NULL"
    )
  )
  conn.execute(
    text(
      f"ALTER TABLE {schema}.entries ADD CONSTRAINT "
      f"fk_{schema}_entries_triggered_by_event "
      f"FOREIGN KEY (triggered_by_event_id) REFERENCES {schema}.events(id) "
      f"ON DELETE RESTRICT"
    )
  )


def _downgrade_in_tenant(conn, schema: str) -> None:
  TenantOps(conn, schema).drop_constraint(
    "entries", f"fk_{schema}_entries_triggered_by_event"
  )
  conn.execute(
    text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_entries_triggered_by_event")
  )
  TenantOps(conn, schema).drop_column("entries", "triggered_by_event_id")


def upgrade() -> None:
  # Public schema
  op.add_column(
    "entries", sa.Column("triggered_by_event_id", sa.String(), nullable=True)
  )
  op.create_index(
    "idx_entries_triggered_by_event",
    "entries",
    ["triggered_by_event_id"],
    postgresql_where="triggered_by_event_id IS NOT NULL",
  )
  op.create_foreign_key(
    "fk_entries_triggered_by_event",
    "entries",
    "events",
    ["triggered_by_event_id"],
    ["id"],
    ondelete="RESTRICT",
  )

  # Tenant schemas
  conn = op.get_bind()
  for_each_tenant_schema(conn, _upgrade_in_tenant)


def downgrade() -> None:
  conn = op.get_bind()
  for_each_tenant_schema(conn, _downgrade_in_tenant)

  op.drop_constraint("fk_entries_triggered_by_event", "entries", type_="foreignkey")
  op.drop_index("idx_entries_triggered_by_event", table_name="entries")
  op.drop_column("entries", "triggered_by_event_id")
