"""The `start_changed` fiscal calendar event.

An entity's calendar start can now move before its first close: earlier to
take in history (a bank feed brings months the calendar began after), later
to drop empty leading months. Each move is recorded in
`fiscal_calendar_events`, so the CHECK on `event_type` has to admit it.

Autogenerate does not see CHECK changes, so the constraint is replaced by
hand on `public` and on every tenant schema: added NOT VALID, then
validated, so the scan holds a share lock rather than an exclusive one.

The downgrade refuses while any schema holds a `start_changed` row; the
audit trail is not something a downgrade should silently delete.

Revision ID: 0042
Revises: 0041
Create Date: 2026-10-09
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0042"
down_revision = "0041"
branch_labels = None
depends_on = None

_NAME = "ck_fiscal_calendar_events_event_type"
_OLD = (
  "event_type IN ('initialized', 'target_changed', 'period_closed', "
  "'period_reopened', 'target_advanced_auto')"
)
_NEW = (
  "event_type IN ('initialized', 'target_changed', 'period_closed', "
  "'period_reopened', 'target_advanced_auto', 'start_changed')"
)


def _replace(conn: Connection, schema: str, check: str) -> None:
  table = f'"{schema}".fiscal_calendar_events'
  # By definition, not name: older schemas may carry the CHECK under another
  # name (the dual-naming history 0025 cleaned up for `events`).
  existing = conn.execute(
    text(
      "SELECT conname FROM pg_constraint "
      "WHERE conrelid = CAST(:table AS regclass) AND contype = 'c' "
      "AND pg_get_constraintdef(oid) LIKE '%event_type%'"
    ),
    {"table": table},
  ).scalars()
  for name in list(existing):
    conn.execute(text(f'ALTER TABLE {table} DROP CONSTRAINT "{name}"'))
  conn.execute(
    text(f"ALTER TABLE {table} ADD CONSTRAINT {_NAME} CHECK ({check}) NOT VALID")
  )
  conn.execute(text(f"ALTER TABLE {table} VALIDATE CONSTRAINT {_NAME}"))


def _widen(conn: Connection, schema: str) -> None:
  _replace(conn, schema, _NEW)


def _narrow(conn: Connection, schema: str) -> None:
  moved = conn.execute(
    text(
      f'SELECT count(*) FROM "{schema}".fiscal_calendar_events '
      "WHERE event_type = 'start_changed'"
    )
  ).scalar()
  if moved:
    raise RuntimeError(
      f"Schema {schema} holds {moved} start_changed calendar events; "
      "downgrading would delete that audit trail. Remove them deliberately first."
    )
  _replace(conn, schema, _OLD)


def upgrade() -> None:
  conn = op.get_bind()
  _widen(conn, "public")
  for_each_tenant_schema(conn, _widen)


def downgrade() -> None:
  conn = op.get_bind()
  _narrow(conn, "public")
  for_each_tenant_schema(conn, _narrow)
