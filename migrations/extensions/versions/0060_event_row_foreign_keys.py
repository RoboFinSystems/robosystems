"""Every tenant carries the two event-row foreign keys.

`entries.triggered_by_event_id → events(id)` and `events.agent_id →
agents(id)` were added by 0005 to the tenant schemas that existed then. The
models never declared them, so every tenant built since by `create_all` has
neither: an entry can name an event that does not exist, and an event an
agent that was deleted. The models declare both now, under the names 0005
used, so new tenants get them; this adds them to the tenants built between.

Each schema is counted first, read-only. With no orphaned rows the
constraint is added and validated. With orphans it is added `NOT VALID`,
which holds every new row to it without failing the deploy on old ones, and
the count is printed; `VALIDATE CONSTRAINT` once they are repaired.

Revision ID: 0060
Revises: 0059
Create Date: 2026-10-21

"""

from alembic import op
from sqlalchemy import text
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import for_each_tenant_schema

revision = "0060"
down_revision = "0059"
branch_labels = None
depends_on = None

# (table, column, constraint, referenced table, ON DELETE clause)
FOREIGN_KEYS = (
  (
    "entries",
    "triggered_by_event_id",
    "fk_entries_triggered_by_event",
    "events",
    " ON DELETE RESTRICT",
  ),
  ("events", "agent_id", "fk_events_agent_id", "agents", ""),
)


def _has_foreign_key(conn: Connection, schema: str, table: str, column: str) -> bool:
  return bool(
    conn.execute(
      text(
        "SELECT 1 FROM pg_constraint c "
        "JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey) "
        "WHERE c.contype = 'f' AND c.conrelid = to_regclass(:table) "
        "AND a.attname = :column"
      ),
      {"table": f'"{schema}".{table}', "column": column},
    ).first()
  )


def _add(conn: Connection, schema: str) -> None:
  for table, column, name, target, on_delete in FOREIGN_KEYS:
    if conn.execute(
      text("SELECT to_regclass(:t)"), {"t": f'"{schema}".{table}'}
    ).scalar() is None or _has_foreign_key(conn, schema, table, column):
      continue
    orphans = conn.execute(
      text(
        f'SELECT count(*) FROM "{schema}".{table} r '
        f"WHERE r.{column} IS NOT NULL AND NOT EXISTS "
        f'(SELECT 1 FROM "{schema}".{target} t WHERE t.id = r.{column})'
      )
    ).scalar_one()
    validity = " NOT VALID" if orphans else ""
    conn.execute(
      text(
        f'ALTER TABLE "{schema}".{table} ADD CONSTRAINT "{name}" '
        f'FOREIGN KEY ({column}) REFERENCES "{schema}".{target}(id)'
        f"{on_delete}{validity}"
      )
    )
    if orphans:
      print(
        f"{schema}.{table}.{column}: {orphans} rows name a missing {target} "
        f'row; "{name}" added NOT VALID, validate once they are repaired.'
      )


def _drop(conn: Connection, schema: str) -> None:
  for table, _column, name, _target, _on_delete in FOREIGN_KEYS:
    conn.execute(
      text(
        f'ALTER TABLE IF EXISTS "{schema}".{table} DROP CONSTRAINT IF EXISTS "{name}"'
      )
    )


def upgrade() -> None:
  conn = op.get_bind()
  for_each_tenant_schema(conn, _add)


def downgrade() -> None:
  # Only the tenants this added them to: 0005's schemas had them before, and
  # their constraint names are schema-prefixed, so these names miss them.
  conn = op.get_bind()
  for_each_tenant_schema(conn, _drop)
