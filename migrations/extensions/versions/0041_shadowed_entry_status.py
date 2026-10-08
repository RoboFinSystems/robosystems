"""The `shadowed` entry status.

A shadow close (a QuickBooks connection whose write_policy is `shadow`)
posts nothing and publishes nothing: the drafts in its window become
expectations compared with what QuickBooks holds. They leave `draft` so no
later close, policy change or restamp can post or publish them, and they
are not `posted`, since they never landed. That is a fourth status, and the
CHECK on `entries.status` has to admit it.

Autogenerate does not see CHECK changes, so the constraint is replaced by
hand on `public` and on every tenant schema. The downgrade returns shadowed
rows to `draft` first; nothing else refers to the status.

Revision ID: 0041
Revises: 0040
Create Date: 2026-10-07
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0041"
down_revision = "0040"
branch_labels = None
depends_on = None

_OLD = "status IN ('draft', 'posted', 'reversed')"
_NEW = "status IN ('draft', 'posted', 'reversed', 'shadowed')"


def _widen(conn: Connection, schema: str) -> None:
  conn.execute(
    text(f'ALTER TABLE "{schema}".entries DROP CONSTRAINT IF EXISTS check_entry_status')
  )
  conn.execute(
    text(
      f'ALTER TABLE "{schema}".entries ADD CONSTRAINT check_entry_status CHECK ({_NEW})'
    )
  )


def _narrow(conn: Connection, schema: str) -> None:
  conn.execute(
    text(f"UPDATE \"{schema}\".entries SET status = 'draft' WHERE status = 'shadowed'")
  )
  conn.execute(
    text(f'ALTER TABLE "{schema}".entries DROP CONSTRAINT IF EXISTS check_entry_status')
  )
  conn.execute(
    text(
      f'ALTER TABLE "{schema}".entries ADD CONSTRAINT check_entry_status CHECK ({_OLD})'
    )
  )


def upgrade() -> None:
  conn = op.get_bind()
  _widen(conn, "public")
  for_each_tenant_schema(conn, _widen)


def downgrade() -> None:
  conn = op.get_bind()
  _narrow(conn, "public")
  for_each_tenant_schema(conn, _narrow)
