"""report sharing tables and provenance columns

Revision ID: d8f3a5b2c4e6
Revises: c7e2f4a8b3d1
Create Date: 2026-03-30 02:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d8f3a5b2c4e6"
down_revision = "c7e2f4a8b3d1"
branch_labels = None
depends_on = None


def upgrade() -> None:
  conn = op.get_bind()

  schemas = conn.execute(
    sa.text(
      "SELECT schema_name FROM information_schema.schemata "
      "WHERE schema_name ~ '^kg[0-9a-f]{16,}$'"
    )
  ).fetchall()

  for (schema_name,) in schemas:
    _add_provenance_columns(conn, schema_name)
    _create_report_shares(conn, schema_name)


def _add_provenance_columns(conn, schema: str) -> None:
  """Add sharing provenance columns to report_definitions."""
  s = schema
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".report_definitions '
      f"ADD COLUMN IF NOT EXISTS source_graph_id TEXT"
    )
  )
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".report_definitions '
      f"ADD COLUMN IF NOT EXISTS source_report_id TEXT"
    )
  )
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".report_definitions '
      f"ADD COLUMN IF NOT EXISTS shared_at TIMESTAMPTZ"
    )
  )


def _create_report_shares(conn, schema: str) -> None:
  """Create report_shares table for tracking outbound shares."""
  s = schema
  conn.execute(
    sa.text(f"""
      CREATE TABLE IF NOT EXISTS "{s}".report_shares (
        id TEXT PRIMARY KEY,
        report_id TEXT NOT NULL,
        target_graph_id TEXT NOT NULL,
        shared_by TEXT NOT NULL,
        shared_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        revoked_at TIMESTAMPTZ,
        fact_count INTEGER NOT NULL DEFAULT 0
      )
    """)
  )

  conn.execute(
    sa.text(
      f"CREATE INDEX IF NOT EXISTS idx_report_shares_report "
      f'ON "{s}".report_shares (report_id)'
    )
  )
  conn.execute(
    sa.text(
      f"CREATE INDEX IF NOT EXISTS idx_report_shares_target "
      f'ON "{s}".report_shares (target_graph_id)'
    )
  )


def downgrade() -> None:
  conn = op.get_bind()

  schemas = conn.execute(
    sa.text(
      "SELECT schema_name FROM information_schema.schemata "
      "WHERE schema_name ~ '^kg[0-9a-f]{16,}$'"
    )
  ).fetchall()

  for (schema_name,) in schemas:
    s = schema_name
    conn.execute(sa.text(f'DROP TABLE IF EXISTS "{s}".report_shares'))
    conn.execute(
      sa.text(
        f'ALTER TABLE "{s}".report_definitions DROP COLUMN IF EXISTS source_graph_id'
      )
    )
    conn.execute(
      sa.text(
        f'ALTER TABLE "{s}".report_definitions DROP COLUMN IF EXISTS source_report_id'
      )
    )
    conn.execute(
      sa.text(f'ALTER TABLE "{s}".report_definitions DROP COLUMN IF EXISTS shared_at')
    )
