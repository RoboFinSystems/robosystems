"""report taxonomy_id and report_facts table

Revision ID: c7e2f4a8b3d1
Revises: b5d9e3f7a1c2
Create Date: 2026-03-29 20:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c7e2f4a8b3d1"
down_revision = "b5d9e3f7a1c2"
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
    _migrate_report_definitions(conn, schema_name)
    _create_report_facts(conn, schema_name)


def _migrate_report_definitions(conn, schema: str) -> None:
  """Add taxonomy_id, migrate data from report_type, drop report_type."""
  s = schema

  # Add taxonomy_id column
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".report_definitions ADD COLUMN IF NOT EXISTS taxonomy_id TEXT'
    )
  )

  # Migrate existing rows: all current reports use US GAAP
  conn.execute(
    sa.text(
      f'UPDATE "{s}".report_definitions '
      f"SET taxonomy_id = 'tax_usgaap_reporting' "
      f"WHERE taxonomy_id IS NULL"
    )
  )

  # Set NOT NULL
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".report_definitions ALTER COLUMN taxonomy_id SET NOT NULL'
    )
  )

  # Drop the old report_type column and its CHECK constraint
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".report_definitions '
      f"DROP CONSTRAINT IF EXISTS check_report_type"
    )
  )
  conn.execute(
    sa.text(f'ALTER TABLE "{s}".report_definitions DROP COLUMN IF EXISTS report_type')
  )

  # Update index
  conn.execute(sa.text(f'DROP INDEX IF EXISTS "{s}".idx_report_defs_type'))
  conn.execute(
    sa.text(
      f"CREATE INDEX IF NOT EXISTS idx_report_defs_taxonomy "
      f'ON "{s}".report_definitions (taxonomy_id)'
    )
  )


def _create_report_facts(conn, schema: str) -> None:
  """Create report_facts table for storing generated financial data points."""
  s = schema
  conn.execute(
    sa.text(f"""
      CREATE TABLE IF NOT EXISTS "{s}".report_facts (
        id TEXT PRIMARY KEY,
        report_id TEXT NOT NULL,
        element_id TEXT NOT NULL,
        value DOUBLE PRECISION NOT NULL,
        period_start DATE,
        period_end DATE NOT NULL,
        period_type TEXT NOT NULL,
        unit TEXT NOT NULL DEFAULT 'USD',
        entity_id TEXT NOT NULL,
        fact_set_id TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )
    """)
  )

  conn.execute(
    sa.text(
      f"CREATE INDEX IF NOT EXISTS idx_report_facts_report "
      f'ON "{s}".report_facts (report_id)'
    )
  )
  conn.execute(
    sa.text(
      f"CREATE INDEX IF NOT EXISTS idx_report_facts_element "
      f'ON "{s}".report_facts (element_id)'
    )
  )
  conn.execute(
    sa.text(
      f"CREATE INDEX IF NOT EXISTS idx_report_facts_period "
      f'ON "{s}".report_facts (period_start, period_end)'
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

    # Drop report_facts table
    conn.execute(sa.text(f'DROP TABLE IF EXISTS "{s}".report_facts'))

    # Restore report_type column
    conn.execute(
      sa.text(
        f'ALTER TABLE "{s}".report_definitions '
        f"ADD COLUMN IF NOT EXISTS report_type TEXT"
      )
    )
    conn.execute(
      sa.text(
        f'UPDATE "{s}".report_definitions '
        f"SET report_type = 'custom' WHERE report_type IS NULL"
      )
    )
    conn.execute(
      sa.text(
        f'ALTER TABLE "{s}".report_definitions ALTER COLUMN report_type SET NOT NULL'
      )
    )

    # Drop taxonomy_id
    conn.execute(
      sa.text(f'ALTER TABLE "{s}".report_definitions DROP COLUMN IF EXISTS taxonomy_id')
    )
    conn.execute(sa.text(f'DROP INDEX IF EXISTS "{s}".idx_report_defs_taxonomy'))
