"""add report_definitions table

Revision ID: b5d9e3f7a1c2
Revises: a3f8b2c1d4e5
Create Date: 2026-03-29 18:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b5d9e3f7a1c2"
down_revision = "a3f8b2c1d4e5"
branch_labels = None
depends_on = None


def upgrade() -> None:
  # report_definitions is tenant-only (no public schema table).
  # Existing tenant schemas get the table via this migration.
  # New tenant schemas get it via provision_tenant_schema() → create_all().
  conn = op.get_bind()

  schemas = conn.execute(
    sa.text(
      "SELECT schema_name FROM information_schema.schemata "
      "WHERE schema_name ~ '^kg[0-9a-f]{16,}$'"
    )
  ).fetchall()

  for (schema_name,) in schemas:
    _create_report_definitions(conn, schema_name)


def _create_report_definitions(conn, schema: str) -> None:
  s = schema
  conn.execute(
    sa.text(f"""
      CREATE TABLE IF NOT EXISTS "{s}".report_definitions (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,

        -- Report type
        report_type TEXT NOT NULL,
        CONSTRAINT check_report_type CHECK (
          report_type IN ('income_statement', 'balance_sheet', 'cash_flow', 'equity', 'custom')
        ),

        -- Configuration
        mapping_id TEXT,
        period_type TEXT NOT NULL DEFAULT 'monthly',
        period_start DATE,
        period_end DATE,
        comparative BOOLEAN NOT NULL DEFAULT true,

        -- Generated output references
        graph_report_id TEXT,
        last_generated TIMESTAMPTZ,
        generation_status TEXT NOT NULL DEFAULT 'pending',
        CONSTRAINT check_generation_status CHECK (
          generation_status IN ('pending', 'generating', 'review', 'published', 'failed')
        ),

        -- AI provenance
        ai_generated BOOLEAN NOT NULL DEFAULT false,
        ai_intent TEXT,
        ai_workspace_id TEXT,
        ai_confidence DOUBLE PRECISION,

        -- Metadata
        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,

        -- Timestamps
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        created_by TEXT NOT NULL
      )
    """)
  )

  conn.execute(
    sa.text(
      f'CREATE INDEX IF NOT EXISTS idx_report_defs_type ON "{s}".report_definitions (report_type)'
    )
  )
  conn.execute(
    sa.text(
      f'CREATE INDEX IF NOT EXISTS idx_report_defs_status ON "{s}".report_definitions (generation_status)'
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
    conn.execute(sa.text(f'DROP TABLE IF EXISTS "{schema_name}".report_definitions'))
