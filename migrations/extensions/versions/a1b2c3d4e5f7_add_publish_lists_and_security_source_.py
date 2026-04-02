"""add publish lists and security source_graph_id

Revision ID: a1b2c3d4e5f7
Revises: f4c96d54073f
Create Date: 2026-04-01 23:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f7"
down_revision = "f4c96d54073f"
branch_labels = None
depends_on = None


def _create_publish_lists(conn, schema: str) -> None:
  """Create publish_lists and publish_list_members tables in a tenant schema."""
  s = schema
  conn.execute(
    sa.text(f"""
      CREATE TABLE IF NOT EXISTS "{s}".publish_lists (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        created_by TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT uq_publish_lists_name UNIQUE (name)
      )
    """)
  )
  conn.execute(
    sa.text(
      f'CREATE INDEX IF NOT EXISTS idx_publish_lists_name ON "{s}".publish_lists (name)'
    )
  )
  conn.execute(
    sa.text(f"""
      CREATE TABLE IF NOT EXISTS "{s}".publish_list_members (
        id TEXT PRIMARY KEY,
        publish_list_id TEXT NOT NULL REFERENCES "{s}".publish_lists(id) ON DELETE CASCADE,
        target_graph_id TEXT NOT NULL,
        added_by TEXT NOT NULL,
        added_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT uq_publish_list_members_pair UNIQUE (publish_list_id, target_graph_id)
      )
    """)
  )
  conn.execute(
    sa.text(
      f'CREATE INDEX IF NOT EXISTS idx_plm_list ON "{s}".publish_list_members (publish_list_id)'
    )
  )
  conn.execute(
    sa.text(
      f'CREATE INDEX IF NOT EXISTS idx_plm_target ON "{s}".publish_list_members (target_graph_id)'
    )
  )


def _add_source_graph_id(conn, schema: str) -> None:
  """Add source_graph_id column to securities table in a tenant schema."""
  s = schema
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".securities ADD COLUMN IF NOT EXISTS source_graph_id TEXT'
    )
  )
  conn.execute(
    sa.text(
      f'CREATE INDEX IF NOT EXISTS idx_securities_source_graph ON "{s}".securities (source_graph_id)'
    )
  )


def upgrade() -> None:
  # New tables — created in public schema by Alembic.
  # Tenant schemas pick them up via provision_tenant_schema() -> create_all().
  op.create_table(
    "publish_lists",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("name", sa.String(), nullable=False),
    sa.Column("description", sa.String(), nullable=True),
    sa.Column("created_by", sa.String(), nullable=False),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("updated_at", sa.DateTime(), nullable=False),
    sa.PrimaryKeyConstraint("id"),
    sa.UniqueConstraint("name", name="uq_publish_lists_name"),
  )
  op.create_index("idx_publish_lists_name", "publish_lists", ["name"], unique=False)

  op.create_table(
    "publish_list_members",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("publish_list_id", sa.String(), nullable=False),
    sa.Column("target_graph_id", sa.String(), nullable=False),
    sa.Column("added_by", sa.String(), nullable=False),
    sa.Column("added_at", sa.DateTime(), nullable=False),
    sa.ForeignKeyConstraint(
      ["publish_list_id"],
      ["publish_lists.id"],
      ondelete="CASCADE",
    ),
    sa.PrimaryKeyConstraint("id"),
    sa.UniqueConstraint(
      "publish_list_id", "target_graph_id", name="uq_publish_list_members_pair"
    ),
  )
  op.create_index(
    "idx_plm_list", "publish_list_members", ["publish_list_id"], unique=False
  )
  op.create_index(
    "idx_plm_target", "publish_list_members", ["target_graph_id"], unique=False
  )

  # Existing tenant schemas need the new tables and column added directly.
  # provision_tenant_schema() handles new graphs, but existing ones need migration.
  conn = op.get_bind()
  schemas = conn.execute(
    sa.text(
      "SELECT schema_name FROM information_schema.schemata "
      "WHERE schema_name ~ '^kg[0-9a-f]{16,}$'"
    )
  ).fetchall()

  for (schema_name,) in schemas:
    _create_publish_lists(conn, schema_name)
    _add_source_graph_id(conn, schema_name)


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
    conn.execute(sa.text(f'DROP INDEX IF EXISTS "{s}".idx_securities_source_graph'))
    conn.execute(
      sa.text(f'ALTER TABLE "{s}".securities DROP COLUMN IF EXISTS source_graph_id')
    )
    conn.execute(sa.text(f'DROP TABLE IF EXISTS "{s}".publish_list_members'))
    conn.execute(sa.text(f'DROP TABLE IF EXISTS "{s}".publish_lists'))

  op.drop_index("idx_plm_target", table_name="publish_list_members")
  op.drop_index("idx_plm_list", table_name="publish_list_members")
  op.drop_table("publish_list_members")
  op.drop_index("idx_publish_lists_name", table_name="publish_lists")
  op.drop_table("publish_lists")
