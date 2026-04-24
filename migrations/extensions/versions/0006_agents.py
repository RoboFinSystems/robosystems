"""Phase 2 — agents table + FK from events.agent_id.

Creates the `agents` counterparty table in both the public schema
(template for new tenants) and every existing tenant schema.

Adds a real FK constraint from events.agent_id → agents.id. The column
already exists (landed in Phase 1 as plain VARCHAR); this migration
promotes it to a referential FK so bogus agent IDs are rejected at the DB
level.

Revision ID: 0006
Revises: 1fc9d9a31249
Create Date: 2026-04-24
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

revision = "0006"
down_revision = "1fc9d9a31249"
branch_labels = None
depends_on = None

_AGENT_TYPE_CHECK = (
  "agent_type IN ('customer', 'vendor', 'employee', 'owner', "
  "'supplier', 'government', 'lender', 'self', 'other')"
)


def _create_in_tenant(conn, schema: str) -> None:
  conn.execute(
    text(f"""
      CREATE TABLE IF NOT EXISTS {schema}.agents (
        id VARCHAR PRIMARY KEY,
        agent_type VARCHAR NOT NULL,
        name VARCHAR NOT NULL,
        legal_name VARCHAR,
        tax_id VARCHAR,
        registration_number VARCHAR,
        duns VARCHAR,
        lei VARCHAR,
        email VARCHAR,
        phone VARCHAR,
        address JSONB,
        source VARCHAR NOT NULL DEFAULT 'native',
        external_id VARCHAR,
        is_active BOOLEAN NOT NULL DEFAULT true,
        is_1099_recipient BOOLEAN NOT NULL DEFAULT false,
        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        created_by VARCHAR NOT NULL,
        CONSTRAINT check_{schema}_agent_type CHECK ({_AGENT_TYPE_CHECK})
      )
    """)
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_agents_type "
      f"ON {schema}.agents (agent_type)"
    )
  )
  conn.execute(
    text(
      f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{schema}_agents_source_external "
      f"ON {schema}.agents (source, external_id) "
      f"WHERE external_id IS NOT NULL"
    )
  )

  # Promote events.agent_id to a proper FK
  conn.execute(
    text(
      f"ALTER TABLE {schema}.events ADD CONSTRAINT "
      f"fk_{schema}_events_agent_id "
      f"FOREIGN KEY (agent_id) REFERENCES {schema}.agents(id)"
    )
  )


def _drop_in_tenant(conn, schema: str) -> None:
  # Drop FK first, then table
  TenantOps(conn, schema).drop_constraint("events", f"fk_{schema}_events_agent_id")
  conn.execute(
    text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_agents_source_external")
  )
  conn.execute(text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_agents_type"))
  conn.execute(text(f"DROP TABLE IF EXISTS {schema}.agents"))


def upgrade() -> None:
  # Public schema — agents table
  op.create_table(
    "agents",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("agent_type", sa.String(), nullable=False),
    sa.Column("name", sa.String(), nullable=False),
    sa.Column("legal_name", sa.String(), nullable=True),
    sa.Column("tax_id", sa.String(), nullable=True),
    sa.Column("registration_number", sa.String(), nullable=True),
    sa.Column("duns", sa.String(), nullable=True),
    sa.Column("lei", sa.String(), nullable=True),
    sa.Column("email", sa.String(), nullable=True),
    sa.Column("phone", sa.String(), nullable=True),
    sa.Column(
      "address", sa.dialects.postgresql.JSONB(astext_type=sa.Text()), nullable=True
    ),
    sa.Column("source", sa.String(), nullable=False, server_default="native"),
    sa.Column("external_id", sa.String(), nullable=True),
    sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
    sa.Column(
      "is_1099_recipient", sa.Boolean(), nullable=False, server_default="false"
    ),
    sa.Column(
      "metadata",
      sa.dialects.postgresql.JSONB(astext_type=sa.Text()),
      nullable=False,
      server_default=sa.text("'{}'::jsonb"),
    ),
    sa.Column(
      "created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.Column(
      "updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.Column("created_by", sa.String(), nullable=False),
    sa.CheckConstraint(_AGENT_TYPE_CHECK, name="check_agent_type"),
    sa.PrimaryKeyConstraint("id"),
  )
  op.create_index("idx_agents_type", "agents", ["agent_type"])
  op.create_index(
    "idx_agents_source_external",
    "agents",
    ["source", "external_id"],
    unique=True,
    postgresql_where="external_id IS NOT NULL",
  )

  # Public schema — promote events.agent_id to FK
  op.create_foreign_key("fk_events_agent_id", "events", "agents", ["agent_id"], ["id"])

  # Tenant schemas
  conn = op.get_bind()
  for_each_tenant_schema(conn, _create_in_tenant)


def downgrade() -> None:
  conn = op.get_bind()
  for_each_tenant_schema(conn, _drop_in_tenant)

  op.drop_constraint("fk_events_agent_id", "events", type_="foreignkey")
  op.drop_index("idx_agents_source_external", table_name="agents")
  op.drop_index("idx_agents_type", table_name="agents")
  op.drop_table("agents")
