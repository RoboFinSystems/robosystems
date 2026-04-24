"""add events table and triggered_by_event_id to transactions

Revision ID: 1fc9d9a31249
Revises: 0004
Create Date: 2026-04-24 12:16:07.322145

Both the public schema (template for new tenants) and every existing
tenant schema receive the same DDL — the standard multi-tenant pattern
for this extensions DB.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

revision = "1fc9d9a31249"
down_revision = "0004"
branch_labels = None
depends_on = None

_STATUS_CHECK = (
  "status IN ('captured', 'classified', 'committed', 'pending', "
  "'fulfilled', 'voided', 'superseded')"
)
_CATEGORY_CHECK = (
  "event_category IN ('sales', 'purchase', 'financing', 'payroll', "
  "'treasury', 'adjustment', 'recognition', 'other')"
)
_RESOURCE_TYPE_CHECK = (
  "resource_type IN ('goods', 'services', 'money', 'right', "
  "'obligation', 'information', 'labor') OR resource_type IS NULL"
)


def _create_in_tenant(conn, schema: str) -> None:
  conn.execute(
    text(f"""
      CREATE TABLE IF NOT EXISTS {schema}.events (
        id VARCHAR PRIMARY KEY,
        event_type VARCHAR NOT NULL,
        event_category VARCHAR NOT NULL,
        agent_id VARCHAR,
        resource_type VARCHAR,
        resource_element_id VARCHAR,
        occurred_at TIMESTAMP NOT NULL,
        effective_at TIMESTAMP,
        status VARCHAR NOT NULL DEFAULT 'captured',
        source VARCHAR NOT NULL,
        external_id VARCHAR,
        external_url VARCHAR,
        replaced_by_event_id VARCHAR,
        replaces_event_id VARCHAR,
        amount BIGINT,
        currency VARCHAR NOT NULL DEFAULT 'USD',
        description VARCHAR,
        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        created_by VARCHAR NOT NULL,
        CONSTRAINT check_{schema}_event_status CHECK ({_STATUS_CHECK}),
        CONSTRAINT check_{schema}_event_category CHECK ({_CATEGORY_CHECK}),
        CONSTRAINT check_{schema}_event_resource_type CHECK ({_RESOURCE_TYPE_CHECK})
      )
    """)
  )
  for idx_suffix, col in [
    ("type", "event_type"),
    ("category", "event_category"),
    ("occurred_at", "occurred_at"),
    ("status", "status"),
    ("agent", "agent_id"),
  ]:
    conn.execute(
      text(
        f"CREATE INDEX IF NOT EXISTS idx_{schema}_events_{idx_suffix} "
        f"ON {schema}.events ({col})"
      )
    )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_events_source_external "
      f"ON {schema}.events (source, external_id)"
    )
  )

  conn.execute(
    text(f"""
      CREATE TABLE IF NOT EXISTS {schema}.event_dimensions (
        event_id VARCHAR NOT NULL
          REFERENCES {schema}.events(id) ON DELETE CASCADE,
        dimension_id VARCHAR NOT NULL
          REFERENCES {schema}.dimensions(id) ON DELETE RESTRICT,
        PRIMARY KEY (event_id, dimension_id)
      )
    """)
  )

  t = TenantOps(conn, schema)
  t.add_column("transactions", "triggered_by_event_id", "VARCHAR")
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_transactions_triggered_by_event "
      f"ON {schema}.transactions (triggered_by_event_id) "
      f"WHERE triggered_by_event_id IS NOT NULL"
    )
  )


def _drop_in_tenant(conn, schema: str) -> None:
  conn.execute(
    text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_transactions_triggered_by_event")
  )
  TenantOps(conn, schema).drop_column("transactions", "triggered_by_event_id")
  conn.execute(text(f"DROP TABLE IF EXISTS {schema}.event_dimensions"))
  for idx_suffix in (
    "source_external",
    "agent",
    "status",
    "occurred_at",
    "category",
    "type",
  ):
    conn.execute(
      text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_events_{idx_suffix}")
    )
  conn.execute(text(f"DROP TABLE IF EXISTS {schema}.events"))


def upgrade() -> None:
  # Public schema — events table (template for new tenants)
  op.create_table(
    "events",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("event_type", sa.String(), nullable=False),
    sa.Column("event_category", sa.String(), nullable=False),
    sa.Column("agent_id", sa.String(), nullable=True),
    sa.Column("resource_type", sa.String(), nullable=True),
    sa.Column("resource_element_id", sa.String(), nullable=True),
    sa.Column("occurred_at", sa.DateTime(), nullable=False),
    sa.Column("effective_at", sa.DateTime(), nullable=True),
    sa.Column("status", sa.String(), nullable=False, server_default="captured"),
    sa.Column("source", sa.String(), nullable=False),
    sa.Column("external_id", sa.String(), nullable=True),
    sa.Column("external_url", sa.String(), nullable=True),
    sa.Column("replaced_by_event_id", sa.String(), nullable=True),
    sa.Column("replaces_event_id", sa.String(), nullable=True),
    sa.Column("amount", sa.BigInteger(), nullable=True),
    sa.Column("currency", sa.String(), nullable=False, server_default="USD"),
    sa.Column("description", sa.String(), nullable=True),
    sa.Column(
      "metadata",
      postgresql.JSONB(astext_type=sa.Text()),
      nullable=False,
      server_default=sa.text("'{}'::jsonb"),
    ),
    sa.Column(
      "created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.Column("created_by", sa.String(), nullable=False),
    sa.CheckConstraint(_STATUS_CHECK, name="check_event_status"),
    sa.CheckConstraint(_CATEGORY_CHECK, name="check_event_category"),
    sa.CheckConstraint(_RESOURCE_TYPE_CHECK, name="check_event_resource_type"),
    sa.PrimaryKeyConstraint("id"),
  )
  op.create_index("idx_events_type", "events", ["event_type"])
  op.create_index("idx_events_category", "events", ["event_category"])
  op.create_index("idx_events_occurred_at", "events", ["occurred_at"])
  op.create_index("idx_events_status", "events", ["status"])
  op.create_index("idx_events_agent", "events", ["agent_id"])
  op.create_index("idx_events_source_external", "events", ["source", "external_id"])

  # Public schema — event_dimensions junction
  op.create_table(
    "event_dimensions",
    sa.Column("event_id", sa.String(), nullable=False),
    sa.Column("dimension_id", sa.String(), nullable=False),
    sa.ForeignKeyConstraint(["event_id"], ["events.id"], ondelete="CASCADE"),
    sa.ForeignKeyConstraint(["dimension_id"], ["dimensions.id"], ondelete="RESTRICT"),
    sa.PrimaryKeyConstraint("event_id", "dimension_id"),
  )

  # Public schema — triggered_by_event_id on transactions
  op.add_column(
    "transactions", sa.Column("triggered_by_event_id", sa.String(), nullable=True)
  )
  op.create_index(
    "idx_transactions_triggered_by_event",
    "transactions",
    ["triggered_by_event_id"],
    postgresql_where="triggered_by_event_id IS NOT NULL",
  )

  # Tenant schemas — apply the same DDL to every existing tenant
  conn = op.get_bind()
  for_each_tenant_schema(conn, _create_in_tenant)


def downgrade() -> None:
  conn = op.get_bind()
  for_each_tenant_schema(conn, _drop_in_tenant)

  op.drop_index(
    "idx_transactions_triggered_by_event",
    table_name="transactions",
    postgresql_where="triggered_by_event_id IS NOT NULL",
  )
  op.drop_column("transactions", "triggered_by_event_id")
  op.drop_table("event_dimensions")
  op.drop_index("idx_events_source_external", table_name="events")
  op.drop_index("idx_events_agent", table_name="events")
  op.drop_index("idx_events_status", table_name="events")
  op.drop_index("idx_events_occurred_at", table_name="events")
  op.drop_index("idx_events_category", table_name="events")
  op.drop_index("idx_events_type", table_name="events")
  op.drop_table("events")
