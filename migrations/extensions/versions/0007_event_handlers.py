"""Phase 3 — event_handlers table + classification_rules backfill.

Creates the `event_handlers` dynamic rule registry in both the public schema
and every existing tenant schema. This is the generalization of ClassificationRule
(bank-feed only) to all event types.

Also backfills any existing `classification_rules` rows into `event_handlers`
with event_type='bank_transaction'. The backfill is idempotent (WHERE NOT EXISTS
guard). ClassificationRule table is kept for backwards compatibility.

Revision ID: 0007
Revises: 0006
Create Date: 2026-04-24
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import for_each_tenant_schema

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None

_ORIGIN_CHECK = "origin IN ('hub', 'tenant')"


def _create_in_tenant(conn, schema: str) -> None:
  conn.execute(
    text(f"""
      CREATE TABLE IF NOT EXISTS {schema}.event_handlers (
        id VARCHAR PRIMARY KEY,
        name VARCHAR NOT NULL,
        description VARCHAR,
        event_type VARCHAR NOT NULL,
        event_category VARCHAR,
        match_source VARCHAR,
        match_agent_type VARCHAR,
        match_resource_type VARCHAR,
        match_metadata_expression JSONB,
        transaction_template JSONB NOT NULL,
        priority INTEGER NOT NULL DEFAULT 0,
        is_active BOOLEAN NOT NULL DEFAULT true,
        origin VARCHAR NOT NULL DEFAULT 'tenant',
        suggested_by VARCHAR,
        confidence FLOAT,
        approved_by VARCHAR,
        approved_at TIMESTAMP,
        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        created_by VARCHAR NOT NULL,
        CONSTRAINT check_{schema}_handler_origin CHECK ({_ORIGIN_CHECK})
      )
    """)
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_handlers_event_type "
      f"ON {schema}.event_handlers (event_type, is_active)"
    )
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_handlers_priority "
      f"ON {schema}.event_handlers (priority)"
    )
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_handlers_unapproved "
      f"ON {schema}.event_handlers (approved_by) "
      f"WHERE approved_by IS NULL AND suggested_by = 'ai'"
    )
  )

  # Backfill from classification_rules (idempotent)
  conn.execute(
    text(f"""
      INSERT INTO {schema}.event_handlers
        (id, name, event_type, match_source,
         transaction_template, priority, is_active, origin,
         suggested_by, confidence, approved_by, approved_at,
         created_at, updated_at, created_by)
      SELECT
        'hdl_' || substr(id, 6),
        name,
        'bank_transaction',
        match_source,
        jsonb_build_object(
          'transactions', jsonb_build_array(
            jsonb_build_object(
              'entry_template', jsonb_build_object(
                'debit',  jsonb_build_object(
                  'element_id', debit_element_id,
                  'amount',     '{{{{ event.amount }}}}'
                ),
                'credit', jsonb_build_object(
                  'element_id', credit_element_id,
                  'amount',     '{{{{ event.amount }}}}'
                )
              )
            )
          )
        ),
        priority,
        is_active,
        'tenant',
        suggested_by,
        confidence,
        approved_by,
        approved_at,
        created_at,
        updated_at,
        created_by
      FROM {schema}.classification_rules
      WHERE NOT EXISTS (
        SELECT 1 FROM {schema}.event_handlers
        WHERE event_type = 'bank_transaction'
          AND name = classification_rules.name
      )
    """)
  )


def _drop_in_tenant(conn, schema: str) -> None:
  conn.execute(text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_handlers_unapproved"))
  conn.execute(text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_handlers_priority"))
  conn.execute(text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_handlers_event_type"))
  conn.execute(text(f"DROP TABLE IF EXISTS {schema}.event_handlers"))


def upgrade() -> None:
  # Public schema — event_handlers table
  op.create_table(
    "event_handlers",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("name", sa.String(), nullable=False),
    sa.Column("description", sa.String(), nullable=True),
    sa.Column("event_type", sa.String(), nullable=False),
    sa.Column("event_category", sa.String(), nullable=True),
    sa.Column("match_source", sa.String(), nullable=True),
    sa.Column("match_agent_type", sa.String(), nullable=True),
    sa.Column("match_resource_type", sa.String(), nullable=True),
    sa.Column(
      "match_metadata_expression",
      sa.dialects.postgresql.JSONB(astext_type=sa.Text()),
      nullable=True,
    ),
    sa.Column(
      "transaction_template",
      sa.dialects.postgresql.JSONB(astext_type=sa.Text()),
      nullable=False,
    ),
    sa.Column("priority", sa.Integer(), nullable=False, server_default="0"),
    sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
    sa.Column("origin", sa.String(), nullable=False, server_default="tenant"),
    sa.Column("suggested_by", sa.String(), nullable=True),
    sa.Column("confidence", sa.Float(), nullable=True),
    sa.Column("approved_by", sa.String(), nullable=True),
    sa.Column("approved_at", sa.DateTime(), nullable=True),
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
    sa.CheckConstraint(_ORIGIN_CHECK, name="check_handler_origin"),
    sa.PrimaryKeyConstraint("id"),
  )
  op.create_index(
    "idx_handlers_event_type", "event_handlers", ["event_type", "is_active"]
  )
  op.create_index("idx_handlers_priority", "event_handlers", ["priority"])
  op.create_index(
    "idx_handlers_unapproved",
    "event_handlers",
    ["approved_by"],
    postgresql_where="approved_by IS NULL AND suggested_by = 'ai'",
  )

  # Backfill classification_rules → event_handlers in public schema
  conn = op.get_bind()
  conn.execute(
    text("""
      INSERT INTO event_handlers
        (id, name, event_type, match_source,
         transaction_template, priority, is_active, origin,
         suggested_by, confidence, approved_by, approved_at,
         created_at, updated_at, created_by)
      SELECT
        'hdl_' || substr(id, 6),
        name,
        'bank_transaction',
        match_source,
        jsonb_build_object(
          'transactions', jsonb_build_array(
            jsonb_build_object(
              'entry_template', jsonb_build_object(
                'debit',  jsonb_build_object(
                  'element_id', debit_element_id,
                  'amount',     '{{ event.amount }}'
                ),
                'credit', jsonb_build_object(
                  'element_id', credit_element_id,
                  'amount',     '{{ event.amount }}'
                )
              )
            )
          )
        ),
        priority,
        is_active,
        'tenant',
        suggested_by,
        confidence,
        approved_by,
        approved_at,
        created_at,
        updated_at,
        created_by
      FROM classification_rules
      WHERE NOT EXISTS (
        SELECT 1 FROM event_handlers
        WHERE event_type = 'bank_transaction'
          AND name = classification_rules.name
      )
    """)
  )

  # Tenant schemas
  for_each_tenant_schema(conn, _create_in_tenant)


def downgrade() -> None:
  conn = op.get_bind()
  for_each_tenant_schema(conn, _drop_in_tenant)

  op.drop_index("idx_handlers_unapproved", table_name="event_handlers")
  op.drop_index("idx_handlers_priority", table_name="event_handlers")
  op.drop_index("idx_handlers_event_type", table_name="event_handlers")
  op.drop_table("event_handlers")
