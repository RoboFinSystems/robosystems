"""Event-driven ledger schema (consolidated).

Squashes Phases 1-4b of the event-driven ledger build into a single
migration, plus the REA duality + ``event_class`` schema additions
proposed alongside the spec revision. The original four migrations
(events table, agents + events.agent_id FK, event_handlers,
entries.triggered_by_event_id audit column) and the duality follow-up
were never deployed beyond local development — collapsing them into one
schema diff makes this feature easier to reason about as a unit.

Tables created:
  - ``agents``: REA counterparty registry (customers, vendors, etc.)
  - ``events``: real-world business events with status lifecycle, FK to
    agents. Includes the duality chain (``obligated_by_event_id`` for
    forward-materialized scheduled events; ``discharges_event_id`` for
    settlement of obligations) and ``event_class`` (``'economic'`` vs
    ``'support'``) orthogonal to ``event_category``.
  - ``event_dimensions``: M:N junction between events and dimensions
  - ``event_handlers``: dynamic rule registry that drives event → GL writes

Columns added:
  - ``transactions.triggered_by_event_id``: audit chain from GL transaction
    to originating event (no FK — events table is per-tenant; the link is
    informational and validated at the application layer).
  - ``entries.triggered_by_event_id``: parallel audit column for entries
    that have no Transaction parent (e.g., closing entries). FK enforced
    against the events table (same schema, ON DELETE RESTRICT).

Tables dropped:
  - ``classification_rules``: superseded by ``event_handlers``. The
    original table was an artifact of the initial implementation and
    never produced rows in any environment that matters; the model and
    re-exports are removed in the same change.

Both the public schema (template for new tenants) and every existing
tenant schema receive the same DDL — the standard multi-tenant pattern
for this extensions DB.

Revision ID: 0005
Revises: 0004
Create Date: 2026-04-24
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None

_STATUS_CHECK = (
  "status IN ('captured', 'classified', 'committed', 'pending', "
  "'fulfilled', 'voided', 'superseded')"
)
_CATEGORY_CHECK = (
  "(event_class = 'economic' AND event_category IN ("
  "'sales', 'purchase', 'financing', 'payroll', "
  "'treasury', 'adjustment', 'recognition', 'other')) "
  "OR (event_class = 'support' AND event_category IN ("
  "'control', 'approval', 'reconciliation', 'inquiry'))"
)
_EVENT_CLASS_CHECK = "event_class IN ('economic', 'support')"
_RESOURCE_TYPE_CHECK = (
  "resource_type IN ('goods', 'services', 'money', 'right', "
  "'obligation', 'information', 'labor') OR resource_type IS NULL"
)
# Event source provenance — adapter-driven (quickbooks, xero, plaid) or
# native (manual, schedule, system). Adding a new source means widening
# this CHECK plus updating the adapter that emits the value. Must stay
# in sync with the SQLAlchemy model in
# ``models/extensions/roboledger/event.py``.
_SOURCE_CHECK = (
  "source IN ('manual', 'system', 'schedule', 'quickbooks', 'xero', 'plaid')"
)
_AGENT_TYPE_CHECK = (
  "agent_type IN ('customer', 'vendor', 'employee', 'owner', "
  "'supplier', 'government', 'lender', 'self', 'other')"
)
_HANDLER_ORIGIN_CHECK = "origin IN ('hub', 'tenant')"


# ── Tenant DDL ──────────────────────────────────────────────────────────


def _create_in_tenant(conn, schema: str) -> None:
  # 1. agents — must come before events because events.agent_id FKs to it.
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
        connection_id VARCHAR,
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
      f"ON {schema}.agents (source, connection_id, external_id) "
      f"WHERE external_id IS NOT NULL AND connection_id IS NOT NULL"
    )
  )

  # 2. events — agent_id FK declared inline.
  conn.execute(
    text(f"""
      CREATE TABLE IF NOT EXISTS {schema}.events (
        id VARCHAR PRIMARY KEY,
        event_type VARCHAR NOT NULL,
        event_category VARCHAR NOT NULL,
        event_class VARCHAR NOT NULL DEFAULT 'economic',
        agent_id VARCHAR REFERENCES {schema}.agents(id),
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
        obligated_by_event_id VARCHAR,
        discharges_event_id VARCHAR,
        amount BIGINT,
        currency VARCHAR NOT NULL DEFAULT 'USD',
        description VARCHAR,
        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        created_by VARCHAR NOT NULL,
        CONSTRAINT check_{schema}_event_status CHECK ({_STATUS_CHECK}),
        CONSTRAINT check_{schema}_event_category CHECK ({_CATEGORY_CHECK}),
        CONSTRAINT check_{schema}_event_class CHECK ({_EVENT_CLASS_CHECK}),
        CONSTRAINT check_{schema}_event_resource_type CHECK ({_RESOURCE_TYPE_CHECK}),
        CONSTRAINT check_{schema}_event_source CHECK ({_SOURCE_CHECK})
      )
    """)
  )
  for idx_suffix, col in [
    ("type", "event_type"),
    ("category", "event_category"),
    ("occurred_at", "occurred_at"),
    ("status", "status"),
    ("agent", "agent_id"),
    ("obligated_by", "obligated_by_event_id"),
    ("discharges", "discharges_event_id"),
  ]:
    conn.execute(
      text(
        f"CREATE INDEX IF NOT EXISTS idx_{schema}_events_{idx_suffix} "
        f"ON {schema}.events ({col})"
      )
    )
  # Partial unique index: dedup key for external source ingestion, mirrors
  # the agents table. Native-source events (external_id NULL) are exempt.
  conn.execute(
    text(
      f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{schema}_events_source_external "
      f"ON {schema}.events (source, external_id) "
      f"WHERE external_id IS NOT NULL"
    )
  )

  # 3. event_dimensions junction.
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

  # 4. transactions.triggered_by_event_id (audit column, no FK — informational).
  t = TenantOps(conn, schema)
  t.add_column("transactions", "triggered_by_event_id", "VARCHAR")
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_transactions_triggered_by_event "
      f"ON {schema}.transactions (triggered_by_event_id) "
      f"WHERE triggered_by_event_id IS NOT NULL"
    )
  )

  # 5. entries.triggered_by_event_id + FK to events.
  t.add_column("entries", "triggered_by_event_id", "VARCHAR")
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_entries_triggered_by_event "
      f"ON {schema}.entries (triggered_by_event_id) "
      f"WHERE triggered_by_event_id IS NOT NULL"
    )
  )
  conn.execute(
    text(
      f"ALTER TABLE {schema}.entries ADD CONSTRAINT "
      f"fk_{schema}_entries_triggered_by_event "
      f"FOREIGN KEY (triggered_by_event_id) REFERENCES {schema}.events(id) "
      f"ON DELETE RESTRICT"
    )
  )

  # 6. event_handlers.
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
        CONSTRAINT check_{schema}_handler_origin CHECK ({_HANDLER_ORIGIN_CHECK})
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

  # 7. Retire the legacy classification_rules table — superseded by
  # event_handlers (no consumers, no writers, never produced rows in
  # practice). Indexes are dropped implicitly with the table.
  conn.execute(text(f"DROP TABLE IF EXISTS {schema}.classification_rules"))


def _drop_in_tenant(conn, schema: str) -> None:
  # Reverse of _create_in_tenant.

  # Recreate classification_rules so the migration is reversible. Mirrors
  # the DDL from 0001_initial_schema's tenant provisioning.
  conn.execute(
    text(f"""
      CREATE TABLE IF NOT EXISTS {schema}.classification_rules (
        id VARCHAR PRIMARY KEY,
        name VARCHAR NOT NULL,
        description VARCHAR,
        match_source VARCHAR,
        match_type VARCHAR,
        match_merchant VARCHAR,
        match_category VARCHAR,
        match_min_amount BIGINT,
        match_max_amount BIGINT,
        target_element_id VARCHAR NOT NULL REFERENCES {schema}.elements(id),
        entry_type VARCHAR NOT NULL,
        debit_element_id VARCHAR NOT NULL REFERENCES {schema}.elements(id),
        credit_element_id VARCHAR NOT NULL REFERENCES {schema}.elements(id),
        dimensions JSONB NOT NULL DEFAULT '[]'::jsonb,
        suggested_by VARCHAR,
        confidence FLOAT,
        approved_by VARCHAR,
        approved_at TIMESTAMP,
        is_active BOOLEAN NOT NULL DEFAULT true,
        priority INTEGER NOT NULL DEFAULT 0,
        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        created_by VARCHAR NOT NULL
      )
    """)
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_rules_active "
      f"ON {schema}.classification_rules (is_active, priority) "
      f"WHERE is_active = true"
    )
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_rules_source "
      f"ON {schema}.classification_rules (match_source)"
    )
  )

  conn.execute(text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_handlers_unapproved"))
  conn.execute(text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_handlers_priority"))
  conn.execute(text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_handlers_event_type"))
  conn.execute(text(f"DROP TABLE IF EXISTS {schema}.event_handlers"))

  t = TenantOps(conn, schema)
  t.drop_constraint("entries", f"fk_{schema}_entries_triggered_by_event")
  conn.execute(
    text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_entries_triggered_by_event")
  )
  t.drop_column("entries", "triggered_by_event_id")

  conn.execute(
    text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_transactions_triggered_by_event")
  )
  t.drop_column("transactions", "triggered_by_event_id")

  conn.execute(text(f"DROP TABLE IF EXISTS {schema}.event_dimensions"))
  for idx_suffix in (
    "source_external",
    "discharges",
    "obligated_by",
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

  conn.execute(
    text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_agents_source_external")
  )
  conn.execute(text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_agents_type"))
  conn.execute(text(f"DROP TABLE IF EXISTS {schema}.agents"))


# ── Public-schema DDL via Alembic op.* ──────────────────────────────────


def upgrade() -> None:
  # 1. agents
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
    sa.Column("address", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column("source", sa.String(), nullable=False, server_default="native"),
    sa.Column("external_id", sa.String(), nullable=True),
    sa.Column("connection_id", sa.String(), nullable=True),
    sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
    sa.Column(
      "is_1099_recipient", sa.Boolean(), nullable=False, server_default="false"
    ),
    sa.Column(
      "metadata",
      postgresql.JSONB(astext_type=sa.Text()),
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
    ["source", "connection_id", "external_id"],
    unique=True,
    postgresql_where="external_id IS NOT NULL AND connection_id IS NOT NULL",
  )

  # 2. events (agent_id FK declared inline)
  op.create_table(
    "events",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("event_type", sa.String(), nullable=False),
    sa.Column("event_category", sa.String(), nullable=False),
    sa.Column("event_class", sa.String(), nullable=False, server_default="economic"),
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
    sa.Column("obligated_by_event_id", sa.String(), nullable=True),
    sa.Column("discharges_event_id", sa.String(), nullable=True),
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
    sa.CheckConstraint(_EVENT_CLASS_CHECK, name="check_event_class"),
    sa.CheckConstraint(_RESOURCE_TYPE_CHECK, name="check_event_resource_type"),
    sa.CheckConstraint(_SOURCE_CHECK, name="check_event_source"),
    sa.ForeignKeyConstraint(["agent_id"], ["agents.id"], name="fk_events_agent_id"),
    sa.PrimaryKeyConstraint("id"),
  )
  op.create_index("idx_events_type", "events", ["event_type"])
  op.create_index("idx_events_category", "events", ["event_category"])
  op.create_index("idx_events_occurred_at", "events", ["occurred_at"])
  op.create_index("idx_events_status", "events", ["status"])
  op.create_index("idx_events_agent", "events", ["agent_id"])
  op.create_index("idx_events_obligated_by", "events", ["obligated_by_event_id"])
  op.create_index("idx_events_discharges", "events", ["discharges_event_id"])
  op.create_index(
    "idx_events_source_external",
    "events",
    ["source", "external_id"],
    unique=True,
    postgresql_where="external_id IS NOT NULL",
  )

  # 3. event_dimensions junction
  op.create_table(
    "event_dimensions",
    sa.Column("event_id", sa.String(), nullable=False),
    sa.Column("dimension_id", sa.String(), nullable=False),
    sa.ForeignKeyConstraint(["event_id"], ["events.id"], ondelete="CASCADE"),
    sa.ForeignKeyConstraint(["dimension_id"], ["dimensions.id"], ondelete="RESTRICT"),
    sa.PrimaryKeyConstraint("event_id", "dimension_id"),
  )

  # 4. transactions.triggered_by_event_id
  op.add_column(
    "transactions", sa.Column("triggered_by_event_id", sa.String(), nullable=True)
  )
  op.create_index(
    "idx_transactions_triggered_by_event",
    "transactions",
    ["triggered_by_event_id"],
    postgresql_where="triggered_by_event_id IS NOT NULL",
  )

  # 5. entries.triggered_by_event_id + FK
  op.add_column(
    "entries", sa.Column("triggered_by_event_id", sa.String(), nullable=True)
  )
  op.create_index(
    "idx_entries_triggered_by_event",
    "entries",
    ["triggered_by_event_id"],
    postgresql_where="triggered_by_event_id IS NOT NULL",
  )
  op.create_foreign_key(
    "fk_entries_triggered_by_event",
    "entries",
    "events",
    ["triggered_by_event_id"],
    ["id"],
    ondelete="RESTRICT",
  )

  # 6. event_handlers
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
      postgresql.JSONB(astext_type=sa.Text()),
      nullable=True,
    ),
    sa.Column(
      "transaction_template",
      postgresql.JSONB(astext_type=sa.Text()),
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
      postgresql.JSONB(astext_type=sa.Text()),
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
    sa.CheckConstraint(_HANDLER_ORIGIN_CHECK, name="check_handler_origin"),
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

  # 7. Retire the legacy classification_rules table (public schema).
  # The model is gone, no code writes here, and event_handlers is the
  # supported surface. Drop indexes first because they were created
  # explicitly in 0001_initial_schema and persist alongside the table.
  op.drop_index("idx_rules_active", table_name="classification_rules")
  op.drop_index("idx_rules_source", table_name="classification_rules")
  op.drop_table("classification_rules")

  # Tenant schemas — apply the same DDL to every existing tenant.
  conn = op.get_bind()
  for_each_tenant_schema(conn, _create_in_tenant)


def downgrade() -> None:
  conn = op.get_bind()
  for_each_tenant_schema(conn, _drop_in_tenant)

  # Recreate classification_rules in the public schema so the migration
  # is reversible. Mirrors 0001_initial_schema.
  op.create_table(
    "classification_rules",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("name", sa.String(), nullable=False),
    sa.Column("description", sa.String(), nullable=True),
    sa.Column("match_source", sa.String(), nullable=True),
    sa.Column("match_type", sa.String(), nullable=True),
    sa.Column("match_merchant", sa.String(), nullable=True),
    sa.Column("match_category", sa.String(), nullable=True),
    sa.Column("match_min_amount", sa.BigInteger(), nullable=True),
    sa.Column("match_max_amount", sa.BigInteger(), nullable=True),
    sa.Column("target_element_id", sa.String(), nullable=False),
    sa.Column("entry_type", sa.String(), nullable=False),
    sa.Column("debit_element_id", sa.String(), nullable=False),
    sa.Column("credit_element_id", sa.String(), nullable=False),
    sa.Column("dimensions", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column("suggested_by", sa.String(), nullable=True),
    sa.Column("confidence", sa.Float(), nullable=True),
    sa.Column("approved_by", sa.String(), nullable=True),
    sa.Column("approved_at", sa.DateTime(), nullable=True),
    sa.Column("is_active", sa.Boolean(), nullable=False),
    sa.Column("priority", sa.Integer(), nullable=False),
    sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("updated_at", sa.DateTime(), nullable=False),
    sa.Column("created_by", sa.String(), nullable=False),
    sa.ForeignKeyConstraint(["target_element_id"], ["elements.id"]),
    sa.ForeignKeyConstraint(["debit_element_id"], ["elements.id"]),
    sa.ForeignKeyConstraint(["credit_element_id"], ["elements.id"]),
    sa.PrimaryKeyConstraint("id"),
  )
  op.create_index(
    "idx_rules_active",
    "classification_rules",
    ["is_active", "priority"],
    postgresql_where=sa.text("is_active = true"),
  )
  op.create_index("idx_rules_source", "classification_rules", ["match_source"])

  # event_handlers
  op.drop_index("idx_handlers_unapproved", table_name="event_handlers")
  op.drop_index("idx_handlers_priority", table_name="event_handlers")
  op.drop_index("idx_handlers_event_type", table_name="event_handlers")
  op.drop_table("event_handlers")

  # entries.triggered_by_event_id + FK
  op.drop_constraint("fk_entries_triggered_by_event", "entries", type_="foreignkey")
  op.drop_index("idx_entries_triggered_by_event", table_name="entries")
  op.drop_column("entries", "triggered_by_event_id")

  # transactions.triggered_by_event_id
  op.drop_index(
    "idx_transactions_triggered_by_event",
    table_name="transactions",
    postgresql_where="triggered_by_event_id IS NOT NULL",
  )
  op.drop_column("transactions", "triggered_by_event_id")

  # event_dimensions + events
  op.drop_table("event_dimensions")
  op.drop_index("idx_events_source_external", table_name="events")
  op.drop_index("idx_events_discharges", table_name="events")
  op.drop_index("idx_events_obligated_by", table_name="events")
  op.drop_index("idx_events_agent", table_name="events")
  op.drop_index("idx_events_status", table_name="events")
  op.drop_index("idx_events_occurred_at", table_name="events")
  op.drop_index("idx_events_category", table_name="events")
  op.drop_index("idx_events_type", table_name="events")
  op.drop_table("events")

  # agents
  op.drop_index("idx_agents_source_external", table_name="agents")
  op.drop_index("idx_agents_type", table_name="agents")
  op.drop_table("agents")
