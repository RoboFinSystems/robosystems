"""initial extensions schema

Squashed migration representing the complete extensions database schema.
Includes RoboLedger (ledger + schedules + fiscal calendar + close workflow),
RoboInvestor (portfolios), and the shared XBRL taxonomy system.

This consolidates what was previously spread across three migrations:
  - 0001 initial schema
  - 0002 add entry provenance column
  - 0003 add fiscal_calendar tables + facts.fact_scope column

All three are now a single initial state since none had shipped to production.

Public-schema shared tables (US GAAP / SFAC 6 seeds, shared across tenants):
  - taxonomies, structures, associations

Tenant-template tables (created here in public, copied into each kg* schema
on first tenant access by provision_tenant_schema() via schema_translate_map):
  - fiscal_periods, fiscal_calendar, fiscal_calendar_events
  - elements, transactions, entries, line_items, dimensions,
    junction tables, classification_rules, reports,
    facts (with fact_scope column), report_shares, entities,
    portfolios, securities, positions, publish_lists, publish_list_members

Future migrations should use the helpers module for tenant DDL::

    from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

    def upgrade():
        op.add_column("elements", sa.Column("new_col", sa.String()))
        conn = op.get_bind()
        def apply(conn, schema):
            t = TenantOps(conn, schema)
            t.add_column("elements", "new_col", "TEXT")
        for_each_tenant_schema(conn, apply)

Revision ID: 0001
Revises:
Create Date: 2026-04-12
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
  # ──────────────────────────────────────────────────────────────────────
  # Utility function
  # ──────────────────────────────────────────────────────────────────────
  op.execute("""
    CREATE OR REPLACE FUNCTION public.generate_prefixed_id(prefix TEXT)
    RETURNS TEXT AS $$
    BEGIN
      RETURN prefix || '_' || replace(gen_random_uuid()::text, '-', '');
    END;
    $$ LANGUAGE plpgsql;
  """)

  # ──────────────────────────────────────────────────────────────────────
  # Tenant-template tables (no schema arg → created in public as template,
  # provision_tenant_schema() copies them into each kg* tenant schema on
  # first access). fiscal_periods is per-graph data — it belongs in the
  # tenant schema, not shared public.
  # ──────────────────────────────────────────────────────────────────────

  op.create_table(
    "fiscal_periods",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("graph_id", sa.String(), nullable=False),
    sa.Column("name", sa.String(), nullable=False),
    sa.Column("start_date", sa.Date(), nullable=False),
    sa.Column("end_date", sa.Date(), nullable=False),
    sa.Column("period_type", sa.String(), nullable=False),
    sa.Column("status", sa.String(), nullable=False),
    sa.Column("closed_at", sa.DateTime(), nullable=True),
    sa.Column("closed_by", sa.String(), nullable=True),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("updated_at", sa.DateTime(), nullable=False),
    sa.CheckConstraint(
      "status IN ('open', 'closing', 'closed')",
      name="check_fiscal_period_status",
    ),
    sa.CheckConstraint("start_date < end_date", name="check_fiscal_period_dates"),
    sa.PrimaryKeyConstraint("id"),
    sa.UniqueConstraint("graph_id", "name", name="uq_fiscal_period_graph_name"),
  )
  op.create_index(
    "idx_fiscal_periods_graph",
    "fiscal_periods",
    ["graph_id"],
  )
  op.create_index(
    "idx_fiscal_periods_dates",
    "fiscal_periods",
    ["graph_id", "start_date", "end_date"],
  )

  # -- Fiscal Calendar (tenant-scoped; see specs/ledger-close-workflow.md) --
  # Per-graph rolling close state: closed_through_period (system-maintained)
  # and close_target_period (user-settable). One row per tenant.
  op.create_table(
    "fiscal_calendar",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("graph_id", sa.String(), nullable=False),
    sa.Column(
      "fiscal_year_start_month", sa.Integer(), nullable=False, server_default="1"
    ),
    sa.Column("closed_through_period", sa.String(), nullable=True),
    sa.Column("close_target_period", sa.String(), nullable=True),
    sa.Column("initialized_at", sa.DateTime(), nullable=True),
    sa.Column("last_close_at", sa.DateTime(), nullable=True),
    sa.Column(
      "created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.Column(
      "updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.Column("created_by", sa.String(), nullable=True),
    sa.Column("updated_by", sa.String(), nullable=True),
    sa.CheckConstraint(
      "fiscal_year_start_month BETWEEN 1 AND 12",
      name="ck_fiscal_calendar_year_start_month",
    ),
    sa.CheckConstraint(
      "close_target_period IS NULL "
      "OR closed_through_period IS NULL "
      "OR close_target_period >= closed_through_period",
      name="ck_fiscal_calendar_target_ge_through",
    ),
    sa.PrimaryKeyConstraint("id"),
    sa.UniqueConstraint("graph_id", name="uq_fiscal_calendar_graph"),
  )
  op.create_index("idx_fiscal_calendar_graph", "fiscal_calendar", ["graph_id"])

  # -- Fiscal Calendar Events (append-only audit log of calendar mutations) --
  op.create_table(
    "fiscal_calendar_events",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("fiscal_calendar_id", sa.String(), nullable=False),
    sa.Column("graph_id", sa.String(), nullable=False),
    sa.Column("event_type", sa.String(), nullable=False),
    sa.Column("period", sa.String(), nullable=True),
    sa.Column("from_value", sa.String(), nullable=True),
    sa.Column("to_value", sa.String(), nullable=True),
    sa.Column("actor_id", sa.String(), nullable=True),
    sa.Column("actor_type", sa.String(), nullable=False, server_default="user"),
    sa.Column("note", sa.Text(), nullable=True),
    sa.Column("reason", sa.Text(), nullable=True),
    sa.Column(
      "created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.CheckConstraint(
      "event_type IN ("
      "'initialized', 'target_changed', 'period_closed', 'period_reopened', "
      "'target_advanced_auto'"
      ")",
      name="ck_fiscal_calendar_events_event_type",
    ),
    sa.CheckConstraint(
      "actor_type IN ('user', 'agent', 'system')",
      name="ck_fiscal_calendar_events_actor_type",
    ),
    sa.ForeignKeyConstraint(
      ["fiscal_calendar_id"],
      ["fiscal_calendar.id"],
      name="fk_fiscal_calendar_events_calendar",
      ondelete="CASCADE",
    ),
    sa.PrimaryKeyConstraint("id"),
  )
  op.create_index(
    "idx_fiscal_calendar_events_graph_time",
    "fiscal_calendar_events",
    ["graph_id", "created_at"],
  )

  # ──────────────────────────────────────────────────────────────────────
  # Public-schema shared tables
  # ──────────────────────────────────────────────────────────────────────

  op.create_table(
    "taxonomies",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("name", sa.String(), nullable=False),
    sa.Column("description", sa.String(), nullable=True),
    sa.Column("taxonomy_type", sa.String(), nullable=False),
    sa.Column("version", sa.String(), nullable=True),
    sa.Column("standard", sa.String(), nullable=True),
    sa.Column("namespace_uri", sa.String(), nullable=True),
    sa.Column("is_shared", sa.Boolean(), nullable=False, server_default="false"),
    sa.Column("source_taxonomy_id", sa.String(), nullable=True),
    sa.Column("target_taxonomy_id", sa.String(), nullable=True),
    # Extension chain (TAXONOMY_EXTENDS_TAXONOMY in graph) — version upgrades,
    # entity extensions, industry overlays. Distinct from mapping relationships.
    sa.Column("parent_taxonomy_id", sa.String(), nullable=True),
    sa.Column("extension_type", sa.String(), nullable=True),
    sa.Column("effective_date", sa.Date(), nullable=True),
    sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
    sa.Column("is_locked", sa.Boolean(), nullable=False, server_default="false"),
    sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default="{}"),
    sa.Column(
      "created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.Column(
      "updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.Column("created_by", sa.String(), nullable=False, server_default="system"),
    sa.PrimaryKeyConstraint("id"),
    sa.ForeignKeyConstraint(["source_taxonomy_id"], ["taxonomies.id"], use_alter=True),
    sa.ForeignKeyConstraint(["target_taxonomy_id"], ["taxonomies.id"], use_alter=True),
    sa.ForeignKeyConstraint(["parent_taxonomy_id"], ["taxonomies.id"], use_alter=True),
    sa.CheckConstraint(
      "taxonomy_type IN ('chart_of_accounts', 'reporting', 'mapping', 'schedule')",
      name="check_taxonomy_type",
    ),
    sa.CheckConstraint(
      "extension_type IS NULL OR extension_type IN "
      "('version', 'entity_extension', 'industry', 'jurisdiction')",
      name="check_taxonomy_extension_type",
    ),
    schema="public",
  )
  op.create_index(
    "idx_taxonomies_type",
    "taxonomies",
    ["taxonomy_type"],
    schema="public",
  )
  op.create_index(
    "idx_taxonomies_standard",
    "taxonomies",
    ["standard"],
    schema="public",
    postgresql_where=sa.text("standard IS NOT NULL"),
  )
  op.create_index(
    "idx_taxonomies_parent",
    "taxonomies",
    ["parent_taxonomy_id"],
    schema="public",
    postgresql_where=sa.text("parent_taxonomy_id IS NOT NULL"),
  )

  op.create_table(
    "structures",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("name", sa.String(), nullable=False),
    sa.Column("description", sa.String(), nullable=True),
    sa.Column("structure_type", sa.String(), nullable=False),
    sa.Column("taxonomy_id", sa.String(), nullable=False),
    sa.Column("graph_structure_id", sa.String(), nullable=True),
    sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
    sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default="{}"),
    sa.Column(
      "created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.Column(
      "updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.Column("created_by", sa.String(), nullable=False, server_default="system"),
    sa.PrimaryKeyConstraint("id"),
    sa.ForeignKeyConstraint(["taxonomy_id"], ["taxonomies.id"]),
    sa.CheckConstraint(
      "structure_type IN ("
      "'chart_of_accounts', 'income_statement', 'balance_sheet', "
      "'cash_flow_statement', 'equity_statement', 'coa_mapping', 'custom', 'schedule'"
      ")",
      name="check_structure_type",
    ),
    schema="public",
  )
  op.create_index(
    "idx_structures_taxonomy",
    "structures",
    ["taxonomy_id"],
    schema="public",
  )
  op.create_index(
    "idx_structures_type",
    "structures",
    ["structure_type"],
    schema="public",
  )

  # ──────────────────────────────────────────────────────────────────────
  # Tenant-template tables (no schema= → public for Alembic tracking,
  # then created per-tenant by provision_tenant_schema)
  # ──────────────────────────────────────────────────────────────────────

  # -- Elements (Chart of Accounts + taxonomy concepts) --
  op.create_table(
    "elements",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("code", sa.String(), nullable=True),
    sa.Column("name", sa.String(), nullable=False),
    sa.Column("description", sa.String(), nullable=True),
    sa.Column("qname", sa.String(), nullable=True),
    sa.Column("namespace", sa.String(), nullable=True),
    sa.Column("uri", sa.String(), nullable=True),
    sa.Column("classification", sa.String(), nullable=False),
    sa.Column("sub_classification", sa.String(), nullable=True),
    sa.Column("balance_type", sa.String(), nullable=False),
    sa.Column("period_type", sa.String(), nullable=False),
    sa.Column("is_abstract", sa.Boolean(), nullable=False),
    sa.Column("is_monetary", sa.Boolean(), nullable=False),
    sa.Column("element_type", sa.String(), nullable=False),
    sa.Column("parent_id", sa.String(), nullable=True),
    sa.Column("depth", sa.Integer(), nullable=False),
    sa.Column("path", sa.String(), nullable=False),
    sa.Column("taxonomy_id", sa.String(), nullable=True),
    sa.Column("source", sa.String(), nullable=False),
    sa.Column("currency", sa.String(), nullable=False),
    sa.Column("is_active", sa.Boolean(), nullable=False),
    sa.Column("is_placeholder", sa.Boolean(), nullable=False),
    sa.Column("external_id", sa.String(), nullable=True),
    sa.Column("external_source", sa.String(), nullable=True),
    # Logical reference to platform `Connection.id`. Cross-database, not
    # an FK. Scopes deletes during re-sync so multi-connection graphs
    # (e.g. parent + sub QB books) don't stomp each other's CoA.
    # Library-origin elements have connection_id NULL.
    sa.Column("connection_id", sa.String(), nullable=True),
    sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column("version", sa.Integer(), nullable=False),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("updated_at", sa.DateTime(), nullable=False),
    sa.Column("created_by", sa.String(), nullable=False),
    sa.CheckConstraint(
      "classification IN ('asset', 'liability', 'equity', 'revenue', 'expense')",
      name="check_element_classification",
    ),
    sa.CheckConstraint(
      "balance_type IN ('debit', 'credit')",
      name="check_element_balance_type",
    ),
    sa.CheckConstraint(
      "period_type IN ('duration', 'instant')",
      name="check_element_period_type",
    ),
    sa.CheckConstraint(
      "element_type IN ('concept', 'abstract', 'axis', 'member', 'hypercube')",
      name="check_element_type",
    ),
    sa.CheckConstraint(
      "source IN ('sfac6', 'us-gaap', 'ifrs', 'quickbooks', 'xero', "
      "'plaid', 'native', 'import', 'system')",
      name="check_element_source",
    ),
    sa.ForeignKeyConstraint(["parent_id"], ["elements.id"]),
    sa.ForeignKeyConstraint(["taxonomy_id"], ["taxonomies.id"]),
    sa.PrimaryKeyConstraint("id"),
  )
  op.create_index("idx_elements_classification", "elements", ["classification"])
  op.create_index("idx_elements_parent", "elements", ["parent_id"])
  op.create_index(
    "idx_elements_external", "elements", ["external_id", "external_source"]
  )
  # Used by OLTPLoader to scope deletes per (source, connection_id) tuple
  # during a re-sync — without this index the delete is a seq scan over
  # the elements table.
  op.create_index(
    "idx_elements_external_source_connection",
    "elements",
    ["external_source", "connection_id"],
    postgresql_where=sa.text("connection_id IS NOT NULL"),
  )
  op.create_index(
    "idx_elements_active",
    "elements",
    ["is_active"],
    postgresql_where=sa.text("is_active = true"),
  )
  op.create_index("idx_elements_taxonomy", "elements", ["taxonomy_id"])
  op.create_index("idx_elements_source", "elements", ["source"])
  op.create_index(
    "idx_elements_qname",
    "elements",
    ["qname"],
    unique=True,
    postgresql_where=sa.text("qname IS NOT NULL"),
  )
  op.create_index(
    "idx_elements_namespace",
    "elements",
    ["namespace"],
    postgresql_where=sa.text("namespace IS NOT NULL"),
  )

  # -- Element Associations (public schema — shared seed data) --
  op.create_table(
    "associations",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("structure_id", sa.String(), nullable=False),
    sa.Column("from_element_id", sa.String(), nullable=False),
    sa.Column("to_element_id", sa.String(), nullable=False),
    sa.Column(
      "association_type", sa.String(), nullable=False, server_default="presentation"
    ),
    sa.Column("arcrole", sa.String(), nullable=True),
    sa.Column("order_value", sa.Float(), nullable=True, server_default="0"),
    sa.Column("weight", sa.Float(), nullable=True),
    sa.Column("confidence", sa.Float(), nullable=True),
    sa.Column("suggested_by", sa.String(), nullable=True),
    sa.Column("approved_by", sa.String(), nullable=True),
    sa.Column("approved_at", sa.DateTime(), nullable=True),
    sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default="{}"),
    sa.Column(
      "created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.Column(
      "updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.Column("created_by", sa.String(), nullable=False, server_default="system"),
    sa.PrimaryKeyConstraint("id"),
    sa.ForeignKeyConstraint(["structure_id"], ["structures.id"]),
    sa.ForeignKeyConstraint(["from_element_id"], ["elements.id"]),
    sa.ForeignKeyConstraint(["to_element_id"], ["elements.id"]),
    sa.UniqueConstraint(
      "structure_id",
      "from_element_id",
      "to_element_id",
      "association_type",
      name="uq_association_structure_elements_type",
    ),
    sa.CheckConstraint(
      "association_type IN ('presentation', 'calculation', 'mapping')",
      name="check_association_type",
    ),
    schema="public",
  )
  op.create_index(
    "idx_associations_structure",
    "associations",
    ["structure_id"],
    schema="public",
  )
  op.create_index(
    "idx_associations_from",
    "associations",
    ["from_element_id"],
    schema="public",
  )
  op.create_index(
    "idx_associations_to",
    "associations",
    ["to_element_id"],
    schema="public",
  )
  op.create_index(
    "idx_associations_type",
    "associations",
    ["association_type"],
    schema="public",
  )
  op.create_index(
    "idx_associations_unapproved",
    "associations",
    ["approved_by"],
    schema="public",
    postgresql_where=sa.text("approved_by IS NULL AND suggested_by = 'ai'"),
  )

  # -- Dimensions --
  op.create_table(
    "dimensions",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("dimension_type", sa.String(), nullable=False),
    sa.Column("name", sa.String(), nullable=False),
    sa.Column("value", sa.String(), nullable=False),
    sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column("is_active", sa.Boolean(), nullable=False),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("updated_at", sa.DateTime(), nullable=False),
    sa.PrimaryKeyConstraint("id"),
    sa.UniqueConstraint("dimension_type", "value", name="uq_dimension_type_value"),
  )
  op.create_index("idx_dimensions_type", "dimensions", ["dimension_type"])

  # -- Transactions --
  op.create_table(
    "transactions",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("number", sa.String(), nullable=True),
    sa.Column("idempotency_key", sa.String(), nullable=True),
    sa.Column("type", sa.String(), nullable=False),
    sa.Column("category", sa.String(), nullable=True),
    sa.Column("amount", sa.BigInteger(), nullable=False),
    sa.Column("currency", sa.String(), nullable=False),
    sa.Column("date", sa.Date(), nullable=False),
    sa.Column("due_date", sa.Date(), nullable=True),
    sa.Column("merchant_name", sa.String(), nullable=True),
    sa.Column("reference_number", sa.String(), nullable=True),
    sa.Column("description", sa.String(), nullable=True),
    sa.Column("source", sa.String(), nullable=False),
    sa.Column("source_id", sa.String(), nullable=True),
    sa.Column("connection_id", sa.String(), nullable=True),
    sa.Column("status", sa.String(), nullable=False),
    sa.Column("posted_at", sa.DateTime(), nullable=True),
    sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column("version", sa.Integer(), nullable=False),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("updated_at", sa.DateTime(), nullable=False),
    sa.Column("created_by", sa.String(), nullable=False),
    sa.CheckConstraint(
      "status IN ('pending', 'posted', 'void')",
      name="check_transaction_status",
    ),
    sa.CheckConstraint("amount >= 0", name="check_transaction_amount"),
    sa.PrimaryKeyConstraint("id"),
    sa.UniqueConstraint("idempotency_key"),
  )
  op.create_index("idx_transactions_date", "transactions", ["date"])
  op.create_index("idx_transactions_type", "transactions", ["type"])
  op.create_index("idx_transactions_status", "transactions", ["status"])
  op.create_index("idx_transactions_source", "transactions", ["source"])
  op.create_index("idx_transactions_merchant", "transactions", ["merchant_name"])
  op.create_index("idx_transactions_created", "transactions", ["created_at"])

  # -- Entries --
  op.create_table(
    "entries",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("number", sa.String(), nullable=True),
    sa.Column("idempotency_key", sa.String(), nullable=True),
    sa.Column("transaction_id", sa.String(), nullable=True),
    sa.Column("type", sa.String(), nullable=False),
    sa.Column("reversal_of", sa.String(), nullable=True),
    sa.Column("source_structure_id", sa.String(), nullable=True),
    sa.Column("provenance", sa.String(), nullable=True),
    sa.Column("posting_date", sa.Date(), nullable=False),
    sa.Column("memo", sa.String(), nullable=True),
    sa.Column("status", sa.String(), nullable=False),
    sa.Column("posted_at", sa.DateTime(), nullable=True),
    sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column("version", sa.Integer(), nullable=False),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("updated_at", sa.DateTime(), nullable=False),
    sa.Column("created_by", sa.String(), nullable=False),
    sa.CheckConstraint(
      "status IN ('draft', 'posted', 'reversed')",
      name="check_entry_status",
    ),
    sa.CheckConstraint(
      "type IN ('standard', 'adjusting', 'closing', 'reversing')",
      name="check_entry_type",
    ),
    sa.CheckConstraint(
      "provenance IN ("
      "'source_sync', 'ai_generated', 'manual_entry', "
      "'schedule_derived', 'system_computed'"
      ") OR provenance IS NULL",
      name="ck_entries_provenance",
    ),
    sa.ForeignKeyConstraint(["reversal_of"], ["entries.id"]),
    sa.ForeignKeyConstraint(["transaction_id"], ["transactions.id"]),
    sa.PrimaryKeyConstraint("id"),
    sa.UniqueConstraint("idempotency_key"),
  )
  op.create_index("idx_entries_transaction", "entries", ["transaction_id"])
  op.create_index("idx_entries_posting_date", "entries", ["posting_date"])
  op.create_index("idx_entries_status", "entries", ["status"])
  op.create_index("idx_entries_type", "entries", ["type"])
  op.create_index(
    "idx_entries_source_structure",
    "entries",
    ["source_structure_id"],
    postgresql_where=sa.text("source_structure_id IS NOT NULL"),
  )

  # -- Line Items --
  op.create_table(
    "line_items",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("entry_id", sa.String(), nullable=False),
    sa.Column("element_id", sa.String(), nullable=False),
    sa.Column("debit_amount", sa.BigInteger(), nullable=False),
    sa.Column("credit_amount", sa.BigInteger(), nullable=False),
    sa.Column("description", sa.String(), nullable=True),
    sa.Column("line_order", sa.Integer(), nullable=False),
    sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("updated_at", sa.DateTime(), nullable=False),
    sa.CheckConstraint("debit_amount >= 0", name="check_debit_positive"),
    sa.CheckConstraint("credit_amount >= 0", name="check_credit_positive"),
    sa.CheckConstraint(
      "debit_amount > 0 OR credit_amount > 0",
      name="check_at_least_one_amount",
    ),
    sa.CheckConstraint(
      "NOT (debit_amount > 0 AND credit_amount > 0)",
      name="check_not_both_amounts",
    ),
    sa.ForeignKeyConstraint(["element_id"], ["elements.id"]),
    sa.ForeignKeyConstraint(["entry_id"], ["entries.id"], ondelete="CASCADE"),
    sa.PrimaryKeyConstraint("id"),
  )
  op.create_index("idx_line_items_entry", "line_items", ["entry_id"])
  op.create_index("idx_line_items_element", "line_items", ["element_id"])

  # -- Junction tables --
  # Parent-side FKs use CASCADE (tags are discarded with their owning ledger
  # row); dimension-side FKs use RESTRICT (deleting a dimension still in use
  # by any ledger row should be an explicit operator decision).
  op.create_table(
    "transaction_dimensions",
    sa.Column("transaction_id", sa.String(), nullable=False),
    sa.Column("dimension_id", sa.String(), nullable=False),
    sa.ForeignKeyConstraint(["dimension_id"], ["dimensions.id"], ondelete="RESTRICT"),
    sa.ForeignKeyConstraint(
      ["transaction_id"],
      ["transactions.id"],
      ondelete="CASCADE",
    ),
    sa.PrimaryKeyConstraint("transaction_id", "dimension_id"),
  )
  op.create_table(
    "entry_dimensions",
    sa.Column("entry_id", sa.String(), nullable=False),
    sa.Column("dimension_id", sa.String(), nullable=False),
    sa.ForeignKeyConstraint(["dimension_id"], ["dimensions.id"], ondelete="RESTRICT"),
    sa.ForeignKeyConstraint(["entry_id"], ["entries.id"], ondelete="CASCADE"),
    sa.PrimaryKeyConstraint("entry_id", "dimension_id"),
  )
  op.create_table(
    "line_item_dimensions",
    sa.Column("line_item_id", sa.String(), nullable=False),
    sa.Column("dimension_id", sa.String(), nullable=False),
    sa.ForeignKeyConstraint(["dimension_id"], ["dimensions.id"], ondelete="RESTRICT"),
    sa.ForeignKeyConstraint(
      ["line_item_id"],
      ["line_items.id"],
      ondelete="CASCADE",
    ),
    sa.PrimaryKeyConstraint("line_item_id", "dimension_id"),
  )

  # -- Classification Rules --
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

  # -- Reports --
  op.create_table(
    "reports",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("name", sa.String(), nullable=False),
    sa.Column("description", sa.String(), nullable=True),
    sa.Column("taxonomy_id", sa.String(), nullable=False),
    sa.Column("mapping_id", sa.String(), nullable=True),
    sa.Column("period_type", sa.String(), nullable=False),
    sa.Column("period_start", sa.Date(), nullable=True),
    sa.Column("period_end", sa.Date(), nullable=True),
    sa.Column("comparative", sa.Boolean(), nullable=False),
    sa.Column("periods", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column("graph_report_id", sa.String(), nullable=True),
    sa.Column("last_generated", sa.DateTime(), nullable=True),
    sa.Column("generation_status", sa.String(), nullable=False),
    sa.Column("ai_generated", sa.Boolean(), nullable=False),
    sa.Column("ai_intent", sa.String(), nullable=True),
    sa.Column("ai_workspace_id", sa.String(), nullable=True),
    sa.Column("ai_confidence", sa.Float(), nullable=True),
    sa.Column("source_graph_id", sa.String(), nullable=True),
    sa.Column("source_report_id", sa.String(), nullable=True),
    sa.Column("shared_at", sa.DateTime(), nullable=True),
    sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("updated_at", sa.DateTime(), nullable=False),
    sa.Column("created_by", sa.String(), nullable=False),
    sa.PrimaryKeyConstraint("id"),
  )
  op.create_index("idx_reports_taxonomy", "reports", ["taxonomy_id"])
  op.create_index("idx_reports_status", "reports", ["generation_status"])

  # -- Facts (generalized from report_facts — serves reports + schedules) --
  op.create_table(
    "facts",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("report_id", sa.String(), nullable=True),
    sa.Column("element_id", sa.String(), nullable=False),
    sa.Column("value", sa.Float(), nullable=False),
    sa.Column("period_start", sa.Date(), nullable=True),
    sa.Column("period_end", sa.Date(), nullable=False),
    sa.Column("period_type", sa.String(), nullable=False),
    sa.Column("unit", sa.String(), nullable=False),
    sa.Column("entity_id", sa.String(), nullable=False),
    sa.Column("structure_id", sa.String(), nullable=True),
    sa.Column("fact_set_id", sa.String(), nullable=True),
    sa.Column("fact_scope", sa.String(), nullable=False, server_default="in_scope"),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.CheckConstraint(
      "report_id IS NOT NULL OR fact_set_id IS NOT NULL",
      name="check_fact_has_parent",
    ),
    sa.CheckConstraint(
      "fact_scope IN ('historical', 'in_scope')",
      name="ck_facts_scope",
    ),
    sa.PrimaryKeyConstraint("id"),
  )
  op.create_index("idx_facts_report", "facts", ["report_id"])
  op.create_index("idx_facts_element", "facts", ["element_id"])
  op.create_index("idx_facts_period", "facts", ["period_start", "period_end"])
  op.create_index("idx_facts_fact_set", "facts", ["fact_set_id"])
  op.create_index("idx_facts_structure", "facts", ["structure_id"])
  op.create_index(
    "idx_facts_scope_in_scope",
    "facts",
    ["structure_id", "period_start"],
    postgresql_where=sa.text("fact_scope = 'in_scope'"),
  )

  # -- Report Shares --
  op.create_table(
    "report_shares",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("report_id", sa.String(), nullable=False),
    sa.Column("target_graph_id", sa.String(), nullable=False),
    sa.Column("shared_by", sa.String(), nullable=False),
    sa.Column("shared_at", sa.DateTime(), nullable=False),
    sa.Column("revoked_at", sa.DateTime(), nullable=True),
    sa.Column("fact_count", sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint("id"),
  )
  op.create_index("idx_report_shares_report", "report_shares", ["report_id"])
  op.create_index("idx_report_shares_target", "report_shares", ["target_graph_id"])

  # -- Entities --
  op.create_table(
    "entities",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("name", sa.String(), nullable=False),
    sa.Column("legal_name", sa.String(), nullable=True),
    sa.Column("uri", sa.String(), nullable=True),
    sa.Column("cik", sa.String(), nullable=True),
    sa.Column("ticker", sa.String(), nullable=True),
    sa.Column("exchange", sa.String(), nullable=True),
    sa.Column("sic", sa.String(), nullable=True),
    sa.Column("sic_description", sa.String(), nullable=True),
    sa.Column("category", sa.String(), nullable=True),
    sa.Column("state_of_incorporation", sa.String(), nullable=True),
    sa.Column("fiscal_year_end", sa.String(), nullable=True),
    sa.Column("tax_id", sa.String(), nullable=True),
    sa.Column("lei", sa.String(), nullable=True),
    sa.Column("industry", sa.String(), nullable=True),
    sa.Column("entity_type", sa.String(), nullable=True),
    sa.Column("phone", sa.String(), nullable=True),
    sa.Column("website", sa.String(), nullable=True),
    sa.Column("status", sa.String(), nullable=False),
    sa.Column("is_parent", sa.Boolean(), nullable=False),
    sa.Column("parent_entity_id", sa.String(), nullable=True),
    sa.Column("source", sa.String(), nullable=False),
    sa.Column("source_id", sa.String(), nullable=True),
    sa.Column("connection_id", sa.String(), nullable=True),
    sa.Column("address_line1", sa.String(), nullable=True),
    sa.Column("address_city", sa.String(), nullable=True),
    sa.Column("address_state", sa.String(), nullable=True),
    sa.Column("address_postal_code", sa.String(), nullable=True),
    sa.Column("address_country", sa.String(), nullable=True),
    sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column("version", sa.Integer(), nullable=False),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("updated_at", sa.DateTime(), nullable=False),
    sa.Column("created_by", sa.String(), nullable=False),
    sa.ForeignKeyConstraint(["parent_entity_id"], ["entities.id"]),
    sa.PrimaryKeyConstraint("id"),
  )
  op.create_index("idx_entities_source", "entities", ["source"])
  op.create_index("idx_entities_status", "entities", ["status"])
  op.create_index("idx_entities_parent", "entities", ["parent_entity_id"])

  # -- EntityTaxonomy join table (ENTITY_HAS_TAXONOMY in graph) --
  # Per-tenant template: each tenant has their own entity↔taxonomy adoption
  # rows. FKs reference per-tenant entities (tenant schema) and shared
  # public.taxonomies (visible via search_path = '{tenant}, public').
  op.create_table(
    "entity_taxonomies",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("entity_id", sa.String(), nullable=False),
    sa.Column("taxonomy_id", sa.String(), nullable=False),
    sa.Column("is_primary", sa.Boolean(), nullable=False, server_default="false"),
    sa.Column("basis", sa.String(), nullable=False),
    sa.Column("effective_from", sa.Date(), nullable=True),
    sa.Column("effective_to", sa.Date(), nullable=True),
    sa.Column("adoption_context", sa.String(), nullable=True),
    sa.Column(
      "created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.Column(
      "updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.ForeignKeyConstraint(["entity_id"], ["entities.id"], ondelete="CASCADE"),
    sa.ForeignKeyConstraint(["taxonomy_id"], ["taxonomies.id"], ondelete="RESTRICT"),
    sa.PrimaryKeyConstraint("id"),
    sa.UniqueConstraint(
      "entity_id",
      "taxonomy_id",
      "basis",
      name="uq_entity_taxonomy_combo",
    ),
    sa.CheckConstraint(
      "basis IN ('reporting', 'chart_of_accounts', 'mapping', 'schedule')",
      name="check_entity_taxonomy_basis",
    ),
    sa.CheckConstraint(
      "adoption_context IS NULL OR adoption_context IN "
      "('required_by_regulation', 'voluntary', 'contractual')",
      name="check_entity_taxonomy_adoption_context",
    ),
  )
  op.create_index("idx_entity_taxonomies_entity", "entity_taxonomies", ["entity_id"])
  op.create_index(
    "idx_entity_taxonomies_taxonomy", "entity_taxonomies", ["taxonomy_id"]
  )
  op.create_index(
    "idx_entity_taxonomies_primary",
    "entity_taxonomies",
    ["entity_id", "basis"],
    unique=True,
    postgresql_where=sa.text("is_primary = true"),
  )

  # -- Portfolios (RoboInvestor) --
  op.create_table(
    "portfolios",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("name", sa.String(), nullable=False),
    sa.Column("description", sa.String(), nullable=True),
    # Entity linkage (ENTITY_HAS_PORTFOLIO in graph) — the investing entity
    # that owns or manages this portfolio. Nullable for legacy portfolios.
    sa.Column("entity_id", sa.String(), nullable=True),
    sa.Column("ownership_type", sa.String(), nullable=True),
    sa.Column("strategy", sa.String(), nullable=True),
    sa.Column("inception_date", sa.Date(), nullable=True),
    sa.Column("base_currency", sa.String(), nullable=False),
    sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column("version", sa.Integer(), nullable=False),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("updated_at", sa.DateTime(), nullable=False),
    sa.Column("created_by", sa.String(), nullable=False),
    sa.ForeignKeyConstraint(["entity_id"], ["entities.id"], ondelete="RESTRICT"),
    sa.PrimaryKeyConstraint("id"),
    sa.CheckConstraint(
      "ownership_type IS NULL OR ownership_type IN ('direct', 'managed', 'advised')",
      name="check_portfolio_ownership_type",
    ),
  )
  op.create_index("idx_portfolios_strategy", "portfolios", ["strategy"])
  op.create_index(
    "idx_portfolios_entity",
    "portfolios",
    ["entity_id"],
    postgresql_where=sa.text("entity_id IS NOT NULL"),
  )

  # -- Securities (RoboInvestor) --
  op.create_table(
    "securities",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("entity_id", sa.String(), nullable=True),
    sa.Column("source_graph_id", sa.String(), nullable=True),
    sa.Column("name", sa.String(), nullable=False),
    sa.Column("security_type", sa.String(), nullable=False),
    sa.Column("security_subtype", sa.String(), nullable=True),
    sa.Column("terms", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column("is_active", sa.Boolean(), nullable=False),
    sa.Column("authorized_shares", sa.BigInteger(), nullable=True),
    sa.Column("outstanding_shares", sa.BigInteger(), nullable=True),
    sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column("version", sa.Integer(), nullable=False),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("updated_at", sa.DateTime(), nullable=False),
    sa.Column("created_by", sa.String(), nullable=False),
    sa.ForeignKeyConstraint(["entity_id"], ["entities.id"]),
    sa.PrimaryKeyConstraint("id"),
  )
  op.create_index("idx_securities_entity", "securities", ["entity_id"])
  op.create_index("idx_securities_source_graph", "securities", ["source_graph_id"])
  op.create_index("idx_securities_type", "securities", ["security_type"])
  op.create_index(
    "idx_securities_active",
    "securities",
    ["is_active"],
    postgresql_where=sa.text("is_active = true"),
  )

  # -- Positions (RoboInvestor) --
  op.create_table(
    "positions",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("portfolio_id", sa.String(), nullable=False),
    sa.Column("security_id", sa.String(), nullable=False),
    sa.Column("quantity", sa.Float(), nullable=False),
    sa.Column("quantity_type", sa.String(), nullable=False),
    sa.Column("cost_basis", sa.BigInteger(), nullable=False),
    sa.Column("currency", sa.String(), nullable=False),
    sa.Column("current_value", sa.BigInteger(), nullable=True),
    sa.Column("valuation_date", sa.Date(), nullable=True),
    sa.Column("valuation_source", sa.String(), nullable=True),
    sa.Column("acquisition_date", sa.Date(), nullable=True),
    sa.Column("disposition_date", sa.Date(), nullable=True),
    sa.Column("status", sa.String(), nullable=False),
    sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column("notes", sa.String(), nullable=True),
    sa.Column("version", sa.Integer(), nullable=False),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("updated_at", sa.DateTime(), nullable=False),
    sa.Column("created_by", sa.String(), nullable=False),
    sa.ForeignKeyConstraint(["portfolio_id"], ["portfolios.id"]),
    sa.ForeignKeyConstraint(["security_id"], ["securities.id"]),
    sa.PrimaryKeyConstraint("id"),
  )
  op.create_index("idx_positions_portfolio", "positions", ["portfolio_id"])
  op.create_index("idx_positions_security", "positions", ["security_id"])
  op.create_index("idx_positions_status", "positions", ["status"])
  op.create_index(
    "uq_positions_portfolio_security_active",
    "positions",
    ["portfolio_id", "security_id"],
    unique=True,
    postgresql_where=sa.text("status = 'active'"),
  )

  # -- Publish Lists --
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
  op.create_index("idx_publish_lists_name", "publish_lists", ["name"])

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
      "publish_list_id",
      "target_graph_id",
      name="uq_publish_list_members_pair",
    ),
  )
  op.create_index("idx_plm_list", "publish_list_members", ["publish_list_id"])
  op.create_index("idx_plm_target", "publish_list_members", ["target_graph_id"])

  # ──────────────────────────────────────────────────────────────────────
  # Seed reporting taxonomy
  # ──────────────────────────────────────────────────────────────────────
  from robosystems.taxonomy.seed import seed_reporting_taxonomy

  conn = op.get_bind()
  seed_reporting_taxonomy(conn)


def downgrade() -> None:
  # WARNING: This downgrade DESTROYS all tenant data irrecoverably.
  # Every kg* schema is dropped with CASCADE. Only run this on dev databases.
  # For staging/prod, prefer a forward migration to undo changes.
  from migrations.extensions.helpers import for_each_tenant_schema

  conn = op.get_bind()

  # Drop all tenant schemas — CASCADE removes all tables and data within each schema
  for_each_tenant_schema(
    conn,
    lambda conn, schema: conn.execute(
      sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    ),
  )

  # Drop tenant-template tables (reverse dependency order)
  op.drop_table("publish_list_members")
  op.drop_table("publish_lists")
  op.drop_table("positions")
  op.drop_table("securities")
  op.drop_table("portfolios")
  op.drop_table("entity_taxonomies")
  op.drop_table("entities")
  op.drop_table("report_shares")
  op.drop_table("facts")
  op.drop_table("reports")
  op.drop_table("classification_rules")
  op.drop_table("line_item_dimensions")
  op.drop_table("entry_dimensions")
  op.drop_table("transaction_dimensions")
  op.drop_table("line_items")
  op.drop_table("entries")
  op.drop_table("transactions")
  op.drop_table("dimensions")
  op.drop_table("fiscal_calendar_events")
  op.drop_table("fiscal_calendar")

  # Drop public-schema shared tables (reverse dependency order)
  op.drop_table("associations", schema="public")
  op.drop_table("elements")
  op.drop_table("structures", schema="public")
  op.drop_table("taxonomies", schema="public")
  op.drop_table("fiscal_periods")

  op.execute("DROP FUNCTION IF EXISTS public.generate_prefixed_id(TEXT)")
