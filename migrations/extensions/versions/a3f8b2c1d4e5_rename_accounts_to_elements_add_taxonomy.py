"""rename accounts to elements, add taxonomy system

Revision ID: a3f8b2c1d4e5
Revises: 1eec7fa2156e
Create Date: 2026-03-27 12:00:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "a3f8b2c1d4e5"
down_revision = "1eec7fa2156e"
branch_labels = None
depends_on = None


def upgrade() -> None:
  # ──────────────────────────────────────────────────────────────────────
  # 1. Create public.taxonomies table
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
    sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
    sa.Column("is_locked", sa.Boolean(), nullable=False, server_default="false"),
    sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default="{}"),
    sa.Column(
      "created_at",
      sa.DateTime(),
      nullable=False,
      server_default=sa.text("now()"),
    ),
    sa.Column(
      "updated_at",
      sa.DateTime(),
      nullable=False,
      server_default=sa.text("now()"),
    ),
    sa.Column("created_by", sa.String(), nullable=False, server_default="system"),
    sa.PrimaryKeyConstraint("id"),
    sa.ForeignKeyConstraint(["source_taxonomy_id"], ["taxonomies.id"], use_alter=True),
    sa.ForeignKeyConstraint(["target_taxonomy_id"], ["taxonomies.id"], use_alter=True),
    sa.CheckConstraint(
      "taxonomy_type IN ('chart_of_accounts', 'reporting', 'mapping')",
      name="check_taxonomy_type",
    ),
    schema="public",
  )
  op.create_index(
    "idx_taxonomies_type", "taxonomies", ["taxonomy_type"], schema="public"
  )
  op.create_index(
    "idx_taxonomies_standard",
    "taxonomies",
    ["standard"],
    schema="public",
    postgresql_where=sa.text("standard IS NOT NULL"),
  )

  # ──────────────────────────────────────────────────────────────────────
  # 2. Create public.structures table (for shared seed data)
  # ──────────────────────────────────────────────────────────────────────
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
      "created_at",
      sa.DateTime(),
      nullable=False,
      server_default=sa.text("now()"),
    ),
    sa.Column(
      "updated_at",
      sa.DateTime(),
      nullable=False,
      server_default=sa.text("now()"),
    ),
    sa.Column("created_by", sa.String(), nullable=False, server_default="system"),
    sa.PrimaryKeyConstraint("id"),
    sa.ForeignKeyConstraint(["taxonomy_id"], ["taxonomies.id"]),
    sa.CheckConstraint(
      "structure_type IN ("
      "'chart_of_accounts', 'income_statement', 'balance_sheet', "
      "'cash_flow_statement', 'equity_statement', 'coa_mapping', 'custom'"
      ")",
      name="check_structure_type",
    ),
    schema="public",
  )
  op.create_index(
    "idx_structures_taxonomy", "structures", ["taxonomy_id"], schema="public"
  )
  op.create_index(
    "idx_structures_type", "structures", ["structure_type"], schema="public"
  )

  # ──────────────────────────────────────────────────────────────────────
  # 3. Rename accounts → elements in public schema
  # ──────────────────────────────────────────────────────────────────────
  op.rename_table("accounts", "elements")

  # Drop old constraints and indexes
  op.drop_constraint("check_account_classification", "elements", type_="check")
  op.drop_constraint("check_balance_type", "elements", type_="check")
  op.drop_index("idx_accounts_classification", table_name="elements")
  op.drop_index("idx_accounts_parent", table_name="elements")
  op.drop_index("idx_accounts_external", table_name="elements")
  op.drop_index("idx_accounts_active", table_name="elements")

  # Make code nullable (taxonomy concepts don't have user-facing codes)
  op.alter_column("elements", "code", nullable=True)
  # Drop the old unique constraint on code
  op.drop_constraint("accounts_code_key", "elements", type_="unique")

  # Add new columns
  op.add_column("elements", sa.Column("qname", sa.String(), nullable=True))
  op.add_column("elements", sa.Column("namespace", sa.String(), nullable=True))
  op.add_column("elements", sa.Column("uri", sa.String(), nullable=True))
  op.add_column(
    "elements",
    sa.Column("period_type", sa.String(), nullable=False, server_default="duration"),
  )
  op.add_column(
    "elements",
    sa.Column("is_abstract", sa.Boolean(), nullable=False, server_default="false"),
  )
  op.add_column(
    "elements",
    sa.Column("is_monetary", sa.Boolean(), nullable=False, server_default="true"),
  )
  op.add_column(
    "elements",
    sa.Column("element_type", sa.String(), nullable=False, server_default="concept"),
  )
  op.add_column("elements", sa.Column("taxonomy_id", sa.String(), nullable=True))
  op.add_column(
    "elements",
    sa.Column("source", sa.String(), nullable=False, server_default="native"),
  )

  # Add new constraints
  op.create_check_constraint(
    "check_element_classification",
    "elements",
    "classification IN ('asset', 'liability', 'equity', 'revenue', 'expense')",
  )
  op.create_check_constraint(
    "check_element_balance_type",
    "elements",
    "balance_type IN ('debit', 'credit')",
  )
  op.create_check_constraint(
    "check_element_period_type",
    "elements",
    "period_type IN ('duration', 'instant')",
  )
  op.create_check_constraint(
    "check_element_type",
    "elements",
    "element_type IN ('concept', 'abstract', 'axis', 'member', 'hypercube')",
  )
  op.create_check_constraint(
    "check_element_source",
    "elements",
    "source IN ('sfac6', 'us-gaap', 'ifrs', 'quickbooks', 'xero', "
    "'plaid', 'native', 'import')",
  )

  # Add new indexes
  op.create_index("idx_elements_classification", "elements", ["classification"])
  op.create_index("idx_elements_parent", "elements", ["parent_id"])
  op.create_index(
    "idx_elements_external", "elements", ["external_id", "external_source"]
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

  # Add FK to taxonomies
  op.create_foreign_key(
    "fk_elements_taxonomy", "elements", "taxonomies", ["taxonomy_id"], ["id"]
  )

  # Update self-FK from accounts.id to elements.id
  op.drop_constraint("accounts_parent_id_fkey", "elements", type_="foreignkey")
  op.create_foreign_key(
    "elements_parent_id_fkey", "elements", "elements", ["parent_id"], ["id"]
  )

  # ──────────────────────────────────────────────────────────────────────
  # 4. Create public.element_associations table (for shared seed data)
  # ──────────────────────────────────────────────────────────────────────
  op.create_table(
    "element_associations",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("structure_id", sa.String(), nullable=False),
    sa.Column("from_element_id", sa.String(), nullable=False),
    sa.Column("to_element_id", sa.String(), nullable=False),
    sa.Column(
      "association_type",
      sa.String(),
      nullable=False,
      server_default="presentation",
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
      "created_at",
      sa.DateTime(),
      nullable=False,
      server_default=sa.text("now()"),
    ),
    sa.Column(
      "updated_at",
      sa.DateTime(),
      nullable=False,
      server_default=sa.text("now()"),
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
    "element_associations",
    ["structure_id"],
    schema="public",
  )
  op.create_index(
    "idx_associations_from",
    "element_associations",
    ["from_element_id"],
    schema="public",
  )
  op.create_index(
    "idx_associations_to",
    "element_associations",
    ["to_element_id"],
    schema="public",
  )
  op.create_index(
    "idx_associations_type",
    "element_associations",
    ["association_type"],
    schema="public",
  )

  # ──────────────────────────────────────────────────────────────────────
  # 5. Rename FK columns on dependent tables
  # ──────────────────────────────────────────────────────────────────────

  # line_items.account_id → element_id
  op.drop_constraint("line_items_account_id_fkey", "line_items", type_="foreignkey")
  op.drop_index("idx_line_items_account", table_name="line_items")
  op.alter_column("line_items", "account_id", new_column_name="element_id")
  op.create_foreign_key(
    "line_items_element_id_fkey", "line_items", "elements", ["element_id"], ["id"]
  )
  op.create_index("idx_line_items_element", "line_items", ["element_id"])

  # classification_rules: 3 FK columns
  for old_name, new_name in [
    ("target_account_id", "target_element_id"),
    ("debit_account_id", "debit_element_id"),
    ("credit_account_id", "credit_element_id"),
  ]:
    op.drop_constraint(
      f"classification_rules_{old_name}_fkey",
      "classification_rules",
      type_="foreignkey",
    )
    op.alter_column("classification_rules", old_name, new_column_name=new_name)
    op.create_foreign_key(
      f"classification_rules_{new_name}_fkey",
      "classification_rules",
      "elements",
      [new_name],
      ["id"],
    )

  # ──────────────────────────────────────────────────────────────────────
  # 6. Apply changes to existing tenant schemas
  # ──────────────────────────────────────────────────────────────────────
  conn = op.get_bind()
  schemas = conn.execute(
    sa.text(
      "SELECT schema_name FROM information_schema.schemata "
      "WHERE schema_name ~ '^kg[0-9a-f]{16,}$'"
    )
  ).fetchall()

  for (schema_name,) in schemas:
    _upgrade_tenant_schema(conn, schema_name)

  # ──────────────────────────────────────────────────────────────────────
  # 7. Seed reporting taxonomy
  # ──────────────────────────────────────────────────────────────────────
  from robosystems.config.taxonomy.seed import seed_reporting_taxonomy

  seed_reporting_taxonomy(conn)


def _upgrade_tenant_schema(conn, schema: str) -> None:
  """Apply the accounts→elements rename and new tables to a tenant schema."""
  s = schema  # shorthand

  # Rename table
  conn.execute(sa.text(f'ALTER TABLE "{s}".accounts RENAME TO elements'))

  # Drop old constraints/indexes
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".elements DROP CONSTRAINT IF EXISTS check_account_classification'
    )
  )
  conn.execute(
    sa.text(f'ALTER TABLE "{s}".elements DROP CONSTRAINT IF EXISTS check_balance_type')
  )
  conn.execute(sa.text(f'DROP INDEX IF EXISTS "{s}".idx_accounts_classification'))
  conn.execute(sa.text(f'DROP INDEX IF EXISTS "{s}".idx_accounts_parent'))
  conn.execute(sa.text(f'DROP INDEX IF EXISTS "{s}".idx_accounts_external'))
  conn.execute(sa.text(f'DROP INDEX IF EXISTS "{s}".idx_accounts_active'))

  # Make code nullable, drop unique
  conn.execute(sa.text(f'ALTER TABLE "{s}".elements ALTER COLUMN code DROP NOT NULL'))
  conn.execute(
    sa.text(f'ALTER TABLE "{s}".elements DROP CONSTRAINT IF EXISTS accounts_code_key')
  )

  # Add new columns
  conn.execute(
    sa.text(f'ALTER TABLE "{s}".elements ADD COLUMN IF NOT EXISTS qname TEXT')
  )
  conn.execute(
    sa.text(f'ALTER TABLE "{s}".elements ADD COLUMN IF NOT EXISTS namespace TEXT')
  )
  conn.execute(sa.text(f'ALTER TABLE "{s}".elements ADD COLUMN IF NOT EXISTS uri TEXT'))
  conn.execute(
    sa.text(
      f"ALTER TABLE \"{s}\".elements ADD COLUMN IF NOT EXISTS period_type TEXT NOT NULL DEFAULT 'duration'"
    )
  )
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".elements ADD COLUMN IF NOT EXISTS is_abstract BOOLEAN NOT NULL DEFAULT false'
    )
  )
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".elements ADD COLUMN IF NOT EXISTS is_monetary BOOLEAN NOT NULL DEFAULT true'
    )
  )
  conn.execute(
    sa.text(
      f"ALTER TABLE \"{s}\".elements ADD COLUMN IF NOT EXISTS element_type TEXT NOT NULL DEFAULT 'concept'"
    )
  )
  conn.execute(
    sa.text(f'ALTER TABLE "{s}".elements ADD COLUMN IF NOT EXISTS taxonomy_id TEXT')
  )
  conn.execute(
    sa.text(
      f"ALTER TABLE \"{s}\".elements ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'native'"
    )
  )

  # Add new constraints
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".elements ADD CONSTRAINT check_element_classification '
      "CHECK (classification IN ('asset', 'liability', 'equity', 'revenue', 'expense'))"
    )
  )
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".elements ADD CONSTRAINT check_element_balance_type '
      "CHECK (balance_type IN ('debit', 'credit'))"
    )
  )
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".elements ADD CONSTRAINT check_element_period_type '
      "CHECK (period_type IN ('duration', 'instant'))"
    )
  )
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".elements ADD CONSTRAINT check_element_type '
      "CHECK (element_type IN ('concept', 'abstract', 'axis', 'member', 'hypercube'))"
    )
  )
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".elements ADD CONSTRAINT check_element_source '
      "CHECK (source IN ('sfac6', 'us-gaap', 'ifrs', 'quickbooks', 'xero', "
      "'plaid', 'native', 'import'))"
    )
  )

  # Recreate indexes
  conn.execute(
    sa.text(
      f'CREATE INDEX idx_elements_classification ON "{s}".elements (classification)'
    )
  )
  conn.execute(
    sa.text(f'CREATE INDEX idx_elements_parent ON "{s}".elements (parent_id)')
  )
  conn.execute(
    sa.text(
      f'CREATE INDEX idx_elements_external ON "{s}".elements (external_id, external_source)'
    )
  )
  conn.execute(
    sa.text(
      f'CREATE INDEX idx_elements_active ON "{s}".elements (is_active) WHERE is_active = true'
    )
  )
  conn.execute(
    sa.text(f'CREATE INDEX idx_elements_taxonomy ON "{s}".elements (taxonomy_id)')
  )
  conn.execute(sa.text(f'CREATE INDEX idx_elements_source ON "{s}".elements (source)'))
  conn.execute(
    sa.text(
      f'CREATE UNIQUE INDEX idx_elements_qname ON "{s}".elements (qname) WHERE qname IS NOT NULL'
    )
  )
  conn.execute(
    sa.text(
      f'CREATE INDEX idx_elements_namespace ON "{s}".elements (namespace) WHERE namespace IS NOT NULL'
    )
  )

  # Rename FK columns on line_items
  conn.execute(
    sa.text(f'ALTER TABLE "{s}".line_items RENAME COLUMN account_id TO element_id')
  )
  conn.execute(sa.text(f'DROP INDEX IF EXISTS "{s}".idx_line_items_account'))
  conn.execute(
    sa.text(f'CREATE INDEX idx_line_items_element ON "{s}".line_items (element_id)')
  )

  # Rename FK columns on classification_rules
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".classification_rules RENAME COLUMN target_account_id TO target_element_id'
    )
  )
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".classification_rules RENAME COLUMN debit_account_id TO debit_element_id'
    )
  )
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".classification_rules RENAME COLUMN credit_account_id TO credit_element_id'
    )
  )

  # Create tenant structures and element_associations tables
  conn.execute(
    sa.text(f"""
            CREATE TABLE IF NOT EXISTS "{s}".structures (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                structure_type TEXT NOT NULL,
                taxonomy_id TEXT NOT NULL,
                graph_structure_id TEXT,
                is_active BOOLEAN NOT NULL DEFAULT true,
                metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMP NOT NULL DEFAULT now(),
                updated_at TIMESTAMP NOT NULL DEFAULT now(),
                created_by TEXT NOT NULL DEFAULT 'system',
                CONSTRAINT check_structure_type CHECK (structure_type IN (
                    'chart_of_accounts', 'income_statement', 'balance_sheet',
                    'cash_flow_statement', 'equity_statement', 'coa_mapping', 'custom'
                ))
            )
        """)
  )
  conn.execute(
    sa.text(
      f'CREATE INDEX IF NOT EXISTS idx_structures_taxonomy ON "{s}".structures (taxonomy_id)'
    )
  )
  conn.execute(
    sa.text(
      f'CREATE INDEX IF NOT EXISTS idx_structures_type ON "{s}".structures (structure_type)'
    )
  )

  conn.execute(
    sa.text(f"""
            CREATE TABLE IF NOT EXISTS "{s}".element_associations (
                id TEXT PRIMARY KEY,
                structure_id TEXT NOT NULL REFERENCES "{s}".structures(id),
                from_element_id TEXT NOT NULL REFERENCES "{s}".elements(id),
                to_element_id TEXT NOT NULL REFERENCES "{s}".elements(id),
                association_type TEXT NOT NULL DEFAULT 'presentation',
                arcrole TEXT,
                order_value FLOAT DEFAULT 0,
                weight FLOAT,
                confidence FLOAT,
                suggested_by TEXT,
                approved_by TEXT,
                approved_at TIMESTAMP,
                metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMP NOT NULL DEFAULT now(),
                updated_at TIMESTAMP NOT NULL DEFAULT now(),
                created_by TEXT NOT NULL DEFAULT 'system',
                CONSTRAINT uq_association_structure_elements_type
                    UNIQUE (structure_id, from_element_id, to_element_id, association_type),
                CONSTRAINT check_association_type
                    CHECK (association_type IN ('presentation', 'calculation', 'mapping'))
            )
        """)
  )
  conn.execute(
    sa.text(
      f'CREATE INDEX IF NOT EXISTS idx_associations_structure ON "{s}".element_associations (structure_id)'
    )
  )
  conn.execute(
    sa.text(
      f'CREATE INDEX IF NOT EXISTS idx_associations_from ON "{s}".element_associations (from_element_id)'
    )
  )
  conn.execute(
    sa.text(
      f'CREATE INDEX IF NOT EXISTS idx_associations_to ON "{s}".element_associations (to_element_id)'
    )
  )
  conn.execute(
    sa.text(
      f'CREATE INDEX IF NOT EXISTS idx_associations_type ON "{s}".element_associations (association_type)'
    )
  )


def downgrade() -> None:
  conn = op.get_bind()

  # Remove seed data
  conn.execute(sa.text("DELETE FROM public.element_associations"))
  conn.execute(sa.text("DELETE FROM public.structures"))
  conn.execute(
    sa.text("DELETE FROM public.elements WHERE source IN ('sfac6', 'us-gaap')")
  )
  conn.execute(sa.text("DELETE FROM public.taxonomies"))

  # Reverse tenant schema changes
  schemas = conn.execute(
    sa.text(
      "SELECT schema_name FROM information_schema.schemata "
      "WHERE schema_name ~ '^kg[0-9a-f]{16,}$'"
    )
  ).fetchall()

  for (schema_name,) in schemas:
    _downgrade_tenant_schema(conn, schema_name)

  # Drop element_associations table
  op.drop_table("element_associations", schema="public")

  # Reverse FK column renames on classification_rules
  for new_name, old_name in [
    ("target_element_id", "target_account_id"),
    ("debit_element_id", "debit_account_id"),
    ("credit_element_id", "credit_account_id"),
  ]:
    op.drop_constraint(
      f"classification_rules_{new_name}_fkey",
      "classification_rules",
      type_="foreignkey",
    )
    op.alter_column("classification_rules", new_name, new_column_name=old_name)
    op.create_foreign_key(
      f"classification_rules_{old_name}_fkey",
      "classification_rules",
      "accounts",
      [old_name],
      ["id"],
    )

  # Reverse line_items FK rename
  op.drop_index("idx_line_items_element", table_name="line_items")
  op.drop_constraint("line_items_element_id_fkey", "line_items", type_="foreignkey")
  op.alter_column("line_items", "element_id", new_column_name="account_id")
  op.create_foreign_key(
    "line_items_account_id_fkey", "line_items", "accounts", ["account_id"], ["id"]
  )
  op.create_index("idx_line_items_account", "line_items", ["account_id"])

  # Drop new indexes and constraints on elements
  op.drop_index("idx_elements_namespace", table_name="elements")
  op.drop_index("idx_elements_qname", table_name="elements")
  op.drop_index("idx_elements_source", table_name="elements")
  op.drop_index("idx_elements_taxonomy", table_name="elements")
  op.drop_index("idx_elements_active", table_name="elements")
  op.drop_index("idx_elements_external", table_name="elements")
  op.drop_index("idx_elements_parent", table_name="elements")
  op.drop_index("idx_elements_classification", table_name="elements")
  op.drop_constraint("fk_elements_taxonomy", "elements", type_="foreignkey")
  op.drop_constraint("elements_parent_id_fkey", "elements", type_="foreignkey")
  op.drop_constraint("check_element_source", "elements", type_="check")
  op.drop_constraint("check_element_type", "elements", type_="check")
  op.drop_constraint("check_element_period_type", "elements", type_="check")
  op.drop_constraint("check_element_balance_type", "elements", type_="check")
  op.drop_constraint("check_element_classification", "elements", type_="check")

  # Drop new columns
  op.drop_column("elements", "source")
  op.drop_column("elements", "taxonomy_id")
  op.drop_column("elements", "element_type")
  op.drop_column("elements", "is_monetary")
  op.drop_column("elements", "is_abstract")
  op.drop_column("elements", "period_type")
  op.drop_column("elements", "uri")
  op.drop_column("elements", "namespace")
  op.drop_column("elements", "qname")

  # Restore code as NOT NULL + unique
  op.alter_column("elements", "code", nullable=False)
  op.create_unique_constraint("accounts_code_key", "elements", ["code"])

  # Restore old constraints and indexes
  op.create_check_constraint(
    "check_account_classification",
    "elements",
    "classification IN ('asset', 'liability', 'equity', 'revenue', 'expense')",
  )
  op.create_check_constraint(
    "check_balance_type",
    "elements",
    "balance_type IN ('debit', 'credit')",
  )
  op.create_index("idx_accounts_classification", "elements", ["classification"])
  op.create_index("idx_accounts_parent", "elements", ["parent_id"])
  op.create_index(
    "idx_accounts_external", "elements", ["external_id", "external_source"]
  )
  op.create_index(
    "idx_accounts_active",
    "elements",
    ["is_active"],
    postgresql_where=sa.text("is_active = true"),
  )
  op.create_foreign_key(
    "accounts_parent_id_fkey", "elements", "elements", ["parent_id"], ["id"]
  )

  # Rename back to accounts
  op.rename_table("elements", "accounts")

  # Drop structures and taxonomies
  op.drop_table("structures", schema="public")
  op.drop_table("taxonomies", schema="public")


def _downgrade_tenant_schema(conn, schema: str) -> None:
  """Reverse the migration for a tenant schema."""
  s = schema

  # Drop new tables
  conn.execute(sa.text(f'DROP TABLE IF EXISTS "{s}".element_associations'))
  conn.execute(sa.text(f'DROP TABLE IF EXISTS "{s}".structures'))

  # Rename FK columns back
  conn.execute(
    sa.text(f'ALTER TABLE "{s}".line_items RENAME COLUMN element_id TO account_id')
  )
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".classification_rules RENAME COLUMN target_element_id TO target_account_id'
    )
  )
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".classification_rules RENAME COLUMN debit_element_id TO debit_account_id'
    )
  )
  conn.execute(
    sa.text(
      f'ALTER TABLE "{s}".classification_rules RENAME COLUMN credit_element_id TO credit_account_id'
    )
  )

  # Drop new columns and constraints
  conn.execute(sa.text(f'ALTER TABLE "{s}".elements DROP COLUMN IF EXISTS source'))
  conn.execute(sa.text(f'ALTER TABLE "{s}".elements DROP COLUMN IF EXISTS taxonomy_id'))
  conn.execute(
    sa.text(f'ALTER TABLE "{s}".elements DROP COLUMN IF EXISTS element_type')
  )
  conn.execute(sa.text(f'ALTER TABLE "{s}".elements DROP COLUMN IF EXISTS is_monetary'))
  conn.execute(sa.text(f'ALTER TABLE "{s}".elements DROP COLUMN IF EXISTS is_abstract'))
  conn.execute(sa.text(f'ALTER TABLE "{s}".elements DROP COLUMN IF EXISTS period_type'))
  conn.execute(sa.text(f'ALTER TABLE "{s}".elements DROP COLUMN IF EXISTS uri'))
  conn.execute(sa.text(f'ALTER TABLE "{s}".elements DROP COLUMN IF EXISTS namespace'))
  conn.execute(sa.text(f'ALTER TABLE "{s}".elements DROP COLUMN IF EXISTS qname'))
  conn.execute(sa.text(f'ALTER TABLE "{s}".elements ALTER COLUMN code SET NOT NULL'))

  # Rename back
  conn.execute(sa.text(f'ALTER TABLE "{s}".elements RENAME TO accounts'))
