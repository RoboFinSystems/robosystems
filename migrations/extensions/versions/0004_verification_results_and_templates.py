"""verification_results + structure_templates tables.

Data-model landing for the verification infrastructure:

- ``verification_results`` persists the outcome of evaluating a Rule
  against a FactSet / Structure. The engine producer ships in a
  follow-up; this migration lands the table so the MCP
  ``list-verification-failures`` tool + block viewer "Verification
  Results" tab have a durable home to read from.
- ``structure_templates`` holds reusable layout / mechanics presets
  that Structures can pin via the existing
  ``structures.template_id`` column. Template content is expand-pass
  work.

Both tables land in public + every tenant schema. FKs reference the
same-schema parent tables so library-origin rows keep referential
integrity after tenant copy.

Revision ID: 0004
Revises: 0003
Create Date: 2026-04-21
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from migrations.extensions.helpers import for_each_tenant_schema

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


_VERIFICATION_STATUS_CHECK = "status IN ('pass', 'fail', 'error', 'skipped')"
_TEMPLATE_TYPE_CHECK = (
  "template_type IN ('renderer', 'schedule_variant', 'metric_preset')"
)


def _create_in_tenant(conn, schema: str) -> None:
  conn.execute(
    text(f"""
      CREATE TABLE IF NOT EXISTS {schema}.verification_results (
        id VARCHAR PRIMARY KEY,
        rule_id VARCHAR NOT NULL REFERENCES {schema}.rules(id),
        structure_id VARCHAR REFERENCES {schema}.structures(id),
        fact_set_id VARCHAR,
        entity_id VARCHAR,
        period_start DATE,
        period_end DATE,
        status VARCHAR NOT NULL,
        message VARCHAR,
        detail JSONB NOT NULL DEFAULT '{{}}'::jsonb,
        evaluated_at TIMESTAMP NOT NULL DEFAULT now(),
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        created_by VARCHAR NOT NULL DEFAULT 'system',
        CONSTRAINT check_{schema}_verification_result_status
          CHECK ({_VERIFICATION_STATUS_CHECK})
      )
    """)
  )
  for col in ("rule", "structure", "fact_set", "evaluated"):
    column_ref = "evaluated_at" if col == "evaluated" else f"{col}_id"
    conn.execute(
      text(
        f"CREATE INDEX IF NOT EXISTS idx_{schema}_verification_results_{col} "
        f"ON {schema}.verification_results ({column_ref})"
      )
    )

  conn.execute(
    text(f"""
      CREATE TABLE IF NOT EXISTS {schema}.structure_templates (
        id VARCHAR PRIMARY KEY,
        name VARCHAR NOT NULL,
        description VARCHAR,
        target_block_type VARCHAR NOT NULL,
        template_type VARCHAR NOT NULL,
        body JSONB NOT NULL DEFAULT '{{}}'::jsonb,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        created_by VARCHAR NOT NULL DEFAULT 'system',
        CONSTRAINT check_{schema}_structure_template_type
          CHECK ({_TEMPLATE_TYPE_CHECK})
      )
    """)
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_structure_templates_target_type "
      f"ON {schema}.structure_templates (target_block_type)"
    )
  )


def _drop_in_tenant(conn, schema: str) -> None:
  conn.execute(
    text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_structure_templates_target_type")
  )
  conn.execute(text(f"DROP TABLE IF EXISTS {schema}.structure_templates"))

  for col in ("evaluated", "fact_set", "structure", "rule"):
    conn.execute(
      text(f"DROP INDEX IF EXISTS {schema}.idx_{schema}_verification_results_{col}")
    )
  conn.execute(text(f"DROP TABLE IF EXISTS {schema}.verification_results"))


def upgrade() -> None:
  # Public schema — verification_results.
  op.create_table(
    "verification_results",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("rule_id", sa.String(), nullable=False),
    sa.Column("structure_id", sa.String(), nullable=True),
    sa.Column("fact_set_id", sa.String(), nullable=True),
    sa.Column("entity_id", sa.String(), nullable=True),
    sa.Column("period_start", sa.Date(), nullable=True),
    sa.Column("period_end", sa.Date(), nullable=True),
    sa.Column("status", sa.String(), nullable=False),
    sa.Column("message", sa.String(), nullable=True),
    sa.Column(
      "detail",
      postgresql.JSONB(astext_type=sa.Text()),
      nullable=False,
      server_default=sa.text("'{}'::jsonb"),
    ),
    sa.Column(
      "evaluated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.Column(
      "created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.Column("created_by", sa.String(), nullable=False, server_default="system"),
    sa.ForeignKeyConstraint(["rule_id"], ["rules.id"]),
    sa.ForeignKeyConstraint(["structure_id"], ["structures.id"]),
    sa.PrimaryKeyConstraint("id"),
    sa.CheckConstraint(
      _VERIFICATION_STATUS_CHECK, name="check_verification_result_status"
    ),
  )
  op.create_index("idx_verification_results_rule", "verification_results", ["rule_id"])
  op.create_index(
    "idx_verification_results_structure",
    "verification_results",
    ["structure_id"],
  )
  op.create_index(
    "idx_verification_results_fact_set",
    "verification_results",
    ["fact_set_id"],
  )
  op.create_index(
    "idx_verification_results_evaluated",
    "verification_results",
    ["evaluated_at"],
  )

  # Public schema — structure_templates.
  op.create_table(
    "structure_templates",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("name", sa.String(), nullable=False),
    sa.Column("description", sa.String(), nullable=True),
    sa.Column("target_block_type", sa.String(), nullable=False),
    sa.Column("template_type", sa.String(), nullable=False),
    sa.Column(
      "body",
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
    sa.Column("created_by", sa.String(), nullable=False, server_default="system"),
    sa.PrimaryKeyConstraint("id"),
    sa.CheckConstraint(_TEMPLATE_TYPE_CHECK, name="check_structure_template_type"),
  )
  op.create_index(
    "idx_structure_templates_target_type",
    "structure_templates",
    ["target_block_type"],
  )

  conn = op.get_bind()
  for_each_tenant_schema(conn, _create_in_tenant)


def downgrade() -> None:
  conn = op.get_bind()
  for_each_tenant_schema(conn, _drop_in_tenant)

  op.drop_index("idx_structure_templates_target_type", table_name="structure_templates")
  op.drop_table("structure_templates")

  op.drop_index("idx_verification_results_evaluated", table_name="verification_results")
  op.drop_index("idx_verification_results_fact_set", table_name="verification_results")
  op.drop_index("idx_verification_results_structure", table_name="verification_results")
  op.drop_index("idx_verification_results_rule", table_name="verification_results")
  op.drop_table("verification_results")
