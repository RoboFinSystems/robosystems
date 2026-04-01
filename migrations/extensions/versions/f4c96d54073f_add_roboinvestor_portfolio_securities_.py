"""add roboinvestor portfolio securities positions

Revision ID: f4c96d54073f
Revises: d8f3a5b2c4e6
Create Date: 2026-03-31 16:01:00.854221

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "f4c96d54073f"
down_revision = "d8f3a5b2c4e6"
branch_labels = None
depends_on = None


def upgrade() -> None:
  op.create_table(
    "portfolios",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("name", sa.String(), nullable=False),
    sa.Column("description", sa.String(), nullable=True),
    sa.Column("strategy", sa.String(), nullable=True),
    sa.Column("inception_date", sa.Date(), nullable=True),
    sa.Column("base_currency", sa.String(), nullable=False),
    sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column("version", sa.Integer(), nullable=False),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("updated_at", sa.DateTime(), nullable=False),
    sa.Column("created_by", sa.String(), nullable=False),
    sa.PrimaryKeyConstraint("id"),
  )
  op.create_index("idx_portfolios_strategy", "portfolios", ["strategy"], unique=False)

  op.create_table(
    "securities",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("entity_id", sa.String(), nullable=True),
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
    sa.ForeignKeyConstraint(
      ["entity_id"],
      ["entities.id"],
    ),
    sa.PrimaryKeyConstraint("id"),
  )
  op.create_index(
    "idx_securities_active",
    "securities",
    ["is_active"],
    unique=False,
    postgresql_where="is_active = true",
  )
  op.create_index("idx_securities_entity", "securities", ["entity_id"], unique=False)
  op.create_index("idx_securities_type", "securities", ["security_type"], unique=False)

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
    sa.ForeignKeyConstraint(
      ["portfolio_id"],
      ["portfolios.id"],
    ),
    sa.ForeignKeyConstraint(
      ["security_id"],
      ["securities.id"],
    ),
    sa.PrimaryKeyConstraint("id"),
  )
  op.create_index(
    "idx_positions_portfolio", "positions", ["portfolio_id"], unique=False
  )
  op.create_index("idx_positions_security", "positions", ["security_id"], unique=False)
  op.create_index("idx_positions_status", "positions", ["status"], unique=False)
  op.create_index(
    "uq_positions_portfolio_security_active",
    "positions",
    ["portfolio_id", "security_id"],
    unique=True,
    postgresql_where=sa.text("status = 'active'"),
  )


def downgrade() -> None:
  op.drop_index(
    "uq_positions_portfolio_security_active",
    table_name="positions",
    postgresql_where=sa.text("status = 'active'"),
  )
  op.drop_index("idx_positions_status", table_name="positions")
  op.drop_index("idx_positions_security", table_name="positions")
  op.drop_index("idx_positions_portfolio", table_name="positions")
  op.drop_table("positions")
  op.drop_index("idx_securities_type", table_name="securities")
  op.drop_index("idx_securities_entity", table_name="securities")
  op.drop_index(
    "idx_securities_active",
    table_name="securities",
    postgresql_where="is_active = true",
  )
  op.drop_table("securities")
  op.drop_index("idx_portfolios_strategy", table_name="portfolios")
  op.drop_table("portfolios")
