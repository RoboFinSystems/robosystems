"""add the fact_dimensions junction — Fact gains a dimension carrier

Facts were the only ledger noun without a dimension link — transactions,
entries, line_items, and events all carry junctions to the base
``Dimension`` model. ``fact_dimensions`` completes the set so the report
layer can mark a fact as belonging to an explicit member on an axis.
Scenario is the first use: forecast facts gain a ``scenario`` Dimension,
actuals stay unrowed (the XBRL default-member rule), so readers honoring
the documented consolidated-totals contract exclude scenario facts by
construction.

Nothing writes this table yet — this is P0 of the scenario-dimension
build; the forecast emitter and the backfill from
``fact_sets.scenario_id`` land separately.

FK semantics mirror the existing junctions: fact side CASCADE (a tag is
meaningless without its fact), dimension side RESTRICT (dimensions are
shared reference data; deleting one still in use is an explicit operator
decision). New tenant schemas get the table via ``metadata.create_all``
from the model; this migration covers public and existing tenants.

Revision ID: 0026
Revises: 0025
Create Date: 2026-08-06

"""

from __future__ import annotations

from alembic import op

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0026"
down_revision = "0025"
branch_labels = None
depends_on = None


def _create(conn, schema: str) -> None:
  ops = TenantOps(conn, schema)
  ops.create_table(
    "fact_dimensions",
    f"""
    fact_id VARCHAR NOT NULL REFERENCES "{schema}".facts (id) ON DELETE CASCADE,
    dimension_id VARCHAR NOT NULL REFERENCES "{schema}".dimensions (id) ON DELETE RESTRICT,
    PRIMARY KEY (fact_id, dimension_id)
    """,
  )


def _drop(conn, schema: str) -> None:
  TenantOps(conn, schema).drop_table("fact_dimensions")


def upgrade() -> None:
  conn = op.get_bind()
  _create(conn, "public")
  for_each_tenant_schema(conn, _create)


def downgrade() -> None:
  conn = op.get_bind()
  _drop(conn, "public")
  for_each_tenant_schema(conn, _drop)
