"""add provenance column to fact_sets

One additive, nullable JSONB column on ``fact_sets`` to record the typed
``FactProvenance`` descriptor (how the FactSet's facts were constructed —
the auditability spine, ``models/api/fact_provenance``).

Nullable in the DB so pre-feature historical rows stay valid and the
migration is non-blocking; mandatoriness for *new* inserts is enforced at
the application boundary (the ``before_insert`` backstop on the FactSet
model + the ``create_fact_set`` helper), not by a DB NOT NULL.

Fresh tenants get the column via ``metadata.create_all`` at provision;
existing tenant schemas (and public) get it here via the TenantOps
fan-out pattern (matches 0017).

Revision ID: 0018
Revises: 0017
Create Date: 2026-05-30

"""

from __future__ import annotations

from alembic import op

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0018"
down_revision = "0017"
branch_labels = None
depends_on = None


def _add_provenance_column(conn, schema: str) -> None:
  TenantOps(conn, schema).add_column("fact_sets", "provenance", "JSONB", nullable=True)


def _drop_provenance_column(conn, schema: str) -> None:
  TenantOps(conn, schema).drop_column("fact_sets", "provenance")


def upgrade() -> None:
  conn = op.get_bind()
  _add_provenance_column(conn, "public")
  for_each_tenant_schema(conn, _add_provenance_column)


def downgrade() -> None:
  conn = op.get_bind()
  _drop_provenance_column(conn, "public")
  for_each_tenant_schema(conn, _drop_provenance_column)
