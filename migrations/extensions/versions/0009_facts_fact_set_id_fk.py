"""facts.fact_set_id → fact_sets.id FK (§3.5 Phase 1 of §6.5).

Promotes ``facts.fact_set_id`` from an unconstrained string to a real
FK against ``fact_sets.id``. Pairs with the create_report /
create_schedule reorder that now creates the FactSet row before
stamping facts.

Steps per schema (public + every tenant):

1. NULL out any orphan ``fact_set_id`` values on ``facts`` that don't
   resolve to an existing FactSet. The pre-§3.5 code path could stamp
   a ULID on a fact even when ``_create_report_fact_sets``
   subsequently skipped the row (e.g., facts whose ``period_end`` was
   NULL). Reporting Style hasn't shipped to prod so the local-dev
   blast radius is small; we set orphans to NULL rather than try to
   reconstruct historical FactSets.
2. Add the FK with ``ON DELETE SET NULL`` so a cascading FactSet
   delete leaves facts intact (matching the existing pattern where
   ``report_id`` is also a soft pointer).

Revision ID: 0009
Revises: 0008
Create Date: 2026-05-13
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None

_FK_NAME = "fk_facts_fact_set_id"


def _clean_orphans(conn, schema: str) -> None:
  table = "public.facts" if schema == "public" else f'"{schema}".facts'
  fact_sets = "public.fact_sets" if schema == "public" else f'"{schema}".fact_sets'
  conn.execute(
    text(
      f"""
      UPDATE {table}
      SET fact_set_id = NULL
      WHERE fact_set_id IS NOT NULL
        AND fact_set_id NOT IN (SELECT id FROM {fact_sets})
      """
    )
  )


def _add_fk(conn, schema: str) -> None:
  table = "public.facts" if schema == "public" else f'"{schema}".facts'
  fact_sets = "public.fact_sets" if schema == "public" else f'"{schema}".fact_sets'
  conn.execute(text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {_FK_NAME}"))
  conn.execute(
    text(
      f"ALTER TABLE {table} ADD CONSTRAINT {_FK_NAME} "
      f"FOREIGN KEY (fact_set_id) REFERENCES {fact_sets} (id) "
      f"ON DELETE SET NULL"
    )
  )


def _drop_fk(conn, schema: str) -> None:
  table = "public.facts" if schema == "public" else f'"{schema}".facts'
  conn.execute(text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {_FK_NAME}"))


def upgrade() -> None:
  conn = op.get_bind()
  from migrations.extensions.helpers import for_each_tenant_schema

  _clean_orphans(conn, "public")
  for_each_tenant_schema(conn, _clean_orphans)

  _add_fk(conn, "public")
  for_each_tenant_schema(conn, _add_fk)


def downgrade() -> None:
  conn = op.get_bind()
  from migrations.extensions.helpers import for_each_tenant_schema

  _drop_fk(conn, "public")
  for_each_tenant_schema(conn, _drop_fk)
