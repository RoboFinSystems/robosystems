"""Drop facts.report_id (§3.5 / §6.5 final retire).

Completes the FactSets migration started in 0009. With the §3.5 rewrite
of ``create_report`` / ``regenerate_report`` / ``create_schedule``, every
new ``facts`` row is stamped with both ``structure_id`` and
``fact_set_id`` (and the parent ``FactSet`` carries the back-pointer to
``report_id``), making the ``facts.report_id`` column redundant.

Steps per schema (public + every tenant):

1. DELETE any orphan rows where ``fact_set_id IS NULL``. These are
   pre-§3.5 facts that 0009 already null-ed (because their fact_set_id
   didn't resolve) or facts written before ``_persist_report_facts``
   started stamping ``fact_set_id``. Posture matches 0009 — Reporting
   Style hasn't shipped to prod, blast radius is small. Reports can be
   regenerated via ``create_report`` / ``regenerate_report``.
2. Drop the legacy ``check_fact_has_parent`` CHECK constraint.
3. Drop the ``idx_facts_report`` index.
4. Drop the ``report_id`` column itself.
5. Promote ``fact_set_id`` to ``NOT NULL`` — it's now the sole parent
   pointer.
6. Swap the ``fk_facts_fact_set_id`` FK from ``ON DELETE SET NULL`` to
   ``ON DELETE CASCADE``. SET NULL contradicts NOT NULL; CASCADE matches
   the new invariant that every fact has exactly one parent FactSet.

Revision ID: 0010
Revises: 0009
Create Date: 2026-05-13
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


_FK_NAME = "fk_facts_fact_set_id"


def _drop_report_id(conn, schema: str) -> None:
  table = "public.facts" if schema == "public" else f'"{schema}".facts'
  fact_sets = "public.fact_sets" if schema == "public" else f'"{schema}".fact_sets'
  conn.execute(text(f"DELETE FROM {table} WHERE fact_set_id IS NULL"))
  conn.execute(
    text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS check_fact_has_parent")
  )
  if schema == "public":
    conn.execute(text("DROP INDEX IF EXISTS idx_facts_report"))
  else:
    conn.execute(text(f'DROP INDEX IF EXISTS "{schema}".idx_facts_report'))
  conn.execute(text(f"ALTER TABLE {table} DROP COLUMN IF EXISTS report_id"))
  conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN fact_set_id SET NOT NULL"))
  # Swap FK: SET NULL → CASCADE (matches new NOT NULL invariant).
  conn.execute(text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {_FK_NAME}"))
  conn.execute(
    text(
      f"ALTER TABLE {table} ADD CONSTRAINT {_FK_NAME} "
      f"FOREIGN KEY (fact_set_id) REFERENCES {fact_sets} (id) "
      f"ON DELETE CASCADE"
    )
  )


def _restore_report_id(conn, schema: str) -> None:
  table = "public.facts" if schema == "public" else f'"{schema}".facts'
  fact_sets = "public.fact_sets" if schema == "public" else f'"{schema}".fact_sets'
  # Revert FK to ON DELETE SET NULL before relaxing NOT NULL.
  conn.execute(text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {_FK_NAME}"))
  conn.execute(
    text(
      f"ALTER TABLE {table} ADD CONSTRAINT {_FK_NAME} "
      f"FOREIGN KEY (fact_set_id) REFERENCES {fact_sets} (id) "
      f"ON DELETE SET NULL"
    )
  )
  conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN fact_set_id DROP NOT NULL"))
  conn.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS report_id VARCHAR"))
  if schema == "public":
    conn.execute(
      text("CREATE INDEX IF NOT EXISTS idx_facts_report ON public.facts (report_id)")
    )
  else:
    conn.execute(
      text(
        f'CREATE INDEX IF NOT EXISTS idx_facts_report ON "{schema}".facts (report_id)'
      )
    )
  conn.execute(
    text(
      f"ALTER TABLE {table} ADD CONSTRAINT check_fact_has_parent "
      "CHECK (report_id IS NOT NULL OR fact_set_id IS NOT NULL)"
    )
  )


def upgrade() -> None:
  conn = op.get_bind()
  from migrations.extensions.helpers import for_each_tenant_schema

  _drop_report_id(conn, "public")
  for_each_tenant_schema(conn, _drop_report_id)


def downgrade() -> None:
  conn = op.get_bind()
  from migrations.extensions.helpers import for_each_tenant_schema

  _restore_report_id(conn, "public")
  for_each_tenant_schema(conn, _restore_report_id)
