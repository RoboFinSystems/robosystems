"""backfill scenario dimensions onto existing forecast facts

Pure derivation, no value changes (scenario-dimension P1 backfill): every
forecast fact already reaches its scenario by one join
(``facts.fact_set_id → fact_sets.scenario_id``), so this materializes
that relationship as Fact-level dimension rows:

1. One ``scenario`` Dimension per distinct ``scenario_id``, named from
   the owning forecast Structure. Deterministic ids (``dim_scn_`` +
   md5 of the scenario id) make re-runs idempotent alongside the
   ``(dimension_type, value)`` unique constraint.
2. One ``fact_dimensions`` row per fact in a scenario-keyed FactSet.

Actuals (``scenario_id IS NULL``) get no rows — the default-member rule.
Reversible: the downgrade removes exactly the scenario-typed junction
rows and dimensions this created.

Revision ID: 0027
Revises: 0026
Create Date: 2026-08-06

"""

from __future__ import annotations

from alembic import op

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0027"
down_revision = "0026"
branch_labels = None
depends_on = None


def _backfill(conn, schema: str) -> None:
  ops = TenantOps(conn, schema)
  ops.execute("""
    INSERT INTO {s}.dimensions
      (id, dimension_type, name, value, metadata, is_active, created_at, updated_at)
    SELECT
      'dim_scn_' || md5(sc.scenario_id),
      'scenario',
      COALESCE(st.name, sc.scenario_id),
      sc.scenario_id,
      '{}'::jsonb,
      true,
      now(),
      now()
    FROM (
      SELECT DISTINCT scenario_id FROM {s}.fact_sets WHERE scenario_id IS NOT NULL
    ) sc
    LEFT JOIN {s}.structures st ON st.id = sc.scenario_id
    ON CONFLICT (dimension_type, value) DO NOTHING
  """)
  ops.execute("""
    INSERT INTO {s}.fact_dimensions (fact_id, dimension_id)
    SELECT f.id, d.id
    FROM {s}.facts f
    JOIN {s}.fact_sets fs
      ON fs.id = f.fact_set_id AND fs.scenario_id IS NOT NULL
    JOIN {s}.dimensions d
      ON d.dimension_type = 'scenario' AND d.value = fs.scenario_id
    ON CONFLICT DO NOTHING
  """)


def _unwind(conn, schema: str) -> None:
  ops = TenantOps(conn, schema)
  ops.execute("""
    DELETE FROM {s}.fact_dimensions fd
    USING {s}.dimensions d
    WHERE d.id = fd.dimension_id AND d.dimension_type = 'scenario'
  """)
  ops.execute("DELETE FROM {s}.dimensions WHERE dimension_type = 'scenario'")


def upgrade() -> None:
  conn = op.get_bind()
  _backfill(conn, "public")
  for_each_tenant_schema(conn, _backfill)


def downgrade() -> None:
  conn = op.get_bind()
  _unwind(conn, "public")
  for_each_tenant_schema(conn, _unwind)
