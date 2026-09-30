"""Anchor each chart mapping to its own mapping taxonomy.

A ``coa_mapping`` structure was anchored to the chart it maps from, so
nothing recorded the framework it maps into, and every reader took the first
mapping it found. Each one now hangs off a ``mapping`` taxonomy whose
``source_taxonomy_id`` is the chart and whose ``target_taxonomy_id`` is the
framework's ``reporting_standard`` taxonomy; the chart owns it.

Per tenant schema, for every mapping structure still anchored to a chart:

- target = the reporting_standard taxonomy its arcs point into most often,
  or rs-gaap's when it has no arcs yet (the only framework mapped so far)
- insert the mapping taxonomy, named after the structure
- re-point the structure at it (its id is unchanged, so ``reports.mapping_id``
  and the graph are untouched)

Then the unique index: one active mapping per (chart, framework). Created
after the data step so a tenant holding two mappings into one framework fails
the migration loudly instead of being guessed at.

Revision ID: 0037
Revises: 0036
Create Date: 2026-09-30

"""

from datetime import UTC, datetime

from alembic import op
from sqlalchemy import text
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import for_each_tenant_schema
from robosystems.utils.ulid import generate_prefixed_ulid

revision = "0037"
down_revision = "0036"
branch_labels = None
depends_on = None

INDEX_NAME = "uq_taxonomies_mapping_source_target"
DEFAULT_FRAMEWORK = "rs-gaap"


def _anchor(conn: Connection, schema: str) -> None:
  mappings = conn.execute(
    text(
      f"""
      SELECT s.id, s.name, s.description, s.created_by, s.taxonomy_id AS chart_id,
        (
          SELECT e.taxonomy_id
          FROM "{schema}".associations a
          JOIN "{schema}".elements e ON e.id = a.to_element_id
          JOIN "{schema}".taxonomies t ON t.id = e.taxonomy_id
          WHERE a.structure_id = s.id AND t.taxonomy_type = 'reporting_standard'
          GROUP BY e.taxonomy_id
          ORDER BY count(*) DESC
          LIMIT 1
        ) AS arc_target_id
      FROM "{schema}".structures s
      JOIN "{schema}".taxonomies c ON c.id = s.taxonomy_id
      WHERE s.block_type = 'coa_mapping' AND c.taxonomy_type = 'chart_of_accounts'
      """
    )
  ).fetchall()
  if not mappings:
    return

  default_target_id = conn.execute(
    text(
      f"""
      SELECT id FROM "{schema}".taxonomies
      WHERE standard = :framework AND taxonomy_type = 'reporting_standard'
      ORDER BY version DESC LIMIT 1
      """
    ),
    {"framework": DEFAULT_FRAMEWORK},
  ).scalar_one_or_none()

  now = datetime.now(UTC).replace(tzinfo=None)
  for row in mappings:
    target_id = row.arc_target_id or default_target_id
    if target_id is None:
      raise RuntimeError(
        f"{schema}: mapping {row.id} has no arcs and the schema has no "
        f"{DEFAULT_FRAMEWORK} reporting_standard taxonomy to target."
      )
    mapping_taxonomy_id = generate_prefixed_ulid("tax")
    conn.execute(
      text(
        f"""
        INSERT INTO "{schema}".taxonomies (
          id, name, description, taxonomy_type, source_taxonomy_id,
          target_taxonomy_id, is_shared, is_active, is_locked, metadata,
          created_at, updated_at, created_by
        ) VALUES (
          :id, :name, :description, 'mapping', :chart_id, :target_id,
          false, true, false, '{{}}'::jsonb, :now, :now, :created_by
        )
        """
      ),
      {
        "id": mapping_taxonomy_id,
        "name": row.name,
        "description": row.description,
        "chart_id": row.chart_id,
        "target_id": target_id,
        "now": now,
        "created_by": row.created_by,
      },
    )
    conn.execute(
      text(f'UPDATE "{schema}".structures SET taxonomy_id = :tid WHERE id = :sid'),
      {"tid": mapping_taxonomy_id, "sid": row.id},
    )


def _create_index(conn: Connection, schema: str) -> None:
  conn.execute(
    text(
      f'CREATE UNIQUE INDEX IF NOT EXISTS "{INDEX_NAME}" '
      f'ON "{schema}".taxonomies (source_taxonomy_id, target_taxonomy_id) '
      "WHERE taxonomy_type = 'mapping' AND source_taxonomy_id IS NOT NULL "
      "AND is_active"
    )
  )


def _upgrade(conn: Connection, schema: str) -> None:
  _anchor(conn, schema)
  _create_index(conn, schema)


def _downgrade(conn: Connection, schema: str) -> None:
  conn.execute(text(f'DROP INDEX IF EXISTS "{schema}"."{INDEX_NAME}"'))
  conn.execute(
    text(
      f"""
      UPDATE "{schema}".structures s
      SET taxonomy_id = m.source_taxonomy_id
      FROM "{schema}".taxonomies m
      WHERE s.taxonomy_id = m.id
        AND s.block_type = 'coa_mapping'
        AND m.taxonomy_type = 'mapping'
        AND m.source_taxonomy_id IS NOT NULL
      """
    )
  )
  conn.execute(
    text(
      f"""
      DELETE FROM "{schema}".taxonomies
      WHERE taxonomy_type = 'mapping' AND source_taxonomy_id IS NOT NULL
      """
    )
  )


def upgrade() -> None:
  conn = op.get_bind()
  _upgrade(conn, "public")
  for_each_tenant_schema(conn, _upgrade)


def downgrade() -> None:
  conn = op.get_bind()
  _downgrade(conn, "public")
  for_each_tenant_schema(conn, _downgrade)
