"""Entity axis on the ledger tables: expand.

A graph is becoming a reporting group with several entities in it, so the
ledger rows that were implicitly "the graph's" now say whose books they are
in, and each entity keeps its own fiscal calendar. This is the expand half:
the columns, the backfill, and a per-schema default. The contract half (NOT
NULL, defaults dropped) ships in the next release, once no task from before
this one is still writing.

Per tenant schema:

- ``entity_id`` is added, nullable, to ``entries``, ``events``,
  ``transactions``, ``fiscal_calendar`` and ``fiscal_periods``, and to
  ``structures``, where only schedules and reconciliations carry one.
- The calendar and its periods become one set per entity: unique on
  ``(graph_id, entity_id)`` and ``(graph_id, entity_id, name)``, replacing
  the per-graph constraints. A one-entity schema satisfies both, so the swap
  moves no row.
- Every existing row is backfilled to the group parent, the same rule
  ``entity_scope.resolve_entity_id`` applies: the earliest ``is_parent`` row
  that is not a linked counterparty.
- Each column gets that parent as its DEFAULT. Tasks from the previous
  release keep inserting without the column while this deploys, and the
  default lands their rows on the only entity the graph has. On
  ``structures`` that also stamps the few shared blocks created before the
  contract release, which clears them again; no read looks at the column
  for those block types.

A schema with books and no entity (a graph created with
``create_entity=false``) gets one: ``entity_<schema>``, the id the graph
materializer has always assumed for it, named after the schema until someone
renames it. Books with entities but no group parent among them are not a
state this can repair, and stop the migration with the schema's name.

Fresh tenants get the columns from the model, NOT NULL from the start.

Revision ID: 0038
Revises: 0037
Create Date: 2026-10-06

"""

import re
from datetime import UTC, datetime

from alembic import op
from sqlalchemy import text
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema
from robosystems.config.constants import ReportingStyleConstants

revision = "0038"
down_revision = "0037"
branch_labels = None
depends_on = None

# Tables whose every row belongs to one entity, with the index each gets.
LEDGER_TABLES: dict[str, tuple[str, list[str]] | None] = {
  "entries": ("idx_entries_entity_posting_date", ["entity_id", "posting_date"]),
  "events": ("idx_events_entity", ["entity_id"]),
  "transactions": ("idx_transactions_entity", ["entity_id"]),
  "fiscal_calendar": None,
  "fiscal_periods": None,
}
# table -> (per-graph constraint it had, per-entity constraint, columns)
UNIQUE_SWAPS: dict[str, tuple[str, str, list[str]]] = {
  "fiscal_calendar": (
    "uq_fiscal_calendar_graph",
    "uq_fiscal_calendar_graph_entity",
    ["graph_id", "entity_id"],
  ),
  "fiscal_periods": (
    "uq_fiscal_period_graph_name",
    "uq_fiscal_period_graph_entity_name",
    ["graph_id", "entity_id", "name"],
  ),
}
PRIOR_UNIQUE_COLUMNS: dict[str, list[str]] = {
  "fiscal_calendar": ["graph_id"],
  "fiscal_periods": ["graph_id", "name"],
}
STRUCTURES_INDEX = "idx_structures_entity"
ENTITY_BLOCK_TYPES = ("schedule", "reconciliation")
PLACEHOLDER_STYLE_ID = ReportingStyleConstants.DEFAULT_STYLE_ID

_SAFE_ID = re.compile(r"^[A-Za-z0-9_\-]+$")
_BLOCK_TYPES_SQL = ", ".join(f"'{block_type}'" for block_type in ENTITY_BLOCK_TYPES)


def _add_columns(conn: Connection, schema: str) -> None:
  t = TenantOps(conn, schema)
  for table, index in LEDGER_TABLES.items():
    t.add_column(table, "entity_id", "VARCHAR")
    if index is not None:
      t.create_index(index[0], table, index[1])
  t.add_column("structures", "entity_id", "VARCHAR")
  t.create_index(
    STRUCTURES_INDEX, "structures", ["entity_id"], where="entity_id IS NOT NULL"
  )
  for table, (prior, per_entity, columns) in UNIQUE_SWAPS.items():
    t.add_unique(table, per_entity, columns)
    t.drop_constraint(table, prior)


def _group_parent(conn: Connection, schema: str) -> str | None:
  return conn.execute(
    text(
      f"""
      SELECT id FROM "{schema}".entities
      WHERE is_parent = true AND source <> 'linked'
      ORDER BY created_at ASC LIMIT 1
      """
    )
  ).scalar_one_or_none()


def _books(conn: Connection, schema: str) -> dict[str, int]:
  """Row counts of what needs an entity, omitting the empty tables."""
  counts = {
    table: conn.execute(text(f'SELECT count(*) FROM "{schema}".{table}')).scalar_one()
    for table in LEDGER_TABLES
  }
  counts["structures"] = conn.execute(
    text(
      f'SELECT count(*) FROM "{schema}".structures '
      f"WHERE block_type IN ({_BLOCK_TYPES_SQL})"
    )
  ).scalar_one()
  return {table: count for table, count in counts.items() if count}


def _placeholder_parent(conn: Connection, schema: str, books: dict[str, int]) -> str:
  """Give a schema that has books and no entity the one the materializer
  assumed for it."""
  own_entities = conn.execute(
    text(f"SELECT count(*) FROM \"{schema}\".entities WHERE source <> 'linked'")
  ).scalar_one()
  if own_entities:
    raise RuntimeError(
      f"{schema}: books {books} and {own_entities} entities, none of them the "
      "group parent (is_parent). Mark the parent, then re-run."
    )
  entity_id = f"entity_{schema}"
  now = datetime.now(UTC).replace(tzinfo=None)
  conn.execute(
    text(
      f"""
      INSERT INTO "{schema}".entities (
        id, name, reporting_style_id, status, is_parent, source, metadata,
        version, created_at, updated_at, created_by
      ) VALUES (
        :id, :name, :style, 'active', true, 'native', '{{}}'::jsonb,
        1, :now, :now, 'migration-0038'
      )
      """
    ),
    {"id": entity_id, "name": schema, "style": PLACEHOLDER_STYLE_ID, "now": now},
  )
  return entity_id


def _backfill(conn: Connection, schema: str) -> None:
  parent_id = _group_parent(conn, schema)
  if parent_id is None:
    books = _books(conn, schema)
    if not books:
      return
    parent_id = _placeholder_parent(conn, schema, books)
  if not _SAFE_ID.match(parent_id):
    raise RuntimeError(f"{schema}: unexpected entity id {parent_id!r}")

  for table in LEDGER_TABLES:
    conn.execute(
      text(
        f'UPDATE "{schema}".{table} SET entity_id = :parent WHERE entity_id IS NULL'
      ),
      {"parent": parent_id},
    )
  conn.execute(
    text(
      f"""
      UPDATE "{schema}".structures SET entity_id = :parent
      WHERE entity_id IS NULL AND block_type IN ({_BLOCK_TYPES_SQL})
      """
    ),
    {"parent": parent_id},
  )
  for table in (*LEDGER_TABLES, "structures"):
    conn.execute(
      text(
        f'ALTER TABLE "{schema}".{table} '
        f"ALTER COLUMN entity_id SET DEFAULT '{parent_id}'"
      )
    )


def _expand(conn: Connection, schema: str) -> None:
  _add_columns(conn, schema)
  _backfill(conn, schema)


def _revert(conn: Connection, schema: str) -> None:
  t = TenantOps(conn, schema)
  for table, (prior, per_entity, _columns) in UNIQUE_SWAPS.items():
    t.add_unique(table, prior, PRIOR_UNIQUE_COLUMNS[table])
    t.drop_constraint(table, per_entity)
  for table, index in LEDGER_TABLES.items():
    if index is not None:
      t.drop_index(index[0])
    t.drop_column(table, "entity_id")
  t.drop_index(STRUCTURES_INDEX)
  t.drop_column("structures", "entity_id")


def upgrade() -> None:
  conn = op.get_bind()
  # public holds the library and no ledger rows: the structure only.
  _add_columns(conn, "public")
  for_each_tenant_schema(conn, _expand)


def downgrade() -> None:
  conn = op.get_bind()
  _revert(conn, "public")
  for_each_tenant_schema(conn, _revert)
