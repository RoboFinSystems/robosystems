"""Entity axis on the ledger tables: expand.

A graph is becoming a reporting group with several entities in it, so the
ledger rows that were implicitly "the graph's" now say whose books they are
in. This is the expand half: the column, the backfill, and a per-schema
default. The contract half (NOT NULL, default dropped) ships in the next
release, once no task from before this one is still writing.

Per tenant schema:

- ``entity_id`` is added, nullable, to ``entries``, ``events`` and
  ``transactions``, and to ``structures`` (where it stays nullable: NULL is
  shared by the group, and only schedules and reconciliations carry one).
- Every existing row is backfilled to the group parent, the same rule
  ``entity_scope.resolve_entity_id`` applies: the earliest ``is_parent`` row
  that is not a linked counterparty.
- The three ledger tables get that parent as a column DEFAULT. Tasks from the
  previous release keep inserting without the column while this deploys, and
  the default lands their rows on the only entity the graph has.

A schema with ledger rows and no entity to anchor them to stops the migration
with its name, rather than leaving rows no entity-scoped read will ever
return. A schema with neither is left alone.

Fresh tenants get the columns from the model, NOT NULL from the start.

Revision ID: 0038
Revises: 0037
Create Date: 2026-10-06

"""

import re

from alembic import op
from sqlalchemy import text
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

revision = "0038"
down_revision = "0037"
branch_labels = None
depends_on = None

# Tables whose every row belongs to one entity, with the index each gets.
LEDGER_TABLES: dict[str, tuple[str, list[str]]] = {
  "entries": ("idx_entries_entity_posting_date", ["entity_id", "posting_date"]),
  "events": ("idx_events_entity", ["entity_id"]),
  "transactions": ("idx_transactions_entity", ["entity_id"]),
}
STRUCTURES_INDEX = "idx_structures_entity"
ENTITY_BLOCK_TYPES = ("schedule", "reconciliation")

_SAFE_ID = re.compile(r"^[A-Za-z0-9_\-]+$")


def _add_columns(conn: Connection, schema: str) -> None:
  t = TenantOps(conn, schema)
  for table, (index, columns) in LEDGER_TABLES.items():
    t.add_column(table, "entity_id", "VARCHAR")
    t.create_index(index, table, columns)
  t.add_column("structures", "entity_id", "VARCHAR")
  t.create_index(
    STRUCTURES_INDEX, "structures", ["entity_id"], where="entity_id IS NOT NULL"
  )


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


def _unanchored_rows(conn: Connection, schema: str) -> dict[str, int]:
  counts = {
    table: conn.execute(text(f'SELECT count(*) FROM "{schema}".{table}')).scalar_one()
    for table in LEDGER_TABLES
  }
  return {table: count for table, count in counts.items() if count}


def _backfill(conn: Connection, schema: str) -> None:
  parent_id = _group_parent(conn, schema)
  if parent_id is None:
    unanchored = _unanchored_rows(conn, schema)
    if unanchored:
      raise RuntimeError(
        f"{schema}: ledger rows {unanchored} and no group parent entity to "
        "anchor them to (no is_parent row that is not linked). Give the "
        "schema its entity, then re-run."
      )
    return
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
        f'ALTER TABLE "{schema}".{table} '
        f"ALTER COLUMN entity_id SET DEFAULT '{parent_id}'"
      )
    )
  block_types = ", ".join(f"'{block_type}'" for block_type in ENTITY_BLOCK_TYPES)
  conn.execute(
    text(
      f"""
      UPDATE "{schema}".structures SET entity_id = :parent
      WHERE entity_id IS NULL AND block_type IN ({block_types})
      """
    ),
    {"parent": parent_id},
  )


def _expand(conn: Connection, schema: str) -> None:
  _add_columns(conn, schema)
  _backfill(conn, schema)


def _revert(conn: Connection, schema: str) -> None:
  t = TenantOps(conn, schema)
  for table, (index, _columns) in LEDGER_TABLES.items():
    t.drop_index(index)
    t.drop_column(table, "entity_id")
  t.drop_index(STRUCTURES_INDEX)
  t.drop_column("structures", "entity_id")


def upgrade() -> None:
  conn = op.get_bind()
  # public holds the library and no ledger rows: the columns only.
  _add_columns(conn, "public")
  for_each_tenant_schema(conn, _expand)


def downgrade() -> None:
  conn = op.get_bind()
  _revert(conn, "public")
  for_each_tenant_schema(conn, _revert)
