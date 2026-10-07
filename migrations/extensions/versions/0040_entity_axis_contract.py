"""Entity axis on the ledger tables: contract.

The expand half (0038) added ``entity_id`` nullable with the group parent as
each tenant's column DEFAULT, so tasks from the release before it kept
writing through the deploy. Every writer of the release that carried 0038
stamps the entity itself; only a task from before it would still write
through the default, and none serves. So the model's shape reaches every
existing tenant:

- ``entity_id`` becomes NOT NULL on ``entries``, ``events``,
  ``transactions``, ``fiscal_calendar`` and ``fiscal_periods``, and every
  per-schema DEFAULT goes, ``structures`` included: a writer that names no
  entity now fails at the database instead of landing on the parent.
- ``structures`` rows that are neither schedules nor reconciliations get
  ``entity_id`` cleared: a shared block made between the two releases was
  stamped with the parent by the default, and no read looks at the column
  for those block types.

Pre-flights, per tenant, each stopping the migration with the schema's name:
no ledger row may still be NULL, and a schema with books must have exactly
one group parent. A schema a task of the previous release provisioned while
0038 ran has no column at all; it is expanded first, with 0038's own
function, then contracted like the rest.

``SET NOT NULL`` takes an ACCESS EXCLUSIVE lock and would scan the table
under it; each table first gets a ``CHECK (entity_id IS NOT NULL)``
constraint, added ``NOT VALID`` and then validated under a lock that lets
reads and writes through, so Postgres skips that scan. The check is dropped
once the column is NOT NULL.

The downgrade relaxes the five back to nullable and restores the per-schema
defaults; it does not restore the stamp on shared blocks.

Revision ID: 0040
Revises: 0039
Create Date: 2026-10-06

"""

import importlib.util
import re
from pathlib import Path

from alembic import op
from sqlalchemy import text
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

revision = "0040"
down_revision = "0039"
branch_labels = None
depends_on = None

LEDGER_TABLES = (
  "entries",
  "events",
  "transactions",
  "fiscal_calendar",
  "fiscal_periods",
)
DEFAULTED_TABLES = (*LEDGER_TABLES, "structures")
ENTITY_BLOCK_TYPES = ("schedule", "reconciliation")

_SAFE_ID = re.compile(r"^[A-Za-z0-9_\-]+$")
_BLOCK_TYPES_SQL = ", ".join(f"'{block_type}'" for block_type in ENTITY_BLOCK_TYPES)


def _expand_module():
  """0038, loaded by file: its per-schema expand is what a straggler needs."""
  path = Path(__file__).with_name("0038_entity_axis_expand.py")
  spec = importlib.util.spec_from_file_location("migration_0038_for_0040", path)
  assert spec is not None and spec.loader is not None
  module = importlib.util.module_from_spec(spec)
  spec.loader.exec_module(module)
  return module


def _has_entity_column(conn: Connection, schema: str, table: str) -> bool:
  return (
    conn.execute(
      text(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_schema = :schema AND table_name = :table "
        "AND column_name = 'entity_id'"
      ),
      {"schema": schema, "table": table},
    ).first()
    is not None
  )


def _tables_with_null_rows(conn: Connection, schema: str) -> list[str]:
  """Stops at the first NULL of each table rather than counting them."""
  return [
    table
    for table in LEDGER_TABLES
    if conn.execute(
      text(f'SELECT EXISTS (SELECT 1 FROM "{schema}".{table} WHERE entity_id IS NULL)')
    ).scalar_one()
  ]


def _has_books(conn: Connection, schema: str) -> bool:
  for table in LEDGER_TABLES:
    if conn.execute(text(f'SELECT 1 FROM "{schema}".{table} LIMIT 1')).first():
      return True
  return False


def _group_parents(conn: Connection, schema: str) -> list[str]:
  return list(
    conn.execute(
      text(
        f'SELECT id FROM "{schema}".entities '
        "WHERE is_parent = true AND source <> 'linked' ORDER BY created_at ASC"
      )
    ).scalars()
  )


def _preflight(conn: Connection, schema: str) -> None:
  nulls = _tables_with_null_rows(conn, schema)
  if nulls:
    raise RuntimeError(
      f"{schema}: entity_id is NULL on {nulls}; stamp those rows with their "
      "entity, then re-run."
    )
  parents = _group_parents(conn, schema)
  if _has_books(conn, schema) and len(parents) != 1:
    raise RuntimeError(
      f"{schema}: books and {len(parents)} group parents (is_parent, not "
      "linked); exactly one is needed. Mark the parent, then re-run."
    )


def _contract(conn: Connection, schema: str) -> None:
  if not _has_entity_column(conn, schema, "entries"):
    _expand_module()._expand(conn, schema)
  _preflight(conn, schema)
  conn.execute(
    text(
      f'UPDATE "{schema}".structures SET entity_id = NULL '
      f"WHERE entity_id IS NOT NULL AND block_type NOT IN ({_BLOCK_TYPES_SQL})"
    )
  )
  for table in DEFAULTED_TABLES:
    conn.execute(
      text(f'ALTER TABLE "{schema}".{table} ALTER COLUMN entity_id DROP DEFAULT')
    )
  for table in LEDGER_TABLES:
    _set_not_null(conn, schema, table)


def _set_not_null(conn: Connection, schema: str, table: str) -> None:
  """NOT NULL without a scan under the ACCESS EXCLUSIVE lock: a validated
  CHECK proves the column first (Postgres 12+)."""
  check = f"ck_{table}_entity_id_not_null"
  qualified = f'"{schema}".{table}'
  conn.execute(text(f"ALTER TABLE {qualified} DROP CONSTRAINT IF EXISTS {check}"))
  conn.execute(
    text(
      f"ALTER TABLE {qualified} ADD CONSTRAINT {check} "
      "CHECK (entity_id IS NOT NULL) NOT VALID"
    )
  )
  conn.execute(text(f"ALTER TABLE {qualified} VALIDATE CONSTRAINT {check}"))
  TenantOps(conn, schema).alter_column_nullable(table, "entity_id", nullable=False)
  conn.execute(text(f"ALTER TABLE {qualified} DROP CONSTRAINT {check}"))


def _relax(conn: Connection, schema: str) -> None:
  t = TenantOps(conn, schema)
  for table in LEDGER_TABLES:
    t.alter_column_nullable(table, "entity_id", nullable=True)
  parents = _group_parents(conn, schema)
  if not parents:
    return
  parent_id = parents[0]
  if not _SAFE_ID.match(parent_id):
    raise RuntimeError(f"{schema}: unexpected entity id {parent_id!r}")
  for table in DEFAULTED_TABLES:
    t.alter_column_default(table, "entity_id", f"'{parent_id}'")


def upgrade() -> None:
  conn = op.get_bind()
  # public holds the library and no ledger rows: the shape only.
  for table in LEDGER_TABLES:
    _set_not_null(conn, "public", table)
  for_each_tenant_schema(conn, _contract)


def downgrade() -> None:
  conn = op.get_bind()
  for table in LEDGER_TABLES:
    TenantOps(conn, "public").alter_column_nullable(table, "entity_id", nullable=True)
  for_each_tenant_schema(conn, _relax)
