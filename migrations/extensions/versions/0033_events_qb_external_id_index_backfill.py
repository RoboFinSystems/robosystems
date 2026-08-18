"""events qb_external_id index for tenants provisioned after 0014

0014 fanned ``idx_{schema}_events_qb_external_id`` — the partial expression
index on ``metadata->>'qb_external_id'`` that the QuickBooks writeback marker
lookups filter on — out to every tenant that existed when it ran. It never
reached the ``Event`` model, so every tenant provisioned by ``create_all``
since then has no such index and each marker lookup in the loader and the
close's writeback is a sequential scan of ``events``.

The model now declares the index as ``idx_events_qb_external_id``. This
migration creates it in every tenant that holds neither name — the 0014
tenants keep their schema-prefixed copy (same definition; a second one would
only cost writes), and tenants provisioned from here on get it from the model.
Public gets it too, so the template matches the model.

Revision ID: 0033
Revises: 0032
Create Date: 2026-08-18

"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import for_each_tenant_schema

revision = "0033"
down_revision = "0032"
branch_labels = None
depends_on = None

_INDEX = "idx_events_qb_external_id"


def _legacy_index_exists(conn: Connection, schema: str) -> bool:
  return (
    conn.execute(
      text("SELECT 1 FROM pg_indexes WHERE schemaname = :schema AND indexname = :name"),
      {"schema": schema, "name": f"idx_{schema}_events_qb_external_id"},
    ).first()
    is not None
  )


def _create(conn: Connection, schema: str) -> None:
  if schema != "public" and _legacy_index_exists(conn, schema):
    return
  conn.execute(
    text(
      f'CREATE INDEX IF NOT EXISTS "{_INDEX}" '
      f"ON \"{schema}\".events ((metadata->>'qb_external_id')) "
      "WHERE metadata->>'qb_external_id' IS NOT NULL"
    )
  )


def _drop(conn: Connection, schema: str) -> None:
  conn.execute(text(f'DROP INDEX IF EXISTS "{schema}"."{_INDEX}"'))


def upgrade() -> None:
  conn = op.get_bind()
  _create(conn, "public")
  for_each_tenant_schema(conn, _create)


def downgrade() -> None:
  conn = op.get_bind()
  _drop(conn, "public")
  for_each_tenant_schema(conn, _drop)
