"""An ``opening`` entry type: the balance sheet a ledger starts from.

Books whose history does not come with them start from a balance sheet. It is
one entry per entity, written only by ``record-opening-balance``, dated the
eve of the entity's first fiscal period; the statements read it as a
balance, never as activity (spec ``opening-balances`` §2 and §3). The CHECK on
``entries.type`` gains the value in ``public`` and in every tenant.

Autogenerate does not see a CHECK change, so the constraint is replaced by
hand. A partial unique index holds the entity to one opening entry, whatever
writes it. Downgrade restores the four types; it fails while any opening
entry exists, which is the honest outcome, and the migration runs in one
transaction, so no tenant is left narrowed.

Revision ID: 0054
Revises: 0053
Create Date: 2026-10-18
"""

from __future__ import annotations

from alembic import op
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

revision = "0054"
down_revision = "0053"
branch_labels = None
depends_on = None

_NAME = "check_entry_type"
_ONE_PER_ENTITY = "uq_entries_one_opening_per_entity"
_WITH_OPENING = "type IN ('standard', 'adjusting', 'closing', 'reversing', 'opening')"
_WITHOUT = "type IN ('standard', 'adjusting', 'closing', 'reversing')"


def _widen(conn: Connection, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.add_check("entries", _NAME, _WITH_OPENING)
  t.create_index(
    _ONE_PER_ENTITY, "entries", ["entity_id"], unique=True, where="type = 'opening'"
  )


def _narrow(conn: Connection, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.drop_index(_ONE_PER_ENTITY)
  t.add_check("entries", _NAME, _WITHOUT)


def upgrade() -> None:
  conn = op.get_bind()
  _widen(conn, "public")
  for_each_tenant_schema(conn, _widen)


def downgrade() -> None:
  conn = op.get_bind()
  _narrow(conn, "public")
  for_each_tenant_schema(conn, _narrow)
