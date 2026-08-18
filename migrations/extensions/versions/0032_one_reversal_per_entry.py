"""an entry is reversed at most once

``reverse_journal_entry`` read the original entry unlocked, checked that its
status was ``posted``, and then wrote a full reversing entry plus flipped line
items. Two concurrent reversals of the same entry both passed that check and
both posted — the entry got reversed twice. Each reversing entry is internally
balanced, so nothing failed to foot and no validation complained; the books
simply sat one entry's worth off in the other direction.

The command now locks the original, which turns the ordinary race into a clean
409. This index is the half that does not depend on the code path: two
reversing entries against one original is wrong however it arose — a
double-clicked button, a retried request, a future caller that forgets to
lock — and the database is the only place that can say so unconditionally.

Partial, because ``reversal_of`` is NULL on every entry that is not a reversal
and those must stay unconstrained.

**Before deploying**: this will fail if any original already has two reversals.
That failure is the correct outcome — it means real double-reversed entries are
sitting in the ledger and need an accounting decision, not a schema decision.
Check first:

    SELECT reversal_of, COUNT(*) FROM entries
    WHERE reversal_of IS NOT NULL GROUP BY reversal_of HAVING COUNT(*) > 1;

per tenant schema, and resolve any rows it returns before running this.

Revision ID: 0032
Revises: 0031
Create Date: 2026-08-17

"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import for_each_tenant_schema

revision = "0032"
down_revision = "0031"
branch_labels = None
depends_on = None

_INDEX = "uq_entries_one_reversal_per_original"


def _create(conn: Connection, schema: str) -> None:
  conn.execute(
    text(
      f'CREATE UNIQUE INDEX IF NOT EXISTS "{_INDEX}" '
      f'ON "{schema}".entries (reversal_of) WHERE reversal_of IS NOT NULL'
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
