"""One primary closing entry per (schedule, period).

A schedule posts one closing entry per period. Until now that was enforced
only by a `SELECT ... LIMIT 1` in `create_closing_entry` with no lock and no
constraint behind it, so it held only because one caller reaches it under the
obligation row lock. A second writer arriving on a *different* event row — an
operator or MCP close co-pilot firing `schedule_entry_due` while the Dagster
sweep is mid-dispatch — passes the same check and inserts the twin. Close then
posts both, and the reconcile cannot repair it afterwards: its
`ORDER BY created_at DESC LIMIT 1` only ever sees one of the pair.

`posting_date` stands for the period: obligations are minted with
`posting_date = period_end` and the handler passes that through, so both
racers derive the same date. Two real columns rather than an expression index,
so "fiscal periods are calendar months" is not frozen into a migration.

`reversal_of IS NULL` excludes the auto-reversal, which legitimately shares the
schedule and posts on the next period's first day. Keyed on the reversal link
and not on entry type, because `entry_type` is caller-authored and may itself
be "reversing".

`source_structure_id IS NOT NULL` leaves manual and ad-hoc entries alone;
`create_manual_closing_entry` writes NULL there, so a manual adjustment can
never collide with a scheduled one.

Verified zero conflicts across every tenant before writing this. If a future
environment does hold a conflict, the index creation fails loudly with the
offending key — which is the right outcome. To find them first:

    SELECT source_structure_id, posting_date, count(*)
    FROM <schema>.entries
    WHERE source_structure_id IS NOT NULL AND reversal_of IS NULL
    GROUP BY 1, 2 HAVING count(*) > 1;

Revision ID: 0036
Revises: 0035
Create Date: 2026-09-14

"""

from alembic import op
from sqlalchemy import text
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import for_each_tenant_schema

revision = "0036"
down_revision = "0035"
branch_labels = None
depends_on = None

INDEX_NAME = "uq_entries_one_primary_per_schedule_period"


def _create(conn: Connection, schema: str) -> None:
  conn.execute(
    text(
      f'CREATE UNIQUE INDEX IF NOT EXISTS "{INDEX_NAME}" '
      f'ON "{schema}".entries (source_structure_id, posting_date) '
      "WHERE source_structure_id IS NOT NULL AND reversal_of IS NULL"
    )
  )


def _drop(conn: Connection, schema: str) -> None:
  conn.execute(text(f'DROP INDEX IF EXISTS "{schema}"."{INDEX_NAME}"'))


def upgrade() -> None:
  conn = op.get_bind()
  _create(conn, "public")
  for_each_tenant_schema(conn, _create)


def downgrade() -> None:
  conn = op.get_bind()
  _drop(conn, "public")
  for_each_tenant_schema(conn, _drop)
