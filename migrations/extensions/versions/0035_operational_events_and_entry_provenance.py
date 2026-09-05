"""operational event class, and repair Entry.provenance

Two related corrections to origin-tracking on the ledger spine, both
fanned out over every tenant schema.

**1. `event_class = 'operational'`.** The class vocabulary was
`economic | support`, where economic asserts a resource flow and support
means *supporting an economic event* (controls, approvals,
reconciliations, inquiries). A business occurrence that is neither had
nowhere honest to go: `schedule_created` — setting up a schedule, which
moves nothing and posts nothing — was filed as `economic/other`, and a
CRM lead had no legal category under either class. Adds a third class
with `pipeline | engagement | schedule | other`, and moves the existing
`schedule_created` rows onto it. The recognition events a schedule later
emits stay `economic/recognition`; those do drive the GL.

**2. `Entry.provenance` repair.** `create_journal_entry` hardcoded
`manual_entry` regardless of origin, so every entry that arrived over a
connection claimed to be hand-entered and `source_sync` — a value the
CHECK constraint allows — had never been written by anything. The write
already receives the origin (`body.source`, which the event handler sets
from `Event.source`); it was simply discarded. The code now derives it,
and this backfills the rows already written by reading each entry's
linked event.

The backfill is conservative and idempotent: it only touches entries
whose provenance is exactly `manual_entry`, only where a linked event
exists, and it maps a `manual` event straight back to `manual_entry`, so
genuinely hand-entered rows are unchanged and re-running is a no-op.
Entries with no linked event are left alone — there is nothing to derive
from and `manual_entry` remains the honest default.

Revision ID: 0035
Revises: 0034
Create Date: 2026-09-05

"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import for_each_tenant_schema

revision = "0035"
down_revision = "0034"
branch_labels = None
depends_on = None

# Migrations carry their own static snapshot of the model's constants and
# must not import them — see the note on ENTRY_PROVENANCE_VALUES.
_CLASS_OLD = "event_class IN ('economic', 'support')"
_CLASS_NEW = "event_class IN ('economic', 'support', 'operational')"

_CATEGORY_OLD = (
  "(event_class = 'economic' AND event_category IN ("
  "'sales', 'purchase', 'financing', 'payroll', "
  "'treasury', 'adjustment', 'recognition', 'other')) "
  "OR (event_class = 'support' AND event_category IN ("
  "'control', 'approval', 'reconciliation', 'inquiry'))"
)
_CATEGORY_NEW = (
  _CATEGORY_OLD + " OR (event_class = 'operational' AND event_category IN ("
  "'pipeline', 'engagement', 'schedule', 'other'))"
)

# `Event.source` is open on the adapter side: the platform emits manual /
# system / schedule, and anything else is a registered connection, hence a
# sync. Mirrors `provenance_for_source` in
# operations/roboledger/commands/journal_entries.py.
_PROVENANCE_SQL = """
  UPDATE "{schema}".entries e
  SET provenance = CASE ev.source
        WHEN 'manual'   THEN 'manual_entry'
        WHEN 'native'   THEN 'manual_entry'
        WHEN 'system'   THEN 'system_computed'
        WHEN 'schedule' THEN 'schedule_derived'
        ELSE 'source_sync'
      END
  FROM "{schema}".events ev
  WHERE ev.id = e.triggered_by_event_id
    AND e.provenance = 'manual_entry'
"""


def _widen(conn: Connection, schema: str) -> None:
  conn.execute(
    text(f'ALTER TABLE "{schema}".events DROP CONSTRAINT IF EXISTS check_event_class')
  )
  conn.execute(
    text(
      f'ALTER TABLE "{schema}".events ADD CONSTRAINT check_event_class '
      f"CHECK ({_CLASS_NEW})"
    )
  )
  conn.execute(
    text(
      f'ALTER TABLE "{schema}".events DROP CONSTRAINT IF EXISTS check_event_category'
    )
  )
  conn.execute(
    text(
      f'ALTER TABLE "{schema}".events ADD CONSTRAINT check_event_category '
      f"CHECK ({_CATEGORY_NEW})"
    )
  )
  # Re-file the rows that had nowhere to go. Only this event_type — the
  # recognition events a schedule emits are genuinely economic.
  conn.execute(
    text(
      f"UPDATE \"{schema}\".events SET event_class = 'operational', "
      "event_category = 'schedule' "
      "WHERE event_type = 'schedule_created' AND event_class = 'economic'"
    )
  )


def _repair_provenance(conn: Connection, schema: str) -> None:
  conn.execute(text(_PROVENANCE_SQL.format(schema=schema)))


def _narrow(conn: Connection, schema: str) -> None:
  # Put the re-filed rows back first, or the narrowed constraint rejects them.
  conn.execute(
    text(
      f"UPDATE \"{schema}\".events SET event_class = 'economic', "
      "event_category = 'other' "
      "WHERE event_type = 'schedule_created' AND event_class = 'operational'"
    )
  )
  # Any other operational rows written since would violate the old CHECK.
  # Fail loudly rather than silently rewriting business data.
  conn.execute(
    text(
      f'ALTER TABLE "{schema}".events DROP CONSTRAINT IF EXISTS check_event_category'
    )
  )
  conn.execute(
    text(
      f'ALTER TABLE "{schema}".events ADD CONSTRAINT check_event_category '
      f"CHECK ({_CATEGORY_OLD})"
    )
  )
  conn.execute(
    text(f'ALTER TABLE "{schema}".events DROP CONSTRAINT IF EXISTS check_event_class')
  )
  conn.execute(
    text(
      f'ALTER TABLE "{schema}".events ADD CONSTRAINT check_event_class '
      f"CHECK ({_CLASS_OLD})"
    )
  )


def upgrade() -> None:
  conn = op.get_bind()
  _widen(conn, "public")
  for_each_tenant_schema(conn, _widen)
  _repair_provenance(conn, "public")
  for_each_tenant_schema(conn, _repair_provenance)


def downgrade() -> None:
  # provenance is deliberately NOT reverted: the repaired values are the
  # true origins, and restoring the mislabel would be re-introducing the
  # bug. The constraint change is reversible; the data repair is not worth
  # undoing.
  conn = op.get_bind()
  _narrow(conn, "public")
  for_each_tenant_schema(conn, _narrow)
