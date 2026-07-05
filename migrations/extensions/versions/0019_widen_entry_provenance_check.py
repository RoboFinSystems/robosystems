"""widen entry provenance CHECK to include event_handler

The ``ck_entries_provenance`` CHECK shipped in 0001 permitting
{source_sync, ai_generated, manual_entry, schedule_derived, system_computed}.
The event-block engine (``operations/event_block/engine.py``) creates entries
with ``provenance='event_handler'`` — a value the constraint rejected, so the
first real event-handler-driven GL insert would fail with a CheckViolation.
Widen the constraint to permit ``event_handler`` (kept in lockstep with
``ENTRY_PROVENANCE_VALUES`` on the model).

Public schema AND every existing tenant schema get the widened constraint here
via the TenantOps fan-out (matches 0018); fresh tenants get it from the model's
CheckConstraint at provision (``metadata.create_all``).

Note: the downgrade re-narrows the constraint and will fail if any
``provenance='event_handler'`` rows already exist (they would violate the
narrower CHECK) — expected once the value is in use.

Revision ID: 0019
Revises: 0018
Create Date: 2026-07-04

"""

from __future__ import annotations

from alembic import op

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0019"
down_revision = "0018"
branch_labels = None
depends_on = None

_WIDENED = (
  "provenance IN ('source_sync', 'ai_generated', 'manual_entry', "
  "'schedule_derived', 'system_computed', 'event_handler') OR provenance IS NULL"
)
_ORIGINAL = (
  "provenance IN ('source_sync', 'ai_generated', 'manual_entry', "
  "'schedule_derived', 'system_computed') OR provenance IS NULL"
)


def _widen(conn, schema: str) -> None:
  TenantOps(conn, schema).add_check("entries", "ck_entries_provenance", _WIDENED)


def _narrow(conn, schema: str) -> None:
  TenantOps(conn, schema).add_check("entries", "ck_entries_provenance", _ORIGINAL)


def upgrade() -> None:
  conn = op.get_bind()
  _widen(conn, "public")
  for_each_tenant_schema(conn, _widen)


def downgrade() -> None:
  conn = op.get_bind()
  _narrow(conn, "public")
  for_each_tenant_schema(conn, _narrow)
