"""Add events.metadata->>'qb_external_id' partial expression index.

Phase 4 §4.2 step 4 — cross-source matcher. The loader's
`_capture_transactions_as_events` checks each incoming
`source='quickbooks'` row against this index to detect round-tripped
RoboSystems-originated events (events whose `metadata.qb_external_id`
matches the incoming external_id). When matched the loader skips the
INSERT + handler re-fire and stamps `qb_sync_confirmed_at` on the
existing event — avoiding the double-post that would otherwise occur
when our write-back lands the entry in QB and the next sync reads it
back.

Partial: only events with `metadata->>'qb_external_id' IS NOT NULL`
qualify — write-back-routed rows only. The vast majority of events
(QB-inbound, native, schedule pre-write-back) carry no
qb_external_id, so the partial keeps the index tiny.

Applies to both `public.events` (template) and every existing tenant
schema. Per-tenant index name is schema-prefixed
(`idx_{schema}_events_qb_external_id`) matching the convention from
`0012_event_action.py` and `0013_payload_drift_and_element_upsert.py`.

Spurious cross-schema FK + self-FK autogen diffs are stripped — same
pattern seen in the prior extensions migrations.

Revision ID: 0014
Revises: 0013
Create Date: 2026-05-19
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

revision = "0014"
down_revision = "0013"
branch_labels = None
depends_on = None


def _add_in_tenant(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  # Expression index — drop down to raw SQL since `TenantOps.create_index`
  # takes a list of column names, not arbitrary expressions.
  t.execute(
    "CREATE INDEX IF NOT EXISTS "
    f"idx_{schema}_events_qb_external_id "
    "ON {s}.events ((metadata->>'qb_external_id')) "
    "WHERE metadata->>'qb_external_id' IS NOT NULL"
  )


def _drop_in_tenant(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.drop_index(f"idx_{schema}_events_qb_external_id")


def upgrade() -> None:
  # Public template — used when provisioning new tenant schemas via
  # `provision_tenant_schema()` → `ExtensionsBase.metadata.create_all()`.
  op.execute(
    text(
      "CREATE INDEX IF NOT EXISTS idx_events_qb_external_id "
      "ON events ((metadata->>'qb_external_id')) "
      "WHERE metadata->>'qb_external_id' IS NOT NULL"
    )
  )

  # Every existing tenant schema.
  conn = op.get_bind()
  for_each_tenant_schema(conn, _add_in_tenant)


def downgrade() -> None:
  conn = op.get_bind()
  for_each_tenant_schema(conn, _drop_in_tenant)
  op.execute(text("DROP INDEX IF EXISTS idx_events_qb_external_id"))
