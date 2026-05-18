"""Add event_action column to events with canonical 19-verb CHECK.

Adopts a canonical action-verb vocabulary as the refinement of
`event_category` per `ontology-alignment.md` §4.6. The 19 verbs
disambiguate concepts ERPs typically conflate — `transferCustody` vs
`transferAllRights` is the load-bearing distinction for consignment,
drop-shipping, marketplace settlement, and escrow. The vocabulary
converges with Valueflows (Foster / Pavlik / Haugen v1.0), which we
treat as inspiration; canonical naming on our side stays
RoboSystems-native (`event_action` / `EVENT_ACTIONS`).

The column is **nullable**: existing rows keep `event_action = NULL`;
new events from adapters and the manual API surface fill it in going
forward. Legacy `event_category` is retained for analytics continuity.

Applies to both `public.events` (template for new tenants) and every
existing tenant schema.

Revision ID: 0012
Revises: 0011
Create Date: 2026-05-18
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None


# Canonical 19-verb action vocabulary. MUST stay in sync with
# EVENT_ACTIONS in robosystems/models/extensions/roboledger/event.py.
#
# **Ordering is load-bearing**: verbs are listed in ALPHABETICAL order to
# match what SQLAlchemy emits — the model's CheckConstraint builds the
# IN clause via `sorted(EVENT_ACTIONS)`. Alembic's autogenerate compares
# constraint expressions textually, so any ordering mismatch produces
# spurious "alter check_event_action" diffs on every subsequent
# `just migrate-create`. If the vocabulary grows, append + re-sort here
# and update the frozenset; do NOT group by semantic category.
_EVENT_ACTION_CHECK = (
  "event_action IS NULL OR event_action IN ("
  "'accept', 'cite', 'combine', 'consume', 'copy', 'deliverService', "
  "'dropoff', 'lower', 'modify', 'move', 'pickup', 'produce', 'raise', "
  "'separate', 'transfer', 'transferAllRights', 'transferCustody', "
  "'use', 'work'"
  ")"
)


def _add_in_tenant(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.add_column("events", "event_action", "VARCHAR")
  t.add_check("events", f"check_{schema}_event_action", _EVENT_ACTION_CHECK)
  t.create_index(
    f"idx_{schema}_events_action",
    "events",
    ["event_action"],
    where="event_action IS NOT NULL",
  )


def _drop_in_tenant(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.drop_index(f"idx_{schema}_events_action")
  t.drop_constraint("events", f"check_{schema}_event_action")
  t.drop_column("events", "event_action")


def upgrade() -> None:
  # Public template
  op.add_column("events", sa.Column("event_action", sa.String(), nullable=True))
  op.create_check_constraint("check_event_action", "events", _EVENT_ACTION_CHECK)
  op.create_index(
    "idx_events_action",
    "events",
    ["event_action"],
    postgresql_where=sa.text("event_action IS NOT NULL"),
  )

  # Every existing tenant
  conn = op.get_bind()
  for_each_tenant_schema(conn, _add_in_tenant)


def downgrade() -> None:
  conn = op.get_bind()
  for_each_tenant_schema(conn, _drop_in_tenant)

  # Public template
  op.execute(text("DROP INDEX IF EXISTS idx_events_action"))
  op.drop_constraint("check_event_action", "events", type_="check")
  op.drop_column("events", "event_action")
