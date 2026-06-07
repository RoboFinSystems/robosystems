"""Add events.payload_drift + elements UPSERT unique key.

Re-sync fidelity for the QuickBooks adapter — closes the gaps between
documented behavior and actual code, verified against real prod data on
2026-05-18:

- `idx_elements_upsert_key`: partial UNIQUE on
  `(external_source, connection_id, external_id)` where both fields
  are non-null. Defense-in-depth for the loader's lookup-then-update
  path; preserves `elem_*` ULIDs across syncs so downstream
  Associations and IB Fact references hold their FK targets stable.
  Closes the rare duplicate-insert race between concurrent syncs that
  the per-connection sync lock will close fully.

- `events.payload_drift`: boolean flag, default false. Set
  true when an adapter re-sync surfaces a payload diff against a
  `committed`/`fulfilled` event. Drifted payload stashes at
  `metadata_['drift_payload']`; the live payload stays unchanged
  (handler-approved entries are immutable to re-sync).

- `idx_events_payload_drift`: partial index for the reconciliation
  queue read path. Most events carry `drift=false` at all times so the
  partial index stays tiny.

Applies to both `public.*` (template for new tenants) and every existing
tenant schema. Per-tenant index names are schema-prefixed to avoid
cross-tenant collisions, matching the pattern in `0012_event_action.py`.

Spurious cross-schema FK + self-FK autogenerate diffs (framework_bridges,
framework_packages, taxonomies, fk_entries_triggered_by_event,
fk_events_agent_id) are stripped — those FKs live in the DB but aren't
declared via SQLAlchemy `ForeignKey()` in the ORM models, so Alembic's
autogen detects spurious "remove/re-add" diffs on every regeneration.

Revision ID: 0013
Revises: 0012
Create Date: 2026-05-18
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None


def _add_in_tenant(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  # payload_drift column + read-path index
  t.add_column(
    "events",
    "payload_drift",
    "BOOLEAN",
    nullable=False,
    default="false",
  )
  t.create_index(
    f"idx_{schema}_events_payload_drift",
    "events",
    ["payload_drift"],
    where="payload_drift = true",
  )
  # Element UPSERT key
  t.create_index(
    f"idx_{schema}_elements_upsert_key",
    "elements",
    ["external_source", "connection_id", "external_id"],
    unique=True,
    where="external_id IS NOT NULL AND connection_id IS NOT NULL",
  )


def _drop_in_tenant(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.drop_index(f"idx_{schema}_elements_upsert_key")
  t.drop_index(f"idx_{schema}_events_payload_drift")
  t.drop_column("events", "payload_drift")


def upgrade() -> None:
  # Public template — used when provisioning new tenant schemas via
  # `provision_tenant_schema()` → `ExtensionsBase.metadata.create_all()`.
  op.create_index(
    "idx_elements_upsert_key",
    "elements",
    ["external_source", "connection_id", "external_id"],
    unique=True,
    postgresql_where="external_id IS NOT NULL AND connection_id IS NOT NULL",
  )
  op.add_column(
    "events",
    sa.Column(
      "payload_drift",
      sa.Boolean(),
      server_default="false",
      nullable=False,
    ),
  )
  op.create_index(
    "idx_events_payload_drift",
    "events",
    ["payload_drift"],
    unique=False,
    postgresql_where="payload_drift = true",
  )

  # Every existing tenant schema.
  conn = op.get_bind()
  for_each_tenant_schema(conn, _add_in_tenant)


def downgrade() -> None:
  conn = op.get_bind()
  for_each_tenant_schema(conn, _drop_in_tenant)

  op.execute(text("DROP INDEX IF EXISTS idx_events_payload_drift"))
  op.drop_column("events", "payload_drift")
  op.execute(text("DROP INDEX IF EXISTS idx_elements_upsert_key"))
