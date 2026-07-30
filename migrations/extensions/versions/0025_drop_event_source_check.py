"""drop the Event.source CHECK — source validation moves to the ops layer

``check_event_source`` shipped in 0005 as a closed list
(manual/system/schedule/quickbooks/xero/plaid), which made an external
integration's source name impossible without a schema change.
``create_event_block`` now validates ``source`` against the graph's
registered platform Connections (``_validate_event_source``), so the
registry — not a CHECK — owns the source vocabulary. The model's
CheckConstraint is removed in the same change, which is what keeps the
constraint off schemas provisioned after this migration
(``metadata.create_all``).

Two constraint names exist in the wild for the same CHECK: tenant
schemas created by 0005's raw DDL carry ``check_{schema}_event_source``,
while the public template and schemas provisioned via
``metadata.create_all`` carry the unprefixed ``check_event_source``
(same dual-naming history 0007 cleaned up for ``elements``). Both are
dropped per schema; ``DROP CONSTRAINT IF EXISTS`` makes stragglers
harmless.

Note: the downgrade re-adds only the unprefixed name and will fail if
any rows carry a registered external source (they would violate the
re-added closed list) — expected once external sources are in use.

Revision ID: 0025
Revises: 0024
Create Date: 2026-07-30

"""

from __future__ import annotations

from alembic import op

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0025"
down_revision = "0024"
branch_labels = None
depends_on = None

# Byte-identical to 0005's _SOURCE_CHECK — the downgrade re-add.
_SOURCE_CHECK = (
  "source IN ('manual', 'system', 'schedule', 'quickbooks', 'xero', 'plaid')"
)


def _drop(conn, schema: str) -> None:
  ops = TenantOps(conn, schema)
  ops.drop_constraint("events", "check_event_source")
  if schema != "public":
    ops.drop_constraint("events", f"check_{schema}_event_source")


def _re_add(conn, schema: str) -> None:
  TenantOps(conn, schema).add_check("events", "check_event_source", _SOURCE_CHECK)


def upgrade() -> None:
  conn = op.get_bind()
  _drop(conn, "public")
  for_each_tenant_schema(conn, _drop)


def downgrade() -> None:
  conn = op.get_bind()
  _re_add(conn, "public")
  for_each_tenant_schema(conn, _re_add)
