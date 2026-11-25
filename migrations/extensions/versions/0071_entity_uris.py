"""Every entity's URI is the one RoboSystems assigns.

An entity's ``uri`` was set three ways: graph creation defaulted it to
``https://robosystems.ai/entities#<graph_id>``, a new subsidiary to
``https://robosystems.ai/entities#<entity_id>``, and subscription provisioning
to a slug of the name. It is now always
``https://robosystems.ai/entities#<entity_id>``, and this backfills every
tenant to that.

Graph creation also copied the request's ``uri`` into ``website``. Where that
left a value that is not a web address (the slug), the website is cleared;
an http(s) address someone gave stays.

Autogenerate proposed only unrelated drift; none of it is kept.

Revision ID: 0071
Revises: 0070
Create Date: 2026-11-24
"""

from __future__ import annotations

from alembic import op

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema
from robosystems.config.constants import URIConstants

revision = "0071"
down_revision = "0070"
branch_labels = None
depends_on = None

_BASE = URIConstants.ENTITY_URI_BASE


def _assign(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.execute(
    "UPDATE {s}.entities SET website = NULL "
    "WHERE website = uri AND website !~* '^https?://'"
  )
  t.execute(
    "UPDATE {s}.entities SET uri = '"
    + _BASE
    + "' || id WHERE uri IS DISTINCT FROM '"
    + _BASE
    + "' || id"
  )


def upgrade() -> None:
  for_each_tenant_schema(op.get_bind(), _assign)


def downgrade() -> None:
  """The earlier values are not kept; nothing reads ``uri`` to depend on them."""
