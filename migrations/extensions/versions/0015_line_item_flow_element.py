"""add flow_element_id to line_items

Promote the per-line flow concept from the legacy
``line_items.metadata['transaction_description_code']`` JSONB string to a
first-class ``flow_element_id`` FK → elements. Additive + nullable; no
backfill (the metadata flow tag only landed 2026-05-19 and is carried by
re-providable demo data, not production rows). Fresh tenants get the column
via ``metadata.create_all`` at provision; existing tenant schemas (and
public) get it here.

Revision ID: 0015
Revises: 0014
Create Date: 2026-05-20

"""

from __future__ import annotations

from alembic import op

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None

_FK_NAME = "fk_line_items_flow_element"
_IDX_NAME = "idx_line_items_flow_element"


def _add_flow_element(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.add_column("line_items", "flow_element_id", "VARCHAR", nullable=True)
  t.create_index(_IDX_NAME, "line_items", ["flow_element_id"])
  t.add_foreign_key("line_items", _FK_NAME, ["flow_element_id"], "elements", ["id"])


def _drop_flow_element(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.drop_constraint("line_items", _FK_NAME)
  t.drop_index(_IDX_NAME)
  t.drop_column("line_items", "flow_element_id")


def upgrade() -> None:
  conn = op.get_bind()
  _add_flow_element(conn, "public")
  for_each_tenant_schema(conn, _add_flow_element)


def downgrade() -> None:
  conn = op.get_bind()
  _drop_flow_element(conn, "public")
  for_each_tenant_schema(conn, _drop_flow_element)
