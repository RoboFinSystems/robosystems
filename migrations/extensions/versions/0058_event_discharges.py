"""``event_discharges``: what a payment settles, document by document.

``events.discharges_event_id`` names one obligation, so a payment that settles
several invoices or bills links only the first and the aged read undercounts
the rest (spec ``ar-ap-aging`` §5.3). One row per obligation a payment
settles, with the positive cents applied to it; the column keeps the first
row's target so every existing reader of it still works.

Autogenerate also rendered unrelated foreign-key drift on ``public``, so the
body is written by hand: the table alone, in ``public`` and in every tenant.

Revision ID: 0058
Revises: 0057
Create Date: 2026-10-20
"""

from __future__ import annotations

from alembic import op
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

revision = "0058"
down_revision = "0057"
branch_labels = None
depends_on = None

_TABLE = "event_discharges"
_INDEX = "idx_event_discharges_obligation"


def _ddl(schema: str) -> str:
  events = f'"{schema}".events'
  return f"""
    payment_event_id VARCHAR NOT NULL REFERENCES {events} (id) ON DELETE CASCADE,
    obligation_event_id VARCHAR NOT NULL REFERENCES {events} (id) ON DELETE CASCADE,
    amount BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    created_by VARCHAR NOT NULL,
    CONSTRAINT check_event_discharges_amount_positive CHECK (amount > 0),
    PRIMARY KEY (payment_event_id, obligation_event_id)
  """


def _expand(conn: Connection, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.create_table(_TABLE, _ddl(schema))
  t.create_index(_INDEX, _TABLE, ["obligation_event_id"])


def _contract(conn: Connection, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.drop_index(_INDEX)
  t.drop_table(_TABLE)


def upgrade() -> None:
  conn = op.get_bind()
  _expand(conn, "public")
  for_each_tenant_schema(conn, _expand)


def downgrade() -> None:
  conn = op.get_bind()
  for_each_tenant_schema(conn, _contract)
  _contract(conn, "public")
