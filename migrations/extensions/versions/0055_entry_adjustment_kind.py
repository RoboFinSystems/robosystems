"""A consolidating entry's kind and the entity it adjusts.

The consolidation engine (spec ``consolidation`` C2) brings each adjustment to
its month-end target and proposes only the difference from what the
consolidation entity already holds, per kind and per member (D8). That needs
the kind and the member on the entry itself: ``adjustment_kind`` (the §4.2
vocabulary) and ``adjusts_entity_id``. Both are nullable; only the
consolidation entity's entries carry them, so every existing row is
unchanged.

Autogenerate sees the columns and the index on ``public`` but neither the
CHECK nor the tenant schemas, so the body is written by hand, in ``public``
and in every tenant.

Revision ID: 0055
Revises: 0054
Create Date: 2026-10-18
"""

from __future__ import annotations

from alembic import op
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

revision = "0055"
down_revision = "0054"
branch_labels = None
depends_on = None

_KINDS = (
  "elimination_investment",
  "elimination_intercompany",
  "elimination_unrealized_profit",
  "nci_allocation",
  "nci_distribution",
  "equity_method_pickup",
  "equity_method_distribution",
  "acquisition",
  "deconsolidation",
  "translation",
  "manual",
)
_CHECK = "ck_entries_adjustment_kind"
_INDEX = "idx_entries_adjustment"


def _expand(conn: Connection, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.add_column("entries", "adjustment_kind", "VARCHAR")
  t.add_column("entries", "adjusts_entity_id", "VARCHAR")
  kinds = ", ".join(f"'{k}'" for k in _KINDS)
  t.add_check(
    "entries", _CHECK, f"adjustment_kind IN ({kinds}) OR adjustment_kind IS NULL"
  )
  t.create_index(
    _INDEX,
    "entries",
    ["adjusts_entity_id", "adjustment_kind"],
    where="adjustment_kind IS NOT NULL",
  )


def _contract(conn: Connection, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.drop_index(_INDEX)
  t.drop_constraint("entries", _CHECK)
  t.drop_column("entries", "adjusts_entity_id")
  t.drop_column("entries", "adjustment_kind")


def upgrade() -> None:
  conn = op.get_bind()
  _expand(conn, "public")
  for_each_tenant_schema(conn, _expand)


def downgrade() -> None:
  conn = op.get_bind()
  _contract(conn, "public")
  for_each_tenant_schema(conn, _contract)
