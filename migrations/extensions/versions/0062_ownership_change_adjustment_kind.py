"""``ownership_change``: a consolidating entry recording a change in the
parent's share of a member within the same consolidation method (spec
``group-lifecycle`` Phase 2).

The price the parent paid or received for the shares, and the equity
transaction ASC 810 makes of it, are an entry of this kind in the
consolidation entity; the engine reads its price as part of the parent's
investment in the member and the share it records as the member's from its
month. Only the CHECK on ``entries.adjustment_kind`` changes, which
autogenerate does not see, so the body is written by hand, in ``public``
and in every tenant. No row changes.

Revision ID: 0062
Revises: 0061
Create Date: 2026-10-22
"""

from __future__ import annotations

from alembic import op
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

revision = "0062"
down_revision = "0061"
branch_labels = None
depends_on = None

_BEFORE = (
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
_AFTER = (*_BEFORE, "ownership_change")
_CHECK = "ck_entries_adjustment_kind"


def _check(conn: Connection, schema: str, kinds: tuple[str, ...]) -> None:
  t = TenantOps(conn, schema)
  t.drop_constraint("entries", _CHECK)
  listed = ", ".join(f"'{k}'" for k in kinds)
  t.add_check(
    "entries", _CHECK, f"adjustment_kind IN ({listed}) OR adjustment_kind IS NULL"
  )


def upgrade() -> None:
  conn = op.get_bind()
  _check(conn, "public", _AFTER)
  for_each_tenant_schema(conn, lambda c, s: _check(c, s, _AFTER))


def downgrade() -> None:
  # An ownership_change entry left in a schema fails the narrower CHECK there,
  # so the downgrade stops rather than drop the kind under it.
  conn = op.get_bind()
  _check(conn, "public", _BEFORE)
  for_each_tenant_schema(conn, lambda c, s: _check(c, s, _BEFORE))
