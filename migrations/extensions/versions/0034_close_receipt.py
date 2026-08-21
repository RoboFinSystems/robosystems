"""close receipt — fiscal_periods carries the outcome of its own close

The close result (entries posted, the QB/local split, statement stamping,
rule summaries) existed only in the HTTP response. A close that SUCCEEDED
but whose transport failed — the MCP surface's 25s client timeout is the
live case — left no record on the books, so the operator reconstructed
what happened from four separate state reads and had to know not to retry.

``close_receipt`` is written by ``PeriodCloseService.close()`` in the same
transaction as the ``status='closed'`` flip, so a committed close always
carries its receipt and a rolled-back one carries none.

Nullable with no backfill: periods closed before this shipped have no
receipt to reconstruct, and inventing one would be worse than its absence
(readers distinguish "closed, no receipt" from "closed, here is what
happened"). Public schema AND every existing tenant schema get the column
via the TenantOps fan-out; fresh tenants get it from the model at provision.

Revision ID: 0034
Revises: 0033
Create Date: 2026-08-21

"""

from __future__ import annotations

from alembic import op

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0034"
down_revision = "0033"
branch_labels = None
depends_on = None


def _apply(conn, schema: str) -> None:
  TenantOps(conn, schema).add_column("fiscal_periods", "close_receipt", "JSONB")


def _revert(conn, schema: str) -> None:
  TenantOps(conn, schema).drop_column("fiscal_periods", "close_receipt")


def upgrade() -> None:
  conn = op.get_bind()
  _apply(conn, "public")
  for_each_tenant_schema(conn, _apply)


def downgrade() -> None:
  conn = op.get_bind()
  _revert(conn, "public")
  for_each_tenant_schema(conn, _revert)
