"""``uq_entities_ticker_own``: a ticker is unique among a graph's own
entities, case apart (spec ``group-lifecycle`` Phase 3, folded from the
ledger README's multi-entity residuals).

``create-entity`` and ``update-entity`` already refuse a taken ticker under
an advisory lock; the partial unique index is the hardening behind them. A
``linked`` row carries its source graph's ticker and is not held to it.
Autogenerate rendered unrelated foreign-key drift on ``public``, so the body
is written by hand: the index alone, in ``public`` and in every tenant. A
schema holding two own entities on one ticker stops the upgrade and names
them, rather than be left without the index in silence.

Revision ID: 0063
Revises: 0062
Create Date: 2026-10-22
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

revision = "0063"
down_revision = "0062"
branch_labels = None
depends_on = None

_INDEX = "uq_entities_ticker_own"


def _expand(conn: Connection, schema: str) -> None:
  taken = conn.execute(
    text(
      f'SELECT lower(ticker) AS ticker, count(*) AS n FROM "{schema}".entities '
      "WHERE ticker IS NOT NULL AND source <> 'linked' "
      "GROUP BY lower(ticker) HAVING count(*) > 1 ORDER BY 1"
    )
  ).all()
  if taken:
    raise RuntimeError(
      f"{schema}: a ticker is shared by more than one own entity "
      f"({', '.join(f'{r.ticker} x{r.n}' for r in taken)}); give each its own "
      "before this upgrade."
    )
  TenantOps(conn, schema).create_index(
    _INDEX, "entities", ["lower(ticker)"], unique=True, where="source <> 'linked'"
  )


def _contract(conn: Connection, schema: str) -> None:
  TenantOps(conn, schema).drop_index(_INDEX)


def upgrade() -> None:
  conn = op.get_bind()
  _expand(conn, "public")
  for_each_tenant_schema(conn, _expand)


def downgrade() -> None:
  conn = op.get_bind()
  for_each_tenant_schema(conn, _contract)
  _contract(conn, "public")
