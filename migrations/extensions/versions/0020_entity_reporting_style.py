"""move reporting style to the entity

Adds ``reporting_style_id`` to ``entities`` and backfills it from each
entity's legal form — the same derivation the graph-level column used at
creation (``operations/graph/reporting_style_defaults.default_style_for``):

* partnership → PARTNERSHIP_STYLE_ID
* llc / limited_liability_company → LLC_STYLE_ID
* everything else (corporation / subsidiary / blank / unknown) → DEFAULT_STYLE_ID

This is the extensions half of the graph→entity cutover; the sibling platform
migration drops ``graphs.reporting_style_id``. Re-deriving from ``entity_type``
reproduces the prior state for every graph whose Style was not manually
overridden via the (now-removed) graph-level change-reporting-style op. Any
manual overrides are rare pre-launch and reconciled by hand — compare each
tenant's ``entities.reporting_style_id`` against the old
``graphs.reporting_style_id`` before this migration ran.

Fresh tenants get the column via ``metadata.create_all`` at provision (the
model carries a Python-side default); existing tenant schemas (and public) get
it here via the TenantOps fan-out. The column is added with a server default
so existing rows populate, then the server default is dropped so all schemas
match the model (NOT NULL, ORM-supplied default).

Revision ID: 0020
Revises: 0019
Create Date: 2026-07-10

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from migrations.extensions.helpers import (
  TenantOps,
  add_tenant_column,
  for_each_tenant_schema,
)
from robosystems.config.constants import ReportingStyleConstants

# revision identifiers, used by Alembic.
revision = "0020"
down_revision = "0019"
branch_labels = None
depends_on = None

_DEFAULT = ReportingStyleConstants.DEFAULT_STYLE_ID
_PARTNERSHIP = ReportingStyleConstants.PARTNERSHIP_STYLE_ID
_LLC = ReportingStyleConstants.LLC_STYLE_ID


def _backfill(conn, schema: str) -> None:
  """Re-derive each entity's Style from its legal form (mirrors
  ``default_style_for``). Corp/unknown keep the added default."""
  t = TenantOps(conn, schema)
  t.execute(
    "UPDATE {s}.entities SET reporting_style_id = '"
    + _PARTNERSHIP
    + "' WHERE lower(entity_type) = 'partnership'"
  )
  t.execute(
    "UPDATE {s}.entities SET reporting_style_id = '"
    + _LLC
    + "' WHERE lower(entity_type) IN ('llc', 'limited_liability_company')"
  )


def _drop_default(conn, schema: str) -> None:
  TenantOps(conn, schema).execute(
    "ALTER TABLE {s}.entities ALTER COLUMN reporting_style_id DROP DEFAULT"
  )


def upgrade() -> None:
  conn = op.get_bind()

  # 1. Add the column to public + every tenant, server-defaulted so existing
  #    rows populate.
  add_tenant_column(
    "entities",
    sa.Column(
      "reporting_style_id",
      sa.String(),
      nullable=False,
      server_default=_DEFAULT,
    ),
    "VARCHAR",
    nullable=False,
    default=f"'{_DEFAULT}'",
  )

  # 2. Backfill non-corporate forms from entity_type.
  _backfill(conn, "public")
  for_each_tenant_schema(conn, _backfill)

  # 3. Drop the server default so every schema matches the model (the ORM
  #    supplies the Python-side default on insert).
  op.alter_column("entities", "reporting_style_id", server_default=None)
  for_each_tenant_schema(conn, _drop_default)


def downgrade() -> None:
  conn = op.get_bind()
  op.drop_column("entities", "reporting_style_id")

  def _drop(conn, schema: str) -> None:
    TenantOps(conn, schema).drop_column("entities", "reporting_style_id")

  for_each_tenant_schema(conn, _drop)
