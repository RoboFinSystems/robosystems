"""nonnumeric facts — OLTP Fact gains the non-numeric value path

The graph ``Fact`` node has carried the non-numeric shape (value STRING,
fact_type, value_type, content_type, decimals) since inception; the OLTP
``facts`` table was numeric-only (``value FLOAT NOT NULL``), so materialize
hardcoded the non-numeric slots. This migration closes the asymmetry:

- ``facts``: ``value`` becomes nullable; add ``string_value`` (TEXT),
  ``fact_type`` ('Numeric' | 'Nonnumeric'), ``value_type`` ('inline' |
  'external_resource'), ``content_type`` (MIME), ``decimals`` (XBRL
  @decimals). ``ck_facts_value_shape`` enforces numeric XOR non-numeric.
- ``elements``: add ``item_type`` (open XBRL item-type vocabulary; the
  value domain, orthogonal to the structural ``element_type``).
- ``fact_sets``: widen ``check_fact_set_type`` to admit ``'disclosure'``
  (standing Document->text-block binding sets).

NOT NULL columns are added with a constant DEFAULT (instant on PG11+) and
the server default is then dropped so migrated schemas match freshly
provisioned ones (the model uses Python-side defaults only).

Public schema AND every existing tenant schema get the change via the
TenantOps fan-out (matches 0019); fresh tenants get it from the model at
provision (``metadata.create_all``).

Note: the downgrade re-narrows the constraints, restores NOT NULL on
``value``, and drops the columns — it will fail if Nonnumeric rows or
'disclosure' FactSets already exist. Expected once the values are in use.

Revision ID: 0021
Revises: 0020
Create Date: 2026-07-18

"""

from __future__ import annotations

from alembic import op

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0021"
down_revision = "0020"
branch_labels = None
depends_on = None

_FACT_TYPE = "fact_type IN ('Numeric', 'Nonnumeric')"
_VALUE_TYPE = "value_type IN ('inline', 'external_resource')"
_VALUE_SHAPE = (
  "(fact_type = 'Numeric' AND value IS NOT NULL AND string_value IS NULL) OR "
  "(fact_type = 'Nonnumeric' AND string_value IS NOT NULL AND value IS NULL)"
)
_FACTSET_WIDENED = "factset_type IN ('report', 'schedule', 'custom', 'disclosure')"
_FACTSET_ORIGINAL = "factset_type IN ('report', 'schedule', 'custom')"


def _apply(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.add_column("facts", "string_value", "TEXT")
  t.add_column("facts", "fact_type", "VARCHAR", nullable=False, default="'Numeric'")
  t.add_column("facts", "value_type", "VARCHAR", nullable=False, default="'inline'")
  t.add_column("facts", "content_type", "VARCHAR")
  t.add_column("facts", "decimals", "VARCHAR")
  # Existing rows are backfilled from the DEFAULT at ADD time; drop it so
  # migrated schemas match fresh ones (Python-side defaults only).
  t.execute("ALTER TABLE {s}.facts ALTER COLUMN fact_type DROP DEFAULT")
  t.execute("ALTER TABLE {s}.facts ALTER COLUMN value_type DROP DEFAULT")
  t.alter_column_nullable("facts", "value", nullable=True)
  t.add_check("facts", "ck_facts_fact_type", _FACT_TYPE)
  t.add_check("facts", "ck_facts_value_type", _VALUE_TYPE)
  t.add_check("facts", "ck_facts_value_shape", _VALUE_SHAPE)
  t.add_column("elements", "item_type", "VARCHAR")
  t.add_check("fact_sets", "check_fact_set_type", _FACTSET_WIDENED)


def _revert(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.add_check("fact_sets", "check_fact_set_type", _FACTSET_ORIGINAL)
  t.drop_column("elements", "item_type")
  t.drop_constraint("facts", "ck_facts_value_shape")
  t.drop_constraint("facts", "ck_facts_value_type")
  t.drop_constraint("facts", "ck_facts_fact_type")
  t.alter_column_nullable("facts", "value", nullable=False)
  for col in ("decimals", "content_type", "value_type", "fact_type", "string_value"):
    t.drop_column("facts", col)


def upgrade() -> None:
  conn = op.get_bind()
  _apply(conn, "public")
  for_each_tenant_schema(conn, _apply)


def downgrade() -> None:
  conn = op.get_bind()
  _revert(conn, "public")
  for_each_tenant_schema(conn, _revert)
