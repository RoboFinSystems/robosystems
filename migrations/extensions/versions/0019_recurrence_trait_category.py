"""add recurrence trait category

Widen the ``check_trait_category`` CHECK constraint on ``traits.category`` to
admit the new ``recurrence`` axis (member ``nonrecurring``) — an RS analytical
extension to the FASB-metamodel trait vocabulary for earnings-persistence
classification (normalized earnings / NOPAT excluding special items). The
``fac-traits`` package seeds the trait member; ``rs-gaap-traits`` binds it to
the kept operating special-items (restructuring, impairment, disposal
gains/losses, debt extinguishment).

The constraint is created unprefixed (``check_trait_category``) in every schema
by ``0002_taxonomy_library.py``, so this widen runs against ``public`` and every
existing tenant schema — mirroring the CHECK-constraint widen pattern in
``0007_frameworks_bridges.py``. New tenants pick the widened constraint up
automatically via ``ExtensionsBase.metadata.create_all`` at provision.

Revision ID: 0019
Revises: 0018
Create Date: 2026-06-06 12:04:37.532894

"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "0019"
down_revision = "0018"
branch_labels = None
depends_on = None


# The 24 FASB us-gaap metamodel axes + flowClassification + indirectCashFlowReconcilingItem.
# Mirrors check_trait_category in robosystems/models/extensions/trait.py.
_PRIOR_TRAIT_CATEGORY_CHECK = (
  "category IN ("
  "'elementsOfFinancialStatements', 'liquidity', 'activityType', "
  "'operatingNonoperating', 'operatingIntent', 'realizationStatus', "
  "'recordedValue', 'restriction', 'hedging', 'leaseType', "
  "'interestRateType', 'convertibility', 'creditStructure', "
  "'debtGuarantee', 'derivativeContract', 'derivativeInstrument', "
  "'accrualOrPayable', 'priority', 'estimatedFutureActivity', "
  "'statisticalMeasurement', 'taxComponents', 'threshold', 'use', "
  "'indirectCashFlowReconcilingItem', "
  "'flowClassification'"
  ")"
)

# Same list + the new 'recurrence' analytical axis.
_WIDENED_TRAIT_CATEGORY_CHECK = (
  "category IN ("
  "'elementsOfFinancialStatements', 'liquidity', 'activityType', "
  "'operatingNonoperating', 'operatingIntent', 'realizationStatus', "
  "'recordedValue', 'restriction', 'hedging', 'leaseType', "
  "'interestRateType', 'convertibility', 'creditStructure', "
  "'debtGuarantee', 'derivativeContract', 'derivativeInstrument', "
  "'accrualOrPayable', 'priority', 'estimatedFutureActivity', "
  "'statisticalMeasurement', 'taxComponents', 'threshold', 'use', "
  "'indirectCashFlowReconcilingItem', "
  "'flowClassification', "
  "'recurrence'"
  ")"
)


def _trait_table(schema: str) -> str:
  return "public.traits" if schema == "public" else f'"{schema}".traits'


def _widen_trait_category_check(conn, schema: str) -> None:
  """Drop and re-add the traits.category CHECK with the widened list."""
  table = _trait_table(schema)
  conn.execute(
    text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS check_trait_category")
  )
  conn.execute(
    text(
      f"ALTER TABLE {table} ADD CONSTRAINT check_trait_category "
      f"CHECK ({_WIDENED_TRAIT_CATEGORY_CHECK})"
    )
  )


def _restore_trait_category_check(conn, schema: str) -> None:
  """Inverse of :func:`_widen_trait_category_check` for downgrade."""
  table = _trait_table(schema)
  conn.execute(
    text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS check_trait_category")
  )
  conn.execute(
    text(
      f"ALTER TABLE {table} ADD CONSTRAINT check_trait_category "
      f"CHECK ({_PRIOR_TRAIT_CATEGORY_CHECK})"
    )
  )


def upgrade() -> None:
  conn = op.get_bind()
  from migrations.extensions.helpers import for_each_tenant_schema

  _widen_trait_category_check(conn, "public")
  for_each_tenant_schema(conn, _widen_trait_category_check)


def downgrade() -> None:
  conn = op.get_bind()
  from migrations.extensions.helpers import for_each_tenant_schema

  # Downgrade restores the pre-recurrence list. Any 'recurrence' rows must be
  # removed first or the restored CHECK will reject them.
  conn.execute(text("DELETE FROM public.traits WHERE category = 'recurrence'"))

  def _drop_recurrence_rows(c, schema: str) -> None:
    c.execute(text(f"DELETE FROM \"{schema}\".traits WHERE category = 'recurrence'"))

  for_each_tenant_schema(conn, _drop_recurrence_rows)

  _restore_trait_category_check(conn, "public")
  for_each_tenant_schema(conn, _restore_trait_category_check)
