"""immediate cancel

Adds `cancellation_type` to `billing_subscriptions` so the deprovisioning
sensor can distinguish user-initiated immediate cancellations (bypass the
retention window) from period-end cancellations (wait the full window).

Revision ID: ac32cd15e221
Revises: 7ebe3a12f343
Create Date: 2026-05-05 18:28:42.367639

"""

import sqlalchemy as sa
from alembic import op

revision = "ac32cd15e221"
down_revision = "7ebe3a12f343"
branch_labels = None
depends_on = None


def upgrade() -> None:
  op.add_column(
    "billing_subscriptions",
    sa.Column("cancellation_type", sa.String(), nullable=True),
  )
  op.create_index(
    "idx_billing_sub_cancellation_type",
    "billing_subscriptions",
    ["cancellation_type"],
    unique=False,
  )


def downgrade() -> None:
  op.drop_index("idx_billing_sub_cancellation_type", table_name="billing_subscriptions")
  op.drop_column("billing_subscriptions", "cancellation_type")
