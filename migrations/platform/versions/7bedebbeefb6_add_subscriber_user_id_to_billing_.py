"""add subscriber user_id to billing subscriptions

Repository subscriptions are billed to the org but granted per user. The
subscriber previously survived only in Stripe/checkout metadata, so webhook
provisioning fell back to guessing the org owner. This adds the column and
backfills it from the strongest evidence available for existing rows.

Revision ID: 7bedebbeefb6
Revises: ca0ce53039eb
Create Date: 2026-08-02 23:16:53.203852

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7bedebbeefb6"
down_revision = "ca0ce53039eb"
branch_labels = None
depends_on = None


def upgrade() -> None:
  op.add_column(
    "billing_subscriptions", sa.Column("user_id", sa.String(), nullable=True)
  )
  op.create_index(
    "idx_billing_sub_user", "billing_subscriptions", ["user_id"], unique=False
  )
  op.create_foreign_key(
    "fk_billing_subscriptions_user_id",
    "billing_subscriptions",
    "users",
    ["user_id"],
    ["id"],
  )

  # 1. The checkout path already recorded the subscriber in metadata.
  op.execute("""
    UPDATE billing_subscriptions bs
    SET user_id = bs.subscription_metadata->>'user_id'
    WHERE bs.user_id IS NULL
      AND bs.subscription_metadata->>'user_id' IS NOT NULL
      AND EXISTS (
        SELECT 1 FROM users u WHERE u.id = bs.subscription_metadata->>'user_id'
      )
  """)

  # 2. Repository rows created through the direct path left no metadata, but
  #    provisioning granted the subscriber a user_repository row. Use it only
  #    when it identifies exactly one member of the paying org.
  op.execute("""
    UPDATE billing_subscriptions bs
    SET user_id = sole.user_id
    FROM (
      SELECT ur.repository_name, ou.org_id, MIN(ur.user_id) AS user_id
      FROM user_repository ur
      JOIN org_users ou ON ou.user_id = ur.user_id
      GROUP BY ur.repository_name, ou.org_id
      HAVING COUNT(DISTINCT ur.user_id) = 1
    ) sole
    WHERE bs.user_id IS NULL
      AND bs.resource_type = 'repository'
      AND bs.resource_id = sole.repository_name
      AND bs.org_id = sole.org_id
  """)

  # 3. Anything left over predates multi-user orgs, so the org owner is the
  #    subscriber by construction.
  op.execute("""
    UPDATE billing_subscriptions bs
    SET user_id = ou.user_id
    FROM org_users ou
    WHERE bs.user_id IS NULL
      AND bs.resource_type = 'repository'
      AND ou.org_id = bs.org_id
      AND ou.role = 'OWNER'
  """)


def downgrade() -> None:
  op.drop_constraint(
    "fk_billing_subscriptions_user_id", "billing_subscriptions", type_="foreignkey"
  )
  op.drop_index("idx_billing_sub_user", table_name="billing_subscriptions")
  op.drop_column("billing_subscriptions", "user_id")
