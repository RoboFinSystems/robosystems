"""graph user role check

Revision ID: ca0ce53039eb
Revises: 50dae0508279
Create Date: 2026-08-02 22:19:55.499814

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "ca0ce53039eb"
down_revision = "50dae0508279"
branch_labels = None
depends_on = None


def upgrade() -> None:
  # Autogenerate does not detect CHECK constraints — added by hand.
  # Normalize any stray role strings before constraining (defensive; all
  # production writes have only ever produced 'admin' and 'member').
  op.execute(
    "UPDATE graph_users SET role = 'viewer' "
    "WHERE role NOT IN ('viewer', 'member', 'admin')"
  )
  op.create_check_constraint(
    "ck_graph_users_role",
    "graph_users",
    "role IN ('viewer', 'member', 'admin')",
  )


def downgrade() -> None:
  op.drop_constraint("ck_graph_users_role", "graph_users", type_="check")
