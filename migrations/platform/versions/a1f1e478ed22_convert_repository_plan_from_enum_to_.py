"""convert repository_plan from enum to string

Revision ID: a1f1e478ed22
Revises: 2c3325396afb
Create Date: 2026-02-08 13:54:51.658985

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "a1f1e478ed22"
down_revision = "2c3325396afb"
branch_labels = None
depends_on = None

# The old PostgreSQL ENUM type name
ENUM_NAME = "repositoryplan"


def upgrade() -> None:
  # Convert ENUM column to VARCHAR. The ENUM stores uppercase labels (STARTER, ADVANCED)
  # but the app uses lowercase values (starter, advanced). LOWER() normalizes on conversion.
  op.alter_column(
    "user_repository",
    "repository_plan",
    existing_type=postgresql.ENUM("STARTER", "ADVANCED", "UNLIMITED", name=ENUM_NAME),
    type_=sa.String(),
    existing_nullable=False,
    postgresql_using="LOWER(repository_plan::text)",
  )

  # Drop the now-unused PostgreSQL ENUM type
  op.execute(f"DROP TYPE IF EXISTS {ENUM_NAME}")


def downgrade() -> None:
  # Recreate the enum type
  repo_plan_enum = sa.Enum("STARTER", "ADVANCED", "UNLIMITED", name=ENUM_NAME)
  repo_plan_enum.create(op.get_bind(), checkfirst=True)

  # Convert back: lowercase string values -> uppercase enum labels
  op.execute(
    "ALTER TABLE user_repository "
    f"ALTER COLUMN repository_plan TYPE {ENUM_NAME} "
    f"USING UPPER(repository_plan)::{ENUM_NAME}"
  )
