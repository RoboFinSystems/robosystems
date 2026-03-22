"""convert repository_type from enum to string

Revision ID: 2c3325396afb
Revises: 597473a8001c
Create Date: 2026-02-07 14:13:45.841942

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "2c3325396afb"
down_revision = "597473a8001c"
branch_labels = None
depends_on = None

# The old PostgreSQL ENUM type name
ENUM_NAME = "repositorytype"


def upgrade() -> None:
  # Convert ENUM column to VARCHAR. The ENUM stores uppercase labels (SEC, INDUSTRY)
  # but the app uses lowercase values (sec, industry). LOWER() normalizes on conversion.
  op.alter_column(
    "user_repository",
    "repository_type",
    type_=sa.String(),
    existing_nullable=False,
    postgresql_using="LOWER(repository_type::text)",
  )

  # Drop the now-unused PostgreSQL ENUM type
  op.execute(f"DROP TYPE IF EXISTS {ENUM_NAME}")


def downgrade() -> None:
  # Recreate the enum type
  repo_type_enum = sa.Enum("SEC", "INDUSTRY", "ECONOMIC", name=ENUM_NAME)
  repo_type_enum.create(op.get_bind(), checkfirst=True)

  # Convert back: lowercase string values -> uppercase enum labels
  op.execute(
    "ALTER TABLE user_repository "
    f"ALTER COLUMN repository_type TYPE {ENUM_NAME} "
    "USING UPPER(repository_type)::repositorytype"
  )
