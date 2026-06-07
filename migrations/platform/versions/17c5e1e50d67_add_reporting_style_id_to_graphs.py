"""add reporting_style_id to graphs

Every entity graph picks a Reporting Style at provision. The value is a
Structure id in the per-tenant extensions schema (the same deterministic
library UUID lives in every tenant's ``structures`` table); the renderer
picker reads this column to resolve which Network fills each
statement-type slot.

NOT NULL with a server default so the column applies cleanly to any
existing rows (Postgres uses the default to fill them before enforcing
NOT NULL). Reporting Style has not shipped to prod, so no separate
backfill step is required.

Revision ID: 17c5e1e50d67
Revises: ac32cd15e221
Create Date: 2026-05-13 00:28:29.528066

"""

import sqlalchemy as sa
from alembic import op

from robosystems.config.constants import ReportingStyleConstants

# revision identifiers, used by Alembic.
revision = "17c5e1e50d67"
down_revision = "ac32cd15e221"
branch_labels = None
depends_on = None


def upgrade() -> None:
  op.add_column(
    "graphs",
    sa.Column(
      "reporting_style_id",
      sa.String(),
      nullable=False,
      server_default=ReportingStyleConstants.DEFAULT_STYLE_ID,
    ),
  )
  # Supports future "which graphs use style X" queries
  # (style-deprecation workflows, per-style usage analytics).
  # Without this index those queries seq-scan the graphs table.
  op.create_index(
    "idx_graphs_reporting_style",
    "graphs",
    ["reporting_style_id"],
  )


def downgrade() -> None:
  op.drop_index("idx_graphs_reporting_style", table_name="graphs")
  op.drop_column("graphs", "reporting_style_id")
