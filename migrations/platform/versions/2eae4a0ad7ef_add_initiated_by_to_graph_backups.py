"""add initiated_by to graph_backups

Revision ID: 2eae4a0ad7ef
Revises: 5fdb8dda36e3
Create Date: 2026-08-08 21:02:25.895042

Splits two axes that ``backup_type`` was carrying at once: what shape a backup
is (full / incremental) and who started it. Conflating them meant the shape
column could not answer "is this a full dump?" for anything the platform
produced, and there was no way to exempt platform-initiated backups from the
customer's daily quota or to label them in the listing.

Existing ``backup_type = 'system'`` rows are pre-restore snapshots and
migration-export artifacts. They are re-encoded as a full backup with a system
initiator, which is what they always were.

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2eae4a0ad7ef"
down_revision = "5fdb8dda36e3"
branch_labels = None
depends_on = None


def upgrade() -> None:
  op.add_column(
    "graph_backups",
    sa.Column("initiated_by", sa.String(), server_default="user", nullable=False),
  )
  op.create_index(
    op.f("ix_graph_backups_initiated_by"),
    "graph_backups",
    ["initiated_by"],
    unique=False,
  )

  # Re-encode the rows that were using backup_type as an initiator. Ordered so
  # initiated_by is set before backup_type is overwritten — the reverse would
  # lose the only marker identifying which rows to move.
  op.execute(
    """
    UPDATE graph_backups
       SET initiated_by = 'system',
           backup_type = 'full'
     WHERE backup_type = 'system'
    """
  )


def downgrade() -> None:
  # Fold system-initiated rows back into the old shared encoding. Scheduled
  # backups have no pre-split representation, so they come back as plain user
  # backups — the distinction is genuinely absent from the old schema rather
  # than recoverable, and inventing one would be worse than losing it.
  op.execute(
    """
    UPDATE graph_backups
       SET backup_type = 'system'
     WHERE initiated_by = 'system'
    """
  )

  op.drop_index(op.f("ix_graph_backups_initiated_by"), table_name="graph_backups")
  op.drop_column("graph_backups", "initiated_by")
