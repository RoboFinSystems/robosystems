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

Also drops ``encrypted_size_bytes``. Application-layer encryption was retired
before it ever ran, and both writers always passed the compressed size into
this column, so it has never held anything but a duplicate of
``compressed_size_bytes``. That makes the drop reversible: the downgrade
repopulates it from its twin and loses nothing.

Note the contrast with ``encryption_enabled``, which stays. That column records
which backups were once *marked* encrypted — real information, and worth
keeping given the SOC 2 angle. This one records a number that was always
identical to another column.

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

  op.drop_column("graph_backups", "encrypted_size_bytes")


def downgrade() -> None:
  # Restored from its twin rather than defaulted: the column was always a copy
  # of compressed_size_bytes, so this reproduces every historical value exactly.
  op.add_column(
    "graph_backups",
    sa.Column(
      "encrypted_size_bytes", sa.BigInteger(), nullable=False, server_default="0"
    ),
  )
  op.execute("UPDATE graph_backups SET encrypted_size_bytes = compressed_size_bytes")

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
