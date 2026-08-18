"""Entry model — journal entries.

Maps to Entry nodes in the graph. Each entry must independently balance
(total debits = total credits across its line items). One transaction
can have multiple entries (posting, payment, reversal, adjustment).
For QuickBooks data, the mapping is 1:1 (QB is pre-journalized).
"""

from datetime import UTC, datetime

from sqlalchemy import (
  CheckConstraint,
  Column,
  Date,
  DateTime,
  ForeignKey,
  Index,
  Integer,
  String,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid

# Allowed values for Entry.provenance (entry origin). Single source of truth
# for the CHECK constraint below AND for the value any write path may assign;
# adding a provenance to a writer without adding it here fails the constraint
# at insert time (see tests/operations/event_block/test_engine.py). Migrations
# carry their own static snapshot — they must not import model constants.
ENTRY_PROVENANCE_VALUES = (
  "source_sync",
  "ai_generated",
  "manual_entry",
  "schedule_derived",
  "system_computed",
  "event_handler",
)


class Entry(ExtensionsBase):
  __tablename__ = "entries"
  __table_args__ = (
    Index("idx_entries_transaction", "transaction_id"),
    Index("idx_entries_posting_date", "posting_date"),
    Index("idx_entries_status", "status"),
    Index("idx_entries_type", "type"),
    Index(
      "idx_entries_source_structure",
      "source_structure_id",
      postgresql_where="source_structure_id IS NOT NULL",
    ),
    Index(
      "idx_entries_triggered_by_event",
      "triggered_by_event_id",
      postgresql_where="triggered_by_event_id IS NOT NULL",
    ),
    CheckConstraint(
      "status IN ('draft', 'posted', 'reversed')",
      name="check_entry_status",
    ),
    # An entry is reversed at most once. This is a real invariant, not a
    # concurrency workaround: two reversing entries against one original is
    # wrong however it arose — a double-clicked button, a retried request, a
    # race. `reverse_journal_entry` locks the original so the ordinary path
    # returns a clean conflict instead of an IntegrityError, but the guarantee
    # belongs here, where nothing can route around it.
    Index(
      "uq_entries_one_reversal_per_original",
      "reversal_of",
      unique=True,
      postgresql_where="reversal_of IS NOT NULL",
    ),
    CheckConstraint(
      "type IN ('standard', 'adjusting', 'closing', 'reversing')",
      name="check_entry_type",
    ),
    CheckConstraint(
      "provenance IN ("
      + ", ".join(f"'{v}'" for v in ENTRY_PROVENANCE_VALUES)
      + ") OR provenance IS NULL",
      name="ck_entries_provenance",
    ),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("je"))
  number = Column(String, nullable=True)
  idempotency_key = Column(String, unique=True, nullable=True)

  # Relationship
  transaction_id = Column(String, ForeignKey("transactions.id"), nullable=True)

  # Classification
  type = Column(String, nullable=False, default="standard")
  reversal_of = Column(String, ForeignKey("entries.id"), nullable=True)

  # Provenance — links closing entries back to the schedule structure they came from
  source_structure_id = Column(String, nullable=True)

  # Event audit chain — links this entry to the business event that caused it.
  # Set by event handlers (e.g. asset_disposed); NULL when no event drove the
  # entry. Parallel to transactions.triggered_by_event_id but covers the
  # Entry-only case (closing entries created via create_manual_closing_entry
  # have no parent Transaction row).
  triggered_by_event_id = Column(String, nullable=True)

  # Origin tracking — where this entry came from
  provenance = Column(
    String, nullable=True
  )  # ENTRY_PROVENANCE_VALUES: source_sync, ai_generated, manual_entry,
  # schedule_derived, system_computed, event_handler

  # Dates
  posting_date = Column(Date, nullable=False)

  # Description
  memo = Column(String, nullable=True)

  # State
  status = Column(String, nullable=False, default="draft")
  posted_at = Column(DateTime, nullable=True)

  # Metadata
  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)
  version = Column(Integer, nullable=False, default=1)

  # Timestamps
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  updated_at = Column(
    DateTime,
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )
  created_by = Column(String, nullable=False)

  def __repr__(self) -> str:
    return f"<Entry {self.id} {self.type} {self.status}>"
