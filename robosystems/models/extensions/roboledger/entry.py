"""Journal entries. Each entry balances on its own; a transaction may have
several (posting, payment, reversal, adjustment), except QuickBooks data,
which is 1:1 because QB is pre-journalized."""

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

# Entry.provenance vocabulary for the CHECK below and every writer. Migrations
# keep their own static snapshot rather than importing this.
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
    # An entry is reversed at most once. `reverse_journal_entry` also locks the
    # original so the normal path gets a clean conflict, but this is the
    # guarantee.
    Index(
      "uq_entries_one_reversal_per_original",
      "reversal_of",
      unique=True,
      postgresql_where="reversal_of IS NOT NULL",
    ),
    # One closing entry per schedule per period: concurrent dispatchers (the
    # sweep and a manual `schedule_entry_due`) can both pass the unlocked
    # application check. `posting_date` stands for the period because
    # obligations are minted with `posting_date = period_end`. Manual closing
    # entries have no source_structure_id, and `reversal_of IS NULL` exempts
    # the auto-reversal (keyed on the link because `type` is caller-authored).
    # A different `posting_date` for the same period slips past; the
    # `ScheduleService` row lock covers that.
    Index(
      "uq_entries_one_primary_per_schedule_period",
      "source_structure_id",
      "posting_date",
      unique=True,
      postgresql_where="source_structure_id IS NOT NULL AND reversal_of IS NULL",
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

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("je"))
  number = Column(String, nullable=True)
  idempotency_key = Column(String, unique=True, nullable=True)

  transaction_id = Column(String, ForeignKey("transactions.id"), nullable=True)

  type = Column(String, nullable=False, default="standard")
  reversal_of = Column(String, ForeignKey("entries.id"), nullable=True)

  # The schedule structure a closing entry came from.
  source_structure_id = Column(String, nullable=True)

  # The business event that caused this entry. Parallel to
  # transactions.triggered_by_event_id, for entries with no Transaction row.
  triggered_by_event_id = Column(String, nullable=True)

  provenance = Column(String, nullable=True)  # ENTRY_PROVENANCE_VALUES

  posting_date = Column(Date, nullable=False)

  memo = Column(String, nullable=True)

  status = Column(String, nullable=False, default="draft")
  posted_at = Column(DateTime, nullable=True)

  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)
  version = Column(Integer, nullable=False, default=1)

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
