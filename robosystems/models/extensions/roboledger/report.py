"""Report: a named, period-scoped, lockable package of FactSets (one per
Structure), and the unit that materializes to the graph.

Two orthogonal lifecycles: ``generation_status`` (pending → generating →
complete → published) tracks computation; ``filing_status`` (draft →
under_review → filed → archived) tracks review, and ``filed`` is immutable.
A restatement is a new Report linked by ``supersedes_id`` /
``superseded_by_id``.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  Boolean,
  CheckConstraint,
  Column,
  Date,
  DateTime,
  Float,
  ForeignKey,
  Index,
  Integer,
  String,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class Report(ExtensionsBase):
  __tablename__ = "reports"
  __table_args__ = (
    Index("idx_reports_taxonomy", "taxonomy_id"),
    Index("idx_reports_status", "generation_status"),
    Index("idx_reports_filing_status", "filing_status"),
    Index("idx_reports_supersedes", "supersedes_id"),
    CheckConstraint(
      "filing_status IN ('draft', 'under_review', 'filed', 'archived')",
      name="check_report_filing_status",
    ),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("rpt"))
  name = Column(String, nullable=False)
  description = Column(String, nullable=True)

  # Determines which structures (IS, BS, CF) are available.
  taxonomy_id = Column(String, nullable=False)

  mapping_id = Column(String, nullable=True)
  period_type = Column(String, nullable=False, default="monthly")
  period_start = Column(Date, nullable=True)
  period_end = Column(Date, nullable=True)
  comparative = Column(Boolean, nullable=False, default=True)

  # Ordered N-column period specs, e.g. {"start": "2026-01-01", "end":
  # "2026-01-31", "label": "Jan 2026"}; overrides period_start/period_end/
  # comparative when set.
  periods = Column(JSONB, nullable=True)

  # ``generation_count`` increments per (re)generation so each exported bundle
  # stays addressable; ``bundle_url`` is the latest JSON-LD export.
  graph_report_id = Column(String, nullable=True)
  last_generated = Column(DateTime, nullable=True)
  generation_status = Column(String, nullable=False, default="pending")
  generation_count = Column(Integer, nullable=False, default=0, server_default="0")
  bundle_url = Column(String, nullable=True)

  # ``archived`` is for superseded versions.
  filing_status = Column(String, nullable=False, default="draft")
  filed_at = Column(DateTime(timezone=True), nullable=True)
  filed_by = Column(String, nullable=True)

  supersedes_id = Column(String, ForeignKey("reports.id"), nullable=True)
  superseded_by_id = Column(String, ForeignKey("reports.id"), nullable=True)

  ai_generated = Column(Boolean, nullable=False, default=False)
  ai_intent = Column(String, nullable=True)
  ai_workspace_id = Column(String, nullable=True)
  ai_confidence = Column(Float, nullable=True)

  # Set on reports received from another graph.
  source_graph_id = Column(String, nullable=True)
  source_report_id = Column(String, nullable=True)
  shared_at = Column(DateTime, nullable=True)

  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)

  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  updated_at = Column(
    DateTime,
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )
  created_by = Column(String, nullable=False)

  def __repr__(self) -> str:
    return f"<Report {self.name} {self.taxonomy_id}>"
