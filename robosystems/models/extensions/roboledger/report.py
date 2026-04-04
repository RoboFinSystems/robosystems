"""Report model — generated report configurations.

Stores the configuration needed to produce a report. A report is tied to a
Taxonomy (which contains multiple Structures like IS, BS, CF). Facts are
generated for all mapped elements across all structures in the taxonomy.
References Report/Fact/FactSet nodes in the graph after materialization.
"""

from datetime import UTC, datetime

from sqlalchemy import Boolean, Column, Date, DateTime, Float, Index, String
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class Report(ExtensionsBase):
  __tablename__ = "reports"
  __table_args__ = (
    Index("idx_reports_taxonomy", "taxonomy_id"),
    Index("idx_reports_status", "generation_status"),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("rpt"))
  name = Column(String, nullable=False)
  description = Column(String, nullable=True)

  # Taxonomy — determines which structures (IS, BS, CF) are available
  taxonomy_id = Column(String, nullable=False)

  # Configuration
  mapping_id = Column(String, nullable=True)
  period_type = Column(String, nullable=False, default="monthly")
  period_start = Column(Date, nullable=True)
  period_end = Column(Date, nullable=True)
  comparative = Column(Boolean, nullable=False, default=True)

  # Multi-period support — ordered list of period specs for N-column reports
  # Each entry: {"start": "2026-01-01", "end": "2026-01-31", "label": "Jan 2026"}
  # When set, overrides period_start/period_end/comparative for fact generation.
  periods = Column(JSONB, nullable=True)

  # Generated output references
  graph_report_id = Column(String, nullable=True)
  last_generated = Column(DateTime, nullable=True)
  generation_status = Column(String, nullable=False, default="pending")

  # AI provenance
  ai_generated = Column(Boolean, nullable=False, default=False)
  ai_intent = Column(String, nullable=True)
  ai_workspace_id = Column(String, nullable=True)
  ai_confidence = Column(Float, nullable=True)

  # Sharing provenance (populated on received/shared reports, null for local)
  source_graph_id = Column(String, nullable=True)
  source_report_id = Column(String, nullable=True)
  shared_at = Column(DateTime, nullable=True)

  # Metadata
  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)

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
    return f"<Report {self.name} {self.taxonomy_id}>"
