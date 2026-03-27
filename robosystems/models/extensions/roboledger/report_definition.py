"""ReportDefinition model — generated report configurations.

Stores the configuration needed to produce a report. Reusable:
"regenerate my Income Statement for Q2" re-runs the same definition
with new dates. References Report/Fact/FactSet nodes in the graph
after materialization.
"""

from datetime import UTC, datetime

from sqlalchemy import Boolean, Column, DateTime, Float, Index, String
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class ReportDefinition(ExtensionsBase):
  __tablename__ = "report_definitions"
  __table_args__ = (
    Index("idx_report_defs_type", "report_type"),
    Index("idx_report_defs_status", "generation_status"),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("rpt"))
  name = Column(String, nullable=False)
  description = Column(String, nullable=True)

  # Report type
  report_type = Column(String, nullable=False)

  # Configuration
  mapping_id = Column(String, nullable=True)
  period_type = Column(String, nullable=False, default="monthly")
  comparative = Column(Boolean, nullable=False, default=True)

  # Generated output references
  graph_report_id = Column(String, nullable=True)
  last_generated = Column(DateTime, nullable=True)
  generation_status = Column(String, nullable=False, default="pending")

  # AI provenance
  ai_generated = Column(Boolean, nullable=False, default=False)
  ai_intent = Column(String, nullable=True)
  ai_workspace_id = Column(String, nullable=True)
  ai_confidence = Column(Float, nullable=True)

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
    return f"<ReportDefinition {self.name} {self.report_type}>"
