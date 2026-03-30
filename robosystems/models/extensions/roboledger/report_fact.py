"""ReportFact model — generated financial data points.

Bridge between fact generation (Python computation) and graph materialization
(postgres_scanner reads this table). Each row represents one discrete financial
data point: an element's aggregated balance for a specific period.

Written by generate_report_facts(), read by LedgerMaterializer and
render_structure_view().
"""

from datetime import UTC, datetime

from sqlalchemy import Column, Date, DateTime, Float, Index, String

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class ReportFact(ExtensionsBase):
  __tablename__ = "report_facts"
  __table_args__ = (
    Index("idx_report_facts_report", "report_id"),
    Index("idx_report_facts_element", "element_id"),
    Index("idx_report_facts_period", "period_start", "period_end"),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("fact"))
  report_id = Column(String, nullable=False)
  element_id = Column(String, nullable=False)
  value = Column(Float, nullable=False)  # natural-sign dollars
  period_start = Column(Date, nullable=True)
  period_end = Column(Date, nullable=False)
  period_type = Column(String, nullable=False)  # duration or instant
  unit = Column(String, nullable=False, default="USD")
  entity_id = Column(String, nullable=False)
  fact_set_id = Column(String, nullable=True)  # structure's fact set grouping
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))

  def __repr__(self) -> str:
    return f"<ReportFact {self.element_id} = {self.value}>"
