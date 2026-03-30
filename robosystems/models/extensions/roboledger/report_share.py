"""ReportShare model — tracks reports shared to other graphs.

When a CFO publishes a report and shares it with another graph (e.g., a
shareholder's portfolio), this record lives in the SOURCE graph's tenant
schema. The target graph receives a copy of report_definitions + report_facts
with provenance fields (source_graph_id, source_report_id) so the data
survives rebuilds and materializations.
"""

from datetime import UTC, datetime

from sqlalchemy import Column, DateTime, Index, Integer, String

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class ReportShare(ExtensionsBase):
  __tablename__ = "report_shares"
  __table_args__ = (
    Index("idx_report_shares_report", "report_id"),
    Index("idx_report_shares_target", "target_graph_id"),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("share"))
  report_id = Column(String, nullable=False)
  target_graph_id = Column(String, nullable=False)
  shared_by = Column(String, nullable=False)  # user ID
  shared_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  revoked_at = Column(DateTime, nullable=True)
  fact_count = Column(Integer, nullable=False, default=0)

  def __repr__(self) -> str:
    return f"<ReportShare {self.report_id} → {self.target_graph_id}>"
