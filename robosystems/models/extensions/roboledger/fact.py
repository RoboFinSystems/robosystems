"""Fact model — financial data points.

Bridge between fact generation (Python computation) and graph materialization
(postgres_scanner reads this table). Each row represents one discrete financial
data point: an element's aggregated balance for a specific period.

Every Fact belongs to exactly one FactSet (the parent envelope that pins the
period bounds and back-references the Report or Schedule that created it).
Reports stamp facts via ``_persist_report_facts``; schedules stamp them via
``ScheduleService``. Both create the FactSet row before the facts that
reference it.

Written by generate_report_facts(), read by ExtensionsMaterializer and
render_structure_view().
"""

from datetime import UTC, datetime

from sqlalchemy import (
  CheckConstraint,
  Column,
  Date,
  DateTime,
  Float,
  ForeignKey,
  Index,
  String,
  text,
)

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class Fact(ExtensionsBase):
  __tablename__ = "facts"
  __table_args__ = (
    Index("idx_facts_element", "element_id"),
    Index("idx_facts_period", "period_start", "period_end"),
    Index("idx_facts_fact_set", "fact_set_id"),
    Index("idx_facts_structure", "structure_id"),
    Index(
      "idx_facts_scope_in_scope",
      "structure_id",
      "period_start",
      postgresql_where=text("fact_scope = 'in_scope'"),
    ),
    CheckConstraint(
      "fact_scope IN ('historical', 'in_scope')",
      name="ck_facts_scope",
    ),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("fact"))
  element_id = Column(String, nullable=False)
  value = Column(Float, nullable=False)  # natural-sign dollars
  period_start = Column(Date, nullable=True)
  period_end = Column(Date, nullable=False)
  period_type = Column(String, nullable=False)  # duration or instant
  unit = Column(String, nullable=False, default="USD")
  entity_id = Column(String, nullable=False)
  structure_id = Column(String, nullable=True)  # structure this fact belongs to
  # FK → fact_sets.id with ON DELETE CASCADE. The FactSet is created
  # before the fact is stamped. The column is NOT NULL as of
  # migration 0010 — every fact has exactly one parent FactSet; deleting
  # the FactSet cascades to its facts.
  fact_set_id = Column(
    String, ForeignKey("fact_sets.id", ondelete="CASCADE"), nullable=False
  )
  # fact_scope distinguishes "historical" (already reflected in opening balances,
  # ignored by close workflow) from "in_scope" (close workflow drafts entries from
  # these). Defaults to 'in_scope' so existing facts and non-schedule facts are
  # always visible to existing queries.
  fact_scope = Column(String, nullable=False, default="in_scope")
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))

  def __repr__(self) -> str:
    return f"<Fact {self.element_id} = {self.value}>"
