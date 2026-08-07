"""Fact model — financial data points.

Bridge between fact generation (Python computation) and graph materialization
(postgres_scanner reads this table). Each row represents one discrete financial
data point: an element's aggregated balance for a specific period, or a
non-numeric (string / text-block) value such as a bound disclosure narrative.

A fact is numeric XOR non-numeric: ``fact_type`` discriminates, and the
``ck_facts_value_shape`` CHECK enforces exactly one of ``value`` /
``string_value`` populated. This mirrors the graph ``Fact`` node in
``schemas/extensions/roboledger.py``.

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
  Text,
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
    CheckConstraint(
      "fact_type IN ('Numeric', 'Nonnumeric')",
      name="ck_facts_fact_type",
    ),
    CheckConstraint(
      "value_type IN ('inline', 'external_resource')",
      name="ck_facts_value_type",
    ),
    CheckConstraint(
      "(fact_type = 'Numeric' AND value IS NOT NULL AND string_value IS NULL) OR "
      "(fact_type = 'Nonnumeric' AND string_value IS NOT NULL AND value IS NULL)",
      name="ck_facts_value_shape",
    ),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("fact"))
  element_id = Column(String, nullable=False)
  value = Column(Float, nullable=True)  # natural-sign dollars; NULL for Nonnumeric
  string_value = Column(Text, nullable=True)  # inline text payload for Nonnumeric
  fact_type = Column(String, nullable=False, default="Numeric")
  # value_type is always 'inline' today; 'external_resource' is reserved for
  # blocks externalized to S3/OpenSearch (the SEC pipeline pattern).
  value_type = Column(String, nullable=False, default="inline")
  content_type = Column(String, nullable=True)  # MIME, e.g. 'text/markdown'
  # XBRL @decimals for numeric facts. NULL means unspecified; materialize
  # substitutes '-2' for numeric rows so graph output stays stable.
  decimals = Column(String, nullable=True)
  period_start = Column(Date, nullable=True)
  period_end = Column(Date, nullable=False)
  period_type = Column(String, nullable=False)  # duration or instant
  # Units apply to numeric facts only; the column keeps its USD default for
  # all rows, and materialize skips the FACT_HAS_UNIT edge for Nonnumeric.
  unit = Column(String, nullable=False, default="USD")
  entity_id = Column(String, nullable=False)
  structure_id = Column(String, nullable=True)  # structure this fact belongs to
  # Every fact has exactly one parent FactSet, created before the fact is
  # stamped; deleting the FactSet cascades to its facts.
  fact_set_id = Column(
    String, ForeignKey("fact_sets.id", ondelete="CASCADE"), nullable=False
  )
  # fact_scope distinguishes "historical" (already reflected in opening
  # balances, ignored by the close workflow) from "in_scope" (the close
  # workflow drafts entries from these). Defaults to 'in_scope' so
  # non-schedule facts stay visible to scope-unaware queries.
  fact_scope = Column(String, nullable=False, default="in_scope")
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))

  def __repr__(self) -> str:
    shown = self.value if self.value is not None else self.string_value
    return f"<Fact {self.element_id} = {shown}>"
