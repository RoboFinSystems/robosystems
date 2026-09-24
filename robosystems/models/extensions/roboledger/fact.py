"""Facts: one element's value for one period, numeric or not.

``fact_type`` discriminates numeric from non-numeric (text-block) facts, and
``ck_facts_value_shape`` requires exactly one of ``value`` / ``string_value``.
Every fact belongs to one FactSet, which is created first.
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
  # Always 'inline' today; 'external_resource' is reserved.
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
  structure_id = Column(String, nullable=True)
  fact_set_id = Column(
    String, ForeignKey("fact_sets.id", ondelete="CASCADE"), nullable=False
  )
  # "historical" facts are already in opening balances and ignored by close;
  # "in_scope" facts are drafted into entries.
  fact_scope = Column(String, nullable=False, default="in_scope")
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))

  def __repr__(self) -> str:
    shown = self.value if self.value is not None else self.string_value
    return f"<Fact {self.element_id} = {shown}>"
