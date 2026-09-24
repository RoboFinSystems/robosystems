"""VerificationResult model — persisted outcome of a Rule evaluation.

``fact_set_id`` has no FK constraint because the engine may write results
before the FactSet commits; it is NULL for checks run outside a FactSet
(library-time structural checks).
"""

from datetime import UTC, datetime

from sqlalchemy import (
  CheckConstraint,
  Column,
  Date,
  DateTime,
  ForeignKey,
  Index,
  String,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class VerificationResult(ExtensionsBase):
  __tablename__ = "verification_results"
  __table_args__ = (
    Index("idx_verification_results_rule", "rule_id"),
    Index("idx_verification_results_structure", "structure_id"),
    Index("idx_verification_results_fact_set", "fact_set_id"),
    Index("idx_verification_results_evaluated", "evaluated_at"),
    CheckConstraint(
      "status IN ('pass', 'fail', 'error', 'skipped')",
      name="check_verification_result_status",
    ),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("vr"))

  rule_id = Column(String, ForeignKey("rules.id"), nullable=False)
  structure_id = Column(String, ForeignKey("structures.id"), nullable=True)
  fact_set_id = Column(String, nullable=True)
  entity_id = Column(String, nullable=True)

  # Period scope of the evaluation (matches the FactSet window).
  period_start = Column(Date, nullable=True)
  period_end = Column(Date, nullable=True)

  status = Column(String, nullable=False)
  message = Column(String, nullable=True)

  # Engine-side detail (bound variables, evaluated expression, numeric
  # residual, etc.) for diagnostics.
  detail = Column("detail", JSONB, nullable=False, default=dict)

  evaluated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  created_by = Column(String, nullable=False, default="system")

  def __repr__(self) -> str:
    return f"<VerificationResult {self.id} rule={self.rule_id} status={self.status}>"
