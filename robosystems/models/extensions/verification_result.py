"""VerificationResult model — persisted outcome of a Rule evaluation.

Phase iota data-model landing. Each row records one evaluation of a Rule
against a particular FactSet / Structure context:

- ``rule_id`` FKs the rule being evaluated.
- ``structure_id`` (nullable) FKs the block the rule was evaluated
  against — matches the rule's polymorphic target when applicable.
- ``fact_set_id`` (nullable) FKs the FactSet the evaluation bound
  ``$Variable`` references against. Null for rules that run outside a
  FactSet context (e.g. library-time structural checks).
- ``status`` IN ('pass','fail','error','skipped') captures the
  outcome; ``message`` carries the human-readable explanation.

The engine (Phase δ.3) is the producer; this model exists so engine
output has a durable home that MCP tools + the block viewer can read.
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
