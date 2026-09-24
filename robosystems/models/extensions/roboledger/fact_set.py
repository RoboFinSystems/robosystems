"""FactSet: a period-specific instantiation of a Structure.

A Structure accumulates one FactSet per period run; an Information Block
envelope is Structure + FactSet. Facts reference their FactSet (deleting it
cascades to them), and only the FactSet points at a parent Report.
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
  event,
  text,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class ProvenanceRequiredError(ValueError):
  """Raised when a FactSet is inserted without a provenance descriptor.

  ``create_fact_set`` stamps it; the ``before_insert`` backstop below makes
  any other insert fail rather than produce an ungrounded fact.
  """


class FactSet(ExtensionsBase):
  __tablename__ = "fact_sets"
  __table_args__ = (
    Index("idx_fact_sets_structure", "structure_id"),
    Index("idx_fact_sets_period", "period_start", "period_end"),
    Index("idx_fact_sets_entity", "entity_id"),
    Index("idx_fact_sets_report", "report_id"),
    Index(
      "idx_fact_sets_scenario",
      "scenario_id",
      postgresql_where=text("scenario_id IS NOT NULL"),
    ),
    CheckConstraint(
      "factset_type IN ('report', 'schedule', 'custom', 'disclosure', 'metric')",
      name="check_fact_set_type",
    ),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("fs"))

  # Nullable in the DB, but every write path populates it.
  structure_id = Column(String, ForeignKey("structures.id"), nullable=True)

  period_start = Column(Date, nullable=True)
  period_end = Column(Date, nullable=False)

  # report: statement renderers. schedule: closing-entry generators.
  # custom: agent-authored derivative blocks. disclosure: the standing
  # Document->fact text-block binds report builds snapshot from. metric: one
  # standing computed-metric set per (structure, entity, period_end).
  factset_type = Column(String, nullable=False, default="report")

  # Matches facts.entity_id, so scans bound to one entity skip the join.
  entity_id = Column(String, nullable=False)

  # Nullable for the cross-graph share path, which learns the target Report id
  # only after copying the snapshot.
  report_id = Column(String, nullable=True)

  # Forecast scenario: NULL is actuals; otherwise the owning ``forecast``
  # Structure. CASCADE, never SET NULL: an orphaned scenario set demoted to
  # NULL would masquerade as actuals.
  scenario_id = Column(
    String,
    ForeignKey("structures.id", ondelete="CASCADE"),
    nullable=True,
  )

  # Free-form (render pins, template id, agent prompt); provenance has its own
  # column.
  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)

  # Typed ``FactProvenance`` (discriminated on ``origin``). Nullable for old
  # rows; required on insert by the backstop below.
  provenance = Column(JSONB, nullable=True)

  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  updated_at = Column(
    DateTime,
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )
  created_by = Column(String, nullable=False, default="system")

  def __repr__(self) -> str:
    return (
      f"<FactSet {self.id} structure={self.structure_id} "
      f"period={self.period_start}→{self.period_end} type={self.factset_type}>"
    )


@event.listens_for(FactSet, "before_insert")
def _require_provenance(_mapper, _connection, target: FactSet) -> None:
  """Reject a FactSet insert without provenance. Insert only, and a presence
  check only: ``create_fact_set`` already validated the typed union."""
  prov = target.provenance
  if not prov or not isinstance(prov, dict) or "origin" not in prov:
    raise ProvenanceRequiredError(
      f"FactSet {target.id!r} (type={target.factset_type!r}) inserted without a "
      "provenance descriptor. Construct it via "
      "operations.roboledger.fact_set.create_fact_set so it is stamped."
    )
