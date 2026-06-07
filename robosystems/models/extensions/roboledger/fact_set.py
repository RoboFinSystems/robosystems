"""FactSet model — period-specific instantiation of a Structure.

A Structure accumulates many FactSets over time — one per period run.
Structure = Information Model + Mechanics declaration (persistent);
FactSet = a period-specific instantiation of that Structure;
Information Block envelope = Structure + FactSet for a given period.

The table provides one period-scoped grouping concept that statements
and schedules share. ``create_report`` creates a FactSet row first and
stamps all facts with ``fact_set_id`` (the FK on
``facts.fact_set_id`` is NOT NULL ON DELETE CASCADE, so deleting the
FactSet cascades to its facts). FactSet carries its own ``report_id``
back-pointer to the parent Report — the ``facts.report_id`` column
was retired in migration 0010.
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
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class ProvenanceRequiredError(ValueError):
  """Raised when a FactSet is inserted without a provenance descriptor.

  Every FactSet must record how its facts were constructed (the
  auditability spine — see ``models/api/fact_provenance``). Stamping is
  mandatory at emission the way double-entry balance is mandatory: the
  blessed ``operations/roboledger/fact_set.create_fact_set`` helper sets
  it, and the ``before_insert`` backstop below makes an unstamped insert
  fail rather than silently produce an ungrounded fact.
  """


class FactSet(ExtensionsBase):
  __tablename__ = "fact_sets"
  __table_args__ = (
    Index("idx_fact_sets_structure", "structure_id"),
    Index("idx_fact_sets_period", "period_start", "period_end"),
    Index("idx_fact_sets_entity", "entity_id"),
    Index("idx_fact_sets_report", "report_id"),
    CheckConstraint(
      "factset_type IN ('report', 'schedule', 'custom')",
      name="check_fact_set_type",
    ),
  )

  # Identity — ``fs_``-prefixed ULID matches the id shape
  # ``schedules/service.py`` already stamps on ``facts.fact_set_id`` today.
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("fs"))

  # Structure the fact set instantiates. Nullable because the expand-pass
  # backfill may encounter historic rows whose structure linkage was
  # implicit; new writes always populate it.
  structure_id = Column(String, ForeignKey("structures.id"), nullable=True)

  # Period coverage — same semantics as ``facts.period_start/period_end``.
  period_start = Column(Date, nullable=True)
  period_end = Column(Date, nullable=False)

  # Kind of FactSet — 'report' for statement renderers, 'schedule' for
  # closing-entry generators, 'custom' for agent-authored derivative
  # blocks. Enum closure enforced by the CHECK constraint above.
  factset_type = Column(String, nullable=False, default="report")

  # Multi-tenant + cross-link fields. ``entity_id`` matches
  # ``facts.entity_id`` so queries can bound FactSet scans to one entity
  # without joining facts.
  entity_id = Column(String, nullable=False)

  # ``report_id`` back-pointer to the parent Report. Nullable so the
  # cross-graph share path can mint a FactSet that references a target
  # Report whose id isn't known until the snapshot is copied; report
  # facts created via ``create_report`` always populate it.
  report_id = Column(String, nullable=True)

  # Free-form metadata (render config pins, template id at creation time,
  # agent prompt, etc.). NOTE: typed fact provenance lives on its own
  # first-class ``provenance`` column below, not in this bag.
  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)

  # Typed ``FactProvenance`` descriptor (discriminated on ``origin``) —
  # how this FactSet's facts were constructed. Nullable in the DB so
  # pre-feature historical rows stay valid; mandatoriness for *new*
  # inserts is enforced at the application boundary (the ``before_insert``
  # backstop below + the ``create_fact_set`` helper), not by a DB NOT NULL.
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
  """Mandatory-at-emission backstop: a new FactSet must carry provenance.

  Fires only on INSERT, so historical rows (``provenance IS NULL``) are
  never re-validated — an UPDATE of a legacy row does not trip this. A
  cheap presence guard (not a full re-parse): the typed union is already
  validated in ``create_fact_set`` before the value lands here.
  """
  prov = target.provenance
  if not prov or not isinstance(prov, dict) or "origin" not in prov:
    raise ProvenanceRequiredError(
      f"FactSet {target.id!r} (type={target.factset_type!r}) inserted without a "
      "provenance descriptor. Construct it via "
      "operations.roboledger.fact_set.create_fact_set so it is stamped."
    )
