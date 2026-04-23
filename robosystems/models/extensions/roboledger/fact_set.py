"""FactSet model — period-specific instantiation of a Structure.

Phase ζ data-model landing. From the spec §5.3:

    A Structure accumulates many FactSets over time — one per period run.
    Structure = Information Model + Mechanics declaration (persistent).
    FactSet    = a period-specific instantiation of that Structure.
    Information Block envelope = Structure + FactSet for a given period.

The table exists so statements and schedules share one period-scoped
grouping concept instead of the split today (statements use ``report_id``,
schedules use ``fact_set_id`` as a free-floating string). Over the expand
pass, ``create_report`` creates a FactSet row first and stamps all facts
with ``fact_set_id``; ``report_id`` retires once every reader resolves
through FactSet.

The blitz ships the table + FKs + writer support. Backfill of existing
rows, ``report_id`` column retirement, and the dimension seeder are
expand-pass work.
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
  # blocks (Phase η). Enum closure enforced by CHECK above.
  factset_type = Column(String, nullable=False, default="report")

  # Multi-tenant + cross-link fields. ``entity_id`` matches
  # ``facts.entity_id`` so queries can bound FactSet scans to one entity
  # without joining facts.
  entity_id = Column(String, nullable=False)

  # Optional ``report_id`` back-pointer — populated while reports still
  # live in the ``reports`` table; drops out when ``report_id`` retires.
  report_id = Column(String, nullable=True)

  # Provenance + free-form metadata (render config pins, template id at
  # creation time, agent prompt, etc.).
  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)

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
