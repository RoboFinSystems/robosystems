"""StructureTemplate model — reusable layout/configuration for a Structure.

Reusable templates for Structure rows. A template defines the canonical
shape (axes, mechanics, slot ordering, subtotal labels) that one or more
Structures instantiate via ``structures.template_id``. Library-seeded
templates (the four statement types + common Schedule variants) share
the vocabulary with tenant-authored templates so the same column powers
both cases.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  CheckConstraint,
  Column,
  DateTime,
  Index,
  String,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class StructureTemplate(ExtensionsBase):
  __tablename__ = "structure_templates"
  __table_args__ = (
    Index("idx_structure_templates_target_type", "target_block_type"),
    CheckConstraint(
      "template_type IN ('renderer', 'schedule_variant', 'metric_preset')",
      name="check_structure_template_type",
    ),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("tmpl"))

  name = Column(String, nullable=False)
  description = Column(String, nullable=True)

  # Which block_type this template applies to ('balance_sheet',
  # 'schedule', 'metric', …). Stored as a string rather than a CHECK
  # to avoid recompiling when new block types register.
  target_block_type = Column(String, nullable=False)

  # Kind of template:
  #   renderer         — layout / subtotal / ordering overrides for
  #                      statement-family blocks
  #   schedule_variant — preset mechanics for common Schedule shapes
  #                      (straight-line depreciation, declining-balance,
  #                      units-of-production)
  #   metric_preset    — canned MetricMechanics for ratios / covenants
  template_type = Column(String, nullable=False)

  # Opaque body — renderer config, mechanics preset, whatever the
  # matching block_type consumes. JSON-LD style so future typed readers
  # can validate sub-shapes without a schema change.
  body = Column(JSONB, nullable=False, default=dict)

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
      f"<StructureTemplate {self.id} {self.target_block_type} {self.template_type}>"
    )
