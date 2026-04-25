"""Handlers for ``block_type='metric'`` — the derivative construction mode.

Typed :class:`MetricMechanics` ships as one arm of the
``ArtifactMechanics`` discriminated union, and ``metric`` is registered
as a known block type. The envelope builder reads mechanics from the
typed ``artifact_mechanics`` column and surfaces the block as a
placeholder with ``facts=[]``; the derivation evaluator that computes
metric values from source-block FactSets is not yet implemented. The
create/update/delete handlers raise :class:`NotImplementedError` via
the registry's ``make_not_implemented_handler`` factory.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from robosystems.models.api.information_block import (
  ArtifactResponse,
  InformationBlockEnvelope,
  InformationModelResponse,
  MetricMechanics,
)
from robosystems.models.extensions import Structure, Taxonomy
from robosystems.operations.information_block.envelope import (
  load_latest_fact_set_for_structure,
  load_verification_results_for_structure,
)

if TYPE_CHECKING:
  from sqlalchemy.orm import Session

METRIC_BLOCK_TYPE = "metric"
METRIC_DISPLAY_NAME = "Metric"
METRIC_CATEGORY = "Reporting"


def build_envelope(
  session: Session, structure_id: str
) -> InformationBlockEnvelope | None:
  """Reload a metric Structure and pack it into the Information Block envelope.

  Mechanics are read off the typed ``artifact_mechanics`` column;
  ``facts`` stays empty until the derivation evaluator is implemented.
  The envelope shape is stable so callers and UI can already render a
  metric block as a placeholder.
  """
  from sqlalchemy import select

  structure = session.get(Structure, structure_id)
  if structure is None or structure.structure_type != METRIC_BLOCK_TYPE:
    return None

  if structure.artifact_mechanics:
    mechanics = MetricMechanics.model_validate(structure.artifact_mechanics)
  else:
    mechanics = MetricMechanics(kind="metric")

  taxonomy_name = session.execute(
    select(Taxonomy.name).where(Taxonomy.id == structure.taxonomy_id)
  ).scalar()

  fact_set = load_latest_fact_set_for_structure(session, structure_id)
  verification_results = load_verification_results_for_structure(session, structure_id)

  return InformationBlockEnvelope(
    id=structure.id,
    block_type=METRIC_BLOCK_TYPE,
    name=structure.name,
    display_name=METRIC_DISPLAY_NAME,
    category=METRIC_CATEGORY,
    taxonomy_id=structure.taxonomy_id,
    taxonomy_name=taxonomy_name,
    information_model=InformationModelResponse(
      concept_arrangement=structure.concept_arrangement or "arithmetic",
      member_arrangement=structure.member_arrangement,
    ),
    artifact=ArtifactResponse(
      topic=structure.description,
      parenthetical_note=structure.parenthetical_note,
      template=None,
      mechanics=mechanics,
    ),
    fact_set=fact_set,
    verification_results=verification_results,
  )


__all__ = [
  "METRIC_BLOCK_TYPE",
  "METRIC_CATEGORY",
  "METRIC_DISPLAY_NAME",
  "build_envelope",
]
