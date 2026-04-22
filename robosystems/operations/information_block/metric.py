"""Handlers for ``block_type='metric'`` — the derivative construction mode.

Phase η data-model landing: ``MetricMechanics`` now exists as a typed
arm of the ``ArtifactMechanics`` discriminated union, and ``metric`` is
registered as a known block type. The derivation evaluator that
actually computes metric values from source-block FactSets ships in a
follow-up — the create/update/delete handlers raise ``NotImplementedError``
in the meantime, mirroring the statement-family handler pattern.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

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


def _create_not_implemented(session: Session, body: Any, created_by: str) -> str:
  """Create raises until Phase η's expand pass lands the evaluator."""
  raise NotImplementedError(
    "create-metric-block is not implemented yet. Phase η ships the "
    "typed MetricMechanics arm; the derivation evaluator + create path "
    "land in a follow-up once the Rule engine (Phase δ.3) stabilizes."
  )


def _update_not_implemented(session: Session, body: Any, created_by: str) -> str:
  raise NotImplementedError(
    "update-metric-block is not implemented yet (Phase η follow-up)."
  )


def _delete_not_implemented(session: Session, body: Any, created_by: str) -> str:
  raise NotImplementedError(
    "delete-metric-block is not implemented yet (Phase η follow-up)."
  )


def build_envelope(
  session: Session, structure_id: str
) -> InformationBlockEnvelope | None:
  """Reload a metric Structure and pack it into the Information Block envelope.

  Pre-evaluator behaviour: mechanics are read off the typed
  ``artifact_mechanics`` column; ``facts`` stays empty until the
  derivation evaluator lands. The envelope shape is stable so callers
  and UI can already render a metric block as a placeholder.
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
  "_create_not_implemented",
  "_delete_not_implemented",
  "_update_not_implemented",
  "build_envelope",
]
