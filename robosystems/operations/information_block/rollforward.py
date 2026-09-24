"""Handlers for ``block_type='rollforward'``: a balance-sheet source element
plus attribution filters, persisted as ``RollforwardMechanics``.

The filter engine
(:mod:`robosystems.operations.roboledger.reports.rollforward_filters`) is not
wired into rendering, so the envelope carries no facts.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.rollforward import (
  AttributionFilter,
  CreateRollforwardRequest,
  DeleteRollforwardRequest,
  UpdateRollforwardRequest,
)
from robosystems.models.api.information_block import (
  ArtifactResponse,
  InformationBlockEnvelope,
  InformationModelResponse,
  RollforwardMechanics,
)
from robosystems.models.extensions import Element, Structure
from robosystems.operations.information_block.envelope import (
  association_to_connection,
  elements_to_lites,
  load_base_envelope_atoms,
)

ROLLFORWARD_BLOCK_TYPE = "rollforward"
ROLLFORWARD_DISPLAY_NAME = "Rollforward"
ROLLFORWARD_CATEGORY = "Reporting"


def _resolve_qname(session: Session, qname: str, *, what: str) -> Element:
  """Resolve a qname to its tenant Element row. Raises ValueError on miss;
  ``what`` names the field in the message."""
  el = session.execute(select(Element).where(Element.qname == qname)).scalar()
  if el is None:
    raise ValueError(
      f"Rollforward {what} qname={qname!r} did not resolve to an Element. "
      "The taxonomy may not be loaded for this tenant, or the qname has a "
      "typo."
    )
  return el


def _resolve_filters(
  session: Session, filters: list[AttributionFilter]
) -> list[AttributionFilter]:
  """Fill each filter's ``target_element_id`` from its ``target_qname``."""
  resolved: list[AttributionFilter] = []
  for i, f in enumerate(filters):
    el = _resolve_qname(session, f.target_qname, what=f"filter target #{i}")
    resolved.append(
      AttributionFilter(
        target_qname=f.target_qname,
        target_element_id=el.id,
        predicate=f.predicate,
      )
    )
  return resolved


def create(
  session: Session,
  payload: CreateRollforwardRequest,
  created_by: str,
) -> str:
  """Create a rollforward block; returns the new structure_id."""
  bs_element = _resolve_qname(session, payload.bs_source_qname, what="bs_source")
  default_tag_element = (
    _resolve_qname(session, payload.default_change_tag_qname, what="default_change_tag")
    if payload.default_change_tag_qname
    else None
  )
  resolved_filters = _resolve_filters(session, payload.attribution_filters)

  mechanics = RollforwardMechanics(
    bs_source_element_id=bs_element.id,
    bs_source_qname=payload.bs_source_qname,
    default_change_tag_element_id=(
      default_tag_element.id if default_tag_element else None
    ),
    default_change_tag_qname=payload.default_change_tag_qname,
    attribution_filters=resolved_filters,
    validation_mode=payload.validation_mode,
  )

  # Default owner: the BS source element's taxonomy.
  taxonomy_id = payload.taxonomy_id or bs_element.taxonomy_id

  structure = Structure(
    name=payload.name,
    block_type=ROLLFORWARD_BLOCK_TYPE,
    taxonomy_id=taxonomy_id,
    concept_arrangement="roll_forward",
    member_arrangement=None,
    artifact_mechanics=mechanics.model_dump(mode="json"),
    metadata_={},
    created_by=created_by,
  )
  session.add(structure)
  session.flush()
  # Read the id before commit: a post-commit refresh may run on a pooled
  # connection whose search_path was reset to `public`.
  structure_id = structure.id
  session.commit()
  return structure_id


def _load_rollforward_or_404(session: Session, structure_id: str) -> Structure:
  # Locked: `update` read-modify-writes `artifact_mechanics`. Contention
  # raises `RowLockedError` (409).
  from robosystems.operations.locking import lock_by_id

  structure = lock_by_id(
    session,
    Structure,
    structure_id,
    f"Rollforward {structure_id} is being written by another process. Retry in a moment.",
  )
  if structure is None or structure.block_type != ROLLFORWARD_BLOCK_TYPE:
    raise ValueError(
      f"Rollforward structure_id={structure_id!r} not found (or wrong block_type)."
    )
  return structure


def update(
  session: Session,
  payload: UpdateRollforwardRequest,
  updated_by: str,
) -> str:
  """Update a rollforward block in place.

  The BS source is immutable; delete and re-create to change it.
  """
  structure = _load_rollforward_or_404(session, payload.structure_id)
  current = RollforwardMechanics.model_validate(structure.artifact_mechanics or {})

  if payload.name is not None:
    structure.name = payload.name

  # None leaves the default tag unchanged; it can't be cleared by update.
  default_tag_element_id = current.default_change_tag_element_id
  default_tag_qname = current.default_change_tag_qname
  if payload.default_change_tag_qname is not None:
    default_tag_el = _resolve_qname(
      session, payload.default_change_tag_qname, what="default_change_tag"
    )
    default_tag_element_id = default_tag_el.id
    default_tag_qname = payload.default_change_tag_qname

  attribution_filters = current.attribution_filters
  if payload.attribution_filters is not None:
    attribution_filters = _resolve_filters(session, payload.attribution_filters)

  validation_mode = (
    payload.validation_mode
    if payload.validation_mode is not None
    else current.validation_mode
  )

  next_mechanics = RollforwardMechanics(
    bs_source_element_id=current.bs_source_element_id,
    bs_source_qname=current.bs_source_qname,
    default_change_tag_element_id=default_tag_element_id,
    default_change_tag_qname=default_tag_qname,
    attribution_filters=attribution_filters,
    validation_mode=validation_mode,
  )
  structure.artifact_mechanics = next_mechanics.model_dump(mode="json")
  structure.updated_by = updated_by
  session.flush()
  structure_id = structure.id
  session.commit()
  return structure_id


def delete(
  session: Session,
  payload: DeleteRollforwardRequest,
  deleted_by: str,
) -> str:
  """Hard-delete a rollforward block; ledger LineItems are untouched."""
  structure = _load_rollforward_or_404(session, payload.structure_id)
  structure_id = structure.id
  session.delete(structure)
  session.commit()
  return structure_id


def build_envelope(
  session: Session,
  structure_id: str,
  fact_set_id: str | None = None,
  scenario_id: str | None = None,
  series: bool = False,
  series_history: int | None = None,
  series_forecast: int | None = None,
) -> InformationBlockEnvelope | None:
  """Reload a rollforward Structure and pack its envelope (no facts;
  ``scenario_id`` is ignored). ``None`` when not found or not a rollforward.
  """
  atoms = load_base_envelope_atoms(
    session,
    structure_id,
    expected_block_type=ROLLFORWARD_BLOCK_TYPE,
    fact_set_id=fact_set_id,
  )
  if atoms is None:
    return None

  structure = atoms.structure
  mechanics = RollforwardMechanics.model_validate(structure.artifact_mechanics or {})

  return InformationBlockEnvelope(
    id=structure.id,
    block_type=ROLLFORWARD_BLOCK_TYPE,
    name=structure.name,
    display_name=ROLLFORWARD_DISPLAY_NAME,
    category=ROLLFORWARD_CATEGORY,
    taxonomy_id=structure.taxonomy_id,
    taxonomy_name=atoms.taxonomy_name,
    disclosure_id=None,
    information_model=InformationModelResponse(
      concept_arrangement=structure.concept_arrangement or "roll_forward",
      member_arrangement=structure.member_arrangement,
    ),
    artifact=ArtifactResponse(
      topic=structure.description,
      renderer_note=structure.renderer_note,
      template=None,
      mechanics=mechanics,
    ),
    elements=elements_to_lites(session, atoms.elements),
    connections=[
      association_to_connection(a, atoms.classifications_by_assoc.get(a.id, []))
      for a in atoms.associations
    ],
    facts=[],
    rules=atoms.rules,
    fact_set=atoms.fact_set,
    verification_results=atoms.verification_results,
    verification_summary=atoms.verification_summary,
  )


__all__ = [
  "ROLLFORWARD_BLOCK_TYPE",
  "ROLLFORWARD_CATEGORY",
  "ROLLFORWARD_DISPLAY_NAME",
  "build_envelope",
  "create",
  "delete",
  "update",
]
