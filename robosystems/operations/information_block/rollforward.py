"""Handlers for ``block_type='rollforward'`` — declarative-mode
attribution block.

Implements **Tier 2 of the rollforward attribution design**. The user
authors a Structure declaring a BS source element and a list of
attribution filters; at render time the filter engine
(:mod:`robosystems.operations.roboledger.reports.rollforward_filters`)
evaluates the filters against ledger LineItems and produces attributed
facts per period.

This module owns the authoring surface — ``create`` / ``update`` /
``delete`` persist the typed :class:`RollforwardMechanics` to
``structures.artifact_mechanics``; ``build_envelope`` round-trips the
typed mechanics and an empty facts list. The filter engine is callable
directly from caller code (the cross-taxonomy reconciliation harness in
``local/scripts/run_reconciliation.py`` is the first consumer); filter
evaluation is not yet wired into the live ``fact_grid`` rendering
pipeline.

Pattern mirrors :mod:`robosystems.operations.information_block.schedule`
(the canonical declarative-mode reference). Differences are:
 - No pre-generated facts at create time (rollforward is lazy)
 - No domain-service object (the structure persistence is straight SQL)
 - No closed_through / fiscal_calendar coupling (period scoping is
   per-render, not per-block)
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

# Display identity — mirrored in the registry entry. Kept here so the
# envelope builder can fill the wire shape without a registry re-import
# (avoids a circular dependency — registry.py imports these handlers).
ROLLFORWARD_BLOCK_TYPE = "rollforward"
ROLLFORWARD_DISPLAY_NAME = "Rollforward"
ROLLFORWARD_CATEGORY = "Reporting"


def _resolve_qname(session: Session, qname: str, *, what: str) -> Element:
  """Resolve a qname to its tenant Element row. Raises ValueError on miss.

  ``what`` names the field for the error surface (``bs_source``,
  ``filter target #N``, ``default_change_tag``) so the caller surfaces
  a precise message at the API boundary.
  """
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
  """Replace each filter's ``target_element_id`` from its ``target_qname``.

  The wire model accepts qname-only authorship — operators don't need
  to know the element_id. The persisted form carries both for fast
  filter evaluation (we hit element_id in the SQL, not qname).
  """
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
  """Create a rollforward block. Persists the Structure row + typed
  mechanics; returns the new structure_id.

  Resolves all qnames to element_ids at write time so the filter
  evaluator can match on element_id without re-resolving on every
  read. The wire model retains the qnames for caller convenience.
  """
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

  # Default taxonomy resolution mirrors the schedule pattern — owning
  # taxonomy of the BS source element when no explicit taxonomy_id was
  # supplied. The BS source's taxonomy is the natural owner since the
  # rollforward decomposes its period change.
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
  session.commit()
  return structure.id


def _load_rollforward_or_404(session: Session, structure_id: str) -> Structure:
  structure = session.get(Structure, structure_id)
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

  Mutable: name, default_change_tag_qname, attribution_filters,
  validation_mode. BS source is immutable (changing it would invalidate
  every previously rendered period). To change BS source: delete and
  re-create.
  """
  structure = _load_rollforward_or_404(session, payload.structure_id)
  current = RollforwardMechanics.model_validate(structure.artifact_mechanics or {})

  if payload.name is not None:
    structure.name = payload.name

  # Partial-update semantics: an absent (None) ``default_change_tag_qname``
  # means "leave unchanged" — there's no wire-level way to clear the
  # default tag via update. To remove it, delete and re-create the
  # rollforward block. See UpdateRollforwardRequest's field doc.
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
  session.commit()
  return structure.id


def delete(
  session: Session,
  payload: DeleteRollforwardRequest,
  deleted_by: str,
) -> str:
  """Hard-delete a rollforward block.

  The block's facts (if any get persisted later) cascade with the
  Structure row. Underlying ledger LineItems are untouched — only the
  rollforward's projection of them.
  """
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
  """Reload a rollforward Structure and pack its envelope.

  ``fact_set_id`` is accepted for signature parity but unused —
  rollforward facts are not persisted (the filter engine is called
  directly from the reconciliation harness; the renderer wiring is not
  yet in place). The envelope's ``facts`` list is empty. ``scenario_id``
  is likewise parity-only — rollforwards attribute posted ledger lines.

  Returns ``None`` when the structure doesn't exist or isn't a
  rollforward.
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
