"""Envelope builder for text-block disclosure structures.

A text-block note (``concept_arrangement`` in ``TEXT_BLOCK_CAPS``) holds
narrative — ``Nonnumeric`` facts bound from a platform Document via
``bind-text-block`` — not a numeric grid, so the statement family's
CAP-agnostic rendering (rows + footed subtotals) doesn't apply. This
builder shares every envelope atom with the statement path
(:func:`load_base_envelope_atoms`) and swaps only the ``view.rendering``
projection: one row per bound narrative fact carrying ``text_value``.

Dispatch happens in :mod:`disclosure` — ``build_envelope`` routes
text-block CAPs here and every other disclosure CAP through the
statement builder.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy import select

from robosystems.models.api.information_block import (
  ArtifactResponse,
  InformationBlockEnvelope,
  InformationModelResponse,
  RenderingLite,
  RenderingPeriodLite,
  RenderingRowLite,
  StatementMechanics,
  ViewProjections,
)
from robosystems.models.extensions.roboledger import Fact
from robosystems.operations.information_block.envelope import (
  DISCLOSURE_BLOCK_TYPE,
  association_to_connection,
  element_to_lite,
  fact_to_lite,
  load_base_envelope_atoms,
  load_disclosure_id_for_structure,
)

if TYPE_CHECKING:
  from sqlalchemy.orm import Session


def build_text_block_envelope(
  session: Session,
  structure_id: str,
  fact_set_id: str | None = None,
) -> InformationBlockEnvelope | None:
  """Pack the envelope for a text-block disclosure structure.

  Facts resolve the way statements do: pinned by ``fact_set_id`` when
  given (the report package/bundle path passes the report's snapshot
  set), else the structure's latest FactSet — which for a bound note is
  the standing ``'disclosure'`` set or the newest report snapshot,
  either of which carries the narrative.

  Returns ``None`` when the structure doesn't exist or isn't a
  ``regulatory_disclosure``. An arc-bearing but not-yet-bound note
  yields an envelope with an empty rendering (consistent with an
  unpicked numeric note).
  """
  atoms = load_base_envelope_atoms(
    session,
    structure_id,
    expected_block_type=DISCLOSURE_BLOCK_TYPE,
    fact_set_id=fact_set_id,
  )
  if atoms is None:
    return None
  structure = atoms.structure

  facts: list[Fact] = []
  if atoms.fact_set is not None:
    facts = list(
      session.execute(select(Fact).where(Fact.fact_set_id == atoms.fact_set.id))
      .scalars()
      .all()
    )

  if not facts and not atoms.associations:
    # Neither content nor arcs — a bare registry row, not a renderable
    # block (mirrors the roll_up path's arc-presence filter).
    return None

  elements_by_id = {e.id: e for e in atoms.elements}

  text_facts = [f for f in facts if f.fact_type == "Nonnumeric"]
  rows: list[RenderingRowLite] = []
  period_keys: set[tuple[date, date]] = set()
  for f in sorted(text_facts, key=lambda f: (f.element_id, f.period_end)):
    element = elements_by_id.get(f.element_id)
    rows.append(
      RenderingRowLite(
        element_id=f.element_id,
        element_qname=element.qname if element else None,
        element_name=element.name if element else f.element_id,
        text_value=f.string_value,
      )
    )
    start = f.period_start if f.period_start is not None else f.period_end
    period_keys.add((start, f.period_end))

  rendering = RenderingLite(
    rows=rows,
    periods=[
      RenderingPeriodLite(start=s, end=e)
      for s, e in sorted(period_keys, key=lambda pk: (pk[1], pk[0]))
    ],
    validation=None,
    unmapped_count=0,
  )

  if structure.artifact_mechanics:
    mechanics = StatementMechanics.model_validate(structure.artifact_mechanics)
  else:
    mechanics = StatementMechanics(kind="statement_renderer")

  disclosure_id = load_disclosure_id_for_structure(session, structure.id)
  return InformationBlockEnvelope(
    id=structure.id,
    block_type=DISCLOSURE_BLOCK_TYPE,
    name=structure.name,
    display_name=structure.name,
    category="Reporting",
    taxonomy_id=structure.taxonomy_id,
    taxonomy_name=atoms.taxonomy_name,
    disclosure_id=disclosure_id,
    information_model=InformationModelResponse(
      concept_arrangement=structure.concept_arrangement or "text_block",
      member_arrangement=structure.member_arrangement or "whole_part",
    ),
    artifact=ArtifactResponse(
      topic=structure.description,
      renderer_note=structure.renderer_note,
      template=None,
      mechanics=mechanics,
    ),
    elements=[element_to_lite(e) for e in atoms.elements],
    connections=[
      association_to_connection(a, atoms.classifications_by_assoc.get(a.id, []))
      for a in atoms.associations
    ],
    facts=[fact_to_lite(f, elements_by_id) for f in facts],
    rules=atoms.rules,
    fact_set=atoms.fact_set,
    verification_results=atoms.verification_results,
    verification_summary=atoms.verification_summary,
    view=ViewProjections(rendering=rendering),
  )


__all__ = ["build_text_block_envelope"]
