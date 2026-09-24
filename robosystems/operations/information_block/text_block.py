"""Envelope builder for text-block disclosure structures.

A text-block note holds narrative facts bound via ``bind-text-block``, so
``view.rendering`` is one row per narrative fact rather than a numeric grid.
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
  elements_to_lites,
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

  Facts come from ``fact_set_id`` when pinned, else the latest FactSet.
  ``None`` when the structure is missing, isn't a ``regulatory_disclosure``,
  or has neither content nor arcs; an unbound note renders empty.
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
    elements=elements_to_lites(session, atoms.elements),
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
