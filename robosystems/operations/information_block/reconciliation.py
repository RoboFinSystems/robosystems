"""Envelope builder for ``block_type='reconciliation'``: one column per
period's standing comparison, one row per concept. The comparisons are
written by ``refresh-reconciliations``; the block write slots are stubs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from robosystems.models.api.information_block import (
  ArtifactResponse,
  InformationBlockEnvelope,
  InformationModelResponse,
  ReconciliationMechanics,
  RenderingLite,
  RenderingPeriodLite,
  RenderingRowLite,
  ViewProjections,
)
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.roboledger.fact_set import FactSet
from robosystems.operations.information_block.envelope import (
  association_to_connection,
  elements_to_lites,
  fact_set_to_lite,
  fact_to_lite,
  load_base_envelope_atoms,
)

if TYPE_CHECKING:
  from sqlalchemy.orm import Session

RECONCILIATION_BLOCK_TYPE = "reconciliation"
RECONCILIATION_DISPLAY_NAME = "Reconciliation"
RECONCILIATION_CATEGORY = "Close"
# The forecast lever set's precedent: a standing set scoped by its structure.
RECONCILIATION_FACTSET_TYPE = "custom"


def standing_fact_sets(
  session: Session, structure_id: str, fact_set_id: str | None = None
) -> list[FactSet]:
  """The block's standing comparison per period_end, oldest first (or just
  ``fact_set_id`` when pinned)."""
  if fact_set_id is not None:
    row = session.get(FactSet, fact_set_id)
    return [row] if row is not None else []
  rows = (
    session.execute(
      select(FactSet)
      .where(
        FactSet.structure_id == structure_id,
        FactSet.factset_type == RECONCILIATION_FACTSET_TYPE,
        FactSet.scenario_id.is_(None),
      )
      .order_by(FactSet.period_end.asc(), FactSet.created_at.desc())
    )
    .scalars()
    .all()
  )
  by_period: dict = {}
  for fact_set in rows:
    by_period.setdefault(fact_set.period_end, fact_set)
  return list(by_period.values())


def build_envelope(
  session: Session,
  structure_id: str,
  fact_set_id: str | None = None,
  scenario_id: str | None = None,
  series: bool = False,
  series_history: int | None = None,
  series_forecast: int | None = None,
) -> InformationBlockEnvelope | None:
  """Pack a reconciliation Structure and its period-by-period comparisons.

  Always the full series; ``scenario_id`` is ignored, since a reconciliation
  checks the books. ``None`` when not found or not a reconciliation.
  """
  atoms = load_base_envelope_atoms(
    session,
    structure_id,
    expected_block_type=RECONCILIATION_BLOCK_TYPE,
    fact_set_id=fact_set_id,
  )
  if atoms is None:
    return None
  structure = atoms.structure

  fact_sets = standing_fact_sets(session, structure_id, fact_set_id)
  facts: list[Fact] = []
  if fact_sets:
    facts = list(
      session.execute(
        select(Fact).where(Fact.fact_set_id.in_([fs.id for fs in fact_sets]))
      )
      .scalars()
      .all()
    )
  value_by_set_element = {(f.fact_set_id, f.element_id): f.value for f in facts}
  elements_by_id = {e.id: e for e in atoms.elements}

  ordered_arcs = sorted(
    atoms.associations,
    key=lambda a: a.order_value if a.order_value is not None else float("inf"),
  )
  rows = [
    RenderingRowLite(
      element_id=arc.to_element_id,
      element_qname=elements_by_id[arc.to_element_id].qname,
      element_name=elements_by_id[arc.to_element_id].name,
      balance_type=elements_by_id[arc.to_element_id].balance_type,
      item_type=elements_by_id[arc.to_element_id].item_type,
      values=[value_by_set_element.get((fs.id, arc.to_element_id)) for fs in fact_sets],
    )
    for arc in ordered_arcs
    if arc.to_element_id in elements_by_id
  ]
  rendering = RenderingLite(
    rows=rows,
    periods=[
      RenderingPeriodLite(start=fs.period_end, end=fs.period_end) for fs in fact_sets
    ],
    validation=None,
    unmapped_count=0,
  )

  return InformationBlockEnvelope(
    id=structure.id,
    block_type=RECONCILIATION_BLOCK_TYPE,
    name=structure.name,
    display_name=RECONCILIATION_DISPLAY_NAME,
    category=RECONCILIATION_CATEGORY,
    taxonomy_id=structure.taxonomy_id,
    taxonomy_name=atoms.taxonomy_name,
    information_model=InformationModelResponse(
      concept_arrangement=structure.concept_arrangement,
      member_arrangement=structure.member_arrangement,
    ),
    artifact=ArtifactResponse(
      topic=structure.description,
      renderer_note=structure.renderer_note,
      template=None,
      mechanics=ReconciliationMechanics.model_validate(structure.artifact_mechanics),
    ),
    elements=elements_to_lites(session, atoms.elements),
    connections=[
      association_to_connection(a, atoms.classifications_by_assoc.get(a.id, []))
      for a in atoms.associations
    ],
    facts=[fact_to_lite(f, elements_by_id) for f in facts],
    rules=atoms.rules,
    fact_set=fact_set_to_lite(fact_sets[-1]) if fact_sets else atoms.fact_set,
    verification_results=atoms.verification_results,
    verification_summary=atoms.verification_summary,
    view=ViewProjections(rendering=rendering),
  )


__all__ = [
  "RECONCILIATION_BLOCK_TYPE",
  "RECONCILIATION_CATEGORY",
  "RECONCILIATION_DISPLAY_NAME",
  "RECONCILIATION_FACTSET_TYPE",
  "build_envelope",
  "standing_fact_sets",
]
