"""Envelope builder for ``block_type='metric'``: one column per standing
metric FactSet, one row per catalog concept. Facts are written by
``compute-metrics`` / ``assert-metrics``; the block write slots are stubs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from robosystems.models.api.information_block import (
  ArtifactResponse,
  InformationBlockEnvelope,
  InformationModelResponse,
  MetricMechanics,
  RenderingLite,
  RenderingPeriodLite,
  RenderingRowLite,
  ViewProjections,
)
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.roboledger.fact_set import FactSet
from robosystems.operations.information_block.chart import build_chart_projection
from robosystems.operations.information_block.envelope import (
  association_to_connection,
  elements_to_lites,
  fact_set_to_lite,
  fact_to_lite,
  load_base_envelope_atoms,
)

if TYPE_CHECKING:
  from sqlalchemy.orm import Session

METRIC_BLOCK_TYPE = "metric"
METRIC_DISPLAY_NAME = "Metric"
METRIC_CATEGORY = "Reporting"


def _load_metric_fact_sets(
  session: Session,
  structure_id: str,
  fact_set_id: str | None,
  scenario_id: str | None = None,
) -> list[FactSet]:
  """The structure's standing metric FactSets, one per period_end, oldest
  first (or just ``fact_set_id`` when pinned).

  A ``scenario_id`` adds that scenario's sets; actuals win at a shared
  period_end, then newest.
  """
  if fact_set_id is not None:
    row = session.get(FactSet, fact_set_id)
    return [row] if row is not None else []
  rows = (
    session.execute(
      select(FactSet)
      .where(
        FactSet.structure_id == structure_id,
        FactSet.factset_type == "metric",
        FactSet.scenario_id.is_(None)
        if scenario_id is None
        else FactSet.scenario_id.is_(None) | (FactSet.scenario_id == scenario_id),
      )
      .order_by(
        FactSet.period_end.asc(),
        FactSet.scenario_id.asc().nulls_first(),
        FactSet.created_at.desc(),
      )
    )
    .scalars()
    .all()
  )
  by_period: dict = {}
  for fs in rows:
    by_period.setdefault(fs.period_end, fs)
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
  """Pack a metric Structure + its standing time series into the envelope.

  Always the full series (``series`` is ignored). Rows follow presentation
  order; a never-computed block renders rows with no columns.
  ``scenario_id`` adds that scenario's forward columns, labelled as forecast.
  """
  atoms = load_base_envelope_atoms(
    session,
    structure_id,
    expected_block_type=METRIC_BLOCK_TYPE,
    fact_set_id=fact_set_id,
    scenario_id=scenario_id,
  )
  if atoms is None:
    return None
  structure = atoms.structure

  fact_sets = _load_metric_fact_sets(session, structure_id, fact_set_id, scenario_id)

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

  # Catalog rows in presentation-arc order (dedupe preserving order).
  ordered_arcs = sorted(
    (a for a in atoms.associations if a.to_element_id is not None),
    key=lambda a: a.order_value if a.order_value is not None else float("inf"),
  )
  concept_ids: list[str] = []
  for arc in ordered_arcs:
    if arc.to_element_id not in concept_ids:
      concept_ids.append(arc.to_element_id)

  rows: list[RenderingRowLite] = []
  for element_id in concept_ids:
    element = elements_by_id.get(element_id)
    rows.append(
      RenderingRowLite(
        element_id=element_id,
        element_qname=element.qname if element else None,
        element_name=element.name if element else element_id,
        balance_type=element.balance_type if element else None,
        item_type=element.item_type if element else None,
        values=[value_by_set_element.get((fs.id, element_id)) for fs in fact_sets],
      )
    )

  rendering = RenderingLite(
    rows=rows,
    periods=[
      RenderingPeriodLite(
        start=fs.period_start if fs.period_start is not None else fs.period_end,
        end=fs.period_end,
        label=(
          f"{fs.period_end.strftime('%b %Y')} (forecast)"
          if fs.scenario_id is not None
          else None
        ),
        forecast=True if fs.scenario_id is not None else None,
      )
      for fs in fact_sets
    ],
    validation=None,
    unmapped_count=0,
  )

  if structure.artifact_mechanics:
    mechanics = MetricMechanics.model_validate(structure.artifact_mechanics)
  else:
    mechanics = MetricMechanics(kind="metric")

  latest_lite = fact_set_to_lite(fact_sets[-1]) if fact_sets else atoms.fact_set

  return InformationBlockEnvelope(
    id=structure.id,
    block_type=METRIC_BLOCK_TYPE,
    name=structure.name,
    display_name=METRIC_DISPLAY_NAME,
    category=METRIC_CATEGORY,
    taxonomy_id=structure.taxonomy_id,
    taxonomy_name=atoms.taxonomy_name,
    information_model=InformationModelResponse(
      concept_arrangement=structure.concept_arrangement or "arithmetic",
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
    facts=[fact_to_lite(f, elements_by_id) for f in facts],
    rules=atoms.rules,
    fact_set=latest_lite,
    verification_results=atoms.verification_results,
    verification_summary=atoms.verification_summary,
    view=ViewProjections(
      rendering=rendering,
      chart=build_chart_projection(rendering, elements_by_id),
    ),
  )


__all__ = [
  "METRIC_BLOCK_TYPE",
  "METRIC_CATEGORY",
  "METRIC_DISPLAY_NAME",
  "build_envelope",
]
