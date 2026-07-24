"""Handlers for ``block_type='forecast'`` — the authored scenario container.

The forecast block is the FP&A engine's authored surface (F-1): scenario
identity (name + ``scenario_kind``), horizon, base period, and lever
assertions on ``rs-driver:*`` catalog elements. The block IS the
scenario — its structure id is the ``scenario_id`` every derived forward
FactSet carries (NULL = actuals), and deleting the block removes the
whole scenario slice.

Persistence follows the rules-for-mechanics / **facts-for-values**
doctrine: lever *mechanics* are the library-seeded rs-driver Derive
rules; lever *values* land as authored Numeric facts in ONE
``factset_type='custom'`` FactSet whose ``scenario_id`` is the forecast
block itself (self-referential — the levers belong to the scenario,
cascade-delete with it, and stay invisible to actuals reads and to
metric operand binding, which only joins ``('report', 'metric')`` sets).

``create`` / ``update`` / ``delete`` follow the Schedule/rollforward
declarative pattern; ``build_envelope`` renders the lever grid
metric-style (one row per lever, one column per horizon month) so the
frontend reuses the metric rendering path unchanged. The derivation
itself is ``compute-forecast`` (:mod:`.forecast_compute`) — authoring
never computes.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy import select

from robosystems.models.api.fact_provenance import AssertedProvenance
from robosystems.models.api.information_block import (
  ArtifactResponse,
  ForecastMechanics,
  InformationBlockEnvelope,
  InformationModelResponse,
  LeverAssertionLite,
  RenderingLite,
  RenderingPeriodLite,
  RenderingRowLite,
  ViewProjections,
)
from robosystems.models.extensions import Element, Structure
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.roboledger.fact_set import FactSet
from robosystems.operations.information_block.envelope import (
  elements_to_lites,
  fact_set_to_lite,
  fact_to_lite,
  load_base_envelope_atoms,
)
from robosystems.operations.information_block.metrics import (
  _default_entity_id,
  _latest_report_period_end_before,
  _metric_unit,
)
from robosystems.operations.roboledger.fact_set import create_fact_set
from robosystems.operations.roboledger.fiscal_calendar.periods import (
  add_months,
  period_date_range,
  period_from_date,
)
from robosystems.utils.ulid import generate_prefixed_ulid

if TYPE_CHECKING:
  from sqlalchemy.orm import Session

  from robosystems.models.api.extensions.forecasts import (
    CreateForecastRequest,
    DeleteForecastRequest,
    LeverAssertionRequest,
    UpdateForecastRequest,
  )

FORECAST_BLOCK_TYPE = "forecast"
FORECAST_DISPLAY_NAME = "Forecast"
FORECAST_CATEGORY = "Planning"

_LEVER_SOURCE = "rs-driver"


def _resolve_base_period(
  session: Session, entity_id: str, requested: str | None
) -> str:
  """Resolve the seed month the walk projects forward from.

  Request value → fiscal calendar ``closed_through_period`` → the
  entity's newest actual report month (the data-driven fallback the
  M-2 prior-period resolver established — the calendar is not the
  period spine for annual-comparative tenants). Stored resolved in the
  mechanics so recompute is deterministic.
  """
  if requested is not None:
    return requested

  from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar

  calendar = (
    session.query(FiscalCalendar).order_by(FiscalCalendar.created_at.asc()).first()
  )
  if calendar is not None and calendar.closed_through_period:
    return calendar.closed_through_period

  newest = _latest_report_period_end_before(session, entity_id, date(9999, 12, 31))
  if newest is not None:
    return period_from_date(newest)

  raise ValueError(
    "Cannot resolve a base period: the fiscal calendar has no closed-through "
    "period and no actual report facts exist to project from. Pass "
    "base_period explicitly."
  )


def _expand_levers(
  session: Session,
  levers: list[LeverAssertionRequest],
  base_period: str,
  horizon_months: int,
) -> list[LeverAssertionLite]:
  """Resolve + expand wire-level lever assertions to explicit per-month maps.

  Every qname must resolve to an ``rs-driver``-source element; the
  expansion applies the uniform ``value`` fill then the per-month
  overrides, so compute never interpolates. Overrides naming months
  outside the horizon are rejected (silent drops would make the
  asserted set lie).
  """
  months = [add_months(base_period, i) for i in range(1, horizon_months + 1)]
  month_set = set(months)

  expanded: list[LeverAssertionLite] = []
  seen: set[str] = set()
  for lever in levers:
    if lever.qname in seen:
      raise ValueError(f"Duplicate lever qname {lever.qname!r} in the assertion set.")
    seen.add(lever.qname)

    element = session.execute(
      select(Element).where(Element.qname == lever.qname).limit(1)
    ).scalar_one_or_none()
    if element is None:
      raise ValueError(
        f"Lever qname {lever.qname!r} did not resolve to an Element. "
        "Levers must name rs-driver catalog concepts (see the Driver "
        "Catalog structure)."
      )
    if element.source != _LEVER_SOURCE:
      raise ValueError(
        f"Lever qname {lever.qname!r} resolves to a source={element.source!r} "
        f"element — levers must be {_LEVER_SOURCE!r} catalog concepts."
      )

    overrides = lever.values_by_period or {}
    outside = sorted(set(overrides) - month_set)
    if outside:
      raise ValueError(
        f"Lever {lever.qname!r} has values_by_period entries outside the "
        f"horizon ({base_period} + {horizon_months} months): {outside}"
      )

    values: dict[str, float] = {}
    for month in months:
      if month in overrides:
        values[month] = float(overrides[month])
      elif lever.value is not None:
        values[month] = float(lever.value)
    if not values:
      raise ValueError(f"Lever {lever.qname!r} asserts no months within the horizon.")

    expanded.append(
      LeverAssertionLite(
        qname=lever.qname,
        element_id=element.id,
        item_type=element.item_type,
        values_by_period=values,
      )
    )
  return expanded


def _load_lever_fact_set(session: Session, structure_id: str) -> FactSet | None:
  """The scenario's lever FactSet — ``'custom'`` + self-referential scenario."""
  return session.execute(
    select(FactSet)
    .where(
      FactSet.structure_id == structure_id,
      FactSet.factset_type == "custom",
      FactSet.scenario_id == structure_id,
    )
    .order_by(FactSet.created_at.desc())
    .limit(1)
  ).scalar_one_or_none()


def _write_lever_fact_set(
  session: Session,
  structure: Structure,
  mechanics: ForecastMechanics,
  entity_id: str,
  created_by: str,
) -> FactSet:
  """Persist the lever assertions as authored facts in one scenario set."""
  asserted_months = sorted({m for lv in mechanics.levers for m in lv.values_by_period})
  span_start = period_date_range(asserted_months[0])[0]
  span_end = period_date_range(asserted_months[-1])[1]

  fact_set = create_fact_set(
    session,
    structure_id=structure.id,
    scenario_id=structure.id,
    period_start=span_start,
    period_end=span_end,
    factset_type="custom",
    entity_id=entity_id,
    provenance=AssertedProvenance(
      source_system="forecast-levers", asserted_by=created_by
    ),
    created_by=created_by,
  )
  session.flush()

  elements_by_id = {
    e.id: e
    for e in session.execute(
      select(Element).where(Element.id.in_([lv.element_id for lv in mechanics.levers]))
    )
    .scalars()
    .all()
  }
  for lever in mechanics.levers:
    element = elements_by_id.get(lever.element_id)
    unit = _metric_unit(element) if element is not None else "pure"
    for month, value in sorted(lever.values_by_period.items()):
      month_start, month_end = period_date_range(month)
      session.add(
        Fact(
          id=generate_prefixed_ulid("fact"),
          element_id=lever.element_id,
          value=value,
          fact_type="Numeric",
          period_start=month_start,
          period_end=month_end,
          period_type="duration",
          unit=unit,
          entity_id=entity_id,
          structure_id=structure.id,
          fact_set_id=fact_set.id,
        )
      )
  session.flush()
  return fact_set


def create(
  session: Session,
  payload: CreateForecastRequest,
  created_by: str,
) -> str:
  """Create a forecast block. Persists the Structure + typed mechanics +
  the lever FactSet; returns the new structure_id. Never computes —
  run ``compute-forecast`` to derive the forward months.
  """
  entity_id = payload.entity_id or _default_entity_id(session)
  base_period = _resolve_base_period(session, entity_id, payload.base_period)
  levers = _expand_levers(session, payload.levers, base_period, payload.horizon_months)

  mechanics = ForecastMechanics(
    scenario_kind=payload.scenario_kind,
    horizon_months=payload.horizon_months,
    base_period=base_period,
    levers=levers,
  )

  # The lever elements all live in the tenant's rs-driver taxonomy —
  # the natural owner for the scenario container (rollforward precedent:
  # owning taxonomy of the anchor element).
  lever_element = session.get(Element, levers[0].element_id)
  assert lever_element is not None  # resolved in _expand_levers
  structure = Structure(
    name=payload.name,
    block_type=FORECAST_BLOCK_TYPE,
    taxonomy_id=lever_element.taxonomy_id,
    concept_arrangement="set",
    member_arrangement=None,
    artifact_mechanics=mechanics.model_dump(mode="json"),
    metadata_={},
    created_by=created_by,
  )
  session.add(structure)
  session.flush()

  _write_lever_fact_set(session, structure, mechanics, entity_id, created_by)
  session.commit()
  return structure.id


def _load_forecast_or_404(session: Session, structure_id: str) -> Structure:
  structure = session.get(Structure, structure_id)
  if structure is None or structure.block_type != FORECAST_BLOCK_TYPE:
    raise ValueError(
      f"Forecast structure_id={structure_id!r} not found (or wrong block_type)."
    )
  return structure


def update(
  session: Session,
  payload: UpdateForecastRequest,
  updated_by: str,
) -> str:
  """Update a forecast block in place.

  Mutable: name, scenario_kind, horizon_months, base_period, levers.
  Changing ``horizon_months`` or ``base_period`` requires re-supplying
  ``levers`` — the persisted expansion can't be re-derived (the
  uniform-vs-override distinction is gone), so re-expansion needs the
  wire-level assertions. A lever replacement rewrites the lever FactSet
  wholesale. Previously computed scenario months are NOT recomputed —
  they go stale until the next ``compute-forecast`` run.
  """
  structure = _load_forecast_or_404(session, payload.structure_id)
  current = ForecastMechanics.model_validate(structure.artifact_mechanics or {})

  if payload.name is not None:
    structure.name = payload.name

  scenario_kind = payload.scenario_kind or current.scenario_kind
  horizon_months = payload.horizon_months or current.horizon_months
  base_period = payload.base_period or current.base_period

  window_changed = (
    horizon_months != current.horizon_months or base_period != current.base_period
  )
  if window_changed and payload.levers is None:
    raise ValueError(
      "Changing horizon_months or base_period requires re-supplying `levers` "
      "— the horizon window shifted, so every lever's per-month assertions "
      "must be restated."
    )

  levers = current.levers
  if payload.levers is not None:
    levers = _expand_levers(session, payload.levers, base_period, horizon_months)

  next_mechanics = ForecastMechanics(
    scenario_kind=scenario_kind,
    horizon_months=horizon_months,
    base_period=base_period,
    levers=levers,
  )
  structure.artifact_mechanics = next_mechanics.model_dump(mode="json")
  structure.updated_by = updated_by

  if payload.levers is not None:
    existing = _load_lever_fact_set(session, structure.id)
    entity_id = existing.entity_id if existing is not None else None
    if existing is not None:
      session.delete(existing)  # facts cascade via the DB FK
      session.flush()
    _write_lever_fact_set(
      session,
      structure,
      next_mechanics,
      entity_id or _default_entity_id(session),
      updated_by,
    )

  session.flush()
  session.commit()
  return structure.id


def delete(
  session: Session,
  payload: DeleteForecastRequest,
  deleted_by: str,
) -> str:
  """Hard-delete a forecast block and its entire scenario slice.

  Explicitly deletes every FactSet keyed to this scenario (the lever
  set AND the computed forward statement/metric months — they attach to
  OTHER structures, so the structure delete alone wouldn't reach them;
  the DB-level ``scenario_id ... ON DELETE CASCADE`` is the backstop).
  Actuals are never touched.
  """
  structure = _load_forecast_or_404(session, payload.structure_id)
  structure_id = structure.id

  scenario_sets = (
    session.execute(select(FactSet).where(FactSet.scenario_id == structure_id))
    .scalars()
    .all()
  )
  # Verification rows pin the scenario's month sets (compute-forecast
  # verifies each month, F-2); no DB-level FK ties them to fact_sets,
  # so sweep them here or they orphan invisibly.
  set_ids = [fs.id for fs in scenario_sets]
  if set_ids:
    from robosystems.models.extensions import VerificationResult

    for stale in (
      session.execute(
        select(VerificationResult).where(VerificationResult.fact_set_id.in_(set_ids))
      )
      .scalars()
      .all()
    ):
      session.delete(stale)
  for fact_set in scenario_sets:
    session.delete(fact_set)
  session.flush()

  session.delete(structure)
  session.commit()
  return structure_id


def build_envelope(
  session: Session,
  structure_id: str,
  fact_set_id: str | None = None,
  scenario_id: str | None = None,
  series: bool = False,
) -> InformationBlockEnvelope | None:
  """Reload a forecast Structure and pack its envelope.

  ``scenario_id`` is accepted for dispatch-signature parity and ignored
  — the forecast block IS the scenario; its envelope is the lever grid
  regardless of which scenario filter the read carried.

  Renders the **lever grid** metric-style: one row per lever (authoring
  order, ``item_type`` driving percent/days formatting), one period
  column per horizon month, values from the mechanics' expanded
  assertions. ``computed_months`` is filled from the scenario's derived
  statement sets. No chart arm — the scenario's time-series surface is
  the metric/statement blocks read with the scenario filter, not the
  container itself.
  """
  atoms = load_base_envelope_atoms(
    session,
    structure_id,
    expected_block_type=FORECAST_BLOCK_TYPE,
    fact_set_id=fact_set_id,
  )
  if atoms is None:
    return None
  structure = atoms.structure

  mechanics = ForecastMechanics.model_validate(structure.artifact_mechanics or {})

  # Runtime state: how many forward months have computed scenario sets.
  computed_months = (
    session.execute(
      select(FactSet.period_end)
      .where(
        FactSet.scenario_id == structure_id,
        FactSet.factset_type == "report",
      )
      .distinct()
    )
    .scalars()
    .all()
  )
  mechanics = mechanics.model_copy(update={"computed_months": len(computed_months)})

  # Lever elements are not reachable via associations (the container is
  # arc-less) — load them off the mechanics for the envelope's element
  # list and the row metadata.
  lever_elements = list(
    session.execute(
      select(Element).where(Element.id.in_([lv.element_id for lv in mechanics.levers]))
    )
    .scalars()
    .all()
  )
  elements_by_id = {e.id: e for e in lever_elements}

  months = [
    add_months(mechanics.base_period, i) for i in range(1, mechanics.horizon_months + 1)
  ]
  rows = [
    RenderingRowLite(
      element_id=lever.element_id,
      element_qname=lever.qname,
      element_name=(
        elements_by_id[lever.element_id].name
        if lever.element_id in elements_by_id
        else lever.qname
      ),
      balance_type=None,
      item_type=lever.item_type,
      values=[lever.values_by_period.get(month) for month in months],
    )
    for lever in mechanics.levers
  ]
  rendering = RenderingLite(
    rows=rows,
    periods=[
      RenderingPeriodLite(
        start=period_date_range(month)[0],
        end=period_date_range(month)[1],
      )
      for month in months
    ],
    validation=None,
    unmapped_count=0,
  )

  lever_set = _load_lever_fact_set(session, structure_id)
  facts: list[Fact] = []
  if lever_set is not None:
    facts = list(
      session.execute(select(Fact).where(Fact.fact_set_id == lever_set.id))
      .scalars()
      .all()
    )

  return InformationBlockEnvelope(
    id=structure.id,
    block_type=FORECAST_BLOCK_TYPE,
    name=structure.name,
    display_name=FORECAST_DISPLAY_NAME,
    category=FORECAST_CATEGORY,
    taxonomy_id=structure.taxonomy_id,
    taxonomy_name=atoms.taxonomy_name,
    information_model=InformationModelResponse(
      concept_arrangement=structure.concept_arrangement or "set",
      member_arrangement=structure.member_arrangement,
    ),
    artifact=ArtifactResponse(
      topic=structure.description,
      renderer_note=structure.renderer_note,
      template=None,
      mechanics=mechanics,
    ),
    elements=elements_to_lites(session, lever_elements),
    connections=[],
    facts=[fact_to_lite(f, elements_by_id) for f in facts],
    rules=atoms.rules,
    fact_set=fact_set_to_lite(lever_set) if lever_set is not None else None,
    verification_results=atoms.verification_results,
    verification_summary=atoms.verification_summary,
    view=ViewProjections(rendering=rendering, chart=None),
  )


__all__ = [
  "FORECAST_BLOCK_TYPE",
  "FORECAST_CATEGORY",
  "FORECAST_DISPLAY_NAME",
  "build_envelope",
  "create",
  "delete",
  "update",
]
