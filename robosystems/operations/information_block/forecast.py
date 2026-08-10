"""Handlers for ``block_type='forecast'`` — the authored scenario container.

The forecast block is the FP&A engine's authored surface: scenario identity
(name + ``scenario_kind``), horizon, base period, lever assertions on
``rs-driver:*`` catalog elements, direct **line assertions** on statement
leaves (manual overrides that win over driver rules and carry-forward for
the months they name), and **line growth** entries (per-line
month-over-month trajectories — the generic form of the revenue growth
lever, for lines the catalog doesn't drive). The block IS the scenario —
its structure id is the ``scenario_id`` every derived forward FactSet
carries (NULL = actuals), and deleting the block removes the whole
scenario slice.

Persistence follows the rules-for-mechanics / **facts-for-values**
doctrine: lever *mechanics* are the library-seeded rs-driver Derive
rules; lever and line-assertion *values* land as authored Numeric facts
in ONE ``factset_type='custom'`` FactSet whose ``scenario_id`` is the
forecast block itself (self-referential — the assertions belong to the
scenario, cascade-delete with it, and stay invisible to actuals reads
and to metric operand binding, which only joins ``('report', 'metric')``
sets).

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
  LineAssertionLite,
  LineGrowthLite,
  RenderingLite,
  RenderingPeriodLite,
  RenderingRowLite,
  ViewProjections,
)
from robosystems.models.extensions import Dimension, Element, Structure
from robosystems.models.extensions.roboledger.dimension_junctions import (
  fact_dimensions,
)
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.roboledger.fact_set import FactSet
from robosystems.operations.information_block.envelope import (
  elements_to_lites,
  fact_set_to_lite,
  fact_to_lite,
  load_base_envelope_atoms,
  window_month_axis,
)
from robosystems.operations.information_block.forecast_history import (
  back_solve_lever_history,
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
from robosystems.operations.roboledger.reports.calc_dag import (
  load_rs_gaap_calculations,
)
from robosystems.utils.ulid import generate_prefixed_ulid

if TYPE_CHECKING:
  from sqlalchemy.orm import Session

  from robosystems.models.api.extensions.forecasts import (
    CreateForecastRequest,
    DeleteForecastRequest,
    LeverAssertionRequest,
    LineAssertionRequest,
    LineGrowthRequest,
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

  Request value → fiscal calendar ``closed_through_period`` → the entity's
  newest actual report month. The last fallback is data-driven because the
  calendar is not the period spine for annual-comparative tenants. Stored
  resolved in the mechanics so recompute is deterministic.
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


_ASSERTION_BLOCKED_SOURCES = {
  _LEVER_SOURCE: "rs-driver concepts are levers — assert them via `levers`",
  "rs-metric": "rs-metric concepts are computed by compute-metrics, never asserted",
}


def _expand_line_assertions(
  session: Session,
  assertions: list[LineAssertionRequest],
  base_period: str,
  horizon_months: int,
) -> list[LineAssertionLite]:
  """Resolve + expand wire-level line assertions to explicit per-month maps.

  Same grammar and expansion as levers; the validation differs: an
  assertion names a **statement leaf** — any resolvable non-abstract
  concept that isn't an rs-driver lever or rs-metric output, and isn't
  a parent in the global rs-gaap calc DAG (**leaves only** — subtotals
  stay derived so a manual line still articulates and stays
  verification-gated).
  """
  if not assertions:
    return []

  months = [add_months(base_period, i) for i in range(1, horizon_months + 1)]
  month_set = set(months)
  calc_parent_ids = set(load_rs_gaap_calculations(session).keys())

  expanded: list[LineAssertionLite] = []
  seen: set[str] = set()
  for assertion in assertions:
    if assertion.qname in seen:
      raise ValueError(
        f"Duplicate line-assertion qname {assertion.qname!r} in the assertion set."
      )
    seen.add(assertion.qname)

    element = session.execute(
      select(Element).where(Element.qname == assertion.qname).limit(1)
    ).scalar_one_or_none()
    if element is None:
      raise ValueError(
        f"Line-assertion qname {assertion.qname!r} did not resolve to an "
        "Element — assertions name rs-gaap (or tenant extension) "
        "statement concepts."
      )
    blocked = _ASSERTION_BLOCKED_SOURCES.get(element.source or "")
    if blocked is not None:
      raise ValueError(f"Line-assertion qname {assertion.qname!r}: {blocked}.")
    if element.is_abstract:
      raise ValueError(
        f"Line-assertion qname {assertion.qname!r} is abstract — assert a "
        "concrete statement leaf."
      )
    if element.id in calc_parent_ids:
      raise ValueError(
        f"Line-assertion qname {assertion.qname!r} is a calc-DAG subtotal — "
        "assertions are leaves-only so the manual value still articulates "
        "(subtotals derive from their children)."
      )

    overrides = assertion.values_by_period or {}
    outside = sorted(set(overrides) - month_set)
    if outside:
      raise ValueError(
        f"Line assertion {assertion.qname!r} has values_by_period entries "
        f"outside the horizon ({base_period} + {horizon_months} months): "
        f"{outside}"
      )

    values: dict[str, float] = {}
    for month in months:
      if month in overrides:
        values[month] = float(overrides[month])
      elif assertion.value is not None:
        values[month] = float(assertion.value)
    if not values:
      raise ValueError(
        f"Line assertion {assertion.qname!r} asserts no months within the horizon."
      )

    expanded.append(
      LineAssertionLite(
        qname=assertion.qname,
        element_id=element.id,
        item_type=element.item_type,
        period_type=element.period_type or "duration",
        values_by_period=values,
      )
    )
  return expanded


def _expand_line_growth(
  session: Session,
  entries: list[LineGrowthRequest],
  base_period: str,
  horizon_months: int,
) -> list[LineGrowthLite]:
  """Resolve + expand wire-level line-growth entries to per-month rate maps.

  Same grammar and expansion as levers/assertions; the validation is the
  line-assertion set plus **duration leaves only** — a balance-sheet
  line rolls from the IS and the working-capital levers, so growing it
  directly would fight the roll. Cross-list conflicts (a grown line
  that's also asserted, or already driven by an active catalog rule)
  are checked by :func:`_check_line_growth_conflicts` against the FINAL
  lists, because an update can change one list without the other.
  """
  if not entries:
    return []

  months = [add_months(base_period, i) for i in range(1, horizon_months + 1)]
  month_set = set(months)
  calc_parent_ids = set(load_rs_gaap_calculations(session).keys())

  expanded: list[LineGrowthLite] = []
  seen: set[str] = set()
  for entry in entries:
    if entry.qname in seen:
      raise ValueError(
        f"Duplicate line-growth qname {entry.qname!r} in the growth set."
      )
    seen.add(entry.qname)

    element = session.execute(
      select(Element).where(Element.qname == entry.qname).limit(1)
    ).scalar_one_or_none()
    if element is None:
      raise ValueError(
        f"Line-growth qname {entry.qname!r} did not resolve to an Element "
        "— growth entries name rs-gaap (or tenant extension) statement "
        "concepts."
      )
    blocked = _ASSERTION_BLOCKED_SOURCES.get(element.source or "")
    if blocked is not None:
      raise ValueError(f"Line-growth qname {entry.qname!r}: {blocked}.")
    if element.is_abstract:
      raise ValueError(
        f"Line-growth qname {entry.qname!r} is abstract — grow a concrete "
        "statement leaf."
      )
    if element.id in calc_parent_ids:
      raise ValueError(
        f"Line-growth qname {entry.qname!r} is a calc-DAG subtotal — "
        "growth is leaves-only so the grown line still articulates "
        "(subtotals derive from their children)."
      )
    if element.period_type == "instant":
      raise ValueError(
        f"Line-growth qname {entry.qname!r} is a balance-sheet (instant) "
        "line — the BS rolls from the IS and the working-capital levers; "
        "grow the driving income-statement line instead."
      )

    overrides = entry.values_by_period or {}
    outside = sorted(set(overrides) - month_set)
    if outside:
      raise ValueError(
        f"Line growth {entry.qname!r} has values_by_period entries "
        f"outside the horizon ({base_period} + {horizon_months} months): "
        f"{outside}"
      )

    values: dict[str, float] = {}
    for month in months:
      if month in overrides:
        values[month] = float(overrides[month])
      elif entry.value is not None:
        values[month] = float(entry.value)
    if not values:
      raise ValueError(
        f"Line growth {entry.qname!r} asserts no months within the horizon."
      )

    expanded.append(
      LineGrowthLite(
        qname=entry.qname,
        element_id=element.id,
        values_by_period=values,
      )
    )
  return expanded


def _check_line_growth_conflicts(
  session: Session,
  levers: list[LeverAssertionLite],
  line_assertions: list[LineAssertionLite],
  line_growth: list[LineGrowthLite],
) -> None:
  """One owner per line — validated against the FINAL authored lists.

  A grown line that's also line-asserted, or already driven by a catalog
  rule whose levers this scenario sets, would have two writers with
  order-dependent outcomes. Runs on both create and update (an update
  can replace ``levers`` alone and silently activate a rule over a
  stored growth entry).
  """
  if not line_growth:
    return
  growth_qnames = {g.qname for g in line_growth}
  overlap = growth_qnames & {a.qname for a in line_assertions}
  if overlap:
    raise ValueError(
      f"Line(s) named by both line_growth and line_assertions: "
      f"{sorted(overlap)} — an assertion pins the value, growth derives "
      "it; pick one per line."
    )

  from robosystems.operations.information_block.forecast_history import (
    DRIVER_PREFIX,
    driver_rules,
  )

  lever_qnames = {lv.qname for lv in levers}
  for rule in driver_rules(session):
    variables = rule.rule_variables or []
    rule_levers = {
      v["variable_qname"]
      for v in variables
      if isinstance(v, dict)
      and isinstance(v.get("variable_qname"), str)
      and v["variable_qname"].startswith(DRIVER_PREFIX)
    }
    if not rule_levers or not rule_levers <= lever_qnames:
      continue  # rule inactive in this scenario
    target = (
      session.get(Element, rule.target_element_id) if rule.target_element_id else None
    )
    if target is not None and target.qname in growth_qnames:
      raise ValueError(
        f"Line growth {target.qname!r} is already driven by "
        f"{', '.join(sorted(rule_levers))} in this scenario — set that "
        "lever instead of a growth entry."
      )


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


def _ensure_scenario_dimension(session: Session, scenario_id: str, name: str) -> str:
  """Get-or-create the ``scenario`` Dimension for a forecast Structure.

  ``value`` is the structure id (stable across renames — the unique
  constraint on ``(dimension_type, value)`` makes this idempotent);
  ``name`` carries the display legibility. Shared by both scenario fact
  producers: the lever-set writer here and ``_upsert_month_set`` in
  :mod:`.forecast_compute`.
  """
  existing = session.execute(
    select(Dimension.id).where(
      Dimension.dimension_type == "scenario",
      Dimension.value == scenario_id,
    )
  ).scalar_one_or_none()
  if existing is not None:
    return existing
  dimension = Dimension(
    dimension_type="scenario",
    name=name,
    value=scenario_id,
  )
  session.add(dimension)
  session.flush()
  return dimension.id


def _write_lever_fact_set(
  session: Session,
  structure: Structure,
  mechanics: ForecastMechanics,
  entity_id: str,
  created_by: str,
) -> FactSet:
  """Persist the lever + line assertions as authored facts in one scenario set.

  Line-growth rates are deliberately NOT written as facts (a rate on a
  monetary statement element would be a unit-lying fact — the mechanics
  copy is their single authored store), but their months still widen
  the set's period envelope so it spans everything the scenario asserts.
  """
  asserted_months = sorted(
    {m for lv in mechanics.levers for m in lv.values_by_period}
    | {m for la in mechanics.line_assertions for m in la.values_by_period}
    | {m for lg in mechanics.line_growth for m in lg.values_by_period}
  )
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

  authored_element_ids = [lv.element_id for lv in mechanics.levers] + [
    la.element_id for la in mechanics.line_assertions
  ]
  elements_by_id = {
    e.id: e
    for e in session.execute(
      select(Element).where(Element.id.in_(authored_element_ids))
    )
    .scalars()
    .all()
  }

  new_facts: list[Fact] = []

  def _add_fact(element_id: str, month: str, value: float, period_type: str) -> None:
    element = elements_by_id.get(element_id)
    unit = _metric_unit(element) if element is not None else "pure"
    month_start, month_end = period_date_range(month)
    new_facts.append(
      Fact(
        id=generate_prefixed_ulid("fact"),
        element_id=element_id,
        value=value,
        fact_type="Numeric",
        period_start=None if period_type == "instant" else month_start,
        period_end=month_end,
        period_type=period_type,
        unit=unit,
        entity_id=entity_id,
        structure_id=structure.id,
        fact_set_id=fact_set.id,
      )
    )

  for lever in mechanics.levers:
    for month, value in sorted(lever.values_by_period.items()):
      _add_fact(lever.element_id, month, value, "duration")
  for assertion in mechanics.line_assertions:
    for month, value in sorted(assertion.values_by_period.items()):
      _add_fact(assertion.element_id, month, value, assertion.period_type)
  session.add_all(new_facts)
  session.flush()
  if new_facts:
    scenario_dimension_id = _ensure_scenario_dimension(
      session, structure.id, structure.name or structure.id
    )
    session.execute(
      fact_dimensions.insert(),
      [
        {"fact_id": fact.id, "dimension_id": scenario_dimension_id}
        for fact in new_facts
      ],
    )
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
  line_assertions = _expand_line_assertions(
    session, payload.line_assertions, base_period, payload.horizon_months
  )
  line_growth = _expand_line_growth(
    session, payload.line_growth, base_period, payload.horizon_months
  )
  _check_line_growth_conflicts(session, levers, line_assertions, line_growth)

  mechanics = ForecastMechanics(
    scenario_kind=payload.scenario_kind,
    horizon_months=payload.horizon_months,
    base_period=base_period,
    levers=levers,
    line_assertions=line_assertions,
    line_growth=line_growth,
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
  structure_id = structure.id
  session.commit()
  return structure_id


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
  wholesale. Already-computed scenario months are NOT recomputed — they go
  stale until the next ``compute-forecast`` run.
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
  if window_changed and payload.line_assertions is None and current.line_assertions:
    raise ValueError(
      "Changing horizon_months or base_period requires re-supplying "
      "`line_assertions` (or an empty list to clear them) — the horizon "
      "window shifted, so every asserted month must be restated."
    )
  if window_changed and payload.line_growth is None and current.line_growth:
    raise ValueError(
      "Changing horizon_months or base_period requires re-supplying "
      "`line_growth` (or an empty list to clear it) — the horizon window "
      "shifted, so every growth month must be restated."
    )

  levers = current.levers
  if payload.levers is not None:
    levers = _expand_levers(session, payload.levers, base_period, horizon_months)
  line_assertions = current.line_assertions
  if payload.line_assertions is not None:
    line_assertions = _expand_line_assertions(
      session, payload.line_assertions, base_period, horizon_months
    )
  line_growth = current.line_growth
  if payload.line_growth is not None:
    line_growth = _expand_line_growth(
      session, payload.line_growth, base_period, horizon_months
    )
  # Against the FINAL lists — replacing `levers` alone can activate a
  # catalog rule over a STORED growth entry.
  _check_line_growth_conflicts(session, levers, line_assertions, line_growth)

  next_mechanics = ForecastMechanics(
    scenario_kind=scenario_kind,
    horizon_months=horizon_months,
    base_period=base_period,
    levers=levers,
    line_assertions=line_assertions,
    line_growth=line_growth,
  )
  structure.artifact_mechanics = next_mechanics.model_dump(mode="json")
  structure.updated_by = updated_by

  if payload.levers is not None or payload.line_assertions is not None:
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
  structure_id = structure.id
  session.commit()
  return structure_id


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
  # verifies each month); no DB-level FK ties them to fact_sets, so sweep
  # them here or they orphan invisibly.
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
  series_history: int | None = None,
  series_forecast: int | None = None,
) -> InformationBlockEnvelope | None:
  """Reload a forecast Structure and pack its envelope.

  ``scenario_id`` is accepted for dispatch-signature parity and ignored
  — the forecast block IS the scenario; its envelope is the lever grid
  regardless of which scenario filter the read carried.

  Renders the **lever grid** metric-style: one row per lever (authoring
  order, ``item_type`` driving percent/days formatting), values from the
  mechanics' expanded assertions, then one row per line assertion and per
  line-growth entry. ``computed_months`` is filled from the scenario's
  derived statement sets. No chart arm — the scenario's time-series
  surface is the metric/statement blocks read with the scenario filter,
  not the container itself.

  Columns span the **closed months plus the horizon**, not the horizon
  alone (:mod:`.forecast_history`): behind the seam each lever shows the
  rate the book actually ran at, recovered by inverting its own Derive
  rule against the month's actuals, so an assumption reads as one series
  across the seam. Actual beats forecast at overlap — a horizon month
  that has since closed renders its realized rate, never the stale
  assertion made for it. ``periods[].forecast`` marks the forward columns
  exactly as the statement series does, and its presence anywhere in the
  rendering is a client's signal that the envelope carries history.

  ``series_history`` / ``series_forecast`` window the month axis with
  statement-series semantics (last N actual months, first N forecast
  months; ``None`` = unbounded) so the assumptions grid stays in
  register with windowed statement reads on the Plan page.
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

  # Authored elements (levers + line assertions) are not reachable via
  # associations (the container is arc-less) — load them off the
  # mechanics for the envelope's element list and the row metadata.
  authored_element_ids = (
    [lv.element_id for lv in mechanics.levers]
    + [la.element_id for la in mechanics.line_assertions]
    + [lg.element_id for lg in mechanics.line_growth]
  )
  authored_elements = list(
    session.execute(select(Element).where(Element.id.in_(authored_element_ids)))
    .scalars()
    .all()
  )
  elements_by_id = {e.id: e for e in authored_elements}

  horizon_months = [
    add_months(mechanics.base_period, i) for i in range(1, mechanics.horizon_months + 1)
  ]
  # The realized side of the grid: each lever's rule inverted against the
  # closed months' actuals, so an assumption reads as one series across
  # the seam instead of starting from nothing at the horizon.
  history = back_solve_lever_history(session, mechanics)
  actual_months = set(history.months)
  # Actual beats forecast at overlap — a horizon month that has since
  # closed is history now, and its cell shows the rate the book ran at,
  # not the rate that was asserted for it.
  months = sorted(actual_months | set(horizon_months))
  forecast_months = {month for month in horizon_months if month not in actual_months}
  # Seam-adjacent windowing, in register with the statement series: the
  # Plan grid unions this axis with the windowed statement columns, so
  # an unwindowed assumptions axis would resurface every trimmed month
  # as a phantom column (statement rows blank, mis-read as forecast).
  months = window_month_axis(months, forecast_months, series_history, series_forecast)

  def lever_value(lever: LeverAssertionLite, month: str) -> float | None:
    if month in forecast_months:
      return lever.values_by_period.get(month)
    return history.lever_values.get(lever.qname, {}).get(month)

  def line_value(assertion: LineAssertionLite, month: str) -> float | None:
    if month in forecast_months:
      return assertion.values_by_period.get(month)
    return history.line_values.get(assertion.element_id, {}).get(month)

  def growth_value(entry: LineGrowthLite, month: str) -> float | None:
    """Asserted rate ahead of the seam; the rate the line actually ran
    at behind it (``v[t]/v[t-1] - 1`` — the trivial inversion), blanked
    when either month's actual is missing or the prior is zero."""
    if month in forecast_months:
      return entry.values_by_period.get(month)
    values = history.line_values.get(entry.element_id, {})
    current_v = values.get(month)
    prior_v = values.get(add_months(month, -1))
    if current_v is None or prior_v is None or prior_v == 0:
      return None
    return current_v / prior_v - 1.0

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
      values=[lever_value(lever, month) for month in months],
    )
    for lever in mechanics.levers
  ]
  # Line assertions render as additional assumption rows — the manual
  # overrides sit in the same grid the levers do, in authoring order.
  rows.extend(
    RenderingRowLite(
      element_id=assertion.element_id,
      element_qname=assertion.qname,
      element_name=(
        elements_by_id[assertion.element_id].name
        if assertion.element_id in elements_by_id
        else assertion.qname
      ),
      balance_type=(
        elements_by_id[assertion.element_id].balance_type
        if assertion.element_id in elements_by_id
        else None
      ),
      item_type=assertion.item_type,
      values=[line_value(assertion, month) for month in months],
    )
    for assertion in mechanics.line_assertions
  )
  # Line-growth entries render as rate rows — asserted rates ahead of
  # the seam, realized month-over-month rates behind it.
  rows.extend(
    RenderingRowLite(
      element_id=entry.element_id,
      element_qname=entry.qname,
      element_name=(
        elements_by_id[entry.element_id].name
        if entry.element_id in elements_by_id
        else entry.qname
      ),
      balance_type=None,
      item_type=entry.item_type,
      values=[growth_value(entry, month) for month in months],
    )
    for entry in mechanics.line_growth
  )
  rendering = RenderingLite(
    rows=rows,
    periods=[
      RenderingPeriodLite(
        start=period_date_range(month)[0],
        end=period_date_range(month)[1],
        # The seam marker, same contract as the statement series: True
        # only where the column is the scenario's own forward month,
        # absent on realized months.
        forecast=True if month in forecast_months else None,
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
    elements=elements_to_lites(session, authored_elements),
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
