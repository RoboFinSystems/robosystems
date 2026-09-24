"""compute-forecast — walk a scenario's driver cascade into forward FactSets.

:mod:`.forecast` holds the authored surface (scenario identity, lever
assertions, line assertions, growth rates); this module derives everything
downstream, one forward month at a time from the walk's **anchor**.

The anchor is not the block's ``base_period``. ``base_period`` is the origin
of the authored window (every lever is keyed to a month inside it). With
``base_anchor='seam'`` (the default) the anchor advances to the newest closed
month inside the horizon, so the first forward month opens on real balances;
``base_anchor='fixed'`` pins it to ``base_period``. Per month, in order:

1. **Carry-forward** — every income-statement leaf that carried a fact in the
   base month's actual report and isn't rule-driven repeats its prior value.
   Line assertions override both carry and schedule projection for the months
   they name, and displace a driver rule targeting the same element that
   month (the rule lands in ``skipped``).
2. **Driver rules** — the rs-driver catalog's ``Derive`` rules in dependency
   order (``topo_sort_calculations`` over same-month operand edges). A rule
   is *active* for the scenario iff every rs-driver operand it names has
   asserted lever values. Lever values bind from the scenario's lever
   FactSet, ``$X[t-1]`` operands bind the previous month's value
   (:func:`.rules.expressions.desugar_priors`), and same-month rs-gaap
   operands bind the current month's computed values, prior month as
   fallback.
3. **Calc-DAG subtotals** — ``resolve_calc_dag`` over the merged
   rs-gaap-calculations + local income-statement arcs derives GrossProfit →
   OperatingIncome → NetIncome the same way the report pivot does.

Each month upserts a scenario income-statement set
(``factset_type='report'``, congruent with the actual monthly sets), a
balance-sheet set, and a derived cash-flow set when an actual one exists, all
keyed by ``fact_sets.scenario_id``; re-running a month replaces its values.
Without an actual balance sheet at the base period, the balance-sheet set is
only the rule-driven working-capital instants.

Soft-fail per rule per month: a missing lever month or unbound operand skips
that rule with a reason and its target falls back to carry-forward.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import and_, not_, select

from robosystems.models.api.fact_provenance import ForecastProvenance
from robosystems.models.api.information_block import (
  ComputeForecastRequest,
  ComputeForecastResponse,
  ForecastMechanics,
  ForecastMonthLite,
  SkippedForecastLite,
)
from robosystems.models.extensions import (
  Association,
  Element,
  Rule,
  Structure,
)
from robosystems.models.extensions.roboledger.dimension_junctions import (
  fact_dimensions,
)
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.roboledger.fact_set import FactSet
from robosystems.operations.information_block.forecast import (
  FORECAST_BLOCK_TYPE,
  _ensure_scenario_dimension,
  _load_lever_fact_set,
)
from robosystems.operations.information_block.forecast_articulation import (
  ArticulationContext,
  derive_cash_flow,
  load_articulation_context,
  roll_balance_sheet,
  schedule_is_delta,
)
from robosystems.operations.information_block.forecast_history import (
  DRIVER_PREFIX,
  driver_rules,
  newest_actual_structure_id,
  numeric_facts,
)
from robosystems.operations.information_block.metrics import (
  _default_entity_id,
  _metric_unit,
)
from robosystems.operations.information_block.rules.expressions import (
  InvalidRuleExpression,
  desugar_aggregates,
  desugar_priors,
  evaluate_derivation,
  lhs_variable_names,
  parse_arithmetic_expression,
)
from robosystems.operations.roboledger.fact_set import create_fact_set
from robosystems.operations.roboledger.fiscal_calendar.periods import (
  add_months,
  period_date_range,
  period_from_date,
)
from robosystems.operations.roboledger.reports.calc_dag import (
  load_rs_gaap_calculations,
  merge_calculations,
  resolve_calc_dag,
  topo_sort_calculations,
)
from robosystems.utils.ulid import generate_prefixed_ulid

if TYPE_CHECKING:
  from datetime import date

  from sqlalchemy.orm import Session
  from sqlalchemy.sql.elements import ColumnElement


@dataclass
class _ActiveRule:
  """A driver rule resolved + activated for this scenario run."""

  rule: Rule
  target: Element
  qname_by_name: dict[str, str]
  operand_names: list[str]


def _actual_set_at(
  session: Session,
  structure_id: str,
  entity_id: str,
  period_start: date,
  period_end: date,
  *,
  canonical_only: bool = False,
) -> FactSet | None:
  """The newest actual report set for a structure at exactly one month.

  The ``period_start`` bound is load-bearing: the final monthly period_end
  coincides with the FY end, and without it the ANNUAL comparative set
  would win.

  Canonical sets (report_id IS NULL, the close-time stamp) beat publication
  snapshots. ``canonical_only`` drops snapshots entirely: a canonical set is
  what makes a month closed, while a snapshot can exist for an open, future
  or reopened month (``create_report`` never consults the fiscal calendar,
  and reopen retracts only canonical sets).
  """
  stmt = select(FactSet).where(
    FactSet.structure_id == structure_id,
    FactSet.factset_type == "report",
    FactSet.scenario_id.is_(None),
    FactSet.entity_id == entity_id,
    FactSet.period_end == period_end,
    FactSet.period_start >= period_start,
  )
  if canonical_only:
    stmt = stmt.where(FactSet.report_id.is_(None))
  return session.execute(
    stmt.order_by(
      FactSet.report_id.isnot(None).asc(),
      FactSet.created_at.desc(),
    ).limit(1)
  ).scalar_one_or_none()


def _newest_actual_month(
  session: Session, structure_id: str, entity_id: str
) -> str | None:
  """Newest month carrying a canonical (close-stamped) set for a structure.

  Only an upper bound for the anchor scan: it skips the monthly-window
  guard, so it may land on the annual comparative set.
  """
  newest = session.execute(
    select(FactSet.period_end)
    .where(
      FactSet.structure_id == structure_id,
      FactSet.factset_type == "report",
      FactSet.scenario_id.is_(None),
      FactSet.report_id.is_(None),
      FactSet.entity_id == entity_id,
    )
    .order_by(FactSet.period_end.desc())
    .limit(1)
  ).scalar()
  return None if newest is None else period_from_date(newest)


def _resolve_anchor_period(
  session: Session,
  *,
  base_period: str,
  horizon_months: int,
  is_structure_id: str,
  bs_structure_id: str | None,
  entity_id: str,
) -> str:
  """The newest month inside the horizon that can seed the walk.

  Candidates are the months after ``base_period`` inside the horizon,
  newest first (so a gap in the closed history can't strand the anchor);
  the first with a monthly **canonical** income statement (and balance
  sheet, when required) wins. Canonical is required, not preferred: the
  walk seeds from the anchor and :func:`_invalidate_superseded_head`
  deletes every scenario month at or before it, and only close-stamping
  means closed.

  The newest actual month bounds the scan so the common case (nothing
  closed past the base) doesn't walk the whole horizon. Returns
  ``base_period`` when nothing later qualifies.
  """
  newest_actual_month = _newest_actual_month(session, is_structure_id, entity_id)
  if newest_actual_month is None:
    return base_period

  for offset in range(horizon_months, 0, -1):
    month = add_months(base_period, offset)
    if month > newest_actual_month:
      continue
    month_start, month_end = period_date_range(month)
    if (
      _actual_set_at(
        session,
        is_structure_id,
        entity_id,
        month_start,
        month_end,
        canonical_only=True,
      )
      is None
    ):
      continue
    if bs_structure_id is not None and (
      _actual_set_at(
        session,
        bs_structure_id,
        entity_id,
        month_start,
        month_end,
        canonical_only=True,
      )
      is None
    ):
      # An income statement without the matching balance sheet cannot seed
      # the roll; keep walking back rather than anchoring half-blind.
      continue
    return month
  return base_period


def _local_calc_arcs(
  session: Session, structure_id: str
) -> dict[str, list[tuple[str, float]]]:
  """A structure's own calculation arcs as a parent → children map."""
  rows = (
    session.execute(
      select(Association).where(
        Association.structure_id == structure_id,
        Association.association_type == "calculation",
      )
    )
    .scalars()
    .all()
  )
  local: dict[str, list[tuple[str, float]]] = {}
  for a in rows:
    if a.from_element_id is None or a.to_element_id is None:
      continue
    weight = float(a.weight) if a.weight is not None else 1.0
    local.setdefault(a.from_element_id, []).append((a.to_element_id, weight))
  return local


def cmd_compute_forecast(
  session: Session,
  body: ComputeForecastRequest,
  created_by: str,
) -> ComputeForecastResponse:
  """Walk the scenario's driver cascade and upsert its forward FactSets.

  ``session.flush()`` before returning; the OperationSpec wrapper owns
  the commit.
  """
  structure = session.get(Structure, body.structure_id)
  if structure is None:
    raise ValueError(f"Structure not found: {body.structure_id}")
  if structure.block_type != FORECAST_BLOCK_TYPE:
    raise ValueError(
      f"compute-forecast targets a block_type='forecast' structure; "
      f"{body.structure_id!r} is {structure.block_type!r}"
    )
  scenario_id = structure.id
  scenario_dimension_id = _ensure_scenario_dimension(
    session, scenario_id, structure.name or scenario_id
  )
  mechanics = ForecastMechanics.model_validate(structure.artifact_mechanics or {})

  months_n = body.months or mechanics.horizon_months
  if months_n > mechanics.horizon_months:
    raise ValueError(
      f"months={months_n} exceeds the block's horizon_months="
      f"{mechanics.horizon_months} — lever assertions don't extend past "
      "the horizon."
    )

  lever_set = _load_lever_fact_set(session, scenario_id)
  if lever_set is None:
    raise ValueError(
      f"Forecast {scenario_id!r} has no lever FactSet — the block is "
      "corrupt; re-create it via update-information-block."
    )
  entity_id = body.entity_id or lever_set.entity_id or _default_entity_id(session)

  # Lever and line-assertion values bind from the authored facts, not the
  # mechanics copy. Levers key by qname (rule operands name qnames);
  # assertions key by element id.
  element_qname_by_id: dict[str, str] = {
    lv.element_id: lv.qname for lv in mechanics.levers
  }
  assertion_period_type: dict[str, str] = {
    la.element_id: la.period_type for la in mechanics.line_assertions
  }
  # Growth rates live only in the mechanics: a rate stored as a fact on a
  # monetary element would lie about its unit.
  growth_rates: dict[str, dict[str, float]] = {
    lg.element_id: lg.values_by_period for lg in mechanics.line_growth
  }
  growth_qname_by_id: dict[str, str] = {
    lg.element_id: lg.qname for lg in mechanics.line_growth
  }
  lever_values: dict[str, dict[str, float]] = {}
  assertion_values: dict[str, dict[str, float]] = {}
  for fact in numeric_facts(session, lever_set.id):
    if fact.value is None:
      continue
    month = period_from_date(fact.period_end)
    qname = element_qname_by_id.get(fact.element_id)
    if qname is not None:
      lever_values.setdefault(qname, {})[month] = float(fact.value)
    elif fact.element_id in assertion_period_type:
      assertion_values.setdefault(fact.element_id, {})[month] = float(fact.value)

  # ── Resolve the actual structures + seed month ────────────────────────
  is_structure_id = newest_actual_structure_id(session, "income_statement")
  if is_structure_id is None:
    raise ValueError(
      "No actual income-statement sets exist to project from — close at "
      "least one month first (closing a period stamps its statement "
      "sets). If months are already closed without statements, set up "
      "the CoA mapping and reporting style, then reclose."
    )
  bs_structure_id = newest_actual_structure_id(session, "balance_sheet")

  anchor_period = mechanics.base_period
  if mechanics.base_anchor == "seam":
    # Require a balance sheet at the anchor only if the base month has one:
    # re-anchoring must not silently drop the BS roll, but an IS-only
    # scenario shouldn't be held to it.
    base_window = period_date_range(mechanics.base_period)
    rolls_balance_sheet = bs_structure_id is not None and (
      _actual_set_at(session, bs_structure_id, entity_id, *base_window) is not None
    )
    anchor_period = _resolve_anchor_period(
      session,
      base_period=mechanics.base_period,
      horizon_months=mechanics.horizon_months,
      is_structure_id=is_structure_id,
      bs_structure_id=bs_structure_id if rolls_balance_sheet else None,
      entity_id=entity_id,
    )

  base_start, base_end = period_date_range(anchor_period)
  base_is_set = _actual_set_at(
    session, is_structure_id, entity_id, base_start, base_end
  )
  if base_is_set is None:
    raise ValueError(
      f"No actual income statement at the base period "
      f"{anchor_period} (a monthly set whose window starts "
      f"{base_start} and ends {base_end}). Close the months through the "
      "base period (closing stamps each month's statement sets), or set "
      "base_period to a month that has one."
    )

  prior_values: dict[str, float] = {}
  base_is_element_ids: list[str] = []
  for fact in numeric_facts(session, base_is_set.id):
    if fact.value is None:
      continue
    if fact.element_id not in prior_values:
      base_is_element_ids.append(fact.element_id)
    prior_values[fact.element_id] = float(fact.value)

  # BS instants seed [t-1]/carry context for balance-driven rules and the
  # full roll: base_bs_element_ids preserves emission order, bs_prior is the
  # roll's month-zero state.
  base_bs_set = None
  base_bs_element_ids: list[str] = []
  bs_prior: dict[str, float] = {}
  if bs_structure_id is not None:
    base_bs_set = _actual_set_at(
      session, bs_structure_id, entity_id, base_start, base_end
    )
    if base_bs_set is not None:
      for fact in numeric_facts(session, base_bs_set.id):
        if fact.value is None:
          continue
        if fact.element_id not in bs_prior:
          base_bs_element_ids.append(fact.element_id)
        bs_prior[fact.element_id] = float(fact.value)
        if fact.element_id not in prior_values:
          prior_values[fact.element_id] = float(fact.value)

  # ── Activate driver rules for this scenario ───────────────────────────
  qname_cache: dict[str, Element | None] = {}

  def _element_by_qname(qname: str) -> Element | None:
    if qname not in qname_cache:
      qname_cache[qname] = session.execute(
        select(Element).where(Element.qname == qname).limit(1)
      ).scalar_one_or_none()
    return qname_cache[qname]

  skipped: list[SkippedForecastLite] = []
  active_rules: list[_ActiveRule] = []
  for rule in driver_rules(session):
    variables = rule.rule_variables or []
    names = [v.get("variable_name") for v in variables if isinstance(v, dict)]
    if not all(isinstance(n, str) and n for n in names):
      continue
    qname_by_name: dict[str, str] = {
      v["variable_name"]: v["variable_qname"]
      for v in variables
      if isinstance(v, dict) and isinstance(v.get("variable_qname"), str)
    }
    lever_qnames = [q for q in qname_by_name.values() if q.startswith(DRIVER_PREFIX)]
    # Active iff every lever operand has asserted values in this scenario.
    if not lever_qnames or any(q not in lever_values for q in lever_qnames):
      continue
    target_element = (
      session.get(Element, rule.target_element_id) if rule.target_element_id else None
    )
    if target_element is None:
      skipped.append(
        SkippedForecastLite(
          rule_id=rule.id,
          element_qname=None,
          period=anchor_period,
          reason="rule has no resolvable target element",
        )
      )
      continue
    active_rules.append(
      _ActiveRule(
        rule=rule,
        target=target_element,
        qname_by_name=qname_by_name,
        operand_names=[n for n in names if isinstance(n, str)],
      )
    )

  # Same-month dependency order: an operand qname naming another active
  # rule's target is a same-period edge. Synthesized __prior_* operands
  # are excluded by construction (they bind the PREVIOUS month), so
  # compounding self-reference cannot create a cycle.
  active_by_target_qname = {
    ar.target.qname: ar for ar in active_rules if ar.target.qname
  }
  dependency_map: dict[str, list[tuple[str, float]]] = {}
  for ar in active_rules:
    if not ar.target.qname:
      continue
    same_month_deps = {
      q
      for q in ar.qname_by_name.values()
      if q != ar.target.qname and q in active_by_target_qname
    }
    dependency_map[ar.target.qname] = [(q, 1.0) for q in same_month_deps]
  ordered_active = [
    active_by_target_qname[q]
    for q in topo_sort_calculations(dependency_map)
    if q in active_by_target_qname
  ]

  active_target_ids = {ar.target.id for ar in active_rules}
  active_driver_qnames = sorted(
    {
      q
      for ar in active_rules
      for q in ar.qname_by_name.values()
      if q.startswith(DRIVER_PREFIX)
    }
  )

  # ── Calc DAG + carry pool ─────────────────────────────────────────────
  global_calcs = load_rs_gaap_calculations(session)
  calculations = merge_calculations(
    global_calcs, _local_calc_arcs(session, is_structure_id)
  )
  if bs_structure_id is not None:
    calculations = merge_calculations(
      calculations, _local_calc_arcs(session, bs_structure_id)
    )
  calc_order = topo_sort_calculations(calculations)
  calc_targets = set(calculations.keys())
  parents_by_child = _build_parents_by_child(calculations)
  carry_pool = [
    el
    for el in base_is_element_ids
    if el not in calc_targets and el not in active_target_ids
  ]
  # Asserted duration lines join the carry pool: a ramp's last asserted
  # value carries into the unasserted months (where the driver rules
  # take over), exactly like any other leaf. Without this, a line
  # asserted into a base month that never carried it would vanish the
  # month after its last assertion.
  base_id_set = set(base_is_element_ids)
  for element_id in sorted(
    el for el, pt in assertion_period_type.items() if pt != "instant"
  ):
    if (
      element_id not in base_id_set
      and element_id not in calc_targets
      and element_id not in active_target_ids
    ):
      carry_pool.append(element_id)
  # Grown lines join the carry pool too: a month without a rate holds
  # the line's last grown value instead of vanishing (grow-then-hold
  # ramps come out of a sparse values_by_period naturally).
  for element_id in sorted(growth_rates):
    if (
      element_id not in base_id_set
      and element_id not in calc_targets
      and element_id not in active_target_ids
      and element_id not in carry_pool
    ):
      carry_pool.append(element_id)

  # ── Articulation context — BS roll + schedules + derived CF ───────────
  # The mapping id rides the base report set's PivotProvenance; without
  # it schedule contributions can't route CoA→rs-gaap and are skipped.
  mapping_id: str | None = None
  for seed_set in (base_is_set, base_bs_set):
    prov = getattr(seed_set, "provenance", None) if seed_set is not None else None
    if isinstance(prov, dict) and prov.get("origin") == "pivot":
      mapping_id = prov.get("mapping_id")
      if mapping_id:
        break
  cf_structure_id = newest_actual_structure_id(session, "cash_flow_statement")

  diagnostics: list[str] = []

  # The window's end is fixed, so a re-anchored walk computes fewer months,
  # not later ones.
  window_end = add_months(mechanics.base_period, mechanics.horizon_months)
  months = []
  for offset in range(1, months_n + 1):
    month = add_months(anchor_period, offset)
    if month > window_end:
      break
    months.append(month)

  if not months:
    # Fully overtaken: the close has passed the window's end. Clear the
    # superseded months and return rather than raise (a raise would roll the
    # cleanup back). The lever set survives, so extending the horizon
    # revives the scenario.
    cleared = _invalidate_superseded_head(
      session,
      scenario_id=scenario_id,
      entity_id=entity_id,
      through_period_end=base_end,
    )
    diagnostics.append(
      f"Forecast {scenario_id!r} has no forward months left: its horizon "
      f"ends {window_end} and the books are closed through {anchor_period}. "
      f"Nothing was computed. "
      + (
        f"Its {cleared} previously computed fact set(s) were all months "
        "that have since closed, and have been cleared. "
        if cleared
        else ""
      )
      + "Extend horizon_months to forecast from the current close, or "
      "re-create the scenario on a later base. The authored levers are "
      "untouched."
    )
    session.flush()
    return ComputeForecastResponse(
      structure_id=structure.id,
      scenario_id=scenario_id,
      entity_id=entity_id,
      base_period=mechanics.base_period,
      anchor_period=anchor_period,
      months=months_n,
      months_computed=[],
      halted_at=None,
      skipped=skipped,
      diagnostics=diagnostics,
    )

  if anchor_period != mechanics.base_period:
    diagnostics.append(
      f"Walk re-anchored from base {mechanics.base_period} to {anchor_period}, "
      f"the newest closed month inside the horizon: opening balances come "
      f"from that month's actuals, so the first forward month rolls off real "
      f"figures. {len(months)} of the horizon's {mechanics.horizon_months} "
      f"months remain forward-looking (the rest have closed). Levers keep "
      f"their authored months — nothing needs restating. Set "
      f"base_anchor='fixed' to pin the walk to the base instead."
    )

  ctx: ArticulationContext | None = None
  if bs_structure_id is not None and base_bs_set is not None:
    horizon_end = period_date_range(months[-1])[1]
    ctx = load_articulation_context(
      session,
      bs_structure_id=bs_structure_id,
      cf_structure_id=cf_structure_id,
      mapping_id=mapping_id,
      entity_id=entity_id,
      base_bs_element_ids=base_bs_element_ids,
      bs_prior=bs_prior,
      base_is_element_ids=base_is_element_ids,
      base_start=base_start,
      horizon_end=horizon_end,
    )
    diagnostics.extend(ctx.diagnostics)
    if cf_structure_id is None:
      diagnostics.append(
        "no actual cash-flow statement exists — scenario CF sets skipped"
      )
    if mapping_id is None:
      diagnostics.append(
        "base report carries no mapping provenance — schedule projections skipped"
      )
  else:
    diagnostics.append(
      "no actual balance sheet at the base period — emitting the "
      "rule-driven working-capital instants only (no BS roll / CF)"
    )

  for element_id in sorted(growth_rates):
    if element_id not in prior_values:
      diagnostics.append(
        f"line growth on {growth_qname_by_id.get(element_id, element_id)}: "
        "no base-month value — grows from 0"
      )

  elements_by_id: dict[str, Element] = {}

  def _element(element_id: str) -> Element | None:
    if element_id not in elements_by_id:
      loaded = session.get(Element, element_id)
      if loaded is not None:
        elements_by_id[element_id] = loaded
    return elements_by_id.get(element_id)

  # ── The walk ──────────────────────────────────────────────────────────
  months_computed: list[ForecastMonthLite] = []
  halted_at: str | None = None
  unverified_months: list[str] = []
  active_instant_ids = {
    ar.target.id for ar in ordered_active if ar.target.period_type == "instant"
  }
  prior_bs = dict(bs_prior)
  prev_period_end = base_end
  prev_month = anchor_period

  for month_index, month in enumerate(months, start=1):
    month_start, month_end = period_date_range(month)
    current: dict[str, float] = {}

    # (a) Carry-forward — unmodeled IS leaves repeat their prior value.
    for element_id in carry_pool:
      if element_id in prior_values:
        current[element_id] = prior_values[element_id]

    # (a2) Schedule deltas override the carry for schedule expense lines,
    # referenced to the previous walk month (see schedule_is_delta).
    if ctx is not None:
      for element_id in list(current):
        delta = schedule_is_delta(ctx, element_id, month, prev_month)
        if not delta:
          continue
        before = current[element_id]
        after = before + delta
        # Clamp run-off at zero: when base actuals carry less than the
        # schedule facts claim (stale vintages, GL-only corrections), the
        # run-off overshoots, and verification can't catch an economic
        # incoherence.
        if delta < 0 and before >= 0 and after < 0:
          after = 0.0
          clamped_el = _element(element_id)
          skipped.append(
            SkippedForecastLite(
              rule_id=None,
              element_qname=clamped_el.qname if clamped_el else element_id,
              period=month,
              reason=(
                "schedule run-off clamped at zero: the schedule projection "
                "exceeds what the line's base actuals carry"
              ),
            )
          )
        current[element_id] = after

    # (a2b) Line growth: line[t] = line[t-1] * (1 + rate[t]), overriding
    # carry and schedule for the months it names. A stale overlap with a
    # catalog rule (activated after the growth entry was stored) yields to
    # the rule.
    grown_this_month: set[str] = set()
    for element_id, rate_by_month in growth_rates.items():
      rate = rate_by_month.get(month)
      if rate is None:
        continue
      if element_id in active_target_ids:
        skipped.append(
          SkippedForecastLite(
            rule_id=None,
            element_qname=growth_qname_by_id.get(element_id),
            period=month,
            reason="line growth displaced by catalog driver rule",
          )
        )
        continue
      current[element_id] = prior_values.get(element_id, 0.0) * (1.0 + rate)
      grown_this_month.add(element_id)

    # (a3) Line assertions win over carry and schedule for the months they
    # name; a displaced driver rule is skipped in (b).
    asserted_this_month: set[str] = set()
    for element_id, by_month in assertion_values.items():
      if month in by_month:
        current[element_id] = by_month[month]
        asserted_this_month.add(element_id)
    month_asserted_instants = {
      el for el in asserted_this_month if assertion_period_type.get(el) == "instant"
    }
    # Grown values are as authored as asserted ones; push-down must not
    # rescale either.
    pinned_this_month = asserted_this_month | grown_this_month

    # (b) Driver rules in same-month dependency order.
    asserted_ancestors = _ancestor_closure(
      asserted_this_month - month_asserted_instants, parents_by_child
    )
    displaced_targets: set[str] = set()
    for ar in ordered_active:
      if ar.target.id in asserted_this_month:
        displaced_targets.add(ar.target.id)
        skipped.append(
          SkippedForecastLite(
            rule_id=ar.rule.id,
            element_qname=ar.target.qname,
            period=month,
            reason="displaced by line assertion",
          )
        )
        continue
      # A rule driving a calc parent whose valued subtree is entirely
      # pinned has nothing to drive; displace it.
      if ar.target.id in asserted_ancestors and _subtree_all_pinned(
        ar.target.id, calculations, current, pinned_this_month
      ):
        displaced_targets.add(ar.target.id)
        skipped.append(
          SkippedForecastLite(
            rule_id=ar.rule.id,
            element_qname=ar.target.qname,
            period=month,
            reason="displaced by line assertion (contributing children pinned)",
          )
        )
        continue
      raw = ar.rule.rule_expression if isinstance(ar.rule.rule_expression, str) else ""
      expr, prior_operands = desugar_priors(raw)
      expr, avg_operands = desugar_aggregates(expr)
      try:
        parsed = parse_arithmetic_expression(
          expr, ar.operand_names + list(prior_operands) + list(avg_operands)
        )
        lhs_names = lhs_variable_names(parsed)
      except InvalidRuleExpression as exc:
        skipped.append(
          SkippedForecastLite(
            rule_id=ar.rule.id,
            element_qname=ar.target.qname,
            period=month,
            reason=f"expression error: {exc}",
          )
        )
        continue
      if len(lhs_names) != 1:
        skipped.append(
          SkippedForecastLite(
            rule_id=ar.rule.id,
            element_qname=ar.target.qname,
            period=month,
            reason=f"expected a single LHS variable, got {lhs_names!r}",
          )
        )
        continue

      values: dict[str, float] = {}
      missing: list[str] = []
      for name in ar.operand_names:
        if name == lhs_names[0]:
          continue
        qname = ar.qname_by_name.get(name)
        if qname is None:
          missing.append(name)
          continue
        if qname.startswith(DRIVER_PREFIX):
          lever_month_values = lever_values.get(qname, {})
          if month not in lever_month_values:
            missing.append(f"{qname} (lever not asserted for {month})")
            continue
          values[name] = lever_month_values[month]
          continue
        operand_element = _element_by_qname(qname)
        if operand_element is None:
          missing.append(qname)
          continue
        # Same-month value, then same-month derived from children, then
        # prior month: a stale prior never beats a derivable current value.
        if operand_element.id in current:
          values[name] = current[operand_element.id]
          continue
        derived = _derive_from_children(operand_element.id, calculations, current)
        if derived is not None:
          values[name] = derived
        elif operand_element.id in prior_values:
          values[name] = prior_values[operand_element.id]
        else:
          missing.append(qname)

      for synth_name, base_name in prior_operands.items():
        qname = ar.qname_by_name.get(base_name)
        operand_element = _element_by_qname(qname) if qname else None
        if operand_element is None:
          missing.append(qname or base_name)
          continue
        if operand_element.id in prior_values:
          values[synth_name] = prior_values[operand_element.id]
        else:
          missing.append(f"{qname}[t-1] (no prior value)")

      for synth_name in avg_operands:
        # avg() would need a begin/end pair the walk doesn't track.
        missing.append(f"{synth_name} (avg() unsupported in compute-forecast)")

      if missing:
        skipped.append(
          SkippedForecastLite(
            rule_id=ar.rule.id,
            element_qname=ar.target.qname,
            period=month,
            reason="unbound operand(s)",
            missing=missing,
          )
        )
        continue

      try:
        value = evaluate_derivation(parsed, values)
      except InvalidRuleExpression as exc:
        skipped.append(
          SkippedForecastLite(
            rule_id=ar.rule.id,
            element_qname=ar.target.qname,
            period=month,
            reason=f"evaluation error: {exc}",
          )
        )
        continue
      current[ar.target.id] = value

    # A skipped rule's target falls back to carry-forward. Displaced targets
    # don't: a carried parent would beat the derived child sum at the
    # subtotal step.
    for element_id in active_target_ids:
      if (
        element_id not in current
        and element_id not in displaced_targets
        and element_id in prior_values
      ):
        current[element_id] = prior_values[element_id]

    # (b2) A rule targeting a calc parent scales its children
    # proportionally, so the statement's RollUp verification holds.
    _scale_rule_target_children(
      current, active_target_ids, calculations, pinned=pinned_this_month
    )

    # (c) Calc-DAG subtotals — derive, never carry (present = direct wins).
    resolved = resolve_calc_dag(current, set(current), calculations, calc_order)

    # (d) Upsert the month's scenario sets. Provenance `base_period` is the
    # anchor this run seeded from, not the authored base.
    provenance = ForecastProvenance(
      scenario_structure_id=scenario_id,
      base_period=anchor_period,
      month_index=month_index,
      drivers=active_driver_qnames,
    )

    is_facts: list[tuple[Element, float]] = []
    emitted_is: set[str] = set()
    for element_id in base_is_element_ids:
      element = _element(element_id)
      if element is None or element_id not in resolved:
        continue
      is_facts.append((element, resolved[element_id]))
      emitted_is.add(element_id)
    # Duration lines outside the base month's report still emit, with their
    # calc ancestors, or the set can't roll up and fails verification.
    extra_is: set[str] = set()
    for element_id in current:
      if element_id in emitted_is:
        continue
      element = _element(element_id)
      if element is None or element.period_type == "instant":
        continue
      extra_is.add(element_id)
    for ancestor_id in _ancestor_closure(extra_is, parents_by_child):
      if ancestor_id in emitted_is or ancestor_id in extra_is:
        continue
      element = _element(ancestor_id)
      if element is not None and element.period_type != "instant":
        extra_is.add(ancestor_id)
    for element_id in sorted(extra_is):
      element = _element(element_id)
      if element is not None and element_id in resolved:
        is_facts.append((element, resolved[element_id]))
        emitted_is.add(element_id)

    # (d2) Balance-sheet roll + derived CF. With an articulation context the
    # BS is the full roll (carry, rules, schedules, RE, balancing cash) and
    # the CF derives from its deltas; without one (no actual BS at the base
    # period) only the rule-driven working-capital instants are emitted.
    bs_facts: list[tuple[Element, float]] = []
    cf_facts: list[tuple[Element, float]] = []
    bs_values: dict[str, float] = {}
    if ctx is not None:
      bs_values = roll_balance_sheet(
        ctx,
        month_end=month_end,
        prev_end=prev_period_end,
        prior_bs=prior_bs,
        rule_values=current,
        rule_instant_targets=active_instant_ids | month_asserted_instants,
        resolved_is=resolved,
        calculations=calculations,
        calc_order=calc_order,
      )
      final_bs = resolve_calc_dag(bs_values, set(bs_values), calculations, calc_order)
      emitted_bs: set[str] = set()
      for element_id in base_bs_element_ids:
        element = _element(element_id)
        if element is None or element_id not in final_bs:
          continue
        bs_facts.append((element, final_bs[element_id]))
        emitted_bs.add(element_id)
      for element_id in sorted(set(bs_values) - emitted_bs):
        element = _element(element_id)
        if element is not None:
          bs_facts.append((element, bs_values[element_id]))

      if ctx.cf_structure_id is not None:
        cf_values, _plug = derive_cash_flow(
          ctx,
          bs=bs_values,
          prior_bs=prior_bs,
          resolved_is=resolved,
          calculations=calculations,
          calc_order=calc_order,
        )
        for element_id in sorted(cf_values):
          element = _element(element_id)
          if element is not None:
            cf_facts.append((element, cf_values[element_id]))
    else:
      emitted_instants: set[str] = set()
      for ar in ordered_active:
        if ar.target.id in current and ar.target.period_type == "instant":
          bs_facts.append((ar.target, current[ar.target.id]))
          emitted_instants.add(ar.target.id)
      for element_id in sorted(month_asserted_instants - emitted_instants):
        element = _element(element_id)
        if element is not None and element_id in current:
          bs_facts.append((element, current[element_id]))

    is_set_id = _upsert_month_set(
      session,
      structure_id=is_structure_id,
      entity_id=entity_id,
      scenario_id=scenario_id,
      scenario_dimension_id=scenario_dimension_id,
      period_start=month_start,
      period_end=month_end,
      provenance=provenance,
      created_by=created_by,
      facts=is_facts,
    )
    bs_set_id = None
    if bs_structure_id is not None and bs_facts:
      bs_set_id = _upsert_month_set(
        session,
        structure_id=bs_structure_id,
        entity_id=entity_id,
        scenario_id=scenario_id,
        scenario_dimension_id=scenario_dimension_id,
        period_start=month_start,
        period_end=month_end,
        provenance=provenance,
        created_by=created_by,
        facts=bs_facts,
      )
    cf_set_id = None
    if ctx is not None and ctx.cf_structure_id is not None and cf_facts:
      cf_set_id = _upsert_month_set(
        session,
        structure_id=ctx.cf_structure_id,
        entity_id=entity_id,
        scenario_id=scenario_id,
        scenario_dimension_id=scenario_dimension_id,
        period_start=month_start,
        period_end=month_end,
        provenance=provenance,
        created_by=created_by,
        facts=cf_facts,
      )

    # (f) Verify each scenario set with the rule corpus that gates actuals,
    # pinned by fact_set_id.
    verification_passed, verification_failures = _verify_month_sets(
      session,
      sets=(
        (is_structure_id, is_set_id),
        (bs_structure_id, bs_set_id),
        (ctx.cf_structure_id if ctx is not None else None, cf_set_id),
      ),
      period_end=month_end,
      created_by=created_by,
      global_calculations=global_calcs,
    )

    months_computed.append(
      ForecastMonthLite(
        period=month,
        period_start=month_start,
        period_end=month_end,
        income_statement_fact_set_id=is_set_id,
        balance_sheet_fact_set_id=bs_set_id,
        cash_flow_fact_set_id=cf_set_id,
        computed_count=len(is_facts) + len(bs_facts) + len(cf_facts),
        verification_passed=verification_passed,
        verification_failures=verification_failures,
      )
    )

    # (f.1) Halt on a failed month: every later month would chain off it.
    # The failed month's facts are kept for diagnosis. `None` (no rules
    # produced results) doesn't halt but is surfaced below as unverified.
    if verification_passed is False:
      halted_at = month
      diagnostics.append(
        f"Walk halted at {month}: verification failed, and every later "
        f"month would be derived from it. "
        f"{len(months_computed)} of {months_n} months computed. "
        f"Failures: {'; '.join(verification_failures[:3]) or 'unreported'}"
      )
      break
    if verification_passed is None:
      unverified_months.append(month)

    # (e) Roll the window: next month's [t-1]/carry context is this
    # month's resolved IS values + the full balance sheet.
    prior_values.clear()
    for element, value in is_facts:
      prior_values[element.id] = value
    if bs_values:
      prior_values.update(bs_values)
      prior_bs = bs_values
    else:
      for element, value in bs_facts:
        prior_values[element.id] = value
    prev_period_end = month_end
    prev_month = month

  if unverified_months:
    diagnostics.append(
      f"{len(unverified_months)} month(s) ran no verification rules and are "
      f"unverified, not verified: {', '.join(unverified_months[:6])}"
      f"{' …' if len(unverified_months) > 6 else ''}. "
      f"A scenario whose structures carry no bound rules cannot be gated."
    )

  # The scenario must hold exactly the months this run produced.
  if months_computed:
    dropped = _invalidate_stale_tail(
      session,
      scenario_id=scenario_id,
      entity_id=entity_id,
      through_period_end=months_computed[-1].period_end,
    )
    if dropped:
      diagnostics.append(
        f"Dropped {dropped} scenario fact set(s) past {months_computed[-1].period}, "
        f"left over from a longer previous run."
      )

    if anchor_period != mechanics.base_period:
      superseded = _invalidate_superseded_head(
        session,
        scenario_id=scenario_id,
        entity_id=entity_id,
        through_period_end=base_end,
      )
      if superseded:
        diagnostics.append(
          f"Dropped {superseded} scenario fact set(s) at or before "
          f"{anchor_period}, computed by an earlier run for months that "
          f"have since closed."
        )

  session.flush()
  return ComputeForecastResponse(
    structure_id=structure.id,
    scenario_id=scenario_id,
    entity_id=entity_id,
    base_period=mechanics.base_period,
    anchor_period=anchor_period,
    months=months_n,
    months_computed=months_computed,
    halted_at=halted_at,
    skipped=skipped,
    diagnostics=diagnostics,
  )


def _build_parents_by_child(
  calculations: dict[str, list[tuple[str, float]]],
) -> dict[str, list[str]]:
  """Reverse the calc DAG: child element id → parent element ids."""
  parents: dict[str, list[str]] = {}
  for parent, children in calculations.items():
    for child_id, _weight in children:
      parents.setdefault(child_id, []).append(parent)
  return parents


def _ancestor_closure(
  seed_ids: set[str], parents_by_child: dict[str, list[str]]
) -> set[str]:
  """Every calc ancestor reachable upward from the seed elements."""
  closure: set[str] = set()
  stack = list(seed_ids)
  while stack:
    for parent in parents_by_child.get(stack.pop(), ()):
      if parent not in closure:
        closure.add(parent)
        stack.append(parent)
  return closure


def _subtree_all_pinned(
  target_id: str,
  calculations: dict[str, list[tuple[str, float]]],
  current: dict[str, float],
  asserted: set[str],
) -> bool:
  """Whether every valued element under ``target_id`` is pinned (and at
  least one is valued)."""
  has_value = False
  seen: set[str] = set()
  stack = [child for child, _w in calculations.get(target_id, ())]
  while stack:
    node = stack.pop()
    if node in seen:
      continue
    seen.add(node)
    if node in current:
      has_value = True
      if node not in asserted:
        return False
    stack.extend(child for child, _w in calculations.get(node, ()))
  return has_value


def _derive_from_children(
  element_id: str,
  calculations: dict[str, list[tuple[str, float]]],
  current: dict[str, float],
  _seen: set[str] | None = None,
) -> float | None:
  """Σ child·weight over ``current``, recursing through subtotal children.

  None when no descendant carries a value: an absent subtree must stay a
  skip, never a fabricated zero.
  """
  seen = _seen or set()
  if element_id in seen:
    return None
  seen.add(element_id)
  children = calculations.get(element_id)
  if not children:
    return None
  total = 0.0
  any_value = False
  for child_id, weight in children:
    if child_id in current:
      total += current[child_id] * weight
      any_value = True
    else:
      sub = _derive_from_children(child_id, calculations, current, seen)
      if sub is not None:
        total += sub * weight
        any_value = True
  return total if any_value else None


def _scale_rule_target_children(
  current: dict[str, float],
  active_target_ids: set[str],
  calculations: dict[str, list[tuple[str, float]]],
  pinned: set[str] | None = None,
) -> None:
  """Scale a rule-driven calc parent's children (and their subtrees) so they
  sum to the driven value, less the ``pinned`` children's contribution.

  A zero unpinned sum leaves the parent untouched: a visible RollUp failure
  beats inventing a split.
  """
  pinned = pinned or set()

  def _scale_subtree(parent: str, factor: float, visited: set[str]) -> None:
    for child, _weight in calculations.get(parent, ()):
      if child in visited or child not in current or child in pinned:
        continue
      visited.add(child)
      current[child] *= factor
      _scale_subtree(child, factor, visited)

  for target in active_target_ids:
    children = calculations.get(target)
    if not children or target not in current:
      continue
    pinned_sum = sum(
      current[child] * weight
      for child, weight in children
      if child in current and child in pinned
    )
    children_sum = sum(
      current[child] * weight
      for child, weight in children
      if child in current and child not in pinned
    )
    if abs(children_sum) < 1e-9:
      continue
    factor = (current[target] - pinned_sum) / children_sum
    if factor == 1.0:
      continue
    _scale_subtree(target, factor, set())


_MAX_FAILURES_PER_MONTH = 5


def _verify_month_sets(
  session: Session,
  *,
  sets: tuple[tuple[str | None, str | None], ...],
  period_end: date,
  created_by: str,
  global_calculations: dict[str, list[tuple[str, float]]],
) -> tuple[bool | None, list[str]]:
  """Run the rule corpus against each emitted scenario set, replacing prior
  results.

  ``passed`` is ``None`` when no rules produced results.
  """
  from robosystems.models.extensions import VerificationResult
  from robosystems.operations.information_block.rules.engine import (
    evaluate_rules_for_structure,
  )

  any_results = False
  failures: list[str] = []
  for structure_id, fact_set_id in sets:
    if structure_id is None or fact_set_id is None:
      continue
    for stale in (
      session.execute(
        select(VerificationResult).where(VerificationResult.fact_set_id == fact_set_id)
      )
      .scalars()
      .all()
    ):
      session.delete(stale)
    results = evaluate_rules_for_structure(
      session,
      structure_id,
      fact_set_id=fact_set_id,
      period_end=period_end,
      created_by=created_by,
      global_calculations=global_calculations,
    )
    if results:
      any_results = True
    for result in results:
      if result.status in ("fail", "error") and len(failures) < _MAX_FAILURES_PER_MONTH:
        failures.append(f"{result.status}: {result.message}")

  if not any_results:
    return None, []
  return not failures, failures


def _invalidate_stale_tail(
  session: Session,
  *,
  scenario_id: str,
  entity_id: str,
  through_period_end: date,
) -> int:
  """Delete scenario sets past the last month this run produced (left by a
  longer or unhalted previous run, and chained off months since replaced)."""
  return _sweep_scenario_sets(
    session,
    scenario_id=scenario_id,
    entity_id=entity_id,
    period_bound=FactSet.period_end > through_period_end,
  )


def _invalidate_superseded_head(
  session: Session,
  *,
  scenario_id: str,
  entity_id: str,
  through_period_end: date,
) -> int:
  """Delete scenario sets at or before the re-anchored month. Reads prefer
  actuals there, but the sets are still visible to direct graph queries."""
  return _sweep_scenario_sets(
    session,
    scenario_id=scenario_id,
    entity_id=entity_id,
    period_bound=FactSet.period_end <= through_period_end,
  )


def _sweep_scenario_sets(
  session: Session,
  *,
  scenario_id: str,
  entity_id: str,
  period_bound: ColumnElement[bool],
) -> int:
  """Delete the scenario's computed sets matching ``period_bound``.

  The authored lever set carries the same ``scenario_id`` and must survive:
  it is excluded by the pair ``_load_lever_fact_set`` identifies it by
  (``structure_id`` = scenario, ``factset_type='custom'``).
  ``VerificationResult.fact_set_id`` has no FK, so those rows go explicitly.
  """
  stale = (
    session.execute(
      select(FactSet).where(
        FactSet.scenario_id == scenario_id,
        FactSet.entity_id == entity_id,
        period_bound,
        not_(
          and_(
            FactSet.structure_id == scenario_id,
            FactSet.factset_type == "custom",
          )
        ),
      )
    )
    .scalars()
    .all()
  )
  if not stale:
    return 0

  from robosystems.models.extensions import VerificationResult

  set_ids = [fs.id for fs in stale]
  for result in (
    session.execute(
      select(VerificationResult).where(VerificationResult.fact_set_id.in_(set_ids))
    )
    .scalars()
    .all()
  ):
    session.delete(result)
  for fact_set in stale:
    session.delete(fact_set)
  session.flush()
  return len(set_ids)


def _upsert_month_set(
  session: Session,
  *,
  structure_id: str,
  entity_id: str,
  scenario_id: str,
  scenario_dimension_id: str,
  period_start: date,
  period_end: date,
  provenance: ForecastProvenance,
  created_by: str,
  facts: list[tuple[Element, float]],
) -> str | None:
  """Full-replace upsert of one scenario set, keyed by (structure, entity,
  factset_type, period_end, scenario).

  Every fact is stamped with the scenario Dimension so it drops out of
  consolidated (``has_dimensions: false``) reads."""
  if not facts:
    return None
  standing = session.execute(
    select(FactSet)
    .where(
      FactSet.structure_id == structure_id,
      FactSet.factset_type == "report",
      FactSet.entity_id == entity_id,
      FactSet.period_end == period_end,
      FactSet.scenario_id == scenario_id,
    )
    .order_by(FactSet.created_at.desc())
    .limit(1)
  ).scalar_one_or_none()

  if standing is None:
    standing = create_fact_set(
      session,
      structure_id=structure_id,
      period_start=period_start,
      period_end=period_end,
      factset_type="report",
      entity_id=entity_id,
      scenario_id=scenario_id,
      provenance=provenance,
      created_by=created_by,
    )
    session.flush()
  else:
    # Full replace — the month's values are re-derived state.
    for fact in (
      session.execute(select(Fact).where(Fact.fact_set_id == standing.id))
      .scalars()
      .all()
    ):
      session.delete(fact)
    standing.provenance = provenance.model_dump(mode="json")

  new_facts: list[Fact] = []
  for element, value in facts:
    period_type = element.period_type or "duration"
    new_facts.append(
      Fact(
        id=generate_prefixed_ulid("fact"),
        element_id=element.id,
        value=value,
        fact_type="Numeric",
        period_start=None if period_type == "instant" else period_start,
        period_end=period_end,
        period_type=period_type,
        unit=_metric_unit(element),
        entity_id=entity_id,
        structure_id=structure_id,
        fact_set_id=standing.id,
      )
    )
  session.add_all(new_facts)
  session.flush()
  session.execute(
    fact_dimensions.insert(),
    [{"fact_id": fact.id, "dimension_id": scenario_dimension_id} for fact in new_facts],
  )
  return standing.id


__all__ = ["cmd_compute_forecast"]
