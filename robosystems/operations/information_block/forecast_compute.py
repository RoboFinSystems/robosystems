"""compute-forecast — walk a scenario's driver cascade into forward FactSets.

:mod:`.forecast` holds the authored surface (scenario identity, lever
assertions, line assertions, growth rates); this module derives everything
downstream, one forward month at a time from the block's ``base_period``.
Per month, in order:

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

Each month upserts one scenario income-statement FactSet
(``factset_type='report'``, congruent with the actual monthly sets so
statement envelopes render scenario columns unchanged), a balance-sheet set,
and — when an actual cash-flow statement exists — a derived cash-flow set.
All are keyed by ``fact_sets.scenario_id`` = the forecast block, and
re-running a month replaces its values. Without an actual balance sheet at
the base period there is no articulation context, and the balance-sheet set
degrades to the rule-driven working-capital instants alone.

Soft-fail per rule per month: a missing lever month or unbound operand skips
that rule with a reason and its target falls back to carry-forward — one
broken rule never aborts the walk.

Deterministic and non-AI, so free under the credit model; the Operator that
*proposes* lever values is the credit-consuming layer on top.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import select

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
) -> FactSet | None:
  """The newest actual report set for a structure at exactly one month.

  The ``period_start >= period_start`` bound is load-bearing: the final
  monthly period_end coincides with the FY end, and without the window
  guard the ANNUAL comparative set (created later) would win and seed
  ``Revenues[t-1]`` with the FY column instead of the month.

  Canonical sets (report_id IS NULL — the close-time stamp) beat
  publication snapshots, same contract as the envelope loaders: a Report
  published later for the base month must not seed the forecast off a
  frozen snapshot that may have been regenerated with a different style.
  """
  return session.execute(
    select(FactSet)
    .where(
      FactSet.structure_id == structure_id,
      FactSet.factset_type == "report",
      FactSet.scenario_id.is_(None),
      FactSet.entity_id == entity_id,
      FactSet.period_end == period_end,
      FactSet.period_start >= period_start,
    )
    .order_by(
      FactSet.report_id.isnot(None).asc(),
      FactSet.created_at.desc(),
    )
    .limit(1)
  ).scalar_one_or_none()


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

  # Lever + line-assertion VALUES bind from the scenario's authored
  # facts (facts are the values; the mechanics copy is the legible
  # round-trip shape). Levers key by qname (rule operands name qnames);
  # assertions key by element id (the walk works in element-id space).
  element_qname_by_id: dict[str, str] = {
    lv.element_id: lv.qname for lv in mechanics.levers
  }
  assertion_period_type: dict[str, str] = {
    la.element_id: la.period_type for la in mechanics.line_assertions
  }
  # Line-growth rates bind from the mechanics directly — rates aren't
  # facts (a rate on a monetary statement element would lie about its
  # unit), so the mechanics copy is their single authored store.
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

  base_start, base_end = period_date_range(mechanics.base_period)
  base_is_set = _actual_set_at(
    session, is_structure_id, entity_id, base_start, base_end
  )
  if base_is_set is None:
    raise ValueError(
      f"No actual income statement at the base period "
      f"{mechanics.base_period} (a monthly set whose window starts "
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
          period=mechanics.base_period,
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

  ctx: ArticulationContext | None = None
  diagnostics: list[str] = []
  if bs_structure_id is not None and base_bs_set is not None:
    horizon_end = period_date_range(add_months(mechanics.base_period, months_n))[1]
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
  months = [add_months(mechanics.base_period, i) for i in range(1, months_n + 1)]
  active_instant_ids = {
    ar.target.id for ar in ordered_active if ar.target.period_type == "instant"
  }
  prior_bs = dict(bs_prior)
  prev_period_end = base_end
  prev_month = mechanics.base_period

  for month_index, month in enumerate(months, start=1):
    month_start, month_end = period_date_range(month)
    current: dict[str, float] = {}

    # (a) Carry-forward — unmodeled IS leaves repeat their prior value.
    for element_id in carry_pool:
      if element_id in prior_values:
        current[element_id] = prior_values[element_id]

    # (a2) Schedule deltas — a schedule's own projection overrides the
    # carry for its expense lines (an ended schedule's expense stops).
    # The reference month is the PREVIOUS walk month, not the base: the
    # carried value already contains the previous month's schedule
    # contribution (prior_values rolls at (e)), so a base-anchored delta
    # re-subtracts the base-vs-current gap every month — cumulative
    # run-off that marches a line negative even on coherent books.
    # Mirrors schedule_instant_movement's prev_end reference on the BS
    # side; deltas telescope to base + sched[m] - sched[base].
    if ctx is not None:
      for element_id in list(current):
        delta = schedule_is_delta(ctx, element_id, month, prev_month)
        if not delta:
          continue
        before = current[element_id]
        after = before + delta
        # Schedule run-off can't take a line below zero. When the base
        # month's actuals carry less than its schedule facts claim
        # (stale overlapping vintages; prior-period corrections that
        # can only land in the GL, never in schedule facts), the full
        # run-off overshoots the base. Verification can't catch it —
        # the incoherence is economic, not arithmetic — so clamp at
        # zero and say so legibly.
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

    # (a2b) Line growth — the generic per-line trajectory:
    # line[t] = line[t-1] * (1 + rate[t]), compounding through the
    # prior-values roll. Overrides the carry and schedule projection
    # for the months it names; rate-less months keep the carry (a).
    # Authoring rejects overlap with assertions and active catalog
    # rules, but a stale overlap (catalog rule activated after the
    # growth entry was stored) yields to the rule, legibly.
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

    # (a3) Line assertions — the manual overrides win over carry and
    # schedule projection for the months they name; a displaced driver
    # rule is skipped legibly in (b), and dependent rules bind the
    # asserted value through the same-month operand path.
    asserted_this_month: set[str] = set()
    for element_id, by_month in assertion_values.items():
      if month in by_month:
        current[element_id] = by_month[month]
        asserted_this_month.add(element_id)
    month_asserted_instants = {
      el for el in asserted_this_month if assertion_period_type.get(el) == "instant"
    }
    # One owner per line: a grown value is as authored as an asserted one,
    # so the push-down must not rescale it away — the driven parent's
    # remainder distributes over the un-owned siblings instead.
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
      # A rule driving a calc PARENT whose entire valued subtree is
      # pinned by assertions this month has nothing to drive — the
      # push-down would have no unpinned child for the remainder, and
      # the final subtotal derivation would contradict the driven value.
      # Displace it as legibly as a direct-target assertion.
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
        # Same-month value first (carried or rule-computed earlier in
        # the topo order), then same-month DERIVED from valued children
        # (a calc parent like Revenues whose only value this month is an
        # asserted leaf), prior month last — a subtotal base still binds
        # when its rule is inactive, and a stale prior never beats a
        # derivable same-month value.
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
        # No driver rule uses avg() today; binding it would need a
        # begin/end pair the walk doesn't track. Honest skip.
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

    # A skipped rule's target falls back to carry-forward for the month —
    # the honest default, and it keeps the cascade fed for dependents.
    # Displaced targets never fall back: the assertion path owns their
    # value (a carried stale parent would beat the freshly derived
    # child sum at the subtotal step, breaking the very rollup the
    # displacement protects).
    for element_id in active_target_ids:
      if (
        element_id not in current
        and element_id not in displaced_targets
        and element_id in prior_values
      ):
        current[element_id] = prior_values[element_id]

    # (b2) Push rule deltas down the composition — a Derive rule that
    # targets a calc PARENT (Revenues, CostOfRevenue) scales the
    # parent's carried children proportionally, the workbook's implicit
    # semantics (every revenue stream grows at g). Without this the
    # statement's own RollUp verification fails: driven parent, stale
    # children. Asserted and grown leaves are pinned — the remainder
    # distributes over the unpinned children only.
    _scale_rule_target_children(
      current, active_target_ids, calculations, pinned=pinned_this_month
    )

    # (c) Calc-DAG subtotals — derive, never carry (present = direct wins).
    resolved = resolve_calc_dag(current, set(current), calculations, calc_order)

    # (d) Upsert the month's scenario sets.
    provenance = ForecastProvenance(
      scenario_structure_id=scenario_id,
      base_period=mechanics.base_period,
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
    # Duration lines living OUTSIDE the base month's report still emit:
    # the asserted (or assertion-carried) line itself AND its derived
    # calc ancestors — without the ancestors the emitted set can't roll
    # up (Revenues missing over an asserted revenue leaf), the month
    # fails verification with a residual equal to the assertion, and
    # the next month's [t-1] operands never see the derived parent.
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

    # (f) Verify the month — the same rule corpus that gates actuals,
    # pinned to each scenario set (the fact_set_id pin scopes balances
    # and binds; no scenario threading inside the engine). Prior runs
    # for a set are replaced, mirroring the fact upsert's drift
    # semantics — results are per-month state, not append-only history.
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

    # (f.1) Stop the walk on a failed month.
    #
    # Step (e) below rolls this month's closing balances into the next
    # month's opening context, so continuing past a failure does not
    # produce N-1 unverified months — it produces N-1 months *derived from
    # a known-wrong one*, each reporting its own verification status as
    # though it stood alone. Truncating is the honest answer: the caller
    # gets the months that verified plus the one that broke, and
    # ``halted_at`` names where to look.
    #
    # The failed month's facts are deliberately kept. They are already
    # written by the time verification runs, and they are what you need in
    # order to see *why* it failed.
    #
    # `None` does not halt. It means no rules produced results — an
    # absence, not a failure — and halting on it would break any graph
    # whose scenario structures carry no bound rules. But it must not read
    # as a pass either, so it is collected and surfaced once below.
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

  # An unverified month is not a verified one. Reported once rather than
  # per-month so a rule corpus that never binds reads as one loud fact
  # instead of N quiet ones — the failure mode being that `None` has always
  # been indistinguishable from a pass to every consumer.
  if unverified_months:
    diagnostics.append(
      f"{len(unverified_months)} month(s) ran no verification rules and are "
      f"unverified, not verified: {', '.join(unverified_months[:6])}"
      f"{' …' if len(unverified_months) > 6 else ''}. "
      f"A scenario whose structures carry no bound rules cannot be gated."
    )

  session.flush()
  return ComputeForecastResponse(
    structure_id=structure.id,
    scenario_id=scenario_id,
    entity_id=entity_id,
    base_period=mechanics.base_period,
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
  """Whether every valued element under ``target_id`` is line-asserted.

  The displacement test for rules that drive a calc PARENT (the growth
  rule targets Revenues): when the target's entire contribution basis
  this month is pinned by assertions, the rule has nothing left to
  drive — push-down has no unpinned child to absorb the remainder — so
  it must be displaced rather than fight the assertion. A partially
  pinned subtree keeps the rule active (the existing pinned push-down
  distributes the remainder over the unpinned children).
  """
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

  Same-month operand fallback: a rule operand naming a calc parent that
  has no direct value yet (Revenues when only an asserted revenue leaf
  exists) binds its derived value instead of falling to a stale prior.
  Returns None when no descendant carries a value — an absent subtree
  must stay a skip, never a fabricated zero.
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
  """Scale a rule-driven calc parent's present children so the composition
  articulates with the driven value.

  Proportional: each child (and its own subtree, recursively) multiplies by
  ``driven / Σ child·weight``. ``pinned`` elements (line-asserted or
  line-grown leaves) are never scaled — the driven parent's remainder after
  the pinned contributions distributes over the unpinned children instead.

  A zero unpinned children-sum leaves the parent untouched: proportional
  scaling has no basis, and the visible RollUp failure beats inventing a
  split. A skipped rule that fell back to carry scales by exactly 1.0, so
  this is a no-op for inactive months.
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
  """Run the rule corpus against each emitted scenario set, pinned.

  Prior results for a set are deleted first — a recompute replaces the
  month's verification state the same way the fact upsert replaces its
  values. Returns ``(passed, failures)``: ``passed`` is ``None`` when
  no rules produced results, else whether nothing failed/errored;
  ``failures`` carries the first few failed/errored messages.
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
  """Upsert one scenario standing set — the metrics full-replace pattern,
  keyed by (structure, entity, factset_type, period_end, **scenario**).

  Every emitted fact is stamped with the scenario Dimension via
  ``fact_dimensions`` — the explicit-member half of the default-member
  rule (actuals carry no dimension rows and stay in consolidated
  totals; scenario facts carry one and drop out for any reader
  honoring the ``has_dimensions`` contract)."""
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
