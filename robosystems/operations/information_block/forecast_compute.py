"""compute-forecast — walk a scenario's driver cascade into forward FactSets.

The forecast engine's derivation path (FP&A F-1). The forecast block
(:mod:`.forecast`) holds the authored surface — scenario identity +
lever assertions; this module derives everything downstream, one forward
month at a time from the block's ``base_period``:

1. **Carry-forward** — every IS leaf that carried a fact in the base
   month's actual report and isn't rule-driven repeats its prior value
   (the engine default for unmodeled lines — no rules involved).
2. **Driver rules** — the rs-driver catalog's ``Derive`` rules, in
   dependency order (``topo_sort_calculations`` over same-month operand
   edges). A rule is *active* for the scenario iff every rs-driver
   operand it names has asserted lever values; lever values bind from
   the scenario's lever FactSet, ``$X[t-1]`` operands bind the previous
   month's value (:func:`expressions.desugar_priors` — the documented
   avg() sibling seam), and same-month rs-gaap operands bind the
   current month's computed values (prior month as fallback).
3. **Calc-DAG subtotals** — ``resolve_calc_dag`` over the merged
   rs-gaap-calculations + local IS arcs derives GrossProfit →
   OperatingIncome → NetIncome exactly the way the report pivot does
   (articulation is reuse, not new math).

Each month upserts one scenario IS FactSet (``factset_type='report'``,
congruent with the actual monthly sets so statement envelopes render
scenario columns unchanged) plus a working-capital BS set (AR/AP
instants only — the full BS roll is F-2), all keyed by
``fact_sets.scenario_id`` = the forecast block. Re-running a month
replaces its values (the compute-metrics drift semantics). Soft-fail
per rule per month: a missing lever month or unbound operand skips that
rule with a reason and its target falls back to carry-forward — one
broken rule never aborts the walk.

Deterministic and non-AI — free under the credit model. The Operator
that *proposes* lever values is the credit-consuming layer on top.
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
  Taxonomy,
)
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.roboledger.fact_set import FactSet
from robosystems.operations.information_block.forecast import (
  FORECAST_BLOCK_TYPE,
  _load_lever_fact_set,
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

_DRIVER_STANDARD = "rs-driver"
_DRIVER_PREFIX = "rs-driver:"


@dataclass
class _ActiveRule:
  """A driver rule resolved + activated for this scenario run."""

  rule: Rule
  target: Element
  qname_by_name: dict[str, str]
  operand_names: list[str]


def _newest_actual_structure_id(session: Session, block_type: str) -> str | None:
  """Structure behind the entity's newest actual report set of a block type.

  Data-driven (never by name) — multi-variant reporting styles mean the
  'income_statement' structure a tenant actually reports under is
  whichever one its newest actual ``'report'`` FactSet instantiates.
  """
  return session.execute(
    select(FactSet.structure_id)
    .join(Structure, FactSet.structure_id == Structure.id)
    .where(
      FactSet.factset_type == "report",
      FactSet.scenario_id.is_(None),
      Structure.block_type == block_type,
    )
    .order_by(FactSet.created_at.desc())
    .limit(1)
  ).scalar()


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
    .order_by(FactSet.created_at.desc())
    .limit(1)
  ).scalar_one_or_none()


def _numeric_facts(session: Session, fact_set_id: str) -> list[Fact]:
  return list(
    session.execute(
      select(Fact).where(
        Fact.fact_set_id == fact_set_id,
        Fact.fact_scope == "in_scope",
        Fact.value.is_not(None),
      )
    )
    .scalars()
    .all()
  )


def _load_driver_rules(session: Session) -> list[Rule]:
  """Every Derive rule seeded by the rs-driver catalog (tenant copy)."""
  taxonomy_ids = (
    session.execute(select(Taxonomy.id).where(Taxonomy.standard == _DRIVER_STANDARD))
    .scalars()
    .all()
  )
  if not taxonomy_ids:
    return []
  return list(
    session.execute(
      select(Rule)
      .where(Rule.taxonomy_id.in_(taxonomy_ids), Rule.rule_pattern == "Derive")
      .order_by(Rule.id)
    )
    .scalars()
    .all()
  )


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

  # Lever VALUES bind from the scenario's authored facts (facts are the
  # values; the mechanics copy is the legible round-trip shape).
  element_qname_by_id: dict[str, str] = {
    lv.element_id: lv.qname for lv in mechanics.levers
  }
  lever_values: dict[str, dict[str, float]] = {}
  for fact in _numeric_facts(session, lever_set.id):
    qname = element_qname_by_id.get(fact.element_id)
    if qname is None or fact.value is None:
      continue
    month = period_from_date(fact.period_end)
    lever_values.setdefault(qname, {})[month] = float(fact.value)

  # ── Resolve the actual structures + seed month ────────────────────────
  is_structure_id = _newest_actual_structure_id(session, "income_statement")
  if is_structure_id is None:
    raise ValueError(
      "No actual income-statement report FactSet exists — generate at "
      "least one report before computing a forecast."
    )
  bs_structure_id = _newest_actual_structure_id(session, "balance_sheet")

  base_start, base_end = period_date_range(mechanics.base_period)
  base_is_set = _actual_set_at(
    session, is_structure_id, entity_id, base_start, base_end
  )
  if base_is_set is None:
    raise ValueError(
      f"No actual income-statement report at the base period "
      f"{mechanics.base_period} (a monthly report whose window starts "
      f"{base_start} and ends {base_end}). Generate monthly reports "
      "through the base period, or set base_period to a month that has "
      "one."
    )

  prior_values: dict[str, float] = {}
  base_is_element_ids: list[str] = []
  for fact in _numeric_facts(session, base_is_set.id):
    if fact.value is None:
      continue
    if fact.element_id not in prior_values:
      base_is_element_ids.append(fact.element_id)
    prior_values[fact.element_id] = float(fact.value)

  # BS instants seed [t-1]/carry context for balance-driven rules.
  if bs_structure_id is not None:
    base_bs_set = _actual_set_at(
      session, bs_structure_id, entity_id, base_start, base_end
    )
    if base_bs_set is not None:
      for fact in _numeric_facts(session, base_bs_set.id):
        if fact.value is not None and fact.element_id not in prior_values:
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
  for rule in _load_driver_rules(session):
    variables = rule.rule_variables or []
    names = [v.get("variable_name") for v in variables if isinstance(v, dict)]
    if not all(isinstance(n, str) and n for n in names):
      continue
    qname_by_name: dict[str, str] = {
      v["variable_name"]: v["variable_qname"]
      for v in variables
      if isinstance(v, dict) and isinstance(v.get("variable_qname"), str)
    }
    lever_qnames = [q for q in qname_by_name.values() if q.startswith(_DRIVER_PREFIX)]
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
      if q.startswith(_DRIVER_PREFIX)
    }
  )

  # ── Calc DAG + carry pool ─────────────────────────────────────────────
  calculations = merge_calculations(
    load_rs_gaap_calculations(session), _local_calc_arcs(session, is_structure_id)
  )
  calc_order = topo_sort_calculations(calculations)
  calc_targets = set(calculations.keys())
  carry_pool = [
    el
    for el in base_is_element_ids
    if el not in calc_targets and el not in active_target_ids
  ]

  elements_by_id: dict[str, Element] = {}

  def _element(element_id: str) -> Element | None:
    if element_id not in elements_by_id:
      loaded = session.get(Element, element_id)
      if loaded is not None:
        elements_by_id[element_id] = loaded
    return elements_by_id.get(element_id)

  # ── The walk ──────────────────────────────────────────────────────────
  months_computed: list[ForecastMonthLite] = []
  months = [add_months(mechanics.base_period, i) for i in range(1, months_n + 1)]

  for month_index, month in enumerate(months, start=1):
    month_start, month_end = period_date_range(month)
    current: dict[str, float] = {}

    # (a) Carry-forward — unmodeled IS leaves repeat their prior value.
    for element_id in carry_pool:
      if element_id in prior_values:
        current[element_id] = prior_values[element_id]

    # (b) Driver rules in same-month dependency order.
    for ar in ordered_active:
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
        if qname.startswith(_DRIVER_PREFIX):
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
        # Same-month value first (carried or rule-computed earlier in the
        # topo order), prior month as the fallback — a subtotal base like
        # Revenues-as-rollup still binds even when its rule is inactive.
        if operand_element.id in current:
          values[name] = current[operand_element.id]
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
    for element_id in active_target_ids:
      if element_id not in current and element_id in prior_values:
        current[element_id] = prior_values[element_id]

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
    for element_id in base_is_element_ids:
      element = _element(element_id)
      if element is None or element_id not in resolved:
        continue
      is_facts.append((element, resolved[element_id]))

    bs_facts: list[tuple[Element, float]] = []
    for ar in ordered_active:
      if ar.target.id in current and ar.target.period_type == "instant":
        bs_facts.append((ar.target, current[ar.target.id]))

    is_set_id = _upsert_month_set(
      session,
      structure_id=is_structure_id,
      entity_id=entity_id,
      scenario_id=scenario_id,
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
        period_start=month_start,
        period_end=month_end,
        provenance=provenance,
        created_by=created_by,
        facts=bs_facts,
      )

    months_computed.append(
      ForecastMonthLite(
        period=month,
        period_start=month_start,
        period_end=month_end,
        income_statement_fact_set_id=is_set_id,
        balance_sheet_fact_set_id=bs_set_id,
        computed_count=len(is_facts) + len(bs_facts),
      )
    )

    # (e) Roll the window: next month's [t-1]/carry context is this
    # month's resolved IS values + the balance instants.
    prior_values.clear()
    for element, value in is_facts:
      prior_values[element.id] = value
    for element, value in bs_facts:
      prior_values[element.id] = value

  session.flush()
  return ComputeForecastResponse(
    structure_id=structure.id,
    scenario_id=scenario_id,
    entity_id=entity_id,
    base_period=mechanics.base_period,
    months=months_n,
    months_computed=months_computed,
    skipped=skipped,
  )


def _upsert_month_set(
  session: Session,
  *,
  structure_id: str,
  entity_id: str,
  scenario_id: str,
  period_start: date,
  period_end: date,
  provenance: ForecastProvenance,
  created_by: str,
  facts: list[tuple[Element, float]],
) -> str | None:
  """Upsert one scenario standing set — the metrics full-replace pattern,
  keyed by (structure, entity, factset_type, period_end, **scenario**)."""
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

  for element, value in facts:
    period_type = element.period_type or "duration"
    session.add(
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
  session.flush()
  return standing.id


__all__ = ["cmd_compute_forecast"]
