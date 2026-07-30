"""compute-metrics / assert-metrics — write a standing metric FactSet.

The metric block's two write paths. ``compute-metrics`` (Metrics M-1 +
the M-2 grammar) evaluates ``Derive`` rules: the same ``$Var``
expression grammar as verification rules, but evaluated for a VALUE —
the LHS names the metric element being computed, the RHS operands bind
to the entity's persisted facts at the requested ``period_end``, and
the result is written as a Numeric fact in a standing
``factset_type='metric'`` FactSet — one per (structure, entity,
period_end), so successive runs accumulate the time series and
re-running a period replaces its values. ``assert-metrics`` is the
observation sibling: externally-observed values arrive in the request
and land on the same standing-set shape with ``AssertedProvenance``;
structures carrying Derive rules are compute-owned and rejected, so
asserted and derived series keep disjoint structures.

M-2 grammar:

- ``avg($X)`` — the period average of an instant operand,
  (begin + end) / 2. Desugared pre-parse to a synthesized ``$__avg_X``
  operand (``expressions.desugar_aggregates``); the begin fact binds at
  a resolved prior period end — ``body.period_start - 1 day`` when the
  request carries a window, else a bound duration operand's start - 1
  day, else the entity's newest report ``period_end`` strictly before
  the requested one (run-cached; never the fiscal calendar, which
  annual-comparative tenants don't populate at month ends).
- **Composition** (DuPont) — an operand naming another rule's target is
  an in-run metric dependency: rules evaluate in Kahn topological order
  (``calc_dag.topo_sort_calculations``), in-run values resolve first,
  and persisted-fact binding is widened to ``('report', 'metric')`` so
  cross-structure metrics resolve when already computed for the period.
  A cyclic or skipped dependency leaves the operand unbound → the
  dependent metric soft-skips.

The future ``$X[t-1]`` prior-period operand (the forecast grammar's
general form) rides the same two seams: a pre-parse rewrite to a
synthesized operand, bound through the same prior-period resolver.

Soft-fail per metric: a missing operand fact (InterestExpense for a
debt-free entity), an unresolvable qname, a missing prior period for
``avg()``, or an undefined ratio (division by zero) skips that metric
with a reason — one broken metric never aborts the run.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from sqlalchemy import func, or_, select, text
from sqlalchemy.orm import Session

from robosystems.models.api.fact_provenance import (
  AssertedProvenance,
  DerivedProvenance,
)
from robosystems.models.api.information_block import (
  AssertedMetricLite,
  AssertMetricsRequest,
  AssertMetricsResponse,
  ComputedMetricLite,
  ComputeMetricsRequest,
  ComputeMetricsResponse,
  MetricObservation,
  SkippedMetricLite,
)
from robosystems.models.extensions import (
  Association,
  Element,
  Rule,
  Structure,
)
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.roboledger.fact_set import FactSet

# From the envelope module (which owns the constant), NOT the registry —
# the registry imports every handler module, so a registry import from a
# module the handlers reach (forecast → metrics) would be circular.
from robosystems.operations.information_block.metric import METRIC_BLOCK_TYPE
from robosystems.operations.information_block.rules.expressions import (
  InvalidRuleExpression,
  desugar_aggregates,
  evaluate_derivation,
  lhs_variable_names,
  parse_arithmetic_expression,
)
from robosystems.operations.roboledger.fact_set import create_fact_set
from robosystems.operations.roboledger.reports.calc_dag import topo_sort_calculations
from robosystems.utils.ulid import generate_prefixed_ulid


@dataclass(frozen=True)
class _BoundOperand:
  value: float
  period_start: date | None


def _default_entity_id(session: Session) -> str:
  """Earliest-created entity — the primary entity for single-entity graphs.

  Same convention as ``reports._get_entity_id`` / the text-block bind.
  """
  row = session.execute(
    text("SELECT id FROM entities ORDER BY created_at ASC LIMIT 1")
  ).fetchone()
  if row is None:
    raise ValueError("No entity found. Import data before computing metrics.")
  return row.id


def _bind_operand(
  session: Session,
  *,
  element_id: str,
  entity_id: str,
  period_end: date,
  period_start: date | None,
  scenario_id: str | None = None,
) -> _BoundOperand | None:
  """Most recent persisted fact for one operand at ``period_end``.

  Joins Fact → FactSet on ``factset_type IN ('report', 'metric')`` —
  the persisted statement facts (including calc-DAG subtotals) plus
  standing metric facts, so a metric operand resolves across structures
  once its own compute has run for the period. Scoped to the entity;
  ``period_end`` must match exactly — instant balances as of that date,
  durations ending on it. Newest set wins when several cover the
  period; among same-set candidates the longest window wins (FY over Q4
  when both end on the date). No qname can collide across the two set
  types: metric facts land only on metric-namespace elements.

  ``scenario_id=None`` binds actuals only (``scenario_id IS NULL`` —
  the pin that keeps forward scenario facts out of every actual
  compute). Non-None binds the scenario's facts WITH actuals as the
  fallback, scenario preferred — the moving seam: a forward month's
  operands resolve from the scenario slice while an ``avg()`` begin
  bind at the seam still reaches the actual base month.
  """
  stmt = (
    select(Fact.value, Fact.period_start)
    .join(FactSet, Fact.fact_set_id == FactSet.id)
    .where(
      FactSet.factset_type.in_(("report", "metric")),
      FactSet.scenario_id.is_(None)
      if scenario_id is None
      else or_(FactSet.scenario_id.is_(None), FactSet.scenario_id == scenario_id),
      Fact.entity_id == entity_id,
      Fact.element_id == element_id,
      Fact.period_end == period_end,
      Fact.fact_scope == "in_scope",
      Fact.value.is_not(None),
    )
    .order_by(
      FactSet.scenario_id.asc().nulls_last(),
      FactSet.created_at.desc(),
      Fact.period_start.asc().nulls_first(),
    )
    .limit(1)
  )
  if period_start is not None:
    stmt = stmt.where(
      or_(Fact.period_start.is_(None), Fact.period_start >= period_start)
    )
  row = session.execute(stmt).first()
  if row is None:
    return None
  return _BoundOperand(value=float(row[0]), period_start=row[1])


def _latest_report_period_end_before(
  session: Session,
  entity_id: str,
  before: date,
  scenario_id: str | None = None,
) -> date | None:
  """The entity's newest report-fact ``period_end`` strictly before a date.

  The data-driven prior-period fallback for ``avg()`` when neither the
  request nor a bound duration operand supplies a window: annual
  comparatives resolve to the prior FY end, monthly series to the prior
  month end. Report facts only — the canonical period spine.
  ``scenario_id`` widens the spine to include the scenario's forward
  months (a forward month's prior is usually the previous forward
  month); ``None`` pins actuals.
  """
  return session.execute(
    select(func.max(Fact.period_end))
    .select_from(Fact)
    .join(FactSet, Fact.fact_set_id == FactSet.id)
    .where(
      FactSet.factset_type == "report",
      FactSet.scenario_id.is_(None)
      if scenario_id is None
      else or_(FactSet.scenario_id.is_(None), FactSet.scenario_id == scenario_id),
      Fact.entity_id == entity_id,
      Fact.period_end < before,
      Fact.fact_scope == "in_scope",
      Fact.value.is_not(None),
    )
  ).scalar()


def _metric_unit(target: Element) -> str:
  """Fact unit for a computed metric — from ``item_type``, else the
  ``is_monetary`` binary (NULL ``item_type`` keeps the M-1 behavior)."""
  if target.item_type is not None:
    return {"monetary": "USD", "days": "days"}.get(target.item_type, "pure")
  return "USD" if target.is_monetary else "pure"


def _load_derive_rules(
  session: Session, structure_id: str, element_ids: list[str]
) -> list[Rule]:
  """Derive rules scoped to the structure or its elements."""
  conditions = [Rule.target_structure_id == structure_id]
  if element_ids:
    conditions.append(Rule.target_element_id.in_(element_ids))
  return list(
    session.execute(
      select(Rule)
      .where(or_(*conditions), Rule.rule_pattern == "Derive")
      .order_by(Rule.id)
    )
    .scalars()
    .all()
  )


def cmd_compute_metrics(
  session: Session,
  body: ComputeMetricsRequest,
  created_by: str,
) -> ComputeMetricsResponse:
  """Compute every Derive rule on a metric block for one period.

  Upserts the period's standing metric FactSet: found → all its facts are
  deleted and provenance is re-stamped (re-bind drift semantics); absent →
  created via the blessed ``create_fact_set`` path with
  ``DerivedProvenance``. ``session.flush()`` before returning; the
  OperationSpec wrapper owns the commit.
  """
  structure = session.get(Structure, body.structure_id)
  if structure is None:
    raise ValueError(f"Structure not found: {body.structure_id}")
  if structure.block_type != METRIC_BLOCK_TYPE:
    raise ValueError(
      f"compute-metrics targets a block_type='metric' structure; "
      f"{body.structure_id!r} is {structure.block_type!r}"
    )

  entity_id = body.entity_id or _default_entity_id(session)

  associations = (
    session.execute(
      select(Association).where(Association.structure_id == body.structure_id)
    )
    .scalars()
    .all()
  )
  element_ids = {
    x
    for x in (
      {a.from_element_id for a in associations}
      | {a.to_element_id for a in associations}
    )
    if x is not None
  }
  # Presentation order of the catalog — computed facts and response rows
  # follow it so the envelope renders in arc order.
  order_by_element: dict[str, float] = {}
  for a in associations:
    if a.to_element_id is not None and a.order_value is not None:
      order_by_element.setdefault(a.to_element_id, float(a.order_value))

  rules = _load_derive_rules(session, body.structure_id, list(element_ids))
  rules.sort(
    key=lambda r: order_by_element.get(r.target_element_id or "", float("inf"))
  )

  # Dependency-ordered evaluation (M-2 composition): an operand qname
  # naming another rule's target is an in-run metric dependency, so the
  # run evaluates dependencies first. Rules sort by dependency DEPTH
  # (longest in-run chain below the target), arc order as the tiebreak —
  # depth is deterministic where Kahn's emission order among independents
  # is not, so independent metrics keep presentation order. Cycle
  # remnants get a finite depth and soft-skip when their operands stay
  # unbound.
  targets_by_rule = {
    rule.id: (
      session.get(Element, rule.target_element_id) if rule.target_element_id else None
    )
    for rule in rules
  }
  run_target_qnames = {
    t.qname for t in targets_by_rule.values() if t is not None and t.qname
  }
  dependency_map: dict[str, list[tuple[str, float]]] = {}
  for rule in rules:
    target = targets_by_rule.get(rule.id)
    if target is None or not target.qname:
      continue
    operand_qnames = {
      v.get("variable_qname")
      for v in (rule.rule_variables or [])
      if isinstance(v, dict)
    }
    dependency_map[target.qname] = [
      (q, 1.0)
      for q in operand_qnames
      if q and q != target.qname and q in run_target_qnames
    ]
  depth_by_qname: dict[str, int] = {}
  for qname in topo_sort_calculations(dependency_map):
    depth_by_qname[qname] = 1 + max(
      (depth_by_qname.get(d, 0) for d, _ in dependency_map.get(qname, [])),
      default=-1,
    )
  rules.sort(
    key=lambda r: (
      depth_by_qname.get(getattr(targets_by_rule.get(r.id), "qname", None) or "", 0),
      order_by_element.get(r.target_element_id or "", float("inf")),
    )
  )

  computed: list[ComputedMetricLite] = []
  skipped: list[SkippedMetricLite] = []
  pending_facts: list[dict] = []
  computed_by_qname: dict[str, float] = {}

  qname_cache: dict[str, Element | None] = {}

  def _element_by_qname(qname: str) -> Element | None:
    if qname not in qname_cache:
      qname_cache[qname] = session.execute(
        select(Element).where(Element.qname == qname).limit(1)
      ).scalar_one_or_none()
    return qname_cache[qname]

  # Data-driven prior period for avg() — resolved at most once per run
  # so every parameterless avg in the run shares one begin date.
  _prior_cache: list[date | None] = []

  def _data_driven_prior() -> date | None:
    if not _prior_cache:
      _prior_cache.append(
        _latest_report_period_end_before(
          session, entity_id, body.period_end, body.scenario_id
        )
      )
    return _prior_cache[0]

  def _begin_date(duration_start: date | None) -> date | None:
    if body.period_start is not None:
      return body.period_start - timedelta(days=1)
    if duration_start is not None:
      return duration_start - timedelta(days=1)
    return _data_driven_prior()

  for rule in rules:
    target = targets_by_rule.get(rule.id)
    target_qname = target.qname if target is not None else None

    def _skip(reason: str, missing: list[str] | None = None) -> None:
      skipped.append(
        SkippedMetricLite(
          rule_id=rule.id,
          element_qname=target_qname,
          reason=reason,
          missing=missing or [],
        )
      )

    if target is None:
      _skip("rule has no resolvable target element")
      continue

    variables = rule.rule_variables or []
    names = [v.get("variable_name") for v in variables]
    if not all(isinstance(n, str) and n for n in names):
      _skip("rule variables are missing variable_name entries")
      continue
    qname_by_name = {v["variable_name"]: v.get("variable_qname") for v in variables}

    raw_expression = (
      rule.rule_expression if isinstance(rule.rule_expression, str) else ""
    )
    expression, avg_operands = desugar_aggregates(raw_expression)
    try:
      parsed = parse_arithmetic_expression(expression, names + list(avg_operands))
      lhs_names = lhs_variable_names(parsed)
    except InvalidRuleExpression as exc:
      _skip(f"expression error: {exc}")
      continue
    if len(lhs_names) != 1:
      _skip(f"expected a single LHS metric variable, got {lhs_names!r}")
      continue

    operand_names = [n for n in names if n != lhs_names[0]]
    values: dict[str, float] = {}
    missing: list[str] = []
    duration_start: date | None = None
    for name in operand_names:
      qname = qname_by_name.get(name)
      if qname and qname in computed_by_qname:
        # In-run metric dependency — topo order guarantees it ran first.
        values[name] = computed_by_qname[qname]
        continue
      operand_element = _element_by_qname(qname) if qname else None
      if operand_element is None:
        missing.append(qname or name)
        continue
      bound = _bind_operand(
        session,
        element_id=operand_element.id,
        entity_id=entity_id,
        period_end=body.period_end,
        period_start=body.period_start,
        scenario_id=body.scenario_id,
      )
      if bound is None:
        if qname and qname in run_target_qnames:
          missing.append(f"{qname} (metric not computed)")
        else:
          missing.append(qname or name)
        continue
      values[name] = bound.value
      if bound.period_start is not None and duration_start is None:
        duration_start = bound.period_start

    # Second pass — avg() operands. The end value is the base operand's
    # bound value (declared operands always bind above); the begin fact
    # binds at the resolved prior period end.
    for synth_name, base_name in avg_operands.items():
      if base_name not in values:
        if base_name not in names:
          missing.append(f"{base_name} (avg operand not declared)")
        continue  # base already recorded as missing above
      begin_date = _begin_date(duration_start)
      qname = qname_by_name.get(base_name)
      if begin_date is None:
        missing.append(f"{qname or base_name} (no prior period for avg)")
        continue
      operand_element = _element_by_qname(qname) if qname else None
      if operand_element is None:
        missing.append(qname or base_name)
        continue
      begin_bound = _bind_operand(
        session,
        element_id=operand_element.id,
        entity_id=entity_id,
        period_end=begin_date,
        period_start=None,
        scenario_id=body.scenario_id,
      )
      if begin_bound is None:
        missing.append(f"{qname} (prior @ {begin_date})")
        continue
      values[synth_name] = (begin_bound.value + values[base_name]) / 2.0

    if missing:
      _skip("no report fact bound for operand(s)", missing=missing)
      continue

    try:
      value = evaluate_derivation(parsed, values)
    except InvalidRuleExpression as exc:
      _skip(f"evaluation error: {exc}")
      continue

    unit = _metric_unit(target)
    period_type = target.period_type or "instant"
    fact_period_start = None
    if period_type == "duration":
      fact_period_start = body.period_start or duration_start

    pending_facts.append(
      {
        "element_id": target.id,
        "value": value,
        "period_start": fact_period_start,
        "period_type": period_type,
        "unit": unit,
      }
    )
    computed.append(
      ComputedMetricLite(
        rule_id=rule.id,
        element_id=target.id,
        element_qname=target_qname,
        name=target.name,
        value=value,
        unit=unit,
        period_type=period_type,
        item_type=target.item_type,
      )
    )
    if target_qname:
      computed_by_qname[target_qname] = value

  # Evaluation ran in dependency order; present in arc order.
  computed.sort(key=lambda m: order_by_element.get(m.element_id, float("inf")))
  pending_facts.sort(
    key=lambda pf: order_by_element.get(pf["element_id"], float("inf"))
  )

  provenance = DerivedProvenance(computation="compute-metrics", source_fact_ids=[])

  standing = session.execute(
    select(FactSet)
    .where(
      FactSet.structure_id == body.structure_id,
      FactSet.factset_type == "metric",
      FactSet.entity_id == entity_id,
      FactSet.period_end == body.period_end,
      # Scenario slices keep their own standing sets — an actual compute
      # never replaces a scenario month and vice versa.
      FactSet.scenario_id.is_(None)
      if body.scenario_id is None
      else FactSet.scenario_id == body.scenario_id,
    )
    .order_by(FactSet.created_at.desc())
    .limit(1)
  ).scalar_one_or_none()

  if standing is None and pending_facts:
    standing = create_fact_set(
      session,
      structure_id=body.structure_id,
      period_start=body.period_start,
      period_end=body.period_end,
      factset_type="metric",
      entity_id=entity_id,
      scenario_id=body.scenario_id,
      provenance=provenance,
      created_by=created_by,
    )
    session.flush()
  elif standing is not None:
    # Full replace — the period's values are re-derived state.
    for fact in (
      session.execute(select(Fact).where(Fact.fact_set_id == standing.id))
      .scalars()
      .all()
    ):
      session.delete(fact)
    standing.provenance = provenance.model_dump(mode="json")

  if standing is not None:
    for pf in pending_facts:
      session.add(
        Fact(
          id=generate_prefixed_ulid("fact"),
          element_id=pf["element_id"],
          value=pf["value"],
          fact_type="Numeric",
          period_start=pf["period_start"],
          period_end=body.period_end,
          period_type=pf["period_type"],
          unit=pf["unit"],
          entity_id=entity_id,
          structure_id=body.structure_id,
          fact_set_id=standing.id,
        )
      )
    session.flush()

  return ComputeMetricsResponse(
    structure_id=body.structure_id,
    entity_id=entity_id,
    period_end=body.period_end,
    fact_set_id=standing.id if standing is not None else None,
    computed=computed,
    skipped=skipped,
  )


# Mirrors forecast.py's _ASSERTION_BLOCKED_SOURCES — duplicated rather than
# imported because forecast imports this module, so the reverse import would
# be circular. Library-computed and lever concepts are never asserted here.
_ASSERTION_BLOCKED_ELEMENT_SOURCES = {
  "rs-metric": "rs-metric concepts are computed by compute-metrics, never asserted",
  "rs-driver": "rs-driver concepts are forecast levers — assert them via `levers`",
}


def cmd_assert_metrics(
  session: Session,
  body: AssertMetricsRequest,
  created_by: str,
) -> AssertMetricsResponse:
  """Write externally-observed metric values for one period.

  The observation sibling of ``cmd_compute_metrics``: same standing
  FactSet upsert (found → all its facts are deleted and provenance is
  re-stamped; absent → created via the blessed ``create_fact_set``
  path), but the values arrive in the request and the set is stamped
  with ``AssertedProvenance``. Structures carrying Derive rules are
  compute-owned and rejected — asserted and derived metric series keep
  disjoint structures, which is also what keeps the two writers' full-
  replace semantics from clobbering each other. ``session.flush()``
  before returning; the OperationSpec wrapper owns the commit.
  """
  structure = session.get(Structure, body.structure_id)
  if structure is None:
    raise ValueError(f"Structure not found: {body.structure_id}")
  if structure.block_type != METRIC_BLOCK_TYPE:
    raise ValueError(
      f"assert-metrics targets a block_type='metric' structure; "
      f"{body.structure_id!r} is {structure.block_type!r}"
    )

  entity_id = body.entity_id or _default_entity_id(session)

  associations = (
    session.execute(
      select(Association).where(Association.structure_id == body.structure_id)
    )
    .scalars()
    .all()
  )
  element_ids = {
    x
    for x in (
      {a.from_element_id for a in associations}
      | {a.to_element_id for a in associations}
    )
    if x is not None
  }
  order_by_element: dict[str, float] = {}
  for a in associations:
    if a.to_element_id is not None and a.order_value is not None:
      order_by_element.setdefault(a.to_element_id, float(a.order_value))

  rules = _load_derive_rules(session, body.structure_id, list(element_ids))
  if rules:
    raise ValueError(
      f"Structure {body.structure_id!r} carries {len(rules)} Derive rule(s) "
      "and is compute-owned (compute-metrics); asserted and derived metric "
      "series keep disjoint structures"
    )

  seen: set[str] = set()
  duplicates: set[str] = set()
  for obs in body.observations:
    if obs.qname in seen:
      duplicates.add(obs.qname)
    seen.add(obs.qname)
  if duplicates:
    raise ValueError(
      f"Duplicate observation qname(s): {sorted(duplicates)!r} — one "
      "observation per concept per period"
    )

  qname_cache: dict[str, Element | None] = {}

  def _element_by_qname(qname: str) -> Element | None:
    if qname not in qname_cache:
      qname_cache[qname] = session.execute(
        select(Element).where(Element.qname == qname).limit(1)
      ).scalar_one_or_none()
    return qname_cache[qname]

  unknown: list[str] = []
  abstract: list[str] = []
  blocked: list[str] = []
  outside: list[str] = []
  resolved: list[tuple[MetricObservation, Element]] = []
  for obs in body.observations:
    element = _element_by_qname(obs.qname)
    if element is None:
      unknown.append(obs.qname)
      continue
    if element.is_abstract:
      abstract.append(obs.qname)
      continue
    doctrine = _ASSERTION_BLOCKED_ELEMENT_SOURCES.get(element.source or "")
    if doctrine is not None:
      blocked.append(f"{obs.qname} ({doctrine})")
      continue
    if element_ids and element.id not in element_ids:
      # Facts for concepts outside the presentation catalog would persist
      # but never render in the envelope — reject rather than no-op.
      outside.append(obs.qname)
      continue
    resolved.append((obs, element))

  problems: list[str] = []
  if unknown:
    problems.append(f"unknown qname(s): {unknown!r}")
  if abstract:
    problems.append(f"abstract concept(s) cannot carry facts: {abstract!r}")
  if blocked:
    problems.append(f"blocked concept(s): {blocked!r}")
  if outside:
    problems.append(
      f"concept(s) not on the structure's presentation catalog: {outside!r}"
    )
  if problems:
    raise ValueError("assert-metrics rejected: " + "; ".join(problems))

  resolved.sort(key=lambda pair: order_by_element.get(pair[1].id, float("inf")))

  asserted: list[AssertedMetricLite] = []
  pending_facts: list[dict] = []
  for obs, element in resolved:
    unit = _metric_unit(element)
    period_type = element.period_type or "instant"
    fact_period_start = body.period_start if period_type == "duration" else None
    pending_facts.append(
      {
        "element_id": element.id,
        "value": obs.value,
        "period_start": fact_period_start,
        "period_type": period_type,
        "unit": unit,
      }
    )
    asserted.append(
      AssertedMetricLite(
        element_id=element.id,
        element_qname=obs.qname,
        name=element.name,
        value=obs.value,
        unit=unit,
        period_type=period_type,
        item_type=element.item_type,
      )
    )

  provenance = AssertedProvenance(
    source_system=body.source_system,
    asserted_by=created_by,
    basis_note=body.basis_note,
  )

  standing = session.execute(
    select(FactSet)
    .where(
      FactSet.structure_id == body.structure_id,
      FactSet.factset_type == "metric",
      FactSet.entity_id == entity_id,
      FactSet.period_end == body.period_end,
      # Asserted series are actuals — the scenario axis never applies.
      FactSet.scenario_id.is_(None),
    )
    .order_by(FactSet.created_at.desc())
    .limit(1)
  ).scalar_one_or_none()

  replaced = standing is not None
  if standing is None:
    standing = create_fact_set(
      session,
      structure_id=body.structure_id,
      period_start=body.period_start,
      period_end=body.period_end,
      factset_type="metric",
      entity_id=entity_id,
      provenance=provenance,
      created_by=created_by,
    )
    session.flush()
  else:
    # Full replace — the period's values are re-asserted state.
    for fact in (
      session.execute(select(Fact).where(Fact.fact_set_id == standing.id))
      .scalars()
      .all()
    ):
      session.delete(fact)
    standing.provenance = provenance.model_dump(mode="json")

  for pf in pending_facts:
    session.add(
      Fact(
        id=generate_prefixed_ulid("fact"),
        element_id=pf["element_id"],
        value=pf["value"],
        fact_type="Numeric",
        period_start=pf["period_start"],
        period_end=body.period_end,
        period_type=pf["period_type"],
        unit=pf["unit"],
        entity_id=entity_id,
        structure_id=body.structure_id,
        fact_set_id=standing.id,
      )
    )
  session.flush()

  return AssertMetricsResponse(
    structure_id=body.structure_id,
    entity_id=entity_id,
    period_end=body.period_end,
    fact_set_id=standing.id,
    asserted=asserted,
    replaced=replaced,
  )


__all__ = ["cmd_assert_metrics", "cmd_compute_metrics"]
