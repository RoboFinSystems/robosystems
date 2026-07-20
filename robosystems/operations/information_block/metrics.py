"""compute-metrics — evaluate Derive rules into a standing metric FactSet.

The metric block's compute path (Metrics M-1). ``Derive`` rules carry the
same ``$Var`` expression grammar as verification rules, but are evaluated
for a VALUE: the LHS names the metric element being computed, the RHS
operands bind to the entity's persisted report facts at the requested
``period_end``, and the result is written as a Numeric fact in a standing
``factset_type='metric'`` FactSet — one per (structure, entity,
period_end), so successive runs accumulate the time series and re-running
a period replaces its values.

Soft-fail per metric: a missing operand fact (InterestExpense for a
debt-free entity), an unresolvable qname, or an undefined ratio
(division by zero) skips that metric with a reason — one broken metric
never aborts the run.

Operands bind against ``factset_type='report'`` facts only, so a metric
referencing another metric simply skips in M-1 — composition (DuPont)
is the M-2 DAG feature.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from sqlalchemy import or_, select, text
from sqlalchemy.orm import Session

from robosystems.models.api.fact_provenance import DerivedProvenance
from robosystems.models.api.information_block import (
  ComputedMetricLite,
  ComputeMetricsRequest,
  ComputeMetricsResponse,
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
from robosystems.operations.information_block.registry import METRIC_BLOCK_TYPE
from robosystems.operations.information_block.rules.expressions import (
  InvalidRuleExpression,
  evaluate_derivation,
  lhs_variable_names,
  parse_arithmetic_expression,
)
from robosystems.operations.roboledger.fact_set import create_fact_set
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
) -> _BoundOperand | None:
  """Most recent persisted report fact for one operand at ``period_end``.

  Joins Fact → FactSet on ``factset_type='report'`` (the persisted
  statement facts, including calc-DAG subtotals) scoped to the entity.
  ``period_end`` must match exactly — instant balances as of that date,
  durations ending on it. Newest report wins when several cover the
  period; among same-set candidates the longest window wins (FY over Q4
  when both end on the date).
  """
  stmt = (
    select(Fact.value, Fact.period_start)
    .join(FactSet, Fact.fact_set_id == FactSet.id)
    .where(
      FactSet.factset_type == "report",
      Fact.entity_id == entity_id,
      Fact.element_id == element_id,
      Fact.period_end == period_end,
      Fact.fact_scope == "in_scope",
      Fact.value.is_not(None),
    )
    .order_by(
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

  computed: list[ComputedMetricLite] = []
  skipped: list[SkippedMetricLite] = []
  pending_facts: list[dict] = []

  qname_cache: dict[str, Element | None] = {}

  def _element_by_qname(qname: str) -> Element | None:
    if qname not in qname_cache:
      qname_cache[qname] = session.execute(
        select(Element).where(Element.qname == qname).limit(1)
      ).scalar_one_or_none()
    return qname_cache[qname]

  for rule in rules:
    target = (
      session.get(Element, rule.target_element_id) if rule.target_element_id else None
    )
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

    expression = rule.rule_expression if isinstance(rule.rule_expression, str) else ""
    try:
      parsed = parse_arithmetic_expression(expression, names)
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
      )
      if bound is None:
        missing.append(qname or name)
        continue
      values[name] = bound.value
      if bound.period_start is not None and duration_start is None:
        duration_start = bound.period_start

    if missing:
      _skip("no report fact bound for operand(s)", missing=missing)
      continue

    try:
      value = evaluate_derivation(parsed, values)
    except InvalidRuleExpression as exc:
      _skip(f"evaluation error: {exc}")
      continue

    unit = "USD" if target.is_monetary else "pure"
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
      )
    )

  provenance = DerivedProvenance(computation="compute-metrics", source_fact_ids=[])

  standing = session.execute(
    select(FactSet)
    .where(
      FactSet.structure_id == body.structure_id,
      FactSet.factset_type == "metric",
      FactSet.entity_id == entity_id,
      FactSet.period_end == body.period_end,
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


__all__ = ["cmd_compute_metrics"]
