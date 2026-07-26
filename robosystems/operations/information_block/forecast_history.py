"""Back-solve a scenario's levers from what actually happened.

The lever grid answers *"what am I assuming?"* for the months ahead.
Behind the seam that question has a factual answer instead: the rate the
book actually ran at. This module inverts the ``rs-driver`` Derive rules
against the closed months' canonical statement sets so the Assumptions
block reads as ONE continuous series across the seam — realized rates on
the left, asserted rates on the right.

It also closes a smaller correctness gap. A scenario whose horizon
opened before today still holds asserted lever values for months that
have since closed; rendering them as-is presents an assumption as though
it described history. Those months are facts now, so the realized value
supersedes the assertion — the same actual-beats-forecast rule
:func:`envelope.load_statement_fact_set_series` applies to statement
columns (the moving seam).

**Inversion is generic, not per-rule.** Every seeded driver rule is
affine in its lever operand::

    Revenues               = Revenues[t-1] * (1 + RevenueGrowthRate)
    CostOfRevenue          = Revenues * CostOfRevenueRate
    ReceivablesNetCurrent  = (Revenues / 30) * DaysSalesOutstanding
    AccountsPayableCurrent = (CostOfRevenue / 30) * DaysPayableOutstanding

so evaluating the RHS at ``lever = 0`` and ``lever = 1`` recovers the
intercept and the slope, and the lever falls out of the month's actual
target value::

    target = a + b*lever    ⇒    lever = (target_actual - a) / b

Nothing is hardcoded to the four catalog drivers: a fifth lever whose
rule is affine in it back-solves for free, and a rule that isn't (slope
0) blanks its historical cells rather than guessing. Every failure mode
— unbound operand, missing prior month, no actual target, ``avg()`` in
the expression — blanks that one cell and leaves the rest of the grid
standing.

Read-only and deterministic: nothing here writes facts. The realized
rate is a *rendering* of actuals already stamped by close, not a new
fact-producing path.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlalchemy import select

from robosystems.models.api.information_block import ForecastMechanics
from robosystems.models.extensions import Element, Rule, Structure, Taxonomy
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.roboledger.fact_set import FactSet
from robosystems.operations.information_block.envelope import (
  load_statement_fact_set_series,
)
from robosystems.operations.information_block.rules.expressions import (
  InvalidRuleExpression,
  desugar_aggregates,
  desugar_priors,
  evaluate_derivation,
  lhs_variable_names,
  parse_arithmetic_expression,
)
from robosystems.operations.roboledger.fiscal_calendar.periods import (
  period_from_date,
)

if TYPE_CHECKING:
  from sqlalchemy.orm import Session

DRIVER_STANDARD = "rs-driver"
DRIVER_PREFIX = "rs-driver:"

# Statement families the driver operands live in. Cash flow carries no
# driver anchors today (the CF is derived, never driven).
_ACTUAL_BLOCK_TYPES = ("income_statement", "balance_sheet")

# A lever whose rule barely moves its target can't be recovered from the
# target: the division blows the value up. Blank the cell instead.
_MIN_SLOPE = 1e-9


def driver_rules(session: Session) -> list[Rule]:
  """Every Derive rule seeded by the rs-driver catalog (tenant copy)."""
  taxonomy_ids = (
    session.execute(select(Taxonomy.id).where(Taxonomy.standard == DRIVER_STANDARD))
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


def numeric_facts(session: Session, fact_set_id: str) -> list[Fact]:
  """In-scope valued facts of one FactSet."""
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


def newest_actual_structure_id(session: Session, block_type: str) -> str | None:
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


def _lookup_element(session: Session, qname: str) -> Element | None:
  """One element by qname — the operand/target resolution seam."""
  return session.execute(
    select(Element).where(Element.qname == qname).limit(1)
  ).scalar_one_or_none()


@dataclass
class LeverHistory:
  """Realized lever + line values for the closed months.

  ``months`` is every month the actual statement series covers, ascending
  ``YYYY-MM`` — the same column identities the Plan grid's statement
  sections carry, so the lever rows land in register with them.
  """

  months: list[str] = field(default_factory=list)
  #: rs-driver qname → month → back-solved rate
  lever_values: dict[str, dict[str, float]] = field(default_factory=dict)
  #: asserted line element id → month → the line's actual value
  line_values: dict[str, dict[str, float]] = field(default_factory=dict)


@dataclass
class _SolvableRule:
  """A driver rule pre-parsed for repeated back-solving."""

  lever_qname: str
  target_element_id: str
  lever_name: str
  parsed: object
  #: operand variable name → element id (same-month binding)
  operand_element_ids: dict[str, str]
  #: synthesized ``__prior_*`` name → element id (previous-month binding)
  prior_element_ids: dict[str, str]


def back_solve_lever_history(
  session: Session, mechanics: ForecastMechanics
) -> LeverHistory:
  """Realized lever values for every closed month, by rule inversion.

  Returns an empty history when the tenant has no actual statement sets,
  no rs-driver rules, or nothing to solve — callers render the asserted
  horizon alone in that case, exactly as before.
  """
  lever_qnames = {lever.qname for lever in mechanics.levers}
  # Line assertions AND line-growth targets: both grid families render
  # the line's own actuals behind the seam (growth rows derive the
  # realized rate from consecutive actuals at render time).
  line_element_ids = {
    assertion.element_id for assertion in mechanics.line_assertions
  } | {entry.element_id for entry in mechanics.line_growth}
  if not lever_qnames and not line_element_ids:
    return LeverHistory()

  element_cache: dict[str, Element | None] = {}

  def element_by_qname(qname: str) -> Element | None:
    if qname not in element_cache:
      element_cache[qname] = _lookup_element(session, qname)
    return element_cache[qname]

  solvable = _solvable_rules(session, lever_qnames, element_by_qname)
  if not solvable and not line_element_ids:
    return LeverHistory()

  # Only the elements the grid actually needs — the whole point of a
  # targeted read is that the lever grid stays cheap next to the
  # statement series envelopes it renders beside.
  needed_element_ids = set(line_element_ids)
  for rule in solvable:
    needed_element_ids.add(rule.target_element_id)
    needed_element_ids.update(rule.operand_element_ids.values())
    needed_element_ids.update(rule.prior_element_ids.values())

  months, actuals = _actual_monthly_values(session, needed_element_ids)
  if not months:
    return LeverHistory()

  history = LeverHistory(months=months)
  for index, month in enumerate(months):
    current = actuals.get(month, {})
    prior = actuals.get(months[index - 1], {}) if index > 0 else {}
    for rule in solvable:
      value = _back_solve(rule, current, prior)
      if value is not None:
        history.lever_values.setdefault(rule.lever_qname, {})[month] = value
    # An asserted line's realized counterpart is simply the line's own
    # actual — no inversion needed, it IS the statement value.
    for element_id in line_element_ids:
      actual = current.get(element_id)
      if actual is not None:
        history.line_values.setdefault(element_id, {})[month] = actual
  return history


def _solvable_rules(
  session: Session,
  lever_qnames: set[str],
  element_by_qname,
) -> list[_SolvableRule]:
  """Parse each driver rule once, keeping those we can invert."""
  solvable: list[_SolvableRule] = []
  for rule in driver_rules(session):
    variables = rule.rule_variables or []
    qname_by_name = {
      v["variable_name"]: v["variable_qname"]
      for v in variables
      if isinstance(v, dict)
      and isinstance(v.get("variable_name"), str)
      and isinstance(v.get("variable_qname"), str)
    }
    if not qname_by_name:
      continue
    # The lever this rule is driven by — and only levers this scenario
    # actually asserts (an unasserted catalog rule has no row to fill).
    lever_names = [
      name
      for name, qname in qname_by_name.items()
      if qname.startswith(DRIVER_PREFIX) and qname in lever_qnames
    ]
    if len(lever_names) != 1:
      continue
    lever_name = lever_names[0]
    target = (
      session.get(Element, rule.target_element_id) if rule.target_element_id else None
    )
    if target is None:
      continue

    raw = rule.rule_expression if isinstance(rule.rule_expression, str) else ""
    expr, prior_operands = desugar_priors(raw)
    expr, avg_operands = desugar_aggregates(expr)
    if avg_operands:
      # avg() needs a begin/end pair this walk doesn't track — the same
      # honest skip compute-forecast takes.
      continue
    operand_names = list(qname_by_name)
    try:
      parsed = parse_arithmetic_expression(
        expr, operand_names + list(prior_operands) + list(avg_operands)
      )
      lhs_names = lhs_variable_names(parsed)
    except InvalidRuleExpression:
      continue
    if len(lhs_names) != 1:
      continue

    operand_element_ids: dict[str, str] = {}
    unbound = False
    for name, qname in qname_by_name.items():
      if name in (lhs_names[0], lever_name):
        continue
      element = element_by_qname(qname)
      if element is None:
        unbound = True
        break
      operand_element_ids[name] = element.id
    if unbound:
      continue

    prior_element_ids: dict[str, str] = {}
    for synth_name, base_name in prior_operands.items():
      qname = qname_by_name.get(base_name)
      element = element_by_qname(qname) if qname else None
      if element is None:
        unbound = True
        break
      prior_element_ids[synth_name] = element.id
    if unbound:
      continue

    solvable.append(
      _SolvableRule(
        lever_qname=qname_by_name[lever_name],
        target_element_id=target.id,
        lever_name=lever_name,
        parsed=parsed,
        operand_element_ids=operand_element_ids,
        prior_element_ids=prior_element_ids,
      )
    )
  return solvable


def _back_solve(
  rule: _SolvableRule,
  current: dict[str, float],
  prior: dict[str, float],
) -> float | None:
  """Invert one rule for one month, or None when it can't be bound."""
  target_actual = current.get(rule.target_element_id)
  if target_actual is None:
    return None

  values: dict[str, float] = {}
  for name, element_id in rule.operand_element_ids.items():
    operand = current.get(element_id)
    if operand is None:
      return None
    values[name] = operand
  for name, element_id in rule.prior_element_ids.items():
    operand = prior.get(element_id)
    if operand is None:
      return None
    values[name] = operand

  try:
    values[rule.lever_name] = 0.0
    intercept = evaluate_derivation(rule.parsed, values)
    values[rule.lever_name] = 1.0
    slope = evaluate_derivation(rule.parsed, values) - intercept
  except InvalidRuleExpression:
    return None
  if abs(slope) < _MIN_SLOPE:
    return None
  return (target_actual - intercept) / slope


def _actual_monthly_values(
  session: Session, element_ids: set[str]
) -> tuple[list[str], dict[str, dict[str, float]]]:
  """Actual values per closed month for exactly the elements asked for.

  Months come from the FactSet series, not from the facts — a month with
  no binding value is still a column (it renders blank), which is how a
  gap in the operand data reads honestly rather than silently shifting
  the grid.
  """
  months: set[str] = set()
  by_month: dict[str, dict[str, float]] = {}
  if not element_ids:
    return [], by_month

  for block_type in _ACTUAL_BLOCK_TYPES:
    structure_id = newest_actual_structure_id(session, block_type)
    if structure_id is None:
      continue
    # scenario_id=None → actuals only. The same loader the statement
    # series columns come from, so the months line up by construction.
    series = load_statement_fact_set_series(session, structure_id, None)
    if not series:
      continue
    month_by_set = {
      fact_set.id: period_from_date(fact_set.period_end) for fact_set in series
    }
    months.update(month_by_set.values())
    rows = (
      session.execute(
        select(Fact).where(
          Fact.fact_set_id.in_(list(month_by_set)),
          Fact.element_id.in_(list(element_ids)),
          Fact.fact_scope == "in_scope",
          Fact.value.is_not(None),
        )
      )
      .scalars()
      .all()
    )
    for fact in rows:
      month = month_by_set[fact.fact_set_id]
      by_month.setdefault(month, {})[fact.element_id] = float(fact.value)

  return sorted(months), by_month
