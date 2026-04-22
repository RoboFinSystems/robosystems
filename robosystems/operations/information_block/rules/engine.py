"""Rule evaluation engine entry point.

``evaluate_rules_for_structure`` is the single public function. It:

1. Loads all rules scoped to the structure (via the existing
   ``load_rules_for_structure`` helper so element/association-scoped
   rules are included alongside structure-scoped ones).
2. For each rule: resolves ``$Variable`` → fact value bindings by
   qname lookup, dispatches to the per-pattern evaluator, and writes
   a :class:`~robosystems.models.extensions.VerificationResult` row.
3. Returns the written rows (ids assigned after ``session.flush()``).

The engine is side-effect-free from the caller's perspective — it
``session.add()``s rows and calls ``session.flush()`` to assign ids,
but leaves ``session.commit()`` to the OperationSpec wrapper (which
commits on success per the existing pattern).

Binding semantics
-----------------
For each ``{variable_name, variable_qname}`` entry in
``rule.rule_variables``:

1. Resolve ``variable_qname`` → ``element_id`` via elements table.
2. If the element is missing → bind ``variable_name`` to ``None``
   (the pattern evaluator surfaces this as ``skipped`` or ``fail``).
3. Resolve ``element_id`` + ``structure_id`` + period window → fact
   value via the facts table (``fact_scope = 'in_scope'``, most
   recent ``period_end`` first). None on miss.

One query per variable (N+1 is fine for 3-5 variables per rule).
"""

from __future__ import annotations

from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.extensions import (
  Association,
  Element,
  Rule,
  VerificationResult,
)
from robosystems.models.extensions.roboledger import Fact
from robosystems.operations.information_block.envelope import load_rules_for_structure
from robosystems.operations.information_block.rules.evaluators import (
  EvaluationOutcome,
  evaluate_rule,
)


def _bind_variables(
  session: Session,
  rule: Rule,
  structure_id: str,
  period_start: date | None,
  period_end: date | None,
) -> dict[str, float | None]:
  """Resolve each ``$Variable`` in a rule to a fact value or ``None``."""
  bindings: dict[str, float | None] = {}
  for var in rule.rule_variables or []:
    name = var.get("variable_name", "")
    qname = var.get("variable_qname", "")
    if not name:
      continue

    element_id: str | None = session.execute(
      select(Element.id).where(Element.qname == qname).limit(1)
    ).scalar()

    if element_id is None:
      bindings[name] = None
      continue

    stmt = (
      select(Fact.value)
      .where(
        Fact.element_id == element_id,
        Fact.structure_id == structure_id,
        Fact.fact_scope == "in_scope",
      )
      .order_by(Fact.period_end.desc())
      .limit(1)
    )
    if period_end is not None:
      stmt = stmt.where(Fact.period_end <= period_end)
    if period_start is not None:
      stmt = stmt.where(Fact.period_start >= period_start)

    value: float | None = session.execute(stmt).scalar()
    bindings[name] = value

  return bindings


def evaluate_rules_for_structure(
  session: Session,
  structure_id: str,
  *,
  fact_set_id: str | None = None,
  period_start: date | None = None,
  period_end: date | None = None,
  created_by: str = "engine",
) -> list[VerificationResult]:
  """Evaluate every rule scoped to ``structure_id`` and persist results.

  Loads rules via :func:`~robosystems.operations.information_block.envelope.load_rules_for_structure`
  (which includes element and association-scoped rules for the structure's
  atoms). For each rule: binds variables → dispatches to the pattern
  evaluator → writes one :class:`VerificationResult` row.

  A binding or dispatch failure writes ``status='error'`` rather than
  propagating — one broken rule can't abort the whole evaluation run.

  ``session.flush()`` is called before returning so that row ids are
  assigned; the caller (OperationSpec wrapper) owns the commit.
  """
  associations = (
    session.execute(select(Association).where(Association.structure_id == structure_id))
    .scalars()
    .all()
  )
  element_ids = {a.from_element_id for a in associations} | {
    a.to_element_id for a in associations
  }

  rule_lites = load_rules_for_structure(
    session,
    structure_id,
    element_ids=list(element_ids),
    association_ids=[a.id for a in associations],
  )

  if not rule_lites:
    return []

  rule_ids = [r.id for r in rule_lites]
  rules = session.execute(select(Rule).where(Rule.id.in_(rule_ids))).scalars().all()
  rule_map = {r.id: r for r in rules}

  results: list[VerificationResult] = []
  for rule_lite in rule_lites:
    rule = rule_map.get(rule_lite.id)
    if rule is None:
      continue
    try:
      bindings = _bind_variables(session, rule, structure_id, period_start, period_end)
      outcome = evaluate_rule(rule, bindings)
    except Exception as exc:
      outcome = EvaluationOutcome(
        status="error",
        message=f"engine failure: {exc}",
        detail={"exception_type": type(exc).__name__},
      )
    row = VerificationResult(
      rule_id=rule.id,
      structure_id=structure_id,
      fact_set_id=fact_set_id,
      period_start=period_start,
      period_end=period_end,
      status=outcome.status,
      message=outcome.message,
      detail=outcome.detail or {},
      created_by=created_by,
    )
    session.add(row)
    results.append(row)

  session.flush()
  return results


__all__ = [
  "evaluate_rules_for_structure",
]
