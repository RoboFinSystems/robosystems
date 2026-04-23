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
3. Resolve ``element_id`` + period window → fact value via the facts
   table (``fact_scope = 'in_scope'``, most recent ``period_end`` first).
   Structure-scoped facts win. If no structure fact exists, facts from
   the latest matching statement/report are accepted via one pinned
   ``report_id`` because report creation still writes structure-agnostic
   facts until the FactSet expand pass stamps ``structure_id``/
   ``fact_set_id``. Schedules do not use that fallback; their own
   generated facts are the source of truth. None on miss.

One query per variable (N+1 is fine for 3-5 variables per rule).
"""

from __future__ import annotations

from datetime import date

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from robosystems.models.extensions import (
  Association,
  Element,
  Report,
  Rule,
  Structure,
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
  fact_set_id: str | None = None,
  allow_report_fallback: bool = True,
  fallback_report_id: str | None = None,
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

    base_stmt = (
      select(Fact.value)
      .where(
        Fact.element_id == element_id,
        Fact.fact_scope == "in_scope",
      )
      .order_by(Fact.period_end.desc(), Fact.created_at.desc())
      .limit(1)
    )
    if period_end is not None:
      base_stmt = base_stmt.where(Fact.period_end <= period_end)
    if period_start is not None:
      base_stmt = base_stmt.where(Fact.period_start >= period_start)

    if fact_set_id is not None:
      stmt = base_stmt.where(Fact.fact_set_id == fact_set_id)
      value: float | None = session.execute(stmt).scalar()
      bindings[name] = value
      continue

    # Structure-scoped facts are authoritative for declarative blocks
    # such as schedules. Statement/report facts are currently
    # structure-agnostic (report_id set, structure_id null), so fall
    # back to those only when the block has no direct fact for the
    # requested variable.
    stmt = base_stmt.where(Fact.structure_id == structure_id)
    value = session.execute(stmt).scalar()
    if value is None and allow_report_fallback and fallback_report_id is not None:
      report_stmt = base_stmt.where(
        Fact.structure_id.is_(None),
        Fact.report_id == fallback_report_id,
      )
      value = session.execute(report_stmt).scalar()
    bindings[name] = value

  return bindings


def _latest_report_id_for_fallback(
  session: Session,
  element_ids: set[str],
  period_start: date | None,
  period_end: date | None,
) -> str | None:
  """Pick one report for all structure-agnostic fact fallback bindings."""
  if not element_ids:
    return None

  stmt = (
    select(Report.id)
    .join(Fact, Fact.report_id == Report.id)
    .where(
      Fact.element_id.in_(element_ids),
      Fact.fact_scope == "in_scope",
      Fact.structure_id.is_(None),
      Fact.report_id.is_not(None),
    )
    .order_by(Report.created_at.desc(), Report.id.desc())
    .limit(1)
  )
  if period_end is not None:
    stmt = stmt.where(Fact.period_end <= period_end)
  if period_start is not None:
    stmt = stmt.where(Fact.period_start >= period_start)
  return session.execute(stmt).scalar()


def _bind_sum_variables(
  session: Session,
  rule: Rule,
  structure_id: str,
) -> dict[str, float | None]:
  """Aggregate SUM of duration facts per variable — used exclusively by SumEquals rules."""
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
    row = session.execute(
      text(
        "SELECT ROUND(SUM(value)::numeric, 2) AS total "
        "FROM facts "
        "WHERE element_id = :eid AND structure_id = :sid AND period_type = 'duration'"
      ),
      {"eid": element_id, "sid": structure_id},
    ).fetchone()
    bindings[name] = float(row.total) if row and row.total is not None else None
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
  structure = session.get(Structure, structure_id)
  if structure is None:
    raise ValueError(f"Structure not found: {structure_id}")

  associations = (
    session.execute(select(Association).where(Association.structure_id == structure_id))
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
  allow_report_fallback = structure.structure_type != "schedule"
  fallback_report_id = (
    _latest_report_id_for_fallback(session, element_ids, period_start, period_end)
    if allow_report_fallback and fact_set_id is None
    else None
  )

  results: list[VerificationResult] = []
  for rule_lite in rule_lites:
    rule = rule_map.get(rule_lite.id)
    if rule is None:
      continue
    try:
      if rule.rule_pattern == "SumEquals":
        bindings = _bind_sum_variables(session, rule, structure_id)
      else:
        bindings = _bind_variables(
          session,
          rule,
          structure_id,
          period_start,
          period_end,
          fact_set_id=fact_set_id,
          allow_report_fallback=allow_report_fallback,
          fallback_report_id=fallback_report_id,
        )
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
