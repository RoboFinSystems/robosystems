"""Command wrapper for the evaluate-rules operation."""

from __future__ import annotations

from sqlalchemy.orm import Session

from robosystems.models.api.information_block import (
  EvaluateRulesRequest,
  EvaluateRulesResponse,
  VerificationResultLite,
)
from robosystems.operations.information_block.envelope import (
  verification_result_to_lite,
)
from robosystems.operations.information_block.rules.engine import (
  evaluate_rules_for_structure,
)


def cmd_evaluate_rules(
  session: Session,
  body: EvaluateRulesRequest,
  created_by: str,
) -> EvaluateRulesResponse:
  """Run the rule engine for a structure and return the evaluation summary.

  Calls :func:`evaluate_rules_for_structure`, projects results to
  :class:`VerificationResultLite`, and builds a ``summary`` dict keyed
  by status (``pass``, ``fail``, ``error``, ``skipped``).
  """
  rows = evaluate_rules_for_structure(
    session,
    body.structure_id,
    fact_set_id=body.fact_set_id,
    period_start=body.period_start,
    period_end=body.period_end,
    created_by=created_by,
  )

  results: list[VerificationResultLite] = [verification_result_to_lite(r) for r in rows]

  summary: dict[str, int] = {"pass": 0, "fail": 0, "error": 0, "skipped": 0}
  for r in results:
    summary[r.status] = summary.get(r.status, 0) + 1

  return EvaluateRulesResponse(
    structure_id=body.structure_id,
    results=results,
    summary=summary,
  )


__all__ = ["cmd_evaluate_rules"]
