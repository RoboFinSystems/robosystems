"""Reconciliation reads: every block's standing for a period."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.reconciliations import (
  ReconciliationListResponse,
  ReconciliationRow,
  ReconciliationStatus,
  ReconciliationSummary,
)
from robosystems.models.api.information_block import ReconciliationMechanics
from robosystems.models.extensions import Element, Structure, VerificationResult
from robosystems.models.extensions.roboledger import Fact, FactSet
from robosystems.operations.information_block.reconciliation import (
  RECONCILIATION_BLOCK_TYPE,
  RECONCILIATION_FACTSET_TYPE,
)
from robosystems.operations.roboledger.fiscal_calendar import period_date_range

_QNAME_PREFIX = "rs-rec:"


def _status(result: VerificationResult | None) -> ReconciliationStatus:
  if result is None:
    return "not_started"
  return "reconciled" if result.status == "pass" else "unreconciled"


def _observed_at(provenance: dict | None) -> datetime | None:
  value = (provenance or {}).get("observed_at")
  return datetime.fromisoformat(value) if value else None


def list_reconciliations(session: Session, period: str) -> ReconciliationListResponse:
  """Each active reconciliation block's standing at the period's last day.

  A block with no comparison for the period is ``not_started``. Raises
  ``ValueError`` on a malformed period.
  """
  _, as_of = period_date_range(period)
  structures = (
    session.execute(
      select(Structure)
      .where(
        Structure.block_type == RECONCILIATION_BLOCK_TYPE,
        Structure.is_active.is_(True),
      )
      .order_by(Structure.created_at.asc(), Structure.id.asc())
    )
    .scalars()
    .all()
  )
  structure_ids = [str(s.id) for s in structures]

  fact_sets: dict[str, FactSet] = {}
  if structure_ids:
    for fact_set in session.execute(
      select(FactSet)
      .where(
        FactSet.structure_id.in_(structure_ids),
        FactSet.factset_type == RECONCILIATION_FACTSET_TYPE,
        FactSet.period_end == as_of,
        FactSet.scenario_id.is_(None),
      )
      .order_by(FactSet.created_at.desc())
    ).scalars():
      fact_sets.setdefault(str(fact_set.structure_id), fact_set)
  fact_set_ids = [str(fs.id) for fs in fact_sets.values()]

  values: dict[tuple[str, str], float] = {}
  results: dict[str, VerificationResult] = {}
  if fact_set_ids:
    for fact_set_id, qname, value in session.execute(
      select(Fact.fact_set_id, Element.qname, Fact.value)
      .join(Element, Element.id == Fact.element_id)
      .where(Fact.fact_set_id.in_(fact_set_ids))
    ):
      values[(str(fact_set_id), str(qname).removeprefix(_QNAME_PREFIX))] = value
    for result in session.execute(
      select(VerificationResult)
      .where(VerificationResult.fact_set_id.in_(fact_set_ids))
      .order_by(VerificationResult.evaluated_at.desc())
    ).scalars():
      results.setdefault(str(result.fact_set_id), result)

  def _count(fact_set_id: str, name: str) -> int | None:
    value = values.get((fact_set_id, name))
    return int(value) if value is not None else None

  summaries: list[ReconciliationSummary] = []
  for structure in structures:
    mechanics = ReconciliationMechanics.model_validate(structure.artifact_mechanics)
    fact_set = fact_sets.get(str(structure.id))
    fact_set_id = str(fact_set.id) if fact_set is not None else ""
    provenance = fact_set.provenance if fact_set is not None else None
    metadata = (fact_set.metadata_ or {}) if fact_set is not None else {}
    summaries.append(
      ReconciliationSummary(
        structure_id=str(structure.id),
        name=structure.name,
        scope=mechanics.scope,
        method=mechanics.method,
        element_id=mechanics.element_id,
        required_for_close=mechanics.required_for_close,
        materiality=mechanics.materiality,
        period=period,
        as_of=as_of,
        status=_status(results.get(fact_set_id)),
        unreconciled_difference=values.get((fact_set_id, "UnreconciledDifference")),
        accounts_compared=_count(fact_set_id, "AccountsCompared"),
        accounts_different=_count(fact_set_id, "AccountsDifferent"),
        source=(provenance or {}).get("source"),
        compared_at=_observed_at(provenance),
        fact_set_id=fact_set_id or None,
        differences=[
          ReconciliationRow.model_validate(row)
          for row in metadata.get("differences") or []
        ],
      )
    )

  return ReconciliationListResponse(
    period=period, as_of=as_of, reconciliations=summaries
  )


_CLOSEABLE_STATUSES = frozenset({"reconciled", "reviewed"})


def unreconciled_for_close(
  session: Session, period: str
) -> list[ReconciliationSummary]:
  """The reconciliations a close of ``period`` waits on that are not
  reconciled for it, including any never compared."""
  return [
    rec
    for rec in list_reconciliations(session, period).reconciliations
    if rec.required_for_close and rec.status not in _CLOSEABLE_STATUSES
  ]
