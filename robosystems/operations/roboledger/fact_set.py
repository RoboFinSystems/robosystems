"""Blessed construction path for FactSet rows.

Every FactSet must be stamped with a typed ``FactProvenance`` descriptor
at emission (the auditability spine — see ``models/api/fact_provenance``).
``create_fact_set`` is the single place that validates the descriptor and
writes it to the first-class ``fact_sets.provenance`` column; the
``before_insert`` backstop on the model fails any insert that bypasses it.

The three producers (report pivot, schedule, cross-graph share) route
through here; future producers (metrics, rev-rec, disclosures) inherit
the contract by doing the same.
"""

from __future__ import annotations

from datetime import date

from pydantic import TypeAdapter
from sqlalchemy.orm import Session

from robosystems.models.api.fact_provenance import FactProvenance
from robosystems.models.extensions.roboledger.fact_set import FactSet

_PROVENANCE = TypeAdapter(FactProvenance)


def create_fact_set(
  session: Session,
  *,
  period_end: date,
  factset_type: str,
  entity_id: str,
  provenance: FactProvenance,
  created_by: str,
  structure_id: str | None = None,
  period_start: date | None = None,
  report_id: str | None = None,
  metadata: dict | None = None,
  id: str | None = None,
) -> FactSet:
  """Construct + ``session.add`` a stamped FactSet and return it.

  ``provenance`` is validated through the discriminated union and stored
  as JSON on the dedicated ``provenance`` column. Pass an arm instance
  (``PivotProvenance(...)`` etc.); a raw dict is accepted and validated.
  """
  validated = _PROVENANCE.validate_python(provenance)
  fact_set = FactSet(
    structure_id=structure_id,
    period_start=period_start,
    period_end=period_end,
    factset_type=factset_type,
    entity_id=entity_id,
    report_id=report_id,
    provenance=validated.model_dump(mode="json"),
  )
  if id is not None:
    fact_set.id = id
  if metadata is not None:
    fact_set.metadata_ = metadata
  fact_set.created_by = created_by
  session.add(fact_set)
  return fact_set
