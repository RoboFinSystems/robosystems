"""The one construction path for FactSet rows: ``create_fact_set`` validates the
typed ``FactProvenance`` and writes it to ``fact_sets.provenance``; a
``before_insert`` backstop fails any insert that bypasses it.
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
  scenario_id: str | None = None,
  metadata: dict | None = None,
  id: str | None = None,
) -> FactSet:
  """Construct + ``session.add`` a stamped FactSet and return it.

  ``provenance`` may be an arm instance or a raw dict. ``scenario_id`` NULL
  means actuals; otherwise it is the owning forecast Structure.
  """
  validated = _PROVENANCE.validate_python(provenance)
  fact_set = FactSet(
    structure_id=structure_id,
    period_start=period_start,
    period_end=period_end,
    factset_type=factset_type,
    entity_id=entity_id,
    report_id=report_id,
    scenario_id=scenario_id,
    provenance=validated.model_dump(mode="json"),
  )
  if id is not None:
    fact_set.id = id
  if metadata is not None:
    fact_set.metadata_ = metadata
  fact_set.created_by = created_by
  session.add(fact_set)
  return fact_set
