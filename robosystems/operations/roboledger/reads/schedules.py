"""Schedule read operations.

Thin read-side wrappers over `ScheduleService` that assemble the
wire-facing Pydantic responses. Both the REST router and the GraphQL
resolvers call these so the response shape is defined once.
"""

from __future__ import annotations

from datetime import date

from sqlalchemy.orm import Session

from robosystems.models.api.extensions.schedules import (
  PeriodCloseItemResponse,
  PeriodCloseStatusResponse,
)


def get_period_close_status(
  session: Session,
  service,
  period_start: date,
  period_end: date,
) -> PeriodCloseStatusResponse:
  """Return close status for all schedules in a fiscal period."""
  status = service.get_period_close_status(session, period_start, period_end)
  return PeriodCloseStatusResponse(
    fiscal_period_start=status.fiscal_period_start,
    fiscal_period_end=status.fiscal_period_end,
    period_status=status.period_status,
    schedules=[
      PeriodCloseItemResponse(
        structure_id=s.structure_id,
        structure_name=s.structure_name,
        amount=s.amount,
        status=s.status,
        entry_id=s.entry_id,
        reversal_entry_id=s.reversal_entry_id,
        reversal_status=s.reversal_status,
      )
      for s in status.schedules
    ],
    total_draft=status.total_draft,
    total_posted=status.total_posted,
  )
