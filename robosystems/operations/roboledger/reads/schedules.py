"""Schedule reads: ScheduleService results shaped into API responses."""

from __future__ import annotations

from datetime import date

from pydantic import ValidationError
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.extensions.schedules import (
  CloseReceiptResponse,
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
    close_receipt=_parse_receipt(status.close_receipt),
  )


def _parse_receipt(raw: dict | None) -> CloseReceiptResponse | None:
  """None for a missing or unreadable receipt: this read is how an operator
  recovers a lost close response, so a bad receipt must not fail it."""
  if not raw:
    return None
  try:
    return CloseReceiptResponse.model_validate(raw)
  except ValidationError:
    logger.warning(
      "Stored close receipt failed validation; reporting no receipt",
      extra={"period": raw.get("period"), "version": raw.get("version")},
    )
    return None
