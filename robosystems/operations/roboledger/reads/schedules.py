"""Schedule read operations.

Thin read-side wrappers over `ScheduleService` that assemble the
wire-facing Pydantic responses. Both the REST router and the GraphQL
resolvers call these so the response shape is defined once.
"""

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
    # The stored receipt is validated on the way out rather than trusted:
    # rows written by an older shape (or hand-edited) surface as no receipt
    # instead of failing the whole close-status read.
    close_receipt=_parse_receipt(status.close_receipt),
  )


def _parse_receipt(raw: dict | None) -> CloseReceiptResponse | None:
  """Project a stored close receipt onto its typed response model.

  Returns None for a missing or unreadable receipt. `get-period-close-status`
  is the read an operator runs when a close's response was lost, so it must
  not be the thing that breaks on a receipt it cannot parse.
  """
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
