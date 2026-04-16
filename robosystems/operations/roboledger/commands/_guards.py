"""Cross-cutting guard checks shared by multiple command modules.

These guards run inside an open extensions session and raise domain
exceptions that callers map to HTTP status codes. They are NOT
OperationSpec `pre_validate` hooks (which run before the session
opens) — they require DB access.
"""

from __future__ import annotations

from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session


class ClosedPeriodError(ValueError):
  """Raised when a write targets a posting_date inside a closed fiscal period.

  The caller should map this to HTTP 422 with the detail message
  explaining which period is closed and how to proceed (reopen first).
  """

  def __init__(self, period_name: str, posting_date: date) -> None:
    super().__init__(
      f"Cannot write to closed period {period_name!r} "
      f"(posting_date={posting_date}). "
      f"Reopen the period first if an adjustment is needed."
    )
    self.period_name = period_name
    self.posting_date = posting_date


def assert_period_not_closed(session: Session, posting_date: date) -> None:
  """Raise `ClosedPeriodError` if the fiscal period containing
  `posting_date` is closed.

  No-op if no `FiscalPeriod` row covers the date (e.g., fresh tenant
  without periods seeded). This matches `ScheduleService._assert_period_not_closed`
  but is a standalone function so journal entry commands and any
  future write ops can use it without coupling to the schedule layer.
  """
  row = session.execute(
    text("""
      SELECT name, status
      FROM fiscal_periods
      WHERE start_date <= :posting_date AND end_date >= :posting_date
      LIMIT 1
    """),
    {"posting_date": posting_date},
  ).fetchone()

  if row is not None and row.status == "closed":
    raise ClosedPeriodError(row.name, posting_date)
