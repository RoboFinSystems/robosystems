"""Cross-cutting guard checks shared by multiple command modules.

These guards run inside an open extensions session and raise domain
exceptions that callers map to HTTP status codes. They are NOT
OperationSpec `pre_validate` hooks (which run before the session
opens) — they require DB access.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from robosystems.operations.locking import acquire_shared_period_fence

_LIBRARY_SEEDER = "library-seeder"


def rule_summary(results: list) -> dict[str, int] | None:
  """Tally verification results by status. Returns None when no rules exist."""
  if not results:
    return None
  tally: dict[str, int] = {"pass": 0, "fail": 0, "error": 0, "skipped": 0}
  for r in results:
    tally[r.status] = tally.get(r.status, 0) + 1
  return tally


class LibraryImmutableError(PermissionError):
  """Raised when a mutation targets a library-seeded row in a tenant schema.

  Library-origin rows are distinguished by ``created_by='library-seeder'``
  (applied by ``robosystems/operations/taxonomy_block/library_creator.py``
  during the canonical JSON-LD load). Tenant schemas carry a copy of those rows
  for search-path shadowing; they are read-only from tenant-scoped command
  paths. Tenant authoring happens via tenant-origin rows that coexist with
  the library copy, distinguished by their own ``created_by`` audit value.

  This raises before PostgreSQL's ``raise_library_immutable`` trigger
  would, giving callers a clean domain exception to map to HTTP 403
  rather than a bare ``ProgrammingError``.
  """

  def __init__(self, kind: str, identifier: str) -> None:
    super().__init__(
      f"Cannot mutate library-seeded {kind} {identifier!r}: "
      f"library rows are read-only in tenant schemas. "
      f"Author tenant-origin content instead."
    )
    self.kind = kind
    self.identifier = identifier


def assert_not_library_origin(row: Any) -> None:
  """Raise :class:`LibraryImmutableError` if ``row`` was library-seeded.

  Accepts any SQLAlchemy row (or any object with ``created_by`` and
  ``id`` attributes). No-op if ``created_by`` is absent or not the
  library-seeder literal.
  """
  if row is None:
    return
  if getattr(row, "created_by", None) == _LIBRARY_SEEDER:
    kind = type(row).__name__.lower()
    identifier = str(getattr(row, "qname", None) or getattr(row, "id", "?"))
    raise LibraryImmutableError(kind, identifier)


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


_PERIOD_COVERING_DATE = """
  SELECT graph_id, name, status
  FROM fiscal_periods
  WHERE start_date <= :posting_date AND end_date >= :posting_date
  LIMIT 1
"""


def _period_covering(session: Session, posting_date: date):
  return session.execute(
    text(_PERIOD_COVERING_DATE),
    {"posting_date": posting_date},
  ).fetchone()


def assert_period_not_closed(session: Session, *posting_dates: date) -> None:
  """Raise `ClosedPeriodError` if any fiscal period covering the given
  dates is closed.

  Takes the shared period fence on each distinct period (sorted, so two
  writers that touch overlapping months cannot deadlock) and re-reads
  status under that fence. Close holds the exclusive side of the same
  fence across its QuickBooks publish and the database close, so a
  writer cannot observe `open` and then commit after the close has
  finished.

  No-op if no `FiscalPeriod` row covers a date (e.g., fresh tenant
  without periods seeded). The shared fence is transaction-scoped and
  released when the caller's session commits or rolls back.
  """
  dates = [d for d in posting_dates if d is not None]
  if not dates:
    return

  found: list[tuple[tuple[str, str], date]] = []
  seen: set[tuple[str, str]] = set()
  for posting_date in dates:
    row = _period_covering(session, posting_date)
    if row is None:
      continue
    key = (row.graph_id, row.name)
    if key in seen:
      continue
    seen.add(key)
    found.append((key, posting_date))

  found.sort(key=lambda item: item[0])
  for (graph_id, name), posting_date in found:
    acquire_shared_period_fence(
      session,
      graph_id,
      name,
      detail=(
        f"Period {name} is being closed or reopened by another process. "
        "Retry in a moment."
      ),
    )
    row = _period_covering(session, posting_date)
    if row is not None and row.status == "closed":
      raise ClosedPeriodError(row.name, posting_date)
