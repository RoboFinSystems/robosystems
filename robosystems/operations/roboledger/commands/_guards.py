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
  (applied by ``robosystems/taxonomy/writers/library_writer.py`` during
  the canonical JSON-LD load). Tenant schemas carry a copy of those rows
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


def assert_tenant_taxonomy(session: Session, taxonomy_id: str | None) -> None:
  """Raise :class:`LibraryImmutableError` if the taxonomy is library-seeded.

  Tenants can author elements / structures / associations into taxonomies
  they own, but not into library taxonomies. This guard gates writes at
  the ``create_*`` boundary where the row doesn't exist yet (so
  :func:`assert_not_library_origin` doesn't apply).

  No-op if ``taxonomy_id`` is None (some commands accept an optional
  taxonomy reference) or if the taxonomy doesn't exist (caller handles
  that separately — we don't preempt not-found errors here).
  """
  if taxonomy_id is None:
    return
  row = session.execute(
    text("SELECT created_by FROM taxonomies WHERE id = :id LIMIT 1"),
    {"id": taxonomy_id},
  ).fetchone()
  if row is not None and row.created_by == _LIBRARY_SEEDER:
    raise LibraryImmutableError("taxonomy", taxonomy_id)


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
