"""Guard checks shared by command modules. They need an open extensions
session, unlike OperationSpec ``pre_validate`` hooks."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from robosystems.models.extensions import Element
from robosystems.operations.locking import acquire_shared_period_fence
from robosystems.operations.roboledger.commands.connections import SEVERABLE_SOURCES

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
  """A mutation targets a library-seeded row (``created_by='library-seeder'``).

  Raised ahead of the ``raise_library_immutable`` trigger for a clean 403.
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
  if row is None:
    return
  if getattr(row, "created_by", None) == _LIBRARY_SEEDER:
    kind = type(row).__name__.lower()
    identifier = str(getattr(row, "qname", None) or getattr(row, "id", "?"))
    raise LibraryImmutableError(kind, identifier)


class ClosedPeriodError(ValueError):
  """A write targets a posting_date inside a closed fiscal period."""

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
  """Raise `ClosedPeriodError` if any period covering the dates is closed.

  Takes the shared, transaction-scoped period fence on each distinct period in
  sorted order (so overlapping writers cannot deadlock) and re-reads status
  under it; close holds the exclusive side, so a writer cannot see `open` and
  commit after the close. Dates with no `FiscalPeriod` pass.
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


class InactiveAccountError(ValueError):
  """A line item names a retired (``is_active=false``) chart account, which
  keeps its history but is closed to new activity."""

  def __init__(self, accounts: list[tuple[str, str | None, str | None]]) -> None:
    self.accounts = accounts
    named = ", ".join(
      f"{code or '?'} {name or ''}".strip() + f" ({element_id})"
      for element_id, code, name in accounts
    )
    super().__init__(
      f"Cannot post to inactive account(s): {named}. Reactivate the account "
      "(update-taxonomy-block, is_active=true) or choose another one."
    )


def assert_accounts_postable(
  session: Session, element_ids: Iterable[str], *, source: str | None = None
) -> None:
  """Raise `InactiveAccountError` if any line-item element is retired.

  A ``source`` in `SEVERABLE_SOURCES` is exempt: a synced ledger replays
  history against accounts retired after use. Callers pass ``source`` only for
  that replay shape (posted history), never for drafts.
  """
  if source and source.lower() in SEVERABLE_SOURCES:
    return
  ids = sorted({str(eid) for eid in element_ids if eid})
  if not ids:
    return
  rows = session.execute(
    select(Element.id, Element.code, Element.name).where(
      Element.id.in_(ids), Element.is_active.is_(False)
    )
  ).all()
  inactive = [(str(eid), code, name) for eid, code, name in rows]
  if inactive:
    raise InactiveAccountError(inactive)
