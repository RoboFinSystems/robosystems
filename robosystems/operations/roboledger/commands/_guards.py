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
from robosystems.operations.roboledger.entity_scope import (
  is_group_parent,
  resolve_entity_id,
)

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
  WHERE entity_id = :entity_id
    AND start_date <= :posting_date AND end_date >= :posting_date
  LIMIT 1
"""


def _period_covering(session: Session, entity_id: str, posting_date: date):
  return session.execute(
    text(_PERIOD_COVERING_DATE),
    {"entity_id": entity_id, "posting_date": posting_date},
  ).fetchone()


def assert_period_not_closed(
  session: Session, *posting_dates: date, entity_id: str | None = None
) -> None:
  """Raise `ClosedPeriodError` if any of the dates falls in a period that is
  closed in ``entity_id``'s books (default the group parent's).

  A date is closed when its month is on or before that entity's
  ``closed_through_period``, or its ``FiscalPeriod`` row says ``closed``. A
  month with no row is not open: rows reach back only so far, and
  ``closed_through`` closes every month before it.

  Takes the shared, transaction-scoped period fence on each distinct month in
  sorted order, keyed by the month itself rather than by a row, and re-reads
  under it. Close holds the exclusive side from before it creates the month's
  row, so a writer cannot slip into a month while it is being closed. The
  fence is per graph and month, not per entity: one entity's close briefly
  holds a sibling's writers out of the same month.
  """
  dates = [d for d in posting_dates if d is not None]
  if not dates:
    return
  ledger = session.execute(
    text(
      "SELECT graph_id FROM fiscal_calendar "
      "UNION ALL SELECT graph_id FROM fiscal_periods LIMIT 1"
    )
  ).first()
  if ledger is None:
    return

  first_date_by_month: dict[str, date] = {}
  for posting_date in sorted(dates):
    first_date_by_month.setdefault(f"{posting_date:%Y-%m}", posting_date)

  for month in sorted(first_date_by_month):
    acquire_shared_period_fence(
      session,
      ledger.graph_id,
      month,
      detail=(
        f"Period {month} is being closed or reopened by another process. "
        "Retry in a moment."
      ),
    )

  closed = closed_periods(session, dates, entity_id=entity_id)
  if closed:
    month, posting_date = closed[0]
    raise ClosedPeriodError(month, posting_date)


def closed_periods(
  session: Session, dates: Iterable[date], *, entity_id: str | None = None
) -> list[tuple[str, date]]:
  """The months among ``dates`` closed in ``entity_id``'s books (default the
  group parent's), each with its first date, sorted.

  The one statement of the rule: a month on or before ``closed_through``, or
  one whose ``FiscalPeriod`` row is ``closed``. Takes no lock; writers go
  through `assert_period_not_closed`, which fences first.
  """
  first_date_by_month: dict[str, date] = {}
  for posting_date in sorted(d for d in dates if d is not None):
    first_date_by_month.setdefault(f"{posting_date:%Y-%m}", posting_date)
  if not first_date_by_month:
    return []
  entity_id = entity_id or resolve_entity_id(session)
  closed_through = session.execute(
    text(
      "SELECT closed_through_period FROM fiscal_calendar "
      "WHERE entity_id = :entity_id LIMIT 1"
    ),
    {"entity_id": entity_id},
  ).scalar()
  closed: list[tuple[str, date]] = []
  for month, posting_date in sorted(first_date_by_month.items()):
    if closed_through and month <= closed_through:
      closed.append((month, posting_date))
      continue
    row = _period_covering(session, entity_id, posting_date)
    if row is not None and row.status == "closed":
      closed.append((month, posting_date))
  return closed


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


class AccountOutsideEntityChartError(ValueError):
  """A line item names an account in another entity's chart of accounts.
  Each entity keeps its books in its own chart; entities combine at the
  reporting concepts their charts map to, never at an account."""

  def __init__(self, accounts: list[tuple[str, str | None, str | None]]) -> None:
    self.accounts = accounts
    named = ", ".join(
      f"{code or '?'} {name or ''}".strip() + f" ({element_id})"
      for element_id, code, name in accounts
    )
    super().__init__(
      f"Cannot post to another entity's account(s): {named}. Use the "
      "matching account in this entity's own chart of accounts."
    )


_ACCOUNTS_OUTSIDE_ENTITY_CHART = text("""
  SELECT e.id, e.code, e.name,
         EXISTS (
           SELECT 1 FROM entity_taxonomies owner
           WHERE owner.taxonomy_id = t.id AND owner.basis = 'chart_of_accounts'
         ) AS owned
  FROM elements e
  JOIN taxonomies t
    ON t.id = e.taxonomy_id AND t.taxonomy_type = 'chart_of_accounts'
  WHERE e.id = ANY(:element_ids)
    AND NOT EXISTS (
      SELECT 1 FROM entity_taxonomies own
      WHERE own.taxonomy_id = t.id
        AND own.basis = 'chart_of_accounts'
        AND own.entity_id = :entity_id
    )
  ORDER BY e.code, e.id
""")


def _accounts_outside_entity_chart(
  session: Session, element_ids: list[str], entity_id: str
) -> list[tuple[str, str | None, str | None]]:
  """The chart accounts among ``element_ids`` that are not ``entity_id``'s.

  A chart linked to no entity is the group parent's. Anything that is not a
  chart account (a library concept posted to directly) is nobody's and passes.
  """
  rows = session.execute(
    _ACCOUNTS_OUTSIDE_ENTITY_CHART,
    {"element_ids": element_ids, "entity_id": entity_id},
  ).all()
  if not rows:
    return []
  unowned_are_ours = any(not row.owned for row in rows) and is_group_parent(
    session, entity_id
  )
  return [
    (str(row.id), row.code, row.name)
    for row in rows
    if row.owned or not unowned_are_ours
  ]


def assert_accounts_postable(
  session: Session,
  element_ids: Iterable[str],
  *,
  source: str | None = None,
  entity_id: str | None = None,
) -> None:
  """Raise `InactiveAccountError` if any line-item element is retired, and
  `AccountOutsideEntityChartError` if one belongs to an entity other than
  ``entity_id``, the entity whose books the lines go into.

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
  if entity_id is not None:
    foreign = _accounts_outside_entity_chart(session, ids, entity_id)
    if foreign:
      raise AccountOutsideEntityChartError(foreign)
