"""Where a reconciliation's independent side comes from: one resolver per method.

A resolver answers one question for a period end: what does something outside
the ledger say each account holds? The engine compares that with the ledger and
knows nothing about where it came from, so a new source is a new resolver.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time
from typing import Literal, Protocol

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from robosystems.adapters.quickbooks.reports import (
  TrialBalanceReport,
  parse_trial_balance_report,
)
from robosystems.models.api.extensions.reconciliations import ReconciliationMethod
from robosystems.models.extensions import ElementTrait, Structure, Trait
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger import Event
from robosystems.operations.roboledger.reads.fiscal_calendar import live_qb_connection

_QUICKBOOKS = "quickbooks"
_SCHEDULES = "schedules"

# Where a schedule carries a balance the ledger should hold: the account it
# credits, and the cost account it names. Liabilities are left out, since a
# payment settles one outside any schedule.
_CARRYING_TRAITS = frozenset({"asset", "contraAsset"})
_RETRACTED_EVENT_STATUSES = ("voided", "superseded")


class NoSourceLedgerError(ValueError):
  """The graph has no synced ledger to compare against."""


class NothingToReconcileError(NoSourceLedgerError):
  """No check applies to this ledger."""

  def __init__(self) -> None:
    super().__init__(
      "Nothing to reconcile: this graph has no connected source ledger, and "
      "no schedule carries a balance on an asset account."
    )


@dataclass(frozen=True)
class ReconciliationWindow:
  period: str
  period_end: date
  fiscal_year_start: date


@dataclass(frozen=True)
class UnmatchedBalance:
  """A source account with a balance and no chart account in the ledger."""

  source_account_id: str
  name: str
  amount_cents: int


@dataclass(frozen=True)
class IndependentComponent:
  """One part of an account's independent balance, debit-positive cents."""

  structure_id: str
  name: str
  amount_cents: int
  note: str | None = None


@dataclass(frozen=True)
class IndependentSide:
  """Balances are debit-positive cents keyed by element id.

  ``covered_element_ids`` are the accounts the source knows; one it knows
  and reports nothing for holds zero there. A ``ledger``-scope side speaks
  for every account, so one it does not cover is a finding; an ``account``
  one speaks only for the accounts it covers.
  """

  method: ReconciliationMethod
  source: str
  balances: dict[str, int]
  covered_element_ids: frozenset[str]
  scope: Literal["ledger", "account"] = "ledger"
  source_account_ids: dict[str, str] = field(default_factory=dict)
  unmatched: list[UnmatchedBalance] = field(default_factory=list)
  components: dict[str, list[IndependentComponent]] = field(default_factory=dict)
  basis: str | None = None
  connection_id: str | None = None
  last_sync_at: datetime | None = None


class Resolver(Protocol):
  def resolve(self, session: Session, window: ReconciliationWindow) -> IndependentSide:
    """The independent side at ``window.period_end``."""
    ...


class SourceLedgerResolver:
  """The synced accounting system's own trial balance: checks our copy of
  its books, which every other check on a synced ledger rests on."""

  def __init__(self, graph_id: str) -> None:
    self.graph_id = graph_id

  def resolve(self, session: Session, window: ReconciliationWindow) -> IndependentSide:
    report, connection_id, last_sync_at = self._fetch(window)

    by_source_id: dict[str, str] = {}
    linked = session.execute(
      select(Element.id, Element.external_id, Element.connection_id)
      .where(
        Element.external_source == _QUICKBOOKS,
        Element.external_id.isnot(None),
      )
      .order_by(Element.id)
    ).all()
    for element_id, external_id, element_connection_id in linked:
      # A reconnected company can leave an earlier connection's copy of the
      # account; the live connection's element is the mirror.
      if str(external_id) not in by_source_id or (
        str(element_connection_id) == connection_id
      ):
        by_source_id[str(external_id)] = str(element_id)

    balances: dict[str, int] = {}
    unmatched: list[UnmatchedBalance] = []
    for account in report.accounts:
      element_id = by_source_id.get(account.account_id)
      if element_id is not None:
        balances[element_id] = balances.get(element_id, 0) + account.net_cents
      elif account.net_cents:
        unmatched.append(
          UnmatchedBalance(
            source_account_id=account.account_id,
            name=account.name,
            amount_cents=account.net_cents,
          )
        )

    return IndependentSide(
      method="source_ledger",
      source=_QUICKBOOKS,
      balances=balances,
      covered_element_ids=frozenset(by_source_id.values()),
      source_account_ids={eid: sid for sid, eid in by_source_id.items()},
      unmatched=unmatched,
      basis=report.basis,
      connection_id=connection_id,
      last_sync_at=last_sync_at,
    )

  def _fetch(
    self, window: ReconciliationWindow
  ) -> tuple[TrialBalanceReport, str, datetime | None]:
    """The source's trial balance, with the connection it was read through.

    Asked from the fiscal year's first day, so income and expense accounts
    are year-to-date whichever way the source reads the start date.
    """
    from robosystems.adapters.quickbooks.client import QBClient
    from robosystems.database import SessionFactory
    from robosystems.models.core.connection.connection_credentials import (
      ConnectionCredentials,
    )

    with SessionFactory() as platform_session:
      connection = live_qb_connection(platform_session, self.graph_id)
      if connection is None or not connection.realm_id:
        raise NoSourceLedgerError(
          "This graph has no connected QuickBooks ledger. The source_ledger "
          "check compares a synced ledger with the books it was synced from."
        )
      credentials_row = ConnectionCredentials.get_by_connection_id(
        connection.id, platform_session
      )
      if credentials_row is None:
        raise NoSourceLedgerError(
          f"Connection {connection.id} has no credentials; reconnect QuickBooks."
        )
      connection_id = str(connection.id)
      realm_id = str(connection.realm_id)
      last_sync_at = connection.last_sync
      credentials = credentials_row.get_credentials()

    client = QBClient(
      realm_id=realm_id,
      qb_credentials=credentials,
      connection_id=connection_id,
    )
    raw = client.get_trial_balance(
      window.fiscal_year_start.isoformat(), window.period_end.isoformat()
    )
    return parse_trial_balance_report(raw), connection_id, last_sync_at


class ScheduleRegisterResolver:
  """What each asset account's schedules say it carries at the period end.

  A schedule carries a balance on the account it credits (a prepaid drawing
  down from cost, or depreciation accumulating) and on the cost account it
  names. One that was disposed of, or ended early, carries nothing from then
  on: whatever is left in the account has no schedule behind it.
  ``also_cover`` adds accounts no schedule reaches any more, which then
  compare against zero.
  """

  def __init__(self, also_cover: frozenset[str] = frozenset()) -> None:
    self.also_cover = also_cover

  def resolve(self, session: Session, window: ReconciliationWindow) -> IndependentSide:
    as_of = window.period_end
    schedules = [
      schedule
      for schedule in session.execute(
        select(Structure)
        .where(Structure.block_type == "schedule", Structure.is_active.is_(True))
        .order_by(Structure.name.asc(), Structure.id.asc())
      ).scalars()
      if not _template(schedule).get("auto_reverse")
    ]
    carried = _carried_by_schedule(session, as_of)
    opening = _opening_by_schedule(session)
    disposed = _disposal_dates(session, as_of)

    candidate_ids = {
      element_id
      for schedule in schedules
      for element_id in (_credit_id(schedule), _asset_id(schedule))
      if element_id
    }
    traits = _primary_traits(session, candidate_ids)
    debit_normal = {
      str(element_id)
      for element_id, balance_type in session.execute(
        select(Element.id, Element.balance_type).where(Element.id.in_(candidate_ids))
      )
      if balance_type == "debit"
    }

    balances: dict[str, int] = {}
    components: dict[str, list[IndependentComponent]] = {}

    def _add(
      element_id: str, schedule: Structure, cents: int, note: str | None
    ) -> None:
      balances[element_id] = balances.get(element_id, 0) + cents
      components.setdefault(element_id, []).append(
        IndependentComponent(
          structure_id=str(schedule.id),
          name=schedule.name,
          amount_cents=cents,
          note=note,
        )
      )

    for schedule in schedules:
      schedule_id = str(schedule.id)
      if schedule_id not in carried:
        continue  # Not started by the period end.
      note = _ended_note(schedule, disposed.get(schedule_id), as_of)
      credit_id = _credit_id(schedule)
      asset_id = _asset_id(schedule)

      if credit_id and traits.get(credit_id) in _CARRYING_TRAITS:
        if schedule_id in opening:
          draws_down = opening[schedule_id] > 0
        else:
          draws_down = credit_id in debit_normal and traits[credit_id] != "contraAsset"
        cents = carried[schedule_id] if draws_down else -carried[schedule_id]
        _add(credit_id, schedule, 0 if note else cents, note)

      if (
        asset_id and asset_id != credit_id and traits.get(asset_id) in _CARRYING_TRAITS
      ):
        cost = int((_schedule_metadata(schedule)).get("original_amount") or 0)
        _add(asset_id, schedule, 0 if note else cost, note)

    for element_id in self.also_cover:
      balances.setdefault(element_id, 0)

    return IndependentSide(
      method="schedule_register",
      source=_SCHEDULES,
      scope="account",
      balances=balances,
      covered_element_ids=frozenset(balances),
      components=components,
    )


def _template(schedule: Structure) -> dict:
  return (schedule.metadata_ or {}).get("entry_template") or {}


def _schedule_metadata(schedule: Structure) -> dict:
  return (schedule.metadata_ or {}).get("schedule_metadata") or {}


def _credit_id(schedule: Structure) -> str | None:
  return _template(schedule).get("credit_element_id")


def _asset_id(schedule: Structure) -> str | None:
  return _schedule_metadata(schedule).get("asset_element_id")


def _ended_note(
  schedule: Structure, disposed_on: date | None, as_of: date
) -> str | None:
  """Why the schedule carries nothing at ``as_of``, or ``None`` when it does."""
  if disposed_on is not None:
    return f"Disposed of on {disposed_on.isoformat()}."
  ended = _schedule_metadata(schedule).get("end_date")
  if (
    (schedule.metadata_ or {}).get("truncations")
    and ended
    and ended <= as_of.isoformat()
  ):
    return f"Ended early on {ended}."
  return None


_CREDIT_ELEMENT_SQL = "s.metadata->'entry_template'->>'credit_element_id'"


def _carried_by_schedule(session: Session, as_of: date) -> dict[str, int]:
  """Each started schedule's balance on its credited account at ``as_of``,
  as the schedule states it: remaining cost, or the amount accumulated."""
  rows = session.execute(
    text(f"""
      SELECT DISTINCT ON (f.structure_id) f.structure_id, f.value
      FROM facts f
      JOIN structures s ON s.id = f.structure_id
      WHERE s.block_type = 'schedule'
        AND f.period_type = 'instant'
        AND f.period_end <= :as_of
        AND f.element_id = {_CREDIT_ELEMENT_SQL}
      ORDER BY f.structure_id, f.period_end DESC
    """),
    {"as_of": as_of},
  )
  return {str(row.structure_id): round(float(row.value) * 100) for row in rows}


def _opening_by_schedule(session: Session) -> dict[str, int]:
  """Each schedule's opening balance on its credited account: cost for one
  that draws down, zero for one that accumulates."""
  rows = session.execute(
    text(f"""
      SELECT DISTINCT ON (f.structure_id) f.structure_id, f.value
      FROM facts f
      JOIN structures s ON s.id = f.structure_id
      WHERE s.block_type = 'schedule'
        AND f.period_type = 'instant'
        AND f.period_start IS NULL
        AND f.element_id = {_CREDIT_ELEMENT_SQL}
      ORDER BY f.structure_id, f.period_end ASC
    """)
  )
  return {str(row.structure_id): round(float(row.value) * 100) for row in rows}


def _disposal_dates(session: Session, as_of: date) -> dict[str, date]:
  schedule_id = Event.metadata_["schedule_id"].astext
  rows = session.execute(
    select(schedule_id, func.min(Event.occurred_at))
    .where(
      Event.event_type == "asset_disposed",
      Event.status.notin_(_RETRACTED_EVENT_STATUSES),
      Event.occurred_at <= datetime.combine(as_of, time.max, tzinfo=UTC),
    )
    .group_by(schedule_id)
  )
  return {str(sid): occurred_at.date() for sid, occurred_at in rows if sid}


def _primary_traits(session: Session, element_ids: set[str]) -> dict[str, str]:
  if not element_ids:
    return {}
  rows = session.execute(
    select(ElementTrait.element_id, Trait.identifier)
    .join(Trait, Trait.id == ElementTrait.trait_id)
    .where(
      ElementTrait.element_id.in_(element_ids),
      ElementTrait.is_primary.is_(True),
      Trait.category == "elementsOfFinancialStatements",
    )
  )
  return {str(element_id): str(identifier) for element_id, identifier in rows}
