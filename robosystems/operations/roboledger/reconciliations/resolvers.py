"""Where a reconciliation's independent side comes from: one resolver per method.

A resolver answers one question for a period end: what does something outside
the ledger say each account holds? The engine compares that with the ledger and
knows nothing about where it came from, so a new source is a new resolver.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.adapters.quickbooks.reports import (
  TrialBalanceReport,
  parse_trial_balance_report,
)
from robosystems.models.api.extensions.reconciliations import ReconciliationMethod
from robosystems.models.extensions.element import Element
from robosystems.operations.roboledger.reads.fiscal_calendar import live_qb_connection

_QUICKBOOKS = "quickbooks"


class NoSourceLedgerError(ValueError):
  """The graph has no synced ledger to compare against."""


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
class IndependentSide:
  """Balances are debit-positive cents keyed by element id.

  ``covered_element_ids`` are the accounts the source knows; one it knows
  and reports nothing for holds zero there.
  """

  method: ReconciliationMethod
  source: str
  balances: dict[str, int]
  covered_element_ids: frozenset[str]
  source_account_ids: dict[str, str] = field(default_factory=dict)
  unmatched: list[UnmatchedBalance] = field(default_factory=list)
  basis: str | None = None
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
      select(Element.id, Element.external_id, Element.connection_id).where(
        Element.external_source == _QUICKBOOKS,
        Element.external_id.isnot(None),
      )
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
