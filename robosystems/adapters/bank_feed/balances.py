"""Balances a bank feed reports, kept as observations.

Every sync asks the bank for its accounts and gets their balances with
them; a feed reports only the current figure, so a day not recorded is
history the reconciliation can never recover. Each reading becomes a
``balance_observed`` support event on the entity whose books the account
keeps — no books, no inbox, one per account, kind and day.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.extensions.element import Element
from robosystems.operations.roboledger.entity_scope import ensure_entity_id
from robosystems.operations.roboledger.reconciliations.observations import (
  BANK_AVAILABLE,
  BANK_CURRENT,
  FeedBalance,
  record_feed_balance,
)

__all__ = [
  "BANK_AVAILABLE",
  "BANK_CURRENT",
  "FeedBalance",
  "FeedBalanceReport",
  "record_feed_balances",
]


@dataclass
class FeedBalanceReport:
  recorded: int = 0
  unchanged: int = 0
  skipped: int = 0

  def as_counts(self) -> dict[str, int]:
    return {
      "balances_recorded": self.recorded,
      "balances_unchanged": self.unchanged,
      "balances_skipped": self.skipped,
    }


def record_feed_balances(
  session: Session,
  readings: Iterable[FeedBalance],
  *,
  source: str,
  connection_id: str,
  account_elements: Mapping[str, str],
  account_entities: Mapping[str, str | None],
  created_by: str,
) -> FeedBalanceReport:
  """Record a sync's balance readings against the chart accounts the feed's
  accounts are linked to. A reading for an account with no chart account is
  skipped; one whose account has no entity of its own is the group parent's.
  Flushes; the caller commits."""
  readings = list(readings)
  report = FeedBalanceReport()
  element_ids = {
    account_elements[r.account_id]
    for r in readings
    if account_elements.get(r.account_id)
  }
  elements: dict[str, Element] = {}
  if element_ids:
    rows = session.execute(
      select(Element).where(Element.id.in_(sorted(element_ids)))
    ).scalars()
    elements = {str(e.id): e for e in rows}
  parent_id: str | None = None
  for reading in readings:
    element = elements.get(account_elements.get(reading.account_id) or "")
    if element is None:
      report.skipped += 1
      continue
    entity_id = account_entities.get(reading.account_id)
    if not entity_id:
      parent_id = parent_id or ensure_entity_id(session)
      entity_id = parent_id
    _, outcome = record_feed_balance(
      session,
      element=element,
      entity_id=entity_id,
      source=source,
      connection_id=connection_id,
      reading=reading,
      created_by=created_by,
    )
    if outcome == "recorded":
      report.recorded += 1
    else:
      report.unchanged += 1
  return report
