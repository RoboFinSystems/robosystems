"""Balance readings kept as observations, against real Postgres: one per
account, kind and day; a later figure for the day supersedes; the event
belongs to the entity whose books the account keeps."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from robosystems.adapters.bank_feed.balances import (
  BANK_AVAILABLE,
  BANK_CURRENT,
  FeedBalance,
  record_feed_balances,
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger import Event
from tests.ledger_entity import PARENT_ENTITY_ID
from tests.operations.roboledger.commands.test_reconciling_items_db import (
  session,
)

__all__ = ["session"]

pytestmark = pytest.mark.unit

NOON = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)
EVENING = datetime(2026, 10, 7, 21, 0, tzinfo=UTC)
NEXT_DAY = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)


def _element(session, name: str, *, balance_type: str = "debit") -> str:
  element = Element(
    name=name, code=name[:6], balance_type=balance_type, created_by="test"
  )
  session.add(element)
  session.flush()
  return str(element.id)


def _reading(account_id, kind, cents, observed_at=NOON) -> FeedBalance:
  return FeedBalance(
    account_id=account_id,
    kind=kind,
    as_of=observed_at.date(),
    stated_cents=cents,
    observed_at=observed_at,
    currency="USD",
  )


def _record(session, readings, links, entities=None):
  report = record_feed_balances(
    session,
    readings,
    source="plaid",
    connection_id="conn_1",
    account_elements=links,
    account_entities=entities or {},
    created_by="usr",
  )
  session.commit()
  return report


def _observations(session, element_id: str) -> list[Event]:
  return list(
    session.execute(
      select(Event)
      .where(
        Event.event_type == "balance_observed",
        Event.resource_element_id == element_id,
      )
      .order_by(Event.occurred_at, Event.id)
    ).scalars()
  )


class TestRecordFeedBalances:
  def test_each_figure_is_one_committed_support_event(self, session):
    checking = _element(session, "Checking")
    report = _record(
      session,
      [
        _reading("chk", BANK_CURRENT, 120050),
        _reading("chk", BANK_AVAILABLE, 100000),
      ],
      {"chk": checking},
    )
    assert (report.recorded, report.unchanged, report.skipped) == (2, 0, 0)
    events = _observations(session, checking)
    assert [(e.metadata_["kind"], e.amount, e.status) for e in events] == [
      (BANK_CURRENT, 120050, "committed"),
      (BANK_AVAILABLE, 100000, "committed"),
    ]
    current = events[0]
    assert (current.event_class, current.event_category) == (
      "support",
      "reconciliation",
    )
    assert current.source == "plaid"
    assert current.entity_id == PARENT_ENTITY_ID
    assert current.effective_at == datetime(2026, 10, 7)
    assert current.metadata_ == {
      "kind": BANK_CURRENT,
      "as_of": "2026-10-07",
      "observed_at": "2026-10-07T12:00:00+00:00",
      "stated_balance_cents": 120050,
      "currency": "USD",
      "account_id": "chk",
      "connection_id": "conn_1",
    }
    assert current.external_id == "plaid_balance_bank_current_chk_20261007T120000Z"

  def test_the_same_figure_again_is_not_a_second_reading(self, session):
    checking = _element(session, "Checking")
    _record(session, [_reading("chk", BANK_CURRENT, 5000)], {"chk": checking})
    report = _record(
      session,
      [_reading("chk", BANK_CURRENT, 5000, observed_at=EVENING)],
      {"chk": checking},
    )
    assert (report.recorded, report.unchanged) == (0, 1)
    assert len(_observations(session, checking)) == 1

  def test_a_later_figure_for_the_day_supersedes_the_earlier(self, session):
    checking = _element(session, "Checking")
    _record(session, [_reading("chk", BANK_CURRENT, 5000)], {"chk": checking})
    report = _record(
      session,
      [_reading("chk", BANK_CURRENT, 4200, observed_at=EVENING)],
      {"chk": checking},
    )
    assert (report.recorded, report.unchanged) == (1, 0)
    first, second = _observations(session, checking)
    assert (first.status, first.replaced_by_event_id) == ("superseded", second.id)
    assert (second.status, second.replaces_event_id, second.amount) == (
      "committed",
      first.id,
      4200,
    )

  def test_an_older_stamp_arriving_late_does_not_replace_the_newer(self, session):
    checking = _element(session, "Checking")
    _record(
      session,
      [_reading("chk", BANK_CURRENT, 4200, observed_at=EVENING)],
      {"chk": checking},
    )
    report = _record(
      session,
      [_reading("chk", BANK_CURRENT, 5000, observed_at=NOON)],
      {"chk": checking},
    )
    assert (report.recorded, report.unchanged) == (0, 1)
    (event,) = _observations(session, checking)
    assert (event.status, event.amount) == ("committed", 4200)

  def test_a_superseded_reading_replayed_is_not_a_collision(self, session):
    checking = _element(session, "Checking")
    first = _reading("chk", BANK_CURRENT, 5000)
    _record(session, [first], {"chk": checking})
    _record(
      session,
      [_reading("chk", BANK_CURRENT, 4200, observed_at=EVENING)],
      {"chk": checking},
    )
    report = _record(session, [first], {"chk": checking})
    assert (report.recorded, report.unchanged) == (0, 1)
    assert [e.status for e in _observations(session, checking)] == [
      "superseded",
      "committed",
    ]

  def test_a_new_day_is_a_new_reading_beside_the_old(self, session):
    checking = _element(session, "Checking")
    _record(session, [_reading("chk", BANK_CURRENT, 5000)], {"chk": checking})
    _record(
      session,
      [_reading("chk", BANK_CURRENT, 4200, observed_at=NEXT_DAY)],
      {"chk": checking},
    )
    events = _observations(session, checking)
    assert [(e.status, e.metadata_["as_of"]) for e in events] == [
      ("committed", "2026-10-07"),
      ("committed", "2026-10-08"),
    ]

  def test_a_cards_balance_owed_is_kept_debit_positive(self, session):
    card = _element(session, "Company Card", balance_type="credit")
    _record(session, [_reading("card", BANK_CURRENT, 35025)], {"card": card})
    (event,) = _observations(session, card)
    assert event.amount == -35025
    assert event.metadata_["stated_balance_cents"] == 35025

  def test_the_reading_belongs_to_the_accounts_entity(self, session):
    sub_checking = _element(session, "Cadence Checking")
    _record(
      session,
      [_reading("sub", BANK_CURRENT, 100)],
      {"sub": sub_checking},
      entities={"sub": "ent_sub"},
    )
    (event,) = _observations(session, sub_checking)
    assert event.entity_id == "ent_sub"

  def test_an_account_with_no_chart_account_is_skipped(self, session):
    checking = _element(session, "Checking")
    report = _record(
      session,
      [_reading("chk", BANK_CURRENT, 1), _reading("ghost", BANK_CURRENT, 2)],
      {"chk": checking},
    )
    assert (report.recorded, report.skipped) == (1, 1)
