"""The month's classification stats, on real Postgres: counted from the
outcome each commit stamped, per suggestion source, with the lines posted
before outcomes existed kept outside the share."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from robosystems.models.extensions.roboledger import Event
from robosystems.operations.roboledger.reads.classification import (
  classification_stats,
)
from tests.ledger_entity import entity_account

pytestmark = pytest.mark.unit


@pytest.fixture()
def books(two_entities):
  t = two_entities
  t.cash = entity_account(t.session, t.parent.id, "Checking")
  t.sub_cash = entity_account(t.session, t.sub.id, "Sub Checking")
  t.session.commit()
  return t


def _line(t, *, day=2, month=10, status="committed", entity=None, **metadata):
  entity = entity or t.parent
  event = Event(
    entity_id=entity.id,
    event_type="bank_transaction",
    event_category="purchase",
    event_class="economic",
    resource_type="money",
    resource_element_id=t.cash if entity is t.parent else t.sub_cash,
    occurred_at=datetime(2026, month, day, tzinfo=UTC),
    effective_at=datetime(2026, month, day, tzinfo=UTC),
    status=status,
    source="plaid",
    amount=-5_000,
    description="line",
    metadata_=metadata,
    created_by="test",
  )
  t.session.add(event)
  t.session.flush()
  return event


def test_the_month_is_counted_per_source(books):
  learned = {"suggested_element_id": "el_a", "suggestion_source": "agent_default"}
  _line(books, suggestion_outcome="accepted", **learned)
  _line(books, day=3, suggestion_outcome="accepted", **learned)
  _line(books, day=4, suggestion_outcome="overridden", **learned)
  _line(books, day=5, suggestion_outcome="none")
  _line(
    books, day=6, suggestion_outcome="split", suggested_element_id="el_b"
  )  # a suggestion from before the ladder named its source: the feed's
  _line(books, day=7)  # committed before outcomes were stamped
  _line(books, day=8, status="captured", **learned)
  _line(books, day=9, status="classified", **learned)
  _line(books, day=20, month=9, suggestion_outcome="accepted", **learned)
  _line(books, day=2, entity=books.sub, suggestion_outcome="overridden", **learned)
  books.session.commit()

  stats = classification_stats(books.session, "2026-10", books.parent.id)

  assert (stats.period, stats.entity_id) == ("2026-10", books.parent.id)
  assert (stats.lines, stats.accepted, stats.overridden) == (5, 2, 1)
  assert (stats.unsuggested, stats.split, stats.unstamped) == (1, 1, 1)
  assert stats.open_lines == 2
  assert stats.human_choice_share == 0.6
  assert [
    (r.source, r.lines, r.accepted, r.overridden, r.unsuggested, r.split)
    for r in stats.by_source
  ] == [
    ("agent_default", 3, 2, 1, 0, 0),
    ("none", 1, 0, 0, 1, 0),
    ("tier0", 1, 0, 0, 0, 1),
  ]


def test_a_month_with_no_outcomes_has_no_share(books):
  _line(books, day=8, status="captured", suggested_element_id="el_a")
  books.session.commit()

  stats = classification_stats(books.session, "2026-10", books.parent.id)

  assert (stats.lines, stats.open_lines, stats.human_choice_share) == (0, 1, None)
  assert stats.by_source == []


def test_the_subsidiary_is_counted_on_its_own(books):
  _line(
    books,
    entity=books.sub,
    suggestion_outcome="overridden",
    suggested_element_id="el_a",
    suggestion_source="agent_default",
  )
  books.session.commit()

  stats = classification_stats(books.session, "2026-10", books.sub.id)

  assert (stats.lines, stats.overridden, stats.human_choice_share) == (1, 1, 1.0)


def test_a_malformed_period_is_refused(books):
  with pytest.raises(ValueError, match="YYYY-MM"):
    classification_stats(books.session, "October", books.parent.id)
