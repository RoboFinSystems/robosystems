"""The batch that takes suggestions, on real Postgres: every open line the
filters select goes through the single-line update, oldest first, and a line
that cannot be taken is answered with the reason instead of forced."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from robosystems.models.api.extensions.agent import AcceptSuggestionsRequest
from robosystems.models.extensions.roboledger import Agent, Entry, Event
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.roboledger.classification import read_default
from robosystems.operations.roboledger.commands.suggestions import (
  accept_suggestions,
)
from tests.ledger_entity import entity_account

pytestmark = pytest.mark.unit

GRAPH_ID = "kg0123456789abcdef0d"


@pytest.fixture()
def feed(two_entities):
  """A parent's bank account, two expense accounts, and Gusto as a Plaid
  counterparty."""
  t = two_entities
  entity = t.parent.id
  t.cash = entity_account(t.session, entity, "Checking")
  t.payroll = entity_account(t.session, entity, "Payroll")
  t.software = entity_account(t.session, entity, "Software")
  agent = Agent(
    agent_type="vendor",
    name="Gusto",
    source="plaid",
    external_id="gusto",
    connection_id="conn_1",
    created_by="test",
  )
  t.session.add(agent)
  t.session.flush()
  t.agent = agent
  t.session.commit()
  return t


def _line(
  t,
  *,
  day: int = 2,
  month: int = 10,
  status: str = "captured",
  suggested: str | None = "software",
  source: str = "plaid",
  agent: bool = True,
  **metadata,
) -> Event:
  """A feed line on the parent's checking account, suggested to Software from
  the bank's category unless told otherwise."""
  suggestion = {}
  if suggested is not None:
    suggestion = {
      "suggested_element_id": getattr(t, suggested),
      "suggested_account_name": suggested.title(),
      "suggestion_source": "tier0",
    }
  event = Event(
    entity_id=t.parent.id,
    event_type="bank_transaction",
    event_category="purchase",
    event_class="economic",
    resource_type="money",
    resource_element_id=t.cash,
    agent_id=t.agent.id if agent else None,
    occurred_at=datetime(2026, month, day, tzinfo=UTC),
    effective_at=datetime(2026, month, day, tzinfo=UTC),
    status=status,
    source=source,
    amount=-12_000,
    description=f"GUSTO {month:02d}-{day:02d}",
    metadata_={**suggestion, **metadata},
    created_by="test",
  )
  t.session.add(event)
  t.session.commit()
  return event


def _run(t, **fields):
  return accept_suggestions(
    t.session,
    AcceptSuggestionsRequest(entity_id=t.parent.id, **fields),
    "usr_1",
    graph_id=GRAPH_ID,
  )


def _status(t, event: Event) -> str:
  t.session.expire_all()
  return str(t.session.get(Event, event.id).status)


def _metadata(t, event: Event) -> dict:
  t.session.expire_all()
  return dict(t.session.get(Event, event.id).metadata_ or {})


def test_every_open_suggested_line_is_taken_oldest_first(feed):
  late = _line(feed, day=9)
  early = _line(feed, day=1)
  middle = _line(feed, day=5)
  name_only = _line(feed, suggested=None, suggested_account_name="Bank fees")
  chosen = _line(feed, status="classified", classified_element_id=feed.payroll)

  out = _run(feed)

  assert (out.selected, out.classified, out.committed, out.skipped) == (3, 3, 0, 0)
  assert out.truncated is False
  assert [r.event_id for r in out.lines] == [early.id, middle.id, late.id]
  assert all(r.outcome == "classified" for r in out.lines)
  assert all(r.element_id == feed.software for r in out.lines)
  for event in (early, middle, late):
    assert _status(feed, event) == "classified"
    metadata = _metadata(feed, event)
    assert metadata["accept_suggestion"] is True
    assert metadata["classified_via"] == "accept-suggestions"
  assert _status(feed, name_only) == "captured"
  assert "accept_suggestion" not in _metadata(feed, name_only)
  assert _metadata(feed, chosen)["classified_element_id"] == feed.payroll


def test_commit_posts_each_line_and_teaches_the_default(feed):
  first = _line(feed, day=1)
  second = _line(feed, day=2)

  out = _run(feed, commit=True)

  assert (out.classified, out.committed, out.skipped) == (0, 2, 0)
  for event in (first, second):
    assert _status(feed, event) == "committed"
    assert _metadata(feed, event)["suggestion_outcome"] == "accepted"
    assert (
      feed.session.query(Entry).filter(Entry.triggered_by_event_id == event.id).count()
      == 1
    )
  feed.session.refresh(feed.agent)
  default = read_default(feed.agent.metadata_)
  assert (default.element_id, default.confirmations, default.overrides) == (
    feed.software,
    2,
    0,
  )


def test_explicit_ids_answer_every_line(feed):
  open_line = _line(feed, day=3)
  chosen = _line(feed, status="classified", classified_element_id=feed.payroll)
  name_only = _line(feed, suggested=None, suggested_account_name="Bank fees")
  _line(feed, day=4)  # not named, so not touched

  out = _run(feed, event_ids=[open_line.id, chosen.id, name_only.id])

  assert out.selected == 3
  by_id = {r.event_id: r for r in out.lines}
  assert by_id[open_line.id].outcome == "classified"
  assert (by_id[chosen.id].outcome, by_id[chosen.id].reason) == (
    "skipped",
    "not_open",
  )
  assert (by_id[name_only.id].outcome, by_id[name_only.id].reason) == (
    "skipped",
    "no_suggestion",
  )
  assert "Bank fees" in by_id[name_only.id].detail
  assert _status(feed, chosen) == "classified"


def test_the_filters_narrow_the_selection(feed):
  learned = _line(feed, day=1, suggestion_source="agent_default")
  banks = _line(feed, day=2)
  september = _line(feed, day=20, month=9, suggestion_source="agent_default")
  no_agent = _line(feed, day=3, agent=False, suggestion_source="agent_default")

  by_source = _run(feed, suggestion_source="agent_default", dry_run=True)
  assert {r.event_id for r in by_source.lines} == {
    learned.id,
    september.id,
    no_agent.id,
  }

  by_dates = _run(
    feed,
    occurred_from=date(2026, 10, 1),
    occurred_to=date(2026, 10, 2),
    dry_run=True,
  )
  assert {r.event_id for r in by_dates.lines} == {learned.id, banks.id}

  by_agent = _run(feed, agent_id=feed.agent.id, dry_run=True)
  assert {r.event_id for r in by_agent.lines} == {
    learned.id,
    banks.id,
    september.id,
  }

  assert _run(feed, source="mercury", dry_run=True).selected == 0


def test_dry_run_counts_without_writing(feed):
  line = _line(feed)
  name_only = _line(feed, suggested=None)

  out = _run(feed, event_ids=[line.id, name_only.id], commit=True, dry_run=True)

  assert out.dry_run is True
  assert (out.committed, out.skipped) == (1, 1)
  assert _status(feed, line) == "captured"
  assert "accept_suggestion" not in _metadata(feed, line)


def test_a_closed_month_is_skipped_and_the_walk_goes_on(feed):
  august = _line(feed, day=12, month=8)
  october = _line(feed, day=2)
  feed.session.add(
    FiscalPeriod(
      graph_id=GRAPH_ID,
      entity_id=feed.parent.id,
      name="2026-08",
      start_date=date(2026, 8, 1),
      end_date=date(2026, 8, 31),
      period_type="month",
      status="closed",
    )
  )
  feed.session.commit()

  out = _run(feed, commit=True)

  by_id = {r.event_id: r for r in out.lines}
  assert (by_id[august.id].outcome, by_id[august.id].reason) == (
    "skipped",
    "closed_period",
  )
  assert by_id[october.id].outcome == "committed"
  assert (out.committed, out.skipped) == (1, 1)
  assert _status(feed, august) == "captured"
  assert _status(feed, october) == "committed"


def test_a_suggestion_the_handler_cannot_post_is_skipped(feed):
  """A suggestion is only resolved to an account when the line posts, so the
  refusal comes from the commit, and the line stays open with the reason."""
  stale = _line(feed, day=1, suggested_element_id="elem_gone")
  good = _line(feed, day=2)

  out = _run(feed, commit=True)

  by_id = {r.event_id: r for r in out.lines}
  assert (by_id[stale.id].outcome, by_id[stale.id].reason) == ("skipped", "refused")
  assert "elem_gone" in (by_id[stale.id].detail or "")
  assert by_id[good.id].outcome == "committed"
  assert _status(feed, stale) == "captured"
  assert _status(feed, good) == "committed"


def test_the_limit_bounds_a_call(feed):
  first = _line(feed, day=1)
  second = _line(feed, day=2)
  _line(feed, day=3)

  out = _run(feed, limit=2)

  assert (out.selected, out.classified, out.truncated) == (3, 2, True)
  assert [r.event_id for r in out.lines] == [first.id, second.id]
