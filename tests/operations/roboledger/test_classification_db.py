"""Learned classification on real Postgres: a counterparty's default account
is learned from committed lines and offered as the next line's suggestion."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from robosystems.adapters.bank_feed.load import refresh_hints
from robosystems.models.api.extensions.agent import (
  LearnClassificationDefaultsRequest,
  UpdateAgentRequest,
)
from robosystems.models.extensions.roboledger import Agent, Event
from robosystems.operations.roboledger.classification import (
  apply_ladder,
  learn_from_commit,
  read_default,
)
from robosystems.operations.roboledger.commands.agent import (
  learn_classification_defaults,
  update_agent,
)
from tests.ledger_entity import entity_account

pytestmark = pytest.mark.unit


@pytest.fixture()
def feed(two_entities):
  """A parent's bank account, three expense accounts, Gusto as a Plaid
  counterparty, and the bank's own category hint pointing at Software."""
  t = two_entities
  entity = t.parent.id
  t.cash = entity_account(t.session, entity, "Checking")
  t.payroll = entity_account(t.session, entity, "Payroll")
  t.fees = entity_account(t.session, entity, "Professional fees")
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


def _line(t, *, status="captured", **metadata) -> Event:
  event = Event(
    entity_id=t.parent.id,
    event_type="bank_transaction",
    event_category="purchase",
    event_class="economic",
    resource_type="money",
    resource_element_id=t.cash,
    agent_id=t.agent.id,
    occurred_at=datetime(2026, 10, 2, tzinfo=UTC),
    effective_at=datetime(2026, 10, 2, tzinfo=UTC),
    status=status,
    source="plaid",
    amount=-120_000,
    description="GUSTO PAYROLL",
    metadata_={
      "suggested_element_id": t.software,
      "suggested_account_name": "Software",
      **metadata,
    },
    created_by="test",
  )
  t.session.add(event)
  t.session.flush()
  return event


def _commit(t, event: Event, **metadata) -> None:
  event.metadata_ = {**(event.metadata_ or {}), **metadata}
  event.status = "committed"
  learn_from_commit(t.session, event, "usr_1")
  t.session.flush()


def _default(t):
  t.session.refresh(t.agent)
  return read_default(t.agent.metadata_)


def test_with_no_default_the_banks_category_is_the_suggestion(feed):
  out = apply_ladder(feed.session, dict(_line(feed).metadata_), feed.agent.id)

  assert out["suggested_element_id"] == feed.software
  assert out["suggestion_source"] == "tier0"
  assert out["feed_suggested_element_id"] == feed.software


def test_the_first_commit_teaches_the_default_and_resuggests_open_lines(feed):
  committed = _line(feed, status="classified")
  waiting = _line(feed)

  _commit(feed, committed, classified_element_id=feed.payroll)

  default = _default(feed)
  assert (default.element_id, default.confirmations, default.overrides) == (
    feed.payroll,
    1,
    0,
  )
  assert default.learned_from == committed.id
  assert committed.metadata_["suggestion_outcome"] == "overridden"
  feed.session.refresh(waiting)
  assert waiting.metadata_["suggested_element_id"] == feed.payroll
  assert waiting.metadata_["suggestion_source"] == "agent_default"
  assert (
    "Gusto, classified to Payroll on 1 of 1" in waiting.metadata_["suggestion_basis"]
  )


def test_agreeing_lines_confirm_and_others_count_as_overrides(feed):
  first = _line(feed)
  accepted = _line(feed)
  _commit(feed, first, classified_element_id=feed.payroll)
  feed.session.refresh(accepted)
  _commit(feed, accepted, accept_suggestion=True)
  _commit(feed, _line(feed), classified_element_id=feed.fees)

  default = _default(feed)
  assert (default.element_id, default.confirmations, default.overrides) == (
    feed.payroll,
    2,
    1,
  )
  assert accepted.metadata_["suggestion_outcome"] == "accepted"


def test_the_default_moves_only_when_the_committer_asks(feed):
  _commit(feed, _line(feed), classified_element_id=feed.payroll)
  _commit(
    feed, _line(feed), classified_element_id=feed.fees, remember_classification=True
  )

  default = _default(feed)
  assert (default.element_id, default.confirmations, default.overrides) == (
    feed.fees,
    1,
    0,
  )


def test_declining_to_remember_leaves_no_default(feed):
  _commit(
    feed, _line(feed), classified_element_id=feed.fees, remember_classification=False
  )
  assert _default(feed) is None


def test_a_split_teaches_nothing(feed):
  line = _line(feed)
  _commit(
    feed,
    line,
    classified_allocations=[
      {"element_id": feed.payroll, "amount": 100_000},
      {"element_id": feed.fees, "amount": 20_000},
    ],
  )
  assert _default(feed) is None
  assert line.metadata_["suggestion_outcome"] == "split"


def test_always_ask_offers_the_banks_category_and_says_why(feed):
  _commit(feed, _line(feed), classified_element_id=feed.payroll)
  update_agent(
    feed.session,
    UpdateAgentRequest(agent_id=feed.agent.id, classification_mode="always_ask"),
    "usr_1",
  )
  waiting = _line(feed)
  out = apply_ladder(feed.session, dict(waiting.metadata_), feed.agent.id)

  assert out["suggested_element_id"] == feed.software
  assert out["suggestion_source"] == "tier0"
  assert out["suggestion_basis"] == "Gusto is set to always ask."


def test_a_default_set_by_hand_resuggests_and_clearing_it_restores_the_banks(feed):
  waiting = _line(feed)
  feed.session.commit()

  agent = update_agent(
    feed.session,
    UpdateAgentRequest(agent_id=feed.agent.id, classification_element_id=feed.payroll),
    "usr_1",
  )
  assert agent.classification is not None
  assert (agent.classification.account_name, agent.classification.confirmations) == (
    "Payroll",
    0,
  )
  feed.session.refresh(waiting)
  assert waiting.metadata_["suggested_element_id"] == feed.payroll

  update_agent(
    feed.session,
    UpdateAgentRequest(agent_id=feed.agent.id, classification_element_id=""),
    "usr_1",
  )
  feed.session.refresh(waiting)
  assert waiting.metadata_["suggested_element_id"] == feed.software
  assert waiting.metadata_["suggestion_source"] == "tier0"


def test_the_default_is_not_set_through_metadata(feed):
  with pytest.raises(ValueError, match="classification_element_id"):
    update_agent(
      feed.session,
      UpdateAgentRequest(
        agent_id=feed.agent.id,
        metadata_patch={"classification": {"element_id": feed.payroll}},
      ),
      "usr_1",
    )


def test_a_later_pull_keeps_the_learned_suggestion_on_top(feed):
  _commit(feed, _line(feed), classified_element_id=feed.payroll)
  waiting = _line(feed)
  feed.session.refresh(waiting)

  changed = refresh_hints(
    waiting,
    {"suggested_element_id": feed.fees, "suggested_account_name": "Professional fees"},
    ("suggested_element_id", "suggested_account_name"),
  )

  assert changed
  assert waiting.metadata_["suggested_element_id"] == feed.payroll
  assert waiting.metadata_["feed_suggested_element_id"] == feed.fees


def test_history_seeds_each_counterparty_with_its_most_used_account(feed):
  for account in (feed.payroll, feed.payroll, feed.fees):
    _line(feed, status="committed", classified_element_id=account)
  waiting = _line(feed)
  feed.session.commit()

  dry = learn_classification_defaults(
    feed.session, LearnClassificationDefaultsRequest(dry_run=True), "usr_1"
  )
  assert (dry.agents_learned, dry.lines_read) == (1, 3)
  assert _default(feed) is None

  learned = learn_classification_defaults(
    feed.session, LearnClassificationDefaultsRequest(), "usr_1"
  )
  assert (learned.agents_learned, learned.open_lines_resuggested) == (1, 1)
  default = _default(feed)
  assert (default.element_id, default.confirmations, default.overrides) == (
    feed.payroll,
    2,
    1,
  )
  feed.session.refresh(waiting)
  assert waiting.metadata_["suggestion_source"] == "agent_default"

  again = learn_classification_defaults(
    feed.session, LearnClassificationDefaultsRequest(), "usr_1"
  )
  assert (again.agents_learned, again.agents_kept) == (0, 1)
