"""Learned classification on real Postgres: a counterparty's default account
is learned from committed lines and offered as the next line's suggestion."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from robosystems.adapters.bank_feed.load import refresh_hints
from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.api.extensions.agent import (
  LearnClassificationDefaultsRequest,
  UpdateAgentRequest,
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger import Agent, Event
from robosystems.operations.event_block.commands import (
  _refuse_overlooking_the_open_item,
  _resuggest_after_obligation,
  _resuggest_after_settlement,
  _settle_the_suggested_item,
  create_event_block_in_session,
)
from robosystems.operations.event_block.python_handlers.types import (
  HandlerMetadataValidationError,
)
from robosystems.operations.roboledger.classification import (
  apply_ladder,
  learn_from_commit,
  read_default,
)
from robosystems.operations.roboledger.commands.agent import (
  learn_classification_defaults,
  update_agent,
)
from robosystems.operations.roboledger.reads.agent import agent_to_response
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


def _line(t, *, status="captured", entity=None, cash=None, **metadata) -> Event:
  event = Event(
    entity_id=entity or t.parent.id,
    event_type="bank_transaction",
    event_category="purchase",
    event_class="economic",
    resource_type="money",
    resource_element_id=cash or t.cash,
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
  return read_default(t.session, t.agent.metadata_)


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


@pytest.mark.parametrize("key", ["classification", "classifications"])
def test_the_default_is_not_set_through_metadata(feed, key):
  with pytest.raises(ValueError, match="classification_element_id"):
    update_agent(
      feed.session,
      UpdateAgentRequest(
        agent_id=feed.agent.id,
        metadata_patch={key: {"element_id": feed.payroll}},
      ),
      "usr_1",
    )


@pytest.fixture()
def sub_books(feed):
  """The subsidiary's own bank account and payroll account, on its chart."""
  feed.sub_cash = entity_account(feed.session, feed.sub.id, "Sub checking")
  feed.sub_payroll = entity_account(feed.session, feed.sub.id, "Sub payroll")
  feed.session.commit()
  return feed


def _sub_line(t, **kwargs) -> Event:
  return _line(t, entity=t.sub.id, cash=t.sub_cash, **kwargs)


def test_one_entitys_commit_never_moves_anothers_default(sub_books):
  t = sub_books
  parent_waiting = _line(t)
  sub_waiting = _sub_line(t)
  _commit(t, _line(t), classified_element_id=t.payroll)

  _commit(
    t, _sub_line(t), classified_element_id=t.sub_payroll, remember_classification=True
  )

  t.session.refresh(t.agent)
  assert read_default(t.session, t.agent.metadata_).element_id == t.payroll
  assert (
    read_default(t.session, t.agent.metadata_, t.sub.id).element_id == t.sub_payroll
  )
  t.session.refresh(parent_waiting)
  t.session.refresh(sub_waiting)
  assert parent_waiting.metadata_["suggested_element_id"] == t.payroll
  assert sub_waiting.metadata_["suggested_element_id"] == t.sub_payroll
  assert sub_waiting.metadata_["suggestion_source"] == "agent_default"


def test_a_default_from_before_the_entity_key_stays_with_its_charts_entity(sub_books):
  t = sub_books
  t.agent.metadata_ = {
    "classification": {"element_id": t.payroll, "mode": "suggest", "confirmations": 4}
  }
  t.session.flush()

  assert read_default(t.session, t.agent.metadata_).element_id == t.payroll
  assert read_default(t.session, t.agent.metadata_, t.sub.id) is None
  sub_waiting = _sub_line(t)
  out = apply_ladder(t.session, dict(sub_waiting.metadata_), t.agent.id, t.sub.id)
  assert out["suggestion_source"] == "tier0"

  _commit(t, _sub_line(t), classified_element_id=t.sub_payroll)

  t.session.refresh(t.agent)
  assert "classification" not in t.agent.metadata_
  parent = read_default(t.session, t.agent.metadata_)
  assert (parent.element_id, parent.confirmations) == (t.payroll, 4)
  response = agent_to_response(t.agent)
  assert response.classification is not None
  assert response.classification.entity_id == t.parent.id
  assert {(c.entity_id, c.element_id) for c in response.classifications} == {
    (t.parent.id, t.payroll),
    (t.sub.id, t.sub_payroll),
  }


def test_a_default_set_by_hand_for_a_subsidiary_takes_an_account_on_its_chart(
  sub_books,
):
  t = sub_books
  parent_waiting = _line(t)
  sub_waiting = _sub_line(t)
  t.session.commit()

  with pytest.raises(ValueError, match="not on the chart"):
    update_agent(
      t.session,
      UpdateAgentRequest(
        agent_id=t.agent.id,
        classification_element_id=t.payroll,
        classification_entity_id=t.sub.id,
      ),
      "usr_1",
    )
  t.session.rollback()

  update_agent(
    t.session,
    UpdateAgentRequest(
      agent_id=t.agent.id,
      classification_element_id=t.sub_payroll,
      classification_entity_id=t.sub.id,
    ),
    "usr_1",
  )
  t.session.refresh(parent_waiting)
  t.session.refresh(sub_waiting)
  assert parent_waiting.metadata_["suggested_element_id"] == t.software
  assert sub_waiting.metadata_["suggested_element_id"] == t.sub_payroll


def test_history_learns_each_entitys_default_from_its_own_lines(sub_books):
  t = sub_books
  _line(t, status="committed", classified_element_id=t.payroll)
  for _ in range(2):
    _sub_line(t, status="committed", classified_element_id=t.sub_payroll)
  t.session.commit()

  learned = learn_classification_defaults(
    t.session, LearnClassificationDefaultsRequest(), "usr_1"
  )

  assert learned.agents_learned == 2
  t.session.refresh(t.agent)
  assert read_default(t.session, t.agent.metadata_).element_id == t.payroll
  sub = read_default(t.session, t.agent.metadata_, t.sub.id)
  assert (sub.element_id, sub.confirmations) == (t.sub_payroll, 2)


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


def _captured_through_the_api(t, **overrides) -> Event:
  body = CreateEventBlockRequest(
    **{
      "event_type": "bank_transaction",
      "event_category": "treasury",
      "source": "manual",
      "occurred_at": datetime(2026, 10, 9, tzinfo=UTC),
      "amount": -4_500,
      "agent_id": t.agent.id,
      "resource_type": "money",
      "resource_element_id": t.cash,
      **overrides,
    }
  )
  event, _ = create_event_block_in_session(t.session, body, "usr_1", graph_id="kg_test")
  return event


def test_a_line_captured_through_the_api_gets_the_learned_suggestion(feed):
  _commit(feed, _line(feed, status="classified"), classified_element_id=feed.payroll)

  event = _captured_through_the_api(feed)

  assert event.status == "captured"
  assert event.metadata_["suggested_element_id"] == feed.payroll
  assert event.metadata_["suggestion_source"] == "agent_default"


def test_a_line_with_no_counterparty_or_no_default_is_left_as_sent(feed):
  unknown = _captured_through_the_api(feed, agent_id=None, metadata={"note": "x"})
  assert unknown.metadata_ == {"note": "x"}

  no_default = _captured_through_the_api(feed)
  assert "suggested_element_id" not in no_default.metadata_
  assert "suggestion_source" not in no_default.metadata_


@pytest.fixture()
def payable(feed):
  """The feed's books with a payables control account."""
  feed.payables = entity_account(feed.session, feed.parent.id, "Accounts Payable")
  feed.session.get(Element, feed.payables).metadata_ = {
    "account_type": "Accounts Payable"
  }
  feed.session.commit()
  return feed


def _open_bill(t, *, amount=4_500, day=5) -> Event:
  bill = Event(
    entity_id=t.parent.id,
    event_type="bill_received",
    event_category="purchase",
    event_class="economic",
    agent_id=t.agent.id,
    occurred_at=datetime(2026, 10, day, tzinfo=UTC),
    effective_at=datetime(2026, 10, day, tzinfo=UTC),
    status="fulfilled",
    source="manual",
    amount=amount,
    description="October subscription",
    created_by="test",
  )
  t.session.add(bill)
  t.session.flush()
  return bill


def test_a_line_that_pays_an_open_bill_exactly_is_suggested_as_its_settlement(payable):
  _commit(
    payable, _line(payable, status="classified"), classified_element_id=payable.payroll
  )
  bill = _open_bill(payable)

  event = _captured_through_the_api(payable)

  assert event.metadata_["suggestion_source"] == "open_item"
  assert event.metadata_["suggested_element_id"] == payable.payables
  assert event.metadata_["suggested_discharges_event_id"] == bill.id
  assert "open bill of $45.00 dated 2026-10-05" in event.metadata_["suggestion_basis"]


def test_another_amount_falls_back_to_the_learned_default(payable):
  _commit(
    payable, _line(payable, status="classified"), classified_element_id=payable.payroll
  )
  _open_bill(payable, amount=9_900)

  event = _captured_through_the_api(payable)

  assert event.metadata_["suggestion_source"] == "agent_default"
  assert event.metadata_["suggested_element_id"] == payable.payroll
  assert "suggested_discharges_event_id" not in event.metadata_


def test_a_bill_dated_after_the_line_is_not_what_it_pays(payable):
  _open_bill(payable, day=20)

  event = _captured_through_the_api(payable)

  assert "suggestion_source" not in event.metadata_


def test_taking_the_suggestion_settles_the_bill(payable):
  bill = _open_bill(payable)
  event = _captured_through_the_api(payable)

  _settle_the_suggested_item(payable.session, event)

  assert event.discharges_event_id == bill.id


def test_a_bill_entered_later_resuggests_the_waiting_line(payable):
  waiting = _captured_through_the_api(payable)
  assert "suggestion_source" not in waiting.metadata_

  bill = _open_bill(payable)
  _resuggest_after_obligation(payable.session, bill)

  payable.session.refresh(waiting)
  assert waiting.metadata_["suggestion_source"] == "open_item"
  assert waiting.metadata_["suggested_discharges_event_id"] == bill.id


def test_a_settling_line_teaches_nothing(payable):
  bill = _open_bill(payable)
  line = _line(payable, status="classified")
  line.discharges_event_id = bill.id

  _commit(payable, line, classified_element_id=payable.payables)

  assert _default(payable) is None


def test_always_ask_keeps_the_open_item_out(payable):
  _commit(
    payable, _line(payable, status="classified"), classified_element_id=payable.payroll
  )
  update_agent(
    payable.session,
    UpdateAgentRequest(agent_id=payable.agent.id, classification_mode="always_ask"),
    "usr_1",
  )
  _open_bill(payable)

  event = _captured_through_the_api(payable)

  assert event.metadata_.get("suggestion_source") != "open_item"


def test_a_bill_one_line_has_settled_is_no_longer_offered_to_the_next(payable):
  bill = _open_bill(payable)
  first = _captured_through_the_api(payable)
  second = _captured_through_the_api(payable)
  assert second.metadata_["suggested_discharges_event_id"] == bill.id

  _settle_the_suggested_item(payable.session, first)
  first.status = "committed"
  _resuggest_after_settlement(payable.session, first)

  payable.session.refresh(second)
  assert second.metadata_.get("suggestion_source") != "open_item"
  assert "suggested_discharges_event_id" not in second.metadata_


def test_a_suggested_settlement_is_not_classified_to_another_account(payable):
  _open_bill(payable)
  event = _captured_through_the_api(payable)

  with pytest.raises(HandlerMetadataValidationError, match="not_a_settlement"):
    _refuse_overlooking_the_open_item(
      payable.session, event, {"classified_element_id": payable.software}
    )
  with pytest.raises(HandlerMetadataValidationError, match="open bill of"):
    _refuse_overlooking_the_open_item(
      payable.session,
      event,
      {"classified_allocations": [{"element_id": payable.software, "amount": 4_500}]},
    )


def test_the_suggested_account_a_stated_exception_or_a_link_is_let_through(payable):
  bill = _open_bill(payable)
  event = _captured_through_the_api(payable)

  _refuse_overlooking_the_open_item(payable.session, event, {"accept_suggestion": True})
  _refuse_overlooking_the_open_item(payable.session, event, {"basis": "a memo"})
  _refuse_overlooking_the_open_item(
    payable.session, event, {"classified_element_id": payable.payables}
  )

  event.metadata_ = {**event.metadata_, "not_a_settlement": True}
  _refuse_overlooking_the_open_item(
    payable.session, event, {"classified_element_id": payable.software}
  )

  # A link the caller made to some other item is their own settlement.
  event.metadata_ = {**event.metadata_, "not_a_settlement": False}
  event.discharges_event_id = "evt_another_item"
  _refuse_overlooking_the_open_item(
    payable.session, event, {"classified_element_id": payable.software}
  )

  # The suggested item's own link is not: the account still has to be its.
  event.discharges_event_id = bill.id
  with pytest.raises(HandlerMetadataValidationError, match="not_a_settlement"):
    _refuse_overlooking_the_open_item(
      payable.session, event, {"classified_element_id": payable.software}
    )


def test_a_line_with_a_learned_suggestion_takes_any_account(payable):
  _commit(
    payable, _line(payable, status="classified"), classified_element_id=payable.payroll
  )
  event = _captured_through_the_api(payable)

  _refuse_overlooking_the_open_item(
    payable.session, event, {"classified_element_id": payable.software}
  )


def test_a_line_far_larger_than_the_counterpartys_posted_ones_says_so(feed):
  for _ in range(2):
    _commit(feed, _line(feed, status="classified"), classified_element_id=feed.payroll)

  usual = _captured_through_the_api(feed, amount=-300_000)
  unusual = _captured_through_the_api(feed, amount=-900_000)

  assert "largest" not in usual.metadata_["suggestion_basis"]
  assert unusual.metadata_["suggested_element_id"] == feed.payroll
  assert (
    "This line is $9,000.00; the largest of" in unusual.metadata_["suggestion_basis"]
  )
  assert "2 posted lines was $1,200.00" in unusual.metadata_["suggestion_basis"]
  assert unusual.metadata_["suggestion_caution"] == "unusual_amount"
  assert "suggestion_caution" not in usual.metadata_


def test_one_posted_line_is_too_little_to_call_another_unusual(feed):
  _commit(feed, _line(feed, status="classified"), classified_element_id=feed.payroll)

  event = _captured_through_the_api(feed, amount=-900_000)

  assert "largest" not in event.metadata_["suggestion_basis"]
