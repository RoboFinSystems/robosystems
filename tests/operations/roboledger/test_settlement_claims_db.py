"""An open bill is settled once: by the first bank line that takes it.

Real Postgres. A waiting line that names a bill has claimed it, so no other
line is offered it or may take it on an old suggestion, and a line re-pointed
to another account gives the bill back.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import text

from robosystems.models.api.event_block import (
  CreateEventBlockRequest,
  UpdateEventBlockRequest,
)
from robosystems.models.api.extensions.agent import AcceptSuggestionsRequest
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger import Agent, Event
from robosystems.operations.event_block.commands import (
  HandlerMetadataValidationError,
  create_event_block_in_session,
  update_event_block,
)
from robosystems.operations.roboledger.commands.suggestions import accept_suggestions
from robosystems.operations.roboledger.reads.ar_ap import list_open_items
from tests.ledger_entity import entity_account
from tests.operations.roboledger.conftest import (  # noqa: F401
  tenant_session,
  two_entities,
)

pytestmark = pytest.mark.unit

GRAPH_ID = "kg5e771e0000000000aa"
SETTLES = "suggested_discharges_event_id"


@pytest.fixture()
def books(two_entities):  # noqa: F811
  t = two_entities
  entity = t.parent.id
  t.cash = entity_account(t.session, entity, "Checking")
  t.software = entity_account(t.session, entity, "Software")
  t.payables = entity_account(t.session, entity, "Accounts Payable")
  t.session.get(Element, t.payables).metadata_ = {"account_type": "Accounts Payable"}
  t.agent = Agent(
    agent_type="vendor",
    name="Gusto",
    source="plaid",
    external_id="gusto",
    connection_id="conn_1",
    created_by="test",
  )
  t.session.add(t.agent)
  t.session.commit()
  return t


def _bill(t, day: int = 5) -> str:
  at = datetime(2026, 10, day, tzinfo=UTC)
  bill = Event(
    entity_id=t.parent.id,
    event_type="bill_received",
    event_category="purchase",
    event_class="economic",
    agent_id=t.agent.id,
    occurred_at=at,
    effective_at=at,
    status="fulfilled",
    source="manual",
    amount=4_500,
    description="October subscription",
    created_by="test",
  )
  t.session.add(bill)
  t.session.commit()
  return str(bill.id)


def _line(t, day: int) -> str:
  body = CreateEventBlockRequest(
    event_type="bank_transaction",
    event_category="treasury",
    source="manual",
    occurred_at=datetime(2026, 10, day, tzinfo=UTC),
    amount=-4_500,
    agent_id=t.agent.id,
    resource_type="money",
    resource_element_id=t.cash,
  )
  event, _ = create_event_block_in_session(t.session, body, "usr_1", graph_id=GRAPH_ID)
  t.session.commit()
  return str(event.id)


def _update(t, event_id: str, **fields) -> None:
  update_event_block(
    t.session,
    UpdateEventBlockRequest(event_id=event_id, **fields),
    "usr_1",
    graph_id=GRAPH_ID,
  )


def _event(t, event_id: str) -> Event:
  t.session.expire_all()
  return t.session.get(Event, event_id)


def _open_payables(t) -> list[int]:
  items = list_open_items(
    t.session, "payables", as_of=datetime(2026, 10, 31).date(), entity_id=t.parent.id
  )
  return [item.open_cents for item in items]


def _payables_debits(t) -> int:
  return int(
    t.session.execute(
      text(
        "SELECT coalesce(sum(li.debit_amount), 0) FROM line_items li "
        "JOIN elements el ON el.id = li.element_id WHERE el.id = :id"
      ),
      {"id": t.payables},
    ).scalar()
  )


def test_a_batch_gives_one_bill_to_one_line(books):
  t = books
  bill = _bill(t)
  first, second = _line(t, 9), _line(t, 20)
  assert _event(t, first).metadata_[SETTLES] == bill
  assert _event(t, second).metadata_[SETTLES] == bill

  out = accept_suggestions(
    t.session,
    AcceptSuggestionsRequest(entity_id=t.parent.id),
    "usr_1",
    graph_id=GRAPH_ID,
  )

  assert [line.outcome for line in out.lines] == ["classified", "skipped"]
  assert _event(t, first).discharges_event_id == bill
  assert _event(t, second).discharges_event_id is None
  assert SETTLES not in _event(t, second).metadata_
  _update(t, first, transition_to="committed")
  assert _payables_debits(t) == 4_500
  assert _open_payables(t) == []


def test_a_line_classified_first_keeps_the_bill_from_the_next(books):
  """One line at a time, nothing committed in between."""
  t = books
  bill = _bill(t)
  first, second = _line(t, 9), _line(t, 20)

  _update(
    t, first, transition_to="classified", metadata_patch={"accept_suggestion": True}
  )

  assert _event(t, first).discharges_event_id == bill
  assert SETTLES not in _event(t, second).metadata_


def test_an_old_suggestion_cannot_take_a_bill_another_line_holds(books):
  t = books
  _bill(t)
  first, second = _line(t, 9), _line(t, 20)
  stale = dict(_event(t, second).metadata_)
  _update(
    t, first, transition_to="classified", metadata_patch={"accept_suggestion": True}
  )
  # The second line's suggestion as a client read it before the first was taken.
  _event(t, second).metadata_ = stale
  t.session.commit()

  with pytest.raises(HandlerMetadataValidationError, match="no longer open"):
    _update(
      t, second, transition_to="classified", metadata_patch={"accept_suggestion": True}
    )
  t.session.rollback()
  assert _event(t, second).discharges_event_id is None


def test_two_bills_go_to_two_lines(books):
  t = books
  older, newer = _bill(t, day=3), _bill(t, day=6)
  first, second = _line(t, 9), _line(t, 20)

  out = accept_suggestions(
    t.session,
    AcceptSuggestionsRequest(entity_id=t.parent.id, commit=True),
    "usr_1",
    graph_id=GRAPH_ID,
  )

  assert [line.outcome for line in out.lines] == ["committed", "committed"]
  assert _event(t, first).discharges_event_id == older
  assert _event(t, second).discharges_event_id == newer
  assert _payables_debits(t) == 9_000
  assert _open_payables(t) == []


def test_repointing_a_line_that_took_the_bill_needs_the_word(books):
  t = books
  bill = _bill(t)
  line = _line(t, 9)
  _update(
    t, line, transition_to="classified", metadata_patch={"accept_suggestion": True}
  )

  with pytest.raises(HandlerMetadataValidationError, match="count it twice"):
    _update(t, line, metadata_patch={"classified_element_id": t.software})
  t.session.rollback()
  assert _event(t, line).discharges_event_id == bill


def test_a_line_that_is_not_the_settlement_gives_the_bill_back(books):
  t = books
  _bill(t)
  line, waiting = _line(t, 9), _line(t, 20)
  _update(
    t, line, transition_to="classified", metadata_patch={"accept_suggestion": True}
  )

  _update(
    t,
    line,
    transition_to="committed",
    metadata_patch={"classified_element_id": t.software, "not_a_settlement": True},
  )

  assert _event(t, line).discharges_event_id is None
  assert _payables_debits(t) == 0
  assert _open_payables(t) == [4_500]
  assert SETTLES in _event(t, waiting).metadata_
