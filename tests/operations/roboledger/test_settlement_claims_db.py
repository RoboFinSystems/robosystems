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


ACCEPT = {"accept_suggestion": True}


def _debits(t, element_id: str) -> int:
  return int(
    t.session.execute(
      text(
        "SELECT coalesce(sum(debit_amount), 0) FROM line_items WHERE element_id = :id"
      ),
      {"id": element_id},
    ).scalar()
  )


def _give_the_vendor_a_default(t) -> None:
  from robosystems.operations.roboledger.classification import set_default

  set_default(
    t.session,
    t.session.get(Agent, t.agent.id),
    element_id=t.software,
    mode=None,
    set_by="usr_1",
    entity_id=t.parent.id,
  )
  t.session.commit()


def test_a_bill_paid_by_a_later_dated_line_is_not_offered_to_an_earlier_one(books):
  """The newer line is taken and posted first. A bill is paid whatever the
  payment's date: the older line must not be handed it back."""
  t = books
  _bill(t, day=5)
  older, newer = _line(t, 9), _line(t, 20)

  _update(t, newer, transition_to="committed", metadata_patch=ACCEPT)

  assert SETTLES not in _event(t, older).metadata_
  out = accept_suggestions(
    t.session,
    AcceptSuggestionsRequest(entity_id=t.parent.id, commit=True),
    "usr_1",
    graph_id=GRAPH_ID,
  )
  assert out.lines == []
  assert _payables_debits(t) == 4_500
  assert _open_payables(t) == []


@pytest.mark.parametrize("release", ["void", "clear_link"])
def test_a_claim_that_goes_away_gives_the_offer_back(books, release):
  """Otherwise the waiting line posts to the vendor's default account and the
  bill it really pays stays open."""
  t = books
  _give_the_vendor_a_default(t)
  bill = _bill(t)
  claimer, waiting = _line(t, 9), _line(t, 20)
  _update(t, claimer, transition_to="classified", metadata_patch=ACCEPT)
  assert SETTLES not in _event(t, waiting).metadata_

  if release == "void":
    _update(t, claimer, transition_to="voided")
  else:
    _update(t, claimer, discharges_event_id="")

  assert _event(t, waiting).metadata_[SETTLES] == bill


def test_taking_the_suggestion_after_all_posts_to_its_account(books):
  """Called a separate purchase, then taken as the settlement: the earlier
  answer goes, so the line does not carry the link to another account."""
  t = books
  bill = _bill(t)
  line = _line(t, 9)
  _update(
    t,
    line,
    transition_to="classified",
    metadata_patch={"classified_element_id": t.software, "not_a_settlement": True},
  )

  _update(t, line, metadata_patch=ACCEPT)
  assert "classified_element_id" not in _event(t, line).metadata_
  assert "not_a_settlement" not in _event(t, line).metadata_
  _update(t, line, transition_to="committed")

  assert _event(t, line).discharges_event_id == bill
  assert _debits(t, t.software) == 0
  assert _payables_debits(t) == 4_500
  assert _open_payables(t) == []


def test_calling_it_a_separate_purchase_withdraws_the_acceptance(books):
  t = books
  _bill(t)
  line = _line(t, 9)
  _update(t, line, transition_to="classified", metadata_patch=ACCEPT)

  _update(
    t,
    line,
    metadata_patch={"classified_element_id": t.software, "not_a_settlement": True},
  )

  assert "accept_suggestion" not in _event(t, line).metadata_
  assert _event(t, line).discharges_event_id is None


def test_a_posted_line_keeps_the_bill_its_entry_paid(books):
  """Its entry debited payables; clearing the link would reopen a paid bill
  and offer it to the next line."""
  t = books
  bill = _bill(t)
  paid, waiting = _line(t, 9), _line(t, 20)
  _update(t, paid, transition_to="committed", metadata_patch=ACCEPT)

  _update(
    t,
    paid,
    metadata_patch={"classified_element_id": t.software, "not_a_settlement": True},
  )

  assert _event(t, paid).discharges_event_id == bill
  assert _open_payables(t) == []
  assert SETTLES not in _event(t, waiting).metadata_


def test_two_callers_at_once_do_not_both_take_the_bill(books):
  """The second waits on the bill's row and then finds it claimed."""
  import threading

  from sqlalchemy.orm import sessionmaker

  from robosystems.operations.locking import RowLockedError

  t = books
  bill = _bill(t)
  first, second = _line(t, 9), _line(t, 20)
  schema = t.session.execute(text("SHOW search_path")).scalar()
  maker = sessionmaker(bind=t.session.get_bind())
  refused: list[BaseException] = []

  def take(event_id: str) -> None:
    session = maker()
    session.execute(text(f"SET search_path TO {schema}"))
    try:
      update_event_block(
        session,
        UpdateEventBlockRequest(
          event_id=event_id, transition_to="classified", metadata_patch=ACCEPT
        ),
        "usr_1",
        graph_id=GRAPH_ID,
      )
    except (HandlerMetadataValidationError, RowLockedError) as error:
      refused.append(error)
      session.rollback()
    finally:
      session.close()

  threads = [threading.Thread(target=take, args=(e,)) for e in (first, second)]
  for thread in threads:
    thread.start()
  for thread in threads:
    thread.join(60)

  linked = [_event(t, e).discharges_event_id for e in (first, second)]
  assert sorted(linked, key=str) == sorted([bill, None], key=str)
  assert len(refused) == 1


def test_a_line_linked_before_the_item_was_paid_is_refused_at_posting(books):
  """A link made on a suggestion can be older than this rule: posting is where
  a second payment of the same bill is last caught."""
  t = books
  bill = _bill(t)
  first, second = _line(t, 9), _line(t, 20)
  _update(t, first, transition_to="classified", metadata_patch=ACCEPT)
  # As two lines linked before the claim was counted would stand.
  stale = _event(t, second)
  stale.metadata_ = dict(_event(t, first).metadata_)
  stale.discharges_event_id = bill
  stale.status = "classified"
  t.session.commit()

  with pytest.raises(HandlerMetadataValidationError, match="no longer open"):
    _update(t, second, transition_to="committed")
  t.session.rollback()
  assert _payables_debits(t) == 0


def test_a_line_just_past_midnight_elsewhere_takes_its_own_suggestion(books):
  """The day a line falls on depends on whose clock reads it; whether its
  bill is still unpaid does not."""
  from datetime import timedelta, timezone

  t = books
  at = datetime(2026, 10, 6, 0, 0, tzinfo=UTC)
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
    created_by="test",
  )
  t.session.add(bill)
  t.session.commit()
  bill_id = str(bill.id)
  body = CreateEventBlockRequest(
    event_type="bank_transaction",
    event_category="treasury",
    source="manual",
    occurred_at=datetime(2026, 10, 6, 0, 30, tzinfo=timezone(timedelta(hours=2))),
    amount=-4_500,
    agent_id=t.agent.id,
    resource_type="money",
    resource_element_id=t.cash,
  )
  event, _ = create_event_block_in_session(t.session, body, "usr_1", graph_id=GRAPH_ID)
  t.session.commit()
  line = str(event.id)
  assert _event(t, line).metadata_.get(SETTLES) == bill_id

  _update(t, line, transition_to="classified", metadata_patch=ACCEPT)

  assert _event(t, line).discharges_event_id == bill_id


def test_a_posted_line_no_longer_takes_a_suggestion(books):
  """Its entry already says where it went; a link made now would mark a bill
  paid that the entry never touched."""
  t = books
  _bill(t)
  line = _line(t, 9)
  _update(
    t,
    line,
    transition_to="committed",
    metadata_patch={"classified_element_id": t.software, "not_a_settlement": True},
  )

  with pytest.raises(HandlerMetadataValidationError, match="is posted"):
    _update(t, line, metadata_patch=ACCEPT)
  t.session.rollback()

  assert _event(t, line).discharges_event_id is None
  assert _event(t, line).metadata_["classified_element_id"] == t.software
  assert _open_payables(t) == [4_500]


def test_allocating_what_is_left_of_the_suggested_bill_posts(books):
  """Allocation rows say what the line settles; part of the suggested bill
  having been paid another way does not stop the rest being applied to it."""
  from robosystems.models.api.event_block import DischargeAllocation

  t = books
  bill = _bill(t, day=5)
  at = datetime(2026, 10, 6, tzinfo=UTC)
  other = Event(
    entity_id=t.parent.id,
    event_type="bill_received",
    event_category="purchase",
    event_class="economic",
    agent_id=t.agent.id,
    occurred_at=at,
    effective_at=at,
    status="fulfilled",
    source="manual",
    amount=1_000,
    created_by="test",
  )
  paid_elsewhere = Event(
    entity_id=t.parent.id,
    event_type="bill_paid",
    event_category="purchase",
    event_class="economic",
    agent_id=t.agent.id,
    occurred_at=at,
    effective_at=at,
    status="committed",
    source="quickbooks",
    amount=1_000,
    discharges_event_id=bill,
    created_by="test",
  )
  t.session.add_all([other, paid_elsewhere])
  t.session.commit()
  other_id = str(other.id)
  line = _line(t, 9)

  _update(
    t,
    line,
    transition_to="committed",
    metadata_patch={"classified_element_id": t.payables, "classified_by": "user"},
    discharge_allocations=[
      DischargeAllocation(obligation_event_id=bill, amount=3_500),
      DischargeAllocation(obligation_event_id=other_id, amount=1_000),
    ],
  )

  assert _event(t, line).status == "committed"
  assert _open_payables(t) == []
