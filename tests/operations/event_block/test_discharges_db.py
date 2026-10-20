"""What a payment settles, chosen by a person and checked on every write.

A bank line settles bills when money goes out and invoices when it comes in,
only its own entity's and none retracted, and never for more than it paid. Its
allocations split it across documents; a single link settles one at its full
magnitude. The candidates a line is offered are its counterparty's open items
at its date, an exact amount first, then the oldest due date.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.event_block import (
  DischargeAllocation,
  UpdateEventBlockRequest,
)
from robosystems.models.extensions import Element, Entity
from robosystems.models.extensions.roboledger.agent import Agent
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.event_discharge import EventDischarge
from robosystems.operations.event_block.commands import update_event_block
from robosystems.operations.event_block.discharges import (
  DischargeRefusedError,
  allocate,
  set_settlement_link,
)
from robosystems.operations.roboledger.reads.ar_ap import (
  list_discharge_candidates,
  list_open_items,
)
from tests.ledger_entity import PARENT_ENTITY_ID, entity_account, seed_parent_entity

pytestmark = pytest.mark.unit

SUB = "ent_sub"
ACME = "agt_acme"
GLOBEX = "agt_globex"
ON = date(2026, 9, 30)


@pytest.fixture()
def session():
  url = os.environ.get("TEST_DATABASE_URL")
  if not url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_dsc_{uuid.uuid4().hex[:12]}"
  engine = create_engine(url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
  db = sessionmaker(bind=engine)()
  db.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=db.connection())
  seed_parent_entity(db)
  db.add(
    Entity(
      id=SUB,
      name=SUB,
      is_parent=False,
      entity_role="operating",
      consolidation_method="full",
      source="native",
      created_by="t",
    )
  )
  db.add(Agent(id=ACME, agent_type="vendor", name="Acme", created_by="t"))
  db.add(Agent(id=GLOBEX, agent_type="vendor", name="Globex", created_by="t"))
  db.commit()
  db.execute(text(f'SET search_path TO "{schema}"'))
  try:
    yield db
  finally:
    db.rollback()
    db.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


@pytest.fixture(autouse=True)
def _no_platform(monkeypatch):
  monkeypatch.setattr(
    "robosystems.operations.event_block.commands._validate_routed_connection",
    lambda metadata, graph_id: None,
  )


def _event(
  session,
  event_type: str,
  amount: int,
  on: date = date(2026, 9, 1),
  *,
  due: date | None = None,
  entity: str = PARENT_ENTITY_ID,
  agent: str | None = ACME,
  status: str = "committed",
  metadata: dict | None = None,
) -> Event:
  event = Event(
    id=f"evt_{uuid.uuid4().hex[:12]}",
    entity_id=entity,
    event_type=event_type,
    event_category="purchase",
    occurred_at=datetime.combine(on, datetime.min.time()),
    due_date=due,
    source="native",
    amount=amount,
    agent_id=agent,
    status=status,
    metadata_=metadata or {},
    created_by="t",
  )
  session.add(event)
  session.flush()
  return event


def _bank_line(session, amount: int, **kwargs) -> Event:
  return _event(session, "bank_transaction", amount, ON, **kwargs)


def _open(session) -> dict[str, int]:
  return {
    item.event_id: item.open_cents
    for item in list_open_items(session, "payables", as_of=ON)
  }


def test_a_bank_line_splits_across_two_bills(session):
  first = _event(session, "bill_received", 420000)
  second = _event(session, "bill_received", 100000)
  line = _bank_line(session, -500000)

  allocate(
    session,
    line,
    [
      DischargeAllocation(obligation_event_id=first.id, amount=420000),
      DischargeAllocation(obligation_event_id=second.id, amount=80000),
    ],
    "usr",
  )
  session.flush()

  assert line.discharges_event_id == first.id
  assert _open(session) == {second.id: 20000}


def test_a_single_link_settles_at_the_lines_magnitude(session):
  bill = _event(session, "bill_received", 420000)
  line = _bank_line(session, -300000)

  set_settlement_link(session, line, bill.id)
  session.flush()

  assert _open(session) == {bill.id: 120000}


def test_a_single_link_replaces_allocations(session):
  first = _event(session, "bill_received", 420000)
  second = _event(session, "bill_received", 100000)
  line = _bank_line(session, -100000)
  allocate(
    session,
    line,
    [DischargeAllocation(obligation_event_id=first.id, amount=100000)],
    "usr",
  )
  session.flush()

  set_settlement_link(session, line, second.id)
  session.flush()

  assert session.execute(select(EventDischarge)).all() == []
  assert _open(session) == {first.id: 420000}


def test_an_empty_list_clears_what_the_line_settles(session):
  bill = _event(session, "bill_received", 420000)
  line = _bank_line(session, -420000)
  allocate(
    session,
    line,
    [DischargeAllocation(obligation_event_id=bill.id, amount=420000)],
    "usr",
  )
  session.flush()

  allocate(session, line, [], "usr")
  session.flush()

  assert line.discharges_event_id is None
  assert _open(session) == {bill.id: 420000}


@pytest.mark.parametrize(
  ("target", "reason"),
  [
    ({"event_type": "invoice_issued"}, "settles a bill_received"),
    ({"entity": SUB}, "another entity's books"),
    ({"status": "voided"}, "is voided"),
    ({"event_type": "bank_transaction"}, "not an invoice_issued or a bill_received"),
  ],
)
def test_money_out_settles_only_its_own_live_bills(session, target, reason):
  obligation = _event(
    session,
    target.get("event_type", "bill_received"),
    100000,
    entity=target.get("entity", PARENT_ENTITY_ID),
    status=target.get("status", "committed"),
  )
  line = _bank_line(session, -100000)

  with pytest.raises(DischargeRefusedError, match=reason):
    set_settlement_link(session, line, obligation.id)


def test_allocations_never_apply_more_than_the_line_paid(session):
  bill = _event(session, "bill_received", 420000)
  line = _bank_line(session, -100000)

  with pytest.raises(DischargeRefusedError, match="more than the payment's 100000"):
    allocate(
      session,
      line,
      [DischargeAllocation(obligation_event_id=bill.id, amount=100001)],
      "usr",
    )


def test_an_obligation_settles_nothing(session):
  bill = _event(session, "bill_received", 420000)
  other = _event(session, "bill_received", 100000)

  with pytest.raises(DischargeRefusedError, match="only a payment settles one"):
    set_settlement_link(session, other, bill.id)


def test_another_counterpartys_bill_may_be_settled(session):
  """A parent can pay its subsidiary's vendor, so a different agent is no
  refusal."""
  bill = _event(session, "bill_received", 100000, agent=GLOBEX)
  line = _bank_line(session, -100000)

  set_settlement_link(session, line, bill.id)

  assert line.discharges_event_id == bill.id


def test_the_update_command_records_the_choice(session):
  bill = _event(session, "bill_received", 420000)
  line = _bank_line(session, -420000)

  update_event_block(
    session,
    UpdateEventBlockRequest(
      event_id=line.id,
      discharge_allocations=[
        DischargeAllocation(obligation_event_id=bill.id, amount=420000)
      ],
    ),
    "usr",
    graph_id="kg_test",
  )

  assert _open(session) == {}


def test_the_update_command_refuses_a_bad_link(session):
  invoice = _event(session, "invoice_issued", 420000)
  line = _bank_line(session, -420000)

  with pytest.raises(DischargeRefusedError):
    update_event_block(
      session,
      UpdateEventBlockRequest(event_id=line.id, discharges_event_id=invoice.id),
      "usr",
      graph_id="kg_test",
    )


def test_candidates_put_an_exact_amount_first_then_the_oldest(session):
  old = _event(
    session, "bill_received", 100000, date(2026, 7, 1), due=date(2026, 7, 31)
  )
  newer = _event(
    session, "bill_received", 50000, date(2026, 8, 1), due=date(2026, 8, 31)
  )
  exact = _event(
    session, "bill_received", 420000, date(2026, 9, 1), due=date(2026, 10, 1)
  )
  _event(session, "bill_received", 420000, agent=GLOBEX)
  _event(session, "bill_received", 420000, date(2026, 10, 5))
  line = _bank_line(session, -420000)

  result = list_discharge_candidates(session, line.id)

  assert result is not None
  assert result.kind == "payables"
  assert result.amount_cents == 420000
  assert [c.event_id for c in result.candidates] == [exact.id, old.id, newer.id]


def test_money_in_is_offered_invoices(session):
  invoice = _event(session, "invoice_issued", 25000)
  _event(session, "bill_received", 25000)
  line = _bank_line(session, 25000)

  result = list_discharge_candidates(session, line.id)

  assert result is not None
  assert result.kind == "receivables"
  assert [c.event_id for c in result.candidates] == [invoice.id]


def test_candidates_say_when_the_line_is_classified_to_payables(session):
  payable = entity_account(session, PARENT_ENTITY_ID, "Accounts Payable")
  session.get(Element, payable).metadata_ = {"account_type": "Accounts Payable"}
  _event(session, "bill_received", 420000)
  to_payables = _bank_line(
    session, -420000, status="classified", metadata={"classified_element_id": payable}
  )
  elsewhere = _bank_line(
    session, -420000, status="classified", metadata={"classified_element_id": "x"}
  )

  assert list_discharge_candidates(session, to_payables.id).classified_to_control
  assert not list_discharge_candidates(session, elsewhere.id).classified_to_control


def test_a_committed_line_still_sees_what_it_settles(session):
  """Re-picking a line's settlement offers the bill it already pays."""
  bill = _event(session, "bill_received", 420000)
  line = _bank_line(session, -420000)
  allocate(
    session,
    line,
    [DischargeAllocation(obligation_event_id=bill.id, amount=420000)],
    "usr",
  )
  session.flush()

  result = list_discharge_candidates(session, line.id)

  assert result is not None
  assert [(c.event_id, c.open_cents) for c in result.candidates] == [(bill.id, 420000)]
  # Everywhere else the bill is settled.
  assert _open(session) == {}


@pytest.mark.parametrize("edit", ["link", "clear", "allocate", "clear_allocations"])
def test_a_quickbooks_payments_settlement_is_changed_in_quickbooks(session, edit):
  invoice = _event(session, "invoice_issued", 100000)
  payment = _event(session, "payment_received", 100000)
  payment.source = "quickbooks"
  session.flush()

  with pytest.raises(DischargeRefusedError, match="change it there"):
    if edit == "link":
      set_settlement_link(session, payment, invoice.id)
    elif edit == "clear":
      set_settlement_link(session, payment, "")
    elif edit == "allocate":
      allocate(
        session,
        payment,
        [DischargeAllocation(obligation_event_id=invoice.id, amount=100000)],
        "usr",
      )
    else:
      allocate(session, payment, [], "usr")


def test_no_event_no_candidates(session):
  assert list_discharge_candidates(session, "evt_missing") is None


def test_one_way_to_settle_per_update():
  with pytest.raises(ValueError, match="not both"):
    UpdateEventBlockRequest(
      event_id="evt_line",
      discharges_event_id="evt_bill",
      discharge_allocations=[
        DischargeAllocation(obligation_event_id="evt_bill", amount=1)
      ],
    )
