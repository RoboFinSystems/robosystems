"""A native obligation's due date: stated, or as net terms, and correctable.

An `invoice_issued` or a `bill_received` takes `due_date` or `terms_days`
(net N from `occurred_at`), and its handler copies the date onto the
transaction it writes. A due date moves no ledger row, so it is corrected on
`update-event-block` after the bill has posted, the transaction following. No
other event type carries one.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.event_block import (
  CreateEventBlockRequest,
  UpdateEventBlockRequest,
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger import Event, Transaction
from robosystems.operations.event_block.commands import (
  InvalidEventTransitionError,
  create_event_block_in_session,
  update_event_block,
)
from tests.ledger_entity import seed_parent_entity

pytestmark = pytest.mark.unit

GRAPH_ID = "kg_test"
_COMMANDS = "robosystems.operations.event_block.commands"


@pytest.fixture()
def session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_due_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
  db = sessionmaker(bind=engine)()
  db.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=db.connection())
  seed_parent_entity(db)
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
def _platform_checks(monkeypatch):
  monkeypatch.setattr(f"{_COMMANDS}._validate_event_source", lambda s, g: None)
  monkeypatch.setattr(f"{_COMMANDS}._validate_routed_connection", lambda m, g: None)


def _accounts(session) -> tuple[str, str]:
  ids = []
  for code, name in (("6100", "Repairs"), ("2000", "Accounts Payable")):
    element = Element(name=name, code=code, created_by="t")
    session.add(element)
    session.flush()
    ids.append(str(element.id))
  return ids[0], ids[1]


def _bill(session, **due) -> Event:
  expense, payable = _accounts(session)
  event, _ = create_event_block_in_session(
    session,
    CreateEventBlockRequest(
      event_type="bill_received",
      event_category="purchase",
      event_class="economic",
      source="manual",
      occurred_at=datetime(2026, 10, 1),
      amount=-420000,
      apply_handlers=True,
      metadata={
        "posting_date": "2026-10-01",
        "memo": "Acme repairs",
        "line_items": [
          {"element_id": expense, "debit_amount": 420000, "credit_amount": 0},
          {"element_id": payable, "debit_amount": 0, "credit_amount": 420000},
        ],
      },
      **due,
    ),
    "user_1",
    graph_id=GRAPH_ID,
  )
  session.flush()
  return event


def _transaction_due(session, event: Event) -> date | None:
  return session.execute(
    select(Transaction.due_date).where(Transaction.triggered_by_event_id == event.id)
  ).scalar_one()


def test_net_terms_set_the_due_date_on_the_bill_and_its_transaction(session):
  event = _bill(session, terms_days=30)

  assert event.due_date == date(2026, 10, 31)
  assert _transaction_due(session, event) == date(2026, 10, 31)


def test_a_bill_with_no_terms_has_no_due_date(session):
  """Due on receipt: the aged read treats a null as due on `occurred_at`."""
  event = _bill(session)

  assert event.due_date is None


def test_an_extension_is_a_field_correction_the_transaction_follows(session):
  event = _bill(session, due_date=date(2026, 10, 30))
  event.status = "committed"
  session.flush()

  update_event_block(
    session,
    UpdateEventBlockRequest(event_id=str(event.id), due_date=date(2026, 11, 30)),
    "user_1",
    graph_id=GRAPH_ID,
  )
  session.flush()
  session.refresh(event)

  assert event.due_date == date(2026, 11, 30)
  assert _transaction_due(session, event) == date(2026, 11, 30)


def test_only_an_obligation_takes_a_due_date(session):
  with pytest.raises(ValidationError, match="obligation"):
    CreateEventBlockRequest(
      event_type="bank_transaction",
      event_category="purchase",
      event_class="economic",
      source="manual",
      occurred_at=datetime(2026, 10, 1),
      due_date=date(2026, 10, 31),
    )
  with pytest.raises(ValidationError, match="not both"):
    CreateEventBlockRequest(
      event_type="bill_received",
      event_category="purchase",
      event_class="economic",
      source="manual",
      occurred_at=datetime(2026, 10, 1),
      due_date=date(2026, 10, 31),
      terms_days=30,
    )

  event, _ = create_event_block_in_session(
    session,
    CreateEventBlockRequest(
      event_type="journal_entry_recorded",
      event_category="adjustment",
      event_class="economic",
      source="manual",
      occurred_at=datetime(2026, 10, 1),
    ),
    "user_1",
    graph_id=GRAPH_ID,
  )
  session.flush()
  with pytest.raises(InvalidEventTransitionError, match="due date"):
    update_event_block(
      session,
      UpdateEventBlockRequest(event_id=str(event.id), due_date=date(2026, 10, 31)),
      "user_1",
      graph_id=GRAPH_ID,
    )
