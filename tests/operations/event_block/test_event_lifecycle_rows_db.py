"""An event's ledger rows across its lifecycle, against a real database.

A committed event with a handler writes its rows; one that skips the commit
must not leave the books with none; the template's preview plans what the
write writes; a reversal posts on the date its preview showed; and close posts
the transactions behind the entries it posts.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, date, datetime

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.event_block import (
  CreateEventBlockRequest,
  UpdateEventBlockRequest,
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event_handler import EventHandler
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.models.extensions.roboledger.transaction import Transaction
from robosystems.operations.event_block.commands import (
  InvalidEventTransitionError,
  create_event_block_in_session,
  preview_event_block,
  update_event_block,
)
from robosystems.operations.event_block.engine import EngineValidationError
from robosystems.operations.event_block.python_handlers.journal_entry_reversed import (
  JOURNAL_ENTRY_REVERSED_HANDLER,
)
from robosystems.operations.roboledger.fiscal_calendar.close_service import (
  _post_transactions_with_no_draft,
)
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity

pytestmark = pytest.mark.unit

GRAPH_ID = "kg_test"
JULY_END = date(2026, 7, 31)
DSL_EVENT_TYPE = "royalty_accrued"


@pytest.fixture()
def session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_lifecycle_{uuid.uuid4().hex[:12]}"
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
def _skip_platform_db_checks(monkeypatch):
  """Source and connection registration live in the platform database."""
  monkeypatch.setattr(
    "robosystems.operations.event_block.commands._validate_event_source",
    lambda source, graph_id: None,
  )
  monkeypatch.setattr(
    "robosystems.operations.event_block.commands._validate_routed_connection",
    lambda metadata, graph_id: None,
  )


@pytest.fixture()
def accounts(session) -> tuple[str, str]:
  ids = []
  for code, name in (("1200", "Royalties Receivable"), ("4300", "Royalty Revenue")):
    element = Element(name=name, code=code, created_by="test")
    session.add(element)
    session.flush()
    ids.append(str(element.id))
  session.add(
    FiscalPeriod(
      entity_id=PARENT_ENTITY_ID,
      graph_id=GRAPH_ID,
      name="2026-07",
      start_date=date(2026, 7, 1),
      end_date=JULY_END,
      period_type="month",
      status="open",
    )
  )
  session.flush()
  return ids[0], ids[1]


def _add_rule(session, debit: str, credit: str, amount: str) -> None:
  session.add(
    EventHandler(
      name="Royalty accrual",
      event_type=DSL_EVENT_TYPE,
      transaction_template={
        "transactions": [
          {
            "entry_template": {
              "debit": {"element_id": debit, "amount": amount},
              "credit": {"element_id": credit, "amount": amount},
            }
          }
        ]
      },
      created_at=datetime.now(UTC),
      updated_at=datetime.now(UTC),
      created_by="test",
    )
  )
  session.flush()


def _request(event_type: str = DSL_EVENT_TYPE, **overrides) -> CreateEventBlockRequest:
  fields = {
    "event_type": event_type,
    "event_category": "recognition",
    "event_class": "economic",
    "source": "manual",
    "occurred_at": datetime(2026, 7, 31),
    "amount": 1001,
    "apply_handlers": False,
    "metadata": {},
  }
  fields.update(overrides)
  return CreateEventBlockRequest(**fields)


def _capture(session, **overrides) -> str:
  event, _envelope = create_event_block_in_session(
    session, _request(**overrides), "user_test", graph_id=GRAPH_ID
  )
  session.flush()
  return str(event.id)


def _update(session, event_id: str, **fields):
  schema = session.execute(text("SELECT current_schema()")).scalar_one()
  try:
    return update_event_block(
      session,
      UpdateEventBlockRequest(event_id=event_id, **fields),
      "user_test",
      graph_id=GRAPH_ID,
    )
  finally:
    # The command commits, which hands the connection back to the pool.
    session.rollback()
    session.execute(text(f'SET search_path TO "{schema}"'))


def _entries(session, event_id: str) -> list[Entry]:
  return list(
    session.execute(select(Entry).where(Entry.triggered_by_event_id == event_id))
    .scalars()
    .all()
  )


def test_committing_a_template_only_event_writes_its_rows(session, accounts):
  receivable, revenue = accounts
  _add_rule(session, receivable, revenue, "{{ event.amount }}")
  event_id = _capture(session)

  envelope = _update(session, event_id, transition_to="committed")

  assert envelope.status == "committed"
  [entry] = _entries(session, event_id)
  lines = session.execute(
    select(LineItem).where(LineItem.entry_id == entry.id)
  ).scalars()
  assert sorted((li.debit_amount, li.credit_amount) for li in lines) == [
    (0, 1001),
    (1001, 0),
  ]


def test_a_capture_only_event_still_commits_with_no_rows(session, accounts):
  event_id = _capture(session, event_type="lead_logged")

  envelope = _update(session, event_id, transition_to="committed")

  assert envelope.status == "committed"
  assert _entries(session, event_id) == []


def test_a_classified_event_with_a_handler_cannot_skip_its_commit(session, accounts):
  receivable, revenue = accounts
  _add_rule(session, receivable, revenue, "{{ event.amount }}")
  event_id = _capture(session)
  _update(session, event_id, transition_to="classified")

  for target in ("fulfilled", "pending"):
    with pytest.raises(InvalidEventTransitionError, match="no ledger rows"):
      _update(session, event_id, transition_to=target)


def test_a_classified_event_with_no_handler_may_be_fulfilled(session, accounts):
  event_id = _capture(session, event_type="lead_logged")
  _update(session, event_id, transition_to="classified")

  envelope = _update(session, event_id, transition_to="fulfilled")

  assert envelope.status == "fulfilled"


def test_a_fractional_template_amount_rounds_and_the_preview_agrees(session, accounts):
  receivable, revenue = accounts
  # 1001 / 2 is 500.5 cents: floor division posted 500.
  _add_rule(session, receivable, revenue, "{{ event.amount }} / 2")

  preview = preview_event_block(session, _request(), "user_test")
  event_id = _capture(session)
  _update(session, event_id, transition_to="committed")

  [entry] = _entries(session, event_id)
  posted = session.execute(
    select(func.sum(LineItem.debit_amount)).where(LineItem.entry_id == entry.id)
  ).scalar_one()
  assert posted == 501
  assert preview.would_succeed
  assert [t.amount_cents for t in preview.planned_transactions] == [501]


def test_the_template_preview_refuses_what_the_write_refuses(session, accounts):
  receivable, revenue = accounts
  _add_rule(session, receivable, revenue, "-100")

  preview = preview_event_block(session, _request(), "user_test")

  assert not preview.would_succeed
  assert any("negative" in error for error in preview.validation_errors)


@pytest.mark.parametrize("amount", ["12.50", "Infinity", "1e30"])
def test_an_amount_that_is_not_whole_cents_is_refused_not_posted(
  session, accounts, amount
):
  receivable, revenue = accounts
  _add_rule(session, receivable, revenue, "{{ event.metadata.share }}")
  event_id = _capture(session, metadata={"share": amount})

  preview = preview_event_block(
    session, _request(metadata={"share": amount}), "user_test"
  )
  assert not preview.would_succeed
  with pytest.raises(EngineValidationError):
    _update(session, event_id, transition_to="committed")
  assert _entries(session, event_id) == []


def test_the_preview_reads_the_context_the_write_reads(session, accounts):
  receivable, revenue = accounts
  # Bank money-out is negative; a rule posts its magnitude from amount_abs.
  _add_rule(session, receivable, revenue, "{{ event.amount_abs }}")

  preview = preview_event_block(session, _request(amount=-2500), "user_test")

  assert preview.would_succeed, preview.validation_errors
  assert [t.amount_cents for t in preview.planned_transactions] == [2500]


def test_a_reversal_posts_on_the_date_its_preview_showed(session, accounts):
  receivable, revenue = accounts
  original, _ = create_event_block_in_session(
    session,
    _request(
      event_type="journal_entry_recorded",
      event_category="adjustment",
      apply_handlers=True,
      metadata={
        "posting_date": "2026-07-15",
        "memo": "Royalty true-up",
        "status": "posted",
        "line_items": [
          {"element_id": receivable, "debit_amount": 5000, "credit_amount": 0},
          {"element_id": revenue, "debit_amount": 0, "credit_amount": 5000},
        ],
      },
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  session.flush()
  [original_entry] = _entries(session, str(original.id))
  reversal = _request(
    event_type="journal_entry_reversed",
    event_category="adjustment",
    apply_handlers=True,
    metadata={"entry_id": str(original_entry.id)},
  )

  handler = JOURNAL_ENTRY_REVERSED_HANDLER
  preview = handler.dispatch_preview(
    session,
    reversal,
    handler.metadata_schema.model_validate(reversal.metadata),
    PARENT_ENTITY_ID,
  )
  event, _ = create_event_block_in_session(
    session, reversal, "user_test", graph_id=GRAPH_ID
  )
  session.flush()

  [reversing] = _entries(session, str(event.id))
  assert str(reversing.posting_date) == preview.planned_entries[0]["posting_date"]
  assert reversing.posting_date == JULY_END


def test_close_posts_a_transaction_once_none_of_its_entries_is_a_draft(
  session, accounts
):
  receivable, revenue = accounts
  _add_rule(session, receivable, revenue, "{{ event.amount }}")
  event_id = _capture(session)
  _update(session, event_id, transition_to="committed")
  [entry] = _entries(session, event_id)
  transaction_id = str(entry.transaction_id)
  later_draft = Entry(
    entity_id=entry.entity_id,
    transaction_id=transaction_id,
    type="standard",
    posting_date=date(2026, 8, 31),
    status="draft",
    created_by="test",
  )
  session.add(later_draft)
  entry.status = "posted"
  session.flush()
  now = datetime.now(UTC)

  _post_transactions_with_no_draft(session, [transaction_id], now)
  session.expire_all()
  assert session.get(Transaction, transaction_id).status == "pending"

  later_draft.status = "posted"
  session.flush()
  _post_transactions_with_no_draft(session, [transaction_id], now)
  session.expire_all()
  assert session.get(Transaction, transaction_id).status == "posted"
