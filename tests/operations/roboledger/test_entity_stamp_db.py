"""Every ledger row a writer produces carries the entity it was written for,
on a tenant with a parent, a native subsidiary and a linked counterparty.
Each path defaults to the parent and lands on the subsidiary when asked."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
  ReverseJournalEntryRequest,
)
from robosystems.models.api.extensions.schedules import (
  CreateScheduleRequest,
  EntryTemplateRequest,
)
from robosystems.models.extensions import Element, Structure
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.event_handler import EventHandler
from robosystems.models.extensions.roboledger.fact_set import FactSet
from robosystems.models.extensions.roboledger.transaction import Transaction
from robosystems.operations.event_block.commands import create_event_block_in_session
from robosystems.operations.event_block.promotion import promote_pending_obligations
from robosystems.operations.roboledger.commands.journal_entries import (
  create_journal_entry,
  reverse_journal_entry,
)
from robosystems.operations.roboledger.commands.schedules import create_schedule
from robosystems.operations.roboledger.entity_scope import EntityNotInGraphError
from robosystems.operations.roboledger.reconciliations.blocks import (
  create_account_reconciliation,
)
from robosystems.operations.roboledger.schedules import ScheduleService

pytestmark = pytest.mark.unit

GRAPH_ID = "kg0123456789abcdef07"
POSTING_DATE = date(2026, 9, 15)


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


def _account(session, name: str) -> str:
  element = Element(name=name, code=name[:8], created_by="usr_1")
  session.add(element)
  session.flush()
  return str(element.id)


def _journal_body(debit: str, credit: str, *, status: str = "draft"):
  return CreateJournalEntryRequest(
    posting_date=POSTING_DATE,
    memo="Rent received",
    status=status,
    line_items=[
      JournalEntryLineItemInput(element_id=debit, debit_amount=50_000),
      JournalEntryLineItemInput(element_id=credit, credit_amount=50_000),
    ],
  )


def _entities_of(session, model) -> set[str]:
  return {row[0] for row in session.query(model.entity_id).distinct()}


class TestJournalEntries:
  def test_an_entry_defaults_to_the_parent(self, two_entities):
    t = two_entities
    cash, rent = _account(t.session, "Cash"), _account(t.session, "Rent")

    created = create_journal_entry(t.session, _journal_body(cash, rent), "usr_1")

    entry = t.session.get(Entry, created.id)
    assert entry.entity_id == t.parent.id
    assert t.session.get(Transaction, entry.transaction_id).entity_id == t.parent.id

  def test_an_entry_lands_on_the_named_subsidiary(self, two_entities):
    t = two_entities
    cash, rent = _account(t.session, "Cash"), _account(t.session, "Rent")

    created = create_journal_entry(
      t.session, _journal_body(cash, rent), "usr_1", entity_id=t.sub.id
    )

    entry = t.session.get(Entry, created.id)
    assert entry.entity_id == t.sub.id
    assert t.session.get(Transaction, entry.transaction_id).entity_id == t.sub.id

  def test_a_reversal_stays_in_the_books_it_reverses(self, two_entities):
    t = two_entities
    cash, rent = _account(t.session, "Cash"), _account(t.session, "Rent")
    posted = create_journal_entry(
      t.session,
      _journal_body(cash, rent, status="posted"),
      "usr_1",
      entity_id=t.sub.id,
    )

    reversal = reverse_journal_entry(
      t.session,
      ReverseJournalEntryRequest(entry_id=posted.id, posting_date=POSTING_DATE),
      "usr_1",
    )

    assert t.session.get(Entry, reversal.id).entity_id == t.sub.id

  def test_a_linked_counterparty_has_no_books_here(self, two_entities):
    t = two_entities
    cash, rent = _account(t.session, "Cash"), _account(t.session, "Rent")

    with pytest.raises(EntityNotInGraphError):
      create_journal_entry(
        t.session, _journal_body(cash, rent), "usr_1", entity_id=t.linked.id
      )

    assert t.session.query(Entry).count() == 0


def _recorded_event(debit: str, credit: str, *, apply_handlers: bool):
  return CreateEventBlockRequest(
    event_type="journal_entry_recorded",
    event_category="adjustment",
    event_class="economic",
    source="manual",
    occurred_at=datetime(2026, 9, 15),
    amount=50_000,
    apply_handlers=apply_handlers,
    metadata={
      "posting_date": POSTING_DATE.isoformat(),
      "memo": "Rent received",
      "type": "standard",
      "status": "draft",
      "line_items": [
        {"element_id": debit, "debit_amount": 50_000, "credit_amount": 0},
        {"element_id": credit, "debit_amount": 0, "credit_amount": 50_000},
      ],
    },
  )


class TestEventBlocks:
  def test_a_captured_event_defaults_to_the_parent(self, two_entities):
    t = two_entities
    cash, rent = _account(t.session, "Cash"), _account(t.session, "Rent")

    event, _ = create_event_block_in_session(
      t.session,
      _recorded_event(cash, rent, apply_handlers=False),
      "usr_1",
      graph_id=GRAPH_ID,
    )

    assert event.entity_id == t.parent.id

  def test_a_handlers_rows_inherit_the_events_entity(self, two_entities):
    t = two_entities
    cash, rent = _account(t.session, "Cash"), _account(t.session, "Rent")

    event, _ = create_event_block_in_session(
      t.session,
      _recorded_event(cash, rent, apply_handlers=True),
      "usr_1",
      graph_id=GRAPH_ID,
      entity_id=t.sub.id,
    )

    assert event.entity_id == t.sub.id
    assert _entities_of(t.session, Entry) == {t.sub.id}
    assert _entities_of(t.session, Transaction) == {t.sub.id}

  def test_a_template_handlers_rows_inherit_the_events_entity(self, two_entities):
    t = two_entities
    cash, rent = _account(t.session, "Cash"), _account(t.session, "Rent")
    t.session.add(
      EventHandler(
        name="Rent received",
        event_type="rent_received",
        transaction_template={
          "transactions": [
            {
              "entry_template": {
                "debit": {"element_id": cash, "amount": "{{ event.amount }}"},
                "credit": {"element_id": rent, "amount": "{{ event.amount }}"},
              }
            }
          ]
        },
        created_by="usr_1",
      )
    )
    t.session.flush()

    event, _ = create_event_block_in_session(
      t.session,
      CreateEventBlockRequest(
        event_type="rent_received",
        event_category="sales",
        event_class="economic",
        source="manual",
        occurred_at=datetime(2026, 9, 15),
        amount=50_000,
        apply_handlers=True,
      ),
      "usr_1",
      graph_id=GRAPH_ID,
      entity_id=t.sub.id,
    )

    assert event.entity_id == t.sub.id
    assert _entities_of(t.session, Entry) == {t.sub.id}
    assert _entities_of(t.session, Transaction) == {t.sub.id}


def _depreciation(session, *, entity_id: str | None = None) -> str:
  debit = _account(session, "Depreciation Expense")
  credit = _account(session, "Accumulated Depreciation")
  created = create_schedule(
    session,
    CreateScheduleRequest(
      name="Depreciation",
      element_ids=[debit, credit],
      period_start=date(2026, 7, 1),
      period_end=date(2026, 9, 30),
      monthly_amount=10_000,
      entry_template=EntryTemplateRequest(
        debit_element_id=debit, credit_element_id=credit
      ),
    ),
    created_by="usr_1",
    entity_id=entity_id,
  )
  return created.structure_id


class TestSchedules:
  def test_a_schedule_defaults_to_the_parent(self, two_entities):
    t = two_entities

    structure_id = _depreciation(t.session)

    assert t.session.get(Structure, structure_id).entity_id == t.parent.id
    assert _entities_of(t.session, Event) == {t.parent.id}

  def test_a_subsidiarys_schedule_keeps_everything_in_its_books(self, two_entities):
    t = two_entities

    structure_id = _depreciation(t.session, entity_id=t.sub.id)
    promote_pending_obligations(
      t.session,
      GRAPH_ID,
      as_of=datetime(2026, 10, 1, tzinfo=UTC),
      dispatch_handlers=True,
    )

    assert t.session.get(Structure, structure_id).entity_id == t.sub.id
    assert _entities_of(t.session, Event) == {t.sub.id}
    fact_sets = t.session.query(FactSet).filter(FactSet.structure_id == structure_id)
    assert {fs.entity_id for fs in fact_sets} == {t.sub.id}
    entries = t.session.query(Entry).filter(Entry.source_structure_id == structure_id)
    assert entries.count() == 3
    assert {entry.entity_id for entry in entries} == {t.sub.id}

  def test_a_manual_closing_entry_lands_where_it_is_sent(self, two_entities):
    t = two_entities
    cash, rent = _account(t.session, "Cash"), _account(t.session, "Rent")
    lines = [
      {"element_id": cash, "debit_amount": 5_000, "credit_amount": 0},
      {"element_id": rent, "debit_amount": 0, "credit_amount": 5_000},
    ]
    service = ScheduleService()

    default = service.create_manual_closing_entry(
      t.session,
      posting_date=POSTING_DATE,
      line_items=lines,
      memo="Reclass",
      created_by="usr_1",
    )
    named = service.create_manual_closing_entry(
      t.session,
      posting_date=POSTING_DATE,
      line_items=lines,
      memo="Reclass",
      created_by="usr_1",
      entity_id=t.sub.id,
    )

    assert t.session.get(Entry, default.entry_id).entity_id == t.parent.id
    assert t.session.get(Entry, named.entry_id).entity_id == t.sub.id


def test_a_reconciliation_block_belongs_to_the_parent(two_entities):
  t = two_entities
  cash = _account(t.session, "Cash")

  structure = create_account_reconciliation(
    t.session,
    method="statement",
    element_id=cash,
    account_name="Cash",
    required_for_close=False,
    created_by="usr_1",
  )

  assert structure.entity_id == t.parent.id
