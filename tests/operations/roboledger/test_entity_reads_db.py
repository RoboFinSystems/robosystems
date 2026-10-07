"""The list reads that step 1 left reading the whole graph now read one
entity's books, default the group parent's; and the commands that take their
entity from the request body land on it. Real SQL against a tenant schema."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
)
from robosystems.models.api.extensions.taxonomies import LinkEntityTaxonomyRequest
from robosystems.models.extensions import Element
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.report import Report
from robosystems.operations.event_block.commands import (
  create_event_block,
  preview_event_block,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  create_journal_entry,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  EntityNotFoundError,
  link_entity_taxonomy,
)
from robosystems.operations.roboledger.entity_scope import EntityNotInGraphError
from robosystems.operations.roboledger.reads.ar_ap import (
  compute_open_receivables,
  list_open_receivables_by_agent,
)
from robosystems.operations.roboledger.reads.closing_book import (
  get_closing_book_structures,
)
from robosystems.operations.roboledger.reads.event_block import list_event_blocks
from robosystems.operations.roboledger.reads.reports import list_reports
from robosystems.operations.roboledger.reads.summary import get_ledger_counts
from robosystems.operations.roboledger.reads.transactions import list_transactions
from robosystems.operations.taxonomy_block.coa_mappings import entity_chart_id
from tests.ledger_entity import entity_account

pytestmark = pytest.mark.unit

GRAPH_ID = "kg0123456789abcdef09"
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


def _account(session, name: str, *, owner: str | None = None) -> str:
  """An account filed under no chart (the parent's), or in ``owner``'s chart."""
  if owner is not None:
    return entity_account(session, owner, name)
  element = Element(name=name, code=name[:8], created_by="usr_1")
  session.add(element)
  session.flush()
  return str(element.id)


def _recorded_event(debit: str, credit: str, *, entity_id: str | None = None):
  return CreateEventBlockRequest(
    entity_id=entity_id,
    event_type="journal_entry_recorded",
    event_category="adjustment",
    event_class="economic",
    source="manual",
    occurred_at=datetime(2026, 9, 15),
    amount=50_000,
    apply_handlers=True,
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


def _invoice(entity_id: str, cents: int) -> Event:
  return Event(
    entity_id=entity_id,
    event_type="invoice_issued",
    event_category="sales",
    event_class="economic",
    status="committed",
    occurred_at=datetime(2026, 9, 1, tzinfo=UTC).replace(tzinfo=None),
    source="manual",
    amount=cents,
    currency="USD",
    created_by="usr_1",
  )


@pytest.fixture()
def books(two_entities):
  """One recorded event in each entity's books, on each entity's own accounts;
  the parent's also posted so it has a trial balance."""
  t = two_entities
  parent_cash, parent_rent = _account(t.session, "Cash"), _account(t.session, "Rent")
  sub_cash = _account(t.session, "Cash", owner=t.sub.id)
  sub_rent = _account(t.session, "Rent", owner=t.sub.id)
  create_event_block(
    t.session, _recorded_event(parent_cash, parent_rent), "usr_1", graph_id=GRAPH_ID
  )
  create_event_block(
    t.session,
    _recorded_event(sub_cash, sub_rent, entity_id=t.sub.id),
    "usr_1",
    graph_id=GRAPH_ID,
  )
  create_journal_entry(
    t.session,
    CreateJournalEntryRequest(
      posting_date=POSTING_DATE,
      memo="Opening cash",
      status="posted",
      line_items=[
        JournalEntryLineItemInput(element_id=parent_cash, debit_amount=1_000),
        JournalEntryLineItemInput(element_id=parent_rent, credit_amount=1_000),
      ],
    ),
    "usr_1",
  )
  t.session.add_all([_invoice(t.parent.id, 10_000), _invoice(t.sub.id, 2_500)])
  t.session.commit()
  return t


class TestListReadsAreOneEntitys:
  def test_transactions(self, books):
    s = books.session
    # The parent's posted journal entry made a second transaction.
    assert list_transactions(s).pagination.total == 2
    assert list_transactions(s, entity_id=books.sub.id).pagination.total == 1
    rows = list_transactions(s, entity_id=books.sub.id).transactions
    assert {r.id for r in rows}.isdisjoint(
      {r.id for r in list_transactions(s).transactions}
    )
    with pytest.raises(EntityNotInGraphError):
      list_transactions(s, entity_id=books.linked.id)

  def test_counts(self, books):
    s = books.session
    parent, sub = get_ledger_counts(s), get_ledger_counts(s, books.sub.id)
    assert parent.entity_id == books.parent.id
    assert sub.entity_id == books.sub.id
    # The parent posted a journal entry on top of its recorded event.
    assert (parent.entry_count, sub.entry_count) == (2, 1)
    assert (parent.transaction_count, sub.transaction_count) == (2, 1)
    assert (parent.line_item_count, sub.line_item_count) == (4, 2)
    # Each entity's own chart; the parent's are the unfiled accounts.
    assert (parent.account_count, sub.account_count) == (2, 2)

  def test_event_blocks(self, books):
    s = books.session
    parent = list_event_blocks(s)
    sub = list_event_blocks(s, entity_id=books.sub.id)
    assert {e.event_type for e in parent} == {
      "journal_entry_recorded",
      "invoice_issued",
    }
    assert {e.event_type for e in sub} == {"journal_entry_recorded", "invoice_issued"}
    assert {e.id for e in parent}.isdisjoint({e.id for e in sub})

  def test_open_receivables(self, books):
    s = books.session
    assert compute_open_receivables(s).total_open_cents == 10_000
    assert compute_open_receivables(s, books.sub.id).total_open_cents == 2_500
    by_agent = list_open_receivables_by_agent(s, books.sub.id)
    assert [r.open_balance_cents for r in by_agent] == [2_500]

  def test_reports(self, books):
    s = books.session
    s.add_all(
      [
        Report(
          name="Parent FY",
          taxonomy_id="tax_x",
          entity_id=books.parent.id,
          created_by="u",
        ),
        Report(
          name="Sub FY", taxonomy_id="tax_x", entity_id=books.sub.id, created_by="u"
        ),
        # From before reports carried an entity, or shared in: the parent's.
        Report(name="Old FY", taxonomy_id="tax_x", created_by="u"),
      ]
    )
    s.commit()

    assert {r.name for r in list_reports(s).reports} == {"Parent FY", "Old FY"}
    assert {r.name for r in list_reports(s, entity_id=books.sub.id).reports} == {
      "Sub FY"
    }
    (sub_report,) = list_reports(s, entity_id=books.sub.id).reports
    assert sub_report.entity_id == books.sub.id

  def test_closing_book(self, books):
    s = books.session
    parent = [c.label for c in get_closing_book_structures(s).categories]
    sub = [c.label for c in get_closing_book_structures(s, books.sub.id).categories]
    # Only the parent has posted entries, so only it has a trial balance.
    assert "Trial Balance" in parent
    assert "Trial Balance" not in sub


class TestCommandsTakeTheBodysEntity:
  def test_an_event_lands_on_the_entity_the_body_names(self, two_entities):
    t = two_entities
    cash, rent = (
      _account(t.session, "Cash", owner=t.sub.id),
      _account(t.session, "Rent", owner=t.sub.id),
    )
    envelope = create_event_block(
      t.session,
      _recorded_event(cash, rent, entity_id=t.sub.id),
      "usr_1",
      graph_id=GRAPH_ID,
    )
    assert t.session.get(Event, envelope.id).entity_id == t.sub.id
    assert list_event_blocks(t.session) == []

  def test_an_explicit_entity_wins_over_the_body(self, two_entities):
    """Internal callers name the entity themselves; the body's is for the
    operation surface and never overrides them."""
    t = two_entities
    cash, rent = _account(t.session, "Cash"), _account(t.session, "Rent")
    envelope = create_event_block(
      t.session,
      _recorded_event(cash, rent, entity_id=t.sub.id),
      "usr_1",
      graph_id=GRAPH_ID,
      entity_id=t.parent.id,
    )
    assert t.session.get(Event, envelope.id).entity_id == t.parent.id

  def test_a_preview_is_checked_against_the_bodys_entity(self, two_entities):
    t = two_entities
    sub_cash, sub_rent = (
      _account(t.session, "Cash", owner=t.sub.id),
      _account(t.session, "Rent", owner=t.sub.id),
    )
    parent_cash, parent_rent = _account(t.session, "Cash"), _account(t.session, "Rent")

    ok = preview_event_block(
      t.session, _recorded_event(sub_cash, sub_rent, entity_id=t.sub.id), "usr_1"
    )
    crossed = preview_event_block(
      t.session, _recorded_event(parent_cash, parent_rent, entity_id=t.sub.id), "usr_1"
    )
    assert not ok.validation_errors
    assert crossed.validation_errors

  def test_a_taxonomy_links_to_the_entity_the_body_names(self, two_entities):
    t = two_entities
    _account(t.session, "Cash", owner=t.sub.id)
    chart_id = entity_chart_id(t.session, t.sub.id)
    assert chart_id is not None

    linked = link_entity_taxonomy(
      t.session,
      LinkEntityTaxonomyRequest(
        taxonomy_id=chart_id, basis="reporting", entity_id=t.sub.id
      ),
    )
    assert linked.entity_id == t.sub.id

    with pytest.raises(EntityNotFoundError):
      link_entity_taxonomy(
        t.session,
        LinkEntityTaxonomyRequest(
          taxonomy_id=chart_id, basis="reporting", entity_id=t.linked.id
        ),
      )
