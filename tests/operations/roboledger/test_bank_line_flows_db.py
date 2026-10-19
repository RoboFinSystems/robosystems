"""A bank line's flow, on real Postgres: a second classification beside the
account that overrides its default flow on the cash flow and equity
statements, set when the line is classified or re-tagged once it posted."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from robosystems.models.api.event_block import UpdateEventBlockRequest
from robosystems.models.api.extensions.journal_entries import (
  JournalEntryLineFlow,
  UpdateJournalEntryRequest,
)
from robosystems.models.extensions import ElementTrait, Trait
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger import Entry, Event, LineItem
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.event_block.commands import update_event_block
from robosystems.operations.event_block.python_handlers.types import (
  HandlerMetadataValidationError,
)
from robosystems.operations.roboledger.commands._guards import (
  AccountOutsideEntityChartError,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  update_journal_entry,
)
from tests.ledger_entity import entity_account

pytestmark = pytest.mark.unit

GRAPH_ID = "kg0123456789abcdef0d"
NOTE_PROCEEDS = "rs-gaap:ProceedsFromIssuanceOfLongTermDebt"
CONTRIBUTIONS = "rs-gaap:ProceedsFromPartnershipContribution"


def _flow_concept(session, qname: str, activity: str) -> str:
  trait = (
    session.query(Trait)
    .filter(Trait.category == "activityType", Trait.identifier == activity)
    .first()
  )
  if trait is None:
    trait = Trait(category="activityType", identifier=activity)
    session.add(trait)
    session.flush()
  element = Element(
    name=qname.split(":")[1],
    qname=qname,
    balance_type="debit",
    period_type="duration",
    created_by="test",
  )
  session.add(element)
  session.flush()
  session.add(ElementTrait(element_id=element.id, trait_id=trait.id, is_primary=True))
  session.flush()
  return str(element.id)


@pytest.fixture()
def books(two_entities):
  t = two_entities
  entity = t.parent.id
  t.cash = entity_account(t.session, entity, "Checking")
  t.note = entity_account(t.session, entity, "Notes Payable")
  t.equity = entity_account(t.session, entity, "Members Equity")
  t.note_flow = _flow_concept(t.session, NOTE_PROCEEDS, "financingActivity")
  t.contribution_flow = _flow_concept(t.session, CONTRIBUTIONS, "financingActivity")
  t.session.commit()
  return t


def _deposit(t, cents: int = 1_000_000) -> Event:
  event = Event(
    entity_id=t.parent.id,
    event_type="bank_transaction",
    event_category="treasury",
    event_class="economic",
    resource_type="money",
    resource_element_id=t.cash,
    occurred_at=datetime(2026, 8, 12, tzinfo=UTC),
    effective_at=datetime(2026, 8, 12, tzinfo=UTC),
    status="captured",
    source="plaid",
    amount=cents,
    description="Transfer in from the note holder",
    metadata_={},
    created_by="test",
  )
  t.session.add(event)
  t.session.commit()
  return event


def _update(t, event: Event, **fields):
  return update_event_block(
    t.session,
    UpdateEventBlockRequest(event_id=str(event.id), **fields),
    "usr_1",
    graph_id=GRAPH_ID,
  )


def _lines(t, event: Event) -> list[LineItem]:
  entry = t.session.query(Entry).filter(Entry.triggered_by_event_id == event.id).one()
  return (
    t.session.query(LineItem)
    .filter(LineItem.entry_id == entry.id)
    .order_by(LineItem.line_order)
    .all()
  )


def test_a_flow_given_at_commit_tags_both_sides(books):
  event = _deposit(books)
  _update(
    books,
    event,
    transition_to="committed",
    metadata_patch={
      "classified_element_id": books.note,
      "classified_flow_qname": NOTE_PROCEEDS,
    },
  )

  lines = _lines(books, event)
  assert {str(li.element_id) for li in lines} == {books.cash, books.note}
  assert {str(li.flow_element_id) for li in lines} == {books.note_flow}


def _retag(t, event: Event, flows: dict[str, str | None]) -> None:
  lines = _lines(t, event)
  update_journal_entry(
    t.session,
    UpdateJournalEntryRequest(
      entry_id=str(lines[0].entry_id),
      line_flows=[
        JournalEntryLineFlow(line_id=str(li.id), flow_qname=flows[str(li.element_id)])
        for li in lines
        if str(li.element_id) in flows
      ],
    ),
  )
  t.session.commit()
  t.session.refresh(event)


def test_a_retag_of_its_entry_is_written_back_to_the_bank_line(books):
  event = _deposit(books)
  _update(
    books,
    event,
    transition_to="committed",
    metadata_patch={
      "classified_element_id": books.note,
      "classified_flow_qname": NOTE_PROCEEDS,
    },
  )

  _retag(books, event, {books.note: CONTRIBUTIONS})

  assert event.metadata_["classified_flow_qname"] == CONTRIBUTIONS
  assert {str(li.flow_element_id) for li in _lines(books, event)} == {
    books.contribution_flow
  }

  _retag(books, event, {books.note: None})

  assert "classified_flow_qname" not in event.metadata_


def test_a_retag_of_a_splits_entry_is_written_back_to_each_part(books):
  event = _deposit(books, cents=1_010_000)
  _update(
    books,
    event,
    transition_to="committed",
    metadata_patch={
      "classified_allocations": [
        {"element_id": books.note, "amount": 1_000_000, "flow_qname": NOTE_PROCEEDS},
        {"element_id": books.equity, "amount": 10_000, "flow_qname": CONTRIBUTIONS},
      ]
    },
  )

  _retag(books, event, {books.note: CONTRIBUTIONS})

  assert [
    (a["element_id"], a["flow_qname"])
    for a in event.metadata_["classified_allocations"]
  ] == [(books.note, CONTRIBUTIONS), (books.equity, CONTRIBUTIONS)]


def test_a_line_with_no_flow_stays_on_the_accounts_default(books):
  event = _deposit(books)
  _update(
    books,
    event,
    transition_to="committed",
    metadata_patch={"classified_element_id": books.note},
  )
  assert all(li.flow_element_id is None for li in _lines(books, event))


@pytest.mark.parametrize(
  ("qname", "message"),
  [
    ("rs-gaap:NoSuchFlow", "No flow concept named"),
    ("rs-gaap:NotesPayable", "is not a flow concept"),
  ],
)
def test_a_flow_that_is_not_one_is_refused_at_classify(books, qname, message):
  books.session.add(
    Element(
      name="NotesPayable",
      qname="rs-gaap:NotesPayable",
      balance_type="credit",
      period_type="instant",
      created_by="test",
    )
  )
  books.session.commit()
  event = _deposit(books)

  with pytest.raises(HandlerMetadataValidationError, match=message):
    _update(
      books,
      event,
      transition_to="classified",
      metadata_patch={
        "classified_element_id": books.note,
        "classified_flow_qname": qname,
      },
    )


def test_a_posted_line_in_a_closed_month_is_retagged_in_place(books):
  event = _deposit(books)
  _update(
    books,
    event,
    transition_to="committed",
    metadata_patch={"classified_element_id": books.note},
  )
  amounts = [(li.debit_amount, li.credit_amount) for li in _lines(books, event)]
  books.session.add(
    FiscalPeriod(
      graph_id=GRAPH_ID,
      entity_id=books.parent.id,
      name="2026-08",
      start_date=date(2026, 8, 1),
      end_date=date(2026, 8, 31),
      period_type="month",
      status="closed",
    )
  )
  books.session.commit()

  _update(books, event, metadata_patch={"classified_flow_qname": NOTE_PROCEEDS})

  lines = _lines(books, event)
  assert {str(li.flow_element_id) for li in lines} == {books.note_flow}
  assert [(li.debit_amount, li.credit_amount) for li in lines] == amounts
  books.session.refresh(event)
  (retag,) = event.metadata_["flow_retags"]
  assert (retag["by"], retag["flows"]) == ("usr_1", [NOTE_PROCEEDS])

  _update(books, event, metadata_patch={"classified_flow_qname": ""})
  assert all(li.flow_element_id is None for li in _lines(books, event))


def test_the_account_of_a_posted_line_cannot_change_here(books):
  event = _deposit(books)
  _update(
    books,
    event,
    transition_to="committed",
    metadata_patch={"classified_element_id": books.note},
  )

  with pytest.raises(HandlerMetadataValidationError, match="only its flows"):
    _update(
      books,
      event,
      metadata_patch={
        "classified_element_id": books.equity,
        "classified_flow_qname": CONTRIBUTIONS,
      },
    )


def test_a_split_with_two_flows_gives_each_part_its_own_cash_line(books):
  event = _deposit(books, cents=1_010_000)
  _update(
    books,
    event,
    transition_to="committed",
    metadata_patch={
      "classified_allocations": [
        {"element_id": books.note, "amount": 1_000_000, "flow_qname": NOTE_PROCEEDS},
        {"element_id": books.equity, "amount": 10_000, "flow_qname": CONTRIBUTIONS},
      ]
    },
  )

  cash = [li for li in _lines(books, event) if str(li.element_id) == books.cash]
  assert sorted((li.debit_amount, str(li.flow_element_id)) for li in cash) == sorted(
    [(1_000_000, books.note_flow), (10_000, books.contribution_flow)]
  )


def test_a_split_posted_as_one_cash_line_cannot_take_two_flows(books):
  event = _deposit(books, cents=1_010_000)
  split = [
    {"element_id": books.note, "amount": 1_000_000},
    {"element_id": books.equity, "amount": 10_000},
  ]
  _update(
    books,
    event,
    transition_to="committed",
    metadata_patch={"classified_allocations": split},
  )

  with pytest.raises(HandlerMetadataValidationError, match="different flows"):
    _update(
      books,
      event,
      metadata_patch={
        "classified_allocations": [
          {**split[0], "flow_qname": NOTE_PROCEEDS},
          {**split[1], "flow_qname": CONTRIBUTIONS},
        ]
      },
    )


def test_a_subsidiarys_line_classified_to_the_parents_account_is_refused(books):
  """Classify refuses what commit would: an account outside the line's own
  entity's chart."""
  sub_cash = entity_account(books.session, books.sub.id, "Checking")
  sub_rent = entity_account(books.session, books.sub.id, "Rent Income")
  event = _deposit(books)
  event.entity_id = books.sub.id
  event.resource_element_id = sub_cash
  books.session.commit()

  with pytest.raises(AccountOutsideEntityChartError):
    _update(
      books,
      event,
      transition_to="classified",
      metadata_patch={"classified_element_id": books.note},
    )
  books.session.rollback()

  _update(
    books,
    event,
    transition_to="classified",
    metadata_patch={"classified_element_id": sub_rent},
  )
  assert event.status == "classified"


def test_a_classified_line_repointed_to_the_parents_account_is_refused(books):
  sub_cash = entity_account(books.session, books.sub.id, "Checking")
  sub_rent = entity_account(books.session, books.sub.id, "Rent Income")
  event = _deposit(books)
  event.entity_id = books.sub.id
  event.resource_element_id = sub_cash
  books.session.commit()
  _update(
    books,
    event,
    transition_to="classified",
    metadata_patch={"classified_element_id": sub_rent},
  )

  with pytest.raises(AccountOutsideEntityChartError):
    _update(books, event, metadata_patch={"classified_element_id": books.note})


def test_a_classified_line_keeps_its_other_edits_after_its_account_retires(books):
  """Only a patch that re-points the line is re-checked: a memo edit on a line
  whose account was retired since it was classified still goes through."""
  event = _deposit(books)
  _update(
    books,
    event,
    transition_to="classified",
    metadata_patch={"classified_element_id": books.note},
  )
  books.session.get(Element, books.note).is_active = False
  books.session.commit()

  _update(books, event, metadata_patch={"memo": "Note from the lender"})

  assert event.metadata_["memo"] == "Note from the lender"


# ── A flow must be a row of the entity's cash flow statement ─────────────────


def _cash_flow_network(session, entity_id: str, rows: list[str]) -> None:
  """Compose a cash flow network for the entity's Style presenting ``rows``
  under the financing subtotal, each footing it through a calc arc."""
  from robosystems.models.extensions import (
    Association,
    ReportingStyleNetwork,
    Structure,
    Taxonomy,
  )
  from robosystems.operations.roboledger.reports.network_picker import (
    load_entity_reporting_style,
  )

  taxonomy = Taxonomy(
    name="pres", taxonomy_type="reporting_standard", created_by="test"
  )
  session.add(taxonomy)
  session.flush()
  network = Structure(
    name="Cash Flow — Indirect",
    block_type="cash_flow_statement",
    taxonomy_id=taxonomy.id,
    created_by="test",
  )
  session.add(network)
  session.flush()
  subtotal = Element(
    name="NetCashProvidedByUsedInFinancingActivities",
    qname="rs-gaap:NetCashProvidedByUsedInFinancingActivities",
    balance_type="debit",
    period_type="duration",
    created_by="test",
  )
  session.add(subtotal)
  session.flush()
  for order, qname in enumerate(rows, start=1):
    leaf = session.query(Element).filter(Element.qname == qname).one()
    for association_type in ("presentation", "calculation"):
      session.add(
        Association(
          structure_id=network.id,
          from_element_id=subtotal.id,
          to_element_id=leaf.id,
          association_type=association_type,
          order_value=order,
          weight=1.0,
          created_by="test",
        )
      )
  session.add(
    ReportingStyleNetwork(
      reporting_style_id=load_entity_reporting_style(session, entity_id),
      statement_type="cash_flow_statement",
      network_id=network.id,
    )
  )
  session.commit()


def test_a_flow_the_statement_has_no_row_for_is_refused(books):
  _cash_flow_network(books.session, books.parent.id, [NOTE_PROCEEDS])
  event = _deposit(books)
  with pytest.raises(HandlerMetadataValidationError) as excinfo:
    _update(
      books,
      event,
      transition_to="classified",
      metadata_patch={
        "classified_element_id": books.equity,
        "classified_flow_qname": CONTRIBUTIONS,
      },
    )
  message = str(excinfo.value)
  assert "not a row of this company's cash flow statement" in message
  assert NOTE_PROCEEDS in message


def test_a_flow_the_statement_presents_is_accepted(books):
  _cash_flow_network(books.session, books.parent.id, [NOTE_PROCEEDS])
  event = _deposit(books)
  _update(
    books,
    event,
    transition_to="committed",
    metadata_patch={
      "classified_element_id": books.note,
      "classified_flow_qname": NOTE_PROCEEDS,
    },
  )
  assert {str(li.flow_element_id) for li in _lines(books, event)} == {books.note_flow}
