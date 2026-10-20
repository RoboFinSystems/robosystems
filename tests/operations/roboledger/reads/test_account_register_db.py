"""One account's lines, oldest first, each with the balance it leaves.

The register's closing balance is its opening plus the trial balance's net for
the same window, so the two reads cannot drift. An income or expense account
opens at the fiscal year's start; a page read alone carries its own opening.
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
from robosystems.models.api.extensions.dimensions import CreateDimensionRequest
from robosystems.models.extensions import Element, ElementTrait, Entity, Trait
from robosystems.models.extensions.roboledger.agent import Agent
from robosystems.models.extensions.roboledger.dimension_junctions import (
  line_item_dimensions,
)
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.roboledger.commands.dimensions import create_dimension
from robosystems.operations.roboledger.line_dimensions import LineSlice
from robosystems.operations.roboledger.reads.account_register import (
  AccountNotFoundError,
  InvalidCursorError,
  get_account_register,
)
from robosystems.operations.roboledger.reads.journal_entries import (
  entries_by_event,
  list_journal_entries,
)
from robosystems.operations.roboledger.reads.trial_balance import (
  get_net_balances_cents,
)
from tests.ledger_entity import PARENT_ENTITY_ID, entity_account, seed_parent_entity

pytestmark = pytest.mark.unit

SUB = "ent_sub"


@pytest.fixture()
def session():
  url = os.environ.get("TEST_DATABASE_URL")
  if not url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_register_{uuid.uuid4().hex[:12]}"
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


def _account(session, name: str, trait: str, balance_type: str = "debit") -> str:
  account = entity_account(session, PARENT_ENTITY_ID, name)
  session.get(Element, account).balance_type = balance_type
  trait_id = session.execute(
    select(Trait.id).where(
      Trait.category == "elementsOfFinancialStatements", Trait.identifier == trait
    )
  ).scalar_one_or_none()
  if trait_id is None:
    row = Trait(category="elementsOfFinancialStatements", identifier=trait, name=trait)
    session.add(row)
    session.flush()
    trait_id = row.id
  session.add(ElementTrait(element_id=account, trait_id=trait_id, is_primary=True))
  session.flush()
  return account


def _entry(
  session,
  lines: list[tuple[str, int, int]],
  on: date,
  *,
  entity: str = PARENT_ENTITY_ID,
  status: str = "posted",
  event_id: str | None = None,
  reversal_of: str | None = None,
) -> tuple[str, list[str]]:
  entry = Entry(
    entity_id=entity,
    posting_date=on,
    status=status,
    triggered_by_event_id=event_id,
    reversal_of=reversal_of,
    created_by="t",
  )
  session.add(entry)
  session.flush()
  ids = []
  for order, (account, debit, credit) in enumerate(lines):
    line = LineItem(
      entry_id=entry.id,
      element_id=account,
      debit_amount=debit,
      credit_amount=credit,
      line_order=order,
    )
    session.add(line)
    session.flush()
    ids.append(str(line.id))
  return str(entry.id), ids


@pytest.fixture()
def books(session):
  cash = _account(session, "Cash", "asset")
  revenue = _account(session, "Sales", "revenue", "credit")
  return cash, revenue


def test_lines_run_from_the_opening_to_a_closing_the_trial_balance_agrees_with(
  session, books
):
  cash, revenue = books
  _entry(session, [(cash, 10000, 0), (revenue, 0, 10000)], date(2026, 8, 10))
  _entry(session, [(cash, 2500, 0), (revenue, 0, 2500)], date(2026, 9, 3))
  _entry(session, [(cash, 0, 4000), (revenue, 4000, 0)], date(2026, 9, 20))

  register = get_account_register(session, cash, date(2026, 9, 1), date(2026, 9, 30))

  assert register.opening_balance == 100.0
  assert [(line.debit, line.credit, line.balance) for line in register.lines] == [
    (25.0, 0.0, 125.0),
    (0.0, 40.0, 85.0),
  ]
  assert register.closing_balance == 85.0
  assert register.line_count == 2
  assert register.opening_from is None
  window = get_net_balances_cents(
    session, date(2026, 9, 1), date(2026, 9, 30), entity_id=PARENT_ENTITY_ID
  )[cash]
  assert round((register.closing_balance - register.opening_balance) * 100) == window
  inception = get_net_balances_cents(
    session, None, date(2026, 9, 30), entity_id=PARENT_ENTITY_ID
  )[cash]
  assert round(register.closing_balance * 100) == inception


def test_an_income_account_reads_credit_positive_and_opens_at_the_fiscal_year(
  session, books
):
  cash, revenue = books
  session.add(
    FiscalCalendar(graph_id="g", entity_id=PARENT_ENTITY_ID, fiscal_year_start_month=7)
  )
  _entry(session, [(cash, 50000, 0), (revenue, 0, 50000)], date(2026, 6, 15))
  _entry(session, [(cash, 10000, 0), (revenue, 0, 10000)], date(2026, 8, 10))
  _entry(session, [(cash, 2500, 0), (revenue, 0, 2500)], date(2026, 9, 3))

  register = get_account_register(session, revenue, date(2026, 9, 1), date(2026, 9, 30))

  assert register.account.temporary is True
  assert register.account.balance_type == "credit"
  assert register.opening_from == date(2026, 7, 1)
  assert register.opening_balance == 100.0
  assert [line.balance for line in register.lines] == [125.0]
  assert register.closing_balance == 125.0


def test_drafts_stay_out_of_every_balance_and_come_back_on_request(session, books):
  cash, revenue = books
  _entry(session, [(cash, 10000, 0), (revenue, 0, 10000)], date(2026, 9, 3))
  _entry(
    session,
    [(cash, 7700, 0), (revenue, 0, 7700)],
    date(2026, 9, 30),
    status="draft",
  )

  plain = get_account_register(session, cash, date(2026, 9, 1), date(2026, 9, 30))
  with_drafts = get_account_register(
    session, cash, date(2026, 9, 1), date(2026, 9, 30), include_drafts=True
  )

  assert plain.drafts == []
  assert with_drafts.closing_balance == plain.closing_balance == 100.0
  [draft] = with_drafts.drafts
  assert (draft.status, draft.debit, draft.balance) == ("draft", 77.0, None)


def test_a_reversal_shows_both_halves_and_names_each_other(session, books):
  cash, revenue = books
  original, _ = _entry(
    session,
    [(cash, 10000, 0), (revenue, 0, 10000)],
    date(2026, 9, 3),
    status="reversed",
  )
  reversal, _ = _entry(
    session,
    [(cash, 0, 10000), (revenue, 10000, 0)],
    date(2026, 9, 4),
    reversal_of=original,
  )

  register = get_account_register(session, cash, date(2026, 9, 1), date(2026, 9, 30))

  first, second = register.lines
  assert (first.entry_id, first.status, first.reversed_by) == (
    original,
    "reversed",
    reversal,
  )
  assert (second.entry_id, second.reversal_of) == (reversal, original)
  assert register.closing_balance == 0.0


def test_each_page_carries_its_own_opening(session, books):
  cash, revenue = books
  for day in range(1, 6):
    _entry(
      session, [(cash, day * 1000, 0), (revenue, 0, day * 1000)], date(2026, 9, day)
    )

  whole = get_account_register(session, cash, date(2026, 9, 1), date(2026, 9, 30))
  pages, cursor = [], None
  while True:
    page = get_account_register(
      session, cash, date(2026, 9, 1), date(2026, 9, 30), after=cursor, limit=2
    )
    pages.append(page)
    cursor = page.next_cursor
    if cursor is None:
      break

  assert [len(page.lines) for page in pages] == [2, 2, 1]
  assert pages[1].page_opening_balance == pages[0].lines[-1].balance
  assert pages[2].page_opening_balance == pages[1].lines[-1].balance
  assert [line.balance for page in pages for line in page.lines] == [
    line.balance for line in whole.lines
  ]
  assert {page.closing_balance for page in pages} == {whole.closing_balance}


def test_the_register_reads_one_entity_s_books(session, books):
  cash, revenue = books
  _entry(session, [(cash, 10000, 0), (revenue, 0, 10000)], date(2026, 9, 3))
  _entry(session, [(cash, 3000, 0), (revenue, 0, 3000)], date(2026, 9, 4), entity=SUB)

  parent = get_account_register(session, cash, date(2026, 9, 1), date(2026, 9, 30))
  sub = get_account_register(
    session, cash, date(2026, 9, 1), date(2026, 9, 30), entity_id=SUB
  )

  assert parent.entity_id == PARENT_ENTITY_ID
  assert [line.debit for line in parent.lines] == [100.0]
  assert [line.debit for line in sub.lines] == [30.0]


def test_a_line_names_the_counterparty_its_event_does(session, books):
  cash, revenue = books
  session.add(Agent(id="agt_acme", agent_type="customer", name="Acme", created_by="t"))
  event = Event(
    id="evt_sale",
    entity_id=PARENT_ENTITY_ID,
    event_type="sale",
    event_category="sales",
    occurred_at=datetime(2026, 9, 3),
    source="native",
    amount=10000,
    agent_id="agt_acme",
    status="committed",
    created_by="t",
  )
  session.add(event)
  session.flush()
  _entry(
    session,
    [(cash, 10000, 0), (revenue, 0, 10000)],
    date(2026, 9, 3),
    event_id="evt_sale",
  )

  [line] = get_account_register(
    session, cash, date(2026, 9, 1), date(2026, 9, 30)
  ).lines

  assert (
    line.triggered_by_event_id,
    line.triggered_by_event_type,
    line.agent_id,
    line.agent_name,
  ) == ("evt_sale", "sale", "agt_acme", "Acme")


def test_a_dimension_slice_reads_only_its_lines(session, books):
  cash, revenue = books
  roastery = create_dimension(
    session,
    CreateDimensionRequest(dimension_type="department", name="Roastery"),
    "t",
  ).id
  _, (tagged, _) = _entry(
    session, [(cash, 10000, 0), (revenue, 0, 10000)], date(2026, 9, 3)
  )
  _entry(session, [(cash, 4000, 0), (revenue, 0, 4000)], date(2026, 9, 4))
  session.execute(
    line_item_dimensions.insert().values(line_item_id=tagged, dimension_id=roastery)
  )
  session.flush()

  member = get_account_register(
    session,
    cash,
    date(2026, 9, 1),
    date(2026, 9, 30),
    line_slice=LineSlice("department", roastery),
  )
  unallocated = get_account_register(
    session,
    cash,
    date(2026, 9, 1),
    date(2026, 9, 30),
    line_slice=LineSlice("department"),
  )

  assert member.sliced is True
  assert [(line.line_id, line.debit) for line in member.lines] == [(tagged, 100.0)]
  assert [d.name for d in member.lines[0].dimensions] == ["Roastery"]
  assert [line.debit for line in unallocated.lines] == [40.0]


def test_an_unknown_account_or_cursor_is_refused(session, books):
  cash, _ = books
  with pytest.raises(AccountNotFoundError):
    get_account_register(session, "elem_missing")
  with pytest.raises(InvalidCursorError):
    get_account_register(session, cash, after="not-a-cursor")


def test_drafts_come_with_the_first_page_only_and_say_when_cut(session, books):
  cash, revenue = books
  for day in range(1, 4):
    _entry(session, [(cash, 1000, 0), (revenue, 0, 1000)], date(2026, 9, day))
    _entry(
      session,
      [(cash, 500, 0), (revenue, 0, 500)],
      date(2026, 9, day + 10),
      status="draft",
    )

  first = get_account_register(
    session, cash, date(2026, 9, 1), date(2026, 9, 30), include_drafts=True, limit=2
  )
  second = get_account_register(
    session,
    cash,
    date(2026, 9, 1),
    date(2026, 9, 30),
    include_drafts=True,
    limit=2,
    after=first.next_cursor,
  )

  assert [d.posting_date.day for d in first.drafts] == [11, 12]
  assert first.drafts_truncated is True
  assert (second.drafts, second.drafts_truncated) == ([], False)


def test_an_income_account_pages_from_its_fiscal_year_opening(session, books):
  cash, revenue = books
  session.add(
    FiscalCalendar(graph_id="g", entity_id=PARENT_ENTITY_ID, fiscal_year_start_month=7)
  )
  _entry(session, [(cash, 90000, 0), (revenue, 0, 90000)], date(2026, 6, 30))
  _entry(session, [(cash, 10000, 0), (revenue, 0, 10000)], date(2026, 7, 15))
  for day in range(1, 4):
    _entry(session, [(cash, 1000, 0), (revenue, 0, 1000)], date(2026, 9, day))

  first = get_account_register(session, revenue, date(2026, 9, 1), limit=2)
  second = get_account_register(
    session, revenue, date(2026, 9, 1), limit=2, after=first.next_cursor
  )

  assert first.opening_balance == 100.0
  assert [line.balance for line in first.lines] == [110.0, 120.0]
  assert second.page_opening_balance == 120.0
  assert [line.balance for line in second.lines] == [130.0]
  assert second.closing_balance == 130.0


def test_an_event_lists_the_entries_it_wrote_and_the_journal_names_it(session, books):
  cash, revenue = books
  for event_id in ("evt_a", "evt_b"):
    session.add(
      Event(
        id=event_id,
        entity_id=PARENT_ENTITY_ID,
        event_type="invoice_issued",
        event_category="sales",
        occurred_at=datetime(2026, 9, 3),
        source="native",
        amount=10000,
        status="committed",
        created_by="t",
      )
    )
  session.flush()
  first, _ = _entry(
    session, [(cash, 10000, 0), (revenue, 0, 10000)], date(2026, 9, 3), event_id="evt_a"
  )
  second, _ = _entry(
    session, [(cash, 500, 0), (revenue, 0, 500)], date(2026, 9, 4), event_id="evt_a"
  )
  superseded, _ = _entry(
    session,
    [(cash, 900, 0), (revenue, 0, 900)],
    date(2026, 9, 2),
    status="superseded",
    event_id="evt_a",
  )
  _entry(session, [(cash, 100, 0), (revenue, 0, 100)], date(2026, 9, 5))

  by_event = entries_by_event(session, ["evt_a", "evt_b"])
  journal = list_journal_entries(session)

  assert [entry.id for entry in by_event["evt_a"]] == [first, second]
  assert "evt_b" not in by_event
  assert superseded not in {entry.id for entry in by_event["evt_a"]}
  named = {entry.id: entry.triggered_by_event_type for entry in journal.entries}
  assert named[first] == "invoice_issued"
  assert sorted(v for v in named.values() if v is None) == [None]
