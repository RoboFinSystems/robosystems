"""Books that start from a balance sheet: one ``opening`` entry, dated the eve
of the first fiscal month, read as the books' starting balances and never as
activity. Runs against a tenant provisioned the way a graph's is, with a
services chart, so the statements are the real pivot through its mapping."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError

from robosystems.config import env
from robosystems.db.extensions import extensions_session, provision_tenant_schema
from robosystems.models.api.extensions.chart_of_accounts import (
  InitializeChartOfAccountsRequest,
)
from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
  OpeningBalanceInput,
  RecordOpeningBalanceRequest,
  ReverseJournalEntryRequest,
)
from robosystems.models.extensions import Element, Entity
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.operations.roboledger.commands.chart_of_accounts import (
  initialize_chart_of_accounts,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import close_period
from robosystems.operations.roboledger.commands.journal_entries import (
  create_journal_entry,
  reverse_journal_entry,
)
from robosystems.operations.roboledger.commands.opening_balances import (
  OpeningBalanceError,
  OpeningBalanceLockedError,
  record_opening_balance,
)
from robosystems.operations.roboledger.fiscal_calendar import (
  FiscalCalendarService,
  PeriodCloseService,
)
from robosystems.operations.roboledger.reads.reports import (
  get_live_financial_statement,
)

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd49"
PARENT = "ent_cedar"
EVE = date(2026, 6, 30)
JULY = (date(2026, 7, 1), date(2026, 7, 31))
_COMMANDS = "robosystems.operations.roboledger.commands.fiscal_calendar"
CASH = "rs-gaap:CashAndCashEquivalentsAtCarryingValue"


@pytest.fixture()
def tenant():
  url = env.EXTENSIONS_DATABASE_URL
  if not url:
    pytest.skip("EXTENSIONS_DATABASE_URL not configured")
  engine = create_engine(url)
  try:
    with engine.connect() as probe:
      probe.execute(text("SELECT 1"))
  except OperationalError as exc:
    engine.dispose()
    pytest.skip(f"extensions database unreachable: {exc.orig}")
  with engine.begin() as conn:
    conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
  try:
    provision_tenant_schema(GRAPH)
    with extensions_session(GRAPH) as session:
      session.add(
        Entity(
          id=PARENT,
          name="Cedar Advisory LLC",
          entity_type="llc",
          is_parent=True,
          created_by="usr_1",
        )
      )
      session.flush()
      chart = initialize_chart_of_accounts(
        session, InitializeChartOfAccountsRequest(template="services"), "usr_1"
      ).taxonomy_id
      service = FiscalCalendarService()
      service.initialize(session, GRAPH, actor_id="usr_1", entity_id=PARENT)
      service.ensure_fiscal_periods(
        session,
        GRAPH,
        start_period="2026-07",
        end_period="2026-08",
        entity_id=PARENT,
      )
      session.commit()
    yield chart
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


@pytest.fixture(autouse=True)
def _no_platform():
  with (
    patch(f"{_COMMANDS}.exclusive_period_fence"),
    patch("robosystems.operations.extensions.staleness.mark_graph_stale"),
    patch(
      "robosystems.operations.roboledger.reads.fiscal_calendar.qb_sync_state",
      return_value=(False, None),
    ),
  ):
    yield


def _account(session, chart_id: str, code: str) -> str:
  return str(
    session.execute(
      select(Element.id).where(Element.taxonomy_id == chart_id, Element.code == code)
    ).scalar_one()
  )


def _opening(session, chart, **over) -> RecordOpeningBalanceRequest:
  """Cash 60,000, equipment 90,000 less 30,000 depreciated, against owner's
  equity 100,000 and retained earnings 20,000."""
  a = lambda code: _account(session, chart, code)  # noqa: E731
  body = {
    "balances": [
      OpeningBalanceInput(element_id=a("1000"), debit_amount=6_000_000),
      OpeningBalanceInput(element_id=a("1300"), debit_amount=9_000_000),
      OpeningBalanceInput(element_id=a("1350"), credit_amount=3_000_000),
      OpeningBalanceInput(element_id=a("3000"), credit_amount=10_000_000),
      OpeningBalanceInput(element_id=a("3100"), credit_amount=2_000_000),
    ],
    **over,
  }
  return RecordOpeningBalanceRequest(**body)


def _july_revenue(session, chart) -> None:
  create_journal_entry(
    session,
    CreateJournalEntryRequest(
      posting_date=date(2026, 7, 15),
      memo="Advisory fee received",
      status="posted",
      line_items=[
        JournalEntryLineItemInput(
          element_id=_account(session, chart, "1000"), debit_amount=500_000
        ),
        JournalEntryLineItemInput(
          element_id=_account(session, chart, "4000"), credit_amount=500_000
        ),
      ],
    ),
    "usr_1",
  )


def _statement(session, statement_type: str):
  return get_live_financial_statement(
    session,
    graph_id=GRAPH,
    statement_type=statement_type,
    period_start=JULY[0],
    period_end=JULY[1],
  )


def _values(response) -> dict[str, list[float | None]]:
  return {f.qname: f.values for f in response.facts}


def test_the_opening_is_the_books_starting_balance_not_activity(tenant):
  """Dated the eve of July, the opening is June 30's balance sheet. July's
  cash flow moves only July's cash, with no capex or stock issued out of
  the opening; June's flows reach back before the books, so their column is
  left out; and the equity statement opens on the opening's equity."""
  with extensions_session(GRAPH) as session:
    entry = record_opening_balance(session, _opening(session, tenant), "usr_1")
    _july_revenue(session, tenant)
    session.commit()

    assert (entry.posting_date, entry.type) == (EVE, "opening")

    balance_sheet = _statement(session, "balance_sheet")
    assert [p.end for p in balance_sheet.periods] == [JULY[1], EVE]
    assert _values(balance_sheet)[CASH] == [65_000.0, 60_000.0]

    cash_flow = _statement(session, "cash_flow_statement")
    assert [p.start for p in cash_flow.omitted_periods] == [date(2026, 6, 1)]
    flows = _values(cash_flow)
    assert flows["rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease"][0] == 5_000.0
    assert "rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment" not in flows
    assert cash_flow.validation.warnings == []

    equity = _statement(session, "equity_statement")
    opening = next(f for f in equity.facts if f.period_role == "start")
    closing = next(f for f in equity.facts if f.period_role == "end")
    assert (opening.values[0], closing.values[0]) == (120_000.0, 125_000.0)
    assert equity.validation.status == "passed"


def test_a_difference_needs_an_equity_account_and_income_is_refused(tenant):
  with extensions_session(GRAPH) as session:
    a = lambda code: _account(session, tenant, code)  # noqa: E731
    short = [OpeningBalanceInput(element_id=a("1000"), debit_amount=1_000)]
    with pytest.raises(OpeningBalanceError, match="name the equity"):
      record_opening_balance(
        session, RecordOpeningBalanceRequest(balances=short), "usr_1"
      )
    with pytest.raises(OpeningBalanceError, match="income or expense"):
      record_opening_balance(
        session,
        RecordOpeningBalanceRequest(
          balances=[
            OpeningBalanceInput(element_id=a("1000"), debit_amount=1_000),
            OpeningBalanceInput(element_id=a("4000"), credit_amount=1_000),
          ]
        ),
        "usr_1",
      )
    absorbed = record_opening_balance(
      session,
      RecordOpeningBalanceRequest(balances=short, equity_element_id=a("3000")),
      "usr_1",
    )
    assert sorted(
      (li.debit_amount, li.credit_amount) for li in absorbed.line_items
    ) == [(0, 1_000), (1_000, 0)]


def test_recording_again_replaces_it_until_the_first_close(tenant):
  service = FiscalCalendarService()
  with extensions_session(GRAPH) as session:
    record_opening_balance(session, _opening(session, tenant), "usr_1")
    record_opening_balance(
      session, _opening(session, tenant, memo="Opening balances, corrected"), "usr_1"
    )
    session.commit()
    openings = session.execute(
      select(Entry.memo).where(Entry.entity_id == PARENT, Entry.type == "opening")
    ).all()
    assert [m for (m,) in openings] == ["Opening balances, corrected"]

    close_period(
      session,
      MagicMock(),
      GRAPH,
      "2026-07",
      actor_id="usr_1",
      allow_stale_sync=False,
      note=None,
      service=service,
      close_service=PeriodCloseService(service),
      entity_id=PARENT,
    )
    with pytest.raises(OpeningBalanceLockedError):
      record_opening_balance(session, _opening(session, tenant), "usr_1")


def test_moving_the_calendar_start_moves_the_opening(tenant):
  with extensions_session(GRAPH) as session:
    record_opening_balance(session, _opening(session, tenant), "usr_1")
    FiscalCalendarService().change_start(
      session, GRAPH, "2026-06", actor_id="usr_1", entity_id=PARENT
    )
    session.commit()
    eve = session.execute(
      select(Entry.posting_date).where(Entry.type == "opening")
    ).scalar_one()
    assert eve == date(2026, 5, 31)


def test_the_opening_cannot_be_reversed(tenant):
  with extensions_session(GRAPH) as session:
    entry = record_opening_balance(session, _opening(session, tenant), "usr_1")
    with pytest.raises(ValueError, match="opening balance"):
      reverse_journal_entry(
        session,
        ReverseJournalEntryRequest(entry_id=entry.id, posting_date=date(2026, 7, 1)),
        "usr_1",
      )
