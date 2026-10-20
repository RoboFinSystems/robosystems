"""A live statement figure, opened on the accounts it was summed from.

With ``include_sources`` each row says how its figure was made and, per
column, lists the accounts behind it with what each contributed, keyed by the
entity whose books hold them. A column lists accounts only when they add up
to the figure, so a derived figure never claims lines it did not sum.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError

from robosystems.config import env
from robosystems.db.extensions import extensions_session, provision_tenant_schema
from robosystems.models.api.extensions.chart_of_accounts import (
  InitializeChartOfAccountsRequest,
)
from robosystems.models.api.extensions.dimensions import (
  CreateDimensionRequest,
  StatementDimension,
)
from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
)
from robosystems.models.extensions import Element, Entity
from robosystems.operations.roboledger.commands.chart_of_accounts import (
  initialize_chart_of_accounts,
)
from robosystems.operations.roboledger.commands.dimensions import create_dimension
from robosystems.operations.roboledger.commands.journal_entries import (
  create_journal_entry,
)
from robosystems.operations.roboledger.fiscal_calendar import FiscalCalendarService
from robosystems.operations.roboledger.reads.reports import (
  DimensionalStatementError,
  get_live_financial_statement,
)

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd64"
PARENT = "ent_src_parent"
_FISCAL = "robosystems.operations.roboledger.commands.fiscal_calendar"
REVENUE = "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"
JULY = (date(2026, 7, 1), date(2026, 7, 31))


@pytest.fixture()
def books():
  """A services chart with an open July and one department. Returns
  ``(chart_id, department_id)``."""
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
    with (
      patch(f"{_FISCAL}.exclusive_period_fence"),
      patch("robosystems.operations.extensions.staleness.mark_graph_stale"),
      extensions_session(GRAPH) as session,
    ):
      session.add(
        Entity(
          id=PARENT,
          name="Source Harbor LLC",
          entity_type="llc",
          is_parent=True,
          created_by="usr_1",
        )
      )
      session.flush()
      chart = initialize_chart_of_accounts(
        session, InitializeChartOfAccountsRequest(template="services"), "usr_1"
      )
      service = FiscalCalendarService()
      service.initialize(
        session, GRAPH, closed_through="2026-06", actor_id="usr_1", entity_id=PARENT
      )
      service.ensure_fiscal_periods(
        session,
        GRAPH,
        start_period="2026-06",
        end_period="2026-07",
        closed_through="2026-06",
        entity_id=PARENT,
      )
      session.commit()
      sales = create_dimension(
        session,
        CreateDimensionRequest(dimension_type="department", name="Sales"),
        "usr_1",
      )
      session.commit()
      yield chart.taxonomy_id, sales.id
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


def _account(session, chart_id: str, code: str) -> str:
  return str(
    session.execute(
      select(Element.id).where(Element.taxonomy_id == chart_id, Element.code == code)
    ).scalar_one()
  )


def _fees(session, chart_id: str, cents: int, tags: list[str] | None = None) -> None:
  create_journal_entry(
    session,
    CreateJournalEntryRequest(
      posting_date=date(2026, 7, 15),
      memo="Consulting fees",
      status="posted",
      line_items=[
        JournalEntryLineItemInput(
          element_id=_account(session, chart_id, "1000"), debit_amount=cents
        ),
        JournalEntryLineItemInput(
          element_id=_account(session, chart_id, "4000"),
          credit_amount=cents,
          dimension_ids=tags,
        ),
      ],
    ),
    "usr_1",
    entity_id=PARENT,
  )


def _july(session, statement_type: str = "income_statement", **kwargs):
  return get_live_financial_statement(
    session,
    graph_id=GRAPH,
    statement_type=statement_type,
    period_start=JULY[0],
    period_end=JULY[1],
    **kwargs,
  )


def test_a_leaf_lists_its_accounts_and_a_subtotal_its_rows(books):
  chart_id, _sales = books
  with extensions_session(GRAPH) as session:
    _fees(session, chart_id, 50_000)
    _fees(session, chart_id, 20_000)

    statement = _july(session, include_sources=True)

    rows = {f.qname: f for f in statement.facts}
    revenue = rows[REVENUE]
    assert revenue.origin == "accounts"
    [source] = revenue.sources[0]
    assert (source.entity_id, source.account_id, source.code, source.value) == (
      PARENT,
      _account(session, chart_id, "4000"),
      "4000",
      700.0,
    )
    assert source.value == revenue.values[0]
    assert rows["rs-gaap:NetIncomeLoss"].origin == "subtotal"
    assert all(len(f.sources) == len(f.values) for f in statement.facts)


def test_without_the_flag_rows_carry_no_sources(books):
  chart_id, _sales = books
  with extensions_session(GRAPH) as session:
    _fees(session, chart_id, 50_000)

    statement = _july(session)

    assert {(f.origin, f.sources) for f in statement.facts} == {(None, None)}


def test_a_figure_its_accounts_do_not_sum_to_lists_none(books):
  chart_id, _sales = books
  with extensions_session(GRAPH) as session:
    _fees(session, chart_id, 50_000)

    sheet = _july(session, "balance_sheet", include_sources=True)

    # The month's earnings close into equity: computed, never posted.
    equity = next(f for f in sheet.facts if f.trait == "equity" and not f.is_subtotal)
    assert equity.values[0] == 500.0
    assert equity.sources[0] == []
    assert equity.origin == "computed"


def test_one_members_slice_lists_only_its_lines(books):
  chart_id, sales = books
  with extensions_session(GRAPH) as session:
    _fees(session, chart_id, 50_000, [sales])
    _fees(session, chart_id, 20_000)

    sliced = _july(
      session,
      dimension=StatementDimension(type="department", member_id=sales),
      include_sources=True,
    )

    revenue = next(f for f in sliced.facts if f.qname == REVENUE)
    assert [s.value for s in revenue.sources[0]] == [500.0]


def test_a_statement_read_by_column_refuses_sources(books):
  with extensions_session(GRAPH) as session:
    with pytest.raises(DimensionalStatementError, match="member_id"):
      _july(
        session,
        dimension=StatementDimension(type="department"),
        include_sources=True,
      )
