"""Management dimensions: the catalog, tagged journal lines, and the slice.

Against the real extensions database with a throwaway tenant: members are
created and retired, a journal line carries one member per axis, a posted
line is re-tagged in place and a reversal carries its original's members, and
the income statement read by member ties out to its undimensioned total.
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
  UpdateDimensionRequest,
)
from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
  ReverseJournalEntryRequest,
  UpdateJournalEntryRequest,
)
from robosystems.models.extensions import Dimension, Element, Entity
from robosystems.operations.roboledger.commands._guards import ClosedPeriodError
from robosystems.operations.roboledger.commands.chart_of_accounts import (
  initialize_chart_of_accounts,
)
from robosystems.operations.roboledger.commands.dimensions import (
  DuplicateDimensionError,
  create_dimension,
  update_dimension,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  create_journal_entry,
  reverse_journal_entry,
  update_journal_entry,
)
from robosystems.operations.roboledger.fiscal_calendar import FiscalCalendarService
from robosystems.operations.roboledger.line_dimensions import LineSlice
from robosystems.operations.roboledger.reads.dimensions import list_dimensions
from robosystems.operations.roboledger.reads.journal_entries import (
  list_journal_entries,
)
from robosystems.operations.roboledger.reads.reports import (
  DimensionalStatementError,
  get_live_financial_statement,
)
from robosystems.operations.roboledger.reads.trial_balance import get_trial_balance

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd51"
PARENT = "ent_dim_parent"
_COMMANDS = "robosystems.operations.roboledger.commands.journal_entries"
_FISCAL = "robosystems.operations.roboledger.commands.fiscal_calendar"
REVENUE = "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"
JULY = (date(2026, 7, 1), date(2026, 7, 31))


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
          name="Dimension Harbor LLC",
          entity_type="llc",
          is_parent=True,
          created_by="usr_1",
        )
      )
    yield
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


@pytest.fixture()
def books(tenant):
  """A services chart, an open July, and two departments. Returns
  ``(chart_id, sales_id, ops_id)``."""
  with (
    patch(f"{_FISCAL}.exclusive_period_fence"),
    patch("robosystems.operations.extensions.staleness.mark_graph_stale"),
  ):
    with extensions_session(GRAPH) as session:
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
      ops = create_dimension(
        session,
        CreateDimensionRequest(dimension_type="department", name="Operations"),
        "usr_1",
      )
      return chart.taxonomy_id, sales.id, ops.id


def _account(session, chart_id: str, code: str) -> str:
  return str(
    session.execute(
      select(Element.id).where(Element.taxonomy_id == chart_id, Element.code == code)
    ).scalar_one()
  )


def _fees(session, chart_id: str, cents: int, revenue_tags: list[str] | None):
  """Posted cash received for services, the revenue line tagged."""
  return create_journal_entry(
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
          dimension_ids=revenue_tags,
        ),
      ],
    ),
    "usr_1",
    entity_id=PARENT,
  )


def _revenue(response, column: int = 0) -> float:
  return next(f.values[column] for f in response.facts if f.qname == REVENUE)


class TestCatalog:
  def test_a_member_takes_a_slug_and_a_second_of_the_same_key_is_refused(self, books):
    with extensions_session(GRAPH) as session:
      member = create_dimension(
        session,
        CreateDimensionRequest(dimension_type="location", name="Pier 39 Café"),
        "usr_1",
      )
      assert (member.value, member.source, member.is_active) == (
        "pier-39-caf",
        "native",
        True,
      )
      with pytest.raises(DuplicateDimensionError):
        create_dimension(
          session,
          CreateDimensionRequest(dimension_type="location", name="Pier 39 Caf"),
          "usr_1",
        )

  def test_the_scenario_axis_is_reserved_for_forecasts(self, books):
    with extensions_session(GRAPH) as session:
      with pytest.raises(ValueError, match="reserved for forecasts"):
        create_dimension(
          session,
          CreateDimensionRequest(dimension_type="scenario", name="Base"),
          "usr_1",
        )

  def test_the_list_counts_lines_and_hides_retired_members_unless_asked(self, books):
    chart_id, sales, ops = books
    with extensions_session(GRAPH) as session:
      _fees(session, chart_id, 10_000, [sales])
      update_dimension(
        session, UpdateDimensionRequest(dimension_id=ops, is_active=False), "usr_1"
      )
      session.add(Dimension(dimension_type="scenario", name="Base", value="s1"))
      session.commit()

      listed = list_dimensions(session)
      everything = list_dimensions(session, include_inactive=True)

      assert [(d.name, d.line_count) for d in listed.dimensions] == [("Sales", 1)]
      assert listed.types == ["department"]
      assert {d.name for d in everything.dimensions} == {"Sales", "Operations"}

  def test_a_synced_member_keeps_its_sources_name_but_can_retire(self, books):
    with extensions_session(GRAPH) as session:
      synced = Dimension(
        dimension_type="class",
        name="Retail",
        value="3",
        metadata_={"source": "quickbooks"},
      )
      session.add(synced)
      session.commit()

      with pytest.raises(ValueError, match="follows quickbooks"):
        update_dimension(
          session,
          UpdateDimensionRequest(dimension_id=synced.id, name="Shops"),
          "usr_1",
        )
      retired = update_dimension(
        session,
        UpdateDimensionRequest(dimension_id=synced.id, is_active=False),
        "usr_1",
      )
      assert retired.is_active is False


class TestTaggedLines:
  def test_a_line_carries_its_members_and_the_journal_reads_them(self, books):
    chart_id, sales, _ops = books
    with extensions_session(GRAPH) as session:
      posted = _fees(session, chart_id, 10_000, [sales])

      assert [li.dimension_ids for li in posted.line_items] == [[], [sales]]
      listed = list_journal_entries(session, entity_id=PARENT, status="posted")
      entry = next(e for e in listed.entries if e.id == posted.id)
      assert [[d.name for d in li.dimensions] for li in entry.line_items] == [
        [],
        ["Sales"],
      ]

  def test_two_members_of_one_axis_on_a_line_are_refused(self, books):
    chart_id, sales, ops = books
    with extensions_session(GRAPH) as session:
      with pytest.raises(ValueError, match="one per axis"):
        _fees(session, chart_id, 10_000, [sales, ops])

  def test_an_unknown_or_retired_member_is_refused(self, books):
    chart_id, sales, ops = books
    with extensions_session(GRAPH) as session:
      with pytest.raises(ValueError, match="No dimension member"):
        _fees(session, chart_id, 10_000, ["dim_nope"])
      update_dimension(
        session, UpdateDimensionRequest(dimension_id=ops, is_active=False), "usr_1"
      )
      with pytest.raises(ValueError, match="retired"):
        _fees(session, chart_id, 10_000, [ops])

  def test_a_posted_line_is_re_tagged_in_place_past_the_period_fence(self, books):
    """A dimension moves no balance, so a closed month's line changes in
    place; a retired member may stay on the line already carrying it, and
    the entry records what each changed line carried before."""
    chart_id, sales, ops = books
    with extensions_session(GRAPH) as session:
      posted = _fees(session, chart_id, 10_000, [sales])
      revenue_line = posted.line_items[1].id
      update_dimension(
        session, UpdateDimensionRequest(dimension_id=sales, is_active=False), "usr_1"
      )

      with patch(
        f"{_COMMANDS}.assert_period_not_closed",
        side_effect=ClosedPeriodError("2026-07", "closed"),
      ):
        kept = update_journal_entry(
          session,
          UpdateJournalEntryRequest(
            entry_id=posted.id,
            line_dimensions=[{"line_id": revenue_line, "dimension_ids": [sales]}],
          ),
        )
        moved = update_journal_entry(
          session,
          UpdateJournalEntryRequest(
            entry_id=posted.id,
            line_dimensions=[{"line_id": revenue_line, "dimension_ids": [ops]}],
          ),
        )

      assert kept.line_items[1].dimension_ids == [sales]
      assert moved.line_items[1].dimension_ids == [ops]
      history = session.execute(
        text("SELECT metadata -> 'dimension_retags' FROM entries WHERE id = :i"),
        {"i": posted.id},
      ).scalar()
      assert [(h["line_ids"], h["from"]) for h in history] == [
        ([revenue_line], {revenue_line: [sales]})
      ]

  def test_a_draft_line_replace_keeps_tags_it_is_given(self, books):
    chart_id, sales, _ops = books
    with extensions_session(GRAPH) as session:
      draft = create_journal_entry(
        session,
        CreateJournalEntryRequest(
          posting_date=date(2026, 7, 15),
          memo="Fees",
          line_items=[
            JournalEntryLineItemInput(
              element_id=_account(session, chart_id, "1000"), debit_amount=500
            ),
            JournalEntryLineItemInput(
              element_id=_account(session, chart_id, "4000"), credit_amount=500
            ),
          ],
        ),
        "usr_1",
        entity_id=PARENT,
      )
      replaced = update_journal_entry(
        session,
        UpdateJournalEntryRequest(
          entry_id=draft.id,
          line_items=[
            {"element_id": _account(session, chart_id, "1000"), "debit_amount": 700},
            {
              "element_id": _account(session, chart_id, "4000"),
              "credit_amount": 700,
              "dimension_ids": [sales],
            },
          ],
        ),
      )
      assert [li.dimension_ids for li in replaced.line_items] == [[], [sales]]

  def test_a_reversal_carries_its_originals_members(self, books):
    chart_id, sales, _ops = books
    with extensions_session(GRAPH) as session:
      posted = _fees(session, chart_id, 10_000, [sales])

      reversal = reverse_journal_entry(
        session,
        ReverseJournalEntryRequest(entry_id=posted.id, posting_date=date(2026, 7, 20)),
        "usr_1",
      )

      assert [li.dimension_ids for li in reversal.line_items] == [[], [sales]]


class TestTheSlice:
  def _july(self, session, **kwargs):
    return get_live_financial_statement(
      session,
      graph_id=GRAPH,
      statement_type="income_statement",
      period_start=JULY[0],
      period_end=JULY[1],
      **kwargs,
    )

  def test_the_income_statement_by_member_ties_out_to_its_total(self, books):
    chart_id, sales, ops = books
    with extensions_session(GRAPH) as session:
      _fees(session, chart_id, 50_000, [sales])
      _fees(session, chart_id, 20_000, [ops])
      _fees(session, chart_id, 10_000, None)

      response = self._july(session, dimension=StatementDimension(type="department"))

      assert [c.name for c in response.dimension.columns] == [
        "Operations",
        "Sales",
        "Unallocated",
        "Total",
      ]
      assert [p.label for p in response.periods] == [
        "Operations",
        "Sales",
        "Unallocated",
        "Total",
      ]
      revenue = next(f.values for f in response.facts if f.qname == REVENUE)
      assert revenue == [200.0, 500.0, 100.0, 800.0]
      assert response.dimension.ties_out is True
      assert response.dimension.differences == []
      undimensioned = self._july(session)
      assert _revenue(undimensioned) == 800.0

  def test_one_member_reads_current_and_prior(self, books):
    chart_id, sales, _ops = books
    with extensions_session(GRAPH) as session:
      _fees(session, chart_id, 50_000, [sales])
      _fees(session, chart_id, 10_000, None)

      sales_only = self._july(
        session, dimension=StatementDimension(type="department", member_id=sales)
      )
      untagged = self._july(
        session,
        dimension=StatementDimension(type="department", member_id="unallocated"),
      )

      assert len(sales_only.periods) == len(self._july(session).periods)
      assert _revenue(sales_only) == 500.0
      assert _revenue(untagged) == 100.0
      assert sales_only.dimension.member_id == sales
      assert sales_only.dimension.ties_out is None

  def test_a_slice_of_any_other_statement_or_a_foreign_member_is_refused(self, books):
    _chart_id, sales, _ops = books
    with extensions_session(GRAPH) as session:
      with pytest.raises(DimensionalStatementError, match="income statement only"):
        get_live_financial_statement(
          session,
          graph_id=GRAPH,
          statement_type="balance_sheet",
          period_start=JULY[0],
          period_end=JULY[1],
          dimension=StatementDimension(type="department"),
        )
      with pytest.raises(DimensionalStatementError, match="No location member"):
        self._july(
          session,
          dimension=StatementDimension(type="location", member_id=sales),
        )

  def test_the_trial_balance_sums_one_members_lines(self, books):
    chart_id, sales, _ops = books
    with extensions_session(GRAPH) as session:
      _fees(session, chart_id, 50_000, [sales])
      _fees(session, chart_id, 10_000, None)

      by_code = {
        row.account_code: row.net_balance
        for row in get_trial_balance(
          session,
          JULY[0],
          JULY[1],
          entity_id=PARENT,
          line_slice=LineSlice("department", sales),
        ).rows
      }
      untagged = {
        row.account_code: row.net_balance
        for row in get_trial_balance(
          session,
          JULY[0],
          JULY[1],
          entity_id=PARENT,
          line_slice=LineSlice("department"),
        ).rows
      }

      assert by_code == {"4000": -500.0}
      assert untagged == {"1000": 600.0, "4000": -100.0}
