"""Tests for the graph-backed financial-statement query.

Verifies Cypher construction for:
- report_id fast path vs ticker fallback path
- period_type filters (annual/quarterly/instant + balance-sheet default)
- dedup keeps first occurrence per full period identity (qname, start, end, type, bucket)
- input validation (at least one of report_id / ticker)
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from robosystems.operations.roboledger.views.financial_statement_query import (
  deduplicate_facts,
  query_financial_statement,
)

MOCK_GRAPH = "kg_test"


@pytest.fixture
def mock_repository():
  repo = AsyncMock()
  repo.execute_query = AsyncMock(return_value=[])
  return repo


class TestQueryFinancialStatement:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_requires_report_id_or_ticker(self):
    with pytest.raises(ValueError):
      await query_financial_statement(
        MOCK_GRAPH,
        statement_type="income_statement",
      )

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_report_id_fast_path(self, mock_repository):
    with patch(
      "robosystems.operations.roboledger.views.financial_statement_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_financial_statement(
        MOCK_GRAPH,
        statement_type="income_statement",
        report_id="rpt_abc",
      )

    query, params = mock_repository.execute_query.call_args[0]
    assert "r:Report" in query
    assert "report_id" in params and params["report_id"] == "rpt_abc"
    # ticker path shouldn't be present
    assert "ent.ticker" not in query or "$ticker" not in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_routes_to_read_endpoint(self, mock_repository):
    """This is a read-only analytical query: it must acquire the repository
    with operation_type="read". On shared repos that routes to the replica ALB;
    the default "write" path resolves the shared master (DynamoDB discovery +
    retry) and times out the MCP tool. Regression guard for that bug."""
    with patch(
      "robosystems.operations.roboledger.views.financial_statement_query.get_graph_repository",
      return_value=mock_repository,
    ) as mock_get_repo:
      await query_financial_statement(
        MOCK_GRAPH,
        statement_type="income_statement",
        report_id="rpt_abc",
      )

    assert mock_get_repo.call_args.kwargs.get("operation_type") == "read"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_ticker_path(self, mock_repository):
    with patch(
      "robosystems.operations.roboledger.views.financial_statement_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_financial_statement(
        MOCK_GRAPH,
        statement_type="income_statement",
        ticker="NVDA",
      )

    query, params = mock_repository.execute_query.call_args[0]
    assert "ent.ticker" in query or "$ticker" in query
    assert params["ticker"] == "NVDA"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_balance_sheet_defaults_to_instant_period(self, mock_repository):
    with patch(
      "robosystems.operations.roboledger.views.financial_statement_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_financial_statement(
        MOCK_GRAPH,
        statement_type="balance_sheet",
        report_id="rpt_bs",
      )
    query, _ = mock_repository.execute_query.call_args[0]
    assert "period_type: 'instant'" in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_projects_the_full_period_identity(self, mock_repository):
    """start_date must be projected or the dedup key cannot see it."""
    with patch(
      "robosystems.operations.roboledger.views.financial_statement_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_financial_statement(
        MOCK_GRAPH, statement_type="income_statement", ticker="NVDA"
      )
    query, _ = mock_repository.execute_query.call_args[0]
    for col in (
      "p.start_date AS start_date",
      "p.end_date AS end_date",
      "p.duration_type AS duration_type",
    ):
      assert col in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_annual_period_filter(self, mock_repository):
    with patch(
      "robosystems.operations.roboledger.views.financial_statement_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_financial_statement(
        MOCK_GRAPH,
        statement_type="income_statement",
        report_id="rpt_inc",
        period_type="annual",
      )
    query, _ = mock_repository.execute_query.call_args[0]
    assert "duration_type: 'annual'" in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_fetch_limit_triple_up_to_1000(self, mock_repository):
    with patch(
      "robosystems.operations.roboledger.views.financial_statement_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_financial_statement(
        MOCK_GRAPH,
        statement_type="income_statement",
        report_id="rpt_x",
        limit=400,
      )
    _, params = mock_repository.execute_query.call_args[0]
    # 400 * 3 = 1200, clamped to 1000
    assert params["limit"] == 1000


class TestDeduplicateFacts:
  @pytest.mark.unit
  def test_keeps_first_per_qname_end_date(self):
    rows = [
      {"qname": "us-gaap:Assets", "end_date": "2025-12-31", "value": 100},
      {"qname": "us-gaap:Assets", "end_date": "2025-12-31", "value": 999},  # dup
      {"qname": "us-gaap:Assets", "end_date": "2024-12-31", "value": 80},
      {"qname": "us-gaap:Revenue", "end_date": "2025-12-31", "value": 50},
    ]
    deduped = deduplicate_facts(rows)
    assert len(deduped) == 3
    # First occurrence wins (value 100, not 999).
    assets_2025 = next(
      r
      for r in deduped
      if r["qname"] == "us-gaap:Assets" and r["end_date"] == "2025-12-31"
    )
    assert assets_2025["value"] == 100

  @pytest.mark.unit
  def test_q4_and_fy_sharing_an_end_date_both_survive(self):
    """The 2026-08-14 case: same qname and end_date, different duration_type."""
    rows = [
      {
        "qname": "us-gaap:Revenues",
        "start_date": "2024-02-01",
        "end_date": "2025-01-31",
        "period_type": "duration",
        "duration_type": "annual",
        "value": 400,
      },
      {
        "qname": "us-gaap:Revenues",
        "start_date": "2024-11-01",
        "end_date": "2025-01-31",
        "period_type": "duration",
        "duration_type": "quarterly",
        "value": 100,
      },
    ]
    assert [r["value"] for r in deduplicate_facts(rows)] == [400, 100]

  @pytest.mark.unit
  def test_distinct_periods_within_one_duration_bucket_both_survive(self):
    """duration_type is a coarse bucket. Two periods that share qname,
    end_date, period_type AND bucket but start on different days are still
    two facts — a two-month and a five-month stub both classified ``other``,
    or a 52- and a 53-week year with one fiscal year end. Only start_date
    tells them apart, so it has to be in the key."""
    rows = [
      {
        "qname": "us-gaap:Revenues",
        "start_date": "2024-11-01",
        "end_date": "2024-12-31",
        "period_type": "duration",
        "duration_type": "other",
        "value": 20,
      },
      {
        "qname": "us-gaap:Revenues",
        "start_date": "2024-08-01",
        "end_date": "2024-12-31",
        "period_type": "duration",
        "duration_type": "other",
        "value": 50,
      },
      {
        "qname": "us-gaap:Revenues",
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "period_type": "duration",
        "duration_type": "annual",
        "value": 120,
      },
      {
        "qname": "us-gaap:Revenues",
        "start_date": "2023-12-31",
        "end_date": "2024-12-31",
        "period_type": "duration",
        "duration_type": "annual",
        "value": 121,
      },
    ]
    assert [r["value"] for r in deduplicate_facts(rows)] == [20, 50, 120, 121]

  @pytest.mark.unit
  def test_same_period_from_two_filings_still_collapses(self):
    """The dedup's actual job: one period reported twice keeps the first row."""
    rows = [
      {
        "qname": "us-gaap:Assets",
        "start_date": None,
        "end_date": "2024-12-31",
        "period_type": "instant",
        "duration_type": None,
        "value": 100,
      },
      {
        "qname": "us-gaap:Assets",
        "start_date": None,
        "end_date": "2024-12-31",
        "period_type": "instant",
        "duration_type": None,
        "value": 101,
      },
    ]
    assert [r["value"] for r in deduplicate_facts(rows)] == [100]

  @pytest.mark.unit
  def test_empty_input(self):
    assert deduplicate_facts([]) == []

  @pytest.mark.unit
  def test_missing_keys_treated_as_empty_string(self):
    rows = [{"value": 1}, {"value": 2}]
    deduped = deduplicate_facts(rows)
    # Both map to (""", "") — first wins.
    assert len(deduped) == 1

  @pytest.mark.unit
  def test_same_period_reported_at_two_precisions_keeps_the_precise_one(self):
    """3M FY2024 R&D: 1,085 (decimals -6) on the income statement and 1,100
    (decimals -8) in the narrative share the element, period and context.
    The statement figure must win in either row order; before 2026-09-03
    the first row the engine returned won, and the tool answered 1,100."""
    statement = {
      "qname": "us-gaap:ResearchAndDevelopmentExpense",
      "start_date": "2024-01-01",
      "end_date": "2024-12-31",
      "period_type": "duration",
      "duration_type": "annual",
      "value": 1_085_000_000,
      "decimals": "-6",
    }
    narrative = {**statement, "value": 1_100_000_000, "decimals": "-8"}
    assert [r["value"] for r in deduplicate_facts([statement, narrative])] == [
      1_085_000_000
    ]
    assert [r["value"] for r in deduplicate_facts([narrative, statement])] == [
      1_085_000_000
    ]


class TestProjectsDecimals:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_decimals_is_projected_for_the_dedup(self, mock_repository):
    """The dedup ranks survivors by decimals, so the query must project it."""
    with patch(
      "robosystems.operations.roboledger.views.financial_statement_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_financial_statement(
        MOCK_GRAPH, statement_type="income_statement", ticker="MMM"
      )
    query, _ = mock_repository.execute_query.call_args[0]
    assert "f.decimals AS decimals" in query
