"""Tests for the graph-backed financial-statement query.

Verifies Cypher construction for:
- report_id fast path vs ticker fallback path
- period_type filters (annual/quarterly/instant + balance-sheet default)
- dedup keeps first occurrence per (qname, end_date)
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
  def test_empty_input(self):
    assert deduplicate_facts([]) == []

  @pytest.mark.unit
  def test_missing_keys_treated_as_empty_string(self):
    rows = [{"value": 1}, {"value": 2}]
    deduped = deduplicate_facts(rows)
    # Both map to (""", "") — first wins.
    assert len(deduped) == 1
