"""Tests for SEC adapter's report resolver.

Verifies:
- form-code selection by period_type
- fiscal_year filter threading
- None on no match, and a raised SECReportResolutionError on lookup failure
  (the two must stay distinguishable — callers fall back to an unscoped
  sweep on None, which is only safe when the query actually ran)
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from robosystems.adapters.sec.mcp.report_resolver import (
  ANNUAL_FORMS,
  QUARTERLY_FORMS,
  SECReportResolutionError,
  resolve_sec_report,
)

MOCK_GRAPH = "sec"


@pytest.fixture
def mock_repository():
  repo = AsyncMock()
  repo.execute_query = AsyncMock(return_value=[])
  return repo


class TestResolveSecReport:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_default_period_type_uses_annual_forms(self, mock_repository):
    with patch(
      "robosystems.adapters.sec.mcp.report_resolver.get_graph_repository",
      return_value=mock_repository,
    ):
      await resolve_sec_report(MOCK_GRAPH, ticker="NVDA")
    _, params = mock_repository.execute_query.call_args[0]
    assert params["forms"] == list(ANNUAL_FORMS)
    assert params["ticker"] == "NVDA"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_quarterly_period_type_uses_quarterly_forms(self, mock_repository):
    with patch(
      "robosystems.adapters.sec.mcp.report_resolver.get_graph_repository",
      return_value=mock_repository,
    ):
      await resolve_sec_report(MOCK_GRAPH, ticker="NVDA", period_type="quarterly")
    _, params = mock_repository.execute_query.call_args[0]
    assert params["forms"] == list(QUARTERLY_FORMS)

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_instant_period_type_uses_quarterly_forms(self, mock_repository):
    """Instant uses quarterly-form set (balance sheets appear in both)."""
    with patch(
      "robosystems.adapters.sec.mcp.report_resolver.get_graph_repository",
      return_value=mock_repository,
    ):
      await resolve_sec_report(MOCK_GRAPH, ticker="NVDA", period_type="instant")
    _, params = mock_repository.execute_query.call_args[0]
    assert params["forms"] == list(QUARTERLY_FORMS)

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_routes_to_read_endpoint(self, mock_repository):
    """Read-only resolution must route to the read endpoint (replica ALB on the
    shared SEC repo). The default "write" path resolves the shared master
    (DynamoDB discovery + retry) and times out the MCP tool. Regression guard."""
    with patch(
      "robosystems.adapters.sec.mcp.report_resolver.get_graph_repository",
      return_value=mock_repository,
    ) as mock_get_repo:
      await resolve_sec_report(MOCK_GRAPH, ticker="NVDA")
    assert mock_get_repo.call_args.kwargs.get("operation_type") == "read"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_fiscal_year_filter_threaded(self, mock_repository):
    with patch(
      "robosystems.adapters.sec.mcp.report_resolver.get_graph_repository",
      return_value=mock_repository,
    ):
      await resolve_sec_report(MOCK_GRAPH, ticker="NVDA", fiscal_year=2025)
    query, params = mock_repository.execute_query.call_args[0]
    assert "r.fiscal_year_focus = $fiscal_year" in query
    assert params["fiscal_year"] == 2025

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_no_match_returns_none(self, mock_repository):
    mock_repository.execute_query.return_value = []
    with patch(
      "robosystems.adapters.sec.mcp.report_resolver.get_graph_repository",
      return_value=mock_repository,
    ):
      result = await resolve_sec_report(MOCK_GRAPH, ticker="XYZ")
    assert result is None

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_match_returns_first_row(self, mock_repository):
    mock_repository.execute_query.return_value = [
      {
        "identifier": "rpt_123",
        "form": "10-K",
        "filing_date": "2025-09-01",
        "fiscal_year": 2025,
        "fiscal_period": "FY",
      }
    ]
    with patch(
      "robosystems.adapters.sec.mcp.report_resolver.get_graph_repository",
      return_value=mock_repository,
    ):
      result = await resolve_sec_report(MOCK_GRAPH, ticker="NVDA")
    assert result is not None
    assert result["identifier"] == "rpt_123"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_repository_error_raises_not_returns_none(self):
    """A failed lookup must be distinguishable from an empty one.

    This previously returned None on any exception. Callers treat None as
    "no such filing" and fall back to an unscoped ticker sweep, so a
    transient graph error silently became a confident answer about the
    wrong fiscal year. The swallow was the defect; the old test asserted it.
    """
    with patch(
      "robosystems.adapters.sec.mcp.report_resolver.get_graph_repository",
      side_effect=RuntimeError("repo down"),
    ):
      with pytest.raises(SECReportResolutionError):
        await resolve_sec_report(MOCK_GRAPH, ticker="NVDA")

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_query_error_also_raises(self, mock_repository):
    """The failure can come from execute_query, not just repo acquisition."""
    mock_repository.execute_query.side_effect = RuntimeError("query timeout")
    with patch(
      "robosystems.adapters.sec.mcp.report_resolver.get_graph_repository",
      return_value=mock_repository,
    ):
      with pytest.raises(SECReportResolutionError):
        await resolve_sec_report(MOCK_GRAPH, ticker="NVDA")
