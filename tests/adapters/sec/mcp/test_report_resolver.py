"""Tests for SEC adapter's report resolver.

Verifies:
- form-code selection by period_type
- fiscal_year filter threading
- None on no match, and a raised SECReportResolutionError on lookup failure
  (the two must stay distinguishable — callers fall back to an unscoped
  sweep on None, which is only safe when the query actually ran)
"""

from __future__ import annotations

import json
import time
from unittest.mock import AsyncMock, patch

import pytest

from robosystems.adapters.sec.mcp.report_resolver import (
  ANNUAL_FORMS,
  QUARTERLY_FORMS,
  SECReportResolutionError,
  _renamed_cache,
  build_sec_report_query,
  renamed_filer_cik,
  renamed_filer_cik_sync,
  resolve_sec_report,
)

MOCK_GRAPH = "sec"
S3 = "robosystems.adapters.sec.mcp.report_resolver.S3Client"


@pytest.fixture(autouse=True)
def catalog():
  """No catalog document unless a test seeds one; never a real bucket."""
  _renamed_cache.clear()
  with patch(S3) as client:
    client.return_value.read_string.return_value = None
    yield client.return_value
  _renamed_cache.clear()


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
    # The filing's coordinates and its document ride along, so the views
    # can say where the filing is served without a second read.
    assert "r.accession_number AS accession" in query
    assert "ent.cik AS cik" in query
    assert "r.uri AS uri, r.is_inline_xbrl AS is_inline_xbrl" in query
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


@pytest.mark.unit
class TestRenamedFilers:
  def _seed(self, catalog, **document):
    catalog.read_string.return_value = json.dumps(document)

  def test_an_old_ticker_resolves_to_the_filers_cik(self, catalog):
    self._seed(catalog, cik="0001390777", moved_to="BNY")
    assert renamed_filer_cik_sync("bk") == "0001390777"

  def test_a_new_ticker_resolves_to_the_filers_cik(self, catalog):
    self._seed(catalog, cik="0001390777", former_tickers=["BK"])
    assert renamed_filer_cik_sync("BNY") == "0001390777"

  def test_an_unrenamed_filer_keeps_the_ticker_path(self, catalog):
    self._seed(catalog, cik="0000034088", filings=[])
    assert renamed_filer_cik_sync("XOM") is None

  def test_a_ticker_with_no_document_keeps_the_ticker_path(self):
    assert renamed_filer_cik_sync("NONE") is None

  def test_a_catalog_that_cannot_be_read_keeps_the_ticker_path(self, catalog):
    catalog.read_string.side_effect = RuntimeError("503")
    assert renamed_filer_cik_sync("BK") is None

  def test_a_failed_read_is_remembered_only_briefly(self, catalog):
    catalog.read_string.side_effect = RuntimeError("503")
    assert renamed_filer_cik_sync("BK") is None
    assert renamed_filer_cik_sync("BK") is None
    assert catalog.read_string.call_count == 1

    catalog.read_string.side_effect = None
    self._seed(catalog, cik="0001390777", moved_to="BNY")
    expires, cached = _renamed_cache["BK"]
    assert cached is None
    assert expires - time.monotonic() <= 30
    _renamed_cache["BK"] = (time.monotonic() - 1, None)
    assert renamed_filer_cik_sync("BK") == "0001390777"

  def test_a_lookup_is_cached(self, catalog):
    self._seed(catalog, cik="0001390777", moved_to="BNY")
    renamed_filer_cik_sync("BK")
    renamed_filer_cik_sync("BK")
    assert catalog.read_string.call_count == 1

  @pytest.mark.parametrize("symbol", ["../secret", "A B", "0001390777", ""])
  def test_what_is_not_a_ticker_reads_nothing(self, catalog, symbol):
    assert renamed_filer_cik_sync(symbol) is None
    catalog.read_string.assert_not_called()

  @pytest.mark.asyncio
  async def test_only_the_sec_repository_looks_up(self, catalog):
    self._seed(catalog, cik="0001390777", moved_to="BNY")
    assert await renamed_filer_cik("kg123", "BK") is None
    catalog.read_string.assert_not_called()

  def test_the_query_matches_a_renamed_filer_by_cik(self):
    query, parameters = build_sec_report_query(ticker="BK", cik="0001390777")
    assert "ent.cik = $cik" in query
    assert "ent.ticker" not in query.split("RETURN")[0]
    assert parameters["cik"] == "0001390777"
    assert "ticker" not in parameters

  def test_the_query_otherwise_matches_by_ticker(self):
    query, parameters = build_sec_report_query(ticker="NVDA")
    assert "ent.ticker = $ticker" in query
    assert parameters["ticker"] == "NVDA"

  @pytest.mark.asyncio
  async def test_resolving_a_renamed_filer_goes_by_cik(self, catalog, mock_repository):
    self._seed(catalog, cik="0001390777", moved_to="BNY")
    mock_repository.execute_query.return_value = [{"identifier": "r"}]
    with patch(
      "robosystems.adapters.sec.mcp.report_resolver.get_graph_repository",
      AsyncMock(return_value=mock_repository),
    ):
      await resolve_sec_report(MOCK_GRAPH, ticker="BK")
    query, parameters = mock_repository.execute_query.call_args.args
    assert "ent.cik = $cik" in query
    assert parameters["cik"] == "0001390777"
