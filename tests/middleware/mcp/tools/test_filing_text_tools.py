"""Tests for the ``describe-filing``, ``search-text`` and ``read-text`` MCP tools.

Thin over the ops layer: resolve the filing, call the view, turn domain errors
into ``{"error": ...}``. The views are covered in
``tests/operations/roboledger/views/test_filing_text.py``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.filing_text_tools import (
  DescribeFilingTool,
  ReadTextTool,
  SearchTextTool,
)
from robosystems.operations.roboledger.views.filing_text import (
  READ_MAX_LENGTH,
  SEARCH_MAX_HITS,
  SEARCH_MAX_WINDOW,
  FilingRef,
  QueryError,
)
from robosystems.operations.roboledger.views.information_blocks import (
  ReportNotFoundError,
)

MODULE = "robosystems.middleware.mcp.tools.filing_text_tools"
SELECTORS = {"ticker", "report_id", "fiscal_year", "period_type", "accession", "form"}


def _client(graph_id: str = "sec"):
  client = MagicMock()
  client.graph_id = graph_id
  return client


class TestDefinitions:
  @pytest.mark.unit
  def test_names_and_inputs(self):
    describe = DescribeFilingTool(_client()).get_tool_definition()
    search = SearchTextTool(_client()).get_tool_definition()
    read = ReadTextTool(_client()).get_tool_definition()
    assert [describe["name"], search["name"], read["name"]] == [
      "describe-filing",
      "search-text",
      "read-text",
    ]
    assert set(describe["inputSchema"]["properties"]) == SELECTORS
    assert set(search["inputSchema"]["properties"]) == SELECTORS | {
      "query",
      "window",
      "max_hits",
    }
    assert search["inputSchema"]["required"] == ["query"]
    assert set(read["inputSchema"]["properties"]) == SELECTORS | {"offset", "length"}

  @pytest.mark.unit
  def test_search_text_routes_against_search_documents(self):
    text = SearchTextTool(_client()).get_tool_definition()["description"]
    assert "search-documents" in text
    assert "Not a regular expression" in text


@pytest.mark.asyncio
class TestExecute:
  @pytest.mark.unit
  async def test_search_resolves_then_searches(self):
    ref = FilingRef(
      report_id="rpt_abc", resolved={"report_id": "rpt_abc", "form": "10-K"}
    )
    with (
      patch(f"{MODULE}.resolve_filing", new=AsyncMock(return_value=ref)) as resolve,
      patch(
        f"{MODULE}.query_search_text",
        new=AsyncMock(return_value={"graph_id": "sec", "total": 2, "hits": []}),
      ) as search,
    ):
      out = await SearchTextTool(_client()).execute(
        {"query": "going concern", "ticker": " nvda ", "max_hits": 999}
      )
    assert resolve.await_args.kwargs["ticker"] == "nvda"
    assert search.await_args.args == ("sec", ref, "going concern")
    assert search.await_args.kwargs == {"window": None, "max_hits": SEARCH_MAX_HITS}
    assert out["total"] == 2
    assert out["resolved_report"]["report_id"] == "rpt_abc"

  @pytest.mark.unit
  async def test_window_is_capped(self):
    with (
      patch(
        f"{MODULE}.resolve_filing", new=AsyncMock(return_value=FilingRef(report_id="r"))
      ),
      patch(f"{MODULE}.query_search_text", new=AsyncMock(return_value={})) as search,
    ):
      await SearchTextTool(_client()).execute({"query": "x", "window": 10_000})
    assert search.await_args.kwargs["window"] == SEARCH_MAX_WINDOW

  @pytest.mark.unit
  async def test_an_8k_stamps_its_own_resolution(self):
    ref = FilingRef(accession="a", resolved={"accession": "a", "form": "8-K"})
    with (
      patch(f"{MODULE}.resolve_filing", new=AsyncMock(return_value=ref)),
      patch(
        f"{MODULE}.query_read_text", new=AsyncMock(return_value={"text": "t"})
      ) as read,
    ):
      out = await ReadTextTool(_client()).execute(
        {"accession": "a", "offset": 10, "length": 99_999}
      )
    assert read.await_args.kwargs == {"offset": 10, "length": READ_MAX_LENGTH}
    assert out["resolved_report"] == {"accession": "a", "form": "8-K"}

  @pytest.mark.unit
  async def test_query_is_required(self):
    assert "error" in await SearchTextTool(_client()).execute({"query": "  "})

  @pytest.mark.unit
  async def test_bad_numbers_are_an_error(self):
    out = await ReadTextTool(_client()).execute({"offset": "abc"})
    assert "error" in out

  @pytest.mark.unit
  @pytest.mark.parametrize("exc", [ReportNotFoundError("gone"), QueryError("empty")])
  async def test_domain_errors_become_error_payloads(self, exc):
    with patch(f"{MODULE}.resolve_filing", new=AsyncMock(side_effect=exc)):
      out = await DescribeFilingTool(_client()).execute({"ticker": "X"})
    assert out == {"error": str(exc)}
