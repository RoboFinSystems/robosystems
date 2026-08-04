"""Tests for query_fact_grid — the shared ops-layer Cypher query used by
both the REST `build-fact-grid` operation and the MCP tool.

Covers LadybugDB-specific behaviors:

- Inline node anchoring for single-element / single-ticker filters (fast
  path, avoids full Fact scans).
- Parameterized fallback for multi-value / non-ticker filters.
- `DISTINCT` and `ORDER BY` intentionally omitted (both broken together
  in LadybugDB) — dedup + sort happen in Python.
- `has_dimensions = false` stays in WHERE (inline boolean filters
  silently return zero results in LadybugDB).
"""

from unittest.mock import AsyncMock, patch

import pytest

from robosystems.operations.roboledger.views.fact_query import (
  _deduplicate_fact_rows,
  _is_ticker,
  _safe_str,
  query_fact_grid,
)

MOCK_GRAPH_ID = "kg_test123"
PATCH_REPO = "robosystems.operations.roboledger.views.fact_query.get_graph_repository"


@pytest.fixture
def mock_repository():
  repo = AsyncMock()
  repo.execute_query = AsyncMock(return_value=[])
  return repo


# ---------------------------------------------------------------------------
# Inline sanitization helpers
# ---------------------------------------------------------------------------


class TestSafeStr:
  @pytest.mark.unit
  def test_accepts_qname(self):
    assert _safe_str("us-gaap:Assets") == "us-gaap:Assets"

  @pytest.mark.unit
  def test_accepts_canonical_concept(self):
    assert _safe_str("total_assets") == "total_assets"

  @pytest.mark.unit
  def test_rejects_injection_attempts(self):
    assert _safe_str("foo'; DROP TABLE facts --") is None
    assert _safe_str("foo bar") is None  # space
    assert _safe_str("foo}bar") is None  # brace


class TestIsTicker:
  @pytest.mark.unit
  def test_accepts_standard_ticker(self):
    assert _is_ticker("NVDA") is True

  @pytest.mark.unit
  def test_accepts_dotted_ticker(self):
    assert _is_ticker("BRK.B") is True
    assert _is_ticker("BF-B") is True

  @pytest.mark.unit
  def test_rejects_cik(self):
    assert _is_ticker("0000320193") is False  # all digits — CIK

  @pytest.mark.unit
  def test_rejects_company_name(self):
    assert _is_ticker("Apple Inc.") is False  # contains space


# ---------------------------------------------------------------------------
# query_fact_grid — Cypher construction
# ---------------------------------------------------------------------------


class TestQueryFactGrid:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_routes_to_read_endpoint(self, mock_repository):
    """Read-only analytical query: must acquire the repository with
    operation_type="read" so shared repos (SEC) route to the replica ALB.
    The default "write" path resolves the shared master (DynamoDB discovery +
    retry) and times out the MCP tool. Regression guard for that bug."""
    with patch(PATCH_REPO, return_value=mock_repository) as mock_get_repo:
      await query_fact_grid(MOCK_GRAPH_ID, elements=["us-gaap:Assets"])
    assert mock_get_repo.call_args.kwargs.get("operation_type") == "read"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_single_element_uses_inline_anchor(self, mock_repository):
    """Single qname → inline `(el:Element {qname: 'x'})` for fast lookup."""
    with patch(PATCH_REPO, return_value=mock_repository):
      await query_fact_grid(MOCK_GRAPH_ID, elements=["us-gaap:Assets"])

    query = mock_repository.execute_query.call_args[0][0]
    params = mock_repository.execute_query.call_args[0][1]
    assert "(el:Element {qname: 'us-gaap:Assets'})" in query
    # No parameterized fallback when inline anchoring succeeded.
    assert "elements" not in params
    assert "el.qname IN $elements" not in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_multiple_elements_use_parameterized_where(self, mock_repository):
    """Multi-element → fall back to `el.qname IN $elements`."""
    with patch(PATCH_REPO, return_value=mock_repository):
      await query_fact_grid(
        MOCK_GRAPH_ID, elements=["us-gaap:Assets", "us-gaap:Liabilities"]
      )

    query = mock_repository.execute_query.call_args[0][0]
    params = mock_repository.execute_query.call_args[0][1]
    assert "el.qname IN $elements" in query
    assert params["elements"] == ["us-gaap:Assets", "us-gaap:Liabilities"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_single_canonical_concept_uses_inline_anchor(self, mock_repository):
    with patch(PATCH_REPO, return_value=mock_repository):
      await query_fact_grid(MOCK_GRAPH_ID, canonical_concepts=["revenue"])

    query = mock_repository.execute_query.call_args[0][0]
    assert "(el:Element {canonical_concept: 'revenue'})" in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_canonical_concept_multi_uses_parameterized_where(
    self, mock_repository
  ):
    with patch(PATCH_REPO, return_value=mock_repository):
      await query_fact_grid(MOCK_GRAPH_ID, canonical_concepts=["revenue", "net_income"])
    query = mock_repository.execute_query.call_args[0][0]
    params = mock_repository.execute_query.call_args[0][1]
    assert "el.canonical_concept IN $canonical_concepts" in query
    assert params["canonical_concepts"] == ["revenue", "net_income"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_combined_elements_and_concepts_use_or(self, mock_repository):
    with patch(PATCH_REPO, return_value=mock_repository):
      await query_fact_grid(
        MOCK_GRAPH_ID,
        elements=["us-gaap:Assets"],
        canonical_concepts=["revenue"],
      )
    query = mock_repository.execute_query.call_args[0][0]
    assert (
      "el.qname IN $elements OR el.canonical_concept IN $canonical_concepts" in query
    )

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_period_type_annual(self, mock_repository):
    with patch(PATCH_REPO, return_value=mock_repository):
      await query_fact_grid(
        MOCK_GRAPH_ID, elements=["us-gaap:NetIncomeLoss"], period_type="annual"
      )
    query = mock_repository.execute_query.call_args[0][0]
    assert "p.duration_type = 'annual'" in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_period_type_instant(self, mock_repository):
    with patch(PATCH_REPO, return_value=mock_repository):
      await query_fact_grid(
        MOCK_GRAPH_ID, elements=["us-gaap:Assets"], period_type="instant"
      )
    query = mock_repository.execute_query.call_args[0][0]
    assert "p.period_type = 'instant'" in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_ticker_entity_uses_inline_anchor(self, mock_repository):
    """Single ticker → inline `(ent:Entity {ticker: 'NVDA'})`."""
    with patch(PATCH_REPO, return_value=mock_repository):
      await query_fact_grid(MOCK_GRAPH_ID, elements=["us-gaap:Assets"], entity="NVDA")

    query = mock_repository.execute_query.call_args[0][0]
    params = mock_repository.execute_query.call_args[0][1]
    assert "(ent:Entity {ticker: 'NVDA'})" in query
    # No parameterized fallback when inline succeeded.
    assert "entities" not in params

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_cik_entity_uses_parameterized_where(self, mock_repository):
    """CIKs (digit-only) → parameterized WHERE, not inline."""
    with patch(PATCH_REPO, return_value=mock_repository):
      await query_fact_grid(
        MOCK_GRAPH_ID, elements=["us-gaap:Assets"], entity="0000320193"
      )
    query = mock_repository.execute_query.call_args[0][0]
    params = mock_repository.execute_query.call_args[0][1]
    assert "ent.ticker IN $entities" in query
    assert params["entities"] == ["0000320193"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_multi_entity_uses_parameterized_where(self, mock_repository):
    with patch(PATCH_REPO, return_value=mock_repository):
      await query_fact_grid(
        MOCK_GRAPH_ID, elements=["us-gaap:Assets"], entities=["NVDA", "AAPL"]
      )
    query = mock_repository.execute_query.call_args[0][0]
    params = mock_repository.execute_query.call_args[0][1]
    assert "ent.ticker IN $entities" in query
    assert params["entities"] == ["NVDA", "AAPL"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_report_filters(self, mock_repository):
    with patch(PATCH_REPO, return_value=mock_repository):
      await query_fact_grid(
        MOCK_GRAPH_ID,
        elements=["us-gaap:Assets"],
        form="10-K",
        fiscal_year=2024,
        fiscal_period="FY",
      )
    query = mock_repository.execute_query.call_args[0][0]
    params = mock_repository.execute_query.call_args[0][1]
    assert "REPORT_HAS_FACT" in query
    assert params["form"] == "10-K"
    assert params["fiscal_year"] == 2024
    assert params["fiscal_period"] == "FY"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_has_dimensions_always_false(self, mock_repository):
    with patch(PATCH_REPO, return_value=mock_repository):
      await query_fact_grid(MOCK_GRAPH_ID, elements=["us-gaap:Assets"])
    query = mock_repository.execute_query.call_args[0][0]
    assert "f.has_dimensions = false" in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_query_omits_distinct_and_order_by(self, mock_repository):
    """LadybugDB quirk: DISTINCT + ORDER BY returns empty. Dedup happens in Python."""
    with patch(PATCH_REPO, return_value=mock_repository):
      await query_fact_grid(MOCK_GRAPH_ID, elements=["us-gaap:Assets"])
    query = mock_repository.execute_query.call_args[0][0]
    assert "DISTINCT" not in query
    assert "ORDER BY" not in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_entity_columns_included_when_filtered(self, mock_repository):
    with patch(PATCH_REPO, return_value=mock_repository):
      await query_fact_grid(MOCK_GRAPH_ID, elements=["us-gaap:Assets"], entity="NVDA")
    query = mock_repository.execute_query.call_args[0][0]
    assert "entity_ticker" in query
    assert "entity_name" in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_entity_columns_included_without_filter(self, mock_repository):
    """Entity identity is always returned — without it facts from different
    filers are indistinguishable."""
    with patch(PATCH_REPO, return_value=mock_repository):
      await query_fact_grid(MOCK_GRAPH_ID, elements=["us-gaap:Assets"])
    query = mock_repository.execute_query.call_args[0][0]
    assert "entity_ticker" in query
    assert "entity_name" in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_leads_with_element_anchor_without_entity_filter(self, mock_repository):
    """Never lead with (f:Fact) — that full-scans the fact table."""
    with patch(PATCH_REPO, return_value=mock_repository):
      await query_fact_grid(MOCK_GRAPH_ID, elements=["us-gaap:Assets"])
    query = mock_repository.execute_query.call_args[0][0]
    assert query.startswith("MATCH (el:Element {qname: 'us-gaap:Assets'})")
    assert "MATCH (f:Fact)" not in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_leads_with_entity_anchor_when_filtered(self, mock_repository):
    with patch(PATCH_REPO, return_value=mock_repository):
      await query_fact_grid(MOCK_GRAPH_ID, elements=["us-gaap:Assets"], entity="NVDA")
    query = mock_repository.execute_query.call_args[0][0]
    assert query.startswith("MATCH (ent:Entity {ticker: 'NVDA'})")

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_empty_results_returns_empty_list(self, mock_repository):
    mock_repository.execute_query.return_value = []
    with patch(PATCH_REPO, return_value=mock_repository):
      result, truncated = await query_fact_grid(
        MOCK_GRAPH_ID, elements=["us-gaap:Assets"]
      )

    assert result == []
    assert truncated is False

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_periods_filter(self, mock_repository):
    with patch(PATCH_REPO, return_value=mock_repository):
      await query_fact_grid(
        MOCK_GRAPH_ID,
        elements=["us-gaap:Assets"],
        periods=["2024-12-31", "2025-01-26"],
      )
    query = mock_repository.execute_query.call_args[0][0]
    params = mock_repository.execute_query.call_args[0][1]
    assert "p.end_date IN $periods" in query
    assert params["periods"] == ["2024-12-31", "2025-01-26"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_dedup_applied_to_results(self, mock_repository):
    """Duplicate rows (same element+period) collapse; first row wins."""
    mock_repository.execute_query.return_value = [
      {
        "element_id": "us-gaap:Assets",
        "element_name": "Assets",
        "period_end": "2025-12-31",
        "value": 100.0,
        "unit": "USD",
      },
      {
        "element_id": "us-gaap:Assets",
        "element_name": "Assets",
        "period_end": "2025-12-31",
        "value": 999.0,  # duplicate — should be discarded
        "unit": "USD",
      },
      {
        "element_id": "us-gaap:Assets",
        "element_name": "Assets",
        "period_end": "2024-12-31",
        "value": 90.0,
        "unit": "USD",
      },
    ]
    with patch(PATCH_REPO, return_value=mock_repository):
      result, truncated = await query_fact_grid(
        MOCK_GRAPH_ID, elements=["us-gaap:Assets"]
      )
    assert len(result) == 2
    # Sorted DESC by period_end
    assert result[0]["period_end"] == "2025-12-31"
    assert result[0]["value"] == 100.0
    assert result[1]["period_end"] == "2024-12-31"
    assert truncated is False

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_limit_truncates_after_sort(self, mock_repository):
    """Truncation keeps the most recent periods, not an arbitrary slice."""
    mock_repository.execute_query.return_value = [
      {
        "element_id": "us-gaap:Assets",
        "element_name": "Assets",
        "period_end": end,
        "value": 1.0,
        "unit": "USD",
        "entity_ticker": "NVDA",
      }
      for end in ["2023-12-31", "2025-12-31", "2024-12-31"]
    ]
    with patch(PATCH_REPO, return_value=mock_repository):
      result, truncated = await query_fact_grid(
        MOCK_GRAPH_ID, elements=["us-gaap:Assets"], limit=2
      )

    assert truncated is True
    assert [r["period_end"] for r in result] == ["2025-12-31", "2024-12-31"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_limit_not_applied_in_cypher(self, mock_repository):
    """ORDER BY + LIMIT forces a full materialize-then-sort in LadybugDB
    (25s timeout over 269k facts), so the limit is a Python-side bound."""
    with patch(PATCH_REPO, return_value=mock_repository):
      await query_fact_grid(MOCK_GRAPH_ID, elements=["us-gaap:Assets"], limit=10)
    query = mock_repository.execute_query.call_args[0][0]
    assert "LIMIT" not in query.upper()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_projects_the_full_period_signature(self, mock_repository):
    """The dedup key needs both ends of the period, so the query must
    project them. Without period_start the caller cannot distinguish a
    quarterly fact from the YTD fact sharing its end_date — and neither
    can the dedup."""
    with patch(PATCH_REPO, return_value=mock_repository):
      await query_fact_grid(MOCK_GRAPH_ID, elements=["us-gaap:Assets"])
    query = mock_repository.execute_query.call_args[0][0]
    assert "p.start_date as period_start" in query
    assert "p.end_date as period_end" in query
    assert "p.duration_type as duration_type" in query


class TestDeduplicateFactRows:
  @pytest.mark.unit
  def test_keeps_first_per_key(self):
    rows = [
      {"element_id": "us-gaap:Assets", "period_end": "2025-12-31", "value": 100},
      {"element_id": "us-gaap:Assets", "period_end": "2025-12-31", "value": 999},
      {"element_id": "us-gaap:Revenue", "period_end": "2025-12-31", "value": 50},
    ]
    deduped = _deduplicate_fact_rows(rows)
    assert len(deduped) == 2

  @pytest.mark.unit
  def test_entity_is_part_of_the_key(self):
    """Same (qname, period) but different tickers should NOT dedup."""
    rows = [
      {
        "element_id": "us-gaap:Assets",
        "period_end": "2025-12-31",
        "entity_ticker": "NVDA",
        "value": 100,
      },
      {
        "element_id": "us-gaap:Assets",
        "period_end": "2025-12-31",
        "entity_ticker": "AAPL",
        "value": 200,
      },
    ]
    deduped = _deduplicate_fact_rows(rows)
    assert len(deduped) == 2

  @pytest.mark.unit
  def test_overlapping_durations_sharing_an_end_date_are_not_collapsed(self):
    """A 10-Q reports the same element for both the quarterly and the
    year-to-date window ending on the same day. Keying on period_end alone
    dropped one of them and returned the other with no way to tell which —
    reproduced live on SEC: NVDA us-gaap:Revenues for 2024-10-27 returned
    the nine-month 91,166,000,000 while the quarterly 35,082,000,000 was
    silently discarded.
    """
    rows = [
      {
        "element_id": "us-gaap:Revenues",
        "period_start": "2024-01-29",
        "period_end": "2024-10-27",
        "duration_type": "nine_months",
        "entity_ticker": "NVDA",
        "value": 91_166_000_000,
      },
      {
        "element_id": "us-gaap:Revenues",
        "period_start": "2024-07-29",
        "period_end": "2024-10-27",
        "duration_type": "quarterly",
        "entity_ticker": "NVDA",
        "value": 35_082_000_000,
      },
    ]
    deduped = _deduplicate_fact_rows(rows)
    assert len(deduped) == 2
    assert {r["duration_type"] for r in deduped} == {"nine_months", "quarterly"}

  @pytest.mark.unit
  def test_true_duplicates_still_collapse(self):
    """Identical period signature — the dedup this function exists for."""
    rows = [
      {
        "element_id": "us-gaap:Revenues",
        "period_start": "2024-07-29",
        "period_end": "2024-10-27",
        "duration_type": "quarterly",
        "entity_ticker": "NVDA",
        "value": 35_082_000_000,
      },
      {
        "element_id": "us-gaap:Revenues",
        "period_start": "2024-07-29",
        "period_end": "2024-10-27",
        "duration_type": "quarterly",
        "entity_ticker": "NVDA",
        "value": 35_082_000_000,
      },
    ]
    assert len(_deduplicate_fact_rows(rows)) == 1

  @pytest.mark.unit
  def test_instants_without_a_start_date_still_collapse(self):
    """Instant facts carry no start_date; the missing key element must not
    turn two reads of the same instant into two rows."""
    rows = [
      {
        "element_id": "us-gaap:Assets",
        "period_end": "2024-10-27",
        "entity_ticker": "NVDA",
        "value": 1,
      },
      {
        "element_id": "us-gaap:Assets",
        "period_end": "2024-10-27",
        "entity_ticker": "NVDA",
        "value": 1,
      },
    ]
    assert len(_deduplicate_fact_rows(rows)) == 1

  @pytest.mark.unit
  def test_sorted_desc_by_period_end(self):
    rows = [
      {"element_id": "a", "period_end": "2024-12-31", "value": 1},
      {"element_id": "b", "period_end": "2025-06-30", "value": 2},
      {"element_id": "c", "period_end": "2023-12-31", "value": 3},
    ]
    deduped = _deduplicate_fact_rows(rows)
    assert [r["period_end"] for r in deduped] == [
      "2025-06-30",
      "2024-12-31",
      "2023-12-31",
    ]
