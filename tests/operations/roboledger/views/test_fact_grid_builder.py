import pytest

from robosystems.models.api.views import ViewAxisConfig, ViewConfig
from robosystems.operations.roboledger.views.fact_grid_builder import (
  FactGridBuilder,
  summarize_by_element,
)


def _fact(
  element_id,
  element_name,
  value,
  period_end,
  ticker="AAPL",
  unit="USD",
  period_start=None,
  duration_type=None,
):
  """A fact record shaped exactly as `query_fact_grid` returns one."""
  return {
    "element_id": element_id,
    "element_name": element_name,
    "period_start": period_start,
    "period_end": period_end,
    "duration_type": duration_type,
    "value": value,
    "unit": unit,
    "entity_ticker": ticker,
    "entity_name": "Apple Inc.",
  }


class TestFactGridBuilder:
  @pytest.fixture
  def sample_fact_data(self):
    return [
      _fact("us-gaap:Assets", "Assets", 1000000, "2024-12-31"),
      _fact("us-gaap:Cash", "Cash", 100000, "2024-12-31"),
      _fact(
        "us-gaap:AccountsReceivableNetCurrent",
        "AccountsReceivableNetCurrent",
        200000,
        "2024-12-31",
      ),
      _fact("us-gaap:Assets", "Assets", 1500000, "2023-12-31"),
      _fact("us-gaap:Cash", "Cash", 150000, "2023-12-31"),
    ]

  @pytest.fixture
  def multi_entity_fact_data(self):
    return [
      _fact("us-gaap:Assets", "Assets", 1000000, "2024-12-31", ticker="AAPL"),
      _fact("us-gaap:Assets", "Assets", 2000000, "2024-12-31", ticker="MSFT"),
    ]

  def test_build_empty_grid(self):
    builder = FactGridBuilder()

    fact_grid = builder.build([], ViewConfig(), "test_source")

    assert fact_grid.metadata.fact_count == 0
    assert fact_grid.metadata.dimension_count == 0
    assert fact_grid.facts == []

  def test_build_basic_grid(self, sample_fact_data):
    builder = FactGridBuilder()

    fact_grid = builder.build(sample_fact_data, ViewConfig(), "test_source")

    assert fact_grid.metadata.fact_count == 5
    assert fact_grid.metadata.dimension_count > 0
    assert len(fact_grid.facts) == 5

  def test_facts_are_not_collapsed_across_entities(self, multi_entity_fact_data):
    """Same element, same period, two filers stay two facts.

    The whole reason there is no server-side pivot: collapsing this pair
    would silently report AAPL + MSFT assets as one number.
    """
    builder = FactGridBuilder()

    fact_grid = builder.build(multi_entity_fact_data, ViewConfig(), "test_source")

    assert len(fact_grid.facts) == 2
    assert {f["value"] for f in fact_grid.facts} == {1000000, 2000000}

  def test_aspect_filtering_periods(self, sample_fact_data):
    builder = FactGridBuilder()
    view_config = ViewConfig(
      columns=[ViewAxisConfig(type="period", selected_members=["2024-12-31"])]
    )

    fact_grid = builder.build(sample_fact_data, view_config, "test_source")

    assert len(fact_grid.facts) == 3
    assert all(f["period_end"] == "2024-12-31" for f in fact_grid.facts)

  def test_aspect_filtering_elements(self, sample_fact_data):
    builder = FactGridBuilder()
    view_config = ViewConfig(
      rows=[ViewAxisConfig(type="element", selected_members=["us-gaap:Cash"])]
    )

    fact_grid = builder.build(sample_fact_data, view_config, "test_source")

    assert len(fact_grid.facts) == 2
    assert all(f["element_id"] == "us-gaap:Cash" for f in fact_grid.facts)

  def test_aspect_filtering_entities(self, multi_entity_fact_data):
    builder = FactGridBuilder()
    view_config = ViewConfig(
      rows=[ViewAxisConfig(type="entity", selected_members=["MSFT"])]
    )

    fact_grid = builder.build(multi_entity_fact_data, view_config, "test_source")

    assert len(fact_grid.facts) == 1
    assert fact_grid.facts[0]["entity_ticker"] == "MSFT"

  def test_apply_aspect_filtering_multiple_axes(self, sample_fact_data):
    builder = FactGridBuilder()
    view_config = ViewConfig(
      rows=[ViewAxisConfig(type="element", selected_members=["us-gaap:Cash"])],
      columns=[ViewAxisConfig(type="period", selected_members=["2024-12-31"])],
    )

    filtered = builder._apply_aspect_filtering(sample_fact_data, view_config)

    assert len(filtered) == 1
    assert filtered[0]["element_id"] == "us-gaap:Cash"
    assert filtered[0]["period_end"] == "2024-12-31"

  def test_null_aspect_excluded_by_default(self):
    """A fact missing the filtered aspect drops unless explicitly included."""
    builder = FactGridBuilder()
    facts = [
      _fact("us-gaap:Assets", "Assets", 1000000, "2024-12-31"),
      {**_fact("us-gaap:Assets", "Assets", 5, "2024-12-31"), "entity_ticker": None},
    ]
    view_config = ViewConfig(
      rows=[ViewAxisConfig(type="entity", selected_members=["AAPL"])]
    )

    filtered = builder._apply_aspect_filtering(facts, view_config)

    assert len(filtered) == 1
    assert filtered[0]["entity_ticker"] == "AAPL"

  def test_null_aspect_included_when_requested(self):
    builder = FactGridBuilder()
    facts = [
      _fact("us-gaap:Assets", "Assets", 1000000, "2024-12-31"),
      {**_fact("us-gaap:Assets", "Assets", 5, "2024-12-31"), "entity_ticker": None},
    ]
    view_config = ViewConfig(
      rows=[
        ViewAxisConfig(
          type="entity",
          selected_members=["AAPL"],
          include_null_dimension=True,
        )
      ]
    )

    filtered = builder._apply_aspect_filtering(facts, view_config)

    assert len(filtered) == 2

  def test_extract_dimensions(self, sample_fact_data):
    builder = FactGridBuilder()

    dimensions = builder._extract_dimensions(sample_fact_data)

    dimension_types = {d.type for d in dimensions}
    assert "element" in dimension_types
    assert "period" in dimension_types
    assert "entity" in dimension_types

    element_dim = next(d for d in dimensions if d.type == "element")
    assert "us-gaap:Assets" in element_dim.members
    assert len(element_dim.members) == 3

    period_dim = next(d for d in dimensions if d.type == "period")
    assert period_dim.members == ["2023-12-31", "2024-12-31"]

  def test_entity_dimension_absent_without_entity_filter(self):
    """No entity filter means the query returns no entity keys at all."""
    builder = FactGridBuilder()
    facts = [
      {
        "element_id": "us-gaap:Assets",
        "element_name": "Assets",
        "period_end": "2024-12-31",
        "value": 1000000,
        "unit": "USD",
      }
    ]

    dimensions = builder._extract_dimensions(facts)

    assert "entity" not in {d.type for d in dimensions}

  def test_dimension_axis_type_rejected(self):
    """Dimensional facts are filtered out by the query, so the axis can't work."""
    with pytest.raises(ValueError, match="Axis type must be one of"):
      ViewAxisConfig(type="dimension", selected_members=["Geography"])


class TestSummarizeByElement:
  def test_aggregates_per_element(self):
    facts = [
      _fact("us-gaap:Assets", "Assets", 1000000, "2024-12-31"),
      _fact("us-gaap:Assets", "Assets", 1500000, "2023-12-31"),
      _fact("us-gaap:Cash", "Cash", 100000, "2024-12-31"),
    ]

    summary = summarize_by_element(facts)

    assert summary["us-gaap:Assets"]["count"] == 2
    assert summary["us-gaap:Assets"]["total"] == 2500000
    assert summary["us-gaap:Assets"]["average"] == 1250000
    assert summary["us-gaap:Assets"]["min"] == 1000000
    assert summary["us-gaap:Assets"]["max"] == 1500000
    assert summary["us-gaap:Cash"]["count"] == 1

  def test_keyed_on_qname_not_local_name(self):
    """Two taxonomies can share a local name; qnames keep them apart."""
    facts = [
      _fact("us-gaap:Assets", "Assets", 100, "2024-12-31"),
      _fact("ifrs-full:Assets", "Assets", 200, "2024-12-31"),
    ]

    summary = summarize_by_element(facts)

    assert set(summary) == {"us-gaap:Assets", "ifrs-full:Assets"}

  def test_skips_null_values(self):
    facts = [
      _fact("us-gaap:Assets", "Assets", 100, "2024-12-31"),
      {**_fact("us-gaap:Assets", "Assets", 0, "2023-12-31"), "value": None},
    ]

    summary = summarize_by_element(facts)

    assert summary["us-gaap:Assets"]["count"] == 1

  def test_overlapping_windows_contribute_only_the_narrowest(self):
    """A 10-Q reports both the quarter and the YTD window ending the same
    day. Since the dedup fix both survive into `facts` — summing them would
    double-count the quarter inside its own YTD figure. NVDA's real numbers:
    Q3 FY2025 revenue 35,082M inside the 91,166M nine-month window."""
    facts = [
      _fact(
        "us-gaap:Revenues",
        "Revenues",
        91_166_000_000,
        "2024-10-27",
        ticker="NVDA",
        period_start="2024-01-29",
      ),
      _fact(
        "us-gaap:Revenues",
        "Revenues",
        35_082_000_000,
        "2024-10-27",
        ticker="NVDA",
        period_start="2024-07-29",
      ),
      _fact(
        "us-gaap:Revenues",
        "Revenues",
        30_040_000_000,
        "2024-07-28",
        ticker="NVDA",
        period_start="2024-04-29",
      ),
    ]

    summary = summarize_by_element(facts)

    # The quarterly windows sum; the nine-month container does not join them.
    assert summary["us-gaap:Revenues"]["count"] == 2
    assert summary["us-gaap:Revenues"]["total"] == 65_122_000_000
    assert summary["us-gaap:Revenues"]["max"] == 35_082_000_000

  def test_same_period_end_across_entities_is_not_collapsed(self):
    """The narrowest-window rule is per entity — two filers reporting the
    same window are both real observations."""
    facts = [
      _fact(
        "us-gaap:Revenues",
        "Revenues",
        100,
        "2024-12-31",
        ticker="AAPL",
        period_start="2024-10-01",
      ),
      _fact(
        "us-gaap:Revenues",
        "Revenues",
        200,
        "2024-12-31",
        ticker="MSFT",
        period_start="2024-10-01",
      ),
    ]

    summary = summarize_by_element(facts)

    assert summary["us-gaap:Revenues"]["count"] == 2
    assert summary["us-gaap:Revenues"]["total"] == 300

  def test_instants_without_a_start_are_unaffected(self):
    """Instants carry period_start=None and arrive one-per-end after dedup;
    the narrowest-window rule must pass them through unchanged."""
    facts = [
      _fact("us-gaap:Assets", "Assets", 1000, "2024-12-31"),
      _fact("us-gaap:Assets", "Assets", 900, "2023-12-31"),
    ]

    summary = summarize_by_element(facts)

    assert summary["us-gaap:Assets"]["count"] == 2
    assert summary["us-gaap:Assets"]["total"] == 1900

  def test_empty_input(self):
    assert summarize_by_element([]) == {}
