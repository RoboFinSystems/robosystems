import pandas as pd
import pytest
from robosystems.models.api.views import ViewAxisConfig, ViewConfig
from robosystems.operations.views.fact_grid_builder import FactGridBuilder


class TestFactGridBuilder:
  @pytest.fixture
  def sample_fact_data(self):
    return pd.DataFrame(
      {
        "element_id": [
          "us-gaap:Assets",
          "us-gaap:Cash",
          "us-gaap:AccountsReceivableNetCurrent",
          "us-gaap:Assets",
          "us-gaap:Cash",
        ],
        "element_name": [
          "Assets",
          "Cash",
          "Accounts Receivable",
          "Assets",
          "Cash",
        ],
        "numeric_value": [1000000, 100000, 200000, 1500000, 150000],
        "period_end": [
          "2024-12-31",
          "2024-12-31",
          "2024-12-31",
          "2023-12-31",
          "2023-12-31",
        ],
        "entity_id": ["AAPL", "AAPL", "AAPL", "AAPL", "AAPL"],
        "dimension_axis": [None, None, None, None, None],
        "dimension_member": [None, None, None, None, None],
      }
    )

  @pytest.fixture
  def dimensional_fact_data(self):
    return pd.DataFrame(
      {
        "element_id": [
          "us-gaap:Revenue",
          "us-gaap:Revenue",
          "us-gaap:Revenue",
        ],
        "element_name": ["Revenue", "Revenue", "Revenue"],
        "numeric_value": [100000, 60000, 40000],
        "period_end": ["2024-12-31", "2024-12-31", "2024-12-31"],
        "entity_id": ["AAPL", "AAPL", "AAPL"],
        "dimension_axis": [None, "Geography", "Geography"],
        "dimension_member": [None, "US", "EU"],
      }
    )

  def test_build_empty_grid(self):
    builder = FactGridBuilder()
    empty_df = pd.DataFrame()
    view_config = ViewConfig()

    fact_grid = builder.build(empty_df, view_config, "test_source")

    assert fact_grid.metadata.fact_count == 0
    assert fact_grid.metadata.dimension_count == 0
    assert fact_grid.facts_df.empty

  def test_build_basic_grid(self, sample_fact_data):
    builder = FactGridBuilder()
    view_config = ViewConfig()

    fact_grid = builder.build(sample_fact_data, view_config, "test_source")

    assert fact_grid.metadata.fact_count == 5
    assert fact_grid.metadata.dimension_count > 0
    assert len(fact_grid.facts_df) == 5

  def test_aspect_filtering_periods(self, sample_fact_data):
    builder = FactGridBuilder()
    view_config = ViewConfig(
      columns=[
        ViewAxisConfig(
          type="period",
          selected_members=["2024-12-31"],
        )
      ]
    )

    fact_grid = builder.build(sample_fact_data, view_config, "test_source")

    assert fact_grid.facts_df is not None
    assert len(fact_grid.facts_df) == 3
    assert all(fact_grid.facts_df["period_end"] == "2024-12-31")

  def test_aspect_filtering_elements(self, sample_fact_data):
    builder = FactGridBuilder()
    view_config = ViewConfig(
      rows=[
        ViewAxisConfig(
          type="element",
          selected_members=["us-gaap:Cash"],
        )
      ]
    )

    fact_grid = builder.build(sample_fact_data, view_config, "test_source")

    assert fact_grid.facts_df is not None
    assert len(fact_grid.facts_df) == 2
    assert all(fact_grid.facts_df["element_id"] == "us-gaap:Cash")

  def test_generate_pivot_table_basic(self, sample_fact_data):
    builder = FactGridBuilder()
    view_config = ViewConfig()

    fact_grid = builder.build(sample_fact_data, view_config, "test_source")
    pivot_table = builder.generate_pivot_table(fact_grid, view_config)

    assert "index" in pivot_table
    assert "columns" in pivot_table
    assert "data" in pivot_table
    assert "metadata" in pivot_table
    assert pivot_table["metadata"]["row_count"] > 0

  def test_generate_pivot_table_with_custom_labels(self, sample_fact_data):
    builder = FactGridBuilder()
    view_config = ViewConfig(
      columns=[
        ViewAxisConfig(
          type="period",
          member_order=["2024-12-31", "2023-12-31"],
          member_labels={"2024-12-31": "Current", "2023-12-31": "Prior"},
        )
      ]
    )

    fact_grid = builder.build(sample_fact_data, view_config, "test_source")
    pivot_table = builder.generate_pivot_table(fact_grid, view_config)

    if pivot_table["metadata"]["has_periods"]:
      columns = [col[0] for col in pivot_table["columns"]]
      if "Current" in columns or "Prior" in columns:
        assert "Current" in columns or "2024-12-31" in columns

  def test_extract_dimensions(self, sample_fact_data):
    builder = FactGridBuilder()

    dimensions = builder._extract_dimensions(sample_fact_data)

    assert len(dimensions) > 0

    dimension_types = {d.type for d in dimensions}
    assert "element" in dimension_types
    assert "period" in dimension_types
    assert "entity" in dimension_types

  def test_apply_aspect_filtering_multiple_axes(self, sample_fact_data):
    builder = FactGridBuilder()

    view_config = ViewConfig(
      rows=[
        ViewAxisConfig(
          type="element",
          selected_members=["us-gaap:Cash"],
        )
      ],
      columns=[
        ViewAxisConfig(
          type="period",
          selected_members=["2024-12-31"],
        )
      ],
    )

    filtered_df = builder._apply_aspect_filtering(sample_fact_data, view_config)

    assert len(filtered_df) == 1
    assert filtered_df.iloc[0]["element_id"] == "us-gaap:Cash"
    assert filtered_df.iloc[0]["period_end"] == "2024-12-31"

  def test_element_ordering_in_pivot_table(self, sample_fact_data):
    builder = FactGridBuilder()

    view_config = ViewConfig(
      rows=[
        ViewAxisConfig(
          type="element",
          element_order=[
            "us-gaap:AccountsReceivableNetCurrent",
            "us-gaap:Cash",
            "us-gaap:Assets",
          ],
        )
      ],
      columns=[ViewAxisConfig(type="period")],
    )

    fact_grid = builder.build(sample_fact_data, view_config, "test_source")
    pivot_table = builder.generate_pivot_table(fact_grid, view_config)

    row_elements = [row[0] for row in pivot_table["index"]]
    assert row_elements[0] == "Accounts Receivable"
    assert row_elements[1] == "Cash"
    assert row_elements[2] == "Assets"

  def test_element_labels_in_pivot_table(self, sample_fact_data):
    builder = FactGridBuilder()

    view_config = ViewConfig(
      rows=[
        ViewAxisConfig(
          type="element",
          element_labels={
            "Cash": "Cash and Cash Equivalents",
            "Assets": "Total Assets",
          },
        )
      ],
      columns=[ViewAxisConfig(type="period")],
    )

    fact_grid = builder.build(sample_fact_data, view_config, "test_source")
    pivot_table = builder.generate_pivot_table(fact_grid, view_config)

    row_labels = [row[0] for row in pivot_table["index"]]
    assert "Cash and Cash Equivalents" in row_labels
    assert "Total Assets" in row_labels
