"""Unit tests for view API models (view_config)."""

import pytest
from pydantic import ValidationError

from robosystems.models.api.views.view_config import (
  CreateViewRequest,
  ViewAxisConfig,
  ViewConfig,
)


@pytest.mark.unit
class TestViewAxisConfig:
  def test_valid_element_axis(self):
    model = ViewAxisConfig(type="element")
    assert model.type == "element"

  def test_valid_period_axis(self):
    model = ViewAxisConfig(type="period")
    assert model.type == "period"

  def test_valid_dimension_axis(self):
    model = ViewAxisConfig(type="dimension", dimension_axis="Segment")
    assert model.dimension_axis == "Segment"

  def test_valid_entity_axis(self):
    model = ViewAxisConfig(type="entity")
    assert model.type == "entity"

  def test_invalid_axis_type(self):
    with pytest.raises(ValidationError) as exc_info:
      ViewAxisConfig(type="invalid")
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("type",) for e in errors)

  def test_include_null_dimension_default(self):
    model = ViewAxisConfig(type="element")
    assert model.include_null_dimension is False

  def test_selected_members(self):
    model = ViewAxisConfig(
      type="period",
      selected_members=["2024-12-31", "2023-12-31"],
    )
    assert len(model.selected_members) == 2

  def test_member_labels(self):
    model = ViewAxisConfig(
      type="period",
      member_labels={"2024-12-31": "Current Year"},
    )
    assert model.member_labels["2024-12-31"] == "Current Year"

  def test_element_order(self):
    model = ViewAxisConfig(
      type="element",
      element_order=["us-gaap:Assets", "us-gaap:Cash"],
    )
    assert len(model.element_order) == 2


@pytest.mark.unit
class TestViewConfig:
  def test_defaults(self):
    model = ViewConfig()
    assert model.rows == []
    assert model.columns == []
    assert model.values == "numeric_value"
    assert model.aggregation_function == "sum"
    assert model.fill_value == 0.0

  def test_custom_config(self):
    model = ViewConfig(
      rows=[ViewAxisConfig(type="element")],
      columns=[ViewAxisConfig(type="period")],
      values="numeric_value",
      aggregation_function="average",
      fill_value=-1.0,
    )
    assert len(model.rows) == 1
    assert model.aggregation_function == "average"


@pytest.mark.unit
class TestCreateViewRequest:
  def test_minimal_request(self):
    model = CreateViewRequest(
      elements=["us-gaap:Assets"],
      period_type="instant",
    )
    assert model.elements == ["us-gaap:Assets"]
    assert model.canonical_concepts == []
    assert model.include_summary is False

  def test_with_canonical_concepts(self):
    model = CreateViewRequest(
      canonical_concepts=["revenue", "net_income"],
      period_type="annual",
    )
    assert model.canonical_concepts == ["revenue", "net_income"]
    assert model.elements == []

  def test_with_entity_filters(self):
    model = CreateViewRequest(
      elements=["us-gaap:Assets"],
      period_type="instant",
      entity="NVDA",
    )
    assert model.entity == "NVDA"

  def test_with_multi_entity(self):
    model = CreateViewRequest(
      elements=["us-gaap:Assets"],
      period_type="instant",
      entities=["NVDA", "AAPL"],
    )
    assert model.entities == ["NVDA", "AAPL"]

  def test_with_report_filters(self):
    model = CreateViewRequest(
      elements=["us-gaap:NetIncomeLoss"],
      form="10-K",
      fiscal_year=2024,
      fiscal_period="FY",
      period_type="annual",
    )
    assert model.form == "10-K"
    assert model.fiscal_year == 2024
    assert model.fiscal_period == "FY"

  def test_invalid_period_type(self):
    with pytest.raises(ValidationError):
      CreateViewRequest(
        elements=["us-gaap:Assets"],
        period_type="biannual",
      )

  def test_defaults(self):
    model = CreateViewRequest()
    assert model.elements == []
    assert model.canonical_concepts == []
    assert model.periods == []
    assert model.entities == []
    assert model.entity is None
    assert model.form is None
    assert model.fiscal_year is None
    assert model.period_type is None
    assert model.include_summary is False
