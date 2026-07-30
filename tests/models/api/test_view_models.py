"""Unit tests for view API models (view_config)."""

import pytest
from pydantic import ValidationError

from robosystems.models.api.views.view_config import (
  DEFAULT_FACT_LIMIT,
  MAX_FACT_LIMIT,
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

  def test_dimension_axis_rejected(self):
    """The fact query filters on `has_dimensions = false`, so a dimension
    axis could only ever be a silent no-op."""
    with pytest.raises(ValidationError):
      ViewAxisConfig(type="dimension")

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

  def test_presentation_fields_are_not_accepted(self):
    """Ordering and labelling belong to whatever renders the facts; the
    request model carries scoping only."""
    model = ViewAxisConfig(
      type="period",
      member_labels={"2024-12-31": "Current Year"},
      element_order=["us-gaap:Assets"],
    )
    assert not hasattr(model, "member_labels")
    assert not hasattr(model, "element_order")


@pytest.mark.unit
class TestViewConfig:
  def test_defaults(self):
    model = ViewConfig()
    assert model.rows == []
    assert model.columns == []

  def test_custom_config(self):
    model = ViewConfig(
      rows=[ViewAxisConfig(type="element")],
      columns=[ViewAxisConfig(type="period")],
    )
    assert len(model.rows) == 1
    assert len(model.columns) == 1

  def test_pivot_knobs_are_not_accepted(self):
    """No server-side pivot means no aggregation function to configure."""
    model = ViewConfig(values="numeric_value", aggregation_function="average")
    assert not hasattr(model, "aggregation_function")
    assert not hasattr(model, "fill_value")


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

  def test_limit_defaults(self):
    model = CreateViewRequest(elements=["us-gaap:Assets"], period_type="instant")
    assert model.limit == DEFAULT_FACT_LIMIT

  def test_limit_above_max_rejected(self):
    with pytest.raises(ValidationError):
      CreateViewRequest(
        elements=["us-gaap:Assets"],
        period_type="instant",
        limit=MAX_FACT_LIMIT + 1,
      )

  def test_limit_below_one_rejected(self):
    with pytest.raises(ValidationError):
      CreateViewRequest(
        elements=["us-gaap:Assets"],
        period_type="instant",
        limit=0,
      )

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
