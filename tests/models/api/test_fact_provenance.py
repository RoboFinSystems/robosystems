"""Unit tests for the FactProvenance discriminated union."""

import pytest
from pydantic import TypeAdapter, ValidationError

from robosystems.models.api.fact_provenance import (
  AssertedProvenance,
  DerivedProvenance,
  FactProvenance,
  PivotProvenance,
  ScheduleProvenance,
)

_adapter = TypeAdapter(FactProvenance)


class TestArmValidation:
  def test_pivot_requires_mapping_and_period(self) -> None:
    p = PivotProvenance(mapping_id="map_1", period="2026-01-01/2026-01-31")
    assert p.origin == "pivot"
    with pytest.raises(ValidationError):
      PivotProvenance(mapping_id="map_1")  # type: ignore[call-arg]

  def test_schedule_requires_structure_and_method(self) -> None:
    s = ScheduleProvenance(structure_id="st_1", method="straight_line")
    assert s.origin == "schedule"
    with pytest.raises(ValidationError):
      ScheduleProvenance(structure_id="st_1")  # type: ignore[call-arg]

  def test_asserted_requires_source_system(self) -> None:
    a = AssertedProvenance(source_system="cross_graph_share")
    assert a.origin == "asserted"
    with pytest.raises(ValidationError):
      AssertedProvenance()  # type: ignore[call-arg]


class TestDerivedValidator:
  def test_derived_accepts_formula(self) -> None:
    assert DerivedProvenance(formula="a + b").origin == "derived"

  def test_derived_accepts_computation_only(self) -> None:
    # auto-derived facts with no single source row (retained earnings, subtotals)
    assert DerivedProvenance(computation="retained_earnings_close").computation

  def test_derived_accepts_source_fact_ids(self) -> None:
    assert DerivedProvenance(source_fact_ids=["fact_1"]).source_fact_ids == ["fact_1"]

  def test_derived_rejects_empty(self) -> None:
    with pytest.raises(ValidationError):
      DerivedProvenance()


class TestDiscriminatorRouting:
  def test_routes_each_origin(self) -> None:
    assert isinstance(
      _adapter.validate_python({"origin": "pivot", "mapping_id": "m", "period": "p"}),
      PivotProvenance,
    )
    assert isinstance(
      _adapter.validate_python(
        {"origin": "schedule", "structure_id": "s", "method": "straight_line"}
      ),
      ScheduleProvenance,
    )
    assert isinstance(
      _adapter.validate_python({"origin": "derived", "computation": "c"}),
      DerivedProvenance,
    )
    assert isinstance(
      _adapter.validate_python({"origin": "asserted", "source_system": "x"}),
      AssertedProvenance,
    )

  def test_rejects_unknown_origin(self) -> None:
    with pytest.raises(ValidationError):
      _adapter.validate_python({"origin": "fabricated", "mapping_id": "m"})

  def test_round_trips_through_json(self) -> None:
    p = PivotProvenance(mapping_id="map_1", period="2026-01-01/2026-01-31")
    dumped = p.model_dump(mode="json")
    assert dumped["origin"] == "pivot"
    assert isinstance(_adapter.validate_python(dumped), PivotProvenance)
