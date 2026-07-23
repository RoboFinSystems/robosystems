"""Forecast handler tests — the authored scenario container.

Exercises create / update / delete / build_envelope plus the base-period
resolution and lever-expansion helpers for the ``forecast`` block type.
The derivation walk is tested separately in ``test_forecast_compute.py``
— this file only covers authoring + envelope shape.
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.extensions.forecasts import (
  CreateForecastRequest,
  DeleteForecastRequest,
  LeverAssertionRequest,
  UpdateForecastRequest,
)
from robosystems.models.api.information_block import (
  ForecastMechanics,
  LeverAssertionLite,
)
from robosystems.operations.information_block import forecast as forecast_handlers


def _element(
  element_id: str,
  qname: str,
  *,
  source: str = "rs-driver",
  item_type: str | None = "percent",
  taxonomy_id: str = "tax_rs_driver",
) -> MagicMock:
  el = MagicMock()
  el.id = element_id
  el.qname = qname
  el.source = source
  el.item_type = item_type
  el.taxonomy_id = taxonomy_id
  el.is_monetary = False
  el.name = qname.split(":")[-1]
  el.code = None
  el.element_type = "concept"
  el.is_abstract = False
  el.balance_type = "debit"
  el.period_type = "duration"
  return el


GROWTH = _element("el_growth", "rs-driver:RevenueGrowthRate")
DSO = _element("el_dso", "rs-driver:DaysSalesOutstanding", item_type="days")


def _body(**overrides: Any) -> CreateForecastRequest:
  payload: dict[str, Any] = {
    "name": "FY26 Operating Budget",
    "scenario_kind": "budget",
    "horizon_months": 3,
    "levers": [
      LeverAssertionRequest(qname="rs-driver:RevenueGrowthRate", value=0.03),
    ],
  }
  payload.update(overrides)
  return CreateForecastRequest(**payload)


class TestExpandLevers:
  def _session(self, *elements: MagicMock) -> MagicMock:
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.side_effect = list(elements)
    return session

  def test_uniform_fill_covers_every_horizon_month(self) -> None:
    session = self._session(GROWTH)
    expanded = forecast_handlers._expand_levers(
      session,
      [LeverAssertionRequest(qname="rs-driver:RevenueGrowthRate", value=0.03)],
      "2026-06",
      3,
    )
    assert len(expanded) == 1
    lever = expanded[0]
    assert lever.element_id == "el_growth"
    assert lever.item_type == "percent"
    assert lever.values_by_period == {
      "2026-07": 0.03,
      "2026-08": 0.03,
      "2026-09": 0.03,
    }

  def test_overrides_win_over_uniform_fill(self) -> None:
    session = self._session(GROWTH)
    expanded = forecast_handlers._expand_levers(
      session,
      [
        LeverAssertionRequest(
          qname="rs-driver:RevenueGrowthRate",
          value=0.03,
          values_by_period={"2026-08": 0.05},
        )
      ],
      "2026-06",
      3,
    )
    assert expanded[0].values_by_period["2026-08"] == 0.05
    assert expanded[0].values_by_period["2026-07"] == 0.03

  def test_override_only_leaves_uncovered_months_unasserted(self) -> None:
    session = self._session(DSO)
    expanded = forecast_handlers._expand_levers(
      session,
      [
        LeverAssertionRequest(
          qname="rs-driver:DaysSalesOutstanding",
          values_by_period={"2026-07": 45.0},
        )
      ],
      "2026-06",
      3,
    )
    assert expanded[0].values_by_period == {"2026-07": 45.0}

  def test_rejects_unresolved_qname(self) -> None:
    session = self._session(None)
    with pytest.raises(ValueError, match="did not resolve"):
      forecast_handlers._expand_levers(
        session,
        [LeverAssertionRequest(qname="rs-driver:Nope", value=1.0)],
        "2026-06",
        3,
      )

  def test_rejects_non_rs_driver_source(self) -> None:
    imposter = _element("el_rev", "rs-gaap:Revenues", source="rs-gaap")
    session = self._session(imposter)
    with pytest.raises(ValueError, match="rs-driver"):
      forecast_handlers._expand_levers(
        session,
        [LeverAssertionRequest(qname="rs-gaap:Revenues", value=1.0)],
        "2026-06",
        3,
      )

  def test_rejects_duplicate_lever(self) -> None:
    session = self._session(GROWTH, GROWTH)
    with pytest.raises(ValueError, match="Duplicate lever"):
      forecast_handlers._expand_levers(
        session,
        [
          LeverAssertionRequest(qname="rs-driver:RevenueGrowthRate", value=0.03),
          LeverAssertionRequest(qname="rs-driver:RevenueGrowthRate", value=0.04),
        ],
        "2026-06",
        3,
      )

  def test_rejects_override_outside_horizon(self) -> None:
    session = self._session(GROWTH)
    with pytest.raises(ValueError, match="outside the horizon"):
      forecast_handlers._expand_levers(
        session,
        [
          LeverAssertionRequest(
            qname="rs-driver:RevenueGrowthRate",
            values_by_period={"2027-01": 0.03},
          )
        ],
        "2026-06",
        3,
      )


class TestResolveBasePeriod:
  def test_request_value_wins(self) -> None:
    session = MagicMock()
    assert (
      forecast_handlers._resolve_base_period(session, "ent_1", "2026-03") == "2026-03"
    )
    session.query.assert_not_called()

  def test_fiscal_calendar_closed_through(self) -> None:
    session = MagicMock()
    calendar = SimpleNamespace(closed_through_period="2026-05")
    session.query.return_value.order_by.return_value.first.return_value = calendar
    assert forecast_handlers._resolve_base_period(session, "ent_1", None) == "2026-05"

  def test_data_driven_fallback_from_newest_report_month(self) -> None:
    session = MagicMock()
    session.query.return_value.order_by.return_value.first.return_value = None
    with patch.object(
      forecast_handlers,
      "_latest_report_period_end_before",
      return_value=date(2026, 4, 30),
    ):
      assert forecast_handlers._resolve_base_period(session, "ent_1", None) == "2026-04"

  def test_raises_when_nothing_to_project_from(self) -> None:
    session = MagicMock()
    session.query.return_value.order_by.return_value.first.return_value = None
    with (
      patch.object(
        forecast_handlers, "_latest_report_period_end_before", return_value=None
      ),
      pytest.raises(ValueError, match="Cannot resolve a base period"),
    ):
      forecast_handlers._resolve_base_period(session, "ent_1", None)


class TestCreate:
  def test_persists_structure_with_mechanics_and_lever_set(self) -> None:
    session = MagicMock()
    # _expand_levers element lookup, then session.get for the taxonomy owner.
    session.execute.return_value.scalar_one_or_none.side_effect = [GROWTH]
    session.get.return_value = GROWTH

    added: list[Any] = []
    session.add.side_effect = added.append

    def _flush() -> None:
      if added:
        added[0].id = "struct_budget_01"

    session.flush.side_effect = _flush

    with (
      patch.object(forecast_handlers, "_default_entity_id", return_value="ent_1"),
      patch.object(forecast_handlers, "_resolve_base_period", return_value="2026-06"),
      patch.object(forecast_handlers, "_write_lever_fact_set") as write_levers,
    ):
      structure_id = forecast_handlers.create(session, _body(), "usr_test")

    assert structure_id == "struct_budget_01"
    persisted = added[0]
    assert persisted.block_type == "forecast"
    assert persisted.taxonomy_id == "tax_rs_driver"
    assert persisted.concept_arrangement == "set"

    mech = ForecastMechanics.model_validate(persisted.artifact_mechanics)
    assert mech.scenario_kind == "budget"
    assert mech.horizon_months == 3
    assert mech.base_period == "2026-06"
    assert mech.levers[0].qname == "rs-driver:RevenueGrowthRate"
    assert mech.computed_months == 0

    write_levers.assert_called_once()
    session.commit.assert_called_once()

  def test_rejects_wrong_source_before_persisting(self) -> None:
    imposter = _element("el_rev", "rs-gaap:Revenues", source="rs-gaap")
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.side_effect = [imposter]
    with (
      patch.object(forecast_handlers, "_default_entity_id", return_value="ent_1"),
      patch.object(forecast_handlers, "_resolve_base_period", return_value="2026-06"),
      pytest.raises(ValueError, match="rs-driver"),
    ):
      forecast_handlers.create(
        session,
        _body(levers=[LeverAssertionRequest(qname="rs-gaap:Revenues", value=1.0)]),
        "usr_test",
      )
    session.commit.assert_not_called()


class TestWriteLeverFactSet:
  def test_self_referential_scenario_set_with_asserted_provenance(self) -> None:
    structure = SimpleNamespace(id="struct_budget_01")
    mechanics = ForecastMechanics(
      scenario_kind="budget",
      horizon_months=2,
      base_period="2026-06",
      levers=[
        LeverAssertionLite(
          qname="rs-driver:RevenueGrowthRate",
          element_id="el_growth",
          item_type="percent",
          values_by_period={"2026-07": 0.03, "2026-08": 0.03},
        )
      ],
    )
    session = MagicMock()
    session.execute.return_value.scalars.return_value.all.return_value = [GROWTH]
    added_facts: list[Any] = []
    session.add.side_effect = added_facts.append

    with patch.object(
      forecast_handlers,
      "create_fact_set",
      return_value=SimpleNamespace(id="fs_lever"),
    ) as create_fs:
      fact_set = forecast_handlers._write_lever_fact_set(
        session, structure, mechanics, "ent_1", "usr_test"
      )

    assert fact_set.id == "fs_lever"
    kwargs = create_fs.call_args.kwargs
    # The block IS the scenario — levers key into its own slice, so they
    # cascade-delete with it and never surface on actuals reads.
    assert kwargs["structure_id"] == "struct_budget_01"
    assert kwargs["scenario_id"] == "struct_budget_01"
    assert kwargs["factset_type"] == "custom"
    assert kwargs["period_start"] == date(2026, 7, 1)
    assert kwargs["period_end"] == date(2026, 8, 31)
    assert kwargs["provenance"].origin == "asserted"
    assert kwargs["provenance"].source_system == "forecast-levers"

    assert len(added_facts) == 2
    first = added_facts[0]
    assert first.element_id == "el_growth"
    assert first.value == 0.03
    assert first.period_start == date(2026, 7, 1)
    assert first.period_end == date(2026, 7, 31)
    assert first.period_type == "duration"
    assert first.unit == "pure"  # percent → 'pure'


class TestUpdate:
  def _structure(self) -> MagicMock:
    structure = MagicMock()
    structure.id = "struct_budget_01"
    structure.block_type = "forecast"
    structure.artifact_mechanics = ForecastMechanics(
      scenario_kind="budget",
      horizon_months=3,
      base_period="2026-06",
      levers=[
        LeverAssertionLite(
          qname="rs-driver:RevenueGrowthRate",
          element_id="el_growth",
          item_type="percent",
          values_by_period={"2026-07": 0.03, "2026-08": 0.03, "2026-09": 0.03},
        )
      ],
    ).model_dump(mode="json")
    return structure

  def test_name_only_update_preserves_mechanics_and_levers(self) -> None:
    structure = self._structure()
    session = MagicMock()
    session.get.return_value = structure

    forecast_handlers.update(
      session,
      UpdateForecastRequest(structure_id="struct_budget_01", name="Renamed"),
      "usr_test",
    )

    assert structure.name == "Renamed"
    mech = ForecastMechanics.model_validate(structure.artifact_mechanics)
    assert mech.horizon_months == 3
    assert mech.levers[0].values_by_period["2026-07"] == 0.03
    # No lever replacement → the lever FactSet is untouched.
    session.delete.assert_not_called()

  def test_window_change_requires_levers(self) -> None:
    structure = self._structure()
    session = MagicMock()
    session.get.return_value = structure
    with pytest.raises(ValueError, match="re-supplying `levers`"):
      forecast_handlers.update(
        session,
        UpdateForecastRequest(structure_id="struct_budget_01", horizon_months=6),
        "usr_test",
      )
    session.commit.assert_not_called()

  def test_missing_or_wrong_type_raises(self) -> None:
    session = MagicMock()
    session.get.return_value = None
    with pytest.raises(ValueError, match="not found"):
      forecast_handlers.update(
        session,
        UpdateForecastRequest(structure_id="struct_gone", name="x"),
        "usr_test",
      )


class TestDelete:
  def test_deletes_every_scenario_set_then_the_structure(self) -> None:
    structure = MagicMock()
    structure.id = "struct_budget_01"
    structure.block_type = "forecast"
    session = MagicMock()
    session.get.return_value = structure

    lever_set = SimpleNamespace(id="fs_lever")
    month_set = SimpleNamespace(id="fs_july_is")
    stale_verification = SimpleNamespace(id="vr_stale")
    session.execute.return_value.scalars.return_value.all.side_effect = [
      [lever_set, month_set],  # the scenario's FactSets
      [stale_verification],  # verification rows pinned to those sets
    ]

    result = forecast_handlers.delete(
      session, DeleteForecastRequest(structure_id="struct_budget_01"), "usr_test"
    )

    assert result == "struct_budget_01"
    deleted = [call.args[0] for call in session.delete.call_args_list]
    # Verification rows first (no DB-level FK ties them to fact_sets —
    # unswept they'd orphan invisibly), then the scenario sets (they
    # attach to OTHER structures — the structure delete alone wouldn't
    # reach them), then the block.
    assert deleted == [stale_verification, lever_set, month_set, structure]
    session.commit.assert_called_once()


class TestBuildEnvelope:
  def test_renders_lever_grid_with_computed_months(self) -> None:
    structure = MagicMock()
    structure.id = "struct_budget_01"
    structure.block_type = "forecast"
    structure.name = "FY26 Operating Budget"
    structure.description = None
    structure.renderer_note = None
    structure.taxonomy_id = "tax_rs_driver"
    structure.concept_arrangement = "set"
    structure.member_arrangement = None
    structure.artifact_mechanics = ForecastMechanics(
      scenario_kind="budget",
      horizon_months=2,
      base_period="2026-06",
      levers=[
        LeverAssertionLite(
          qname="rs-driver:RevenueGrowthRate",
          element_id="el_growth",
          item_type="percent",
          values_by_period={"2026-07": 0.03},
        )
      ],
    ).model_dump(mode="json")

    atoms = SimpleNamespace(
      structure=structure,
      taxonomy_name="rs-driver",
      associations=[],
      elements=[],
      element_ids=[],
      rules=[],
      classifications_by_assoc={},
      fact_set=None,
      verification_results=[],
      verification_summary=None,
    )

    session = MagicMock()
    computed_result = MagicMock()
    computed_result.scalars.return_value.all.return_value = [date(2026, 7, 31)]
    elements_result = MagicMock()
    elements_result.scalars.return_value.all.return_value = [GROWTH]
    lever_set_result = MagicMock()
    lever_set_result.scalar_one_or_none.return_value = None
    session.execute.side_effect = [computed_result, elements_result, lever_set_result]

    with patch.object(
      forecast_handlers, "load_base_envelope_atoms", return_value=atoms
    ):
      envelope = forecast_handlers.build_envelope(session, "struct_budget_01")

    assert envelope is not None
    assert envelope.block_type == "forecast"
    assert envelope.display_name == "Forecast"
    assert envelope.category == "Planning"

    mechanics = envelope.artifact.mechanics
    assert mechanics.kind == "forecast"
    assert mechanics.computed_months == 1

    rendering = envelope.view.rendering
    assert rendering is not None
    assert len(rendering.rows) == 1
    row = rendering.rows[0]
    assert row.element_qname == "rs-driver:RevenueGrowthRate"
    assert row.item_type == "percent"
    # Horizon columns: 2026-07 asserted, 2026-08 unasserted → None.
    assert row.values == [0.03, None]
    assert [p.end for p in rendering.periods] == [
      date(2026, 7, 31),
      date(2026, 8, 31),
    ]

  def test_returns_none_for_wrong_block_type(self) -> None:
    session = MagicMock()
    with patch.object(forecast_handlers, "load_base_envelope_atoms", return_value=None):
      assert forecast_handlers.build_envelope(session, "struct_schedule") is None
