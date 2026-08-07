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
  LineAssertionRequest,
  LineGrowthRequest,
  UpdateForecastRequest,
)
from robosystems.models.api.information_block import (
  ForecastMechanics,
  LeverAssertionLite,
  LineAssertionLite,
)
from robosystems.operations.information_block import forecast as forecast_handlers
from robosystems.operations.information_block.forecast_history import LeverHistory


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


RENT = _element("el_rent", "rs-gaap:RentExpense", source="rs-gaap", item_type=None)
LOAN = _element("el_loan", "rs-gaap:NotesPayable", source="rs-gaap", item_type=None)
LOAN.period_type = "instant"
LOAN.is_monetary = True


class TestExpandLineAssertions:
  def _session(self, *elements: MagicMock) -> MagicMock:
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.side_effect = list(elements)
    return session

  def _expand(self, session: MagicMock, assertions, calc_parents=None):
    with patch.object(
      forecast_handlers,
      "load_rs_gaap_calculations",
      return_value={key: [] for key in calc_parents or []},
    ):
      return forecast_handlers._expand_line_assertions(
        session, assertions, "2026-06", 3
      )

  def test_expands_with_element_metadata(self) -> None:
    session = self._session(RENT)
    expanded = self._expand(
      session, [LineAssertionRequest(qname="rs-gaap:RentExpense", value=0.0)]
    )
    assert len(expanded) == 1
    assertion = expanded[0]
    assert assertion.element_id == "el_rent"
    assert assertion.period_type == "duration"
    assert assertion.values_by_period == {
      "2026-07": 0.0,
      "2026-08": 0.0,
      "2026-09": 0.0,
    }

  def test_captures_instant_period_type(self) -> None:
    session = self._session(LOAN)
    expanded = self._expand(
      session, [LineAssertionRequest(qname="rs-gaap:NotesPayable", value=500.0)]
    )
    assert expanded[0].period_type == "instant"

  def test_empty_list_short_circuits(self) -> None:
    session = MagicMock()
    assert forecast_handlers._expand_line_assertions(session, [], "2026-06", 3) == []
    session.execute.assert_not_called()

  def test_rejects_rs_driver_source(self) -> None:
    session = self._session(GROWTH)
    with pytest.raises(ValueError, match="levers"):
      self._expand(
        session,
        [LineAssertionRequest(qname="rs-driver:RevenueGrowthRate", value=0.03)],
      )

  def test_rejects_rs_metric_source(self) -> None:
    metric = _element(
      "el_wc", "rs-metric:WorkingCapital", source="rs-metric", item_type=None
    )
    session = self._session(metric)
    with pytest.raises(ValueError, match="compute-metrics"):
      self._expand(
        session, [LineAssertionRequest(qname="rs-metric:WorkingCapital", value=1.0)]
      )

  def test_rejects_abstract_element(self) -> None:
    abstract = _element(
      "el_abs", "rs-gaap:OperatingExpensesAbstract", source="rs-gaap", item_type=None
    )
    abstract.is_abstract = True
    session = self._session(abstract)
    with pytest.raises(ValueError, match="abstract"):
      self._expand(
        session,
        [LineAssertionRequest(qname="rs-gaap:OperatingExpensesAbstract", value=1.0)],
      )

  def test_rejects_calc_parent_leaves_only(self) -> None:
    revenues = _element("el_rev", "rs-gaap:Revenues", source="rs-gaap", item_type=None)
    session = self._session(revenues)
    with pytest.raises(ValueError, match="leaves-only"):
      self._expand(
        session,
        [LineAssertionRequest(qname="rs-gaap:Revenues", value=100.0)],
        calc_parents=["el_rev"],
      )

  def test_rejects_unresolved_qname(self) -> None:
    session = self._session(None)
    with pytest.raises(ValueError, match="did not resolve"):
      self._expand(session, [LineAssertionRequest(qname="rs-gaap:Nope", value=1.0)])

  def test_rejects_override_outside_horizon(self) -> None:
    session = self._session(RENT)
    with pytest.raises(ValueError, match="outside the horizon"):
      self._expand(
        session,
        [
          LineAssertionRequest(
            qname="rs-gaap:RentExpense", values_by_period={"2027-01": 0.0}
          )
        ],
      )

  def test_rejects_duplicate_assertion(self) -> None:
    session = self._session(RENT, RENT)
    with pytest.raises(ValueError, match="Duplicate line-assertion"):
      self._expand(
        session,
        [
          LineAssertionRequest(qname="rs-gaap:RentExpense", value=0.0),
          LineAssertionRequest(qname="rs-gaap:RentExpense", value=1.0),
        ],
      )


class TestExpandLineGrowth:
  def _session(self, *elements: MagicMock) -> MagicMock:
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.side_effect = list(elements)
    return session

  def _expand(self, session: MagicMock, entries, calc_parents=None):
    with patch.object(
      forecast_handlers,
      "load_rs_gaap_calculations",
      return_value={key: [] for key in calc_parents or []},
    ):
      return forecast_handlers._expand_line_growth(session, entries, "2026-06", 3)

  def test_expands_rates_with_percent_item_type(self) -> None:
    session = self._session(RENT)
    expanded = self._expand(
      session, [LineGrowthRequest(qname="rs-gaap:RentExpense", value=0.02)]
    )
    assert len(expanded) == 1
    entry = expanded[0]
    assert entry.element_id == "el_rent"
    assert entry.item_type == "percent"
    assert entry.values_by_period == {
      "2026-07": 0.02,
      "2026-08": 0.02,
      "2026-09": 0.02,
    }

  def test_sparse_overrides_leave_uncovered_months_to_the_carry(self) -> None:
    session = self._session(RENT)
    expanded = self._expand(
      session,
      [
        LineGrowthRequest(
          qname="rs-gaap:RentExpense", values_by_period={"2026-07": -0.05}
        )
      ],
    )
    assert expanded[0].values_by_period == {"2026-07": -0.05}

  def test_rejects_instant_element(self) -> None:
    session = self._session(LOAN)
    with pytest.raises(ValueError, match="balance-sheet"):
      self._expand(
        session, [LineGrowthRequest(qname="rs-gaap:NotesPayable", value=0.1)]
      )

  def test_rejects_calc_parent_leaves_only(self) -> None:
    revenues = _element("el_rev", "rs-gaap:Revenues", source="rs-gaap", item_type=None)
    session = self._session(revenues)
    with pytest.raises(ValueError, match="leaves-only"):
      self._expand(
        session,
        [LineGrowthRequest(qname="rs-gaap:Revenues", value=0.03)],
        calc_parents=["el_rev"],
      )

  def test_rejects_rs_driver_source(self) -> None:
    session = self._session(GROWTH)
    with pytest.raises(ValueError, match="levers"):
      self._expand(
        session,
        [LineGrowthRequest(qname="rs-driver:RevenueGrowthRate", value=0.03)],
      )

  def test_empty_list_short_circuits(self) -> None:
    session = MagicMock()
    assert forecast_handlers._expand_line_growth(session, [], "2026-06", 3) == []
    session.execute.assert_not_called()


class TestLineGrowthConflicts:
  """One owner per line — the FINAL-lists cross-check."""

  def _lite_growth(self, qname: str = "rs-gaap:RentExpense"):
    from robosystems.models.api.information_block import LineGrowthLite

    return LineGrowthLite(
      qname=qname, element_id="el_x", values_by_period={"2026-07": 0.02}
    )

  def _lite_assertion(self, qname: str):
    return LineAssertionLite(
      qname=qname,
      element_id="el_x",
      item_type=None,
      period_type="duration",
      values_by_period={"2026-07": 0.0},
    )

  def test_overlap_with_assertions_rejected(self) -> None:
    with pytest.raises(ValueError, match="both line_growth and line_assertions"):
      forecast_handlers._check_line_growth_conflicts(
        MagicMock(),
        [],
        [self._lite_assertion("rs-gaap:RentExpense")],
        [self._lite_growth("rs-gaap:RentExpense")],
      )

  def test_active_catalog_rule_target_rejected(self) -> None:
    """Growth on Revenues while RevenueGrowthRate is set — the catalog
    rule owns the line; the lever is the right knob."""
    rule = SimpleNamespace(
      rule_variables=[
        {"variable_name": "Revenues", "variable_qname": "rs-gaap:Revenues"},
        {
          "variable_name": "RevenueGrowthRate",
          "variable_qname": "rs-driver:RevenueGrowthRate",
        },
      ],
      target_element_id="el_rev",
    )
    session = MagicMock()
    session.get.return_value = _element(
      "el_rev", "rs-gaap:Revenues", source="rs-gaap", item_type=None
    )
    lever = LeverAssertionLite(
      qname="rs-driver:RevenueGrowthRate",
      element_id="el_growth",
      item_type="percent",
      values_by_period={"2026-07": 0.03},
    )
    with (
      patch(
        "robosystems.operations.information_block.forecast_history.driver_rules",
        return_value=[rule],
      ),
      pytest.raises(ValueError, match="already driven by"),
    ):
      forecast_handlers._check_line_growth_conflicts(
        session, [lever], [], [self._lite_growth("rs-gaap:Revenues")]
      )

  def test_inactive_rule_is_no_conflict(self) -> None:
    """Same rule, but the scenario never sets its lever — growth owns
    the line freely."""
    rule = SimpleNamespace(
      rule_variables=[
        {"variable_name": "Revenues", "variable_qname": "rs-gaap:Revenues"},
        {
          "variable_name": "RevenueGrowthRate",
          "variable_qname": "rs-driver:RevenueGrowthRate",
        },
      ],
      target_element_id="el_rev",
    )
    session = MagicMock()
    with patch(
      "robosystems.operations.information_block.forecast_history.driver_rules",
      return_value=[rule],
    ):
      forecast_handlers._check_line_growth_conflicts(
        session, [], [], [self._lite_growth("rs-gaap:Revenues")]
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
    structure = SimpleNamespace(id="struct_budget_01", name="Budget 01")
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
    session.add_all.side_effect = added_facts.extend

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

  def test_line_assertions_ride_the_same_set_with_typed_periods(self) -> None:
    structure = SimpleNamespace(id="struct_budget_01", name="Budget 01")
    mechanics = ForecastMechanics(
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
      line_assertions=[
        LineAssertionLite(
          qname="rs-gaap:NotesPayable",
          element_id="el_loan",
          item_type=None,
          period_type="instant",
          values_by_period={"2026-07": 500.0, "2026-08": 500.0},
        )
      ],
    )
    session = MagicMock()
    session.execute.return_value.scalars.return_value.all.return_value = [GROWTH, LOAN]
    added_facts: list[Any] = []
    session.add.side_effect = added_facts.append
    session.add_all.side_effect = added_facts.extend

    with patch.object(
      forecast_handlers,
      "create_fact_set",
      return_value=SimpleNamespace(id="fs_lever"),
    ) as create_fs:
      forecast_handlers._write_lever_fact_set(
        session, structure, mechanics, "ent_1", "usr_test"
      )

    # The span covers the assertion months beyond the lever's.
    kwargs = create_fs.call_args.kwargs
    assert kwargs["period_start"] == date(2026, 7, 1)
    assert kwargs["period_end"] == date(2026, 8, 31)

    assertion_facts = [f for f in added_facts if f.element_id == "el_loan"]
    assert len(assertion_facts) == 2
    assert all(f.period_type == "instant" for f in assertion_facts)
    assert all(f.period_start is None for f in assertion_facts)
    assert all(f.unit == "USD" for f in assertion_facts)


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

  def _structure_with_assertions(self) -> MagicMock:
    structure = self._structure()
    mechanics = ForecastMechanics.model_validate(structure.artifact_mechanics)
    structure.artifact_mechanics = mechanics.model_copy(
      update={
        "line_assertions": [
          LineAssertionLite(
            qname="rs-gaap:RentExpense",
            element_id="el_rent",
            item_type=None,
            period_type="duration",
            values_by_period={"2026-07": 0.0},
          )
        ]
      }
    ).model_dump(mode="json")
    return structure

  def test_window_change_requires_line_assertions_when_present(self) -> None:
    structure = self._structure_with_assertions()
    session = MagicMock()
    session.get.return_value = structure
    with (
      patch.object(forecast_handlers, "_expand_levers", return_value=[]),
      pytest.raises(ValueError, match="`line_assertions`"),
    ):
      forecast_handlers.update(
        session,
        UpdateForecastRequest(
          structure_id="struct_budget_01",
          horizon_months=6,
          levers=[
            LeverAssertionRequest(qname="rs-driver:RevenueGrowthRate", value=0.03)
          ],
        ),
        "usr_test",
      )
    session.commit.assert_not_called()

  def test_line_assertions_only_update_rewrites_the_authored_set(self) -> None:
    """Passing an empty list clears the assertions AND rewrites the
    authored FactSet (full-replace semantics, levers untouched)."""
    structure = self._structure_with_assertions()
    session = MagicMock()
    session.get.return_value = structure
    existing_set = MagicMock()
    existing_set.entity_id = "ent_1"

    with (
      patch.object(
        forecast_handlers, "_load_lever_fact_set", return_value=existing_set
      ),
      patch.object(forecast_handlers, "_write_lever_fact_set") as write_set,
    ):
      forecast_handlers.update(
        session,
        UpdateForecastRequest(structure_id="struct_budget_01", line_assertions=[]),
        "usr_test",
      )

    mech = ForecastMechanics.model_validate(structure.artifact_mechanics)
    assert mech.line_assertions == []
    assert mech.levers[0].qname == "rs-driver:RevenueGrowthRate"
    session.delete.assert_called_once_with(existing_set)
    write_set.assert_called_once()

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
    docs_result = MagicMock()
    docs_result.all.return_value = []
    session.execute.side_effect = [
      computed_result,
      elements_result,
      lever_set_result,
      docs_result,
    ]

    with (
      patch.object(forecast_handlers, "load_base_envelope_atoms", return_value=atoms),
      # The realized side has its own tests (``test_forecast_history``);
      # a tenant with no closed months renders the horizon alone.
      patch.object(
        forecast_handlers, "back_solve_lever_history", return_value=LeverHistory()
      ),
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

  def test_line_assertions_render_as_assumption_rows(self) -> None:
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
      line_assertions=[
        LineAssertionLite(
          qname="rs-gaap:RentExpense",
          element_id="el_rent",
          item_type=None,
          period_type="duration",
          values_by_period={"2026-07": 0.0, "2026-08": 0.0},
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
    computed_result.scalars.return_value.all.return_value = []
    elements_result = MagicMock()
    elements_result.scalars.return_value.all.return_value = [GROWTH, RENT]
    lever_set_result = MagicMock()
    lever_set_result.scalar_one_or_none.return_value = None
    docs_result = MagicMock()
    docs_result.all.return_value = []
    session.execute.side_effect = [
      computed_result,
      elements_result,
      lever_set_result,
      docs_result,
    ]

    with (
      patch.object(forecast_handlers, "load_base_envelope_atoms", return_value=atoms),
      # The realized side has its own tests (``test_forecast_history``);
      # a tenant with no closed months renders the horizon alone.
      patch.object(
        forecast_handlers, "back_solve_lever_history", return_value=LeverHistory()
      ),
    ):
      envelope = forecast_handlers.build_envelope(session, "struct_budget_01")

    assert envelope is not None
    rendering = envelope.view.rendering
    assert rendering is not None
    assert [r.element_qname for r in rendering.rows] == [
      "rs-driver:RevenueGrowthRate",
      "rs-gaap:RentExpense",
    ]
    assertion_row = rendering.rows[1]
    assert assertion_row.values == [0.0, 0.0]
    assert assertion_row.balance_type == "debit"

  def _history_envelope(
    self,
    history: LeverHistory,
    horizon: int = 2,
    series_history: int | None = None,
    series_forecast: int | None = None,
  ):
    """Build the standard one-lever envelope against a given history."""
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
      horizon_months=horizon,
      base_period="2026-06",
      levers=[
        LeverAssertionLite(
          qname="rs-driver:RevenueGrowthRate",
          element_id="el_growth",
          item_type="percent",
          values_by_period={"2026-07": 0.03, "2026-08": 0.03},
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
    computed_result.scalars.return_value.all.return_value = []
    elements_result = MagicMock()
    elements_result.scalars.return_value.all.return_value = [GROWTH]
    lever_set_result = MagicMock()
    lever_set_result.scalar_one_or_none.return_value = None
    docs_result = MagicMock()
    docs_result.all.return_value = []
    session.execute.side_effect = [
      computed_result,
      elements_result,
      lever_set_result,
      docs_result,
    ]

    with (
      patch.object(forecast_handlers, "load_base_envelope_atoms", return_value=atoms),
      patch.object(forecast_handlers, "back_solve_lever_history", return_value=history),
    ):
      return forecast_handlers.build_envelope(
        session,
        "struct_budget_01",
        series_history=series_history,
        series_forecast=series_forecast,
      )

  def test_realized_months_extend_the_grid_behind_the_seam(self) -> None:
    envelope = self._history_envelope(
      LeverHistory(
        months=["2026-05", "2026-06"],
        lever_values={
          "rs-driver:RevenueGrowthRate": {"2026-05": 0.021, "2026-06": 0.038}
        },
      )
    )
    assert envelope is not None
    rendering = envelope.view.rendering
    assert rendering is not None
    assert [p.end for p in rendering.periods] == [
      date(2026, 5, 31),
      date(2026, 6, 30),
      date(2026, 7, 31),
      date(2026, 8, 31),
    ]
    # Realized rates, then the asserted horizon — one continuous series.
    assert rendering.rows[0].values == [0.021, 0.038, 0.03, 0.03]
    # The seam marker rides the forward columns only.
    assert [p.forecast for p in rendering.periods] == [None, None, True, True]

  def test_a_closed_horizon_month_shows_the_realized_rate_not_the_assertion(
    self,
  ) -> None:
    # The scenario asserted 3% for July; July has since closed at 1.2%.
    envelope = self._history_envelope(
      LeverHistory(
        months=["2026-06", "2026-07"],
        lever_values={
          "rs-driver:RevenueGrowthRate": {"2026-06": 0.038, "2026-07": 0.012}
        },
      )
    )
    assert envelope is not None
    rendering = envelope.view.rendering
    assert rendering is not None
    assert [p.end for p in rendering.periods] == [
      date(2026, 6, 30),
      date(2026, 7, 31),
      date(2026, 8, 31),
    ]
    assert rendering.rows[0].values == [0.038, 0.012, 0.03]
    assert [p.forecast for p in rendering.periods] == [None, None, True]

  def test_a_blank_realized_cell_stays_blank(self) -> None:
    envelope = self._history_envelope(
      LeverHistory(months=["2026-05", "2026-06"], lever_values={})
    )
    assert envelope is not None
    rendering = envelope.view.rendering
    assert rendering is not None
    assert rendering.rows[0].values == [None, None, 0.03, 0.03]

  def test_series_windows_trim_the_axis_in_register_with_statements(self) -> None:
    """The Plan grid unions this axis with the windowed statement series;
    an unwindowed assumptions axis resurfaces every trimmed month as a
    phantom column (statement rows blank, mis-read as forecast)."""
    envelope = self._history_envelope(
      LeverHistory(
        months=["2026-04", "2026-05", "2026-06"],
        lever_values={
          "rs-driver:RevenueGrowthRate": {
            "2026-04": 0.011,
            "2026-05": 0.021,
            "2026-06": 0.038,
          }
        },
      ),
      series_history=1,
      series_forecast=1,
    )
    assert envelope is not None
    rendering = envelope.view.rendering
    assert rendering is not None
    # Last 1 actual month + first 1 forecast month — seam-adjacent.
    assert [p.end for p in rendering.periods] == [
      date(2026, 6, 30),
      date(2026, 7, 31),
    ]
    assert [p.forecast for p in rendering.periods] == [None, True]
    assert rendering.rows[0].values == [0.038, 0.03]

  def test_series_windows_none_keep_the_full_axis(self) -> None:
    envelope = self._history_envelope(
      LeverHistory(
        months=["2026-05", "2026-06"],
        lever_values={
          "rs-driver:RevenueGrowthRate": {"2026-05": 0.021, "2026-06": 0.038}
        },
      ),
      series_history=None,
      series_forecast=None,
    )
    assert envelope is not None
    rendering = envelope.view.rendering
    assert rendering is not None
    assert len(rendering.periods) == 4

  def test_returns_none_for_wrong_block_type(self) -> None:
    session = MagicMock()
    with patch.object(forecast_handlers, "load_base_envelope_atoms", return_value=None):
      assert forecast_handlers.build_envelope(session, "struct_schedule") is None
