"""Tests for Phase ζ FactSet packing on the Information Block envelope.

Exercises :func:`fact_set_to_lite` and
:func:`load_latest_fact_set_for_structure` from
:mod:`robosystems.operations.information_block.envelope` with a mocked
SQLAlchemy session — no database required.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

from robosystems.models.api.information_block import FactSetLite
from robosystems.operations.information_block.envelope import (
  fact_set_to_lite,
  load_fact_set_by_id_for_structure,
  load_latest_fact_set_for_structure,
  load_verification_results_for_structure,
)


def _make_fact_set(
  *,
  fs_id: str = "fs_2026Q1",
  structure_id: str | None = "struct_bs",
  period_start: date | None = date(2026, 1, 1),
  period_end: date = date(2026, 3, 31),
  factset_type: str = "report",
  entity_id: str = "ent_demo",
  report_id: str | None = None,
  scenario_id: str | None = None,
  provenance: dict | None = None,
) -> MagicMock:
  row = MagicMock()
  row.id = fs_id
  row.structure_id = structure_id
  row.period_start = period_start
  row.period_end = period_end
  row.factset_type = factset_type
  row.entity_id = entity_id
  row.report_id = report_id
  row.scenario_id = scenario_id
  row.provenance = provenance
  return row


class TestFactSetToLite:
  def test_projects_all_fields(self) -> None:
    prov = {"origin": "pivot", "mapping_id": "map_1", "period": "p"}
    lite = fact_set_to_lite(_make_fact_set(report_id="rep_42", provenance=prov))
    assert isinstance(lite, FactSetLite)
    assert lite.id == "fs_2026Q1"
    assert lite.structure_id == "struct_bs"
    assert lite.period_start == date(2026, 1, 1)
    assert lite.period_end == date(2026, 3, 31)
    assert lite.factset_type == "report"
    assert lite.entity_id == "ent_demo"
    assert lite.report_id == "rep_42"
    assert lite.provenance == prov

  def test_optional_fields_null(self) -> None:
    row = _make_fact_set(
      structure_id=None, period_start=None, report_id=None, factset_type="schedule"
    )
    lite = fact_set_to_lite(row)
    assert lite.structure_id is None
    assert lite.period_start is None
    assert lite.report_id is None
    assert lite.factset_type == "schedule"
    # Pre-feature / unstamped FactSets project a null provenance.
    assert lite.provenance is None


class TestLoadLatestFactSetForStructure:
  def test_returns_none_when_no_rows(self) -> None:
    session = MagicMock()
    result = MagicMock()
    result.scalar.return_value = None
    session.execute.return_value = result

    assert load_latest_fact_set_for_structure(session, "struct_bs") is None
    session.execute.assert_called_once()

  def test_projects_scalar_row_to_lite(self) -> None:
    session = MagicMock()
    result = MagicMock()
    result.scalar.return_value = _make_fact_set()
    session.execute.return_value = result

    lite = load_latest_fact_set_for_structure(session, "struct_bs")
    assert lite is not None
    assert lite.id == "fs_2026Q1"
    assert lite.factset_type == "report"

  def test_default_read_pins_actuals(self) -> None:
    """**The hijack regression.** Scenario sets live at FUTURE
    period_ends, so without the ``scenario_id IS NULL`` pin one computed
    forecast month would win this period_end-descending read and hijack
    every default envelope. Assert the pin is in the emitted SQL."""
    session = MagicMock()
    result = MagicMock()
    result.scalar.return_value = None
    session.execute.return_value = result

    load_latest_fact_set_for_structure(session, "struct_bs")
    stmt = session.execute.call_args.args[0]
    sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
    assert "scenario_id IS NULL" in sql

  def test_scenario_read_pins_that_scenario_exactly(self) -> None:
    """A scenario read binds ONLY that scenario's sets — never a mix
    with actuals (statement + scenario = the latest forecast month)."""
    session = MagicMock()
    result = MagicMock()
    result.scalar.return_value = None
    session.execute.return_value = result

    load_latest_fact_set_for_structure(session, "struct_bs", "struct_budget")
    stmt = session.execute.call_args.args[0]
    sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
    assert "scenario_id = 'struct_budget'" in sql
    assert "scenario_id IS NULL" not in sql

  def test_future_scenario_set_never_wins_default_read(self) -> None:
    """End-to-end shape of the regression: with a scenario row present
    the default read still projects the actual row the (filtered) query
    returned — the filter is what excludes the scenario set."""
    session = MagicMock()
    result = MagicMock()
    result.scalar.return_value = _make_fact_set(fs_id="fs_actual", scenario_id=None)
    session.execute.return_value = result

    lite = load_latest_fact_set_for_structure(session, "struct_bs")
    assert lite is not None
    assert lite.id == "fs_actual"
    assert lite.scenario_id is None


class TestLoadFactSetByIdForStructure:
  """Pin lookup for the Report-Block read path — match Structure + id, or None."""

  def test_returns_none_when_no_match(self) -> None:
    """The query joins on both id AND structure_id; a mismatched pin
    surfaces as no row."""
    session = MagicMock()
    result = MagicMock()
    result.scalar.return_value = None
    session.execute.return_value = result

    assert load_fact_set_by_id_for_structure(session, "struct_bs", "fs_wrong") is None
    session.execute.assert_called_once()

  def test_projects_match_to_lite(self) -> None:
    session = MagicMock()
    result = MagicMock()
    result.scalar.return_value = _make_fact_set(
      fs_id="fs_pinned", structure_id="struct_bs"
    )
    session.execute.return_value = result

    lite = load_fact_set_by_id_for_structure(session, "struct_bs", "fs_pinned")
    assert lite is not None
    assert lite.id == "fs_pinned"
    assert lite.structure_id == "struct_bs"


class TestVerificationResultsScenarioScope:
  """**The verification-bleed regression** (the F-2 analog of the FactSet
  hijack): compute-forecast verifies every scenario month against the
  statement structures, so the envelope's verification loader must scope
  results by scenario or the actuals envelope grows a failed/passed row
  per scenario month per rule."""

  @staticmethod
  def _session() -> MagicMock:
    session = MagicMock()
    session.execute.return_value.scalars.return_value.all.return_value = []
    return session

  def test_actuals_read_excludes_scenario_set_results(self) -> None:
    session = self._session()
    load_verification_results_for_structure(session, "struct_bs")
    stmt = session.execute.call_args.args[0]
    sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
    # Structure-level results (no set) stay; set-pinned results must
    # come from actuals sets only.
    assert "fact_set_id IS NULL" in sql
    assert "scenario_id IS NULL" in sql

  def test_scenario_read_includes_only_that_scenarios_results(self) -> None:
    session = self._session()
    load_verification_results_for_structure(session, "struct_bs", "struct_budget")
    stmt = session.execute.call_args.args[0]
    sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
    assert "scenario_id = 'struct_budget'" in sql
    assert "scenario_id IS NULL" not in sql
    # Structure-level (unpinned) results still ride along.
    assert "fact_set_id IS NULL" in sql
