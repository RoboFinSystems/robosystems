"""Tests for the metric block envelope builder — the standing time series."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

from robosystems.models.api.information_block import FactSetLite
from robosystems.operations.information_block import metric as metric_handlers
from robosystems.operations.information_block.envelope import BaseEnvelopeAtoms


def _scalars(items: list[Any]) -> MagicMock:
  result = MagicMock()
  result.scalars.return_value.all.return_value = items
  return result


def _structure(*, mechanics: dict | None = None) -> MagicMock:
  s = MagicMock()
  s.id = "str_metrics"
  s.block_type = "metric"
  s.name = "Key Financial Metrics"
  s.description = None
  s.taxonomy_id = "tax_lib"
  s.artifact_mechanics = mechanics
  s.concept_arrangement = "arithmetic"
  s.member_arrangement = None
  s.renderer_note = None
  return s


def _element(
  element_id: str, qname: str, name: str, *, item_type: str | None = None
) -> SimpleNamespace:
  return SimpleNamespace(
    id=element_id,
    qname=qname,
    name=name,
    code=None,
    element_type="concept",
    is_abstract=False,
    is_monetary=False,
    balance_type="debit",
    period_type="instant",
    item_type=item_type,
  )


EL_WC = _element("el_wc", "rs-metric:WorkingCapital", "Working Capital")
EL_CR = _element("el_cr", "rs-metric:CurrentRatio", "Current Ratio")


def _arc(to_element_id: str, order: float) -> SimpleNamespace:
  return SimpleNamespace(
    id=f"arc_{to_element_id}",
    from_element_id="el_kfm",
    to_element_id=to_element_id,
    order_value=order,
    association_type="presentation",
    arcrole="http://www.xbrl.org/2003/arcrole/parent-child",
    weight=None,
  )


def _fact_set(
  fs_id: str, period_end: date, scenario_id: str | None = None
) -> SimpleNamespace:
  return SimpleNamespace(
    id=fs_id,
    structure_id="str_metrics",
    period_start=None,
    period_end=period_end,
    factset_type="metric",
    entity_id="ent_1",
    report_id=None,
    scenario_id=scenario_id,
    provenance={"origin": "derived", "computation": "compute-metrics"},
  )


def _fact(
  fact_id: str, element_id: str, value: float, fs_id: str, period_end: date
) -> SimpleNamespace:
  return SimpleNamespace(
    id=fact_id,
    element_id=element_id,
    value=value,
    string_value=None,
    fact_type="Numeric",
    content_type=None,
    period_start=None,
    period_end=period_end,
    period_type="instant",
    unit="pure",
    fact_scope="in_scope",
    fact_set_id=fs_id,
  )


def _atoms(
  structure: MagicMock, *, associations: list | None = None
) -> BaseEnvelopeAtoms:
  return BaseEnvelopeAtoms(
    structure=structure,
    taxonomy_name="rs-metric",
    associations=associations if associations is not None else [],
    elements=[EL_WC, EL_CR],  # type: ignore[list-item]
    element_ids=["el_wc", "el_cr"],
    rules=[],
    classifications_by_assoc={},
    fact_set=None,
    verification_results=[],
    verification_summary=None,
  )


P1 = date(2025, 12, 31)
P2 = date(2026, 12, 31)


class TestBuildEnvelope:
  def test_returns_none_when_structure_missing(self) -> None:
    session = MagicMock()
    session.get.return_value = None
    assert metric_handlers.build_envelope(session, "struct_missing") is None

  def test_returns_none_when_structure_wrong_type(self) -> None:
    session = MagicMock()
    structure = MagicMock()
    structure.block_type = "schedule"
    session.get.return_value = structure
    assert metric_handlers.build_envelope(session, "struct_other") is None

  def test_multi_period_rendering_aligns_values_per_column(self) -> None:
    structure = _structure()
    arcs = [_arc("el_wc", 1.0), _arc("el_cr", 2.0)]
    fs1, fs2 = _fact_set("fs1", P1), _fact_set("fs2", P2)
    facts = [
      _fact("f1", "el_wc", 55.0, "fs1", P1),
      _fact("f2", "el_wc", 60.0, "fs2", P2),
      # CurrentRatio only computed for the second period.
      _fact("f3", "el_cr", 2.5, "fs2", P2),
    ]
    session = MagicMock()
    session.execute.side_effect = [
      _scalars([fs1, fs2]),  # metric FactSets, period asc
      _scalars(facts),
    ]

    with patch.object(
      metric_handlers,
      "load_base_envelope_atoms",
      return_value=_atoms(structure, associations=arcs),
    ):
      envelope = metric_handlers.build_envelope(session, "str_metrics")

    assert envelope is not None
    rendering = envelope.view.rendering
    assert [p.end for p in rendering.periods] == [P1, P2]
    assert [r.element_qname for r in rendering.rows] == [
      "rs-metric:WorkingCapital",
      "rs-metric:CurrentRatio",
    ]
    assert rendering.rows[0].values == [55.0, 60.0]
    assert rendering.rows[1].values == [None, 2.5]
    assert len(envelope.facts) == 3
    assert envelope.fact_set is not None
    assert envelope.fact_set.id == "fs2"  # latest period is the envelope's set

    # The chart arm rides alongside — panel per format family (both
    # fixture elements are untyped non-monetary → one untyped panel),
    # series joining the rendering rows by element_id.
    chart = envelope.view.chart
    assert chart is not None
    assert len(chart.panels) == 1
    assert [s.element_id for s in chart.panels[0].series] == ["el_wc", "el_cr"]

  def test_never_computed_block_renders_catalog_skeleton(self) -> None:
    structure = _structure()
    arcs = [_arc("el_wc", 1.0), _arc("el_cr", 2.0)]
    session = MagicMock()
    session.execute.side_effect = [_scalars([])]  # no metric FactSets

    with patch.object(
      metric_handlers,
      "load_base_envelope_atoms",
      return_value=_atoms(structure, associations=arcs),
    ):
      envelope = metric_handlers.build_envelope(session, "str_metrics")

    assert envelope is not None
    rendering = envelope.view.rendering
    assert rendering.periods == []
    assert [r.element_name for r in rendering.rows] == [
      "Working Capital",
      "Current Ratio",
    ]
    assert all(r.values == [] for r in rendering.rows)
    assert envelope.facts == []
    assert envelope.fact_set is None

  def test_fact_set_pin_restricts_to_one_period(self) -> None:
    structure = _structure()
    arcs = [_arc("el_wc", 1.0)]
    fs2 = _fact_set("fs2", P2)
    session = MagicMock()
    session.get.return_value = fs2  # _load_metric_fact_sets pin path
    session.execute.side_effect = [
      _scalars([_fact("f2", "el_wc", 60.0, "fs2", P2)]),
    ]
    pinned_atoms = BaseEnvelopeAtoms(
      structure=structure,
      taxonomy_name="rs-metric",
      associations=arcs,
      elements=[EL_WC, EL_CR],  # type: ignore[list-item]
      element_ids=["el_wc", "el_cr"],
      rules=[],
      classifications_by_assoc={},
      fact_set=FactSetLite(
        id="fs2",
        structure_id="str_metrics",
        period_end=P2,
        factset_type="metric",
        entity_id="ent_1",
      ),
      verification_results=[],
      verification_summary=None,
    )

    with patch.object(
      metric_handlers, "load_base_envelope_atoms", return_value=pinned_atoms
    ):
      envelope = metric_handlers.build_envelope(
        session, "str_metrics", fact_set_id="fs2"
      )

    assert envelope is not None
    rendering = envelope.view.rendering
    assert [p.end for p in rendering.periods] == [P2]
    assert rendering.rows[0].values == [60.0]

  def test_mechanics_from_artifact_mechanics_column(self) -> None:
    structure = _structure(
      mechanics={
        "kind": "metric",
        "source_block_ids": ["struct_bs"],
        "derivation_type": "ratio",
        "expression": "$TotalDebt / $TotalEquity",
      }
    )
    session = MagicMock()
    session.execute.side_effect = [_scalars([])]

    with patch.object(
      metric_handlers, "load_base_envelope_atoms", return_value=_atoms(structure)
    ):
      envelope = metric_handlers.build_envelope(session, "str_metrics")

    assert envelope is not None
    assert envelope.display_name == "Metric"
    assert envelope.category == "Reporting"
    assert envelope.taxonomy_name == "rs-metric"
    mechanics = envelope.artifact.mechanics
    assert mechanics.kind == "metric"
    assert mechanics.source_block_ids == ["struct_bs"]
    assert mechanics.derivation_type == "ratio"

  def test_mechanics_default_when_column_null(self) -> None:
    structure = _structure(mechanics=None)
    session = MagicMock()
    session.execute.side_effect = [_scalars([])]

    with patch.object(
      metric_handlers, "load_base_envelope_atoms", return_value=_atoms(structure)
    ):
      envelope = metric_handlers.build_envelope(session, "str_metrics")

    assert envelope is not None
    mechanics = envelope.artifact.mechanics
    assert mechanics.kind == "metric"
    assert mechanics.source_block_ids == []
    assert mechanics.derivation_type is None
    assert mechanics.expression is None


class TestScenarioSeriesMerge:
  """The moving seam: scenario mode merges actual + scenario standing
  sets into one continuous series, actuals preferred at any overlap."""

  P_MAR = date(2026, 3, 31)
  P_APR = date(2026, 4, 30)

  def test_default_query_pins_actuals(self) -> None:
    session = MagicMock()
    session.execute.return_value = _scalars([])
    metric_handlers._load_metric_fact_sets(session, "str_metrics", None)
    stmt = session.execute.call_args.args[0]
    sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
    assert "scenario_id IS NULL" in sql

  def test_scenario_query_widens_to_both_slices(self) -> None:
    session = MagicMock()
    session.execute.return_value = _scalars([])
    metric_handlers._load_metric_fact_sets(
      session, "str_metrics", None, "struct_budget"
    )
    stmt = session.execute.call_args.args[0]
    sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
    assert "scenario_id IS NULL" in sql
    assert "scenario_id = 'struct_budget'" in sql

  def test_seam_prefers_actual_at_overlapping_period_end(self) -> None:
    """The query orders actuals (NULL scenario) before scenario rows per
    period_end; the per-period dedupe keeps the first — so a month that
    closed after the forecast was computed reads as actual."""
    actual_mar = _fact_set("fs_actual_mar", self.P_MAR, scenario_id=None)
    scenario_mar = _fact_set("fs_scn_mar", self.P_MAR, scenario_id="struct_budget")
    scenario_apr = _fact_set("fs_scn_apr", self.P_APR, scenario_id="struct_budget")
    session = MagicMock()
    session.execute.return_value = _scalars([actual_mar, scenario_mar, scenario_apr])
    merged = metric_handlers._load_metric_fact_sets(
      session, "str_metrics", None, "struct_budget"
    )
    assert [fs.id for fs in merged] == ["fs_actual_mar", "fs_scn_apr"]
