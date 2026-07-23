"""Tests for ``cmd_compute_metrics`` — Derive rules → standing metric FactSet."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.information_block import ComputeMetricsRequest
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.operations.information_block import metrics as metrics_mod

PERIOD_END = date(2026, 3, 31)


def _scalars(items: list[Any]) -> MagicMock:
  result = MagicMock()
  result.scalars.return_value.all.return_value = items
  return result


def _scalar_one(value: Any) -> MagicMock:
  result = MagicMock()
  result.scalar_one_or_none.return_value = value
  return result


def _first(row: Any) -> MagicMock:
  result = MagicMock()
  result.first.return_value = row
  return result


def _scalar(value: Any) -> MagicMock:
  result = MagicMock()
  result.scalar.return_value = value
  return result


def _fetchone(row: Any) -> MagicMock:
  result = MagicMock()
  result.fetchone.return_value = row
  return result


def _structure(block_type: str = "metric") -> MagicMock:
  s = MagicMock()
  s.id = "str_metrics"
  s.block_type = block_type
  return s


def _element(
  element_id: str,
  qname: str,
  name: str,
  *,
  monetary: bool = False,
  period_type: str = "instant",
  item_type: str | None = None,
) -> SimpleNamespace:
  return SimpleNamespace(
    id=element_id,
    qname=qname,
    name=name,
    is_monetary=monetary,
    period_type=period_type,
    item_type=item_type,
  )


EL_WC = _element("el_wc", "rs-metric:WorkingCapital", "Working Capital", monetary=True)
EL_CR = _element("el_cr", "rs-metric:CurrentRatio", "Current Ratio")
EL_IC = _element(
  "el_ic", "rs-metric:InterestCoverage", "Interest Coverage", period_type="duration"
)
EL_AC = _element("el_ac", "rs-gaap:AssetsCurrent", "Current Assets")
EL_LC = _element("el_lc", "rs-gaap:LiabilitiesCurrent", "Current Liabilities")
EL_OIL = _element("el_oil", "rs-gaap:OperatingIncomeLoss", "Operating Income")
EL_IE = _element("el_ie", "rs-gaap:InterestExpense", "Interest Expense")


def _arc(to_element_id: str, order: float) -> SimpleNamespace:
  return SimpleNamespace(
    from_element_id="el_kfm", to_element_id=to_element_id, order_value=order
  )


def _rule(
  rule_id: str,
  target_element_id: str,
  expression: str,
  variables: list[tuple[str, str]],
) -> SimpleNamespace:
  return SimpleNamespace(
    id=rule_id,
    target_element_id=target_element_id,
    rule_pattern="Derive",
    rule_expression=expression,
    rule_variables=[{"variable_name": n, "variable_qname": q} for n, q in variables],
  )


RULE_WC = _rule(
  "rule_wc",
  "el_wc",
  "$WorkingCapital = ($AssetsCurrent - $LiabilitiesCurrent)",
  [
    ("WorkingCapital", "rs-metric:WorkingCapital"),
    ("AssetsCurrent", "rs-gaap:AssetsCurrent"),
    ("LiabilitiesCurrent", "rs-gaap:LiabilitiesCurrent"),
  ],
)
RULE_CR = _rule(
  "rule_cr",
  "el_cr",
  "$CurrentRatio = ($AssetsCurrent / $LiabilitiesCurrent)",
  [
    ("CurrentRatio", "rs-metric:CurrentRatio"),
    ("AssetsCurrent", "rs-gaap:AssetsCurrent"),
    ("LiabilitiesCurrent", "rs-gaap:LiabilitiesCurrent"),
  ],
)
RULE_IC = _rule(
  "rule_ic",
  "el_ic",
  "$InterestCoverage = ($OperatingIncomeLoss / $InterestExpense)",
  [
    ("InterestCoverage", "rs-metric:InterestCoverage"),
    ("OperatingIncomeLoss", "rs-gaap:OperatingIncomeLoss"),
    ("InterestExpense", "rs-gaap:InterestExpense"),
  ],
)


def _session(structure: MagicMock, elements: dict[str, Any]) -> MagicMock:
  session = MagicMock()

  def _get(model, pk):
    if pk == structure.id:
      return structure
    return elements.get(pk)

  session.get.side_effect = _get
  return session


def _body(**overrides: Any) -> ComputeMetricsRequest:
  kwargs: dict[str, Any] = {
    "structure_id": "str_metrics",
    "period_end": PERIOD_END,
    "entity_id": "ent_1",
  }
  kwargs.update(overrides)
  return ComputeMetricsRequest(**kwargs)


class TestComputeMetrics:
  def test_computes_all_metrics_in_arc_order(self) -> None:
    structure = _structure()
    session = _session(
      structure, {"el_wc": EL_WC, "el_cr": EL_CR, "el_ac": EL_AC, "el_lc": EL_LC}
    )
    session.execute.side_effect = [
      _scalars([_arc("el_wc", 1.0), _arc("el_cr", 2.0)]),
      # Reversed order from the DB — the arc order must re-sort them.
      _scalars([RULE_CR, RULE_WC]),
      _scalar_one(EL_AC),  # qname lookup AssetsCurrent
      _first((100.0, None)),  # bind AssetsCurrent (rule_wc)
      _scalar_one(EL_LC),  # qname lookup LiabilitiesCurrent
      _first((40.0, None)),  # bind LiabilitiesCurrent (rule_wc)
      _first((100.0, None)),  # bind AssetsCurrent (rule_cr, qname cached)
      _first((40.0, None)),  # bind LiabilitiesCurrent (rule_cr)
      _scalar_one(None),  # standing FactSet lookup
    ]

    with patch.object(
      metrics_mod,
      "create_fact_set",
      return_value=SimpleNamespace(id="fs_new", provenance=None),
    ) as create_fs:
      response = metrics_mod.cmd_compute_metrics(session, _body(), "usr_1")

    assert [m.element_qname for m in response.computed] == [
      "rs-metric:WorkingCapital",
      "rs-metric:CurrentRatio",
    ]
    wc, cr = response.computed
    assert wc.value == pytest.approx(60.0)
    assert wc.unit == "USD"
    assert cr.value == pytest.approx(2.5)
    assert cr.unit == "pure"
    assert response.skipped == []
    assert response.fact_set_id == "fs_new"
    create_fs.assert_called_once()
    assert create_fs.call_args.kwargs["factset_type"] == "metric"

    added_facts = [
      c.args[0] for c in session.add.call_args_list if isinstance(c.args[0], Fact)
    ]
    assert len(added_facts) == 2
    for fact in added_facts:
      assert fact.fact_set_id == "fs_new"
      assert fact.fact_type == "Numeric"
      assert fact.period_end == PERIOD_END
      assert fact.period_start is None  # instant metrics
      assert fact.entity_id == "ent_1"

  def test_missing_operand_skips_with_missing_list(self) -> None:
    structure = _structure()
    session = _session(structure, {"el_ic": EL_IC, "el_oil": EL_OIL, "el_ie": EL_IE})
    session.execute.side_effect = [
      _scalars([_arc("el_ic", 1.0)]),
      _scalars([RULE_IC]),
      _scalar_one(EL_OIL),
      _first((50.0, date(2025, 4, 1))),
      _scalar_one(EL_IE),
      _first(None),  # no InterestExpense fact — debt-free entity
      _scalar_one(None),  # standing lookup
    ]

    with patch.object(metrics_mod, "create_fact_set") as create_fs:
      response = metrics_mod.cmd_compute_metrics(session, _body(), "usr_1")

    assert response.computed == []
    assert len(response.skipped) == 1
    skip = response.skipped[0]
    assert skip.element_qname == "rs-metric:InterestCoverage"
    assert skip.missing == ["rs-gaap:InterestExpense"]
    assert response.fact_set_id is None
    create_fs.assert_not_called()

  def test_division_by_zero_skips_with_reason(self) -> None:
    structure = _structure()
    session = _session(structure, {"el_cr": EL_CR, "el_ac": EL_AC, "el_lc": EL_LC})
    session.execute.side_effect = [
      _scalars([_arc("el_cr", 1.0)]),
      _scalars([RULE_CR]),
      _scalar_one(EL_AC),
      _first((100.0, None)),
      _scalar_one(EL_LC),
      _first((0.0, None)),
      _scalar_one(None),
    ]

    response = metrics_mod.cmd_compute_metrics(session, _body(), "usr_1")

    assert response.computed == []
    assert len(response.skipped) == 1
    assert "division by zero" in response.skipped[0].reason

  def test_rerun_replaces_prior_facts_in_same_standing_set(self) -> None:
    structure = _structure()
    session = _session(structure, {"el_wc": EL_WC, "el_ac": EL_AC, "el_lc": EL_LC})
    standing = MagicMock()
    standing.id = "fs_old"
    old_facts = [MagicMock(), MagicMock()]
    session.execute.side_effect = [
      _scalars([_arc("el_wc", 1.0)]),
      _scalars([RULE_WC]),
      _scalar_one(EL_AC),
      _first((120.0, None)),
      _scalar_one(EL_LC),
      _first((70.0, None)),
      _scalar_one(standing),
      _scalars(old_facts),  # prior facts of the standing set
    ]

    response = metrics_mod.cmd_compute_metrics(session, _body(), "usr_1")

    assert response.fact_set_id == "fs_old"
    assert [c.args[0] for c in session.delete.call_args_list] == old_facts
    assert standing.provenance["origin"] == "derived"
    added_facts = [
      c.args[0] for c in session.add.call_args_list if isinstance(c.args[0], Fact)
    ]
    assert len(added_facts) == 1
    assert added_facts[0].fact_set_id == "fs_old"
    assert added_facts[0].value == pytest.approx(50.0)

  def test_duration_metric_carries_operand_period_start(self) -> None:
    structure = _structure()
    session = _session(structure, {"el_ic": EL_IC, "el_oil": EL_OIL, "el_ie": EL_IE})
    fy_start = date(2025, 4, 1)
    session.execute.side_effect = [
      _scalars([_arc("el_ic", 1.0)]),
      _scalars([RULE_IC]),
      _scalar_one(EL_OIL),
      _first((50.0, fy_start)),
      _scalar_one(EL_IE),
      _first((10.0, fy_start)),
      _scalar_one(None),
    ]

    with patch.object(
      metrics_mod,
      "create_fact_set",
      return_value=SimpleNamespace(id="fs_new", provenance=None),
    ):
      response = metrics_mod.cmd_compute_metrics(session, _body(), "usr_1")

    assert response.computed[0].value == pytest.approx(5.0)
    assert response.computed[0].period_type == "duration"
    added_facts = [
      c.args[0] for c in session.add.call_args_list if isinstance(c.args[0], Fact)
    ]
    assert added_facts[0].period_start == fy_start
    assert added_facts[0].period_type == "duration"

  def test_wrong_block_type_raises(self) -> None:
    structure = _structure(block_type="schedule")
    session = _session(structure, {})
    with pytest.raises(ValueError, match="block_type='metric'"):
      metrics_mod.cmd_compute_metrics(session, _body(), "usr_1")

  def test_structure_missing_raises(self) -> None:
    session = MagicMock()
    session.get.return_value = None
    with pytest.raises(ValueError, match="Structure not found"):
      metrics_mod.cmd_compute_metrics(session, _body(), "usr_1")

  def test_no_entity_raises(self) -> None:
    structure = _structure()
    session = _session(structure, {})
    session.execute.side_effect = [_fetchone(None)]
    with pytest.raises(ValueError, match="No entity found"):
      metrics_mod.cmd_compute_metrics(session, _body(entity_id=None), "usr_1")


EL_NI = _element("el_ni", "rs-gaap:NetIncomeLoss", "Net Income", period_type="duration")
EL_REV = _element("el_rev", "rs-gaap:Revenues", "Revenues", period_type="duration")
EL_SE = _element("el_se", "rs-gaap:StockholdersEquity", "Stockholders' Equity")
EL_A = _element("el_a", "rs-gaap:Assets", "Assets")
EL_ROE = _element(
  "el_roe",
  "rs-metric:ReturnOnEquity",
  "Return on Equity",
  period_type="duration",
  item_type="percent",
)
EL_NPM = _element(
  "el_npm",
  "rs-metric:NetProfitMargin",
  "Net Profit Margin",
  period_type="duration",
  item_type="percent",
)
EL_EM = _element(
  "el_em",
  "rs-metric:EquityMultiplier",
  "Equity Multiplier",
  period_type="duration",
  item_type="multiple",
)

RULE_ROE = _rule(
  "rule_roe",
  "el_roe",
  "$ReturnOnEquity = ($NetIncomeLoss / avg($StockholdersEquity))",
  [
    ("ReturnOnEquity", "rs-metric:ReturnOnEquity"),
    ("NetIncomeLoss", "rs-gaap:NetIncomeLoss"),
    ("StockholdersEquity", "rs-gaap:StockholdersEquity"),
  ],
)
RULE_EM = _rule(
  "rule_em",
  "el_em",
  "$EquityMultiplier = (avg($Assets) / avg($StockholdersEquity))",
  [
    ("EquityMultiplier", "rs-metric:EquityMultiplier"),
    ("Assets", "rs-gaap:Assets"),
    ("StockholdersEquity", "rs-gaap:StockholdersEquity"),
  ],
)
RULE_NPM = _rule(
  "rule_npm",
  "el_npm",
  "$NetProfitMargin = ($NetIncomeLoss / $Revenues)",
  [
    ("NetProfitMargin", "rs-metric:NetProfitMargin"),
    ("NetIncomeLoss", "rs-gaap:NetIncomeLoss"),
    ("Revenues", "rs-gaap:Revenues"),
  ],
)
RULE_DOUBLE_NPM = _rule(
  "rule_double_npm",
  "el_em",
  "$EquityMultiplier = ($NetProfitMargin * 2)",
  [
    ("EquityMultiplier", "rs-metric:EquityMultiplier"),
    ("NetProfitMargin", "rs-metric:NetProfitMargin"),
  ],
)


class TestAvgOperand:
  """avg($X) — the M-2 period-average aggregate."""

  def test_avg_via_request_window(self) -> None:
    """body.period_start present → begin fact binds at period_start - 1d."""
    structure = _structure()
    session = _session(structure, {"el_roe": EL_ROE, "el_ni": EL_NI, "el_se": EL_SE})
    session.execute.side_effect = [
      _scalars([_arc("el_roe", 1.0)]),
      _scalars([RULE_ROE]),
      _scalar_one(EL_NI),
      _first((30.0, date(2026, 3, 1))),  # NetIncomeLoss for the month
      _scalar_one(EL_SE),
      _first((120.0, None)),  # StockholdersEquity end instant
      _first((80.0, None)),  # StockholdersEquity begin instant (@ 2026-02-28)
      _scalar_one(None),  # standing lookup
    ]

    with patch.object(
      metrics_mod,
      "create_fact_set",
      return_value=SimpleNamespace(id="fs_new", provenance=None),
    ):
      response = metrics_mod.cmd_compute_metrics(
        session, _body(period_start=date(2026, 3, 1)), "usr_1"
      )

    assert len(response.computed) == 1
    roe = response.computed[0]
    # 30 / ((80 + 120) / 2) = 0.3
    assert roe.value == pytest.approx(0.3)
    assert roe.item_type == "percent"
    assert roe.unit == "pure"
    # Exactly the choreographed queries ran — in particular, NO
    # data-driven prior lookup (the request window supplied the begin).
    assert len(session.execute.call_args_list) == 8

  def test_avg_via_duration_operand_window(self) -> None:
    """No request window — a bound duration operand's start supplies it."""
    structure = _structure()
    session = _session(structure, {"el_roe": EL_ROE, "el_ni": EL_NI, "el_se": EL_SE})
    fy_start = date(2025, 4, 1)
    session.execute.side_effect = [
      _scalars([_arc("el_roe", 1.0)]),
      _scalars([RULE_ROE]),
      _scalar_one(EL_NI),
      _first((50.0, fy_start)),  # duration operand carries the window
      _scalar_one(EL_SE),
      _first((150.0, None)),
      _first((100.0, None)),  # begin @ fy_start - 1d — no prior-lookup query
      _scalar_one(None),
    ]

    with patch.object(
      metrics_mod,
      "create_fact_set",
      return_value=SimpleNamespace(id="fs_new", provenance=None),
    ):
      response = metrics_mod.cmd_compute_metrics(session, _body(), "usr_1")

    assert response.computed[0].value == pytest.approx(50.0 / 125.0)
    assert response.skipped == []

  def test_avg_data_driven_prior_fallback_shared_across_operands(self) -> None:
    """All-instant rule with no window — one cached prior-period lookup."""
    structure = _structure()
    session = _session(structure, {"el_em": EL_EM, "el_a": EL_A, "el_se": EL_SE})
    session.execute.side_effect = [
      _scalars([_arc("el_em", 1.0)]),
      _scalars([RULE_EM]),
      _scalar_one(EL_A),
      _first((400.0, None)),  # Assets end
      _scalar_one(EL_SE),
      _first((200.0, None)),  # Equity end
      _scalar(date(2026, 2, 28)),  # data-driven prior period (ONE query)
      _first((300.0, None)),  # Assets begin
      _first((100.0, None)),  # Equity begin — prior cached, no second lookup
      _scalar_one(None),
    ]

    with patch.object(
      metrics_mod,
      "create_fact_set",
      return_value=SimpleNamespace(id="fs_new", provenance=None),
    ):
      response = metrics_mod.cmd_compute_metrics(session, _body(), "usr_1")

    # avg(A)=350, avg(SE)=150 → 2.333…
    assert response.computed[0].value == pytest.approx(350.0 / 150.0)
    assert response.computed[0].unit == "pure"
    assert response.computed[0].item_type == "multiple"

  def test_avg_with_no_prior_period_skips_with_reason(self) -> None:
    """First period of a series — the data-driven fallback finds nothing."""
    structure = _structure()
    session = _session(structure, {"el_em": EL_EM, "el_a": EL_A, "el_se": EL_SE})
    session.execute.side_effect = [
      _scalars([_arc("el_em", 1.0)]),
      _scalars([RULE_EM]),
      _scalar_one(EL_A),
      _first((400.0, None)),
      _scalar_one(EL_SE),
      _first((200.0, None)),
      _scalar(None),  # no prior report period exists
      _scalar_one(None),
    ]

    with patch.object(metrics_mod, "create_fact_set") as create_fs:
      response = metrics_mod.cmd_compute_metrics(session, _body(), "usr_1")

    assert response.computed == []
    skip = response.skipped[0]
    assert skip.element_qname == "rs-metric:EquityMultiplier"
    assert any("no prior period for avg" in m for m in skip.missing)
    create_fs.assert_not_called()

  def test_avg_missing_begin_fact_skips_with_prior_annotation(self) -> None:
    structure = _structure()
    session = _session(structure, {"el_roe": EL_ROE, "el_ni": EL_NI, "el_se": EL_SE})
    session.execute.side_effect = [
      _scalars([_arc("el_roe", 1.0)]),
      _scalars([RULE_ROE]),
      _scalar_one(EL_NI),
      _first((30.0, date(2026, 3, 1))),
      _scalar_one(EL_SE),
      _first((120.0, None)),
      _first(None),  # begin instant absent at the prior period end
      _scalar_one(None),
    ]

    response = metrics_mod.cmd_compute_metrics(
      session, _body(period_start=date(2026, 3, 1)), "usr_1"
    )

    assert response.computed == []
    skip = response.skipped[0]
    assert any("prior @ 2026-02-28" in m for m in skip.missing)


class TestComposition:
  """Metric-on-metric operands — the M-2 DuPont dependency feature."""

  def test_in_run_dependency_evaluates_first_and_reuses_value(self) -> None:
    """The composite's operand resolves from the in-run value (no bind
    query), evaluation runs dependency-first, and the response re-sorts
    to presentation-arc order (composite first here)."""
    structure = _structure()
    session = _session(
      structure,
      {"el_em": EL_EM, "el_npm": EL_NPM, "el_ni": EL_NI, "el_rev": EL_REV},
    )
    session.execute.side_effect = [
      # Composite is arc-FIRST — topo must still evaluate NPM before it.
      _scalars([_arc("el_em", 1.0), _arc("el_npm", 2.0)]),
      _scalars([RULE_DOUBLE_NPM, RULE_NPM]),
      _scalar_one(EL_NI),
      _first((20.0, date(2026, 3, 1))),
      _scalar_one(EL_REV),
      _first((100.0, date(2026, 3, 1))),
      # NO bind for the composite's NPM operand — in-run value reused.
      _scalar_one(None),
    ]

    with patch.object(
      metrics_mod,
      "create_fact_set",
      return_value=SimpleNamespace(id="fs_new", provenance=None),
    ):
      response = metrics_mod.cmd_compute_metrics(session, _body(), "usr_1")

    assert [m.element_qname for m in response.computed] == [
      "rs-metric:EquityMultiplier",  # arc order 1.0
      "rs-metric:NetProfitMargin",  # arc order 2.0
    ]
    composite, npm = response.computed
    assert npm.value == pytest.approx(0.2)
    assert composite.value == pytest.approx(0.4)
    assert response.skipped == []

  def test_skipped_dependency_cascades_with_reason(self) -> None:
    structure = _structure()
    session = _session(
      structure,
      {"el_em": EL_EM, "el_npm": EL_NPM, "el_ni": EL_NI, "el_rev": EL_REV},
    )
    session.execute.side_effect = [
      _scalars([_arc("el_em", 1.0), _arc("el_npm", 2.0)]),
      _scalars([RULE_DOUBLE_NPM, RULE_NPM]),
      _scalar_one(EL_NI),
      _first(None),  # NetIncomeLoss missing → NPM skips
      _scalar_one(EL_REV),
      _first((100.0, date(2026, 3, 1))),
      # Composite falls back to a persisted-metric bind, which also misses.
      _scalar_one(EL_NPM),
      _first(None),
      _scalar_one(None),
    ]

    with patch.object(metrics_mod, "create_fact_set") as create_fs:
      response = metrics_mod.cmd_compute_metrics(session, _body(), "usr_1")

    assert response.computed == []
    reasons = {s.element_qname: s.missing for s in response.skipped}
    assert reasons["rs-metric:NetProfitMargin"] == ["rs-gaap:NetIncomeLoss"]
    assert reasons["rs-metric:EquityMultiplier"] == [
      "rs-metric:NetProfitMargin (metric not computed)"
    ]
    create_fs.assert_not_called()

  def test_cyclic_rules_degrade_to_skips(self) -> None:
    el_a = _element("el_ma", "rs-metric:MetricA", "Metric A")
    el_b = _element("el_mb", "rs-metric:MetricB", "Metric B")
    rule_a = _rule(
      "rule_ma",
      "el_ma",
      "$MetricA = ($MetricB * 1)",
      [("MetricA", "rs-metric:MetricA"), ("MetricB", "rs-metric:MetricB")],
    )
    rule_b = _rule(
      "rule_mb",
      "el_mb",
      "$MetricB = ($MetricA * 1)",
      [("MetricB", "rs-metric:MetricB"), ("MetricA", "rs-metric:MetricA")],
    )
    structure = _structure()
    session = _session(structure, {"el_ma": el_a, "el_mb": el_b})
    session.execute.side_effect = [
      _scalars([_arc("el_ma", 1.0), _arc("el_mb", 2.0)]),
      _scalars([rule_a, rule_b]),
      _scalar_one(el_b),  # A's operand B — no in-run value yet
      _first(None),  # no persisted metric fact either
      _scalar_one(el_a),  # B's operand A
      _first(None),
      _scalar_one(None),
    ]

    with patch.object(metrics_mod, "create_fact_set") as create_fs:
      response = metrics_mod.cmd_compute_metrics(session, _body(), "usr_1")

    assert response.computed == []
    assert len(response.skipped) == 2
    for skip in response.skipped:
      assert any("(metric not computed)" in m for m in skip.missing)
    create_fs.assert_not_called()


class TestUnitFamilies:
  """item_type → fact unit mapping (M-2 unit/format)."""

  def test_days_item_type_maps_to_days_unit(self) -> None:
    el_dso = _element(
      "el_dso",
      "rs-metric:DaysSalesOutstanding",
      "Days Sales Outstanding",
      period_type="duration",
      item_type="days",
    )
    rule_dso = _rule(
      "rule_dso",
      "el_dso",
      "$DaysSalesOutstanding = ($Assets / $Revenues)",
      [
        ("DaysSalesOutstanding", "rs-metric:DaysSalesOutstanding"),
        ("Assets", "rs-gaap:Assets"),
        ("Revenues", "rs-gaap:Revenues"),
      ],
    )
    structure = _structure()
    session = _session(structure, {"el_dso": el_dso, "el_a": EL_A, "el_rev": EL_REV})
    session.execute.side_effect = [
      _scalars([_arc("el_dso", 1.0)]),
      _scalars([rule_dso]),
      _scalar_one(EL_A),
      _first((300.0, None)),
      _scalar_one(EL_REV),
      _first((10.0, date(2026, 3, 1))),
      _scalar_one(None),
    ]

    with patch.object(
      metrics_mod,
      "create_fact_set",
      return_value=SimpleNamespace(id="fs_new", provenance=None),
    ):
      response = metrics_mod.cmd_compute_metrics(session, _body(), "usr_1")

    assert response.computed[0].unit == "days"
    assert response.computed[0].item_type == "days"

  def test_monetary_item_type_maps_to_usd(self) -> None:
    el = _element(
      "el_wc2",
      "rs-metric:WorkingCapital",
      "Working Capital",
      monetary=True,
      item_type="monetary",
    )
    structure = _structure()
    session = _session(structure, {"el_wc2": el, "el_ac": EL_AC, "el_lc": EL_LC})
    rule = _rule(
      "rule_wc2",
      "el_wc2",
      "$WorkingCapital = ($AssetsCurrent - $LiabilitiesCurrent)",
      [
        ("WorkingCapital", "rs-metric:WorkingCapital"),
        ("AssetsCurrent", "rs-gaap:AssetsCurrent"),
        ("LiabilitiesCurrent", "rs-gaap:LiabilitiesCurrent"),
      ],
    )
    session.execute.side_effect = [
      _scalars([_arc("el_wc2", 1.0)]),
      _scalars([rule]),
      _scalar_one(EL_AC),
      _first((100.0, None)),
      _scalar_one(EL_LC),
      _first((40.0, None)),
      _scalar_one(None),
    ]

    with patch.object(
      metrics_mod,
      "create_fact_set",
      return_value=SimpleNamespace(id="fs_new", provenance=None),
    ):
      response = metrics_mod.cmd_compute_metrics(session, _body(), "usr_1")

    assert response.computed[0].unit == "USD"


class TestScenarioBinding:
  """The scenario pins on operand binding — the compute-side half of the
  hijack fix. Asserted on the emitted SQL (the predicates ARE the
  behavior; full-cascade coverage lives in the forecast compute tests
  and the showcase e2e)."""

  def _captured_sql(self, session: MagicMock) -> str:
    stmt = session.execute.call_args.args[0]
    return str(stmt.compile(compile_kwargs={"literal_binds": True}))

  def test_bind_operand_default_pins_actuals(self) -> None:
    session = MagicMock()
    session.execute.return_value.first.return_value = None
    metrics_mod._bind_operand(
      session,
      element_id="el_1",
      entity_id="ent_1",
      period_end=date(2026, 3, 31),
      period_start=None,
    )
    sql = self._captured_sql(session)
    assert "scenario_id IS NULL" in sql

  def test_bind_operand_scenario_widens_with_actual_fallback(self) -> None:
    """Scenario binds reach the scenario slice AND actuals — the seam
    fallback that lets an avg() begin bind at the actual base month."""
    session = MagicMock()
    session.execute.return_value.first.return_value = None
    metrics_mod._bind_operand(
      session,
      element_id="el_1",
      entity_id="ent_1",
      period_end=date(2026, 4, 30),
      period_start=None,
      scenario_id="struct_budget",
    )
    sql = self._captured_sql(session)
    assert "scenario_id IS NULL" in sql
    assert "scenario_id = 'struct_budget'" in sql

  def test_prior_period_spine_default_pins_actuals(self) -> None:
    session = MagicMock()
    session.execute.return_value.scalar.return_value = None
    metrics_mod._latest_report_period_end_before(session, "ent_1", date(2026, 3, 31))
    sql = self._captured_sql(session)
    assert "scenario_id IS NULL" in sql

  def test_prior_period_spine_scenario_widens(self) -> None:
    session = MagicMock()
    session.execute.return_value.scalar.return_value = None
    metrics_mod._latest_report_period_end_before(
      session, "ent_1", date(2026, 4, 30), "struct_budget"
    )
    sql = self._captured_sql(session)
    assert "scenario_id = 'struct_budget'" in sql
