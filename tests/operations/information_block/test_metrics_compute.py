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
) -> SimpleNamespace:
  return SimpleNamespace(
    id=element_id,
    qname=qname,
    name=name,
    is_monetary=monetary,
    period_type=period_type,
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
