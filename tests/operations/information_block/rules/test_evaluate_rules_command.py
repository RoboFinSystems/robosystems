"""Tests for cmd_evaluate_rules — command shape and summary counts."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

from robosystems.models.api.information_block import (
  EvaluateRulesRequest,
  EvaluateRulesResponse,
)
from robosystems.operations.information_block.rules.commands import cmd_evaluate_rules


def _make_vr(*, vr_id: str, rule_id: str, structure_id: str, status: str) -> MagicMock:
  vr = MagicMock()
  vr.id = vr_id
  vr.rule_id = rule_id
  vr.structure_id = structure_id
  vr.fact_set_id = None
  vr.status = status
  vr.message = None if status == "pass" else "some failure"
  vr.period_start = None
  vr.period_end = None
  vr.evaluated_at = None
  vr.detail = {}
  return vr


class TestCmdEvaluateRules:
  def test_returns_correct_response_shape(self) -> None:
    session = MagicMock()
    body = EvaluateRulesRequest(structure_id="struct_bs")
    rows = [
      _make_vr(vr_id="vr_1", rule_id="r1", structure_id="struct_bs", status="pass"),
      _make_vr(vr_id="vr_2", rule_id="r2", structure_id="struct_bs", status="fail"),
    ]
    with patch(
      "robosystems.operations.information_block.rules.commands.evaluate_rules_for_structure",
      return_value=rows,
    ):
      response = cmd_evaluate_rules(session, body, created_by="user_1")

    assert isinstance(response, EvaluateRulesResponse)
    assert response.structure_id == "struct_bs"
    assert len(response.results) == 2

  def test_summary_counts_are_correct(self) -> None:
    session = MagicMock()
    body = EvaluateRulesRequest(structure_id="struct_bs")
    rows = [
      _make_vr(vr_id="vr_1", rule_id="r1", structure_id="struct_bs", status="pass"),
      _make_vr(vr_id="vr_2", rule_id="r2", structure_id="struct_bs", status="fail"),
      _make_vr(vr_id="vr_3", rule_id="r3", structure_id="struct_bs", status="fail"),
      _make_vr(vr_id="vr_4", rule_id="r4", structure_id="struct_bs", status="skipped"),
    ]
    with patch(
      "robosystems.operations.information_block.rules.commands.evaluate_rules_for_structure",
      return_value=rows,
    ):
      response = cmd_evaluate_rules(session, body, created_by="user_1")

    assert response.summary["pass"] == 1
    assert response.summary["fail"] == 2
    assert response.summary["skipped"] == 1
    assert response.summary.get("error", 0) == 0

  def test_passes_period_args_to_engine(self) -> None:
    session = MagicMock()
    body = EvaluateRulesRequest(
      structure_id="struct_bs",
      fact_set_id="fs_1",
      period_start=date(2025, 1, 1),
      period_end=date(2025, 12, 31),
    )
    with patch(
      "robosystems.operations.information_block.rules.commands.evaluate_rules_for_structure",
      return_value=[],
    ) as mock_engine:
      cmd_evaluate_rules(session, body, created_by="sys")

    mock_engine.assert_called_once_with(
      session,
      "struct_bs",
      fact_set_id="fs_1",
      period_start=date(2025, 1, 1),
      period_end=date(2025, 12, 31),
      created_by="sys",
    )

  def test_empty_results_returns_zero_summary(self) -> None:
    session = MagicMock()
    body = EvaluateRulesRequest(structure_id="struct_bs")
    with patch(
      "robosystems.operations.information_block.rules.commands.evaluate_rules_for_structure",
      return_value=[],
    ):
      response = cmd_evaluate_rules(session, body, created_by="sys")

    assert response.results == []
    assert all(v == 0 for v in response.summary.values())
