"""Tests for metric block handler — build_envelope distinct paths."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from robosystems.operations.information_block import metric as metric_handlers


def _exec_result(
  *, scalars_all: list[Any] | None = None, scalar: Any = None
) -> MagicMock:
  result = MagicMock()
  if scalars_all is not None:
    result.scalars.return_value.all.return_value = scalars_all
  result.scalar.return_value = scalar
  return result


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

  def test_packs_mechanics_from_artifact_mechanics_column(self) -> None:
    session = MagicMock()
    structure = MagicMock()
    structure.id = "struct_metric_1"
    structure.block_type = "metric"
    structure.name = "Debt-to-Equity Ratio"
    structure.description = "D/E covenant test"
    structure.taxonomy_id = "tax_01"
    structure.concept_arrangement = "arithmetic"
    structure.member_arrangement = None
    structure.renderer_note = None
    structure.artifact_mechanics = {
      "kind": "metric",
      "source_block_ids": ["struct_bs"],
      "derivation_type": "ratio",
      "expression": "$TotalDebt / $TotalEquity",
    }
    session.get.return_value = structure
    session.execute.side_effect = [
      _exec_result(scalar="US GAAP"),  # taxonomy name
      _exec_result(scalar=None),  # latest fact set → None
      _exec_result(scalars_all=[]),  # verification results
    ]

    envelope = metric_handlers.build_envelope(session, "struct_metric_1")
    assert envelope is not None
    assert envelope.id == "struct_metric_1"
    assert envelope.block_type == "metric"
    assert envelope.name == "Debt-to-Equity Ratio"
    assert envelope.display_name == "Metric"
    assert envelope.category == "Reporting"
    assert envelope.taxonomy_name == "US GAAP"
    mechanics = envelope.artifact.mechanics
    assert mechanics.kind == "metric"
    assert mechanics.source_block_ids == ["struct_bs"]
    assert mechanics.derivation_type == "ratio"
    assert envelope.fact_set is None
    assert envelope.verification_results == []

  def test_defaults_mechanics_when_artifact_mechanics_null(self) -> None:
    session = MagicMock()
    structure = MagicMock()
    structure.id = "struct_metric_2"
    structure.block_type = "metric"
    structure.name = "Gross Margin"
    structure.description = None
    structure.taxonomy_id = "tax_02"
    structure.concept_arrangement = None
    structure.member_arrangement = None
    structure.renderer_note = None
    structure.artifact_mechanics = None
    session.get.return_value = structure
    session.execute.side_effect = [
      _exec_result(scalar=None),  # taxonomy name
      _exec_result(scalar=None),  # latest fact set → None
      _exec_result(scalars_all=[]),  # verification results
    ]

    envelope = metric_handlers.build_envelope(session, "struct_metric_2")
    assert envelope is not None
    mechanics = envelope.artifact.mechanics
    assert mechanics.kind == "metric"
    assert mechanics.source_block_ids == []
    assert mechanics.derivation_type is None
    assert mechanics.expression is None
