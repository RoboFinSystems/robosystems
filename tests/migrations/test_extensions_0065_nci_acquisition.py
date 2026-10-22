"""Migration 0065 and the committed packages it brings to ``public``: the
noncontrolling interest's acquisition row exists in rs-gaap and sits under
the interest in each equity roll-forward network, between the contributions
and the distributions, and the movement emitter names the same concept."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from robosystems.operations.roboledger.reports import fact_grid

pytestmark = pytest.mark.unit

_ROOT = Path(__file__).resolve().parents[2]
_VERSIONS = _ROOT / "migrations" / "extensions" / "versions"
_PACKAGES = _ROOT / "frameworks" / "rs-gaap" / "packages"
_ROW = "rs-gaap:NoncontrollingInterestIncreaseFromBusinessCombination"


def _load(name: str):
  spec = importlib.util.spec_from_file_location(name, _VERSIONS / f"{name}.py")
  assert spec is not None and spec.loader is not None
  module = importlib.util.module_from_spec(spec)
  spec.loader.exec_module(module)
  return module


migration = _load("0065_nci_acquisition_movement")


def _graph(standard: str) -> list[dict]:
  return json.loads((_PACKAGES / standard / "v1" / "taxonomy.jsonld").read_text())[
    "@graph"
  ]


def test_the_migration_brings_the_row_the_emitter_names():
  assert {_ROW} == migration._NEW_CONCEPTS
  assert fact_grid._NCI_MOVEMENTS["acquisition"] == _ROW
  assert set(migration._PINS) == {"rs-gaap", "rs-gaap-presentation"}


def test_the_concept_is_in_rs_gaap_as_a_credit_duration():
  concept = next(it for it in _graph("rs-gaap") if it.get("@id") == _ROW)
  assert concept["balance"] == [{"@value": "credit"}]
  assert concept["periodType"] == [{"@value": "duration"}]


def test_the_row_sits_under_the_interest_in_every_equity_rollforward():
  arcs = [
    it
    for it in _graph("rs-gaap-presentation")
    if isinstance(it.get("to"), dict) and it["to"].get("@id") == _ROW
  ]
  assert {a["role"].rsplit("/", 1)[1] for a in arcs} == {
    "Equity-rollforward",
    "Equity-rollforward-PART",
    "Equity-rollforward-LLC",
  }
  assert all(a["from"]["@id"] == fact_grid._NCI_ROOT for a in arcs)
  assert all(41300 < a["order"] < 41600 for a in arcs)
