"""Tests for ``get_report_package`` — Plan-C package-mode read.

Verifies the read path that drives ``/reports/[id]`` in package mode:
load Report metadata, look up its FactSets via ``fact_sets.report_id``,
rehydrate each as an ``InformationBlockEnvelope`` via
``get_information_block_for_fact_set``, and order items by
``Structure.structure_type`` (BS → IS → CF → Equity → Schedule).
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

from robosystems.models.api.information_block import (
  ArtifactResponse,
  InformationBlockEnvelope,
  InformationModelResponse,
  StatementMechanics,
)
from robosystems.operations.roboledger.reads.reports import get_report_package


def _make_report_def() -> MagicMock:
  r = MagicMock()
  r.id = "rpt_01"
  r.name = "Q1 2026 Statements"
  r.description = None
  r.taxonomy_id = "tax_usgaap_reporting"
  r.period_type = "quarterly"
  r.period_start = date(2026, 1, 1)
  r.period_end = date(2026, 3, 31)
  r.generation_status = "complete"
  r.last_generated = datetime(2026, 4, 1, tzinfo=UTC)
  r.filing_status = "draft"
  r.filed_at = None
  r.filed_by = None
  r.supersedes_id = None
  r.superseded_by_id = None
  r.source_graph_id = None
  r.source_report_id = None
  r.shared_at = None
  r.entity_name = None
  r.ai_generated = False
  r.created_at = datetime(2026, 4, 1, tzinfo=UTC)
  r.created_by = "user_01"
  return r


def _make_fact_set(*, fs_id: str, structure_id: str) -> MagicMock:
  fs = MagicMock()
  fs.id = fs_id
  fs.structure_id = structure_id
  fs.report_id = "rpt_01"
  fs.created_at = datetime(2026, 4, 1, tzinfo=UTC)
  return fs


def _make_structure(*, structure_id: str, structure_type: str) -> MagicMock:
  s = MagicMock()
  s.id = structure_id
  s.structure_type = structure_type
  return s


def _envelope(structure_id: str, block_type: str) -> InformationBlockEnvelope:
  return InformationBlockEnvelope(
    id=structure_id,
    block_type=block_type,
    name=f"{block_type}-{structure_id}",
    display_name=block_type.replace("_", " ").title(),
    category="Reporting",
    information_model=InformationModelResponse(concept_arrangement="roll_up"),
    artifact=ArtifactResponse(mechanics=StatementMechanics(kind="statement_renderer")),
  )


def _patch_response_helpers():
  return patch.multiple(
    "robosystems.operations.roboledger.reads.reports",
    resolve_entity_name=MagicMock(return_value=None),
  )


# ── Cases ───────────────────────────────────────────────────────────────


def test_returns_none_when_report_missing() -> None:
  session = MagicMock()
  session.get.return_value = None

  with _patch_response_helpers():
    assert get_report_package(session, "rpt_missing") is None


def test_assembles_items_ordered_by_structure_type() -> None:
  """IS comes back from the SQL query before BS; the result still puts
  BS (display_order=1) ahead of IS (display_order=2)."""
  session = MagicMock()
  session.get.return_value = _make_report_def()

  fs_is = _make_fact_set(fs_id="fs_is", structure_id="struct_is")
  fs_bs = _make_fact_set(fs_id="fs_bs", structure_id="struct_bs")
  struct_is = _make_structure(
    structure_id="struct_is", structure_type="income_statement"
  )
  struct_bs = _make_structure(structure_id="struct_bs", structure_type="balance_sheet")

  # Return order: IS first, BS second — to prove the read sorts deterministically.
  session.execute.return_value.all.return_value = [
    (fs_is, struct_is),
    (fs_bs, struct_bs),
  ]

  envelopes = {
    "fs_is": _envelope("struct_is", "income_statement"),
    "fs_bs": _envelope("struct_bs", "balance_sheet"),
  }

  with (
    _patch_response_helpers(),
    patch(
      "robosystems.operations.information_block.reads.get_information_block_for_fact_set",
      side_effect=lambda _s, fs_id: envelopes[fs_id],
    ),
  ):
    pkg = get_report_package(session, "rpt_01")

  assert pkg is not None
  assert [it.fact_set_id for it in pkg.items] == ["fs_bs", "fs_is"]
  assert pkg.items[0].display_order == 1  # balance_sheet
  assert pkg.items[1].display_order == 2  # income_statement


def test_skips_fact_sets_with_unregistered_block_types() -> None:
  """A FactSet whose Structure isn't a registered block_type can't render —
  the item is skipped silently rather than failing the whole package read."""
  session = MagicMock()
  session.get.return_value = _make_report_def()

  fs_known = _make_fact_set(fs_id="fs_bs", structure_id="struct_bs")
  fs_unknown = _make_fact_set(fs_id="fs_x", structure_id="struct_x")
  struct_bs = _make_structure(structure_id="struct_bs", structure_type="balance_sheet")
  struct_x = _make_structure(
    structure_id="struct_x", structure_type="chart_of_accounts"
  )
  session.execute.return_value.all.return_value = [
    (fs_known, struct_bs),
    (fs_unknown, struct_x),
  ]

  with (
    _patch_response_helpers(),
    patch(
      "robosystems.operations.information_block.reads.get_information_block_for_fact_set",
      side_effect=lambda _s, fs_id: (
        _envelope("struct_bs", "balance_sheet") if fs_id == "fs_bs" else None
      ),
    ),
  ):
    pkg = get_report_package(session, "rpt_01")

  assert pkg is not None
  assert [it.fact_set_id for it in pkg.items] == ["fs_bs"]


def test_carries_filing_lifecycle_fields_through() -> None:
  """The package envelope surfaces filing_status, filed_at, filed_by,
  supersedes/superseded chain, etc. from the underlying Report row."""
  session = MagicMock()
  rd = _make_report_def()
  rd.filing_status = "filed"
  rd.filed_at = datetime(2026, 4, 15, tzinfo=UTC)
  rd.filed_by = "user_filer"
  rd.supersedes_id = "rpt_prior"
  session.get.return_value = rd
  session.execute.return_value.all.return_value = []

  with _patch_response_helpers():
    pkg = get_report_package(session, "rpt_01")

  assert pkg is not None
  assert pkg.filing_status == "filed"
  assert pkg.filed_by == "user_filer"
  assert pkg.supersedes_id == "rpt_prior"
  assert pkg.items == []


def test_unknown_structure_type_falls_back_to_default_order() -> None:
  """Custom / future structure_types not in the display-order map land at the
  fallback slot (50) — between statements (1-4) and schedules (100)."""
  session = MagicMock()
  session.get.return_value = _make_report_def()

  fs = _make_fact_set(fs_id="fs_custom", structure_id="struct_custom")
  structure = _make_structure(structure_id="struct_custom", structure_type="custom")
  session.execute.return_value.all.return_value = [(fs, structure)]

  envelope = _envelope("struct_custom", "custom")

  with (
    _patch_response_helpers(),
    patch(
      "robosystems.operations.information_block.reads.get_information_block_for_fact_set",
      return_value=envelope,
    ),
  ):
    pkg = get_report_package(session, "rpt_01")

  assert pkg is not None
  assert len(pkg.items) == 1
  assert pkg.items[0].display_order == 50
