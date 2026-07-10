"""Tests for report command helpers — FactSet wiring."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.fact_provenance import AssertedProvenance
from robosystems.operations.roboledger.commands.reports import (
  InvalidFilingTransitionError,
  NotAuthorizedError,
  ReportNotFoundError,
  _build_structure_mapping,
  _persist_report_facts,
  _pre_create_report_fact_sets,
  _share_to_target,
  regenerate_report,
)
from robosystems.operations.roboledger.reports.network_picker import (
  NoNetworkForStatementTypeError,
  RenderNetwork,
)


class _FakeFact:
  """Stand-in for a `ReportFact` — only needs the fields the helpers read."""

  def __init__(
    self,
    element_id: str,
    value: float = 0.0,
    period_start: date | None = None,
    period_end: date | None = None,
    period_type: str = "duration",
  ) -> None:
    self.element_id = element_id
    self.value = value
    self.period_start = period_start
    self.period_end = period_end
    self.period_type = period_type


class _FakeFacts:
  def __init__(self, facts: list[_FakeFact]) -> None:
    self.facts = facts


_PICKER_PATH = "robosystems.operations.roboledger.commands.reports.get_render_network"


def test_build_structure_mapping_resolves_each_statement_type_via_picker() -> None:
  """The picker returns one Network per statement_type; element rows for
  each picked Network land in the element→structure dict, and one ULID
  is minted per picked structure_id."""
  session = MagicMock()

  picked = {
    "balance_sheet": RenderNetwork("struct_bs", "BS", "roll_up"),
    "income_statement": RenderNetwork("struct_is", "IS", "arithmetic"),
    "cash_flow_statement": RenderNetwork("struct_cf", "CF", "arithmetic"),
    "equity_statement": RenderNetwork("struct_se", "SE", "roll_forward"),
  }

  def fake_picker(_session, _style_id, stmt_type):
    return picked[stmt_type]

  # Associations query: 1 row per (structure_id, element_id) — one
  # element per picked Network keeps the test legible.
  element_rows = [
    MagicMock(structure_id="struct_bs", element_id="elem_cash"),
    MagicMock(structure_id="struct_is", element_id="elem_rev"),
    MagicMock(structure_id="struct_cf", element_id="elem_op_cash"),
    MagicMock(structure_id="struct_se", element_id="elem_re"),
  ]
  session.execute.return_value.fetchall.return_value = element_rows

  with patch(_PICKER_PATH, side_effect=fake_picker):
    elem_map, fs_map = _build_structure_mapping(session, "025f5d48-style")

  # element_to_structures is multi-valued — an element can appear in
  # multiple structures (e.g. NetIncomeLoss is on IS as a bottom-line
  # and on CF as a calc child).
  assert elem_map == {
    "elem_cash": ["struct_bs"],
    "elem_rev": ["struct_is"],
    "elem_op_cash": ["struct_cf"],
    "elem_re": ["struct_se"],
  }
  assert set(fs_map.keys()) == {"struct_bs", "struct_is", "struct_cf", "struct_se"}
  assert all(v.startswith("fs_") for v in fs_map.values())
  # Every picked structure gets a distinct fact_set_id.
  assert len(set(fs_map.values())) == 4


def test_build_structure_mapping_skips_uncomposed_statement_types() -> None:
  """A Reporting Style that doesn't compose every statement type (e.g.,
  a future vertical style with no equity Network) skips the missing types
  silently — the picker raises ``NoNetworkForStatementTypeError`` which
  the caller catches."""
  session = MagicMock()

  picked = {
    "balance_sheet": RenderNetwork("struct_bs", "BS", "roll_up"),
    "income_statement": RenderNetwork("struct_is", "IS", "arithmetic"),
  }

  def fake_picker(_session, _style_id, stmt_type):
    if stmt_type not in picked:
      raise NoNetworkForStatementTypeError("style_x", stmt_type)
    return picked[stmt_type]

  element_rows = [
    MagicMock(structure_id="struct_bs", element_id="elem_cash"),
    MagicMock(structure_id="struct_is", element_id="elem_rev"),
  ]
  session.execute.return_value.fetchall.return_value = element_rows

  with patch(_PICKER_PATH, side_effect=fake_picker):
    elem_map, fs_map = _build_structure_mapping(session, "style_x")

  assert set(fs_map.keys()) == {"struct_bs", "struct_is"}
  assert "struct_cf" not in fs_map
  assert "struct_se" not in fs_map


def test_build_structure_mapping_returns_empty_when_no_compositions() -> None:
  """A Reporting Style with no compositions at all returns empty dicts
  (downstream FactSet creation is a no-op)."""
  session = MagicMock()

  def fake_picker(_session, _style_id, stmt_type):
    raise NoNetworkForStatementTypeError("empty_style", stmt_type)

  with patch(_PICKER_PATH, side_effect=fake_picker):
    elem_map, fs_map = _build_structure_mapping(session, "empty_style")

  assert elem_map == {}
  assert fs_map == {}
  # No associations query should fire when no Networks were picked.
  session.execute.assert_not_called()


def test_persist_report_facts_stamps_structure_and_fact_set() -> None:
  session = MagicMock()
  facts = _FakeFacts(
    [
      _FakeFact(
        "elem_rev",
        100.0,
        date(2026, 1, 1),
        date(2026, 3, 31),
      ),
      _FakeFact(
        "elem_cash",
        50.0,
        date(2026, 3, 31),
        date(2026, 3, 31),
        "instant",
      ),
      _FakeFact(
        "elem_unmapped",
        1.0,
        date(2026, 1, 1),
        date(2026, 3, 31),
      ),
    ]
  )
  elem_map = {"elem_rev": ["struct_is"], "elem_cash": ["struct_bs"]}
  fs_map = {"struct_is": "fs_IS", "struct_bs": "fs_BS"}

  _persist_report_facts(
    session,
    "rep_01",
    facts,
    "ent_01",
    elem_map,
    fs_map,
  )

  added_facts = [c[0][0] for c in session.add.call_args_list]
  # Unmapped fact is skipped — facts.fact_set_id is NOT NULL so a fact
  # with no Network can't be persisted.
  assert len(added_facts) == 2

  rev = next(f for f in added_facts if f.element_id == "elem_rev")
  cash = next(f for f in added_facts if f.element_id == "elem_cash")

  assert rev.structure_id == "struct_is"
  assert rev.fact_set_id == "fs_IS"
  assert rev.entity_id == "ent_01"

  assert cash.structure_id == "struct_bs"
  assert cash.fact_set_id == "fs_BS"


class _FakePeriod:
  """Stand-in for ``FactPeriodSpec`` — only ``start``/``end`` matter here."""

  def __init__(self, start: date, end: date) -> None:
    self.start = start
    self.end = end


def test_pre_create_report_fact_sets_one_row_per_picked_structure() -> None:
  """One FactSet per picked Network — even Networks that will receive no
  facts (e.g., the demo's CF Network when no cash-flow CoA mappings
  exist). Period envelope comes from the report's ``periods``, not from
  facts that haven't been stamped yet."""
  session = MagicMock()
  periods = [
    _FakePeriod(date(2025, 10, 1), date(2025, 12, 31)),
    _FakePeriod(date(2026, 1, 1), date(2026, 3, 31)),
  ]
  fs_map = {
    "struct_bs": "fs_BS",
    "struct_is": "fs_IS",
    "struct_cf": "fs_CF",
    "struct_se": "fs_SE",
  }

  _pre_create_report_fact_sets(
    session, "rep_01", "ent_01", "usr_test", periods, fs_map, "map_01"
  )

  added = [c[0][0] for c in session.add.call_args_list]
  # Every picked Network gets a row — including ones with no facts.
  assert len(added) == 4
  by_structure = {fs.structure_id: fs for fs in added}
  assert set(by_structure.keys()) == {
    "struct_bs",
    "struct_is",
    "struct_cf",
    "struct_se",
  }
  is_row = by_structure["struct_is"]
  assert is_row.id == "fs_IS"
  assert is_row.factset_type == "report"
  assert is_row.entity_id == "ent_01"
  assert is_row.report_id == "rep_01"
  assert is_row.created_by == "usr_test"
  # Envelope spans the full period range from `periods`.
  assert is_row.period_start == date(2025, 10, 1)
  assert is_row.period_end == date(2026, 3, 31)
  # Report facts are pivoted from the posted ledger via the mapping.
  assert is_row.provenance == {
    "origin": "pivot",
    "mapping_id": "map_01",
    "period": "2025-10-01/2026-03-31",
    "arc_type": None,
    "posting_filter": None,
  }


def test_pre_create_report_fact_sets_noop_on_empty_inputs() -> None:
  """No picked Networks → no FactSet rows. No periods → no rows
  either (we'd have nothing to envelope)."""
  session = MagicMock()
  _pre_create_report_fact_sets(
    session, "rep_01", "ent_01", "usr_test", [], {"struct_x": "fs_x"}, "map_01"
  )
  assert session.add.call_count == 0

  session2 = MagicMock()
  _pre_create_report_fact_sets(
    session2,
    "rep_01",
    "ent_01",
    "usr_test",
    [_FakePeriod(date(2026, 1, 1), date(2026, 3, 31))],
    {},
    "map_01",
  )
  assert session2.add.call_count == 0


def test_pre_create_report_fact_sets_instant_period_key_no_leading_slash() -> None:
  """An instant-only envelope (no period_start) stamps a single-date
  provenance period key, not a leading-slash ``/end`` (the duration form
  uses ``start/end``)."""
  session = MagicMock()
  _pre_create_report_fact_sets(
    session,
    "rep_01",
    "ent_01",
    "usr_test",
    [_FakePeriod(None, date(2026, 3, 31))],
    {"struct_bs": "fs_BS"},
    "map_01",
  )
  fs = session.add.call_args_list[0][0][0]
  assert fs.provenance["period"] == "2026-03-31"


def test_share_to_target_stamps_asserted_provenance() -> None:
  """The cross-graph share producer stamps `asserted` provenance that
  back-references the source graph + report (the third producer in the
  capture triangle)."""
  graph = MagicMock()
  graph.schema_extensions = ["roboledger"]
  platform_session = MagicMock()
  platform_session.execute.return_value.scalar_one_or_none.return_value = graph
  platform_cm = MagicMock()
  platform_cm.__enter__.return_value = platform_session

  target_session = MagicMock()
  ext_cm = MagicMock()
  ext_cm.__enter__.return_value = target_session

  captured: dict = {}

  def _capture(_session, **kwargs):
    captured.update(kwargs)
    fs = MagicMock()
    fs.id = "fs_shared"
    return fs

  source_facts = [
    {
      "element_id": "e1",
      "value": 1.0,
      "period_start": date(2026, 1, 1),
      "period_end": date(2026, 3, 31),
      "period_type": "duration",
      "unit": "USD",
      "entity_id": "ent_src",
    }
  ]
  report_snapshot = {
    "id": "rpt_src",
    "name": "Q1 Report",
    "taxonomy_id": "tax_1",
    "period_type": "annual",
    "comparative": False,
  }

  from robosystems.operations.roboledger.commands import reports as reports_mod

  with (
    patch("robosystems.db.platform.SessionFactory", return_value=platform_cm),
    patch("robosystems.db.extensions.extensions_session", return_value=ext_cm),
    patch.object(reports_mod, "create_fact_set", side_effect=_capture),
    patch.object(reports_mod, "_ensure_linked_entity"),
  ):
    result = _share_to_target(
      source_graph_id="kg_src",
      report_snapshot=report_snapshot,
      source_facts=source_facts,
      target_graph_id="kg_tgt",
      shared_by="usr_share",
    )

  assert result.status == "shared"
  prov = captured["provenance"]
  assert isinstance(prov, AssertedProvenance)
  assert prov.origin == "asserted"
  assert prov.source_system == "cross_graph_share"
  assert "source_graph=kg_src" in prov.basis_note
  assert "source_report=rpt_src" in prov.basis_note


# ── regenerate_report filing-status gate ──────────────────────────────────


class _FakeReport:
  """Stand-in for a `Report` row — only the fields the gate inspects."""

  def __init__(
    self,
    report_id: str,
    *,
    created_by: str = "usr_test",
    filing_status: str = "draft",
  ) -> None:
    self.id = report_id
    self.created_by = created_by
    self.filing_status = filing_status


def _session_with_report(report: _FakeReport | None) -> MagicMock:
  session = MagicMock()
  session.get.return_value = report
  return session


def test_regenerate_report_raises_not_found_when_missing() -> None:
  session = _session_with_report(None)
  body = MagicMock()
  with pytest.raises(ReportNotFoundError):
    regenerate_report(session, "kg_demo", "rpt_missing", body, "usr_test")


def test_regenerate_report_raises_not_authorized_when_owner_mismatch() -> None:
  report = _FakeReport("rpt_01", created_by="usr_owner", filing_status="draft")
  session = _session_with_report(report)
  body = MagicMock()
  with pytest.raises(NotAuthorizedError):
    regenerate_report(session, "kg_demo", "rpt_01", body, "usr_other")


def test_regenerate_report_raises_on_filed_report() -> None:
  """Filed Reports are immutable — regenerate would silently mutate
  stamped facts + published bundle. Restate instead."""
  report = _FakeReport("rpt_01", created_by="usr_test", filing_status="filed")
  session = _session_with_report(report)
  body = MagicMock()
  with pytest.raises(InvalidFilingTransitionError) as exc:
    regenerate_report(session, "kg_demo", "rpt_01", body, "usr_test")
  assert "filed" in str(exc.value)
  assert "supersedes_id" in str(exc.value)


def test_regenerate_report_raises_on_archived_report() -> None:
  """Archived Reports were filed-then-archived — the bundle is still
  the audit-of-record. Regenerate would orphan downstream references."""
  report = _FakeReport("rpt_01", created_by="usr_test", filing_status="archived")
  session = _session_with_report(report)
  body = MagicMock()
  with pytest.raises(InvalidFilingTransitionError):
    regenerate_report(session, "kg_demo", "rpt_01", body, "usr_test")


def test_regenerate_report_allows_draft() -> None:
  """``draft`` is the pre-publish state — regeneration is free; the
  gate must not bite. The full re-run path requires reporting-style /
  fact-grid / S3 wiring not modelled here, so we assert the
  gate-check passes by letting the function blow up *after* it on a
  missing collaborator and matching that signature instead."""
  report = _FakeReport("rpt_01", created_by="usr_test", filing_status="draft")
  session = _session_with_report(report)
  body = MagicMock()
  # ``periods`` attribute access on the MagicMock body succeeds, but
  # downstream `load_entity_reporting_style` / `generate_report_facts`
  # require a real extensions schema. Assert the gate passed by asserting
  # the failure is NOT the gate error.
  with pytest.raises(Exception) as exc:
    regenerate_report(session, "kg_demo", "rpt_01", body, "usr_test")
  assert not isinstance(exc.value, InvalidFilingTransitionError)
  assert not isinstance(exc.value, ReportNotFoundError)
  assert not isinstance(exc.value, NotAuthorizedError)
