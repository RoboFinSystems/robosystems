"""Tests for report command helpers — FactSet wiring."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

from robosystems.operations.roboledger.commands.reports import (
  _build_structure_mapping,
  _persist_report_facts,
  _pre_create_report_fact_sets,
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
  Banking with no equity Network) skips the missing types silently —
  the picker raises ``NoNetworkForStatementTypeError`` which the caller
  catches."""
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
  # Unmapped fact is skipped — facts.fact_set_id is NOT NULL post §3.5 so
  # a fact with no Network can't be persisted.
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

  _pre_create_report_fact_sets(session, "rep_01", "ent_01", "usr_test", periods, fs_map)

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


def test_pre_create_report_fact_sets_noop_on_empty_inputs() -> None:
  """No picked Networks → no FactSet rows. No periods → no rows
  either (we'd have nothing to envelope)."""
  session = MagicMock()
  _pre_create_report_fact_sets(
    session, "rep_01", "ent_01", "usr_test", [], {"struct_x": "fs_x"}
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
  )
  assert session2.add.call_count == 0
