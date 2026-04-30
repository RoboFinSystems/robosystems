"""Tests for report command helpers — FactSet wiring."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

from robosystems.operations.roboledger.commands.reports import (
  _build_structure_mapping,
  _create_report_fact_sets,
  _persist_report_facts,
  _select_canonical_render_targets,
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


def test_build_structure_mapping_filters_report_eligible_types() -> None:
  """The SQL query must restrict to statement/metric types and route
  facts to one canonical structure per block_type."""
  session = MagicMock()
  rows = [
    MagicMock(
      structure_id="struct_is",
      structure_type="income_statement",
      element_id="elem_rev",
    ),
    MagicMock(
      structure_id="struct_is",
      structure_type="income_statement",
      element_id="elem_cogs",
    ),
    MagicMock(
      structure_id="struct_bs",
      structure_type="balance_sheet",
      element_id="elem_cash",
    ),
  ]
  session.execute.return_value.fetchall.return_value = rows

  elem_map, fs_map = _build_structure_mapping(session, "tax_01")

  assert elem_map == {
    "elem_rev": "struct_is",
    "elem_cogs": "struct_is",
    "elem_cash": "struct_bs",
  }
  # One ULID per canonical structure
  assert set(fs_map.keys()) == {"struct_is", "struct_bs"}
  assert all(v.startswith("fs_") for v in fs_map.values())
  assert fs_map["struct_is"] != fs_map["struct_bs"]

  # Verify the query restricts to the expected types
  call_sql = str(session.execute.call_args[0][0])
  assert "income_statement" in call_sql
  assert "balance_sheet" in call_sql
  assert "cash_flow_statement" in call_sql
  assert "equity_statement" in call_sql
  assert "metric" in call_sql
  assert "schedule" not in call_sql


def test_select_canonical_render_targets_picks_richest_when_no_mapping() -> None:
  """Without a CoA mapping, falls back to the richest structure per type
  (most elements). Deterministic id sort breaks remaining ties."""
  rows = [
    # IS Single-step — 2 elements
    MagicMock(
      structure_id="struct_is_single",
      structure_type="income_statement",
      element_id="elem_rev",
    ),
    MagicMock(
      structure_id="struct_is_single",
      structure_type="income_statement",
      element_id="elem_costs",
    ),
    # IS Multi-step — 5 elements (richer)
    MagicMock(
      structure_id="struct_is_multi",
      structure_type="income_statement",
      element_id="elem_rev",
    ),
    MagicMock(
      structure_id="struct_is_multi",
      structure_type="income_statement",
      element_id="elem_cogs",
    ),
    MagicMock(
      structure_id="struct_is_multi",
      structure_type="income_statement",
      element_id="elem_gp",
    ),
    MagicMock(
      structure_id="struct_is_multi",
      structure_type="income_statement",
      element_id="elem_opex",
    ),
    MagicMock(
      structure_id="struct_is_multi",
      structure_type="income_statement",
      element_id="elem_oi",
    ),
  ]
  session = MagicMock()
  canonical = _select_canonical_render_targets(session, rows, mapping_id=None)
  assert canonical == {"income_statement": "struct_is_multi"}


def test_select_canonical_render_targets_picks_by_coa_coverage() -> None:
  """When a CoA mapping is provided, the structure whose elements best
  cover the mapping's targets wins — even if it has fewer total elements
  than a richer alternative."""
  rows = [
    # Single-step has just Revenues + CostsAndExpenses (2 elements)
    MagicMock(
      structure_id="struct_is_single",
      structure_type="income_statement",
      element_id="fac:Revenues",
    ),
    MagicMock(
      structure_id="struct_is_single",
      structure_type="income_statement",
      element_id="fac:CostsAndExpenses",
    ),
    # Multi-step has many MORE elements but doesn't include CostsAndExpenses
    MagicMock(
      structure_id="struct_is_multi",
      structure_type="income_statement",
      element_id="fac:Revenues",
    ),
    MagicMock(
      structure_id="struct_is_multi",
      structure_type="income_statement",
      element_id="fac:CostOfRevenue",
    ),
    MagicMock(
      structure_id="struct_is_multi",
      structure_type="income_statement",
      element_id="fac:GrossProfit",
    ),
    MagicMock(
      structure_id="struct_is_multi",
      structure_type="income_statement",
      element_id="fac:OperatingExpenses",
    ),
    MagicMock(
      structure_id="struct_is_multi",
      structure_type="income_statement",
      element_id="fac:OperatingIncome",
    ),
  ]

  # Mapping points the user's CoA at fac:Revenues + fac:CostsAndExpenses —
  # exactly what Single-step covers and Multi-step does not.
  session = MagicMock()
  mapping_targets = [
    MagicMock(element_id="fac:Revenues"),
    MagicMock(element_id="fac:CostsAndExpenses"),
  ]
  session.execute.return_value.fetchall.return_value = mapping_targets

  canonical = _select_canonical_render_targets(session, rows, mapping_id="map_coa")
  # Single-step wins despite having FEWER elements — coverage beats size.
  assert canonical == {"income_statement": "struct_is_single"}


def test_select_canonical_render_targets_one_per_block_type() -> None:
  """A graph with multiple block_types gets exactly one canonical per type."""
  rows = [
    MagicMock(
      structure_id="struct_bs", structure_type="balance_sheet", element_id="elem_assets"
    ),
    MagicMock(
      structure_id="struct_is", structure_type="income_statement", element_id="elem_rev"
    ),
    MagicMock(
      structure_id="struct_cf",
      structure_type="cash_flow_statement",
      element_id="elem_cash",
    ),
    MagicMock(
      structure_id="struct_se", structure_type="equity_statement", element_id="elem_re"
    ),
  ]
  session = MagicMock()
  canonical = _select_canonical_render_targets(session, rows, mapping_id=None)
  assert canonical == {
    "balance_sheet": "struct_bs",
    "income_statement": "struct_is",
    "cash_flow_statement": "struct_cf",
    "equity_statement": "struct_se",
  }


def test_build_structure_mapping_filters_to_canonical_only() -> None:
  """When two IS variants both contain `fac:Revenues`, only the canonical
  one ends up in the element→structure dict — no more dict-overwrite-wins."""
  session = MagicMock()

  # Use a side_effect to return different result objects per call:
  # 1st call = the structure-and-elements query, 2nd = mapping-targets query.
  rows = [
    MagicMock(
      structure_id="struct_is_single",
      structure_type="income_statement",
      element_id="fac:Revenues",
    ),
    MagicMock(
      structure_id="struct_is_single",
      structure_type="income_statement",
      element_id="fac:CostsAndExpenses",
    ),
    MagicMock(
      structure_id="struct_is_multi",
      structure_type="income_statement",
      element_id="fac:Revenues",
    ),
    MagicMock(
      structure_id="struct_is_multi",
      structure_type="income_statement",
      element_id="fac:CostOfRevenue",
    ),
  ]
  mapping_targets = [
    MagicMock(element_id="fac:Revenues"),
    MagicMock(element_id="fac:CostsAndExpenses"),
  ]

  call_idx = {"i": 0}

  def execute_side(*_args, **_kwargs) -> MagicMock:
    result = MagicMock()
    if call_idx["i"] == 0:
      result.fetchall.return_value = rows
    else:
      result.fetchall.return_value = mapping_targets
    call_idx["i"] += 1
    return result

  session.execute.side_effect = execute_side

  elem_map, fs_map = _build_structure_mapping(session, "tax_01", mapping_id="map_coa")

  # Single-step wins on coverage; both fac:Revenues and fac:CostsAndExpenses
  # route to it. fac:CostOfRevenue is dropped because Multi-step lost.
  assert elem_map == {
    "fac:Revenues": "struct_is_single",
    "fac:CostsAndExpenses": "struct_is_single",
  }
  assert "fac:CostOfRevenue" not in elem_map
  assert set(fs_map.keys()) == {"struct_is_single"}


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
  elem_map = {"elem_rev": "struct_is", "elem_cash": "struct_bs"}
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
  assert len(added_facts) == 3

  rev = next(f for f in added_facts if f.element_id == "elem_rev")
  cash = next(f for f in added_facts if f.element_id == "elem_cash")
  unmapped = next(f for f in added_facts if f.element_id == "elem_unmapped")

  assert rev.report_id == "rep_01"
  assert rev.structure_id == "struct_is"
  assert rev.fact_set_id == "fs_IS"
  assert rev.entity_id == "ent_01"

  assert cash.structure_id == "struct_bs"
  assert cash.fact_set_id == "fs_BS"

  # Facts whose elements aren't mapped to a report-eligible structure
  # still persist (report_id satisfies the CHECK), but carry no
  # structure/fact_set linkage.
  assert unmapped.structure_id is None
  assert unmapped.fact_set_id is None


def test_create_report_fact_sets_one_row_per_structure_with_envelope() -> None:
  """One FactSet per structure that received facts, period envelope
  spans the full range of that structure's facts."""
  session = MagicMock()
  facts = _FakeFacts(
    [
      _FakeFact("elem_rev", 100.0, date(2026, 1, 1), date(2026, 3, 31)),
      _FakeFact("elem_rev", 110.0, date(2025, 10, 1), date(2025, 12, 31)),
      _FakeFact("elem_cash", 50.0, date(2026, 3, 31), date(2026, 3, 31)),
      _FakeFact("elem_unmapped", 1.0, date(2026, 1, 1), date(2026, 3, 31)),
    ]
  )
  elem_map = {"elem_rev": "struct_is", "elem_cash": "struct_bs"}
  fs_map = {"struct_is": "fs_IS", "struct_bs": "fs_BS"}

  _create_report_fact_sets(
    session,
    "rep_01",
    "ent_01",
    "usr_test",
    facts,
    elem_map,
    fs_map,
  )

  added = [c[0][0] for c in session.add.call_args_list]
  assert len(added) == 2
  by_structure = {fs.structure_id: fs for fs in added}

  is_row = by_structure["struct_is"]
  assert is_row.id == "fs_IS"
  assert is_row.factset_type == "report"
  assert is_row.entity_id == "ent_01"
  assert is_row.report_id == "rep_01"
  assert is_row.created_by == "usr_test"
  # Envelope spans both periods' extent
  assert is_row.period_start == date(2025, 10, 1)
  assert is_row.period_end == date(2026, 3, 31)

  bs_row = by_structure["struct_bs"]
  assert bs_row.period_start == date(2026, 3, 31)
  assert bs_row.period_end == date(2026, 3, 31)


def test_create_report_fact_sets_skips_structures_with_no_dated_facts() -> None:
  session = MagicMock()
  facts = _FakeFacts(
    [
      _FakeFact("elem_ghost", 0.0, period_start=None, period_end=None),
    ]
  )
  elem_map = {"elem_ghost": "struct_ghost"}
  fs_map = {"struct_ghost": "fs_ghost"}

  _create_report_fact_sets(
    session, "rep_01", "ent_01", "usr_test", facts, elem_map, fs_map
  )

  # fact_sets.period_end is NOT NULL — a structure with no dated facts
  # cannot produce a valid FactSet row.
  assert session.add.call_count == 0
