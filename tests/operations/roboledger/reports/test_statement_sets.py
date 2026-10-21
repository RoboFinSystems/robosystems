"""Unit tests for canonical statement-set stamping (the close-time pivot).

Covers the two-zone failure contract of ``stamp_canonical_statement_sets``
(soft-skip when reporting isn't set up; ``StatementStampError`` when the
pivot fails on a configured tenant), the replace-delete + minted-set
shape, non-fatal verification, and ``retract_canonical_statement_sets``
(the reopen path: VerificationResult sweep + window-scoped delete).
"""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.extensions.roboledger.fact_set import FactSet
from robosystems.operations.roboledger.reports.statement_sets import (
  StatementStampError,
  _stamp_facts_into_sets,
  retract_canonical_statement_sets,
  stamp_canonical_statement_sets,
  to_cents_precision,
)

GRAPH_ID = "kg01234567890abcdef"
PS = date(2026, 1, 1)
PE = date(2026, 1, 31)
PRIOR_PS = date(2025, 12, 1)
PRIOR_PE = date(2025, 12, 31)

_MOD = "robosystems.operations.roboledger.reports.statement_sets"


def _row(**attrs):
  row = MagicMock()
  for k, v in attrs.items():
    setattr(row, k, v)
  return row


def _exec_fetchone(row):
  m = MagicMock()
  m.fetchone.return_value = row
  return m


def _exec_scalars(values):
  m = MagicMock()
  m.scalars.return_value.all.return_value = values
  return m


def _session(mapping, execute_side_effects):
  """MagicMock session: the book mapping ``_stamp`` resolves; ordered SQL
  results via ``session.execute.side_effect``."""
  session = MagicMock()
  session.book_mapping = mapping
  session.execute.side_effect = execute_side_effects
  return session


def _fake_facts():
  return SimpleNamespace(
    facts=[
      SimpleNamespace(
        element_id="el_cash",
        value=100.0,
        period_start=None,
        period_end=PE,
        period_type="instant",
        source_values=None,
        source_shares=None,
        origin="pivot",
        derivation=None,
      ),
      SimpleNamespace(
        element_id="el_rev",
        value=50.0,
        period_start=PS,
        period_end=PE,
        period_type="duration",
        source_values=None,
        source_shares=None,
        origin="pivot",
        derivation=None,
      ),
      # Prior-month pivot output — the indirect-CF delta basis. Must be
      # consumed by the derivation, never stamped into the close month.
      SimpleNamespace(
        element_id="el_cash",
        value=40.0,
        period_start=None,
        period_end=PRIOR_PE,
        period_type="instant",
        source_values=None,
        source_shares=None,
        origin="pivot",
        derivation=None,
      ),
    ]
  )


def _stamp(session, **patches):
  """Run the stamper with the pivot collaborators patched."""
  defaults = {
    "load_entity_reporting_style": MagicMock(return_value="style_1"),
    "_build_structure_mapping": MagicMock(
      return_value=(
        {"el_cash": ["struct_bs"], "el_rev": ["struct_is"]},
        {"struct_bs": "fs_bs", "struct_is": "fs_is"},
      )
    ),
    "load_close_target_concept": MagicMock(return_value="rs-gaap:RE"),
    "generate_report_facts": MagicMock(return_value=_fake_facts()),
    "_evaluate_report_structures": MagicMock(
      return_value={"pass": 2, "fail": 0, "error": 0, "skipped": 0}
    ),
    "_group_parent_of": MagicMock(return_value=None),
    "consolidated_report_facts": MagicMock(return_value=_fake_facts()),
  }
  defaults.update(patches)
  with (
    patch(f"{_MOD}._group_parent_of", defaults["_group_parent_of"]),
    patch(f"{_MOD}.consolidated_report_facts", defaults["consolidated_report_facts"]),
    patch(f"{_MOD}.find_entity_mapping", MagicMock(return_value=session.book_mapping)),
    patch(
      f"{_MOD}.load_entity_reporting_style", defaults["load_entity_reporting_style"]
    ),
    patch(f"{_MOD}._build_structure_mapping", defaults["_build_structure_mapping"]),
    patch(f"{_MOD}.load_close_target_concept", defaults["load_close_target_concept"]),
    patch(f"{_MOD}.generate_report_facts", defaults["generate_report_facts"]),
    patch(
      f"{_MOD}._evaluate_report_structures", defaults["_evaluate_report_structures"]
    ),
  ):
    return stamp_canonical_statement_sets(
      session,
      graph_id=GRAPH_ID,
      period_start=PS,
      period_end=PE,
      actor_id="usr_close",
    )


# ────────────────────────────────────────────────────────────────────────────
# Soft-skips — reporting not set up, close proceeds without sets
# ────────────────────────────────────────────────────────────────────────────


class TestSoftSkips:
  def test_no_coa_mapping(self):
    session = _session(
      mapping=None,
      execute_side_effects=[_exec_fetchone(_row(id="ent_1"))],  # resolve_entity_id
    )
    result = _stamp(session)
    assert result.stamped is False
    assert result.note == "no_coa_mapping"
    assert result.fact_set_ids == {}
    session.add.assert_not_called()

  def test_no_entity(self):
    session = _session(
      mapping=_row(id="map_1"),
      execute_side_effects=[_exec_fetchone(None)],  # resolve_entity_id
    )
    result = _stamp(session)
    assert result.stamped is False
    assert result.note == "no_entity"

  def test_no_statement_structures(self):
    session = _session(
      mapping=_row(id="map_1"),
      execute_side_effects=[_exec_fetchone(_row(id="ent_1"))],
    )
    result = _stamp(session, _build_structure_mapping=MagicMock(return_value=({}, {})))
    assert result.stamped is False
    assert result.note == "no_statement_structures"

  def test_no_taxonomy(self):
    session = _session(
      mapping=_row(id="map_1"),
      execute_side_effects=[
        _exec_fetchone(_row(id="ent_1")),  # resolve_entity_id
        _exec_fetchone(None),  # rs-gaap taxonomy lookup
      ],
    )
    result = _stamp(session)
    assert result.stamped is False
    assert result.note == "no_taxonomy"


# ────────────────────────────────────────────────────────────────────────────
# Happy path — minted-set shape + verification riding the result
# ────────────────────────────────────────────────────────────────────────────


class TestStampHappyPath:
  def _happy_session(self, existing_canonical_ids=()):
    effects = [
      _exec_fetchone(_row(id="ent_1")),  # resolve_entity_id
      _exec_fetchone(_row(id="tax_1")),  # rs-gaap taxonomy
      _exec_scalars(list(existing_canonical_ids)),  # retract: window select
    ]
    if existing_canonical_ids:
      effects.append(MagicMock())  # fact_sets retract
    return _session(mapping=_row(id="map_1"), execute_side_effects=effects)

  def _added_fact_sets(self, session) -> list[FactSet]:
    return [
      call.args[0]
      for call in session.add.call_args_list
      if isinstance(call.args[0], FactSet)
    ]

  def test_the_consolidation_entity_stamps_the_group(self):
    """The group close's stamp is the consolidated statements, on the
    parent's chart and Style, kept as the consolidation entity's sets."""
    session = self._happy_session()
    consolidated = MagicMock(return_value=_fake_facts())
    own = MagicMock(return_value=_fake_facts())
    style = MagicMock(return_value="style_parent")
    result = _stamp(
      session,
      _group_parent_of=MagicMock(return_value="ent_parent"),
      consolidated_report_facts=consolidated,
      generate_report_facts=own,
      load_entity_reporting_style=style,
    )

    assert result.stamped is True
    own.assert_not_called()
    assert consolidated.call_args.kwargs["parent_id"] == "ent_parent"
    style.assert_called_once_with(session, "ent_parent")
    assert {fs.entity_id for fs in self._added_fact_sets(session)} == {"ent_1"}

  def test_mints_canonical_sets_with_pivot_provenance(self):
    session = self._happy_session()
    result = _stamp(session)

    assert result.stamped is True
    assert result.note is None
    assert result.fact_set_ids == {"struct_bs": "fs_bs", "struct_is": "fs_is"}
    assert result.rule_summary == {"pass": 2, "fail": 0, "error": 0, "skipped": 0}

    fact_sets = self._added_fact_sets(session)
    assert len(fact_sets) == 2
    for fs in fact_sets:
      assert fs.report_id is None
      assert fs.scenario_id is None
      assert fs.factset_type == "report"
      assert fs.period_start == PS
      assert fs.period_end == PE
      assert fs.provenance["origin"] == "pivot"
      assert fs.provenance["mapping_id"] == "map_1"
      assert fs.provenance["period"] == f"{PS}/{PE}"

  def test_replace_retracts_prior_canonical_sets(self):
    """The month's earlier sets stay as it was closed before, retracted and
    read by nothing current; their verification results stay with them."""
    session = self._happy_session(existing_canonical_ids=["fs_old_bs", "fs_old_is"])
    result = _stamp(session)
    assert result.stamped is True

    calls = session.execute.call_args_list
    # 3rd call selected the window's canonical ids; its SQL carries the
    # ownership predicates that protect every other producer's sets.
    window_sql = str(calls[2].args[0])
    assert "factset_type = 'report'" in window_sql
    assert "report_id IS NULL" in window_sql
    assert "scenario_id IS NULL" in window_sql
    assert "retracted_at IS NULL" in window_sql
    assert "period_start = :ps" in window_sql
    assert "period_end = :pe" in window_sql
    # 4th: the retract itself — an update, never a delete.
    retract_sql = str(calls[3].args[0])
    assert "UPDATE fact_sets SET retracted_at" in retract_sql
    assert calls[3].args[1]["ids"] == ["fs_old_bs", "fs_old_is"]
    assert not any("DELETE" in str(c.args[0]) for c in calls)

  def test_verification_failure_is_non_fatal(self):
    session = self._happy_session()
    result = _stamp(
      session,
      _evaluate_report_structures=MagicMock(side_effect=RuntimeError("engine down")),
    )
    assert result.stamped is True
    assert result.rule_summary is None

  def test_the_close_month_is_pivoted_alone(self):
    """A single column derives its own cash flow from its opening balances,
    so no prior month rides the pivot as a delta basis."""
    session = self._happy_session()
    gen = MagicMock(return_value=_fake_facts())
    _stamp(session, generate_report_facts=gen)
    periods = gen.call_args.kwargs["periods"]
    assert [(p.start, p.end, p.label) for p in periods] == [(PS, PE, "2026-01")]


# ────────────────────────────────────────────────────────────────────────────
# Hard fail — reporting configured, pivot raised
# ────────────────────────────────────────────────────────────────────────────


class TestStampHardFail:
  def test_pivot_exception_wraps_in_statement_stamp_error(self):
    session = _session(
      mapping=_row(id="map_1"),
      execute_side_effects=[
        _exec_fetchone(_row(id="ent_1")),
        _exec_fetchone(_row(id="tax_1")),
      ],
    )
    with pytest.raises(StatementStampError) as exc_info:
      _stamp(
        session,
        generate_report_facts=MagicMock(side_effect=ValueError("bad mapping arc")),
      )
    assert "2026-01" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, ValueError)


# ────────────────────────────────────────────────────────────────────────────
# Retraction — the reopen path
# ────────────────────────────────────────────────────────────────────────────


class TestRetractCanonicalSets:
  def test_empty_window_is_a_noop(self):
    session = MagicMock()
    session.execute.side_effect = [_exec_fetchone(_row(id="ent_1")), _exec_scalars([])]
    retracted = retract_canonical_statement_sets(
      session, period_start=PS, period_end=PE
    )
    assert retracted == []
    assert session.execute.call_count == 2
    session.flush.assert_not_called()

  def test_no_entity_retracts_nothing(self):
    session = MagicMock()
    session.execute.side_effect = [_exec_fetchone(None)]
    retracted = retract_canonical_statement_sets(
      session, period_start=PS, period_end=PE
    )
    assert retracted == []
    assert session.execute.call_count == 1

  def test_retracts_the_sets_and_keeps_their_results(self):
    session = MagicMock()
    session.execute.side_effect = [
      _exec_fetchone(_row(id="ent_1")),  # resolve_entity_id
      _exec_scalars(["fs_a", "fs_b"]),
      MagicMock(),  # fact_sets retract
    ]
    retracted = retract_canonical_statement_sets(
      session, period_start=PS, period_end=PE
    )
    assert retracted == ["fs_a", "fs_b"]

    calls = session.execute.call_args_list
    assert len(calls) == 3
    retract_sql = str(calls[2].args[0])
    assert "UPDATE fact_sets SET retracted_at = :now" in retract_sql
    assert calls[2].args[1]["ids"] == ["fs_a", "fs_b"]
    assert calls[2].args[1]["now"] is not None
    session.flush.assert_called_once()

  def test_window_predicates_protect_other_producers_and_entities(self):
    """The window select must exclude publication snapshots
    (report_id NOT NULL), scenario months, non-'report' set types, and every
    other entity's sets."""
    session = MagicMock()
    session.execute.side_effect = [_exec_fetchone(_row(id="ent_1")), _exec_scalars([])]
    retract_canonical_statement_sets(session, period_start=PS, period_end=PE)
    window = session.execute.call_args_list[1]
    sql = str(window.args[0])
    assert "factset_type = 'report'" in sql
    assert "report_id IS NULL" in sql
    assert "retracted_at IS NULL" in sql
    assert "scenario_id IS NULL" in sql
    assert "entity_id = :eid" in sql
    assert window.args[1]["eid"] == "ent_1"


class TestStampRoundsToCents:
  """Every value stamped through ``_stamp_facts_into_sets`` is dollars derived
  from integer cents, so it persists at cents precision — the float tail a
  subtotal picks up in the pivot (``52585 + 5400.02`` → ``57985.020000000004``)
  never reaches ``facts.value`` and so never reaches a published file."""

  def _stamp_one(self, value: float) -> float:
    session = MagicMock()
    fact = SimpleNamespace(
      element_id="el_assets",
      value=value,
      period_start=date(2025, 12, 31),
      period_end=date(2025, 12, 31),
      period_type="instant",
      source_values=None,
      source_shares=None,
      origin="pivot",
      derivation=None,
    )
    _stamp_facts_into_sets(
      session,
      SimpleNamespace(facts=[fact]),
      "ent_1",
      {"el_assets": ["struct_bs"]},
      {"struct_bs": "fs_bs"},
    )
    (row,) = [call.args[0] for call in session.add.call_args_list]
    assert row.unit == "USD"
    # The value is rounded to cents, and says so: the graph and every export
    # read this rather than claiming exact or hundreds.
    assert row.decimals == "2"
    return row.value

  def test_a_float_summed_subtotal_persists_as_cents(self):
    assert self._stamp_one(52585 + 5400.02) == 57985.02
    assert self._stamp_one(0.1 + 0.2) == 0.3

  def test_negative_and_whole_amounts_are_untouched(self):
    assert self._stamp_one(-7385.02) == -7385.02
    assert self._stamp_one(148_000_000.0) == 148_000_000.0

  def test_helper_is_the_ledgers_precision(self):
    assert to_cents_precision(6085.000000000004) == 6085.0
    assert to_cents_precision(1.005) in (1.0, 1.01)  # binary rounding, never a tail


class TestStampOneFactPerContext:
  """The cash-flow plug lands on the operating leaf the ΔBS derivation may
  already have filled for the same period. Readers sum the pair, but two
  values in one context is an inconsistent duplicate in every export, so the
  stamp persists their sum as one fact."""

  FY_START = date(2025, 1, 1)
  FY_END = date(2025, 12, 31)

  def _fact(self, element_id: str, value: float, start: date | None = None):
    return SimpleNamespace(
      element_id=element_id,
      value=value,
      period_start=start or self.FY_START,
      period_end=self.FY_END,
      period_type="duration",
      source_values=None,
      source_shares=None,
      origin="pivot",
      derivation=None,
    )

  def _stamp(self, facts) -> list:
    session = MagicMock()
    _stamp_facts_into_sets(
      session,
      SimpleNamespace(facts=facts),
      "ent_1",
      {"el_other_wc": ["struct_cf"], "el_ni": ["struct_cf"]},
      {"struct_cf": "fs_cf"},
    )
    return [call.args[0] for call in session.add.call_args_list]

  def test_derived_value_and_plug_persist_as_their_sum(self):
    rows = self._stamp(
      [
        self._fact("el_ni", 10_612.03),
        self._fact("el_other_wc", 2_601.64),
        self._fact("el_other_wc", -34_745.08),
      ]
    )
    other = [r for r in rows if r.element_id == "el_other_wc"]
    assert len(other) == 1
    assert other[0].value == -32_143.44
    assert len(rows) == 2

  def test_the_sum_is_rounded_once_after_adding(self):
    rows = self._stamp([self._fact("el_other_wc", 0.1), self._fact("el_other_wc", 0.2)])
    assert [r.value for r in rows] == [0.3]

  def test_different_periods_on_one_element_stay_separate(self):
    rows = self._stamp(
      [
        self._fact("el_other_wc", 5.0),
        self._fact("el_other_wc", 7.0, start=date(2025, 10, 1)),
      ]
    )
    assert sorted(r.value for r in rows) == [5.0, 7.0]


# ────────────────────────────────────────────────────────────────────────────
# The fact's own descriptor and the column freeze (fact-lineage §9.6)
# ────────────────────────────────────────────────────────────────────────────


class TestPerFactDescriptor:
  """Every row ``_stamp_facts_into_sets`` writes says how it was made, when
  it can say so honestly: a pivot whose accounts foot to it carries them;
  anything else stays unknown. A row in a closed column is frozen."""

  JUNE = (date(2026, 6, 1), date(2026, 6, 30))
  JULY = (date(2026, 7, 1), date(2026, 7, 31))

  def _fact(
    self,
    element_id,
    value,
    window,
    sources=None,
    shares=None,
    origin="pivot",
    derivation=None,
  ):
    start, end = window
    return SimpleNamespace(
      element_id=element_id,
      value=value,
      period_start=start,
      period_end=end,
      period_type="duration",
      source_values=sources,
      source_shares=shares,
      origin=origin,
      derivation=derivation,
    )

  def _stamp(self, facts, **kwargs) -> list:
    session = MagicMock()
    _stamp_facts_into_sets(
      session,
      SimpleNamespace(facts=facts),
      "ent_1",
      {"el_rev": ["struct_is"], "el_equity": ["struct_bs"]},
      {"struct_is": "fs_is", "struct_bs": "fs_bs"},
      mapping_id="map_1",
      **kwargs,
    )
    return [call.args[0] for call in session.add.call_args_list]

  def test_a_pivot_that_foots_carries_its_accounts_largest_first(self):
    rows = self._stamp(
      [self._fact("el_rev", 150.0, self.JUNE, {"acct_a": 50.0, "acct_b": 100.0})]
    )
    assert len(rows) == 1
    descriptor = rows[0].provenance
    assert descriptor["origin"] == "pivot"
    assert descriptor["mapping_id"] == "map_1"
    assert descriptor["period"] == "2026-06-01/2026-06-30"
    assert [
      (s["entity_id"], s["account_id"], s["value"]) for s in descriptor["sources"]
    ] == [
      ("ent_1", "acct_b", 100.0),
      ("ent_1", "acct_a", 50.0),
    ]

  def test_a_value_a_derivation_added_into_is_unknown(self):
    """The equity leaf the retained-earnings close adds net income into no
    longer foots to its accounts, so it makes no pivot claim."""
    rows = self._stamp(
      [self._fact("el_equity", 1_250.0, self.JUNE, {"acct_capital": 1_000.0})]
    )
    assert rows[0].provenance is None

  def test_a_key_one_contributor_without_accounts_joined_is_unknown(self):
    """The cash-flow plug lands on a leaf the pivot also filled: the summed
    row was not pivoted whole."""
    rows = self._stamp(
      [
        self._fact("el_rev", 100.0, self.JUNE, {"acct_a": 100.0}),
        self._fact("el_rev", 7.0, self.JUNE, None),
      ]
    )
    assert len(rows) == 1
    assert rows[0].value == 107.0
    assert rows[0].provenance is None

  def test_a_consolidated_sum_keys_each_account_by_its_member(self):
    """The consolidation entity posts on the parent's chart: the same account
    id is two members' lines, and each keeps its own."""
    rows = self._stamp(
      [
        self._fact(
          "el_rev",
          300.0,
          self.JUNE,
          {"acct_shared": 250.0, "acct_member": 50.0},
          {
            ("ent_parent", "acct_shared"): 100.0,
            ("ent_adjustments", "acct_shared"): 150.0,
            ("ent_member", "acct_member"): 50.0,
          },
        )
      ]
    )
    assert [
      (s["entity_id"], s["account_id"], s["value"])
      for s in rows[0].provenance["sources"]
    ] == [
      ("ent_adjustments", "acct_shared", 150.0),
      ("ent_parent", "acct_shared", 100.0),
      ("ent_member", "acct_member", 50.0),
    ]

  def test_without_a_mapping_nothing_is_claimed(self):
    session = MagicMock()
    _stamp_facts_into_sets(
      session,
      SimpleNamespace(facts=[self._fact("el_rev", 1.0, self.JUNE, {"a": 1.0})]),
      "ent_1",
      {"el_rev": ["struct_is"]},
      {"struct_is": "fs_is"},
    )
    assert session.add.call_args[0][0].provenance is None

  def test_a_closed_column_is_written_frozen_and_an_open_one_live(self):
    frozen_at = datetime(2026, 7, 2, 9, 0, tzinfo=UTC)
    rows = self._stamp(
      [
        self._fact("el_rev", 100.0, self.JUNE, {"a": 100.0}),
        self._fact("el_rev", 120.0, self.JULY, {"a": 120.0}),
      ],
      frozen_at=frozen_at,
      freeze_through=self.JUNE[1],
    )
    by_end = {row.period_end: row for row in rows}
    assert by_end[self.JUNE[1]].frozen_at == frozen_at
    assert by_end[self.JULY[1]].frozen_at is None

  def test_nothing_freezes_without_a_cutoff(self):
    rows = self._stamp(
      [self._fact("el_rev", 100.0, self.JUNE, {"a": 100.0})],
      frozen_at=datetime(2026, 7, 2, tzinfo=UTC),
    )
    assert rows[0].frozen_at is None

  def test_a_rollup_names_the_concepts_it_summed(self):
    rows = self._stamp(
      [
        self._fact(
          "el_rev",
          150.0,
          self.JUNE,
          origin="rollup",
          derivation={
            "rule": "calculation",
            "children": [
              {"qname": "rs-gaap:Revenues", "weight": 1.0},
              {"qname": "rs-gaap:CostOfRevenue", "weight": -1.0},
            ],
          },
        )
      ]
    )
    descriptor = rows[0].provenance
    assert descriptor["origin"] == "rollup"
    assert descriptor["rule"] == "calculation"
    assert [(c["qname"], c["weight"]) for c in descriptor["children"]] == [
      ("rs-gaap:Revenues", 1.0),
      ("rs-gaap:CostOfRevenue", -1.0),
    ]

  def test_the_earnings_close_added_into_equity_is_derived_with_its_parts(self):
    """The equity target was pivoted from its accounts, then the close added
    net income into it: the row says both, and lists the pivoted accounts."""
    rows = self._stamp(
      [
        self._fact(
          "el_equity",
          1_250.0,
          self.JUNE,
          {"acct_capital": 1_000.0},
          origin="derived",
          derivation={
            "computations": ["close_to_retained_earnings"],
            "amounts": {"pivot": 1_000.0, "close_to_retained_earnings": 250.0},
          },
        )
      ]
    )
    descriptor = rows[0].provenance
    assert descriptor["origin"] == "derived"
    assert descriptor["computation"] == "pivot+close_to_retained_earnings"
    assert descriptor["amounts"] == {
      "pivot": 1_000.0,
      "close_to_retained_earnings": 250.0,
    }
    assert [(s["account_id"], s["value"]) for s in descriptor["sources"]] == [
      ("acct_capital", 1_000.0)
    ]

  def test_a_pivot_through_a_flow_filter_carries_the_filter_not_accounts(self):
    rows = self._stamp(
      [
        self._fact(
          "el_rev",
          90.0,
          self.JUNE,
          derivation={"posting_filter": {"flow_element": "rs-gaap:ProceedsFromDebt"}},
        )
      ]
    )
    descriptor = rows[0].provenance
    assert descriptor["origin"] == "pivot"
    assert descriptor["sources"] is None
    assert descriptor["posting_filter"] == {"flow_element": "rs-gaap:ProceedsFromDebt"}

  def test_a_derivation_summed_onto_a_pivoted_leaf_is_derived_by_parts(self):
    """The cash-flow plug lands on a leaf the pivot filled: the row names the
    pivoted part and the plug, with the pivoted accounts."""
    rows = self._stamp(
      [
        self._fact("el_rev", 100.0, self.JUNE, {"acct_a": 100.0}),
        self._fact(
          "el_rev",
          7.0,
          self.JUNE,
          origin="derived",
          derivation={"computations": ["cash_reconciliation"]},
        ),
      ]
    )
    descriptor = rows[0].provenance
    assert rows[0].value == 107.0
    assert descriptor["origin"] == "derived"
    assert descriptor["computation"] == "pivot+cash_reconciliation"
    assert descriptor["amounts"] == {"pivot": 100.0, "cash_reconciliation": 7.0}
    assert [s["account_id"] for s in descriptor["sources"]] == ["acct_a"]

  def test_a_fact_that_does_not_say_how_it_was_made_is_unknown(self):
    rows = self._stamp([self._fact("el_rev", 1.0, self.JUNE, {"a": 1.0}, origin=None)])
    assert rows[0].provenance is None

  def test_a_frozen_pivot_hashes_the_landed_lines_under_its_accounts(self):
    """One read per window; the hash is over every line id under the fact's
    accounts, sorted, and the count says how many."""
    session = MagicMock()
    session.execute.return_value.fetchall.return_value = [
      SimpleNamespace(element_id="acct_a", ids=["li_2", "li_1"]),
      SimpleNamespace(element_id="acct_b", ids=["li_3"]),
    ]
    _stamp_facts_into_sets(
      session,
      SimpleNamespace(
        facts=[
          self._fact("el_rev", 10.0, self.JUNE, {"acct_a": 6.0, "acct_b": 4.0}),
          self._fact("el_equity", 6.0, self.JUNE, {"acct_a": 6.0}),
        ]
      ),
      "ent_1",
      {"el_rev": ["struct_is"], "el_equity": ["struct_bs"]},
      {"struct_is": "fs_is", "struct_bs": "fs_bs"},
      mapping_id="map_1",
      frozen_at=datetime(2026, 7, 2, tzinfo=UTC),
      freeze_through=self.JUNE[1],
    )
    rows = [call.args[0] for call in session.add.call_args_list]
    by_element = {row.element_id: row.provenance for row in rows}
    expected = "sha256:" + hashlib.sha256(b"li_1\nli_2\nli_3").hexdigest()
    assert by_element["el_rev"]["content_hash"] == expected
    assert by_element["el_rev"]["line_count"] == 3
    assert by_element["el_equity"]["line_count"] == 2
    # The window was read once for both facts.
    assert session.execute.call_count == 1
    params = session.execute.call_args.args[1]
    assert (params["entity_id"], params["start_date"], params["end_date"]) == (
      "ent_1",
      self.JUNE[0],
      self.JUNE[1],
    )

  def test_a_live_pivot_carries_no_hash(self):
    rows = self._stamp([self._fact("el_rev", 1.0, self.JUNE, {"a": 1.0})])
    assert rows[0].provenance["content_hash"] is None
    assert rows[0].provenance["line_count"] is None
