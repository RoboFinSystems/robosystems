"""Unit tests for canonical statement-set stamping (the close-time pivot).

Covers the two-zone failure contract of ``stamp_canonical_statement_sets``
(soft-skip when reporting isn't set up; ``StatementStampError`` when the
pivot fails on a configured tenant), the replace-delete + minted-set
shape, non-fatal verification, and ``retract_canonical_statement_sets``
(the reopen path: VerificationResult sweep + window-scoped delete).
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.extensions import Fact
from robosystems.models.extensions.roboledger.fact_set import FactSet
from robosystems.operations.roboledger.reports.statement_sets import (
  StatementStampError,
  retract_canonical_statement_sets,
  stamp_canonical_statement_sets,
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
  """MagicMock session: mapping query via ``session.query``; ordered SQL
  results via ``session.execute.side_effect``."""
  session = MagicMock()
  session.query.return_value.filter.return_value.order_by.return_value.first.return_value = mapping
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
      ),
      SimpleNamespace(
        element_id="el_rev",
        value=50.0,
        period_start=PS,
        period_end=PE,
        period_type="duration",
      ),
      # Prior-month pivot output — the indirect-CF delta basis. Must be
      # consumed by the derivation, never stamped into the close month.
      SimpleNamespace(
        element_id="el_cash",
        value=40.0,
        period_start=None,
        period_end=PRIOR_PE,
        period_type="instant",
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
  }
  defaults.update(patches)
  with (
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
    session = _session(mapping=None, execute_side_effects=[])
    result = _stamp(session)
    assert result.stamped is False
    assert result.note == "no_coa_mapping"
    assert result.fact_set_ids == {}
    session.add.assert_not_called()

  def test_no_entity(self):
    session = _session(
      mapping=_row(id="map_1"),
      execute_side_effects=[_exec_fetchone(None)],  # _get_entity_id
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
        _exec_fetchone(_row(id="ent_1")),  # _get_entity_id
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
      _exec_fetchone(_row(id="ent_1")),  # _get_entity_id
      _exec_fetchone(_row(id="tax_1")),  # rs-gaap taxonomy
      _exec_scalars(list(existing_canonical_ids)),  # retract: window select
    ]
    if existing_canonical_ids:
      effects.append(MagicMock())  # VerificationResult sweep
      effects.append(MagicMock())  # fact_sets delete
    return _session(mapping=_row(id="map_1"), execute_side_effects=effects)

  def _added_fact_sets(self, session) -> list[FactSet]:
    return [
      call.args[0]
      for call in session.add.call_args_list
      if isinstance(call.args[0], FactSet)
    ]

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

  def test_replace_deletes_prior_canonical_sets_and_sweeps_results(self):
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
    assert "period_start = :ps" in window_sql
    assert "period_end = :pe" in window_sql
    # 4th: VerificationResult sweep (no DB FK ties results to sets).
    sweep_sql = str(calls[3].args[0])
    assert "verification_results" in sweep_sql.lower()
    # 5th: the canonical sets themselves.
    delete_sql = str(calls[4].args[0])
    assert "DELETE FROM fact_sets" in delete_sql
    assert calls[4].args[1] == {"ids": ["fs_old_bs", "fs_old_is"]}

  def test_verification_failure_is_non_fatal(self):
    session = self._happy_session()
    result = _stamp(
      session,
      _evaluate_report_structures=MagicMock(side_effect=RuntimeError("engine down")),
    )
    assert result.stamped is True
    assert result.rule_summary is None

  def test_pivot_receives_prior_month_delta_basis(self):
    """The pivot must get [prior month, close month] — the indirect-CF
    derivation and the cash foot both no-op below two periods, which is
    the bug that stamped CF = NetIncome + DDA with zero WC deltas."""
    session = self._happy_session()
    gen = MagicMock(return_value=_fake_facts())
    _stamp(session, generate_report_facts=gen)
    periods = gen.call_args.kwargs["periods"]
    assert [(p.start, p.end) for p in periods] == [
      (PRIOR_PS, PRIOR_PE),
      (PS, PE),
    ]
    assert [p.label for p in periods] == ["2025-12", "2026-01"]

  def test_prior_month_facts_derive_but_never_stamp(self):
    """Prior-month pivot output is the delta basis only — every stamped
    Fact must carry the close month's period_end."""
    session = self._happy_session()
    result = _stamp(session)
    assert result.stamped is True
    stamped = [
      call.args[0]
      for call in session.add.call_args_list
      if isinstance(call.args[0], Fact)
    ]
    assert len(stamped) == 2  # el_cash + el_rev for the close month
    assert all(f.period_end == PE for f in stamped)


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
    session.execute.side_effect = [_exec_scalars([])]
    retracted = retract_canonical_statement_sets(
      session, period_start=PS, period_end=PE
    )
    assert retracted == []
    assert session.execute.call_count == 1
    session.flush.assert_not_called()

  def test_sweeps_results_then_deletes_sets(self):
    session = MagicMock()
    session.execute.side_effect = [
      _exec_scalars(["fs_a", "fs_b"]),
      MagicMock(),  # VerificationResult sweep
      MagicMock(),  # fact_sets delete
    ]
    retracted = retract_canonical_statement_sets(
      session, period_start=PS, period_end=PE
    )
    assert retracted == ["fs_a", "fs_b"]

    calls = session.execute.call_args_list
    sweep_sql = str(calls[1].args[0])
    assert "verification_results" in sweep_sql.lower()
    delete_sql = str(calls[2].args[0])
    assert "DELETE FROM fact_sets" in delete_sql
    assert calls[2].args[1] == {"ids": ["fs_a", "fs_b"]}
    session.flush.assert_called_once()

  def test_window_predicates_protect_other_producers(self):
    """The window select must exclude publication snapshots
    (report_id NOT NULL), scenario months, and non-'report' set types."""
    session = MagicMock()
    session.execute.side_effect = [_exec_scalars([])]
    retract_canonical_statement_sets(session, period_start=PS, period_end=PE)
    sql = str(session.execute.call_args_list[0].args[0])
    assert "factset_type = 'report'" in sql
    assert "report_id IS NULL" in sql
    assert "scenario_id IS NULL" in sql
