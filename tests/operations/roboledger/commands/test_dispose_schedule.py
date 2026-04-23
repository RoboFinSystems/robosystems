"""Tests for dispose_schedule command."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.extensions.schedules import DisposeScheduleRequest
from robosystems.models.extensions import Rule
from robosystems.operations.roboledger.commands.schedules import (
  ScheduleNotFoundError,
  dispose_schedule,
)

# ── Helpers ───────────────────────────────────────────────────────────────

_SVC = "robosystems.operations.roboledger.commands.schedules.ScheduleService"


def _make_structure(
  *,
  structure_id: str = "struct_01",
  original_amount: int = 120_000,  # $1200 in cents
  asset_element_id: str = "elem_asset",
  credit_element_id: str = "elem_accum_depr",
) -> MagicMock:
  structure = MagicMock()
  structure.id = structure_id
  structure.structure_type = "schedule"
  structure.artifact_mechanics = {
    "schedule_metadata": {
      "original_amount": original_amount,
      "asset_element_id": asset_element_id,
    },
    "entry_template": {
      "credit_element_id": credit_element_id,
    },
  }
  return structure


def _exec_result(*, row=None, fetchone_row=None, scalars_all=None) -> MagicMock:
  result = MagicMock()
  result.scalar_one_or_none.return_value = row
  result.fetchone.return_value = fetchone_row
  result.scalars.return_value.all.return_value = scalars_all or []
  return result


def _acc_fact(value_dollars: float) -> MagicMock:
  """Simulate an instant fact row with value in dollars (as stored)."""
  row = MagicMock()
  row.value = value_dollars
  return row


@dataclass
class _Query:
  model: type
  deleted_models: list[type]

  def filter(self, *_args) -> _Query:
    return self

  def delete(self, *, synchronize_session: bool = False) -> int:
    self.deleted_models.append(self.model)
    return 1


def _closing_entry_result(entry_id: str = "entry_01") -> MagicMock:
  r = MagicMock()
  r.outcome = "created"
  r.entry_id = entry_id
  r.status = "draft"
  r.posting_date = date(2026, 3, 31)
  r.memo = "Disposal"
  r.debit_element_id = "elem_accum_depr"
  r.credit_element_id = "elem_asset"
  r.amount = 800.0
  r.reason = None
  r.reversal = None
  return r


def _run_dispose(session, body: DisposeScheduleRequest, accumulated_dollars: float):
  """Patch ScheduleService and run dispose_schedule, returning (resp, svc_instance, deleted_models)."""
  deleted_models: list[type] = []
  structure = _make_structure()
  session.execute.side_effect = [
    _exec_result(row=structure),
    _exec_result(fetchone_row=_acc_fact(accumulated_dollars)),
    MagicMock(),  # DELETE FROM verification_results (SumEquals cleanup)
  ]
  session.query.side_effect = lambda model: _Query(model, deleted_models)

  with patch(_SVC) as MockSvc:
    svc = MockSvc.return_value
    svc.truncate_schedule.return_value = {
      "facts_deleted": 9,
      "new_end_date": body.disposal_date,
      "reason": body.reason,
      "structure_id": body.structure_id,
    }
    svc.create_manual_closing_entry.return_value = _closing_entry_result()
    resp = dispose_schedule(session, body, created_by="usr_test")

  return resp, svc, deleted_models


# ── Happy-path tests ─────────────────────────────────────────────────────


class TestDisposeScheduleGainOnSale:
  """Proceeds ($900) > NBV ($200) → gain of $700."""

  def setup_method(self):
    # original = $1200, accumulated = $1000 → NBV = $200
    # proceeds = $900 → gain = $700
    self.session = MagicMock()
    self.body = DisposeScheduleRequest(
      structure_id="struct_01",
      disposal_date=date(2026, 3, 31),
      sale_proceeds=90_000,  # $900 cents
      proceeds_element_id="elem_cash",
      gain_loss_element_id="elem_gain",
      memo="Sold equipment",
      reason="asset sold",
    )
    self.resp, self.svc, self.deleted_models = _run_dispose(
      self.session, self.body, accumulated_dollars=1000.0
    )

  def test_nbv_and_gain_loss_are_correct(self):
    assert self.resp.original_amount == 120_000
    assert self.resp.accumulated_depreciation == 100_000
    assert self.resp.net_book_value == 20_000
    assert self.resp.gain_loss == 70_000  # positive = gain

  def test_facts_deleted_count_passed_through(self):
    assert self.resp.facts_deleted == 9

  def test_closing_entry_created(self):
    assert self.resp.closing_entry.entry_id == "entry_01"
    assert self.resp.closing_entry.outcome == "created"

  def test_line_items_are_balanced_4_way(self):
    items = self.svc.create_manual_closing_entry.call_args.kwargs["line_items"]
    assert len(items) == 4
    total_dr = sum(li["debit_amount"] for li in items)
    total_cr = sum(li["credit_amount"] for li in items)
    assert total_dr == total_cr

  def test_line_items_include_gain_credit(self):
    items = self.svc.create_manual_closing_entry.call_args.kwargs["line_items"]
    gain_lines = [li for li in items if li["element_id"] == "elem_gain"]
    assert len(gain_lines) == 1
    assert gain_lines[0]["credit_amount"] == 70_000
    assert gain_lines[0]["debit_amount"] == 0

  def test_sum_equals_rule_deleted(self):
    assert Rule in self.deleted_models

  def test_session_committed(self):
    self.session.commit.assert_called_once()


class TestDisposeScheduleLossOnSale:
  """Proceeds ($100) < NBV ($200) → loss of $100."""

  def setup_method(self):
    # original = $1200, accumulated = $1000 → NBV = $200
    # proceeds = $100 → loss = $100
    self.session = MagicMock()
    self.body = DisposeScheduleRequest(
      structure_id="struct_01",
      disposal_date=date(2026, 3, 31),
      sale_proceeds=10_000,  # $100
      proceeds_element_id="elem_cash",
      gain_loss_element_id="elem_loss",
      memo="Sold at loss",
      reason="asset sold below book",
    )
    self.resp, self.svc, self.deleted_models = _run_dispose(
      self.session, self.body, accumulated_dollars=1000.0
    )

  def test_gain_loss_is_negative(self):
    assert self.resp.gain_loss == -10_000  # loss

  def test_line_items_include_loss_debit(self):
    items = self.svc.create_manual_closing_entry.call_args.kwargs["line_items"]
    loss_lines = [li for li in items if li["element_id"] == "elem_loss"]
    assert len(loss_lines) == 1
    assert loss_lines[0]["debit_amount"] == 10_000
    assert loss_lines[0]["credit_amount"] == 0

  def test_line_items_balanced(self):
    items = self.svc.create_manual_closing_entry.call_args.kwargs["line_items"]
    total_dr = sum(li["debit_amount"] for li in items)
    total_cr = sum(li["credit_amount"] for li in items)
    assert total_dr == total_cr


class TestDisposeScheduleAbandonmentWithNBV:
  """No proceeds, NBV > 0 → full NBV is a loss, no cash line."""

  def setup_method(self):
    # original = $1200, accumulated = $600 → NBV = $600
    # proceeds = 0 → loss = $600
    self.session = MagicMock()
    self.body = DisposeScheduleRequest(
      structure_id="struct_01",
      disposal_date=date(2026, 6, 30),
      sale_proceeds=None,
      proceeds_element_id=None,
      gain_loss_element_id="elem_loss",
      memo="Abandoned asset",
      reason="abandoned",
    )
    self.resp, self.svc, _ = _run_dispose(
      self.session, self.body, accumulated_dollars=600.0
    )

  def test_gain_loss_equals_negative_nbv(self):
    assert self.resp.net_book_value == 60_000
    assert self.resp.gain_loss == -60_000

  def test_no_cash_line_item(self):
    items = self.svc.create_manual_closing_entry.call_args.kwargs["line_items"]
    cash_lines = [li for li in items if li["element_id"] == "elem_cash"]
    assert len(cash_lines) == 0

  def test_3_line_items(self):
    items = self.svc.create_manual_closing_entry.call_args.kwargs["line_items"]
    assert len(items) == 3  # DR accum_depr, CR asset, DR loss

  def test_line_items_balanced(self):
    items = self.svc.create_manual_closing_entry.call_args.kwargs["line_items"]
    total_dr = sum(li["debit_amount"] for li in items)
    total_cr = sum(li["credit_amount"] for li in items)
    assert total_dr == total_cr


class TestDisposeScheduleFullyDepreciated:
  """Accumulated == original (NBV = 0) → no gain/loss line; 2 line items balance."""

  def setup_method(self):
    # original = $1200, accumulated = $1200 → NBV = 0, gain_loss = 0
    self.session = MagicMock()
    self.body = DisposeScheduleRequest(
      structure_id="struct_01",
      disposal_date=date(2026, 12, 31),
      sale_proceeds=None,
      proceeds_element_id=None,
      gain_loss_element_id=None,  # not required when NBV = 0
      memo="Scrapped fully depreciated asset",
      reason="scrapped",
    )
    self.resp, self.svc, _ = _run_dispose(
      self.session, self.body, accumulated_dollars=1200.0
    )

  def test_nbv_and_gain_loss_are_zero(self):
    assert self.resp.net_book_value == 0
    assert self.resp.gain_loss == 0

  def test_only_2_line_items(self):
    items = self.svc.create_manual_closing_entry.call_args.kwargs["line_items"]
    assert len(items) == 2

  def test_2_line_items_balance(self):
    items = self.svc.create_manual_closing_entry.call_args.kwargs["line_items"]
    total_dr = sum(li["debit_amount"] for li in items)
    total_cr = sum(li["credit_amount"] for li in items)
    assert total_dr == total_cr

  def test_first_line_debits_accumulated_depreciation(self):
    items = self.svc.create_manual_closing_entry.call_args.kwargs["line_items"]
    assert items[0]["element_id"] == "elem_accum_depr"
    assert items[0]["debit_amount"] == 120_000

  def test_second_line_credits_asset_at_cost(self):
    items = self.svc.create_manual_closing_entry.call_args.kwargs["line_items"]
    assert items[1]["element_id"] == "elem_asset"
    assert items[1]["credit_amount"] == 120_000


# ── Validation tests ──────────────────────────────────────────────────────


class TestDisposeScheduleValidation:
  def _session_for_validation(self, accumulated_dollars: float) -> MagicMock:
    session = MagicMock()
    structure = _make_structure()
    session.execute.side_effect = [
      _exec_result(row=structure),
      _exec_result(fetchone_row=_acc_fact(accumulated_dollars)),
      MagicMock(),  # DELETE FROM verification_results (SumEquals cleanup)
    ]
    return session

  def test_requires_proceeds_element_id_when_proceeds_set(self):
    session = self._session_for_validation(600.0)
    body = DisposeScheduleRequest(
      structure_id="struct_01",
      disposal_date=date(2026, 3, 31),
      sale_proceeds=50_000,
      proceeds_element_id=None,  # missing
      gain_loss_element_id="elem_gl",
      memo="Sale",
      reason="sold",
    )
    with pytest.raises(ValueError, match="proceeds_element_id is required"):
      with patch(_SVC):
        dispose_schedule(session, body, created_by="usr_test")

  def test_requires_gain_loss_element_id_when_nbv_and_gain_loss_nonzero(self):
    # accumulated = $600 → NBV = $600, proceeds = $900 → gain = $300
    session = self._session_for_validation(600.0)
    body = DisposeScheduleRequest(
      structure_id="struct_01",
      disposal_date=date(2026, 3, 31),
      sale_proceeds=90_000,
      proceeds_element_id="elem_cash",
      gain_loss_element_id=None,  # missing
      memo="Sale",
      reason="sold",
    )
    with pytest.raises(ValueError, match="gain_loss_element_id is required"):
      with patch(_SVC):
        dispose_schedule(session, body, created_by="usr_test")

  def test_no_gain_loss_element_required_when_fully_depreciated(self):
    # NBV = 0 → no gain/loss element needed
    session = self._session_for_validation(1200.0)  # fully depreciated
    deleted_models: list[type] = []
    session.query.side_effect = lambda model: _Query(model, deleted_models)
    body = DisposeScheduleRequest(
      structure_id="struct_01",
      disposal_date=date(2026, 12, 31),
      sale_proceeds=None,
      proceeds_element_id=None,
      gain_loss_element_id=None,  # fine when NBV = 0
      memo="Scrapped",
      reason="scrapped",
    )
    with patch(_SVC) as MockSvc:
      MockSvc.return_value.truncate_schedule.return_value = {
        "facts_deleted": 0,
        "new_end_date": body.disposal_date,
        "reason": body.reason,
        "structure_id": body.structure_id,
      }
      MockSvc.return_value.create_manual_closing_entry.return_value = (
        _closing_entry_result()
      )
      resp = dispose_schedule(session, body, created_by="usr_test")

    assert resp.net_book_value == 0

  def test_raises_schedule_not_found(self):
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None
    body = DisposeScheduleRequest(
      structure_id="struct_missing",
      disposal_date=date(2026, 3, 31),
      memo="x",
      reason="y",
    )
    with pytest.raises(ScheduleNotFoundError):
      dispose_schedule(session, body, created_by="usr_test")

  def test_raises_when_no_asset_element_id(self):
    structure = MagicMock()
    structure.id = "struct_01"
    structure.structure_type = "schedule"
    structure.artifact_mechanics = {
      "schedule_metadata": {"original_amount": 100_000, "asset_element_id": None},
      "entry_template": {"credit_element_id": "elem_accum_depr"},
    }
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = structure
    body = DisposeScheduleRequest(
      structure_id="struct_01",
      disposal_date=date(2026, 3, 31),
      memo="x",
      reason="y",
    )
    with pytest.raises(ValueError, match="asset_element_id"):
      dispose_schedule(session, body, created_by="usr_test")
