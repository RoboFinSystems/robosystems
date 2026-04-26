"""Tests for operations/roboinvestor/commands/portfolio_block.py."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import IntegrityError

from robosystems.models.api.extensions.investor import (
  CreatePortfolioBlockRequest,
  DeletePortfolioBlockOperation,
  PortfolioBlockEnvelope,
  PortfolioBlockPortfolioFields,
  PortfolioBlockPortfolioPatch,
  PortfolioBlockPositionAdd,
  PortfolioBlockPositionDispose,
  PortfolioBlockPositions,
  PortfolioBlockPositionUpdate,
  UpdatePortfolioBlockOperation,
)
from robosystems.operations.roboinvestor.commands.portfolio_block import (
  ActivePositionsRequireConfirmationError,
  DuplicateActivePositionError,
  PortfolioNotFoundError,
  PositionNotFoundError,
  PositionPortfolioMismatchError,
  SecurityNotFoundError,
  create_portfolio_block,
  delete_portfolio_block,
  update_portfolio_block,
)

CMD_MODULE = "robosystems.operations.roboinvestor.commands.portfolio_block"


def _envelope(portfolio_id: str = "pf_01") -> PortfolioBlockEnvelope:
  return PortfolioBlockEnvelope(
    id=portfolio_id,
    name="Main Fund",
    base_currency="USD",
    positions=[],
    total_cost_basis_dollars=0.0,
    total_current_value_dollars=None,
    active_position_count=0,
    created_at=datetime(2024, 1, 1, tzinfo=UTC),
    updated_at=datetime(2024, 1, 1, tzinfo=UTC),
  )


def _make_portfolio(**overrides) -> MagicMock:
  defaults = {
    "id": "pf_01",
    "name": "Main Fund",
    "description": None,
    "strategy": None,
    "inception_date": None,
    "base_currency": "USD",
    "entity_id": None,
    "metadata_": {},
    "updated_at": datetime(2024, 1, 1, tzinfo=UTC),
  }
  defaults.update(overrides)
  row = MagicMock()
  for k, v in defaults.items():
    setattr(row, k, v)
  return row


def _make_position(**overrides) -> MagicMock:
  defaults = {
    "id": "pos_01",
    "portfolio_id": "pf_01",
    "security_id": "sec_01",
    "quantity": 10.0,
    "quantity_type": "shares",
    "cost_basis": 1000,
    "current_value": None,
    "valuation_date": None,
    "valuation_source": None,
    "acquisition_date": None,
    "disposition_date": None,
    "status": "active",
    "metadata_": {},
    "notes": None,
  }
  defaults.update(overrides)
  row = MagicMock()
  for k, v in defaults.items():
    setattr(row, k, v)
  return row


def _security_validation_result(present_ids: list[str]) -> MagicMock:
  result = MagicMock()
  result.scalars.return_value.all.return_value = present_ids
  return result


def _scalar_one(row) -> MagicMock:
  result = MagicMock()
  result.scalar_one_or_none.return_value = row
  return result


def _scalar(value) -> MagicMock:
  result = MagicMock()
  result.scalar.return_value = value
  return result


def _scalars_all(rows) -> MagicMock:
  result = MagicMock()
  result.scalars.return_value.all.return_value = rows
  return result


# ────────────────────────────────────────────────────────────────────
# create_portfolio_block
# ────────────────────────────────────────────────────────────────────


class TestCreatePortfolioBlock:
  def test_zero_positions_creates_portfolio(self) -> None:
    session = MagicMock()
    fake_portfolio = _make_portfolio(id="pf_new")
    body = CreatePortfolioBlockRequest(
      portfolio=PortfolioBlockPortfolioFields(name="New Fund")
    )

    with (
      patch(
        f"{CMD_MODULE}.Portfolio",
        return_value=fake_portfolio,
      ),
      patch(
        f"{CMD_MODULE}.get_portfolio_block",
        return_value=_envelope("pf_new"),
      ) as m_get,
    ):
      result = create_portfolio_block(session, body, created_by="usr_1")

    session.add.assert_called_once_with(fake_portfolio)
    session.flush.assert_called()
    assert result.id == "pf_new"
    m_get.assert_called_once_with(session, "pf_new")

  def test_validates_securities_before_writing(self) -> None:
    session = MagicMock()
    # Securities check returns no rows for the requested id
    session.execute.side_effect = [_security_validation_result([])]

    body = CreatePortfolioBlockRequest(
      portfolio=PortfolioBlockPortfolioFields(name="Fund"),
      positions=[
        PortfolioBlockPositionAdd(security_id="sec_missing", quantity=1.0, cost_basis=0)
      ],
    )

    with pytest.raises(SecurityNotFoundError):
      create_portfolio_block(session, body, created_by="usr_1")

    session.add.assert_not_called()

  def test_creates_with_positions(self) -> None:
    session = MagicMock()
    # First execute: validate securities. Position add() then triggers flush
    # via the position-mock side effects below.
    session.execute.side_effect = [_security_validation_result(["sec_01", "sec_02"])]
    fake_portfolio = _make_portfolio(id="pf_new")

    body = CreatePortfolioBlockRequest(
      portfolio=PortfolioBlockPortfolioFields(name="Fund"),
      positions=[
        PortfolioBlockPositionAdd(security_id="sec_01", quantity=1.0, cost_basis=100),
        PortfolioBlockPositionAdd(security_id="sec_02", quantity=2.0, cost_basis=200),
      ],
    )

    with (
      patch(f"{CMD_MODULE}.Portfolio", return_value=fake_portfolio),
      patch(
        f"{CMD_MODULE}.get_portfolio_block",
        return_value=_envelope("pf_new"),
      ),
    ):
      result = create_portfolio_block(session, body, created_by="usr_1")

    # 1 portfolio + 2 positions = 3 add calls
    assert session.add.call_count == 3
    assert result.id == "pf_new"

  def test_duplicate_active_position_propagates(self) -> None:
    session = MagicMock()
    session.execute.side_effect = [_security_validation_result(["sec_01"])]
    fake_portfolio = _make_portfolio(id="pf_new")

    integrity_err = IntegrityError("stmt", {}, MagicMock(pgcode="23505"))

    body = CreatePortfolioBlockRequest(
      portfolio=PortfolioBlockPortfolioFields(name="Fund"),
      positions=[
        PortfolioBlockPositionAdd(security_id="sec_01", quantity=1.0, cost_basis=0)
      ],
    )

    flush_calls = {"n": 0}

    def _flush():
      flush_calls["n"] += 1
      if flush_calls["n"] == 2:
        # Second flush is the position add
        raise integrity_err

    session.flush.side_effect = _flush

    with patch(f"{CMD_MODULE}.Portfolio", return_value=fake_portfolio):
      with pytest.raises(DuplicateActivePositionError):
        create_portfolio_block(session, body, created_by="usr_1")


# ────────────────────────────────────────────────────────────────────
# update_portfolio_block
# ────────────────────────────────────────────────────────────────────


class TestUpdatePortfolioBlock:
  def test_raises_when_portfolio_missing(self) -> None:
    session = MagicMock()
    session.execute.side_effect = [_scalar_one(None)]

    body = UpdatePortfolioBlockOperation(
      portfolio_id="pf_missing",
      portfolio=PortfolioBlockPortfolioPatch(name="X"),
    )

    with pytest.raises(PortfolioNotFoundError):
      update_portfolio_block(session, body, created_by="usr_1")

  def test_patches_portfolio_fields(self) -> None:
    session = MagicMock()
    portfolio = _make_portfolio()
    # No add deltas + no update/dispose ids → only the portfolio fetch
    # hits session.execute.
    session.execute.side_effect = [_scalar_one(portfolio)]
    body = UpdatePortfolioBlockOperation(
      portfolio_id="pf_01",
      portfolio=PortfolioBlockPortfolioPatch(name="Renamed", strategy="balanced"),
    )

    with patch(
      f"{CMD_MODULE}.get_portfolio_block",
      return_value=_envelope("pf_01"),
    ):
      result = update_portfolio_block(session, body, created_by="usr_1")

    assert portfolio.name == "Renamed"
    assert portfolio.strategy == "balanced"
    assert result.id == "pf_01"

  def test_applies_add_update_dispose_in_one_call(self) -> None:
    session = MagicMock()
    portfolio = _make_portfolio()
    pos_to_update = _make_position(id="pos_upd", portfolio_id="pf_01", notes="old")
    pos_to_dispose = _make_position(id="pos_dis", portfolio_id="pf_01")

    # Sequence:
    # 1) portfolio fetch
    # 2) validate add securities
    # 3) load positions (update + dispose ids)
    session.execute.side_effect = [
      _scalar_one(portfolio),
      _security_validation_result(["sec_new"]),
      _scalars_all([pos_to_update, pos_to_dispose]),
    ]

    body = UpdatePortfolioBlockOperation(
      portfolio_id="pf_01",
      positions=PortfolioBlockPositions(
        add=[
          PortfolioBlockPositionAdd(security_id="sec_new", quantity=5.0, cost_basis=100)
        ],
        update=[PortfolioBlockPositionUpdate(id="pos_upd", notes="new note")],
        dispose=[
          PortfolioBlockPositionDispose(id="pos_dis", disposition_reason="sold")
        ],
      ),
    )

    with patch(
      f"{CMD_MODULE}.get_portfolio_block",
      return_value=_envelope("pf_01"),
    ):
      update_portfolio_block(session, body, created_by="usr_1")

    assert pos_to_update.notes == "new note"
    assert pos_to_dispose.status == "disposed"
    assert isinstance(pos_to_dispose.disposition_date, date)
    assert pos_to_dispose.metadata_ == {"disposition_reason": "sold"}

  def test_unknown_position_raises(self) -> None:
    session = MagicMock()
    portfolio = _make_portfolio()
    # No add deltas → validate_securities is a no-op (no execute); only
    # the portfolio fetch and the position load hit session.execute.
    session.execute.side_effect = [
      _scalar_one(portfolio),
      # Only one of the two requested ids is returned
      _scalars_all([_make_position(id="pos_a", portfolio_id="pf_01")]),
    ]

    body = UpdatePortfolioBlockOperation(
      portfolio_id="pf_01",
      positions=PortfolioBlockPositions(
        update=[
          PortfolioBlockPositionUpdate(id="pos_a"),
          PortfolioBlockPositionUpdate(id="pos_missing"),
        ]
      ),
    )

    with pytest.raises(PositionNotFoundError):
      update_portfolio_block(session, body, created_by="usr_1")

  def test_position_belonging_to_other_portfolio_raises(self) -> None:
    session = MagicMock()
    portfolio = _make_portfolio()
    foreign = _make_position(id="pos_x", portfolio_id="pf_other")
    # No add deltas → validate_securities is a no-op (no execute call); the
    # side_effect sequence only includes portfolio fetch + position load.
    session.execute.side_effect = [
      _scalar_one(portfolio),
      _scalars_all([foreign]),
    ]

    body = UpdatePortfolioBlockOperation(
      portfolio_id="pf_01",
      positions=PortfolioBlockPositions(
        update=[PortfolioBlockPositionUpdate(id="pos_x")]
      ),
    )

    with pytest.raises(PositionPortfolioMismatchError):
      update_portfolio_block(session, body, created_by="usr_1")

  def test_partial_failure_does_not_call_get_envelope(self) -> None:
    """If validation fails mid-flow, get_portfolio_block is never called —
    the caller's transaction will roll back any partial state."""
    session = MagicMock()
    portfolio = _make_portfolio()
    session.execute.side_effect = [
      _scalar_one(portfolio),
      _scalars_all([]),  # Position lookup returns nothing
    ]

    body = UpdatePortfolioBlockOperation(
      portfolio_id="pf_01",
      positions=PortfolioBlockPositions(
        update=[PortfolioBlockPositionUpdate(id="pos_missing")]
      ),
    )

    with patch(f"{CMD_MODULE}.get_portfolio_block") as m_get:
      with pytest.raises(PositionNotFoundError):
        update_portfolio_block(session, body, created_by="usr_1")
    m_get.assert_not_called()


# ────────────────────────────────────────────────────────────────────
# delete_portfolio_block
# ────────────────────────────────────────────────────────────────────


class TestDeletePortfolioBlock:
  def test_raises_when_portfolio_missing(self) -> None:
    session = MagicMock()
    session.execute.side_effect = [_scalar_one(None)]

    with pytest.raises(PortfolioNotFoundError):
      delete_portfolio_block(
        session, DeletePortfolioBlockOperation(portfolio_id="pf_missing")
      )

  def test_blocks_active_positions_without_confirm(self) -> None:
    session = MagicMock()
    portfolio = _make_portfolio()
    session.execute.side_effect = [
      _scalar_one(portfolio),
      _scalar(2),  # active count
    ]

    with pytest.raises(ActivePositionsRequireConfirmationError) as exc:
      delete_portfolio_block(
        session,
        DeletePortfolioBlockOperation(portfolio_id="pf_01"),
      )
    assert exc.value.active_count == 2
    session.delete.assert_not_called()

  def test_succeeds_when_no_active_positions_and_no_flag(self) -> None:
    session = MagicMock()
    portfolio = _make_portfolio()
    disposed = _make_position(id="pos_d", status="disposed")
    session.execute.side_effect = [
      _scalar_one(portfolio),
      _scalar(0),  # active count
      _scalars_all([disposed]),  # the deletion scan
    ]

    body = DeletePortfolioBlockOperation(portfolio_id="pf_01")
    result = delete_portfolio_block(session, body)

    assert result.deleted is True
    assert result.portfolio_id == "pf_01"
    assert result.positions_deleted == 1
    # 1 position + 1 portfolio
    assert session.delete.call_count == 2

  def test_cascade_deletes_with_confirmation(self) -> None:
    session = MagicMock()
    portfolio = _make_portfolio()
    active = _make_position(id="pos_a", status="active")
    session.execute.side_effect = [
      _scalar_one(portfolio),
      _scalar(1),  # active count
      _scalars_all([active]),
    ]

    body = DeletePortfolioBlockOperation(
      portfolio_id="pf_01", confirm_active_positions=True
    )
    result = delete_portfolio_block(session, body)

    assert result.deleted is True
    assert result.positions_deleted == 1
