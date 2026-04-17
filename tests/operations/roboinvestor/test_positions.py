"""Tests for operations/roboinvestor/{reads,commands}/positions.py."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import IntegrityError

from robosystems.models.api.extensions.investor import CreatePositionRequest
from robosystems.operations.roboinvestor.commands.positions import (
  DuplicateActivePositionError,
  PortfolioNotFoundError,
  SecurityNotFoundError,
  create_position,
  soft_delete_position,
  update_position,
)
from robosystems.operations.roboinvestor.reads.positions import (
  get_position,
  list_positions,
  position_to_response,
)


def _make_position(**overrides) -> MagicMock:
  defaults = {
    "id": "pos_01",
    "portfolio_id": "pf_01",
    "security_id": "sec_01",
    "quantity": 100.0,
    "quantity_type": "shares",
    "cost_basis": 50000,  # cents
    "currency": "USD",
    "current_value": 55000,
    "valuation_date": date(2024, 12, 31),
    "valuation_source": "manual",
    "acquisition_date": date(2024, 6, 1),
    "disposition_date": None,
    "status": "active",
    "notes": None,
    "created_at": datetime(2024, 6, 1, tzinfo=UTC),
    "updated_at": datetime(2024, 6, 1, tzinfo=UTC),
  }
  defaults.update(overrides)
  row = MagicMock()
  for k, v in defaults.items():
    setattr(row, k, v)
  return row


def _make_security(**overrides) -> MagicMock:
  defaults = {
    "id": "sec_01",
    "entity_id": "ent_01",
    "name": "Acme Class A",
    "security_type": "equity",
  }
  defaults.update(overrides)
  s = MagicMock()
  for k, v in defaults.items():
    setattr(s, k, v)
  return s


def _make_entity(**overrides) -> MagicMock:
  defaults = {"id": "ent_01", "name": "Acme Corp"}
  defaults.update(overrides)
  e = MagicMock()
  for k, v in defaults.items():
    setattr(e, k, v)
  return e


def _make_portfolio() -> MagicMock:
  p = MagicMock()
  p.id = "pf_01"
  return p


class TestPositionToResponse:
  def test_converts_cents_to_dollars(self) -> None:
    row = _make_position(cost_basis=12345, current_value=54321)
    resp = position_to_response(row, security_name="Acme", entity_name="Acme Corp")
    assert resp.cost_basis == 12345
    assert resp.cost_basis_dollars == 123.45
    assert resp.current_value_dollars == 543.21
    assert resp.security_name == "Acme"
    assert resp.entity_name == "Acme Corp"

  def test_handles_none_current_value(self) -> None:
    row = _make_position(current_value=None)
    resp = position_to_response(row)
    assert resp.current_value_dollars is None


class TestListPositions:
  def test_enriches_with_security_and_entity_names(self) -> None:
    positions = [_make_position(id="pos_1"), _make_position(id="pos_2")]
    securities = [_make_security()]
    entities = [_make_entity()]

    session = MagicMock()
    count_result = MagicMock()
    count_result.scalar.return_value = 2
    list_result = MagicMock()
    list_result.scalars.return_value.all.return_value = positions
    sec_result = MagicMock()
    sec_result.scalars.return_value.all.return_value = securities
    ent_result = MagicMock()
    ent_result.scalars.return_value.all.return_value = entities
    session.execute.side_effect = [count_result, list_result, sec_result, ent_result]

    result = list_positions(session, portfolio_id="pf_01")

    assert result.pagination.total == 2
    assert len(result.positions) == 2
    assert result.positions[0].security_name == "Acme Class A"
    assert result.positions[0].entity_name == "Acme Corp"

  def test_zero_results(self) -> None:
    session = MagicMock()
    count_result = MagicMock()
    count_result.scalar.return_value = 0
    list_result = MagicMock()
    list_result.scalars.return_value.all.return_value = []
    session.execute.side_effect = [count_result, list_result]

    result = list_positions(session)

    assert result.positions == []


class TestGetPosition:
  def test_returns_enriched_position(self) -> None:
    pos = _make_position()
    sec = _make_security()
    ent = _make_entity()

    session = MagicMock()
    pos_result = MagicMock()
    pos_result.scalar_one_or_none.return_value = pos
    sec_result = MagicMock()
    sec_result.scalars.return_value.all.return_value = [sec]
    ent_result = MagicMock()
    ent_result.scalars.return_value.all.return_value = [ent]
    session.execute.side_effect = [pos_result, sec_result, ent_result]

    result = get_position(session, "pos_01")

    assert result is not None
    assert result.security_name == "Acme Class A"
    assert result.entity_name == "Acme Corp"

  def test_returns_none_when_missing(self) -> None:
    session = MagicMock()
    miss = MagicMock()
    miss.scalar_one_or_none.return_value = None
    session.execute.side_effect = [miss]

    assert get_position(session, "pos_missing") is None


class TestCreatePosition:
  def _base_body(self, **overrides) -> CreatePositionRequest:
    defaults = {
      "portfolio_id": "pf_01",
      "security_id": "sec_01",
      "quantity": 100.0,
      "quantity_type": "shares",
      "cost_basis": 50000,
      "currency": "USD",
      "acquisition_date": date(2024, 6, 1),
    }
    defaults.update(overrides)
    return CreatePositionRequest(**defaults)

  def test_creates_successfully(self) -> None:
    portfolio = _make_portfolio()
    security = _make_security()
    entity = _make_entity()

    session = MagicMock()
    pf_result = MagicMock()
    pf_result.scalar_one_or_none.return_value = portfolio
    sec_result = MagicMock()
    sec_result.scalar_one_or_none.return_value = security
    ent_result = MagicMock()
    ent_result.scalar_one_or_none.return_value = entity
    session.execute.side_effect = [pf_result, sec_result, ent_result]

    fake_pos = _make_position()
    with patch(
      "robosystems.operations.roboinvestor.commands.positions.Position",
      return_value=fake_pos,
    ):
      result = create_position(session, self._base_body(), created_by="usr_1")

    assert result.security_name == "Acme Class A"
    assert result.entity_name == "Acme Corp"
    session.add.assert_called_once_with(fake_pos)
    session.flush.assert_called_once()

  def test_raises_when_portfolio_missing(self) -> None:
    session = MagicMock()
    miss = MagicMock()
    miss.scalar_one_or_none.return_value = None
    session.execute.side_effect = [miss]

    with pytest.raises(PortfolioNotFoundError):
      create_position(session, self._base_body(), created_by="usr_1")
    session.add.assert_not_called()

  def test_raises_when_security_missing(self) -> None:
    session = MagicMock()
    pf_result = MagicMock()
    pf_result.scalar_one_or_none.return_value = _make_portfolio()
    sec_result = MagicMock()
    sec_result.scalar_one_or_none.return_value = None
    session.execute.side_effect = [pf_result, sec_result]

    with pytest.raises(SecurityNotFoundError):
      create_position(session, self._base_body(), created_by="usr_1")
    session.add.assert_not_called()

  def test_translates_integrity_error_to_duplicate(self) -> None:
    session = MagicMock()
    pf_result = MagicMock()
    pf_result.scalar_one_or_none.return_value = _make_portfolio()
    sec_result = MagicMock()
    sec_result.scalar_one_or_none.return_value = _make_security()
    ent_result = MagicMock()
    ent_result.scalar_one_or_none.return_value = _make_entity()
    session.execute.side_effect = [pf_result, sec_result, ent_result]
    orig = Exception("unique violation")
    orig.pgcode = "23505"  # type: ignore[attr-defined]
    session.flush.side_effect = IntegrityError("stmt", {}, orig)

    with (
      patch(
        "robosystems.operations.roboinvestor.commands.positions.Position",
        return_value=_make_position(),
      ),
      pytest.raises(DuplicateActivePositionError),
    ):
      create_position(session, self._base_body(), created_by="usr_1")

  def test_reraises_non_unique_integrity_error(self) -> None:
    session = MagicMock()
    pf_result = MagicMock()
    pf_result.scalar_one_or_none.return_value = _make_portfolio()
    sec_result = MagicMock()
    sec_result.scalar_one_or_none.return_value = _make_security()
    ent_result = MagicMock()
    ent_result.scalar_one_or_none.return_value = _make_entity()
    session.execute.side_effect = [pf_result, sec_result, ent_result]
    orig = Exception("fk violation")
    orig.pgcode = "23503"  # type: ignore[attr-defined]  # foreign_key_violation
    session.flush.side_effect = IntegrityError("stmt", {}, orig)

    with (
      patch(
        "robosystems.operations.roboinvestor.commands.positions.Position",
        return_value=_make_position(),
      ),
      pytest.raises(IntegrityError),
    ):
      create_position(session, self._base_body(), created_by="usr_1")


class TestUpdatePosition:
  def test_applies_updates_and_enriches(self) -> None:
    from robosystems.models.api.extensions.investor import UpdatePositionOperation

    row = _make_position(notes="old")
    sec = _make_security()
    ent = _make_entity()

    session = MagicMock()
    pos_result = MagicMock()
    pos_result.scalar_one_or_none.return_value = row
    sec_result = MagicMock()
    sec_result.scalars.return_value.all.return_value = [sec]
    ent_result = MagicMock()
    ent_result.scalars.return_value.all.return_value = [ent]
    session.execute.side_effect = [pos_result, sec_result, ent_result]

    result = update_position(
      session, UpdatePositionOperation(position_id="pos_01", notes="new")
    )

    assert row.notes == "new"
    assert result.notes == "new"

  def test_raises_when_missing(self) -> None:
    from robosystems.models.api.extensions.investor import UpdatePositionOperation
    from robosystems.operations.roboinvestor.commands.positions import (
      PositionNotFoundError,
    )

    session = MagicMock()
    miss = MagicMock()
    miss.scalar_one_or_none.return_value = None
    session.execute.side_effect = [miss]

    with pytest.raises(PositionNotFoundError):
      update_position(
        session, UpdatePositionOperation(position_id="pos_missing", notes="x")
      )


class TestSoftDeletePosition:
  def test_sets_status_to_disposed(self) -> None:
    from robosystems.models.api.extensions.investor import DeletePositionOperation

    row = _make_position(status="active")
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = row

    result = soft_delete_position(
      session, DeletePositionOperation(position_id="pos_01")
    )
    assert result.deleted is True
    assert row.status == "disposed"

  def test_raises_when_missing(self) -> None:
    from robosystems.models.api.extensions.investor import DeletePositionOperation
    from robosystems.operations.roboinvestor.commands.positions import (
      PositionNotFoundError,
    )

    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None

    with pytest.raises(PositionNotFoundError):
      soft_delete_position(session, DeletePositionOperation(position_id="pos_missing"))
