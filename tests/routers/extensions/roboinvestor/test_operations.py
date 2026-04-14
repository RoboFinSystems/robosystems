"""Tests for the roboinvestor operation routes.

Direct-call tests for all 9 operations. One happy path + one typed
error path per resource (portfolio, security, position) covers the
dispatch wiring for each command without over-testing the dispatcher
itself (that's `tests/middleware/test_extensions.py`).
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.middleware.extensions import OperationEnvelope
from robosystems.models.api.extensions.investor import (
  CreatePortfolioRequest,
  CreatePositionRequest,
  CreateSecurityRequest,
  PortfolioResponse,
  PositionResponse,
  SecurityResponse,
)
from robosystems.operations.roboinvestor.commands.portfolios import (
  PortfolioHasActivePositionsError,
)
from robosystems.operations.roboinvestor.commands.positions import (
  DuplicateActivePositionError,
  PortfolioNotFoundError,
  SecurityNotFoundError,
)
from robosystems.routers.extensions.roboinvestor.operations import (
  DeletePortfolioOperation,
  DeletePositionOperation,
  DeleteSecurityOperation,
  UpdatePortfolioOperation,
  UpdatePositionOperation,
  UpdateSecurityOperation,
  create_portfolio_op,
  create_position_op,
  create_security_op,
  delete_portfolio_op,
  delete_position_op,
  delete_security_op,
  update_portfolio_op,
  update_position_op,
  update_security_op,
)

GRAPH_ID = "kg01234567890abcdef"
MODULE = "robosystems.routers.extensions.roboinvestor.operations"


def _make_user() -> MagicMock:
  user = MagicMock()
  user.id = "usr_test123"
  return user


class _FakeCache:
  def __init__(self) -> None:
    self.store: dict = {}

  async def get(self, graph_id, operation_name, key):
    return self.store.get((graph_id, operation_name, key))

  async def put(self, graph_id, operation_name, key, envelope, ttl_seconds=86400):
    self.store[(graph_id, operation_name, key)] = envelope


def _mock_session_ctx():
  mock_session = MagicMock()
  mock_ctx = MagicMock()
  mock_ctx.__enter__ = MagicMock(return_value=mock_session)
  mock_ctx.__exit__ = MagicMock(return_value=False)
  return mock_ctx, mock_session


def _make_portfolio() -> PortfolioResponse:
  return PortfolioResponse(
    id="pf_01",
    name="Main Fund",
    base_currency="USD",
    created_at=datetime(2024, 1, 1, tzinfo=UTC),
    updated_at=datetime(2024, 1, 1, tzinfo=UTC),
  )


def _make_security() -> SecurityResponse:
  return SecurityResponse(
    id="sec_01",
    name="Acme Class A",
    security_type="equity",
    security_subtype="common",
    terms={},
    is_active=True,
    created_at=datetime(2024, 1, 1, tzinfo=UTC),
    updated_at=datetime(2024, 1, 1, tzinfo=UTC),
  )


def _make_position() -> PositionResponse:
  return PositionResponse(
    id="pos_01",
    portfolio_id="pf_01",
    security_id="sec_01",
    quantity=100.0,
    quantity_type="shares",
    cost_basis=50000,
    cost_basis_dollars=500.0,
    currency="USD",
    current_value=55000,
    current_value_dollars=550.0,
    status="active",
    created_at=datetime(2024, 6, 1, tzinfo=UTC),
    updated_at=datetime(2024, 6, 1, tzinfo=UTC),
  )


# ────────────────────────────────────────────────────────────────────
# Portfolio
# ────────────────────────────────────────────────────────────────────


class TestCreatePortfolioOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = CreatePortfolioRequest(name="New Fund", base_currency="USD")

    with (
      patch(f"{MODULE}.cmd_create_portfolio", return_value=_make_portfolio()),
      patch(f"{MODULE}.extensions_session", return_value=_mock_session_ctx()[0]),
    ):
      env = await create_portfolio_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert isinstance(env, OperationEnvelope)
    assert env.operation == "create-portfolio"
    assert env.status == "completed"
    assert env.result["id"] == "pf_01"

  @pytest.mark.asyncio
  async def test_schema_missing_returns_404(self) -> None:
    from sqlalchemy.exc import ProgrammingError

    with patch(
      f"{MODULE}.extensions_session",
      side_effect=ProgrammingError("stmt", {}, Exception("schema missing")),
    ):
      with pytest.raises(HTTPException) as exc:
        await create_portfolio_op(
          body=CreatePortfolioRequest(name="X"),
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404
    assert "not initialized" in exc.value.detail


class TestUpdatePortfolioOp:
  @pytest.mark.asyncio
  async def test_happy_path_strips_portfolio_id_from_updates(self) -> None:
    body = UpdatePortfolioOperation(portfolio_id="pf_01", name="Renamed")

    with patch(f"{MODULE}.cmd_update_portfolio", return_value=_make_portfolio()) as m:
      with patch(f"{MODULE}.extensions_session", return_value=_mock_session_ctx()[0]):
        env = await update_portfolio_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )

    # portfolio_id is passed positionally, not in the updates dict
    _args, _kwargs = m.call_args
    _session, pf_id, updates = m.call_args[0]
    assert pf_id == "pf_01"
    assert "portfolio_id" not in updates
    assert updates == {"name": "Renamed"}
    assert env.status == "completed"

  @pytest.mark.asyncio
  async def test_not_found_returns_404(self) -> None:
    body = UpdatePortfolioOperation(portfolio_id="pf_missing", name="X")

    with (
      patch(f"{MODULE}.cmd_update_portfolio", return_value=None),
      patch(f"{MODULE}.extensions_session", return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await update_portfolio_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404


class TestDeletePortfolioOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = DeletePortfolioOperation(portfolio_id="pf_01")

    with (
      patch(f"{MODULE}.cmd_delete_portfolio", return_value=True),
      patch(f"{MODULE}.extensions_session", return_value=_mock_session_ctx()[0]),
    ):
      env = await delete_portfolio_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert env.result == {"deleted": True}

  @pytest.mark.asyncio
  async def test_has_active_positions_returns_409(self) -> None:
    body = DeletePortfolioOperation(portfolio_id="pf_01")

    with (
      patch(
        f"{MODULE}.cmd_delete_portfolio",
        side_effect=PortfolioHasActivePositionsError(3),
      ),
      patch(f"{MODULE}.extensions_session", return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await delete_portfolio_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 409
    assert "3 active position" in exc.value.detail

  @pytest.mark.asyncio
  async def test_not_found_returns_404(self) -> None:
    body = DeletePortfolioOperation(portfolio_id="pf_x")

    with (
      patch(f"{MODULE}.cmd_delete_portfolio", return_value=False),
      patch(f"{MODULE}.extensions_session", return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await delete_portfolio_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404


# ────────────────────────────────────────────────────────────────────
# Security
# ────────────────────────────────────────────────────────────────────


class TestCreateSecurityOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = CreateSecurityRequest(
      entity_id="ent_01",
      name="Acme A",
      security_type="equity",
      security_subtype="common",
    )

    with (
      patch(f"{MODULE}.cmd_create_security", return_value=_make_security()),
      patch(f"{MODULE}.extensions_session", return_value=_mock_session_ctx()[0]),
    ):
      env = await create_security_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert env.operation == "create-security"
    assert env.result["id"] == "sec_01"

  @pytest.mark.asyncio
  async def test_entity_not_found_returns_404(self) -> None:
    from robosystems.operations.roboinvestor.commands.securities import (
      EntityNotFoundError,
    )

    body = CreateSecurityRequest(
      entity_id="ent_missing",
      name="Bad",
      security_type="equity",
      security_subtype="common",
    )

    with (
      patch(
        f"{MODULE}.cmd_create_security", side_effect=EntityNotFoundError("ent_missing")
      ),
      patch(f"{MODULE}.extensions_session", return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await create_security_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404
    assert "Entity not found" in exc.value.detail


class TestUpdateSecurityOp:
  @pytest.mark.asyncio
  async def test_strips_security_id_from_updates(self) -> None:
    body = UpdateSecurityOperation(security_id="sec_01", name="Renamed")

    with patch(f"{MODULE}.cmd_update_security", return_value=_make_security()) as m:
      with patch(f"{MODULE}.extensions_session", return_value=_mock_session_ctx()[0]):
        await update_security_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )

    _session, sec_id, updates = m.call_args[0]
    assert sec_id == "sec_01"
    assert "security_id" not in updates
    assert updates == {"name": "Renamed"}


class TestDeleteSecurityOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = DeleteSecurityOperation(security_id="sec_01")

    with (
      patch(f"{MODULE}.cmd_soft_delete_security", return_value=True),
      patch(f"{MODULE}.extensions_session", return_value=_mock_session_ctx()[0]),
    ):
      env = await delete_security_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert env.result == {"deleted": True}


# ────────────────────────────────────────────────────────────────────
# Position
# ────────────────────────────────────────────────────────────────────


class TestCreatePositionOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = CreatePositionRequest(
      portfolio_id="pf_01",
      security_id="sec_01",
      quantity=100.0,
      cost_basis=50000,
      currency="USD",
      acquisition_date=date(2024, 6, 1),
    )

    with (
      patch(f"{MODULE}.cmd_create_position", return_value=_make_position()),
      patch(f"{MODULE}.extensions_session", return_value=_mock_session_ctx()[0]),
    ):
      env = await create_position_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert env.operation == "create-position"
    # Envelope result uses snake_case (Pydantic model_dump default) — GraphQL
    # is the only surface that camelCases via Strawberry's auto_camel_case.
    assert env.result["cost_basis_dollars"] == 500.0

  @pytest.mark.asyncio
  async def test_portfolio_not_found_returns_404(self) -> None:
    body = CreatePositionRequest(
      portfolio_id="pf_missing",
      security_id="sec_01",
      quantity=1.0,
      cost_basis=0,
      currency="USD",
    )

    with (
      patch(
        f"{MODULE}.cmd_create_position",
        side_effect=PortfolioNotFoundError("pf_missing"),
      ),
      patch(f"{MODULE}.extensions_session", return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await create_position_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404
    assert "Portfolio not found" in exc.value.detail

  @pytest.mark.asyncio
  async def test_security_not_found_returns_404(self) -> None:
    body = CreatePositionRequest(
      portfolio_id="pf_01",
      security_id="sec_missing",
      quantity=1.0,
      cost_basis=0,
      currency="USD",
    )

    with (
      patch(
        f"{MODULE}.cmd_create_position",
        side_effect=SecurityNotFoundError("sec_missing"),
      ),
      patch(f"{MODULE}.extensions_session", return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await create_position_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404
    assert "Security not found" in exc.value.detail

  @pytest.mark.asyncio
  async def test_duplicate_active_position_returns_409(self) -> None:
    body = CreatePositionRequest(
      portfolio_id="pf_01",
      security_id="sec_01",
      quantity=1.0,
      cost_basis=0,
      currency="USD",
    )

    with (
      patch(
        f"{MODULE}.cmd_create_position",
        side_effect=DuplicateActivePositionError("already exists"),
      ),
      patch(f"{MODULE}.extensions_session", return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await create_position_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 409


class TestUpdatePositionOp:
  @pytest.mark.asyncio
  async def test_strips_position_id_from_updates(self) -> None:
    body = UpdatePositionOperation(position_id="pos_01", notes="new note")

    with patch(f"{MODULE}.cmd_update_position", return_value=_make_position()) as m:
      with patch(f"{MODULE}.extensions_session", return_value=_mock_session_ctx()[0]):
        await update_position_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )

    _session, pos_id, updates = m.call_args[0]
    assert pos_id == "pos_01"
    assert "position_id" not in updates
    assert updates == {"notes": "new note"}


class TestDeletePositionOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = DeletePositionOperation(position_id="pos_01")

    with (
      patch(f"{MODULE}.cmd_soft_delete_position", return_value=True),
      patch(f"{MODULE}.extensions_session", return_value=_mock_session_ctx()[0]),
    ):
      env = await delete_position_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert env.result == {"deleted": True}

  @pytest.mark.asyncio
  async def test_not_found_returns_404(self) -> None:
    body = DeletePositionOperation(position_id="pos_missing")

    with (
      patch(f"{MODULE}.cmd_soft_delete_position", return_value=False),
      patch(f"{MODULE}.extensions_session", return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await delete_position_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404
