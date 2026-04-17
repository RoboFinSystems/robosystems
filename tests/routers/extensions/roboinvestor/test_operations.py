"""Tests for the roboinvestor operation routes.

All 9 RoboInvestor ops are registrar-generated from `OperationSpec`
declarations. The registrar late-binds commands through `sys.modules`,
so tests patch at the command's origin module path — not the router
module. That mirrors the roboledger registrar tests.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.middleware.operations import OperationEnvelope
from robosystems.models.api.extensions.investor import (
  CreatePortfolioRequest,
  CreatePositionRequest,
  CreateSecurityRequest,
  DeletePortfolioOperation,
  DeletePositionOperation,
  DeleteResult,
  DeleteSecurityOperation,
  PortfolioResponse,
  PositionResponse,
  SecurityResponse,
  UpdatePortfolioOperation,
  UpdatePositionOperation,
  UpdateSecurityOperation,
)
from robosystems.operations.roboinvestor.commands.portfolios import (
  PortfolioHasActivePositionsError,
  PortfolioNotFoundError,
)
from robosystems.operations.roboinvestor.commands.positions import (
  DuplicateActivePositionError,
  PositionNotFoundError,
)
from robosystems.operations.roboinvestor.commands.securities import (
  EntityNotFoundError,
  SecurityNotFoundError,
)
from robosystems.routers.extensions.roboinvestor.operations import (
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

# Command-origin patch paths (registrar resolves commands via sys.modules).
PORTFOLIOS = "robosystems.operations.roboinvestor.commands.portfolios"
SECURITIES = "robosystems.operations.roboinvestor.commands.securities"
POSITIONS = "robosystems.operations.roboinvestor.commands.positions"

# Session factory patch path — the registrar resolves the session factory
# through `sys.modules` too, so the patch must target the origin module.
SESSION_FACTORY = "robosystems.db.extensions.extensions_session"


def _entity_meta(schema_extensions=("roboinvestor",), graph_type="entity"):
  """Stub `GraphExtensionContext` the extension gate returns for a normal
  entity graph provisioned for roboinvestor."""
  from robosystems.middleware.extensions import GraphExtensionContext

  return GraphExtensionContext(
    graph_type=graph_type,
    schema_extensions=tuple(schema_extensions),
    is_repository=False,
  )


def _make_user() -> MagicMock:
  user = MagicMock()
  user.id = "usr_test123"
  return user


class _FakeCache:
  """In-memory idempotency cache matching the real signature."""

  def __init__(self) -> None:
    self.store: dict = {}

  async def get(
    self, user_id, graph_id, operation_name, idempotency_key, body_fingerprint
  ):
    from robosystems.middleware.operations import IdempotencyKeyConflictError

    entry = self.store.get((user_id, graph_id, operation_name, idempotency_key))
    if entry is None:
      return None
    cached_envelope, cached_fp = entry
    if cached_fp != body_fingerprint:
      raise IdempotencyKeyConflictError(operation_name)
    return cached_envelope

  async def put(
    self,
    user_id,
    graph_id,
    operation_name,
    idempotency_key,
    envelope,
    body_fingerprint,
    ttl_seconds=86400,
  ):
    self.store[(user_id, graph_id, operation_name, idempotency_key)] = (
      envelope,
      body_fingerprint,
    )


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
      patch(f"{PORTFOLIOS}.create_portfolio", return_value=_make_portfolio()),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      env = await create_portfolio_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        _ext=_entity_meta(),
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
      SESSION_FACTORY,
      side_effect=ProgrammingError("stmt", {}, Exception("schema missing")),
    ):
      with pytest.raises(HTTPException) as exc:
        await create_portfolio_op(
          body=CreatePortfolioRequest(name="X"),
          graph_id=GRAPH_ID,
          user=_make_user(),
          _ext=_entity_meta(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404
    assert "not initialized" in exc.value.detail


class TestUpdatePortfolioOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = UpdatePortfolioOperation(portfolio_id="pf_01", name="Renamed")

    with (
      patch(f"{PORTFOLIOS}.update_portfolio", return_value=_make_portfolio()) as m,
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      env = await update_portfolio_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        _ext=_entity_meta(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    # Registrar calls `update_portfolio(session, body)` — `body` carries
    # portfolio_id, the command pulls it out.
    _session, passed_body = m.call_args[0]
    assert passed_body.portfolio_id == "pf_01"
    assert passed_body.name == "Renamed"
    assert env.status == "completed"

  @pytest.mark.asyncio
  async def test_not_found_returns_404(self) -> None:
    body = UpdatePortfolioOperation(portfolio_id="pf_missing", name="X")

    with (
      patch(
        f"{PORTFOLIOS}.update_portfolio",
        side_effect=PortfolioNotFoundError("pf_missing"),
      ),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await update_portfolio_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          _ext=_entity_meta(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404


class TestDeletePortfolioOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = DeletePortfolioOperation(portfolio_id="pf_01")

    with (
      patch(f"{PORTFOLIOS}.delete_portfolio", return_value=DeleteResult(deleted=True)),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      env = await delete_portfolio_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        _ext=_entity_meta(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert env.result == {"deleted": True}

  @pytest.mark.asyncio
  async def test_has_active_positions_returns_409(self) -> None:
    body = DeletePortfolioOperation(portfolio_id="pf_01")

    with (
      patch(
        f"{PORTFOLIOS}.delete_portfolio",
        side_effect=PortfolioHasActivePositionsError(3),
      ),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await delete_portfolio_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          _ext=_entity_meta(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 409
    assert "3 active position" in exc.value.detail

  @pytest.mark.asyncio
  async def test_not_found_returns_404(self) -> None:
    body = DeletePortfolioOperation(portfolio_id="pf_x")

    with (
      patch(
        f"{PORTFOLIOS}.delete_portfolio",
        side_effect=PortfolioNotFoundError("pf_x"),
      ),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await delete_portfolio_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          _ext=_entity_meta(),
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
      patch(f"{SECURITIES}.create_security", return_value=_make_security()),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      env = await create_security_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        _ext=_entity_meta(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert env.operation == "create-security"
    assert env.result["id"] == "sec_01"

  @pytest.mark.asyncio
  async def test_entity_not_found_returns_404(self) -> None:
    body = CreateSecurityRequest(
      entity_id="ent_missing",
      name="Bad",
      security_type="equity",
      security_subtype="common",
    )

    with (
      patch(
        f"{SECURITIES}.create_security",
        side_effect=EntityNotFoundError("ent_missing"),
      ),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await create_security_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          _ext=_entity_meta(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404
    assert "Entity not found" in exc.value.detail


class TestUpdateSecurityOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = UpdateSecurityOperation(security_id="sec_01", name="Renamed")

    with (
      patch(f"{SECURITIES}.update_security", return_value=_make_security()) as m,
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      await update_security_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        _ext=_entity_meta(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    _session, passed_body = m.call_args[0]
    assert passed_body.security_id == "sec_01"
    assert passed_body.name == "Renamed"

  @pytest.mark.asyncio
  async def test_not_found_returns_404(self) -> None:
    body = UpdateSecurityOperation(security_id="sec_missing", name="X")
    with (
      patch(
        f"{SECURITIES}.update_security",
        side_effect=SecurityNotFoundError("sec_missing"),
      ),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await update_security_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          _ext=_entity_meta(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404


class TestDeleteSecurityOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = DeleteSecurityOperation(security_id="sec_01")

    with (
      patch(
        f"{SECURITIES}.soft_delete_security", return_value=DeleteResult(deleted=True)
      ),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      env = await delete_security_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        _ext=_entity_meta(),
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
      patch(f"{POSITIONS}.create_position", return_value=_make_position()),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      env = await create_position_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        _ext=_entity_meta(),
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
        f"{POSITIONS}.create_position",
        side_effect=PortfolioNotFoundError("pf_missing"),
      ),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await create_position_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          _ext=_entity_meta(),
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
        f"{POSITIONS}.create_position",
        side_effect=SecurityNotFoundError("sec_missing"),
      ),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await create_position_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          _ext=_entity_meta(),
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
        f"{POSITIONS}.create_position",
        side_effect=DuplicateActivePositionError("already exists"),
      ),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await create_position_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          _ext=_entity_meta(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 409


class TestUpdatePositionOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = UpdatePositionOperation(position_id="pos_01", notes="new note")

    with (
      patch(f"{POSITIONS}.update_position", return_value=_make_position()) as m,
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      await update_position_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        _ext=_entity_meta(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    _session, passed_body = m.call_args[0]
    assert passed_body.position_id == "pos_01"
    assert passed_body.notes == "new note"


class TestDeletePositionOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = DeletePositionOperation(position_id="pos_01")

    with (
      patch(
        f"{POSITIONS}.soft_delete_position", return_value=DeleteResult(deleted=True)
      ),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      env = await delete_position_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        _ext=_entity_meta(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert env.result == {"deleted": True}

  @pytest.mark.asyncio
  async def test_not_found_returns_404(self) -> None:
    body = DeletePositionOperation(position_id="pos_missing")

    with (
      patch(
        f"{POSITIONS}.soft_delete_position",
        side_effect=PositionNotFoundError("pos_missing"),
      ),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await delete_position_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          _ext=_entity_meta(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404
