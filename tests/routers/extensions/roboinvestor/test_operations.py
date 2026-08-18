"""Tests for the roboinvestor operation routes.

All RoboInvestor ops are registrar-generated from `OperationSpec`
declarations. The registrar late-binds commands through `sys.modules`,
so tests patch at the command's origin module path — not the router
module. That mirrors the roboledger registrar tests.

Phase a-prime replaced atom-level portfolio + position CRUD with the
Portfolio Block envelope ops; security ops stay as Master Data CRUD.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.middleware.operations import OperationEnvelope
from robosystems.models.api.extensions.investor import (
  CreatePortfolioBlockRequest,
  CreateSecurityRequest,
  DeletePortfolioBlockOperation,
  DeletePortfolioBlockResponse,
  DeleteResult,
  DeleteSecurityOperation,
  PortfolioBlockEnvelope,
  PortfolioBlockPortfolioFields,
  PortfolioBlockPortfolioPatch,
  PortfolioBlockPositionAdd,
  PortfolioBlockPositionDispose,
  PortfolioBlockPositions,
  PortfolioBlockPositionUpdate,
  SecurityResponse,
  UpdatePortfolioBlockOperation,
  UpdateSecurityOperation,
)
from robosystems.operations.roboinvestor.commands.portfolio_block import (
  ActivePositionsRequireConfirmationError,
  PortfolioNotFoundError,
  PositionNotFoundError,
  SecurityNotFoundError,
)
from robosystems.operations.roboinvestor.commands.securities import (
  EntityNotFoundError,
)
from robosystems.operations.roboinvestor.commands.securities import (
  SecurityNotFoundError as SecurityMasterNotFoundError,
)
from robosystems.routers.extensions.roboinvestor.operations import (
  create_portfolio_block_op,
  create_security_op,
  delete_portfolio_block_op,
  delete_security_op,
  update_portfolio_block_op,
  update_security_op,
)

GRAPH_ID = "kg01234567890abcdef"


@pytest.fixture(autouse=True)
def _bypass_write_role():
  """Registrar handlers enforce the member/admin write role via
  ``require_graph_write_role`` (unit-tested in
  ``tests/middleware/auth/test_dependencies.py``). These direct-call wiring
  tests use a mock user with no DB-backed ``GraphUser`` row, so no-op it."""
  with patch(
    "robosystems.middleware.extensions.require_graph_write_role", return_value=None
  ):
    yield


# Command-origin patch paths (registrar resolves commands via sys.modules).
PORTFOLIO_BLOCK = "robosystems.operations.roboinvestor.commands.portfolio_block"
SECURITIES = "robosystems.operations.roboinvestor.commands.securities"

# Session factory patch path — the registrar resolves the session factory
# through `sys.modules` too, so the patch must target the origin module.
SESSION_FACTORY = "robosystems.db.extensions.extensions_session"


def _entity_meta(schema_extensions=("roboinvestor",), graph_type="entity"):
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


def _make_envelope(
  *,
  portfolio_id: str = "pf_01",
  position_count: int = 0,
) -> PortfolioBlockEnvelope:
  return PortfolioBlockEnvelope(
    id=portfolio_id,
    name="Main Fund",
    base_currency="USD",
    positions=[],
    total_cost_basis_dollars=0.0,
    total_current_value_dollars=None,
    active_position_count=position_count,
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


# ────────────────────────────────────────────────────────────────────
# Portfolio Block — create
# ────────────────────────────────────────────────────────────────────


class TestCreatePortfolioBlockOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = CreatePortfolioBlockRequest(
      portfolio=PortfolioBlockPortfolioFields(name="New Fund"),
      positions=[],
    )

    with (
      patch(
        f"{PORTFOLIO_BLOCK}.create_portfolio_block",
        return_value=_make_envelope(),
      ),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      env = await create_portfolio_block_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        _ext=_entity_meta(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert isinstance(env, OperationEnvelope)
    assert env.operation == "create-portfolio-block"
    assert env.status == "completed"
    assert env.result["id"] == "pf_01"

  @pytest.mark.asyncio
  async def test_unknown_security_returns_404(self) -> None:
    body = CreatePortfolioBlockRequest(
      portfolio=PortfolioBlockPortfolioFields(name="Fund"),
      positions=[
        PortfolioBlockPositionAdd(security_id="sec_missing", quantity=1.0, cost_basis=0)
      ],
    )

    with (
      patch(
        f"{PORTFOLIO_BLOCK}.create_portfolio_block",
        side_effect=SecurityNotFoundError("sec_missing"),
      ),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await create_portfolio_block_op(
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
  async def test_schema_missing_returns_404(self) -> None:
    from sqlalchemy.exc import ProgrammingError

    body = CreatePortfolioBlockRequest(
      portfolio=PortfolioBlockPortfolioFields(name="X"),
    )

    with patch(
      SESSION_FACTORY,
      side_effect=ProgrammingError(
        "stmt", {}, Exception('relation "portfolios" does not exist')
      ),
    ):
      with pytest.raises(HTTPException) as exc:
        await create_portfolio_block_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          _ext=_entity_meta(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404
    assert "not initialized" in exc.value.detail

  @pytest.mark.asyncio
  async def test_other_programming_errors_surface(self) -> None:
    """Only a missing schema/relation is "not initialized". A migration that
    has not reached this tenant, or a SQL fault, must not hide behind the
    friendly 404 — it surfaces (500) and is logged."""
    from sqlalchemy.exc import ProgrammingError

    body = CreatePortfolioBlockRequest(
      portfolio=PortfolioBlockPortfolioFields(name="X"),
    )
    fault = ProgrammingError(
      "stmt", {}, Exception("column portfolios.tags does not exist")
    )
    with (
      patch(f"{PORTFOLIO_BLOCK}.create_portfolio_block", side_effect=fault),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(ProgrammingError):
        await create_portfolio_block_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          _ext=_entity_meta(),
          idempotency_key=None,
          cache=_FakeCache(),
        )

  @pytest.mark.asyncio
  async def test_unmapped_value_error_is_a_422_not_a_404(self) -> None:
    """A domain refusal the error_map did not name is a validation failure
    with its own message, not "not initialized"."""
    body = CreatePortfolioBlockRequest(
      portfolio=PortfolioBlockPortfolioFields(name="X"),
    )
    with (
      patch(
        f"{PORTFOLIO_BLOCK}.create_portfolio_block",
        side_effect=ValueError("quantity must be positive"),
      ),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await create_portfolio_block_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          _ext=_entity_meta(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 422
    assert exc.value.detail == "quantity must be positive"

  @pytest.mark.asyncio
  async def test_graph_id_the_factory_rejects_is_a_404(self) -> None:
    """The session factory refuses ids that are not tenant-schema ids; that
    is still the "not initialized" answer, told apart from a command's own
    ValueError by whether the session was ever bound."""
    body = CreatePortfolioBlockRequest(
      portfolio=PortfolioBlockPortfolioFields(name="X"),
    )
    with patch(
      SESSION_FACTORY, side_effect=ValueError("Invalid graph_id for schema name")
    ):
      with pytest.raises(HTTPException) as exc:
        await create_portfolio_block_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          _ext=_entity_meta(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404
    assert "not initialized" in exc.value.detail


# ────────────────────────────────────────────────────────────────────
# Portfolio Block — update
# ────────────────────────────────────────────────────────────────────


class TestUpdatePortfolioBlockOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = UpdatePortfolioBlockOperation(
      portfolio_id="pf_01",
      portfolio=PortfolioBlockPortfolioPatch(name="Renamed"),
      positions=PortfolioBlockPositions(
        add=[
          PortfolioBlockPositionAdd(
            security_id="sec_01", quantity=10.0, cost_basis=5000
          )
        ],
        update=[PortfolioBlockPositionUpdate(id="pos_01", notes="upd")],
        dispose=[
          PortfolioBlockPositionDispose(id="pos_old", disposition_reason="sold")
        ],
      ),
    )

    with (
      patch(
        f"{PORTFOLIO_BLOCK}.update_portfolio_block",
        return_value=_make_envelope(),
      ) as m,
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      env = await update_portfolio_block_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        _ext=_entity_meta(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    _session, passed_body = m.call_args[0]
    assert passed_body.portfolio_id == "pf_01"
    assert passed_body.portfolio.name == "Renamed"
    assert len(passed_body.positions.add) == 1
    assert len(passed_body.positions.update) == 1
    assert len(passed_body.positions.dispose) == 1
    assert env.status == "completed"

  @pytest.mark.asyncio
  async def test_portfolio_not_found_returns_404(self) -> None:
    body = UpdatePortfolioBlockOperation(portfolio_id="pf_missing")

    with (
      patch(
        f"{PORTFOLIO_BLOCK}.update_portfolio_block",
        side_effect=PortfolioNotFoundError("pf_missing"),
      ),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await update_portfolio_block_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          _ext=_entity_meta(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404

  @pytest.mark.asyncio
  async def test_position_not_found_returns_404(self) -> None:
    body = UpdatePortfolioBlockOperation(
      portfolio_id="pf_01",
      positions=PortfolioBlockPositions(
        update=[PortfolioBlockPositionUpdate(id="pos_missing")]
      ),
    )

    with (
      patch(
        f"{PORTFOLIO_BLOCK}.update_portfolio_block",
        side_effect=PositionNotFoundError("pos_missing"),
      ),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await update_portfolio_block_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          _ext=_entity_meta(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404


# ────────────────────────────────────────────────────────────────────
# Portfolio Block — delete
# ────────────────────────────────────────────────────────────────────


class TestDeletePortfolioBlockOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = DeletePortfolioBlockOperation(
      portfolio_id="pf_01", confirm_active_positions=True
    )

    with (
      patch(
        f"{PORTFOLIO_BLOCK}.delete_portfolio_block",
        return_value=DeletePortfolioBlockResponse(
          deleted=True, portfolio_id="pf_01", positions_deleted=2
        ),
      ),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      env = await delete_portfolio_block_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        _ext=_entity_meta(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert env.result["deleted"] is True
    assert env.result["positions_deleted"] == 2

  @pytest.mark.asyncio
  async def test_active_positions_without_confirm_returns_409(self) -> None:
    body = DeletePortfolioBlockOperation(portfolio_id="pf_01")

    with (
      patch(
        f"{PORTFOLIO_BLOCK}.delete_portfolio_block",
        side_effect=ActivePositionsRequireConfirmationError(3),
      ),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await delete_portfolio_block_op(
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
    body = DeletePortfolioBlockOperation(portfolio_id="pf_x")

    with (
      patch(
        f"{PORTFOLIO_BLOCK}.delete_portfolio_block",
        side_effect=PortfolioNotFoundError("pf_x"),
      ),
      patch(SESSION_FACTORY, return_value=_mock_session_ctx()[0]),
    ):
      with pytest.raises(HTTPException) as exc:
        await delete_portfolio_block_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          _ext=_entity_meta(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404


# ────────────────────────────────────────────────────────────────────
# Security (Master Data CRUD — unchanged)
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
        side_effect=SecurityMasterNotFoundError("sec_missing"),
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
