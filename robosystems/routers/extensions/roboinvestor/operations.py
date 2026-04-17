"""RoboInvestor operation routes.

All 9 operations are declared on a single `OperationRegistrar`. The
registrar mounts each spec as a `POST /extensions/roboinvestor/{graph_id}
/operations/{op_name}` route, wires in the auth dependency + feature
gate, translates domain exceptions via each spec's `error_map`, and
emits the `OperationEnvelope` + audit trail. MCP tools for every spec
are auto-generated via `MCPRegistrar` — no MCP-specific code lives
here.

To add a new RoboInvestor operation: write the ops command, add a
Pydantic request model to `models/api/extensions/investor.py`, and
declare an `OperationSpec` here.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from robosystems.db.extensions import extensions_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.extensions import (
  OperationRegistrar,
  OperationSpec,
)
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.operations import (
  IdempotencyCache,
  IdempotencyKeyConflictError,
  OperationContext,
  OperationEnvelope,
  execute_operation,
  fingerprint_body,
)
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.extensions.investor import (
  CreatePortfolioRequest,
  CreatePositionRequest,
  CreateSecurityRequest,
  DeletePortfolioOperation,
  DeletePositionOperation,
  DeleteSecurityOperation,
  UpdatePortfolioOperation,
  UpdatePositionOperation,
  UpdateSecurityOperation,
)
from robosystems.operations.roboinvestor.commands.portfolios import (
  PortfolioHasActivePositionsError,
  PortfolioNotFoundError,
)
from robosystems.operations.roboinvestor.commands.portfolios import (
  create_portfolio as cmd_create_portfolio,
)
from robosystems.operations.roboinvestor.commands.portfolios import (
  delete_portfolio as cmd_delete_portfolio,
)
from robosystems.operations.roboinvestor.commands.portfolios import (
  update_portfolio as cmd_update_portfolio,
)
from robosystems.operations.roboinvestor.commands.positions import (
  DuplicateActivePositionError,
  PositionNotFoundError,
)
from robosystems.operations.roboinvestor.commands.positions import (
  create_position as cmd_create_position,
)
from robosystems.operations.roboinvestor.commands.positions import (
  soft_delete_position as cmd_soft_delete_position,
)
from robosystems.operations.roboinvestor.commands.positions import (
  update_position as cmd_update_position,
)
from robosystems.operations.roboinvestor.commands.securities import (
  EntityNotFoundError,
  SecurityNotFoundError,
)
from robosystems.operations.roboinvestor.commands.securities import (
  create_security as cmd_create_security,
)
from robosystems.operations.roboinvestor.commands.securities import (
  soft_delete_security as cmd_soft_delete_security,
)
from robosystems.operations.roboinvestor.commands.securities import (
  update_security as cmd_update_security,
)

router = APIRouter()

_OP_TAG = "Extensions: RoboInvestor"
_RATE_LIMIT = Depends(subscription_aware_rate_limit_dependency)


def _not_initialized_404() -> HTTPException:
  """Registrar surfaces this when the extensions schema hasn't materialized."""
  return HTTPException(status_code=404, detail="Investor module not initialized.")


def _ctx(
  *,
  graph_id: str,
  user_id: str,
  op: str,
  idempotency_key: str | None,
  body: object,
) -> OperationContext:
  return OperationContext(
    domain="roboinvestor",
    operation_name=op,
    graph_id=graph_id,
    user_id=user_id,
    idempotency_key=idempotency_key,
    body_fingerprint=fingerprint_body(body),
  )


async def _dispatch(
  ctx: OperationContext,
  runner,
  cache: IdempotencyCache,
  on_fresh_success=None,
) -> OperationEnvelope:
  """Run `execute_operation` and translate idempotency conflicts to 409."""
  try:
    return await execute_operation(
      ctx, runner, idempotency_cache=cache, on_fresh_success=on_fresh_success
    )
  except IdempotencyKeyConflictError as exc:
    raise HTTPException(status_code=409, detail=str(exc))


_registrar = OperationRegistrar(
  router=router,
  domain="roboinvestor",
  tag=_OP_TAG,
  rate_limit_dep=_RATE_LIMIT,
  ctx_builder=_ctx,
  dispatcher=_dispatch,
  session_factory=extensions_session,
  schema_missing_404=_not_initialized_404,
  user_dep=get_current_user_with_graph,
  graph_id_pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  extension="roboinvestor",
)


# ── Portfolio ─────────────────────────────────────────────────────────────

create_portfolio_op = _registrar.register(
  OperationSpec(
    name="create-portfolio",
    summary="Create Portfolio",
    description=(
      "Create an investment portfolio within this graph. Portfolios are "
      "logical groupings of securities (stocks, notes, warrants) owned by "
      "the entity; subsequent positions attach to the portfolio."
    ),
    command=cmd_create_portfolio,
    request_model=CreatePortfolioRequest,
  )
)

update_portfolio_op = _registrar.register(
  OperationSpec(
    name="update-portfolio",
    summary="Update Portfolio",
    description=(
      "Update mutable fields on a portfolio (name, description, strategy, "
      "inception_date, base_currency). Unset fields are ignored — "
      "partial updates are explicit."
    ),
    command=cmd_update_portfolio,
    request_model=UpdatePortfolioOperation,
    error_map={PortfolioNotFoundError: (404, lambda _e: "Portfolio not found.")},
    requires_created_by=False,
  )
)

delete_portfolio_op = _registrar.register(
  OperationSpec(
    name="delete-portfolio",
    summary="Delete Portfolio",
    description=(
      "Hard-delete a portfolio. Returns 409 if the portfolio has active "
      "positions — dispose (soft-delete) them first."
    ),
    command=cmd_delete_portfolio,
    request_model=DeletePortfolioOperation,
    error_map={
      PortfolioNotFoundError: (404, lambda _e: "Portfolio not found."),
      PortfolioHasActivePositionsError: (
        409,
        lambda e: (  # type: ignore[attr-defined]
          f"Portfolio has {e.active_count} active position(s). Dispose them first."
        ),
      ),
    },
    requires_created_by=False,
  )
)

# ── Security ──────────────────────────────────────────────────────────────

create_security_op = _registrar.register(
  OperationSpec(
    name="create-security",
    summary="Create Security",
    description=(
      "Register a security (common stock, preferred stock, warrant, "
      "convertible note, etc.) owned by this graph's entity. Optionally "
      "cross-links to an issuing entity in another graph via "
      "`source_graph_id` for mutual-handshake attribution."
    ),
    command=cmd_create_security,
    request_model=CreateSecurityRequest,
    error_map={EntityNotFoundError: (404, lambda _e: "Entity not found.")},
  )
)

update_security_op = _registrar.register(
  OperationSpec(
    name="update-security",
    summary="Update Security",
    description=(
      "Update mutable fields on a security (name, type, subtype, terms, "
      "share counts, entity linkage). Unset fields are ignored."
    ),
    command=cmd_update_security,
    request_model=UpdateSecurityOperation,
    error_map={SecurityNotFoundError: (404, lambda _e: "Security not found.")},
    requires_created_by=False,
  )
)

delete_security_op = _registrar.register(
  OperationSpec(
    name="delete-security",
    summary="Delete Security",
    description=(
      "Soft-delete the security (`is_active=false`). Historical positions "
      "referencing it remain valid."
    ),
    command=cmd_soft_delete_security,
    request_model=DeleteSecurityOperation,
    error_map={SecurityNotFoundError: (404, lambda _e: "Security not found.")},
    requires_created_by=False,
  )
)

# ── Position ──────────────────────────────────────────────────────────────

create_position_op = _registrar.register(
  OperationSpec(
    name="create-position",
    summary="Create Position",
    description=(
      "Open a position in a security within a portfolio. One active "
      "position per (portfolio, security) pair — creating a second "
      "active position returns 409. Dispose an existing position via "
      "`delete-position` before opening a new one."
    ),
    command=cmd_create_position,
    request_model=CreatePositionRequest,
    error_map={
      PortfolioNotFoundError: (404, lambda _e: "Portfolio not found."),
      SecurityNotFoundError: (404, lambda _e: "Security not found."),
      DuplicateActivePositionError: (409, lambda e: str(e)),
    },
  )
)

update_position_op = _registrar.register(
  OperationSpec(
    name="update-position",
    summary="Update Position",
    description=(
      "Update mutable fields on a position (quantity, cost basis, "
      "valuation, notes, status). Use `delete-position` for soft disposal "
      "instead of setting `status` manually."
    ),
    command=cmd_update_position,
    request_model=UpdatePositionOperation,
    error_map={PositionNotFoundError: (404, lambda _e: "Position not found.")},
    requires_created_by=False,
  )
)

delete_position_op = _registrar.register(
  OperationSpec(
    name="delete-position",
    summary="Delete Position",
    description=(
      "Soft-delete the position (`status='disposed'`). Historical holding "
      "records referencing it remain valid."
    ),
    command=cmd_soft_delete_position,
    request_model=DeletePositionOperation,
    error_map={PositionNotFoundError: (404, lambda _e: "Position not found.")},
    requires_created_by=False,
  )
)
