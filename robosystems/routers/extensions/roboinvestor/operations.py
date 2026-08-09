"""RoboInvestor operation routes.

All operations are declared on a single `OperationRegistrar`. The
registrar mounts each spec as a `POST /extensions/roboinvestor/{graph_id}
/operations/{op_name}` route, wires in the auth dependency + feature
gate, translates domain exceptions via each spec's `error_map`, and
emits the `OperationEnvelope` + audit trail. MCP tools for every spec
are auto-generated via `MCPRegistrar` — no MCP-specific code lives
here.

Portfolio and position writes flow through the Portfolio Block envelope ops
(`create-portfolio-block`, `update-portfolio-block`,
`delete-portfolio-block`) — there is no atom-level CRUD on portfolios or
positions. Securities are Master Data CRUD.
"""

from __future__ import annotations

from typing import cast

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
  CreatePortfolioBlockRequest,
  CreateSecurityRequest,
  DeletePortfolioBlockOperation,
  DeletePortfolioBlockResponse,
  DeleteResult,
  DeleteSecurityOperation,
  PortfolioBlockEnvelope,
  SecurityResponse,
  UpdatePortfolioBlockOperation,
  UpdateSecurityOperation,
)
from robosystems.operations.roboinvestor.commands.portfolio_block import (
  ActivePositionsRequireConfirmationError,
  DuplicateActivePositionError,
  PortfolioNotFoundError,
  PositionNotFoundError,
  PositionPortfolioMismatchError,
  SecurityNotFoundError,
)
from robosystems.operations.roboinvestor.commands.portfolio_block import (
  create_portfolio_block as cmd_create_portfolio_block,
)
from robosystems.operations.roboinvestor.commands.portfolio_block import (
  delete_portfolio_block as cmd_delete_portfolio_block,
)
from robosystems.operations.roboinvestor.commands.portfolio_block import (
  update_portfolio_block as cmd_update_portfolio_block,
)
from robosystems.operations.roboinvestor.commands.securities import (
  EntityNotFoundError,
)
from robosystems.operations.roboinvestor.commands.securities import (
  SecurityNotFoundError as SecurityMasterNotFoundError,
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


# ── Portfolio Block ───────────────────────────────────────────────────────

create_portfolio_block_op = _registrar.register(
  OperationSpec(
    name="create-portfolio-block",
    summary="Create Portfolio Block",
    description=(
      "Create a portfolio with optional initial positions in a single "
      "atomic envelope. Each position references an existing security; "
      "this operation never mints securities (use `create-security`). "
      "Whole envelope validates before any write."
    ),
    command=cmd_create_portfolio_block,
    request_model=CreatePortfolioBlockRequest,
    result_type=PortfolioBlockEnvelope,
    error_map={
      SecurityNotFoundError: (404, lambda e: f"Security not found: {e}"),
      DuplicateActivePositionError: (409, str),
    },
    mark_stale_reason="portfolio_block_created",
  )
)

update_portfolio_block_op = _registrar.register(
  OperationSpec(
    name="update-portfolio-block",
    summary="Update Portfolio Block",
    description=(
      "Patch portfolio fields and apply position deltas (`add` / "
      "`update` / `dispose`) atomically. Partial failures roll back."
    ),
    command=cmd_update_portfolio_block,
    request_model=UpdatePortfolioBlockOperation,
    result_type=PortfolioBlockEnvelope,
    error_map={
      PortfolioNotFoundError: (404, lambda _e: "Portfolio not found."),
      PositionNotFoundError: (404, lambda e: f"Position not found: {e}"),
      PositionPortfolioMismatchError: (409, str),
      SecurityNotFoundError: (404, lambda e: f"Security not found: {e}"),
      DuplicateActivePositionError: (409, str),
    },
    mark_stale_reason="portfolio_block_updated",
  )
)

delete_portfolio_block_op = _registrar.register(
  OperationSpec(
    name="delete-portfolio-block",
    summary="Delete Portfolio Block",
    description=(
      "Cascade-delete the portfolio plus all of its positions. When "
      "active positions exist, the request must include "
      "`confirm_active_positions: true` — safety belt to prevent "
      "accidental cascade. Disposed-only portfolios delete without "
      "the flag."
    ),
    command=cmd_delete_portfolio_block,
    request_model=DeletePortfolioBlockOperation,
    result_type=DeletePortfolioBlockResponse,
    error_map={
      PortfolioNotFoundError: (404, lambda _e: "Portfolio not found."),
      ActivePositionsRequireConfirmationError: (
        409,
        lambda e: (
          f"Portfolio has "
          f"{cast(ActivePositionsRequireConfirmationError, e).active_count} "
          "active position(s); set confirm_active_positions=true to cascade-delete."
        ),
      ),
    },
    mark_stale_reason="portfolio_block_deleted",
    requires_created_by=False,
  )
)


# ── Security (Master Data CRUD) ───────────────────────────────────────────

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
    result_type=SecurityResponse,
    error_map={EntityNotFoundError: (404, lambda _e: "Entity not found.")},
    mark_stale_reason="security_created",
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
    result_type=SecurityResponse,
    error_map={SecurityMasterNotFoundError: (404, lambda _e: "Security not found.")},
    mark_stale_reason="security_updated",
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
    result_type=DeleteResult,
    error_map={SecurityMasterNotFoundError: (404, lambda _e: "Security not found.")},
    mark_stale_reason="security_deleted",
    requires_created_by=False,
  )
)
