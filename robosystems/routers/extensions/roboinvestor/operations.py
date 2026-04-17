"""RoboInvestor operation routes.

Every command endpoint is:
1. A `POST /extensions/roboinvestor/{graph_id}/operations/{op_name}` route
2. Typed request body (path params for resource IDs go in the body —
   see the `*Operation` models below)
3. A `_runner()` closure that opens an extensions session, calls the
   ops layer (`operations/roboinvestor/commands/*`), and translates
   domain exceptions into HTTPExceptions
4. `execute_operation(ctx, runner, cache)` handles envelope wrapping,
   idempotency, and audit logging

New commands are added by:
1. Defining a request model for the operation body
2. Writing a `_runner()` closure that calls the ops layer
3. Adding the route stanza below
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Path
from pydantic import BaseModel
from sqlalchemy.exc import ProgrammingError

from robosystems.db.extensions import extensions_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.extensions import (
  GraphExtensionContext,
  require_graph_extension,
)
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.operations import (
  IdempotencyCache,
  IdempotencyKeyConflictError,
  OperationContext,
  OperationEnvelope,
  execute_operation,
  fingerprint_body,
  get_idempotency_cache,
)
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import OPERATION_ERROR_RESPONSES
from robosystems.models.api.extensions.investor import (
  CreatePortfolioRequest,
  CreatePositionRequest,
  CreateSecurityRequest,
  UpdatePortfolioRequest,
  UpdatePositionRequest,
  UpdateSecurityRequest,
)
from robosystems.models.core import User
from robosystems.operations.roboinvestor.commands.portfolios import (
  PortfolioHasActivePositionsError,
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
  PortfolioNotFoundError,
  SecurityNotFoundError,
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

# Feature-gate dep: rejects repository graphs and graphs that don't
# list "roboinvestor" in their schema_extensions. Shared across every
# hand-written endpoint below so FastAPI can cache the resolution.
_require_roboinvestor = require_graph_extension("roboinvestor")


# ───────────────────────────────────────────────────────────────────────────
# Operation request bodies — wrap resource IDs into the request body so
# every operation URL has the same shape: `POST /operations/{op_name}`.
# ───────────────────────────────────────────────────────────────────────────


class UpdatePortfolioOperation(UpdatePortfolioRequest):
  portfolio_id: str


class DeletePortfolioOperation(BaseModel):
  portfolio_id: str


class UpdateSecurityOperation(UpdateSecurityRequest):
  security_id: str


class DeleteSecurityOperation(BaseModel):
  security_id: str


class UpdatePositionOperation(UpdatePositionRequest):
  position_id: str


class DeletePositionOperation(BaseModel):
  position_id: str


class DeleteResult(BaseModel):
  """Return shape for soft-delete operations."""

  deleted: bool


# ───────────────────────────────────────────────────────────────────────────
# Shared helpers
# ───────────────────────────────────────────────────────────────────────────


def _not_initialized_404() -> HTTPException:
  return HTTPException(status_code=404, detail="Investor module not initialized.")


def _ctx(
  *,
  graph_id: str,
  user_id: str,
  op: str,
  idempotency_key: str | None,
  body: object,
) -> OperationContext:
  """Build the per-request operation context.

  Always computes the body fingerprint so the dispatcher can enforce
  Stripe-style idempotency semantics (replay on key+body match,
  conflict on key reuse with different body).
  """
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


# ───────────────────────────────────────────────────────────────────────────
# Portfolio operations
# ───────────────────────────────────────────────────────────────────────────


@router.post(
  "/create-portfolio",
  response_model=OperationEnvelope,
  operation_id="opCreatePortfolio",
  summary="Create Portfolio",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboinvestor/{graph_id}/operations/create-portfolio",
  method="POST",
  business_event_type="investor_create_portfolio",
)
async def create_portfolio_op(
  body: CreatePortfolioRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboinvestor),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="create-portfolio",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        return cmd_create_portfolio(session, body, created_by=str(user.id))
    except ProgrammingError:
      raise _not_initialized_404()

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/update-portfolio",
  response_model=OperationEnvelope,
  operation_id="opUpdatePortfolio",
  summary="Update Portfolio",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboinvestor/{graph_id}/operations/update-portfolio",
  method="POST",
  business_event_type="investor_update_portfolio",
)
async def update_portfolio_op(
  body: UpdatePortfolioOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboinvestor),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="update-portfolio",
    idempotency_key=idempotency_key,
    body=body,
  )
  updates = body.model_dump(exclude_unset=True, exclude={"portfolio_id"})

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        result = cmd_update_portfolio(session, body.portfolio_id, updates)
    except ProgrammingError:
      raise _not_initialized_404()
    if result is None:
      raise HTTPException(status_code=404, detail="Portfolio not found.")
    return result

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/delete-portfolio",
  response_model=OperationEnvelope,
  operation_id="opDeletePortfolio",
  summary="Delete Portfolio",
  description="Returns 409 if the portfolio has active positions — dispose them first.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboinvestor/{graph_id}/operations/delete-portfolio",
  method="POST",
  business_event_type="investor_delete_portfolio",
)
async def delete_portfolio_op(
  body: DeletePortfolioOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboinvestor),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="delete-portfolio",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        try:
          deleted = cmd_delete_portfolio(session, body.portfolio_id)
        except PortfolioHasActivePositionsError as exc:
          raise HTTPException(
            status_code=409,
            detail=f"Portfolio has {exc.active_count} active position(s). Dispose them first.",
          )
    except ProgrammingError:
      raise _not_initialized_404()
    if not deleted:
      raise HTTPException(status_code=404, detail="Portfolio not found.")
    return DeleteResult(deleted=True)

  return await _dispatch(ctx, _runner, cache)


# ───────────────────────────────────────────────────────────────────────────
# Security operations
# ───────────────────────────────────────────────────────────────────────────


@router.post(
  "/create-security",
  response_model=OperationEnvelope,
  operation_id="opCreateSecurity",
  summary="Create Security",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboinvestor/{graph_id}/operations/create-security",
  method="POST",
  business_event_type="investor_create_security",
)
async def create_security_op(
  body: CreateSecurityRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboinvestor),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="create-security",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        try:
          return cmd_create_security(session, body, created_by=str(user.id))
        except EntityNotFoundError:
          raise HTTPException(status_code=404, detail="Entity not found.")
    except ProgrammingError:
      raise _not_initialized_404()

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/update-security",
  response_model=OperationEnvelope,
  operation_id="opUpdateSecurity",
  summary="Update Security",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboinvestor/{graph_id}/operations/update-security",
  method="POST",
  business_event_type="investor_update_security",
)
async def update_security_op(
  body: UpdateSecurityOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboinvestor),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="update-security",
    idempotency_key=idempotency_key,
    body=body,
  )
  updates = body.model_dump(exclude_unset=True, exclude={"security_id"})

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        result = cmd_update_security(session, body.security_id, updates)
    except ProgrammingError:
      raise _not_initialized_404()
    if result is None:
      raise HTTPException(status_code=404, detail="Security not found.")
    return result

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/delete-security",
  response_model=OperationEnvelope,
  operation_id="opDeleteSecurity",
  summary="Delete Security",
  description="Soft-deletes the security (`is_active=false`). Historical positions referencing it remain valid.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboinvestor/{graph_id}/operations/delete-security",
  method="POST",
  business_event_type="investor_delete_security",
)
async def delete_security_op(
  body: DeleteSecurityOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboinvestor),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="delete-security",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        deleted = cmd_soft_delete_security(session, body.security_id)
    except ProgrammingError:
      raise _not_initialized_404()
    if not deleted:
      raise HTTPException(status_code=404, detail="Security not found.")
    return DeleteResult(deleted=True)

  return await _dispatch(ctx, _runner, cache)


# ───────────────────────────────────────────────────────────────────────────
# Position operations
# ───────────────────────────────────────────────────────────────────────────


@router.post(
  "/create-position",
  response_model=OperationEnvelope,
  operation_id="opCreatePosition",
  summary="Create Position",
  description="Returns 409 if an active position for this security already exists in the portfolio.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboinvestor/{graph_id}/operations/create-position",
  method="POST",
  business_event_type="investor_create_position",
)
async def create_position_op(
  body: CreatePositionRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboinvestor),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="create-position",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        try:
          return cmd_create_position(session, body, created_by=str(user.id))
        except PortfolioNotFoundError:
          raise HTTPException(status_code=404, detail="Portfolio not found.")
        except SecurityNotFoundError:
          raise HTTPException(status_code=404, detail="Security not found.")
        except DuplicateActivePositionError as exc:
          raise HTTPException(status_code=409, detail=str(exc))
    except ProgrammingError:
      raise _not_initialized_404()

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/update-position",
  response_model=OperationEnvelope,
  operation_id="opUpdatePosition",
  summary="Update Position",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboinvestor/{graph_id}/operations/update-position",
  method="POST",
  business_event_type="investor_update_position",
)
async def update_position_op(
  body: UpdatePositionOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboinvestor),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="update-position",
    idempotency_key=idempotency_key,
    body=body,
  )
  updates = body.model_dump(exclude_unset=True, exclude={"position_id"})

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        result = cmd_update_position(session, body.position_id, updates)
    except ProgrammingError:
      raise _not_initialized_404()
    if result is None:
      raise HTTPException(status_code=404, detail="Position not found.")
    return result

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/delete-position",
  response_model=OperationEnvelope,
  operation_id="opDeletePosition",
  summary="Delete Position",
  description="Soft-deletes the position (`is_active=false`). Historical holding records referencing it remain valid.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboinvestor/{graph_id}/operations/delete-position",
  method="POST",
  business_event_type="investor_delete_position",
)
async def delete_position_op(
  body: DeletePositionOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboinvestor),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="delete-position",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        deleted = cmd_soft_delete_position(session, body.position_id)
    except ProgrammingError:
      raise _not_initialized_404()
    if not deleted:
      raise HTTPException(status_code=404, detail="Position not found.")
    return DeleteResult(deleted=True)

  return await _dispatch(ctx, _runner, cache)
