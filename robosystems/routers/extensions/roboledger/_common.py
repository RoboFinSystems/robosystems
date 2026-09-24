"""Shared plumbing for the RoboLedger operation routers: context builder,
dispatcher, schema-missing 404, registrar factory and write gate.

Kept apart from the operations package so `views.py` (mounted on
`FACT_GRID_ENABLED` for SEC-only deployments) can reach `_dispatch` without
importing the ledger command surface.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path

from robosystems.db.extensions import extensions_session
from robosystems.middleware.auth.dependencies import (
  get_current_user_with_graph,
  require_graph_write_role,
)
from robosystems.middleware.extensions import (
  GraphExtensionContext,
  OperationRegistrar,
  guard_command_runner,
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
)
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.core import User
from robosystems.operations.roboledger.fiscal_calendar.close_service import (
  PeriodCloseService,
)
from robosystems.operations.roboledger.fiscal_calendar.service import (
  FiscalCalendarService,
)

_RATE_LIMIT = Depends(subscription_aware_rate_limit_dependency)

# Stateless service singletons, reused across requests.
_fiscal_svc = FiscalCalendarService()
_close_svc = PeriodCloseService(_fiscal_svc)


def _ctx(
  *,
  graph_id: str,
  user_id: str,
  op: str,
  idempotency_key: str | None,
  body: object,
) -> OperationContext:
  return OperationContext(
    domain="roboledger",
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
  """Run `execute_operation` and translate idempotency conflicts to 409.

  Wraps hand-written runners in `guard_command_runner` so the registrar's
  error policy (schema-missing → 404, DB faults → 500, unmapped `ValueError`
  → 422) applies identically to both registration styles.
  """
  guarded = guard_command_runner(
    runner,
    op_name=ctx.operation_name,
    graph_id=ctx.graph_id,
    schema_missing_404=_ledger_404,
  )
  try:
    return await execute_operation(
      ctx, guarded, idempotency_cache=cache, on_fresh_success=on_fresh_success
    )
  except IdempotencyKeyConflictError as exc:
    raise HTTPException(status_code=409, detail=str(exc))


def _result_payload(envelope: OperationEnvelope) -> dict[str, Any]:
  """An `on_fresh_success` envelope's result as a plain dict.

  `wrap_completed` has already run `model_dump(mode="json")`, so attribute
  access on `envelope.result` silently reads nothing.
  """
  result = envelope.result
  return result if isinstance(result, dict) else {}


def _ledger_404() -> HTTPException:
  return HTTPException(
    status_code=404,
    detail="Ledger not initialized. Connect a data source first.",
  )


def make_registrar(router: APIRouter, tag: str) -> OperationRegistrar:
  """A registrar mounting on `router`, publishing under OpenAPI `tag`.

  One per operation module; only the tag varies. Safe for the MCP tool
  adapter: `OperationRegistrar.specs_for_extension` walks every registrar.
  """
  return OperationRegistrar(
    router=router,
    domain="roboledger",
    tag=tag,
    rate_limit_dep=_RATE_LIMIT,
    ctx_builder=_ctx,
    dispatcher=_dispatch,
    session_factory=extensions_session,
    schema_missing_404=_ledger_404,
    user_dep=get_current_user_with_graph,
    graph_id_pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
    extension="roboledger",
  )


# One shared callable so FastAPI's dependency cache resolves it once per request.
_require_roboledger = require_graph_extension("roboledger")


def _require_roboledger_write(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
) -> User:
  """Membership + provisioning + write role for the hand-written ops.

  Neither `get_current_user_with_graph` nor `_require_roboledger` checks the
  graph role, so a `viewer` would pass both; the registrar applies
  `require_graph_write_role` to its ops, and hand-written ops use this.
  """
  require_graph_write_role(str(user.id), graph_id)
  return user
