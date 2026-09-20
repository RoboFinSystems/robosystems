"""Shared plumbing for the RoboLedger operation routers.

The operation surface is split one module per OpenAPI tag (see
`operations/__init__.py`), and `views.py` and `reads.py` are two more routers
on the same URL prefix. All of them build the same operation context, run
through the same dispatcher, and answer a missing tenant schema the same way
— so that machinery lives here rather than in whichever module happened to
define it first.

Keeping it separate also keeps `views.py` honest: the fact-grid router mounts
on `FACT_GRID_ENABLED` for SEC-only deployments, and importing it should not
drag in the whole ledger command surface just to reach `_dispatch`.
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
  """Build the per-request operation context with body fingerprint."""
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

  Every hand-written runner reaches the operation through here, and is
  wrapped in `guard_command_runner` on the way, so the registrar's error
  policy (schema-missing → 404, other database faults → logged 500, unmapped
  `ValueError` → 422) applies to both registration styles identically.
  Handlers keep only their domain-specific mappings.
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
  """Return an `on_fresh_success` envelope's result as a plain dict.

  `wrap_completed` runs every command result through
  `model_dump(mode="json")`, so a hook that reaches for `envelope.result.foo`
  reads absent rather than raising — the share, revoke, and purge hooks all
  shipped that way and marked nothing stale. Going through this helper keeps
  the normalization visible at each call site.
  """
  result = envelope.result
  return result if isinstance(result, dict) else {}


def _ledger_404() -> HTTPException:
  return HTTPException(
    status_code=404,
    detail="Ledger not initialized. Connect a data source first.",
  )


def make_registrar(router: APIRouter, tag: str) -> OperationRegistrar:
  """A registrar mounting on `router`, publishing under `tag`.

  One per operation module. The tag is the only thing that varies: every
  registrar shares this package's context builder, dispatcher, session
  factory and gates, so two operations differ in their OpenAPI grouping and
  in nothing else.

  Splitting the surface across registrars is safe for the MCP tool adapter —
  `OperationRegistrar.specs_for_extension` walks every instantiated
  registrar with a matching `extension`, so a spec is found wherever its
  module lives.
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


# Shared callable so every hand-written `@router.post` endpoint across the
# operation modules participates in FastAPI's dependency-resolution cache
# (one factory call at import, not one per endpoint).
_require_roboledger = require_graph_extension("roboledger")


def _require_roboledger_write(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
) -> User:
  """Membership + provisioning + write role for the hand-written ops.

  Every `@router.post` in the operation modules is a command.
  `get_current_user_with_graph`
  proves graph *membership* and `_require_roboledger` proves *provisioning*;
  neither consults the graph role, so a read-only `viewer` clears both on its
  own. `require_graph_write_role` is the gate that stops that, and the
  registrar applies it to every op it mounts (`middleware/extensions.py`).
  Hand-written ops depend on this helper so both registration styles enforce
  identically.
  """
  require_graph_write_role(str(user.id), graph_id)
  return user
