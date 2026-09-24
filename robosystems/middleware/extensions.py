"""Declarative operation registrar for extensions endpoints.

An `OperationSpec` describes a write operation; an `OperationRegistrar`
mounts it as a POST handler. The spec is adapter-neutral, so MCP and
Operator tools are generated from the same declaration. Operations needing
async dispatch or platform-DB dependencies use hand-written handlers.
"""

import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, ClassVar

from fastapi import APIRouter, Depends, Header, HTTPException, Path
from pydantic import BaseModel
from sqlalchemy.exc import DBAPIError, ProgrammingError
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.db.extensions import is_statement_timeout
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import require_graph_write_role
from robosystems.middleware.billing.enforcement import require_graph_access
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.graph.utils.subgraph import is_subgraph
from robosystems.middleware.operations import (
  IdempotencyCache,
  OperationContext,
  OperationEnvelope,
  get_idempotency_cache,
)
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.models.api.common import OPERATION_ERROR_RESPONSES
from robosystems.models.core import Graph
from robosystems.operations.extensions.staleness import mark_graph_stale

# ── Error-map types ──────────────────────────────────────────────────────

ErrorDetailFactory = Callable[[Exception], str]

# A bare status code (detail is `str(exc)`) or (status, detail_factory).
ErrorMapEntry = int | tuple[int, ErrorDetailFactory]

# Matched with `isinstance` in insertion order: list subclasses first.
ErrorMap = dict[type[Exception], ErrorMapEntry]


def is_schema_missing(exc: ProgrammingError) -> bool:
  """Whether a ``ProgrammingError`` is a missing tenant schema or table: the
  one "not initialized" case. Every other programming error must surface.
  """
  msg = str(exc)
  return "does not exist" in msg and ("schema" in msg or "relation" in msg)


# A statement cut short by the session ceiling: a timeout, not a code fault.
# The message never carries the SQL.
STATEMENT_TIMEOUT_STATUS = 504
STATEMENT_TIMEOUT_DETAIL = (
  "The request exceeded the per-statement time limit for this surface. "
  "Narrow the request (filters, a smaller page) or retry shortly."
)


def statement_timeout_504(op_name: str, graph_id: str) -> HTTPException:
  """The 504 a cancelled statement surfaces as, logged once at the boundary
  so the tenant and operation are attributable without the SQL."""
  logger.warning(f"{op_name} on {graph_id}: statement exceeded the interactive ceiling")
  return HTTPException(
    status_code=STATEMENT_TIMEOUT_STATUS,
    detail=STATEMENT_TIMEOUT_DETAIL,
    headers={"Retry-After": "5"},
  )


def guard_command_runner(
  runner: Callable[[], Any],
  *,
  op_name: str,
  graph_id: str,
  schema_missing_404: Callable[[], HTTPException],
) -> Callable[[], Any]:
  """Give a hand-written command runner the registrar's error policy.

  Outermost layer, so the handler's own ``except`` clauses win. A missing
  schema is the 404, any other ``ProgrammingError`` a logged 500, a statement
  timeout the 504, and an unmapped ``ValueError`` a logged 422.
  """

  def _guarded() -> Any:
    try:
      return runner()
    except HTTPException:
      raise
    except ProgrammingError as exc:
      if is_schema_missing(exc):
        raise schema_missing_404()
      logger.warning(
        f"{op_name} on {graph_id}: database programming error", exc_info=True
      )
      raise
    except DBAPIError as exc:
      if is_statement_timeout(exc):
        raise statement_timeout_504(op_name, graph_id) from exc
      raise
    except ValueError as exc:
      logger.warning(f"{op_name} on {graph_id}: unmapped {type(exc).__name__}: {exc}")
      raise HTTPException(status_code=422, detail=str(exc))

  return _guarded


# ── OperationSpec ────────────────────────────────────────────────────────


@dataclass
class OperationSpec:
  """Declarative description of one extensions operation.

  name: kebab-case; drives the URL path, operationId, audit and metrics.
  command: `(session, body, /, **kwargs) -> Response`.
  requires_graph_id: pass the path graph_id to the command, for cross-DB
    lookups the tenant session can't make.
  pre_validate: runs before any DB session opens; may raise HTTPException.
  on_fresh_success / mark_stale_reason: effect on a non-replayed success;
    `on_fresh_success` wins if both are set.
  result_type: types the envelope's `result` in OpenAPI.
  """

  name: str
  summary: str
  command: Callable
  request_model: type[BaseModel]
  error_map: ErrorMap = field(default_factory=dict)
  description: str | None = None
  path: str | None = None
  business_event_type: str | None = None
  requires_created_by: bool = True
  requires_graph_id: bool = False
  pre_validate: Callable[[BaseModel], None] | None = None
  on_fresh_success: Callable | None = None
  mark_stale_reason: str | None = None
  result_type: type[BaseModel] | None = None

  @property
  def resolved_path(self) -> str:
    return self.path or f"/{self.name}"

  def resolve_business_event_type(self, domain: str) -> str:
    """The explicit `business_event_type`, else `{domain}_{snake_name}`."""
    if self.business_event_type:
      return self.business_event_type
    snake = self.name.replace("-", "_")
    return f"{domain}_{snake}"

  @property
  def openapi_operation_id(self) -> str:
    """create-agent -> createAgent."""
    parts = self.name.split("-")
    return parts[0] + "".join(p.capitalize() for p in parts[1:])


# ── Extension feature gate ───────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class GraphExtensionContext:
  """The graph fields the extension gate reads, returned so handlers don't
  reload the row."""

  graph_type: str
  schema_extensions: tuple[str, ...]
  is_repository: bool


def load_graph_metadata(graph_id: str, session: Session) -> GraphExtensionContext:
  """Load the gate's fields; a missing graph is 403, not 404, so existence
  can't be enumerated."""
  graph = Graph.get_by_id(graph_id, session)
  if graph is None:
    raise HTTPException(
      status_code=403,
      detail=f"Access denied to graph: {graph_id}",
    )
  return GraphExtensionContext(
    graph_type=graph.graph_type or "",
    schema_extensions=tuple(graph.schema_extensions or []),
    is_repository=bool(graph.is_repository),
  )


def require_graph_extension(extension: str) -> Callable[..., GraphExtensionContext]:
  """Dependency factory gating an extensions route on the graph.

  Runs after graph-membership auth. Refuses subgraphs, repository graphs
  (written only by ingestion) and graphs without `extension` provisioned,
  then applies the lifecycle gate at read strength.
  """

  def _dep(
    graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
    session: Session = Depends(get_db_session),
  ) -> GraphExtensionContext:
    # Subgraphs have no tenant schema; the route pattern admits them anyway.
    if is_subgraph(graph_id):
      raise HTTPException(
        status_code=403,
        detail=(
          f"Subgraph '{graph_id}' is not addressable via {extension} "
          "operations; target the parent graph."
        ),
      )
    meta = load_graph_metadata(graph_id, session)
    if meta.is_repository or meta.graph_type == "repository":
      raise HTTPException(
        status_code=403,
        detail=f"{extension} commands are not available on repository graphs",
      )
    if extension not in meta.schema_extensions:
      raise HTTPException(
        status_code=403,
        detail=f"{extension} is not provisioned for this graph",
      )
    # Read strength here closes the whole surface on a suspended graph;
    # commands add the write-strength check via `require_graph_write_role`.
    require_graph_access(graph_id, session, require_write=False)
    return meta

  return _dep


# ── Registrar ────────────────────────────────────────────────────────────


class OperationRegistrar:
  """Binds one domain's plumbing and mounts its `OperationSpec`s as routes."""

  # Every registrar, populated at import, so tool adapters can find all specs.
  _all_instances: ClassVar[list["OperationRegistrar"]] = []

  def __init__(
    self,
    *,
    router: APIRouter,
    domain: str,
    tag: str,
    rate_limit_dep: Any,
    ctx_builder: Callable[..., OperationContext],
    dispatcher: Callable,
    session_factory: Callable,
    schema_missing_404: Callable[[], HTTPException],
    user_dep: Callable,
    graph_id_pattern: str,
    extension: str,
  ) -> None:
    self.router = router
    self.domain = domain
    self.tag = tag
    self.rate_limit_dep = rate_limit_dep
    self.ctx_builder = ctx_builder
    self.dispatcher = dispatcher
    self.session_factory = session_factory
    self.schema_missing_404 = schema_missing_404
    self.user_dep = user_dep
    self.graph_id_pattern = graph_id_pattern
    self.extension = extension
    # Built once so FastAPI can cache it across routes.
    self._extension_dep = require_graph_extension(extension)
    self.full_path_template = f"/extensions/{domain}/{{graph_id}}/operations"
    self._registered: list[OperationSpec] = []
    OperationRegistrar._all_instances.append(self)

  @classmethod
  def specs_for_extension(
    cls, extension: str
  ) -> list[tuple["OperationRegistrar", OperationSpec]]:
    """Every `(registrar, spec)` pair for the extension, for tool adapters."""
    return [
      (reg, spec)
      for reg in cls._all_instances
      if reg.extension == extension
      for spec in reg._registered
    ]

  def register(self, spec: OperationSpec) -> Callable:
    """Mount a POST handler for `spec` and return it (metrics-wrapped)."""
    handler = self._build_handler(spec)
    metrics_wrapped = endpoint_metrics_decorator(
      f"{self.full_path_template}{spec.resolved_path}",
      method="POST",
      business_event_type=spec.resolve_business_event_type(self.domain),
    )(handler)
    response_model = (
      OperationEnvelope[spec.result_type]
      if spec.result_type is not None
      else OperationEnvelope
    )
    self.router.post(
      spec.resolved_path,
      response_model=response_model,
      operation_id=spec.openapi_operation_id,
      summary=spec.summary,
      description=spec.description,
      tags=[self.tag],
      dependencies=[self.rate_limit_dep],
      responses={**OPERATION_ERROR_RESPONSES},
    )(metrics_wrapped)
    self._registered.append(spec)
    return metrics_wrapped

  @property
  def registered_specs(self) -> list[OperationSpec]:
    """All `OperationSpec`s registered through this registrar."""
    return list(self._registered)

  def _build_handler(self, spec: OperationSpec) -> Callable:
    request_model = spec.request_model
    error_map = spec.error_map
    pre_validate = spec.pre_validate
    on_fresh_success = spec.on_fresh_success
    mark_stale_reason = spec.mark_stale_reason
    requires_created_by = spec.requires_created_by
    requires_graph_id = spec.requires_graph_id
    op_name = spec.name
    ctx_builder = self.ctx_builder
    dispatcher = self.dispatcher
    schema_missing_404 = self.schema_missing_404
    graph_id_pattern = self.graph_id_pattern
    user_dep = self.user_dep
    extension_dep = self._extension_dep
    # Late-bound through sys.modules so `mock.patch` at the source applies.
    cmd_module_name = spec.command.__module__
    cmd_func_name = spec.command.__name__
    sf_module_name = self.session_factory.__module__
    sf_func_name = self.session_factory.__qualname__

    def _resolve_command() -> Callable:
      return getattr(sys.modules[cmd_module_name], cmd_func_name)

    def _resolve_session_factory() -> Callable:
      return getattr(sys.modules[sf_module_name], sf_func_name)

    async def handler(
      body: BaseModel,
      graph_id: str = Path(..., pattern=graph_id_pattern),
      user=Depends(user_dep),
      _ext: GraphExtensionContext = Depends(extension_dep),
      idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
      cache: IdempotencyCache = Depends(get_idempotency_cache),
    ) -> OperationEnvelope:
      # Membership isn't write access: enforce the write role first.
      require_graph_write_role(str(user.id), graph_id)

      if pre_validate is not None:
        pre_validate(body)

      ctx = ctx_builder(
        graph_id=graph_id,
        user_id=str(user.id),
        op=op_name,
        idempotency_key=idempotency_key,
        body=body,
      )

      def _runner():
        command = _resolve_command()
        # Tells a factory ValueError (bad graph id) from a command one.
        session_bound = False
        try:
          with _resolve_session_factory()(graph_id) as session:
            session_bound = True
            try:
              kwargs = {}
              if requires_created_by:
                kwargs["created_by"] = str(user.id)
              if requires_graph_id:
                kwargs["graph_id"] = graph_id
              return command(session, body, **kwargs)
            except tuple(error_map.keys()) as exc:
              _raise_mapped(exc, error_map)
              raise AssertionError("unreachable: _raise_mapped always raises")
        except ProgrammingError as exc:
          if is_schema_missing(exc):
            raise schema_missing_404()
          logger.warning(
            f"{op_name} on {graph_id}: database programming error", exc_info=True
          )
          raise
        except DBAPIError as exc:
          if is_statement_timeout(exc):
            raise statement_timeout_504(op_name, graph_id) from exc
          raise
        except ValueError as exc:
          if not session_bound:
            raise schema_missing_404()
          # Unmapped domain refusal: 422, logged so the map gets fixed.
          logger.warning(
            f"{op_name} on {graph_id}: unmapped {type(exc).__name__}: {exc}"
          )
          raise HTTPException(status_code=422, detail=str(exc))

      effective_on_fresh_success = on_fresh_success
      if effective_on_fresh_success is None and mark_stale_reason is not None:
        _reason = mark_stale_reason

        def _mark_stale(_env, _g=graph_id, _r=_reason):
          mark_graph_stale(_g, _r)

        effective_on_fresh_success = _mark_stale

      return await dispatcher(
        ctx, _runner, cache, on_fresh_success=effective_on_fresh_success
      )

    handler.__name__ = f"{op_name.replace('-', '_')}_op"
    handler.__qualname__ = handler.__name__
    # FastAPI must see the concrete request model to validate the body.
    handler.__annotations__ = {
      **handler.__annotations__,
      "body": request_model,
    }
    return handler


def _raise_mapped(exc: Exception, error_map: ErrorMap) -> None:
  """Raise the `HTTPException` `error_map` names for `exc`."""
  for exc_type, mapping in error_map.items():
    if isinstance(exc, exc_type):
      if isinstance(mapping, int):
        raise HTTPException(status_code=mapping, detail=str(exc))
      status_code, detail_factory = mapping
      raise HTTPException(status_code=status_code, detail=detail_factory(exc))
  raise exc


__all__ = [
  "ErrorDetailFactory",
  "ErrorMap",
  "ErrorMapEntry",
  "GraphExtensionContext",
  "OperationRegistrar",
  "OperationSpec",
  "guard_command_runner",
  "is_schema_missing",
  "load_graph_metadata",
  "require_graph_extension",
]
